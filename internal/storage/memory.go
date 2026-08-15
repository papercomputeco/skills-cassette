package storage

import (
	"context"
	"slices"
	"sort"
	"strings"
	"sync"
)

// MemoryStore is the no-database Store. It exists so the cassette starts (and
// its handlers test) without Postgres; it implements the same search, scope,
// sort, and keyset-pagination semantics as the Postgres driver.
type MemoryStore struct {
	mu       sync.Mutex
	skills   map[string]SkillRecord
	versions map[string][]SkillVersionRecord
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		skills:   map[string]SkillRecord{},
		versions: map[string][]SkillVersionRecord{},
	}
}

var _ Store = (*MemoryStore)(nil)

// Kind names the backing store, echoed by /ping so a demo can never be
// misread as durable when it is not.
func (s *MemoryStore) Kind() string { return "memory" }

// Close implements Store; a memory store has nothing to release.
func (s *MemoryStore) Close() {}

// UpsertSkill inserts or replaces a skill keyed by id, preserving created_at,
// author_subject, and download_count on update — the same fields the Postgres
// upsert leaves authoritative.
func (s *MemoryStore) UpsertSkill(_ context.Context, rec SkillRecord) (*SkillRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec.Tags = cloneStrings(rec.Tags)
	rec.GeneratedFromSessionIDs = cloneStrings(rec.GeneratedFromSessionIDs)
	if existing, ok := s.skills[rec.ID]; ok {
		rec.CreatedAt = existing.CreatedAt
		rec.AuthorSubject = existing.AuthorSubject
		rec.DownloadCount = existing.DownloadCount
	}
	s.skills[rec.ID] = rec
	out := rec
	return &out, nil
}

// GetSkill returns a skill by id, or nil when absent.
func (s *MemoryStore) GetSkill(_ context.Context, id string) (*SkillRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rec, ok := s.skills[id]; ok {
		out := rec
		return &out, nil
	}
	return nil, nil
}

// DeleteSkill removes a skill and its version history, reporting whether a
// skill row actually existed.
func (s *MemoryStore) DeleteSkill(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.skills[id]; !ok {
		return false, nil
	}
	delete(s.skills, id)
	delete(s.versions, id)
	return true, nil
}

// ListSkills returns one keyset page honoring the search/scope filters, the
// requested sort, and the cursor in opts.
func (s *MemoryStore) ListSkills(_ context.Context, opts SkillListOpts) ([]SkillRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}

	matched := make([]SkillRecord, 0, len(s.skills))
	for _, rec := range s.skills {
		if !matchesQuery(rec, opts.Query) {
			continue
		}
		if opts.Author != "" && rec.AuthorSubject != opts.Author {
			continue
		}
		if opts.NotAuthor != "" && rec.AuthorSubject == opts.NotAuthor {
			continue
		}
		if opts.Status != "" && rec.Status != opts.Status {
			continue
		}
		matched = append(matched, rec)
	}

	byDownloads := opts.Sort == SkillSortDownloads
	sort.Slice(matched, func(i, j int) bool {
		if byDownloads {
			if matched[i].DownloadCount != matched[j].DownloadCount {
				return matched[i].DownloadCount > matched[j].DownloadCount
			}
		} else if !matched[i].UpdatedAt.Equal(matched[j].UpdatedAt) {
			return matched[i].UpdatedAt.After(matched[j].UpdatedAt)
		}
		return matched[i].ID > matched[j].ID
	})

	// Keyset: keep rows strictly after the cursor row in sort order.
	if opts.CursorID != "" {
		filtered := matched[:0]
		for _, rec := range matched {
			if byDownloads {
				if opts.CursorDownloads == nil {
					continue
				}
				if rec.DownloadCount < *opts.CursorDownloads ||
					(rec.DownloadCount == *opts.CursorDownloads && rec.ID < opts.CursorID) {
					filtered = append(filtered, rec)
				}
				continue
			}
			if opts.CursorTs == nil {
				continue
			}
			if rec.UpdatedAt.Before(*opts.CursorTs) ||
				(rec.UpdatedAt.Equal(*opts.CursorTs) && rec.ID < opts.CursorID) {
				filtered = append(filtered, rec)
			}
		}
		matched = filtered
	}

	if len(matched) > limit {
		matched = matched[:limit]
	}
	out := make([]SkillRecord, len(matched))
	copy(out, matched)
	return out, nil
}

// ListSkillsBySession returns the skills generated from a given session
// (reverse lookup over provenance), newest-edited first.
func (s *MemoryStore) ListSkillsBySession(_ context.Context, sessionID string) ([]SkillRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]SkillRecord, 0)
	for _, rec := range s.skills {
		if slices.Contains(rec.GeneratedFromSessionIDs, sessionID) {
			out = append(out, rec)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].UpdatedAt.After(out[j].UpdatedAt)
		}
		return out[i].ID > out[j].ID
	})
	return out, nil
}

// CountSkills returns the per-tab totals for a search, ignoring scope and
// cursor so every tab shows its full size for the active query.
func (s *MemoryStore) CountSkills(_ context.Context, query, author string) (SkillCounts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var counts SkillCounts
	for _, rec := range s.skills {
		if !matchesQuery(rec, query) {
			continue
		}
		counts.Total++
		if rec.AuthorSubject == author {
			counts.Mine++
		}
		if rec.Status == SkillStatusDraft {
			counts.Drafts++
		}
	}
	return counts, nil
}

// NextSkillVersionNumber returns the next monotonic version number for a
// skill (1 when nothing is published yet).
func (s *MemoryStore) NextSkillVersionNumber(_ context.Context, skillID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	maxN := 0
	for _, v := range s.versions[skillID] {
		if v.VersionNumber > maxN {
			maxN = v.VersionNumber
		}
	}
	return maxN + 1, nil
}

// PublishSkillVersion appends an immutable published snapshot and advances
// the skill head under one lock hold, refusing a duplicate
// (skill_id, version_number) with ErrSkillVersionConflict exactly as the
// Postgres unique constraint would. The head only moves when this is the
// highest published number, so an older overlapping publish never regresses
// a newer head.
func (s *MemoryStore) PublishSkillVersion(_ context.Context, rec SkillVersionRecord) (*SkillVersionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	highest := true
	for _, v := range s.versions[rec.SkillID] {
		if v.VersionNumber == rec.VersionNumber {
			return nil, ErrSkillVersionConflict
		}
		if v.VersionNumber > rec.VersionNumber {
			highest = false
		}
	}
	s.versions[rec.SkillID] = append(s.versions[rec.SkillID], rec)

	if skill, ok := s.skills[rec.SkillID]; ok && highest {
		skill.Version = rec.Semver
		skill.Content = rec.Content
		skill.Status = SkillStatusPublished
		skill.Visibility = "team"
		skill.UpdatedAt = rec.PublishedAt
		s.skills[rec.SkillID] = skill
	}
	out := rec
	return &out, nil
}

// ListSkillVersions returns a skill's published history, newest first.
func (s *MemoryStore) ListSkillVersions(_ context.Context, skillID string) ([]SkillVersionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]SkillVersionRecord, len(s.versions[skillID]))
	copy(out, s.versions[skillID])
	sort.Slice(out, func(i, j int) bool { return out[i].VersionNumber > out[j].VersionNumber })
	return out, nil
}

// IncrementSkillDownloads bumps the real download counter for a skill.
func (s *MemoryStore) IncrementSkillDownloads(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rec, ok := s.skills[id]; ok {
		rec.DownloadCount++
		s.skills[id] = rec
	}
	return nil
}

// matchesQuery mirrors the Postgres ILIKE predicates: a case-insensitive
// substring match over name, description, and each tag.
func matchesQuery(rec SkillRecord, query string) bool {
	if query == "" {
		return true
	}
	needle := strings.ToLower(query)
	if strings.Contains(strings.ToLower(rec.Name), needle) ||
		strings.Contains(strings.ToLower(rec.Description), needle) {
		return true
	}
	for _, tag := range rec.Tags {
		if strings.Contains(strings.ToLower(tag), needle) {
			return true
		}
	}
	return false
}

func cloneStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}
