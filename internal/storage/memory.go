package storage

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

type MemoryStore struct {
	mu       sync.Mutex
	skills   map[string]SkillRecord
	drafts   map[string]SkillDraftRecord
	versions map[string][]SkillVersionRecord
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		skills: map[string]SkillRecord{}, drafts: map[string]SkillDraftRecord{},
		versions: map[string][]SkillVersionRecord{},
	}
}

var _ Store = (*MemoryStore)(nil)

func (s *MemoryStore) Kind() string { return "memory" }
func (s *MemoryStore) Close()       {}

func (s *MemoryStore) CreateDraft(_ context.Context, identity SkillRecord, draft SkillDraftRecord) (*SkillDraftRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skills[identity.ID]; ok {
		return nil, ErrSkillExists
	}
	identity.Visibility = defaultString(identity.Visibility, "private")
	identity.CurrentVersionNumber = 0
	s.skills[identity.ID] = identity
	draft.SkillID = identity.ID
	draft.Revision = 1
	draft.Tags = cloneStrings(draft.Tags)
	draft.GeneratedFromSessionIDs = cloneStrings(draft.GeneratedFromSessionIDs)
	s.drafts[identity.ID] = draft
	out := cloneDraft(draft)
	return &out, nil
}

func (s *MemoryStore) CreateDraftFromPublished(_ context.Context, skillID string, now time.Time) (*SkillDraftRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.drafts[skillID]; ok {
		return nil, ErrDraftExists
	}
	skill, ok := s.skills[skillID]
	if !ok || skill.CurrentVersionNumber == 0 {
		return nil, ErrDraftNotFound
	}
	published := s.currentVersionLocked(skillID)
	if published == nil {
		return nil, ErrDraftNotFound
	}
	draft := draftFromVersion(*published, now)
	s.drafts[skillID] = draft
	out := cloneDraft(draft)
	return &out, nil
}

func (s *MemoryStore) GetDraft(_ context.Context, skillID string) (*SkillDraftRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	draft, ok := s.drafts[skillID]
	if !ok {
		return nil, nil
	}
	out := cloneDraft(draft)
	return &out, nil
}

func (s *MemoryStore) ListDrafts(_ context.Context) ([]SkillDraftRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SkillDraftRecord, 0, len(s.drafts))
	for _, draft := range s.drafts {
		out = append(out, cloneDraft(draft))
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].UpdatedAt.Equal(out[j].UpdatedAt) {
			return out[i].UpdatedAt.After(out[j].UpdatedAt)
		}
		return out[i].SkillID > out[j].SkillID
	})
	return out, nil
}

func (s *MemoryStore) UpdateDraft(_ context.Context, draft SkillDraftRecord, expectedRevision int) (*SkillDraftRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.drafts[draft.SkillID]
	if !ok {
		return nil, ErrDraftNotFound
	}
	if current.Revision != expectedRevision {
		return nil, ErrDraftRevisionConflict
	}
	draft.Revision = current.Revision + 1
	draft.CreatedAt = current.CreatedAt
	draft.Tags = cloneStrings(draft.Tags)
	draft.GeneratedFromSessionIDs = cloneStrings(current.GeneratedFromSessionIDs)
	s.drafts[draft.SkillID] = draft
	out := cloneDraft(draft)
	return &out, nil
}

func (s *MemoryStore) PublishDraft(_ context.Context, skillID string, expectedRevision int, changelog, author string, publishedAt time.Time) (*SkillVersionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	draft, ok := s.drafts[skillID]
	if !ok {
		return nil, ErrDraftNotFound
	}
	if draft.Revision != expectedRevision {
		return nil, ErrDraftRevisionConflict
	}
	identity, ok := s.skills[skillID]
	if !ok {
		return nil, ErrDraftNotFound
	}
	number := identity.CurrentVersionNumber + 1
	version := SkillVersionRecord{
		SkillID: skillID, VersionNumber: number, Semver: fmt.Sprintf("0.1.%d", number-1),
		Slug: draft.Slug, Name: draft.Name, Description: draft.Description, Type: draft.Type,
		Tags: cloneStrings(draft.Tags), Content: draft.Content, IsAIGenerated: draft.IsAIGenerated,
		GeneratedFromSessionIDs: cloneStrings(draft.GeneratedFromSessionIDs), Changelog: changelog,
		AuthorSubject: author, PublishedAt: publishedAt,
	}
	s.versions[skillID] = append(s.versions[skillID], version)
	identity.CurrentVersionNumber = number
	identity.Visibility = "team"
	identity.UpdatedAt = publishedAt
	s.skills[skillID] = identity
	delete(s.drafts, skillID)
	out := cloneVersion(version)
	return &out, nil
}

func (s *MemoryStore) GetSkillIdentity(_ context.Context, id string) (*SkillRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, ok := s.skills[id]
	if !ok {
		return nil, nil
	}
	out := identity
	return &out, nil
}

func (s *MemoryStore) GetSkill(_ context.Context, id string) (*SkillRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.publishedLocked(id), nil
}

func (s *MemoryStore) DeleteSkill(_ context.Context, id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skills[id]; !ok {
		return false, nil
	}
	delete(s.skills, id)
	delete(s.drafts, id)
	delete(s.versions, id)
	return true, nil
}

func (s *MemoryStore) ListSkills(_ context.Context, opts SkillListOpts) ([]SkillRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	matched := make([]SkillRecord, 0, len(s.skills))
	for id, identity := range s.skills {
		if identity.CurrentVersionNumber == 0 {
			continue
		}
		rec := s.publishedLocked(id)
		if rec == nil || !matchesQuery(*rec, opts.Query) ||
			opts.Author != "" && rec.AuthorSubject != opts.Author ||
			opts.NotAuthor != "" && rec.AuthorSubject == opts.NotAuthor {
			continue
		}
		matched = append(matched, *rec)
	}
	byDownloads := opts.Sort == SkillSortDownloads
	sort.Slice(matched, func(i, j int) bool {
		if byDownloads && matched[i].DownloadCount != matched[j].DownloadCount {
			return matched[i].DownloadCount > matched[j].DownloadCount
		}
		if !byDownloads && !matched[i].UpdatedAt.Equal(matched[j].UpdatedAt) {
			return matched[i].UpdatedAt.After(matched[j].UpdatedAt)
		}
		return matched[i].ID > matched[j].ID
	})
	if opts.CursorID != "" {
		filtered := matched[:0]
		for _, rec := range matched {
			if byDownloads {
				if opts.CursorDownloads != nil && (rec.DownloadCount < *opts.CursorDownloads ||
					rec.DownloadCount == *opts.CursorDownloads && rec.ID < opts.CursorID) {
					filtered = append(filtered, rec)
				}
			} else if opts.CursorTs != nil && (rec.UpdatedAt.Before(*opts.CursorTs) ||
				rec.UpdatedAt.Equal(*opts.CursorTs) && rec.ID < opts.CursorID) {
				filtered = append(filtered, rec)
			}
		}
		matched = filtered
	}
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched, nil
}

func (s *MemoryStore) ListSkillsBySession(_ context.Context, sessionID string) ([]SkillRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []SkillRecord{}
	for id, identity := range s.skills {
		if identity.CurrentVersionNumber == 0 {
			continue
		}
		rec := s.publishedLocked(id)
		if rec != nil && slices.Contains(rec.GeneratedFromSessionIDs, sessionID) {
			out = append(out, *rec)
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

func (s *MemoryStore) CountSkills(_ context.Context, query, author string) (SkillCounts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var counts SkillCounts
	for id, identity := range s.skills {
		if identity.CurrentVersionNumber == 0 {
			continue
		}
		rec := s.publishedLocked(id)
		if rec == nil || !matchesQuery(*rec, query) {
			continue
		}
		counts.Total++
		if rec.AuthorSubject == author {
			counts.Mine++
		}
	}
	return counts, nil
}

func (s *MemoryStore) ListSkillVersions(_ context.Context, skillID string) ([]SkillVersionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]SkillVersionRecord, len(s.versions[skillID]))
	for i, version := range s.versions[skillID] {
		out[i] = cloneVersion(version)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VersionNumber > out[j].VersionNumber })
	return out, nil
}

func (s *MemoryStore) IncrementSkillDownloads(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	identity, ok := s.skills[id]
	if ok && identity.CurrentVersionNumber > 0 {
		identity.DownloadCount++
		s.skills[id] = identity
	}
	return nil
}

func (s *MemoryStore) currentVersionLocked(id string) *SkillVersionRecord {
	identity, ok := s.skills[id]
	if !ok || identity.CurrentVersionNumber == 0 {
		return nil
	}
	for i := range s.versions[id] {
		if s.versions[id][i].VersionNumber == identity.CurrentVersionNumber {
			out := cloneVersion(s.versions[id][i])
			return &out
		}
	}
	return nil
}

func (s *MemoryStore) publishedLocked(id string) *SkillRecord {
	identity, ok := s.skills[id]
	if !ok {
		return nil
	}
	version := s.currentVersionLocked(id)
	if version == nil {
		return nil
	}
	identity.Slug, identity.Name, identity.Description, identity.Type = version.Slug, version.Name, version.Description, version.Type
	identity.Version, identity.Tags, identity.Content = version.Semver, cloneStrings(version.Tags), version.Content
	identity.IsAIGenerated = version.IsAIGenerated
	identity.GeneratedFromSessionIDs = cloneStrings(version.GeneratedFromSessionIDs)
	return &identity
}

func draftFromVersion(v SkillVersionRecord, now time.Time) SkillDraftRecord {
	return SkillDraftRecord{SkillID: v.SkillID, Revision: 1, Slug: v.Slug, Name: v.Name,
		Description: v.Description, Type: v.Type, Tags: cloneStrings(v.Tags), Content: v.Content,
		IsAIGenerated: v.IsAIGenerated, GeneratedFromSessionIDs: cloneStrings(v.GeneratedFromSessionIDs),
		CreatedAt: now, UpdatedAt: now}
}

func cloneDraft(in SkillDraftRecord) SkillDraftRecord {
	in.Tags = cloneStrings(in.Tags)
	in.GeneratedFromSessionIDs = cloneStrings(in.GeneratedFromSessionIDs)
	return in
}

func cloneVersion(in SkillVersionRecord) SkillVersionRecord {
	in.Tags = cloneStrings(in.Tags)
	in.GeneratedFromSessionIDs = cloneStrings(in.GeneratedFromSessionIDs)
	return in
}

func matchesQuery(rec SkillRecord, query string) bool {
	if query == "" {
		return true
	}
	needle := strings.ToLower(query)
	if strings.Contains(strings.ToLower(rec.Name), needle) || strings.Contains(strings.ToLower(rec.Description), needle) {
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
	return slices.Clone(in)
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
