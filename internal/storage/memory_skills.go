package storage

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// ResolveEffectiveRevision resolves explicit latest or, when no pointer is
// stored, the deterministic newest-public fallback.
func (s *MemoryStore) ResolveEffectiveRevision(
	_ context.Context,
	opts EffectiveRevisionReadOpts,
) (*AccessibleRevisionRecord, error) {
	if !validUUID(opts.SkillID) {
		return nil, ErrSkillNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	skill, exists := s.skills[opts.SkillID]
	if !exists {
		return nil, ErrSkillNotFound
	}
	return s.resolveEffectiveRevisionLocked(skill), nil
}

// GetEffectiveSkill returns one viewer-aware projection for a stable skill.
func (s *MemoryStore) GetEffectiveSkill(
	_ context.Context,
	opts EffectiveSkillReadOpts,
) (*EffectiveSkillRecord, error) {
	if !validUUID(opts.SkillID) {
		return nil, ErrSkillNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.effectiveSkillLocked(opts.SkillID, opts.CallerSubject, true)
}

// ListEffectiveSkills returns at most one viewer-aware projection per stable
// skill.
func (s *MemoryStore) ListEffectiveSkills(
	_ context.Context,
	opts EffectiveSkillListOpts,
) ([]EffectiveSkillRecord, error) {
	if len(opts.External) > 0 {
		return nil, fmt.Errorf("list effective skills: %w: the in-memory store reads no external views", ErrExternalViewUnavailable)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	matched := make([]EffectiveSkillRecord, 0, len(s.skills))
	for skillID := range s.skills {
		projection, err := s.effectiveSkillLocked(skillID, opts.CallerSubject, false)
		if err != nil {
			continue
		}
		if !matchesEffectiveRevisionQuery(projection.CardRevision, opts.Query) {
			continue
		}
		creator := projection.CardRevision.Revision.CreatorSubject
		if opts.Author != "" && creator != opts.Author {
			continue
		}
		if opts.NotAuthor != "" && creator == opts.NotAuthor {
			continue
		}
		matched = append(matched, *projection)
	}

	byDownloads := opts.Sort == SkillSortDownloads
	sort.Slice(matched, func(left, right int) bool {
		if byDownloads && matched[left].Skill.DownloadCount != matched[right].Skill.DownloadCount {
			return matched[left].Skill.DownloadCount > matched[right].Skill.DownloadCount
		}
		leftUpdated := matched[left].CardRevision.Revision.CreatedAt
		rightUpdated := matched[right].CardRevision.Revision.CreatedAt
		if !byDownloads && !leftUpdated.Equal(rightUpdated) {
			return leftUpdated.After(rightUpdated)
		}
		return matched[left].Skill.ID > matched[right].Skill.ID
	})

	if opts.CursorID != "" {
		filtered := matched[:0]
		for _, projection := range matched {
			if byDownloads {
				if opts.CursorDownloads != nil &&
					(projection.Skill.DownloadCount < *opts.CursorDownloads ||
						(projection.Skill.DownloadCount == *opts.CursorDownloads && projection.Skill.ID < opts.CursorID)) {
					filtered = append(filtered, projection)
				}
				continue
			}
			updatedAt := projection.CardRevision.Revision.CreatedAt
			if opts.CursorTs != nil &&
				(updatedAt.Before(*opts.CursorTs) ||
					(updatedAt.Equal(*opts.CursorTs) && projection.Skill.ID < opts.CursorID)) {
				filtered = append(filtered, projection)
			}
		}
		matched = filtered
	}
	if len(matched) > limit {
		matched = matched[:limit]
	}
	return matched, nil
}

// CountEffectiveSkills counts the same one-entry-per-skill projection selected
// by ListEffectiveSkills.
func (s *MemoryStore) CountEffectiveSkills(
	_ context.Context,
	opts EffectiveSkillCountOpts,
) (SkillCounts, error) {
	if len(opts.External) > 0 {
		return SkillCounts{}, fmt.Errorf("count effective skills: %w: the in-memory store reads no external views", ErrExternalViewUnavailable)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var counts SkillCounts
	for skillID := range s.skills {
		projection, err := s.effectiveSkillLocked(skillID, opts.CallerSubject, false)
		if err != nil || !matchesEffectiveRevisionQuery(projection.CardRevision, opts.Query) {
			continue
		}
		counts.Total++
		if projection.CardRevision.Revision.CreatorSubject == opts.Author {
			counts.Mine++
		}
	}
	return counts, nil
}

// ListEffectiveSkillsBySession returns at most one viewer-aware projection per
// stable skill with accessible provenance for the requested session.
func (s *MemoryStore) ListEffectiveSkillsBySession(
	_ context.Context,
	opts EffectiveSkillSessionListOpts,
) ([]EffectiveSkillRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	type selectedSkill struct {
		id        string
		createdAt time.Time
	}
	selected := make([]selectedSkill, 0, limit)
	for skillID, skill := range s.skills {
		if !s.skillHasAccessibleSessionLocked(skillID, opts.CallerSubject, opts.SessionID) {
			continue
		}
		createdAt, exists := s.effectiveSkillCardCreatedAtLocked(skill, opts.CallerSubject)
		if !exists {
			continue
		}
		selected = append(selected, selectedSkill{id: skillID, createdAt: createdAt})
		sort.Slice(selected, func(left, right int) bool {
			if !selected[left].createdAt.Equal(selected[right].createdAt) {
				return selected[left].createdAt.After(selected[right].createdAt)
			}
			return selected[left].id > selected[right].id
		})
		if len(selected) > limit {
			selected = selected[:limit]
		}
	}

	// Materialize complete viewer-aware projections only after the bounded
	// identity selection. This mirrors Postgres, where LIMIT precedes the
	// projection reads, and keeps memory use proportional to the requested
	// page rather than the total number of matching skills.
	matched := make([]EffectiveSkillRecord, 0, len(selected))
	for _, candidate := range selected {
		projection, err := s.effectiveSkillLocked(candidate.id, opts.CallerSubject, false)
		if err == nil {
			matched = append(matched, *projection)
		}
	}
	return matched, nil
}

// effectiveSkillCardCreatedAtLocked selects only the sort key needed for a
// bounded session list. It deliberately avoids cloning complete revision
// snapshots before the caller has applied Limit.
func (s *MemoryStore) effectiveSkillCardCreatedAtLocked(
	skill SkillRecord,
	callerSubject string,
) (time.Time, bool) {
	var publicRevision *SkillRevisionRecord
	if skill.ExplicitLatestRevisionID != "" {
		revision, revisionExists := s.revisions[skill.ExplicitLatestRevisionID]
		visibility, visibilityExists := s.revisionVisibility[skill.ExplicitLatestRevisionID]
		if revisionExists && visibilityExists && revision.SkillID == skill.ID && visibility.IsPublic {
			publicRevision = &revision
		}
	} else {
		for revisionID, revision := range s.revisions {
			if revision.SkillID != skill.ID {
				continue
			}
			visibility, exists := s.revisionVisibility[revisionID]
			if !exists || !visibility.IsPublic {
				continue
			}
			if publicRevision == nil || revision.SequenceNumber > publicRevision.SequenceNumber ||
				(revision.SequenceNumber == publicRevision.SequenceNumber && revision.ID > publicRevision.ID) {
				candidate := revision
				publicRevision = &candidate
			}
		}
	}
	if publicRevision != nil {
		return publicRevision.CreatedAt, true
	}

	var privateRevision *SkillRevisionRecord
	for revisionID, revision := range s.revisions {
		if revision.SkillID != skill.ID || revision.CreatorSubject != callerSubject {
			continue
		}
		visibility, exists := s.revisionVisibility[revisionID]
		if !exists || visibility.IsPublic {
			continue
		}
		if privateRevision == nil || revision.SequenceNumber > privateRevision.SequenceNumber ||
			(revision.SequenceNumber == privateRevision.SequenceNumber && revision.ID > privateRevision.ID) {
			candidate := revision
			privateRevision = &candidate
		}
	}
	if privateRevision == nil {
		return time.Time{}, false
	}
	return privateRevision.CreatedAt, true
}

func (s *MemoryStore) effectiveSkillLocked(
	skillID string,
	callerSubject string,
	includeOwnedEmpty bool,
) (*EffectiveSkillRecord, error) {
	skill, exists := s.skills[skillID]
	if !exists {
		return nil, ErrSkillNotFound
	}

	effective := s.resolveEffectiveRevisionLocked(skill)
	newestPrivate := s.newestPrivateRevisionLocked(skill.ID, callerSubject)
	card := effective
	if card == nil {
		card = newestPrivate
	}
	if card == nil && (!includeOwnedEmpty || skill.CreatedBySubject != callerSubject) {
		return nil, ErrSkillNotFound
	}

	identity := effectiveSkillIdentity(skill, callerSubject, card)
	projection := &EffectiveSkillRecord{
		Skill: identity, EffectiveRevision: effective,
		NewestPrivateRevision: newestPrivate, CardRevision: card,
	}
	if effective != nil && newestPrivate != nil {
		projection.HasNewerPrivateRevision =
			newestPrivate.Revision.SequenceNumber > effective.Revision.SequenceNumber
	}
	return projection, nil
}

func (s *MemoryStore) resolveEffectiveRevisionLocked(skill SkillRecord) *AccessibleRevisionRecord {
	if skill.ExplicitLatestRevisionID != "" {
		revision, revisionExists := s.revisions[skill.ExplicitLatestRevisionID]
		visibility, visibilityExists := s.revisionVisibility[skill.ExplicitLatestRevisionID]
		if revisionExists && visibilityExists && revision.SkillID == skill.ID && visibility.IsPublic {
			return cloneAccessibleRevision(revision, visibility, true)
		}
		// A stored explicit pointer suppresses fallback. Normal mutations and the
		// migration maintain the same-skill/public invariant; returning a fallback
		// for corrupt metadata would silently ignore the explicit choice.
		return nil
	}

	var selected *SkillRevisionRecord
	var selectedVisibility RevisionVisibilityRecord
	for revisionID, revision := range s.revisions {
		if revision.SkillID != skill.ID {
			continue
		}
		visibility, exists := s.revisionVisibility[revisionID]
		if !exists || !visibility.IsPublic {
			continue
		}
		if selected == nil || revision.SequenceNumber > selected.SequenceNumber ||
			(revision.SequenceNumber == selected.SequenceNumber && revision.ID > selected.ID) {
			candidate := revision
			selected = &candidate
			selectedVisibility = visibility
		}
	}
	if selected == nil {
		return nil
	}
	return cloneAccessibleRevision(*selected, selectedVisibility, false)
}

func (s *MemoryStore) newestPrivateRevisionLocked(skillID, callerSubject string) *AccessibleRevisionRecord {
	var selected *SkillRevisionRecord
	var selectedVisibility RevisionVisibilityRecord
	for revisionID, revision := range s.revisions {
		if revision.SkillID != skillID || revision.CreatorSubject != callerSubject {
			continue
		}
		visibility, exists := s.revisionVisibility[revisionID]
		if !exists || visibility.IsPublic {
			continue
		}
		if selected == nil || revision.SequenceNumber > selected.SequenceNumber ||
			(revision.SequenceNumber == selected.SequenceNumber && revision.ID > selected.ID) {
			candidate := revision
			selected = &candidate
			selectedVisibility = visibility
		}
	}
	if selected == nil {
		return nil
	}
	return cloneAccessibleRevision(*selected, selectedVisibility, false)
}

func (s *MemoryStore) skillIsAccessibleLocked(skill SkillRecord, callerSubject string) bool {
	if skill.CreatedBySubject == callerSubject || s.resolveEffectiveRevisionLocked(skill) != nil {
		return true
	}
	return s.newestPrivateRevisionLocked(skill.ID, callerSubject) != nil
}

func (s *MemoryStore) skillHasAccessibleSessionLocked(skillID, callerSubject, sessionID string) bool {
	for revisionID, revision := range s.revisions {
		if revision.SkillID != skillID || !slices.Contains(revision.Snapshot.SourceSessionIDs, sessionID) {
			continue
		}
		visibility, exists := s.revisionVisibility[revisionID]
		if exists && (visibility.IsPublic || revision.CreatorSubject == callerSubject) {
			return true
		}
	}
	return false
}

func effectiveSkillIdentity(
	skill SkillRecord,
	callerSubject string,
	card *AccessibleRevisionRecord,
) SkillRecord {
	identity := *skillIdentityRecord(skill, callerSubject)
	identity.DownloadCount = skill.DownloadCount
	// The allocator includes other creators' private appends. It is internal
	// state, not viewer-safe projection metadata.
	identity.NextSequenceNumber = 0
	if card != nil {
		identity.UpdatedAt = card.Revision.CreatedAt
	}
	return identity
}

func matchesEffectiveRevisionQuery(revision *AccessibleRevisionRecord, query string) bool {
	if revision == nil || query == "" {
		return revision != nil
	}
	needle := strings.ToLower(query)
	if strings.Contains(strings.ToLower(revision.Revision.Snapshot.Name), needle) ||
		strings.Contains(strings.ToLower(revision.Revision.Snapshot.Description), needle) {
		return true
	}
	for _, tag := range revision.Revision.Snapshot.Tags {
		if strings.Contains(strings.ToLower(tag), needle) {
			return true
		}
	}
	return false
}
