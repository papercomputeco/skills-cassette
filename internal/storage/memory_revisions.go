package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ResolveSkill normalizes a slug and atomically resolves it to one stable
// identity. The conflict response contains identity metadata only, so resolving
// a slug cannot disclose another creator's private revision data.
func (s *MemoryStore) ResolveSkill(_ context.Context, input ResolveSkillInput) (*SkillRecord, error) {
	if !validUUID(input.ID) {
		return nil, errors.New("resolve skill: id is required")
	}
	slug := normalizeSkillSlug(input.Slug)
	if slug == "" {
		return nil, errors.New("resolve skill: slug is required")
	}
	if input.CreatorSubject == "" {
		return nil, errors.New("resolve skill: creator subject is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for id, record := range s.skills {
		if normalizeSkillSlug(record.Slug) != slug {
			continue
		}
		if record.NextSequenceNumber < 1 {
			record.NextSequenceNumber = s.nextRevisionSequenceLocked(record.ID)
			s.skills[id] = record
		}
		return skillIdentityRecord(record, input.CreatorSubject), nil
	}
	if _, exists := s.skills[input.ID]; exists {
		return nil, errors.New("resolve skill: id already exists")
	}

	createdAt := input.CreatedAt.UTC()
	record := SkillRecord{
		ID: input.ID, Slug: slug, NextSequenceNumber: 1,
		CreatedBySubject: input.CreatorSubject,
		// The predecessor row keeps its creator attribution while old handlers
		// remain compiled, but the identity response below does not expose it.
		AuthorSubject: input.CreatorSubject,
		CreatedAt:     createdAt,
		UpdatedAt:     createdAt,
	}
	s.skills[record.ID] = record
	return skillIdentityRecord(record, input.CreatorSubject), nil
}

// AppendRevision atomically validates accessible lineage and appends one
// creator-private immutable revision under the memory-store mutex.
func (s *MemoryStore) AppendRevision(_ context.Context, input AppendRevisionInput) (*SkillRevisionRecord, error) {
	if err := validateAppendRevisionInput(input); err != nil {
		return nil, err
	}
	var err error
	input.Snapshot, err = normalizeAppendRevisionSnapshot(input.Origin, input.Snapshot)
	if err != nil {
		return nil, fmt.Errorf("append revision: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	skill, exists := s.skills[input.SkillID]
	if !exists {
		return nil, ErrSkillNotFound
	}

	existing, err := s.findAppendIdentityLocked(input)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if !revisionMatchesAppend(*existing, input) {
			return nil, ErrRevisionConflict
		}
		return cloneSkillRevision(existing), nil
	}

	if input.BasedOnRevisionID != "" && !s.revisionIsAccessibleLineageLocked(
		input.BasedOnRevisionID, input.SkillID, input.CreatorSubject, true,
	) {
		return nil, ErrRevisionLineageInvalid
	}
	if input.SourceRevisionID != "" && !s.revisionIsAccessibleLineageLocked(
		input.SourceRevisionID, input.SkillID, input.CreatorSubject, false,
	) {
		return nil, ErrRevisionLineageInvalid
	}
	if input.GenerationID != "" {
		if _, exists := s.generations[input.GenerationID]; !exists {
			return nil, ErrRevisionLineageInvalid
		}
	}

	sequence := skill.NextSequenceNumber
	if sequence < 1 {
		sequence = s.nextRevisionSequenceLocked(skill.ID)
	}
	createdAt := input.CreatedAt.UTC()
	record := SkillRevisionRecord{
		ID: input.ID, SkillID: input.SkillID, SequenceNumber: sequence,
		Version: fmt.Sprintf("%d", sequence), CreatorSubject: input.CreatorSubject,
		BasedOnRevisionID: input.BasedOnRevisionID, SourceRevisionID: input.SourceRevisionID,
		Origin: input.Origin, Snapshot: canonicalSkillRevisionSnapshot(input.Snapshot),
		ContentSHA256: skillRevisionSnapshotSHA256(input.Snapshot), ChangeNote: input.ChangeNote,
		GenerationID: input.GenerationID, IdempotencyKey: input.IdempotencyKey,
		LegacyReference: input.LegacyReference, CreatedAt: createdAt,
	}
	visibility := RevisionVisibilityRecord{
		RevisionID: record.ID, IsPublic: false,
		ChangedBySubject: record.CreatorSubject, ChangedAt: createdAt,
	}

	// The revision, visibility metadata, and sequence allocation become visible
	// together while this lock is held. Failed validation above changes none of
	// them, so it cannot reserve a sequence.
	s.revisions[record.ID] = record
	s.revisionVisibility[record.ID] = visibility
	skill.NextSequenceNumber = sequence + 1
	if skill.UpdatedAt.Before(createdAt) {
		skill.UpdatedAt = createdAt
	}
	s.skills[skill.ID] = skill

	return cloneSkillRevision(&record), nil
}

// GetRevision returns an exact revision only when it belongs to the requested
// skill and is public or owned by the caller. Every inaccessible condition uses
// ErrRevisionNotFound so private existence cannot be inferred.
func (s *MemoryStore) GetRevision(_ context.Context, opts RevisionReadOpts) (*AccessibleRevisionRecord, error) {
	if !validUUID(opts.SkillID) || !validUUID(opts.RevisionID) {
		return nil, ErrRevisionNotFound
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, exists := s.revisions[opts.RevisionID]
	if !exists || record.SkillID != opts.SkillID {
		return nil, ErrRevisionNotFound
	}
	visibility, exists := s.revisionVisibility[record.ID]
	if !exists || (!visibility.IsPublic && record.CreatorSubject != opts.CallerSubject) {
		return nil, ErrRevisionNotFound
	}
	skill := s.skills[record.SkillID]
	return cloneAccessibleRevision(record, visibility, skill.ExplicitLatestRevisionID == record.ID), nil
}

// ListRevisions returns newest-first public and caller-owned revisions for one
// skill using sequence/UUID keyset pagination.
func (s *MemoryStore) ListRevisions(_ context.Context, opts RevisionListOpts) ([]AccessibleRevisionRecord, error) {
	limit, err := revisionListLimit(opts)
	if err != nil {
		return nil, err
	}
	if !validUUID(opts.SkillID) {
		return []AccessibleRevisionRecord{}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]AccessibleRevisionRecord, 0, limit)
	for _, record := range s.revisions {
		if record.SkillID != opts.SkillID {
			continue
		}
		visibility, exists := s.revisionVisibility[record.ID]
		if !exists || (!visibility.IsPublic && record.CreatorSubject != opts.CallerSubject) {
			continue
		}
		if !revisionFollowsCursor(record, opts) {
			continue
		}
		// Clone only after the access predicate has passed. This both preserves
		// immutable memory state and mirrors the Postgres query boundary.
		skill := s.skills[record.SkillID]
		out = append(out, *cloneAccessibleRevision(
			record, visibility, skill.ExplicitLatestRevisionID == record.ID,
		))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Revision.SequenceNumber != out[j].Revision.SequenceNumber {
			return out[i].Revision.SequenceNumber > out[j].Revision.SequenceNumber
		}
		return out[i].Revision.ID > out[j].Revision.ID
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func validateAppendRevisionInput(input AppendRevisionInput) error {
	if !validUUID(input.ID) {
		return errors.New("append revision: id is required")
	}
	if !validUUID(input.SkillID) {
		return ErrSkillNotFound
	}
	if input.CreatorSubject == "" {
		return errors.New("append revision: creator subject is required")
	}
	if input.BasedOnRevisionID != "" && !validUUID(input.BasedOnRevisionID) {
		return ErrRevisionLineageInvalid
	}
	if input.SourceRevisionID != "" && !validUUID(input.SourceRevisionID) {
		return ErrRevisionLineageInvalid
	}
	if input.GenerationID != "" && !validUUID(input.GenerationID) {
		return ErrRevisionLineageInvalid
	}
	switch input.Origin {
	case RevisionOriginManual, RevisionOriginGeneration, RevisionOriginDuplicate, RevisionOriginMigrated:
	default:
		return errors.New("append revision: invalid origin")
	}
	return nil
}

func revisionListLimit(opts RevisionListOpts) (int, error) {
	if (opts.CursorSequenceNumber == nil) != (opts.CursorRevisionID == "") {
		return 0, errors.New("list revisions: cursor sequence and revision id are required together")
	}
	if opts.CursorRevisionID != "" && !validUUID(opts.CursorRevisionID) {
		return 0, errors.New("list revisions: invalid cursor revision id")
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultRevisionListLimit
	}
	if limit > maxRevisionListStorageLimit {
		limit = maxRevisionListStorageLimit
	}
	return limit, nil
}

func revisionFollowsCursor(record SkillRevisionRecord, opts RevisionListOpts) bool {
	if opts.CursorSequenceNumber == nil {
		return true
	}
	if record.SequenceNumber != *opts.CursorSequenceNumber {
		return record.SequenceNumber < *opts.CursorSequenceNumber
	}
	return record.ID < opts.CursorRevisionID
}

func (s *MemoryStore) findAppendIdentityLocked(input AppendRevisionInput) (*SkillRevisionRecord, error) {
	foundID := ""
	accessible := false
	for id, record := range s.revisions {
		matches := record.ID == input.ID ||
			(input.IdempotencyKey != "" && record.SkillID == input.SkillID && record.IdempotencyKey == input.IdempotencyKey) ||
			(input.GenerationID != "" && record.GenerationID == input.GenerationID) ||
			(input.LegacyReference != "" && record.LegacyReference == input.LegacyReference)
		if !matches {
			continue
		}
		if foundID != "" && foundID != id {
			return nil, ErrRevisionConflict
		}
		foundID = id
		visibility, visible := s.revisionVisibility[id]
		accessible = visible && (visibility.IsPublic || record.CreatorSubject == input.CreatorSubject)
	}
	if foundID == "" {
		return nil, nil
	}
	if !accessible {
		return nil, ErrRevisionConflict
	}
	record := s.revisions[foundID]
	return cloneSkillRevision(&record), nil
}

func (s *MemoryStore) revisionIsAccessibleLineageLocked(
	revisionID string,
	targetSkillID string,
	callerSubject string,
	sameSkill bool,
) bool {
	record, exists := s.revisions[revisionID]
	if !exists || (record.SkillID == targetSkillID) != sameSkill {
		return false
	}
	visibility, exists := s.revisionVisibility[revisionID]
	return exists && (visibility.IsPublic || record.CreatorSubject == callerSubject)
}

func (s *MemoryStore) nextRevisionSequenceLocked(skillID string) int {
	next := 1
	for _, revision := range s.revisions {
		if revision.SkillID == skillID && revision.SequenceNumber >= next {
			next = revision.SequenceNumber + 1
		}
	}
	return next
}

func skillIdentityRecord(record SkillRecord, callerSubject string) *SkillRecord {
	identity := &SkillRecord{
		ID: record.ID, Slug: record.Slug,
		ExplicitLatestRevisionID: record.ExplicitLatestRevisionID,
		NextSequenceNumber:       record.NextSequenceNumber,
		CreatedAt:                record.CreatedAt,
		UpdatedAt:                record.UpdatedAt,
	}
	if record.CreatedBySubject == callerSubject {
		identity.CreatedBySubject = record.CreatedBySubject
	}
	return identity
}

func cloneSkillRevision(record *SkillRevisionRecord) *SkillRevisionRecord {
	if record == nil {
		return nil
	}
	clone := *record
	clone.Snapshot = canonicalSkillRevisionSnapshot(record.Snapshot)
	return &clone
}

func cloneRevisionVisibility(record RevisionVisibilityRecord) *RevisionVisibilityRecord {
	clone := record
	return &clone
}

func cloneAccessibleRevision(
	record SkillRevisionRecord,
	visibility RevisionVisibilityRecord,
	isExplicitLatest bool,
) *AccessibleRevisionRecord {
	return &AccessibleRevisionRecord{
		Revision:         *cloneSkillRevision(&record),
		Visibility:       visibility,
		IsExplicitLatest: isExplicitLatest,
	}
}
