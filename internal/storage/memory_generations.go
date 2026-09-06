package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CreateGeneration atomically persists one skill/revision-anchored generation
// and its ordered source rows.
func (s *MemoryStore) CreateGeneration(_ context.Context, input CreateGenerationInput) (*SkillGenerationRecord, error) {
	if err := validateSkillGenerationInput(input); err != nil {
		return nil, err
	}
	input.AuthorContext = strings.TrimSpace(input.AuthorContext)
	input.SelectedSessionIDs, _ = normalizedSnapshotIdentities(
		input.SelectedSessionIDs, MaxRevisionSourceSessionIDs, "selected session ids",
	)
	var err error
	input.Snapshot, err = normalizeSkillRevisionSnapshot(input.Snapshot)
	if err != nil {
		return nil, fmt.Errorf("create generation: %w", err)
	}

	input.EvaluationCriteria = cloneRawJSON(input.EvaluationCriteria)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.skills[input.SkillID]; !exists {
		return nil, ErrSkillNotFound
	}
	if input.BaseRevisionID != "" {
		base, exists := s.revisions[input.BaseRevisionID]
		visibility, visible := s.revisionVisibility[input.BaseRevisionID]
		if !exists || !visible || base.SkillID != input.SkillID ||
			(!visibility.IsPublic && base.CreatorSubject != input.CreatorSubject) {
			return nil, ErrRevisionNotFound
		}
	}
	if existing, exists := s.generations[input.ID]; exists {
		if !generationMatchesCreateInput(existing, input) {
			return nil, errors.New("create generation: id already exists")
		}
		out := cloneGenerationRecord(existing)
		return &out, nil
	}

	// Creation timestamps and initial due time come from one authoritative
	// storage-clock sample. Caller timestamps are intentionally ignored.
	createdAt := s.now().UTC()
	generation := SkillGenerationRecord{
		ID: input.ID, SkillID: input.SkillID, BaseRevisionID: input.BaseRevisionID,
		CreatorSubject: input.CreatorSubject, Status: GenerationStatusQueued,
		Snapshot:      canonicalSkillRevisionSnapshot(input.Snapshot),
		AuthorContext: input.AuthorContext, SelectedSessionIDs: cloneStrings(input.SelectedSessionIDs),
		EvaluatorProfile: input.EvaluatorProfile, EvaluatorProfileVersion: input.EvaluatorProfileVersion,
		EvaluationCriteria: cloneRawJSON(input.EvaluationCriteria), NextAttemptAt: createdAt,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	s.generations[generation.ID] = generation
	s.generationSessions[generation.ID] = pendingGenerationSessions(generation.ID, input.SelectedSessionIDs, createdAt)
	out := cloneGenerationRecord(generation)
	return &out, nil
}

// ListSkillGenerations returns one bounded page of newest-first summary rows.
// The bounded insertion keeps temporary memory proportional to the requested
// page rather than the store's complete retained history.
func (s *MemoryStore) ListSkillGenerations(_ context.Context, opts SkillGenerationListOpts) (*SkillGenerationListPage, error) {
	limit, err := normalizeSkillGenerationListOpts(opts)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.skills[opts.SkillID]; !exists {
		return nil, ErrSkillNotFound
	}

	summaries := make([]SkillGenerationSummaryRecord, 0, limit+1)
	for _, generation := range s.generations {
		if generation.SkillID != opts.SkillID ||
			!generationVisibleToCallerLocked(s, generation, opts.CallerSubject) ||
			!generationIsAfterListCursor(generation, opts) {
			continue
		}
		summaries = append(summaries, generationSummary(generation))
		sortGenerationSummaries(summaries)
		if len(summaries) > limit+1 {
			summaries = summaries[:limit+1]
		}
	}
	return generationListPage(summaries, limit), nil
}

// GetSkillGeneration authorizes the generation before any linked artifacts
// are copied. Creators see all states; organization members see only completed
// history whose current result revision is public.
func (s *MemoryStore) GetSkillGeneration(_ context.Context, callerSubject, skillID, generationID string) (*GenerationState, error) {
	if !validUUID(skillID) || !validUUID(generationID) || callerSubject == "" {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	generation, ok := s.generations[generationID]
	if !ok || generation.SkillID != skillID ||
		!generationVisibleToCallerLocked(s, generation, callerSubject) {
		return nil, nil
	}
	state := s.cloneGenerationState(generation)
	return &state, nil
}

// GetGenerationByID applies the same storage authorization without a history
// scan or caller-provided skill id.
func (s *MemoryStore) GetGenerationByID(_ context.Context, callerSubject, generationID string) (*GenerationState, error) {
	if !validUUID(generationID) || callerSubject == "" {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	generation, ok := s.generations[generationID]
	if !ok || !generationVisibleToCallerLocked(s, generation, callerSubject) {
		return nil, nil
	}
	state := s.cloneGenerationState(generation)
	return &state, nil
}

// ClaimGeneration claims one eligible queue row and increments its durable
// attempt count. Multiple rows for one skill remain independently claimable.
func (s *MemoryStore) ClaimGeneration(_ context.Context, input ClaimGenerationInput) (*SkillGenerationRecord, error) {
	if input.WorkerID == "" || input.LeaseDuration <= 0 {
		return nil, errors.New("claim generation: worker and positive lease duration are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	ids := make([]string, 0)
	for id, generation := range s.generations {
		if !generationIsExecutable(generation.Status) || generation.NextAttemptAt.After(now) {
			continue
		}
		if generation.LeaseExpiresAt != nil && generation.LeaseExpiresAt.After(now) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left, right := s.generations[ids[i]], s.generations[ids[j]]
		if !left.NextAttemptAt.Equal(right.NextAttemptAt) {
			return left.NextAttemptAt.Before(right.NextAttemptAt)
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.ID < right.ID
	})
	if len(ids) == 0 {
		return nil, nil
	}
	generation := s.generations[ids[0]]
	leaseExpiresAt := now.Add(input.LeaseDuration)
	generation.ClaimToken = uuid.NewString()
	generation.ClaimOwner = input.WorkerID
	generation.ErrorCode = ""
	generation.ErrorMessage = ""
	generation.LeaseExpiresAt = cloneTime(&leaseExpiresAt)
	generation.LastHeartbeatAt = cloneTime(&now)
	generation.AttemptCount++
	generation.UpdatedAt = now
	if generation.StartedAt == nil {
		generation.StartedAt = cloneTime(&now)
	}
	s.generations[generation.ID] = generation
	out := cloneGenerationRecord(generation)
	return &out, nil
}

func (s *MemoryStore) RenewGenerationLease(_ context.Context, generationID, claimToken string, leaseDuration time.Duration) (bool, error) {
	if leaseDuration <= 0 {
		return false, errors.New("renew generation lease: positive lease duration is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	generation, ok := s.generations[generationID]
	if ok && generation.Status == GenerationStatusCompleted &&
		s.generationResultClaimToken[generationID] == claimToken {
		return false, ErrGenerationCompleted
	}
	if claimErr := memoryClaimError(generation, ok, claimToken, now); claimErr != nil {
		if errors.Is(claimErr, ErrGenerationCanceled) {
			return false, claimErr
		}
		return false, nil
	}
	leaseExpiresAt := now.Add(leaseDuration)
	generation.LeaseExpiresAt = cloneTime(&leaseExpiresAt)
	generation.LastHeartbeatAt = cloneTime(&now)
	generation.UpdatedAt = now
	s.generations[generationID] = generation
	return true, nil
}

// FailGeneration atomically persists a curated terminal failure under a live
// claim. The lock and one storage-clock sample fence cancellation, expiry, and
// reclaim against the terminal transition.
func (s *MemoryStore) FailGeneration(_ context.Context, input FailGenerationInput) (*SkillGenerationRecord, error) {
	if err := validateGenerationFailure(input.Failure); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	generation, ok := s.generations[input.GenerationID]
	if claimErr := memoryClaimError(generation, ok, input.ClaimToken, now); claimErr != nil {
		return nil, claimErr
	}
	generation.Status = GenerationStatusFailed
	generation.ErrorCode = input.Failure.Code
	generation.ErrorMessage = input.Failure.Message
	generation.UpdatedAt = now
	generation.CompletedAt = cloneTime(&now)
	clearGenerationClaim(&generation)
	s.generations[generation.ID] = generation
	out := cloneGenerationRecord(generation)
	return &out, nil
}

// RequeueGeneration atomically persists a curated retry summary and releases a
// live claim. Status is left at its resumable stage and storage time determines
// both UpdatedAt and the next claim due time.
func (s *MemoryStore) RequeueGeneration(_ context.Context, input RequeueGenerationInput) (*SkillGenerationRecord, error) {
	if input.RetryAfter <= 0 {
		return nil, errors.New("requeue generation: positive retry delay is required")
	}
	if err := validateGenerationFailure(input.Failure); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	generation, ok := s.generations[input.GenerationID]
	if claimErr := memoryClaimError(generation, ok, input.ClaimToken, now); claimErr != nil {
		return nil, claimErr
	}
	generation.ErrorCode = input.Failure.Code
	generation.ErrorMessage = input.Failure.Message
	generation.NextAttemptAt = now.Add(input.RetryAfter)
	generation.UpdatedAt = now
	clearGenerationClaim(&generation)
	s.generations[generation.ID] = generation
	out := cloneGenerationRecord(generation)
	return &out, nil
}

// GenerationQueueStats samples the storage clock once and applies exactly the
// same due/status/lease predicate as ClaimGeneration.
func (s *MemoryStore) GenerationQueueStats(_ context.Context) (GenerationQueueStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	var (
		stats     GenerationQueueStats
		oldestDue time.Time
	)
	for _, generation := range s.generations {
		if !generationIsExecutable(generation.Status) || generation.NextAttemptAt.After(now) ||
			(generation.LeaseExpiresAt != nil && generation.LeaseExpiresAt.After(now)) {
			continue
		}
		stats.Depth++
		if oldestDue.IsZero() || generation.NextAttemptAt.Before(oldestDue) {
			oldestDue = generation.NextAttemptAt
		}
	}
	if stats.Depth > 0 && oldestDue.Before(now) {
		stats.Lag = now.Sub(oldestDue)
	}
	return stats, nil
}

func (s *MemoryStore) GetClaimedGeneration(_ context.Context, generationID, claimToken string) (*GenerationState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	generation, ok := s.generations[generationID]
	if claimErr := memoryClaimError(generation, ok, claimToken, s.now().UTC()); claimErr != nil {
		return nil, claimErr
	}
	state := s.cloneGenerationState(generation)
	return &state, nil
}

func (s *MemoryStore) UpdateGenerationStatus(_ context.Context, generationID, claimToken string, from, to GenerationStatus) error {
	if !validGenerationStatusTransition(from, to) {
		return ErrInvalidGenerationState
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	generation, ok := s.generations[generationID]
	if claimErr := memoryClaimError(generation, ok, claimToken, now); claimErr != nil {
		return claimErr
	}
	if generation.Status != from {
		return ErrInvalidGenerationState
	}
	generation.Status = to
	generation.UpdatedAt = now
	s.generations[generationID] = generation
	return nil
}

func (s *MemoryStore) UpdateGenerationSession(_ context.Context, generationID, claimToken string, session GenerationSessionRecord) error {
	if err := validateGenerationArtifactOwner(session.GenerationID, generationID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	generation, ok := s.generations[generationID]
	if claimErr := memoryClaimError(generation, ok, claimToken, now); claimErr != nil {
		return claimErr
	}
	sessions := s.generationSessions[generationID]
	for i := range sessions {
		if sessions[i].SessionID != session.SessionID {
			continue
		}
		session.GenerationID = generationID
		session.Ordinal = sessions[i].Ordinal
		session.UpdatedAt = now
		if err := validateGenerationSessionArtifact(session); err != nil {
			return err
		}
		if session.CandidateID != "" {
			candidate, exists := s.generationCandidateByIDLocked(generationID, session.CandidateID)
			if !exists || candidate.Kind != GenerationCandidateSession || candidate.Ordinal != session.Ordinal ||
				len(candidate.SourceSessionIDs) != 1 || candidate.SourceSessionIDs[0] != session.SessionID {
				return errors.New("generation session candidate not found")
			}
		}
		sessions[i] = cloneGenerationSession(session)
		s.generationSessions[generationID] = sessions
		return nil
	}
	return errors.New("generation session not found")
}

func (s *MemoryStore) PutGenerationCandidate(_ context.Context, generationID, claimToken string, candidate GenerationCandidateRecord) (*GenerationCandidateRecord, error) {
	if err := validateGenerationArtifactOwner(candidate.GenerationID, generationID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	generation, ok := s.generations[generationID]
	if claimErr := memoryClaimError(generation, ok, claimToken, now); claimErr != nil {
		return nil, claimErr
	}
	var err error
	candidate, err = normalizeGenerationCandidateArtifact(candidate, false)
	if err != nil {
		return nil, err
	}
	candidate.GenerationID = generationID
	for storedGenerationID, candidates := range s.generationCandidates {
		for _, existing := range candidates {
			if existing.ID != candidate.ID && (storedGenerationID != generationID || existing.Ordinal != candidate.Ordinal) {
				continue
			}
			if storedGenerationID == generationID && sameGenerationCandidateArtifact(existing, candidate) {
				out := cloneGenerationCandidate(existing)
				return &out, nil
			}
			return nil, errors.New("store generation candidate: conflicting idempotent artifact")
		}
	}
	if err = validateGenerationCandidatePlacement(generation, s.generationSessions[generationID],
		s.generationCandidates[generationID], candidate); err != nil {
		return nil, err
	}
	candidate.SourceSessionIDs = cloneStrings(candidate.SourceSessionIDs)
	candidate.Snapshot = canonicalGenerationCandidateSnapshot(candidate.Snapshot)
	candidate.Insights = cloneRawJSON(candidate.Insights)
	candidate.CreatedAt = now
	s.generationCandidates[generationID] = append(s.generationCandidates[generationID], candidate)
	out := cloneGenerationCandidate(candidate)
	return &out, nil
}

func (s *MemoryStore) PutCandidateEvaluation(_ context.Context, generationID, claimToken string, evaluation CandidateEvaluationRecord) (*CandidateEvaluationRecord, error) {
	if err := validateGenerationArtifactOwner(evaluation.GenerationID, generationID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	generation, ok := s.generations[generationID]
	if claimErr := memoryClaimError(generation, ok, claimToken, now); claimErr != nil {
		return nil, claimErr
	}
	var err error
	evaluation, err = canonicalCandidateEvaluationDetails(evaluation)
	if err != nil {
		return nil, fmt.Errorf("store candidate evaluation: %w", err)
	}
	if evaluation.Profile == "" || evaluation.ProfileVersion == "" ||
		evaluation.Profile != generation.EvaluatorProfile ||
		evaluation.ProfileVersion != generation.EvaluatorProfileVersion {
		return nil, ErrInvalidGenerationState
	}
	if !s.generationHasCandidateLocked(generationID, evaluation.CandidateID) {
		return nil, errors.New("store candidate evaluation: candidate not found")
	}
	for storedGenerationID, evaluations := range s.candidateEvaluations {
		for _, existing := range evaluations {
			matches := existing.ID == evaluation.ID ||
				(existing.CandidateID == evaluation.CandidateID && existing.RequestSHA256 == evaluation.RequestSHA256)
			if !matches {
				continue
			}
			evaluation.GenerationID = generationID
			if storedGenerationID != generationID || (existing.ID == evaluation.ID &&
				(existing.CandidateID != evaluation.CandidateID || existing.RequestSHA256 != evaluation.RequestSHA256)) {
				return nil, errors.New("store candidate evaluation: conflicting idempotent artifact")
			}
			if existing.Profile != generation.EvaluatorProfile ||
				existing.ProfileVersion != generation.EvaluatorProfileVersion ||
				existing.Profile == "" || existing.ProfileVersion == "" {
				return nil, ErrInvalidGenerationState
			}
			out := cloneCandidateEvaluation(existing)
			return &out, nil
		}
	}
	evaluation.GenerationID = generationID
	evaluation.CreatedAt = now
	evaluation.CriterionResults = cloneRawJSON(evaluation.CriterionResults)
	evaluation.Findings = cloneRawJSON(evaluation.Findings)
	evaluation.Strengths = cloneRawJSON(evaluation.Strengths)
	s.candidateEvaluations[generationID] = append(s.candidateEvaluations[generationID], evaluation)
	out := cloneCandidateEvaluation(evaluation)
	return &out, nil
}

func (s *MemoryStore) PutGenerationDiagnostic(_ context.Context, generationID, claimToken string, diagnostic GenerationDiagnosticRecord) (*GenerationDiagnosticRecord, error) {
	if err := validateGenerationArtifactOwner(diagnostic.GenerationID, generationID); err != nil {
		return nil, err
	}
	if err := validateGenerationDiagnostic(diagnostic); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	generation, ok := s.generations[generationID]
	if claimErr := memoryClaimError(generation, ok, claimToken, now); claimErr != nil {
		return nil, claimErr
	}
	if diagnostic.SessionID != "" && !s.generationHasSessionLocked(generationID, diagnostic.SessionID) {
		return nil, errors.New("store generation diagnostic: session not found")
	}
	if diagnostic.CandidateID != "" && !s.generationHasCandidateLocked(generationID, diagnostic.CandidateID) {
		return nil, errors.New("store generation diagnostic: candidate not found")
	}
	for storedGenerationID, diagnostics := range s.generationDiagnostics {
		for _, existing := range diagnostics {
			if existing.ID != diagnostic.ID {
				continue
			}
			if sameGenerationDiagnosticArtifact(existing, generationID, diagnostic) {
				out := existing
				return &out, nil
			}
			if storedGenerationID != generationID || generationDiagnosticSlot(existing) != generationDiagnosticSlot(diagnostic) {
				return nil, errors.New("store generation diagnostic: conflicting idempotent artifact")
			}
		}
	}

	// One generation lock serializes replacement of the bounded logical
	// source+stage slot. A retry updates the safe catalog outcome and timestamp
	// instead of accumulating another row for the same source stage.
	slot := generationDiagnosticSlot(diagnostic)
	retained := s.generationDiagnostics[generationID][:0]
	for _, existing := range s.generationDiagnostics[generationID] {
		if generationDiagnosticSlot(existing) != slot {
			retained = append(retained, existing)
		}
	}
	diagnostic.GenerationID = generationID
	diagnostic.CreatedAt = now
	retained = append(retained, diagnostic)
	s.generationDiagnostics[generationID] = retainNewestGenerationDiagnostics(retained)
	out := diagnostic
	return &out, nil
}

func (s *MemoryStore) CancelSkillGeneration(_ context.Context, callerSubject, skillID, generationID string) (*GenerationState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	canceledAt := s.now().UTC()
	generation, ok := s.generations[generationID]
	if !ok || generation.SkillID != skillID || generation.CreatorSubject != callerSubject {
		return nil, ErrGenerationNotFound
	}
	if !generationIsExecutable(generation.Status) {
		return nil, ErrInvalidGenerationState
	}
	generation.Status = GenerationStatusCanceled
	generation.ErrorCode = ""
	generation.ErrorMessage = ""
	generation.NextAttemptAt = canceledAt
	generation.UpdatedAt = canceledAt
	generation.CompletedAt = cloneTime(&canceledAt)
	clearGenerationClaim(&generation)
	s.generations[generationID] = generation
	state := s.cloneGenerationState(generation)
	return &state, nil
}

// AppendPrivateGenerationResult appends one immutable creator-private result
// and completes its generation while holding the same store lock. The store
// clock, rather than any caller timestamp, fences the claim and timestamps all
// committed rows. Completed retries validate immutable lineage only, so later
// visibility/latest changes cannot alter the returned revision.
func (s *MemoryStore) AppendPrivateGenerationResult(_ context.Context, input AppendPrivateGenerationResultInput) (*SkillRevisionRecord, error) {
	if !validUUID(input.GenerationID) || input.ClaimToken == "" {
		return nil, ErrGenerationClaimLost
	}
	if !validUUID(input.InitialWinnerCandidateID) || !validUUID(input.ResultCandidateID) {
		return nil, ErrInvalidGenerationState
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	generation, exists := s.generations[input.GenerationID]
	if !exists {
		return nil, ErrGenerationClaimLost
	}
	if generation.Status == GenerationStatusCompleted {
		if s.generationResultClaimToken[input.GenerationID] != input.ClaimToken {
			return nil, ErrGenerationClaimLost
		}
		if generation.WinnerCandidateID != input.InitialWinnerCandidateID ||
			generation.ResultCandidateID != input.ResultCandidateID || generation.ResultRevisionID == "" {
			return nil, ErrInvalidGenerationState
		}
		initialWinner, initialFound := s.generationCandidateByIDLocked(generation.ID, input.InitialWinnerCandidateID)
		resultCandidate, resultFound := s.generationCandidateByIDLocked(generation.ID, input.ResultCandidateID)
		if !initialFound || !resultFound || initialWinner.Kind == GenerationCandidateSynthesis ||
			!s.generationCandidateHasMatchingEvaluationLocked(generation, initialWinner.ID) ||
			!s.generationCandidateHasMatchingEvaluationLocked(generation, resultCandidate.ID) {
			return nil, errors.New("append generation result: matching evaluated candidate not found")
		}
		revision, revisionExists := s.revisions[generation.ResultRevisionID]
		_, visibilityExists := s.revisionVisibility[generation.ResultRevisionID]
		expected := AppendRevisionInput{
			ID: generationResultRevisionID(generation.ID), SkillID: generation.SkillID,
			CreatorSubject: generation.CreatorSubject, BasedOnRevisionID: generation.BaseRevisionID,
			Origin: RevisionOriginGeneration, Snapshot: generationCandidateRevisionSnapshot(resultCandidate),
			ChangeNote: generationResultChangeNote, GenerationID: generation.ID,
			IdempotencyKey: "generation:" + generation.ID,
		}
		if !revisionExists || !visibilityExists ||
			revision.ID != expected.ID || !revisionMatchesAppend(revision, expected) {
			return nil, ErrInvalidGenerationState
		}
		return cloneSkillRevision(&revision), nil
	}

	now := s.now().UTC()
	if claimErr := memoryClaimError(generation, true, input.ClaimToken, now); claimErr != nil {
		return nil, claimErr
	}
	if generation.Status != GenerationStatusSynthesizing {
		return nil, ErrInvalidGenerationState
	}
	initialWinner, initialFound := s.generationCandidateByIDLocked(generation.ID, input.InitialWinnerCandidateID)
	resultCandidate, resultFound := s.generationCandidateByIDLocked(generation.ID, input.ResultCandidateID)
	if !initialFound || !resultFound || initialWinner.Kind == GenerationCandidateSynthesis ||
		!s.generationCandidateHasMatchingEvaluationLocked(generation, initialWinner.ID) ||
		!s.generationCandidateHasMatchingEvaluationLocked(generation, resultCandidate.ID) {
		return nil, errors.New("append generation result: matching evaluated candidate not found")
	}

	skill, exists := s.skills[generation.SkillID]
	if !exists {
		return nil, ErrSkillNotFound
	}
	if generation.BaseRevisionID != "" {
		base, baseExists := s.revisions[generation.BaseRevisionID]
		visibility, visibilityExists := s.revisionVisibility[generation.BaseRevisionID]
		if !baseExists || !visibilityExists || base.SkillID != generation.SkillID ||
			(!visibility.IsPublic && base.CreatorSubject != generation.CreatorSubject) {
			return nil, ErrRevisionNotFound
		}
	}
	revisionID := generationResultRevisionID(generation.ID)
	for id, revision := range s.revisions {
		if id == revisionID || revision.GenerationID == generation.ID ||
			(revision.SkillID == generation.SkillID && revision.IdempotencyKey == "generation:"+generation.ID) {
			return nil, ErrRevisionConflict
		}
	}
	snapshot, err := normalizeSkillRevisionSnapshot(generationCandidateRevisionSnapshot(resultCandidate))
	if err != nil {
		return nil, fmt.Errorf("append generation result: %w", err)
	}
	sequence := skill.NextSequenceNumber
	if sequence < 1 {
		sequence = s.nextRevisionSequenceLocked(skill.ID)
	}
	revision := SkillRevisionRecord{
		ID: revisionID, SkillID: generation.SkillID, SequenceNumber: sequence,
		Version: fmt.Sprintf("%d", sequence), CreatorSubject: generation.CreatorSubject,
		BasedOnRevisionID: generation.BaseRevisionID, Origin: RevisionOriginGeneration,
		Snapshot: snapshot, ContentSHA256: skillRevisionSnapshotSHA256(snapshot),
		ChangeNote: generationResultChangeNote, GenerationID: generation.ID,
		IdempotencyKey: "generation:" + generation.ID, CreatedAt: now,
	}
	visibility := RevisionVisibilityRecord{
		RevisionID: revision.ID, IsPublic: false,
		ChangedBySubject: generation.CreatorSubject, ChangedAt: now,
	}

	s.revisions[revision.ID] = revision
	s.revisionVisibility[revision.ID] = visibility
	skill.NextSequenceNumber = sequence + 1
	if skill.UpdatedAt.Before(now) {
		skill.UpdatedAt = now
	}
	s.skills[skill.ID] = skill

	generation.Status = GenerationStatusCompleted
	generation.WinnerCandidateID = initialWinner.ID
	generation.ResultCandidateID = resultCandidate.ID
	generation.ResultRevisionID = revision.ID
	generation.UpdatedAt = now
	generation.CompletedAt = cloneTime(&now)
	clearGenerationClaim(&generation)
	s.generations[generation.ID] = generation
	s.generationResultClaimToken[generation.ID] = input.ClaimToken
	return cloneSkillRevision(&revision), nil
}

func (s *MemoryStore) generationCandidateByIDLocked(generationID, candidateID string) (GenerationCandidateRecord, bool) {
	for _, candidate := range s.generationCandidates[generationID] {
		if candidate.ID == candidateID && candidate.GenerationID == generationID {
			return candidate, true
		}
	}
	return GenerationCandidateRecord{}, false
}

func (s *MemoryStore) generationCandidateHasMatchingEvaluationLocked(generation SkillGenerationRecord, candidateID string) bool {
	found := false
	for _, evaluation := range s.candidateEvaluations[generation.ID] {
		if evaluation.GenerationID != generation.ID || evaluation.CandidateID != candidateID {
			continue
		}
		found = true
		if evaluation.Profile == "" || evaluation.ProfileVersion == "" ||
			evaluation.Profile != generation.EvaluatorProfile ||
			evaluation.ProfileVersion != generation.EvaluatorProfileVersion {
			return false
		}
	}
	return found
}

func validateSkillGenerationInput(input CreateGenerationInput) error {
	if !validUUID(input.ID) {
		return errors.New("create generation: id is required")
	}
	if !validUUID(input.SkillID) {
		return ErrSkillNotFound
	}
	if input.BaseRevisionID != "" && !validUUID(input.BaseRevisionID) {
		return ErrRevisionNotFound
	}
	if input.CreatorSubject == "" {
		return errors.New("create generation: creator subject is required")
	}
	authorContext := strings.TrimSpace(input.AuthorContext)
	if !validSnapshotText(input.AuthorContext, MaxGenerationAuthorContextRunes) ||
		!validSnapshotText(authorContext, MaxGenerationAuthorContextRunes) {
		return errors.New("create generation: author context is invalid or too long")
	}
	if input.EvaluatorProfile == "" || input.EvaluatorProfileVersion == "" {
		return errors.New("create generation: evaluator profile and version are required")
	}
	criteria, err := normalizedJSON(input.EvaluationCriteria, nil)
	if err != nil || len(criteria) == 0 {
		return errors.New("create generation: evaluation criteria are required")
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(criteria, &entries); err != nil || len(entries) == 0 {
		return errors.New("create generation: evaluation criteria must be a non-empty array")
	}
	if _, err := normalizedSnapshotIdentities(
		input.SelectedSessionIDs, MaxRevisionSourceSessionIDs, "selected session ids",
	); err != nil {
		return fmt.Errorf("create generation: %w", err)
	}
	return nil
}

func generationMatchesCreateInput(generation SkillGenerationRecord, input CreateGenerationInput) bool {
	return generation.SkillID == input.SkillID && generation.BaseRevisionID == input.BaseRevisionID &&
		generation.CreatorSubject == input.CreatorSubject && equalRevisionSnapshots(generation.Snapshot, input.Snapshot) &&
		generation.AuthorContext == input.AuthorContext && equalStrings(generation.SelectedSessionIDs, input.SelectedSessionIDs) &&
		generation.EvaluatorProfile == input.EvaluatorProfile &&
		generation.EvaluatorProfileVersion == input.EvaluatorProfileVersion &&
		jsonValuesEqual(generation.EvaluationCriteria, input.EvaluationCriteria)
}

func jsonValuesEqual(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

func pendingGenerationSessions(generationID string, selected []string, createdAt time.Time) []GenerationSessionRecord {
	sessions := make([]GenerationSessionRecord, len(selected))
	for ordinal, sessionID := range selected {
		sessions[ordinal] = GenerationSessionRecord{
			GenerationID: generationID, SessionID: sessionID, Ordinal: ordinal,
			Status: GenerationSessionPending, UpdatedAt: createdAt,
		}
	}
	return sessions
}

func normalizeSkillGenerationListOpts(opts SkillGenerationListOpts) (int, error) {
	if !validUUID(opts.SkillID) || opts.CallerSubject == "" {
		return 0, ErrSkillNotFound
	}
	if (opts.CursorCreatedAt == nil) != (opts.CursorID == "") {
		return 0, errors.New("list skill generations: cursor time and id are required together")
	}
	if opts.CursorID != "" && !validUUID(opts.CursorID) {
		return 0, errors.New("list skill generations: invalid cursor id")
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultGenerationListLimit
	}
	if limit > MaxGenerationListLimit {
		limit = MaxGenerationListLimit
	}
	return limit, nil
}

func generationIsAfterListCursor(generation SkillGenerationRecord, opts SkillGenerationListOpts) bool {
	if opts.CursorCreatedAt == nil {
		return true
	}
	cursorTime := opts.CursorCreatedAt.UTC()
	return generation.CreatedAt.Before(cursorTime) ||
		(generation.CreatedAt.Equal(cursorTime) && generation.ID < opts.CursorID)
}

func generationSummary(generation SkillGenerationRecord) SkillGenerationSummaryRecord {
	return SkillGenerationSummaryRecord{
		ID: generation.ID, SkillID: generation.SkillID,
		BaseRevisionID: generation.BaseRevisionID, Status: generation.Status,
		ResultRevisionID: generation.ResultRevisionID,
		ErrorCode:        generation.ErrorCode, ErrorMessage: generation.ErrorMessage,
		AttemptCount: generation.AttemptCount, CreatedAt: generation.CreatedAt,
		UpdatedAt: generation.UpdatedAt, StartedAt: cloneTime(generation.StartedAt),
		CompletedAt: cloneTime(generation.CompletedAt),
	}
}

func sortGenerationSummaries(generations []SkillGenerationSummaryRecord) {
	sort.Slice(generations, func(i, j int) bool {
		if !generations[i].CreatedAt.Equal(generations[j].CreatedAt) {
			return generations[i].CreatedAt.After(generations[j].CreatedAt)
		}
		return generations[i].ID > generations[j].ID
	})
}

func generationListPage(summaries []SkillGenerationSummaryRecord, limit int) *SkillGenerationListPage {
	page := &SkillGenerationListPage{Generations: summaries}
	if len(summaries) <= limit {
		return page
	}
	page.Generations = summaries[:limit]
	last := page.Generations[len(page.Generations)-1]
	page.NextCreatedAt = cloneTime(&last.CreatedAt)
	page.NextID = last.ID
	return page
}

func (s *MemoryStore) generationHasCandidateLocked(generationID, candidateID string) bool {
	for _, candidate := range s.generationCandidates[generationID] {
		if candidate.ID == candidateID {
			return true
		}
	}
	return false
}

func (s *MemoryStore) generationHasSessionLocked(generationID, sessionID string) bool {
	for _, session := range s.generationSessions[generationID] {
		if session.SessionID == sessionID {
			return true
		}
	}
	return false
}

func (s *MemoryStore) cloneGenerationState(generation SkillGenerationRecord) GenerationState {
	state := GenerationState{Generation: cloneGenerationRecord(generation)}
	storedSessions := s.generationSessions[generation.ID]
	state.Sessions = make([]GenerationSessionRecord, len(storedSessions))
	for i, session := range storedSessions {
		state.Sessions[i] = cloneGenerationSession(session)
	}
	sort.Slice(state.Sessions, func(i, j int) bool { return state.Sessions[i].Ordinal < state.Sessions[j].Ordinal })
	if len(state.Sessions) > maxGenerationStateSessions {
		state.Sessions = state.Sessions[:maxGenerationStateSessions]
	}

	storedCandidates := s.generationCandidates[generation.ID]
	state.Candidates = make([]GenerationCandidateRecord, len(storedCandidates))
	for i, candidate := range storedCandidates {
		state.Candidates[i] = cloneGenerationCandidate(candidate)
	}
	sort.Slice(state.Candidates, func(i, j int) bool { return state.Candidates[i].Ordinal < state.Candidates[j].Ordinal })
	if len(state.Candidates) > maxGenerationStateCandidates {
		state.Candidates = state.Candidates[:maxGenerationStateCandidates]
	}

	storedEvaluations := s.candidateEvaluations[generation.ID]
	state.Evaluations = make([]CandidateEvaluationRecord, len(storedEvaluations))
	for i, evaluation := range storedEvaluations {
		state.Evaluations[i] = cloneCandidateEvaluation(evaluation)
	}
	sort.Slice(state.Evaluations, func(i, j int) bool {
		if !state.Evaluations[i].CreatedAt.Equal(state.Evaluations[j].CreatedAt) {
			return state.Evaluations[i].CreatedAt.Before(state.Evaluations[j].CreatedAt)
		}
		return state.Evaluations[i].ID < state.Evaluations[j].ID
	})
	if len(state.Evaluations) > maxGenerationStateEvaluations {
		state.Evaluations = state.Evaluations[:maxGenerationStateEvaluations]
	}

	state.Diagnostics = retainNewestGenerationDiagnostics(
		append([]GenerationDiagnosticRecord{}, s.generationDiagnostics[generation.ID]...),
	)
	sanitizeGenerationStateArtifacts(&state)
	return state
}

func generationVisibleToCallerLocked(store *MemoryStore, generation SkillGenerationRecord, callerSubject string) bool {
	if generation.CreatorSubject == callerSubject {
		return true
	}
	if generation.Status != GenerationStatusCompleted || generation.ResultRevisionID == "" {
		return false
	}
	revision, revisionExists := store.revisions[generation.ResultRevisionID]
	visibility, visibilityExists := store.revisionVisibility[generation.ResultRevisionID]
	return revisionExists && visibilityExists && visibility.IsPublic &&
		revision.SkillID == generation.SkillID && revision.GenerationID == generation.ID
}

func validMemoryClaim(generation SkillGenerationRecord, claimToken string, now time.Time) bool {
	return claimToken != "" && generation.ClaimToken == claimToken &&
		generation.LeaseExpiresAt != nil && generation.LeaseExpiresAt.After(now) &&
		generationIsExecutable(generation.Status)
}

func memoryClaimError(generation SkillGenerationRecord, exists bool, claimToken string, now time.Time) error {
	if exists && generation.Status == GenerationStatusCanceled {
		return ErrGenerationCanceled
	}
	if !exists || !validMemoryClaim(generation, claimToken, now) {
		return ErrGenerationClaimLost
	}
	return nil
}

func validGenerationStatusTransition(from, to GenerationStatus) bool {
	return (from == GenerationStatusQueued && to == GenerationStatusGeneratingCandidates) ||
		(from == GenerationStatusGeneratingCandidates && to == GenerationStatusEvaluatingCandidates) ||
		(from == GenerationStatusEvaluatingCandidates && to == GenerationStatusSynthesizing)
}

func sanitizeGenerationStateArtifacts(state *GenerationState) {
	if state == nil {
		return
	}
	generation := state.Generation
	generation.SelectedSessionIDs = sanitizedGenerationSelectedSessionIDs(generation.SelectedSessionIDs)
	state.Generation = generation

	storedSessions := make(map[string]GenerationSessionRecord, len(state.Sessions))
	for _, session := range state.Sessions {
		if session.GenerationID == generation.ID && session.SessionID != "" {
			storedSessions[session.SessionID] = session
		}
	}
	sessions := make([]GenerationSessionRecord, 0, len(generation.SelectedSessionIDs))
	for ordinal, sessionID := range generation.SelectedSessionIDs {
		session, exists := storedSessions[sessionID]
		if !exists || session.Ordinal != ordinal || validateGenerationSessionArtifact(session) != nil {
			session = GenerationSessionRecord{
				GenerationID: generation.ID, SessionID: sessionID, Ordinal: ordinal,
				Status: GenerationSessionPending, UpdatedAt: generation.CreatedAt,
			}
		}
		sessions = append(sessions, session)
	}

	rawCandidates := append([]GenerationCandidateRecord(nil), state.Candidates...)
	sort.Slice(rawCandidates, func(i, j int) bool {
		if rawCandidates[i].Ordinal != rawCandidates[j].Ordinal {
			return rawCandidates[i].Ordinal < rawCandidates[j].Ordinal
		}
		return rawCandidates[i].ID < rawCandidates[j].ID
	})
	candidates := make([]GenerationCandidateRecord, 0, len(rawCandidates))
	for _, candidate := range rawCandidates {
		if candidate.GenerationID != generation.ID {
			continue
		}
		canonical, err := normalizeGenerationCandidateArtifact(candidate, false)
		if err != nil {
			continue
		}
		canonical.GenerationID = generation.ID
		if validateGenerationCandidatePlacement(generation, sessions, candidates, canonical) != nil {
			continue
		}
		candidates = append(candidates, canonical)
	}
	candidateByID := make(map[string]GenerationCandidateRecord, len(candidates))
	for _, candidate := range candidates {
		candidateByID[candidate.ID] = candidate
	}
	for index := range sessions {
		session := sessions[index]
		if session.CandidateID == "" {
			continue
		}
		candidate, exists := candidateByID[session.CandidateID]
		if !exists || candidate.Kind != GenerationCandidateSession || candidate.Ordinal != session.Ordinal ||
			len(candidate.SourceSessionIDs) != 1 || candidate.SourceSessionIDs[0] != session.SessionID {
			session.Status = GenerationSessionPending
			session.CandidateID = ""
			session.DiagnosticCode = ""
			sessions[index] = session
		}
	}

	evaluations := make([]CandidateEvaluationRecord, 0, len(state.Evaluations))
	seenEvaluationIDs := make(map[string]struct{}, len(state.Evaluations))
	seenEvaluationRequests := make(map[string]struct{}, len(state.Evaluations))
	for _, evaluation := range state.Evaluations {
		if evaluation.GenerationID != generation.ID || !validUUID(evaluation.ID) || !validUUID(evaluation.CandidateID) ||
			evaluation.Profile != generation.EvaluatorProfile ||
			evaluation.ProfileVersion != generation.EvaluatorProfileVersion {
			continue
		}
		if _, exists := candidateByID[evaluation.CandidateID]; !exists {
			continue
		}
		canonical, err := canonicalCandidateEvaluationDetails(evaluation)
		if err != nil {
			continue
		}
		requestKey := canonical.CandidateID + "\x00" + canonical.RequestSHA256
		if _, exists := seenEvaluationIDs[canonical.ID]; exists {
			continue
		}
		if _, exists := seenEvaluationRequests[requestKey]; exists {
			continue
		}
		seenEvaluationIDs[canonical.ID] = struct{}{}
		seenEvaluationRequests[requestKey] = struct{}{}
		evaluations = append(evaluations, canonical)
	}

	diagnosticsBySlot := make(map[string]GenerationDiagnosticRecord, len(state.Diagnostics))
	for _, diagnostic := range state.Diagnostics {
		if diagnostic.GenerationID != generation.ID || validateGenerationDiagnostic(diagnostic) != nil {
			continue
		}
		if diagnostic.SessionID != "" {
			found := false
			for _, session := range sessions {
				if session.SessionID == diagnostic.SessionID {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		if diagnostic.CandidateID != "" {
			if _, exists := candidateByID[diagnostic.CandidateID]; !exists {
				continue
			}
		}
		slot := generationDiagnosticSlot(diagnostic)
		existing, exists := diagnosticsBySlot[slot]
		if !exists || diagnostic.CreatedAt.After(existing.CreatedAt) ||
			(diagnostic.CreatedAt.Equal(existing.CreatedAt) && diagnostic.ID > existing.ID) {
			diagnosticsBySlot[slot] = diagnostic
		}
	}
	diagnostics := make([]GenerationDiagnosticRecord, 0, len(diagnosticsBySlot))
	for _, diagnostic := range diagnosticsBySlot {
		diagnostics = append(diagnostics, diagnostic)
	}

	state.Sessions = sessions
	state.Candidates = candidates
	state.Evaluations = evaluations
	state.Diagnostics = retainNewestGenerationDiagnostics(diagnostics)
}

func sanitizedGenerationSelectedSessionIDs(values []string) []string {
	selectedSessionIDs := make([]string, 0, min(len(values), MaxRevisionSourceSessionIDs))
	seenSelectedSessionIDs := make(map[string]struct{}, len(values))
	for _, rawSessionID := range values {
		sessionID := strings.TrimSpace(rawSessionID)
		if len(selectedSessionIDs) >= MaxRevisionSourceSessionIDs || sessionID == "" ||
			!validSnapshotIdentity(rawSessionID, MaxRevisionIdentityCodePoints) ||
			!validSnapshotIdentity(sessionID, MaxRevisionIdentityCodePoints) {
			continue
		}
		if _, duplicate := seenSelectedSessionIDs[sessionID]; duplicate {
			continue
		}
		seenSelectedSessionIDs[sessionID] = struct{}{}
		selectedSessionIDs = append(selectedSessionIDs, sessionID)
	}
	return selectedSessionIDs
}

func retainNewestGenerationDiagnostics(diagnostics []GenerationDiagnosticRecord) []GenerationDiagnosticRecord {
	sort.Slice(diagnostics, func(i, j int) bool {
		priority := func(diagnostic GenerationDiagnosticRecord) int {
			if diagnostic.Stage == "synthesis" {
				return 2
			}
			if !diagnostic.Retryable {
				return 1
			}
			return 0
		}
		leftPriority := priority(diagnostics[i])
		rightPriority := priority(diagnostics[j])
		if leftPriority != rightPriority {
			return leftPriority > rightPriority
		}
		if !diagnostics[i].CreatedAt.Equal(diagnostics[j].CreatedAt) {
			return diagnostics[i].CreatedAt.After(diagnostics[j].CreatedAt)
		}
		return diagnostics[i].ID > diagnostics[j].ID
	})
	if len(diagnostics) > maxGenerationStateDiagnostics {
		diagnostics = diagnostics[:maxGenerationStateDiagnostics]
	}
	// Retention gives terminal and synthesis resume markers priority, while
	// presentation remains newest-first across the retained logical slots.
	sort.Slice(diagnostics, func(i, j int) bool {
		if !diagnostics[i].CreatedAt.Equal(diagnostics[j].CreatedAt) {
			return diagnostics[i].CreatedAt.After(diagnostics[j].CreatedAt)
		}
		return diagnostics[i].ID > diagnostics[j].ID
	})
	return diagnostics
}

func generationIsExecutable(status GenerationStatus) bool {
	return status == GenerationStatusQueued || status == GenerationStatusGeneratingCandidates ||
		status == GenerationStatusEvaluatingCandidates || status == GenerationStatusSynthesizing
}

func clearGenerationClaim(generation *SkillGenerationRecord) {
	generation.ClaimToken = ""
	generation.ClaimOwner = ""
	generation.LeaseExpiresAt = nil
}

func cloneGenerationRecord(in SkillGenerationRecord) SkillGenerationRecord {
	out := in
	out.Snapshot = canonicalSkillRevisionSnapshot(in.Snapshot)
	out.SelectedSessionIDs = cloneStrings(in.SelectedSessionIDs)
	out.EvaluationCriteria = cloneRawJSON(in.EvaluationCriteria)
	out.LeaseExpiresAt = cloneTime(in.LeaseExpiresAt)
	out.LastHeartbeatAt = cloneTime(in.LastHeartbeatAt)
	out.StartedAt = cloneTime(in.StartedAt)
	out.CompletedAt = cloneTime(in.CompletedAt)
	return out
}

func cloneGenerationSession(in GenerationSessionRecord) GenerationSessionRecord { return in }

func cloneGenerationCandidate(in GenerationCandidateRecord) GenerationCandidateRecord {
	out := in
	out.SourceSessionIDs = cloneStrings(in.SourceSessionIDs)
	out.Snapshot = canonicalGenerationCandidateSnapshot(in.Snapshot)
	out.Insights = cloneRawJSON(in.Insights)
	return out
}

func cloneCandidateEvaluation(in CandidateEvaluationRecord) CandidateEvaluationRecord {
	out := in
	if in.Score != nil {
		score := *in.Score
		out.Score = &score
	}
	out.CriterionResults = cloneRawJSON(in.CriterionResults)
	out.Findings = cloneRawJSON(in.Findings)
	out.Strengths = cloneRawJSON(in.Strengths)
	out.Panel = cloneRawJSON(in.Panel)
	return out
}

func cloneRawJSON(in json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), in...)
}

func normalizedJSON(value json.RawMessage, fallback []byte) ([]byte, error) {
	if len(value) == 0 {
		return fallback, nil
	}
	if !json.Valid(value) {
		return nil, errors.New("invalid JSON snapshot")
	}
	return cloneRawJSON(value), nil
}
