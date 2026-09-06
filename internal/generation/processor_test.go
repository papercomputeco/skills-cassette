package generation_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
	"github.com/papercomputeco/skills-cassette/internal/generation"
	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const (
	testGenerationID                 = "00000000-0000-0000-0000-000000000002"
	testSkillID                      = "00000000-0000-0000-0000-000000000001"
	testBaseRevisionID               = "00000000-0000-0000-0000-000000000003"
	persistedEvaluatorProfile        = "owner-defined-first-draft-rubric"
	persistedEvaluatorProfileVersion = "2026-09-custom"
	configuredSynthesisFeedbackLimit = 16
)

var persistedEvaluationCriteria = []evaluator.Criterion{
	{ID: "custom-safety", Kind: "content", Description: "Preserve the caller's exact non-default safety rule.", Weight: 7},
	{ID: "custom-order", Kind: "structure", Description: "Respect the caller's intentionally ordered evidence.", Weight: 2},
}

type fakeTranscriptLoader struct {
	mu          sync.Mutex
	transcripts map[string]string
	errors      map[string]error
	calls       []string
}

func (f *fakeTranscriptLoader) LoadTranscript(_ context.Context, sessionID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, sessionID)
	if err := f.errors[sessionID]; err != nil {
		return "", err
	}
	return f.transcripts[sessionID], nil
}

func (f *fakeTranscriptLoader) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type fakeCandidateGenerator struct {
	mu             sync.Mutex
	generateErrors map[string]error
	synthesis      *skill.Candidate
	synthesisError error
	requests       []skill.CandidateRequest
	syntheses      []skill.SynthesisRequest
}

func (f *fakeCandidateGenerator) GenerateCandidate(_ context.Context, request skill.CandidateRequest) (*skill.Candidate, error) {
	f.mu.Lock()
	request.InputSnapshot.Tags = append([]string{}, request.InputSnapshot.Tags...)
	request.InputSnapshot.SourceSessionIDs = append([]string{}, request.InputSnapshot.SourceSessionIDs...)
	f.requests = append(f.requests, request)
	key := request.SourceSessionID
	if key == "" {
		key = "context"
	}
	err := f.generateErrors[key]
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return generatedCandidate("candidate-"+key, request.SkillType, request.SourceSessionID), nil
}

func (f *fakeCandidateGenerator) SynthesizeCandidate(_ context.Context, request skill.SynthesisRequest) (*skill.Candidate, error) {
	f.mu.Lock()
	f.syntheses = append(f.syntheses, cloneSynthesisRequest(request))
	err := f.synthesisError
	candidate := cloneSkillCandidate(f.synthesis)
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if candidate == nil {
		return generatedCandidate("synthesis", request.SkillType, ""), nil
	}
	return candidate, nil
}

func (f *fakeCandidateGenerator) Requests() []skill.CandidateRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]skill.CandidateRequest(nil), f.requests...)
}

func (f *fakeCandidateGenerator) Syntheses() []skill.SynthesisRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]skill.SynthesisRequest, len(f.syntheses))
	for index := range f.syntheses {
		result[index] = cloneSynthesisRequest(f.syntheses[index])
	}
	return result
}

type artifactTimingGenerator struct {
	slowSource  string
	slowEntered chan struct{}
	releaseSlow <-chan struct{}
}

func (g *artifactTimingGenerator) GenerateCandidate(ctx context.Context, request skill.CandidateRequest) (*skill.Candidate, error) {
	if request.SourceSessionID == g.slowSource {
		close(g.slowEntered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-g.releaseSlow:
		}
	}
	return generatedCandidate("candidate-"+request.SourceSessionID, request.SkillType, request.SourceSessionID), nil
}

func (*artifactTimingGenerator) SynthesizeCandidate(context.Context, skill.SynthesisRequest) (*skill.Candidate, error) {
	return nil, errors.New("optional synthesis unavailable")
}

type artifactTimingEvaluator struct {
	slowName    string
	slowEntered chan struct{}
	releaseSlow <-chan struct{}
}

func (e *artifactTimingEvaluator) EvaluateCandidate(ctx context.Context, request evaluator.CandidateEvaluationRequest) (evaluator.CandidateEvaluation, error) {
	if request.Candidate.Name == e.slowName {
		close(e.slowEntered)
		select {
		case <-ctx.Done():
			return evaluator.CandidateEvaluation{}, ctx.Err()
		case <-e.releaseSlow:
		}
	}
	result := candidateEvaluation(request.Profile, 0.8, "pass")
	result.ProfileVersion = request.ProfileVersion
	return result, nil
}

type evaluationOutcome struct {
	evaluation evaluator.CandidateEvaluation
	err        error
}

type fakeCandidateEvaluator struct {
	mu       sync.Mutex
	outcomes map[string]evaluationOutcome
	requests []evaluator.CandidateEvaluationRequest
}

func (f *fakeCandidateEvaluator) EvaluateCandidate(_ context.Context, request evaluator.CandidateEvaluationRequest) (evaluator.CandidateEvaluation, error) {
	f.mu.Lock()
	f.requests = append(f.requests, cloneEvaluationRequest(request))
	outcome, ok := f.outcomes[request.Candidate.Name]
	f.mu.Unlock()
	if ok {
		return outcome.evaluation, outcome.err
	}
	result := candidateEvaluation(request.Profile, 0.8, "pass")
	result.ProfileVersion = request.ProfileVersion
	return result, nil
}

func (f *fakeCandidateEvaluator) Requests() []evaluator.CandidateEvaluationRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make([]evaluator.CandidateEvaluationRequest, len(f.requests))
	for index := range f.requests {
		result[index] = cloneEvaluationRequest(f.requests[index])
	}
	return result
}

type transientFailureStore struct {
	*recordingGenerationStore
	candidateErr  error
	evaluationErr error
}

func (s *transientFailureStore) PutGenerationCandidate(
	ctx context.Context,
	generationID, claimToken string,
	candidate storage.GenerationCandidateRecord,
) (*storage.GenerationCandidateRecord, error) {
	if s.candidateErr != nil {
		return nil, s.candidateErr
	}
	return s.recordingGenerationStore.PutGenerationCandidate(ctx, generationID, claimToken, candidate)
}

func (s *transientFailureStore) PutCandidateEvaluation(
	ctx context.Context,
	generationID, claimToken string,
	evaluation storage.CandidateEvaluationRecord,
) (*storage.CandidateEvaluationRecord, error) {
	if s.evaluationErr != nil {
		return nil, s.evaluationErr
	}
	return s.recordingGenerationStore.PutCandidateEvaluation(ctx, generationID, claimToken, evaluation)
}

type recordingGenerationStore struct {
	*storage.MemoryStore

	mu                 sync.Mutex
	creatorSubject     string
	finalizationInputs []storage.AppendPrivateGenerationResultInput
	finalizationErrors []error
	mutateClaimedState func(*storage.GenerationState)
}

func (s *recordingGenerationStore) GetClaimedGeneration(
	ctx context.Context,
	generationID, claimToken string,
) (*storage.GenerationState, error) {
	state, err := s.MemoryStore.GetClaimedGeneration(ctx, generationID, claimToken)
	if err != nil || state == nil {
		return state, err
	}
	s.mu.Lock()
	mutate := s.mutateClaimedState
	s.mu.Unlock()
	if mutate != nil {
		mutate(state)
	}
	return state, nil
}

func (s *recordingGenerationStore) AppendPrivateGenerationResult(
	ctx context.Context,
	input storage.AppendPrivateGenerationResultInput,
) (*storage.SkillRevisionRecord, error) {
	s.mu.Lock()
	s.finalizationInputs = append(s.finalizationInputs, input)
	var injected error
	if len(s.finalizationErrors) > 0 {
		injected = s.finalizationErrors[0]
		s.finalizationErrors = s.finalizationErrors[1:]
	}
	s.mu.Unlock()
	if injected != nil {
		return nil, injected
	}

	state, err := s.GetGenerationByID(ctx, s.creatorSubject, input.GenerationID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, storage.ErrGenerationNotFound
	}
	for _, candidate := range state.Candidates {
		if candidate.ID != input.ResultCandidateID {
			continue
		}
		return &storage.SkillRevisionRecord{
			ID:                "00000000-0000-0000-0000-100000000002",
			SkillID:           state.Generation.SkillID,
			SequenceNumber:    2,
			Version:           "2",
			CreatorSubject:    state.Generation.CreatorSubject,
			BasedOnRevisionID: state.Generation.BaseRevisionID,
			Origin:            storage.RevisionOriginGeneration,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: candidate.Snapshot.Name, Description: candidate.Snapshot.Description,
				Type: candidate.Snapshot.Type, Tags: append([]string(nil), candidate.Snapshot.Tags...),
				Content: candidate.Snapshot.Content, IsAIGenerated: candidate.Snapshot.IsAIGenerated,
				SourceSessionIDs: append([]string(nil), candidate.SourceSessionIDs...),
			},
			GenerationID: input.GenerationID,
		}, nil
	}
	return nil, errors.New("test finalizer: result candidate not found")
}

func (s *recordingGenerationStore) FinalizationInputs() []storage.AppendPrivateGenerationResultInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]storage.AppendPrivateGenerationResultInput(nil), s.finalizationInputs...)
}

func (s *recordingGenerationStore) FailFinalizationOnce(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalizationErrors = append(s.finalizationErrors, err)
}

func (s *recordingGenerationStore) MutateClaimedState(mutate func(*storage.GenerationState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mutateClaimedState = mutate
}

func generatedCandidate(name, skillType, sessionID string) *skill.Candidate {
	sessions := []string{}
	if sessionID != "" {
		sessions = []string{sessionID}
	}
	return &skill.Candidate{
		Skill: skill.Skill{
			Name: name, Description: "Use when applying " + name + ".", Type: skillType,
			Tags: []string{"workflow"}, Content: "## Steps\n\n1. Apply " + name + ".", Sessions: sessions,
		},
		Insights: []skill.CandidateInsight{{
			Kind: "evidence", Summary: "Reusable step from " + name, Evidence: "Bounded evidence for " + name,
		}},
	}
}

func candidateEvaluation(profile string, value float64, decision string) evaluator.CandidateEvaluation {
	return evaluator.CandidateEvaluation{
		Profile: profile, ProfileVersion: persistedEvaluatorProfileVersion,
		EvaluatorVersion: "test", Score: &value, Decision: decision,
		CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`),
		Strengths: json.RawMessage(`["clear"]`), Panel: json.RawMessage(`{"judge_count":1}`),
	}
}

func claimedGeneration(selected []string, authorContext string) (*recordingGenerationStore, *storage.SkillGenerationRecord) {
	store := &recordingGenerationStore{MemoryStore: storage.NewMemoryStore(), creatorSubject: "owner-1"}
	criteriaJSON, err := json.Marshal(persistedEvaluationCriteria)
	Expect(err).NotTo(HaveOccurred())
	now := time.Now().UTC().Add(-time.Second)
	skillRecord, err := store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
		ID: testSkillID, Slug: "working", CreatorSubject: "owner-1", CreatedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	baseRevision, err := store.AppendRevision(context.Background(), storage.AppendRevisionInput{
		ID: testBaseRevisionID, SkillID: skillRecord.ID, CreatorSubject: "owner-1", Origin: storage.RevisionOriginManual,
		Snapshot: storage.SkillRevisionSnapshot{
			Name: "Working base", Description: "Use when starting generation.", Type: "workflow",
			Tags: []string{"baseline", "persisted"}, Content: "## Steps\n\n1. Start from this exact revision.",
			SourceSessionIDs: []string{"baseline-source-z", "baseline-source-a"},
		},
		CreatedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	_, err = store.SetRevisionVisibility(context.Background(), storage.SetRevisionVisibilityInput{
		SkillID: skillRecord.ID, RevisionID: baseRevision.ID, CallerSubject: "owner-1",
		IsPublic: true, ChangedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	_, err = store.SetExplicitLatestRevision(context.Background(), storage.SetExplicitLatestRevisionInput{
		SkillID: skillRecord.ID, RevisionID: baseRevision.ID, CallerSubject: "owner-1", ChangedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	_, err = store.CreateGeneration(context.Background(), storage.CreateGenerationInput{
		ID: testGenerationID, SkillID: skillRecord.ID, BaseRevisionID: baseRevision.ID, CreatorSubject: "owner-1",
		Snapshot: storage.SkillRevisionSnapshot{
			Name: "Working", Description: "Use when working.", Type: "workflow",
			Tags: []string{"seed", "custom"}, Content: "## Steps\n\n1. Work from the persisted seed.",
			SourceSessionIDs: []string{"seed-source-b", "seed-source-a"},
		},
		AuthorContext: authorContext, SelectedSessionIDs: selected,
		EvaluatorProfile: persistedEvaluatorProfile, EvaluatorProfileVersion: persistedEvaluatorProfileVersion,
		EvaluationCriteria: criteriaJSON, CreatedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	claim, err := store.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{
		WorkerID: "worker-1", LeaseDuration: time.Hour,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(claim).NotTo(BeNil())
	return store, claim
}

func persistedGeneration(store *recordingGenerationStore) storage.GenerationState {
	state, err := store.GetGenerationByID(context.Background(), "owner-1", testGenerationID)
	Expect(err).NotTo(HaveOccurred())
	Expect(state).NotTo(BeNil())
	return *state
}

func expectExactEvaluationContract(
	request evaluator.CandidateEvaluationRequest,
	generationRecord storage.SkillGenerationRecord,
	candidateName string,
	candidateSources []string,
	candidateOrdinal int,
) {
	candidateID := deterministicIDForTest(generationRecord.ID, "candidate", strconv.Itoa(candidateOrdinal))
	expectedSources := append([]string(nil), candidateSources...)
	expectedEvidence := append([]string(nil), candidateSources...)
	if len(expectedSources) == 0 {
		expectedSources = nil
	}
	if len(expectedEvidence) == 0 {
		expectedEvidence = nil
	}
	expected := evaluator.CandidateEvaluationRequest{
		Ref:  deterministicIDForTest(generationRecord.ID, "ref", candidateID),
		Name: candidateName,
		Candidate: evaluator.CandidateBundle{
			Name: candidateName, Description: "Use when applying " + candidateName + ".",
			Type: generationRecord.Snapshot.Type, Tags: []string{"workflow"},
			Content: "## Steps\n\n1. Apply " + candidateName + ".", SourceSessionIDs: expectedSources,
		},
		Baseline: &evaluator.CandidateBundle{
			Name: generationRecord.Snapshot.Name, Description: generationRecord.Snapshot.Description,
			Type: generationRecord.Snapshot.Type, Tags: append([]string(nil), generationRecord.Snapshot.Tags...),
			Content:          generationRecord.Snapshot.Content,
			SourceSessionIDs: append([]string(nil), generationRecord.Snapshot.SourceSessionIDs...),
		},
		AuthorContext: generationRecord.AuthorContext, EvidenceSessionIDs: expectedEvidence,
		OwnerSubject: generationRecord.CreatorSubject, Profile: persistedEvaluatorProfile,
		ProfileVersion: persistedEvaluatorProfileVersion,
		Criteria:       append([]evaluator.Criterion(nil), persistedEvaluationCriteria...),
	}
	Expect(request).To(Equal(expected), "the evaluator must receive the exact persisted request rather than recomputed defaults")
	candidate := storage.GenerationCandidateRecord{
		ID: candidateID, Ordinal: candidateOrdinal,
		Snapshot: storage.GenerationCandidateSnapshot{
			Name: request.Candidate.Name, Description: request.Candidate.Description,
			Type: request.Candidate.Type, Tags: append([]string(nil), request.Candidate.Tags...),
			Content: request.Candidate.Content,
		},
		SourceSessionIDs: append([]string(nil), request.Candidate.SourceSessionIDs...),
	}
	storedHash, err := storage.GenerationCandidateEvaluationRequestSHA256(generationRecord, candidate)
	Expect(err).NotTo(HaveOccurred())
	encoded, err := json.Marshal(request)
	Expect(err).NotTo(HaveOccurred())
	digest := sha256.Sum256(encoded)
	Expect(storedHash).To(Equal(hex.EncodeToString(digest[:])),
		"storage and evaluator requests must share one exact content identity")
}

func deterministicIDForTest(parts ...string) string {
	encoded, err := json.Marshal(parts)
	Expect(err).NotTo(HaveOccurred())
	return uuid.NewSHA1(uuid.NameSpaceOID, encoded).String()
}

func expectNoRevisionMetadataMutation(store *recordingGenerationStore, claim *storage.SkillGenerationRecord) {
	revisions, err := store.ListRevisions(context.Background(), storage.RevisionListOpts{
		SkillID: claim.SkillID, CallerSubject: claim.CreatorSubject,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(revisions).To(HaveLen(1), "failed or retryable generation must not append a revision")
	Expect(revisions[0].Revision.ID).To(Equal(claim.BaseRevisionID))
	Expect(revisions[0].Visibility.IsPublic).To(BeTrue())
	Expect(revisions[0].IsExplicitLatest).To(BeTrue())

	identity, err := store.GetSkill(context.Background(), claim.SkillID)
	Expect(err).NotTo(HaveOccurred())
	Expect(identity).NotTo(BeNil())
	Expect(identity.NextSequenceNumber).To(Equal(2), "generation failure must not consume a sequence")
	Expect(identity.ExplicitLatestRevisionID).To(Equal(claim.BaseRevisionID))
	effective, err := store.ResolveEffectiveRevision(context.Background(), storage.EffectiveRevisionReadOpts{SkillID: claim.SkillID})
	Expect(err).NotTo(HaveOccurred())
	Expect(effective).NotTo(BeNil())
	Expect(effective.Revision.ID).To(Equal(claim.BaseRevisionID))
	Expect(effective.Visibility.IsPublic).To(BeTrue())
}

func requestsByCandidateName(requests []evaluator.CandidateEvaluationRequest) map[string]evaluator.CandidateEvaluationRequest {
	result := make(map[string]evaluator.CandidateEvaluationRequest, len(requests))
	for _, request := range requests {
		result[request.Candidate.Name] = request
	}
	return result
}

func candidateRequestsBySource(requests []skill.CandidateRequest) map[string]skill.CandidateRequest {
	result := make(map[string]skill.CandidateRequest, len(requests))
	for _, request := range requests {
		result[request.SourceSessionID] = request
	}
	return result
}

func sessionStatusesByID(sessions []storage.GenerationSessionRecord) map[string]storage.GenerationSessionStatus {
	result := make(map[string]storage.GenerationSessionStatus, len(sessions))
	for _, session := range sessions {
		result[session.SessionID] = session.Status
	}
	return result
}

func candidateBySource(candidates []storage.GenerationCandidateRecord, source string) *storage.GenerationCandidateRecord {
	for index := range candidates {
		if len(candidates[index].SourceSessionIDs) == 1 && candidates[index].SourceSessionIDs[0] == source {
			candidate := candidates[index]
			return &candidate
		}
	}
	return nil
}

func candidateByID(candidates []storage.GenerationCandidateRecord, id string) *storage.GenerationCandidateRecord {
	for index := range candidates {
		if candidates[index].ID == id {
			candidate := candidates[index]
			return &candidate
		}
	}
	return nil
}

func cloneSkillCandidate(candidate *skill.Candidate) *skill.Candidate {
	if candidate == nil {
		return nil
	}
	cloned := *candidate
	cloned.Skill.Tags = append([]string(nil), candidate.Skill.Tags...)
	cloned.Skill.Sessions = append([]string(nil), candidate.Skill.Sessions...)
	cloned.Insights = append([]skill.CandidateInsight(nil), candidate.Insights...)
	return &cloned
}

func cloneSynthesisRequest(request skill.SynthesisRequest) skill.SynthesisRequest {
	cloned := request
	cloned.InputSnapshot.Tags = append([]string{}, request.InputSnapshot.Tags...)
	cloned.InputSnapshot.SourceSessionIDs = append([]string{}, request.InputSnapshot.SourceSessionIDs...)
	cloned.Winner = *cloneSkillCandidate(&request.Winner)
	cloned.Feedback = make([]skill.SynthesisFeedback, len(request.Feedback))
	for index, feedback := range request.Feedback {
		cloned.Feedback[index] = skill.SynthesisFeedback{
			Insights:  append([]skill.CandidateInsight(nil), feedback.Insights...),
			Findings:  append(json.RawMessage(nil), feedback.Findings...),
			Strengths: append(json.RawMessage(nil), feedback.Strengths...),
		}
	}
	return cloned
}

func expectedCandidateInputSnapshot(snapshot storage.SkillRevisionSnapshot) skill.CandidateInputSnapshot {
	return skill.CandidateInputSnapshot{
		Name: snapshot.Name, Description: snapshot.Description, Type: snapshot.Type,
		Tags: append([]string{}, snapshot.Tags...), Content: snapshot.Content,
		IsAIGenerated:    snapshot.IsAIGenerated,
		SourceSessionIDs: append([]string{}, snapshot.SourceSessionIDs...),
	}
}

func cloneEvaluationRequest(request evaluator.CandidateEvaluationRequest) evaluator.CandidateEvaluationRequest {
	cloned := request
	cloned.Candidate.Tags = append([]string(nil), request.Candidate.Tags...)
	cloned.Candidate.SourceSessionIDs = append([]string(nil), request.Candidate.SourceSessionIDs...)
	if request.Baseline != nil {
		baseline := *request.Baseline
		baseline.Tags = append([]string(nil), request.Baseline.Tags...)
		baseline.SourceSessionIDs = append([]string(nil), request.Baseline.SourceSessionIDs...)
		cloned.Baseline = &baseline
	}
	cloned.EvidenceSessionIDs = append([]string(nil), request.EvidenceSessionIDs...)
	cloned.Criteria = append([]evaluator.Criterion(nil), request.Criteria...)
	return cloned
}

var _ = Describe("generation processor", func() {
	It("processor_creates_one_candidate_per_session_or_context", func() {
		selected := []string{"session-bravo", "session-alpha"}
		store, claim := claimedGeneration(selected, "caller context that must survive exactly")
		loader := &fakeTranscriptLoader{transcripts: map[string]string{
			"session-alpha": "ONLY_ALPHA", "session-bravo": "ONLY_BRAVO",
		}, errors: map[string]error{}}
		generator := &fakeCandidateGenerator{
			generateErrors: map[string]error{}, synthesisError: errors.New("optional synthesis unavailable"),
		}
		judge := &fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}}
		processor := generation.NewProcessor(store, loader, generator, judge)

		result, err := processor.Process(context.Background(), claim.ID, claim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(BeNil())
		Expect(result.ResultCandidate.Kind).To(Equal(storage.GenerationCandidateSession))
		Expect(loader.Calls()).To(ConsistOf(selected))

		candidateRequests := candidateRequestsBySource(generator.Requests())
		Expect(candidateRequests).To(HaveLen(2))
		Expect(candidateRequests["session-alpha"].Transcript).To(Equal("ONLY_ALPHA"))
		Expect(candidateRequests["session-alpha"].Transcript).NotTo(ContainSubstring("ONLY_BRAVO"))
		Expect(candidateRequests["session-bravo"].Transcript).To(Equal("ONLY_BRAVO"))
		Expect(candidateRequests["session-bravo"].Transcript).NotTo(ContainSubstring("ONLY_ALPHA"))
		for _, request := range candidateRequests {
			Expect(request.Transcript).NotTo(And(ContainSubstring("ONLY_ALPHA"), ContainSubstring("ONLY_BRAVO")))
			Expect(request.AuthorContext).To(Equal(claim.AuthorContext))
			Expect(request.InputSnapshot).To(Equal(expectedCandidateInputSnapshot(claim.Snapshot)),
				"every independent inference must receive the exact persisted immutable input")
		}

		evaluationRequests := requestsByCandidateName(judge.Requests())
		Expect(evaluationRequests).To(HaveLen(2))
		for candidateName, request := range evaluationRequests {
			sourceID := request.Candidate.SourceSessionIDs
			Expect(sourceID).To(HaveLen(1), candidateName)
			ordinal := slicesIndex(selected, sourceID[0])
			Expect(ordinal).To(BeNumerically(">=", 0))
			expectExactEvaluationContract(request, *claim, "candidate-"+sourceID[0], sourceID, ordinal)
		}
		state := persistedGeneration(store)
		Expect(state.Generation.Status).To(Equal(storage.GenerationStatusSynthesizing))
		Expect(state.Generation.EvaluatorProfile).To(Equal(persistedEvaluatorProfile))
		Expect(state.Generation.EvaluatorProfileVersion).To(Equal(persistedEvaluatorProfileVersion))
		Expect(state.Generation.EvaluationCriteria).To(MatchJSON(mustJSON(persistedEvaluationCriteria)))
		Expect(state.Candidates).To(HaveLen(2))
		Expect(state.Evaluations).To(HaveLen(2))
		Expect(store.FinalizationInputs()).To(Equal([]storage.AppendPrivateGenerationResultInput{{
			GenerationID: claim.ID, ClaimToken: claim.ClaimToken,
			InitialWinnerCandidateID: result.InitialWinnerCandidate.ID,
			ResultCandidateID:        result.ResultCandidate.ID,
		}}))

		contextStore, contextClaim := claimedGeneration(nil, "write a release checklist in caller order")
		contextLoader := &fakeTranscriptLoader{transcripts: map[string]string{}, errors: map[string]error{}}
		contextGenerator := &fakeCandidateGenerator{generateErrors: map[string]error{}}
		contextJudge := &fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}}
		contextResult, err := generation.NewProcessor(contextStore, contextLoader, contextGenerator, contextJudge).
			Process(context.Background(), contextClaim.ID, contextClaim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(contextResult).NotTo(BeNil())
		Expect(contextResult.ResultCandidate.Kind).To(Equal(storage.GenerationCandidateContext))
		Expect(contextLoader.Calls()).To(BeEmpty())
		contextRequests := contextGenerator.Requests()
		Expect(contextRequests).To(HaveLen(1))
		Expect(contextRequests[0].Transcript).To(BeEmpty())
		Expect(contextRequests[0].SourceSessionID).To(BeEmpty())
		Expect(contextRequests[0].AuthorContext).To(Equal(contextClaim.AuthorContext))
		Expect(contextRequests[0].InputSnapshot).To(Equal(expectedCandidateInputSnapshot(contextClaim.Snapshot)))
		Expect(contextResult.ResultCandidate.SourceSessionIDs).To(BeEmpty())
		contextEvaluationRequests := contextJudge.Requests()
		Expect(contextEvaluationRequests).To(HaveLen(1))
		expectExactEvaluationContract(contextEvaluationRequests[0], *contextClaim, "candidate-context", nil, 0)
	})

	It("persists concurrent artifacts as each task finishes", func() {
		selected := []string{"slow", "fast"}
		store, claim := claimedGeneration(selected, "persist completed tasks immediately")
		generationRelease := make(chan struct{})
		evaluationRelease := make(chan struct{})
		generator := &artifactTimingGenerator{
			slowSource: "slow", slowEntered: make(chan struct{}), releaseSlow: generationRelease,
		}
		judge := &artifactTimingEvaluator{
			slowName: "candidate-slow", slowEntered: make(chan struct{}), releaseSlow: evaluationRelease,
		}
		processor := generation.NewProcessor(store, &fakeTranscriptLoader{
			transcripts: map[string]string{"slow": "slow transcript", "fast": "fast transcript"},
			errors:      map[string]error{},
		}, generator, judge)
		type processOutcome struct {
			result *generation.ProcessResult
			err    error
		}
		done := make(chan processOutcome, 1)
		go func() {
			result, err := processor.Process(context.Background(), claim.ID, claim.ClaimToken)
			done <- processOutcome{result: result, err: err}
		}()

		Eventually(generator.slowEntered).Should(BeClosed())
		Eventually(func() []storage.GenerationCandidateRecord {
			return persistedGeneration(store).Candidates
		}).Should(HaveLen(1))
		state := persistedGeneration(store)
		Expect(state.Candidates[0].SourceSessionIDs).To(Equal([]string{"fast"}),
			"the fast candidate must be durable while its peer is still running")
		close(generationRelease)

		Eventually(judge.slowEntered).Should(BeClosed())
		Eventually(func() []storage.CandidateEvaluationRecord {
			return persistedGeneration(store).Evaluations
		}).Should(HaveLen(1))
		state = persistedGeneration(store)
		fastCandidate := candidateBySource(state.Candidates, "fast")
		Expect(fastCandidate).NotTo(BeNil())
		Expect(state.Evaluations[0].CandidateID).To(Equal(fastCandidate.ID),
			"the fast evaluation must be durable while its peer is still running")
		close(evaluationRelease)

		var outcome processOutcome
		Eventually(done).Should(Receive(&outcome))
		Expect(outcome.err).NotTo(HaveOccurred())
		Expect(outcome.result).NotTo(BeNil())
	})

	It("processor_continues_after_source_failures", func() {
		selected := []string{"transcript-bad", "generation-bad", "evaluation-bad", "good"}
		store, claim := claimedGeneration(selected, "retain healthy evidence")
		loader := &fakeTranscriptLoader{
			transcripts: map[string]string{
				"generation-bad": "generate fails", "evaluation-bad": "judge fails", "good": "healthy",
			},
			errors: map[string]error{"transcript-bad": errors.New("raw storage detail")},
		}
		generator := &fakeCandidateGenerator{
			generateErrors: map[string]error{"generation-bad": errors.New("provider secret")},
		}
		judge := &fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{
			"candidate-evaluation-bad": {err: &evaluator.CallError{Code: "evaluator_rejected", Message: "unsafe raw rejection", Retryable: false}},
			"candidate-good":           {evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.9, "pass")},
		}}

		result, err := generation.NewProcessor(store, loader, generator, judge).
			Process(context.Background(), claim.ID, claim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).NotTo(BeNil())
		Expect(result.ResultCandidate.SourceSessionIDs).To(Equal([]string{"good"}))
		Expect(loader.Calls()).To(ConsistOf(selected))
		Expect(generator.Requests()).To(HaveLen(3))
		evaluationRequests := judge.Requests()
		Expect(evaluationRequests).To(HaveLen(3))
		requests := requestsByCandidateName(evaluationRequests)
		Expect(requests).To(HaveKey("candidate-evaluation-bad"))
		Expect(requests).To(HaveKey("candidate-good"))
		Expect(requests).To(HaveKey("synthesis"))
		for _, sourceID := range []string{"evaluation-bad", "good"} {
			expectExactEvaluationContract(requests["candidate-"+sourceID], *claim,
				"candidate-"+sourceID, []string{sourceID}, slicesIndex(selected, sourceID))
		}
		expectExactEvaluationContract(requests["synthesis"], *claim, "synthesis", []string{"good"}, 4)
		Expect(requests["synthesis"].EvidenceSessionIDs).NotTo(ContainElements(
			"transcript-bad", "generation-bad", "evaluation-bad",
		), "synthesis evidence must contain only successfully evaluated sources")

		state := persistedGeneration(store)
		Expect(sessionStatusesByID(state.Sessions)).To(Equal(map[string]storage.GenerationSessionStatus{
			"transcript-bad": storage.GenerationSessionTranscriptFailed,
			"generation-bad": storage.GenerationSessionCandidateFailed,
			"evaluation-bad": storage.GenerationSessionEvaluationFailed,
			"good":           storage.GenerationSessionEvaluated,
		}))
		Expect(state.Candidates).To(HaveLen(3))
		Expect(state.Evaluations).To(HaveLen(2))
		codes := make([]string, len(state.Diagnostics))
		for index, diagnostic := range state.Diagnostics {
			codes[index] = diagnostic.Code
			Expect(diagnostic.Message).NotTo(Or(
				ContainSubstring("raw storage detail"), ContainSubstring("provider secret"),
				ContainSubstring("unsafe raw rejection"),
			))
			Expect(len(diagnostic.Message)).To(BeNumerically("<=", 1024))
		}
		Expect(codes).To(ConsistOf(
			"transcript_unavailable", "candidate_generation_failed", "evaluator_rejected",
		))
	})

	It("isolates generated snapshots that violate revision limits as source failures", func() {
		selected := []string{"bad-model", "healthy-model"}
		store, claim := claimedGeneration(selected, "isolate malformed model output")
		loader := &fakeTranscriptLoader{
			transcripts: map[string]string{"bad-model": "BAD_MODEL_SOURCE", "healthy-model": "HEALTHY_MODEL_SOURCE"},
			errors:      map[string]error{},
		}
		var badCalls int
		generator := skill.NewGenerator(nil, func(_ context.Context, prompt string) (string, error) {
			if strings.Contains(prompt, "BAD_MODEL_SOURCE") {
				badCalls++
				tags := make([]string, 65)
				for index := range tags {
					tags[index] = fmt.Sprintf("tag-%d", index)
				}
				body, err := json.Marshal(map[string]any{
					"skill":    map[string]any{"name": "bad", "description": "Use when invalid.", "tags": tags, "content": "# Bad"},
					"insights": []any{},
				})
				Expect(err).NotTo(HaveOccurred())
				return string(body), nil
			}
			return `{"skill":{"name":"healthy","description":"Use when healthy.","tags":[],"content":"# Healthy"},"insights":[]}`, nil
		})
		result, err := generation.NewProcessor(store, loader, generator,
			&fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}}).
			Process(context.Background(), claim.ID, claim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(badCalls).To(Equal(3), "invalid model snapshots use only the bounded parse/validation retries")
		Expect(result.ResultCandidate.SourceSessionIDs).To(Equal([]string{"healthy-model"}))
		state := persistedGeneration(store)
		Expect(sessionStatusesByID(state.Sessions)["bad-model"]).To(Equal(storage.GenerationSessionCandidateFailed))
		Expect(state.Diagnostics).To(ContainElement(HaveField("Code", "candidate_generation_failed")))
	})

	It("keeps transient source failures resumable and retries only without viable peers", func() {
		transient := &skill.ExternalCallError{Retryable: true}

		By("finishing from a healthy peer without retrying the whole generation")
		peerStore, peerClaim := claimedGeneration(
			[]string{"tapes-transient", "llm-transient", "healthy"}, "continue healthy source work",
		)
		peerLoader := &fakeTranscriptLoader{
			transcripts: map[string]string{"llm-transient": "llm input", "healthy": "healthy input"},
			errors:      map[string]error{"tapes-transient": transient},
		}
		peerGenerator := &fakeCandidateGenerator{
			generateErrors: map[string]error{"llm-transient": transient},
			synthesisError: errors.New("optional synthesis unavailable"),
		}
		peerJudge := &fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}}
		peerResult, err := generation.NewProcessor(peerStore, peerLoader, peerGenerator, peerJudge).
			Process(context.Background(), peerClaim.ID, peerClaim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(peerResult.ResultCandidate.SourceSessionIDs).To(Equal([]string{"healthy"}))
		peerState := persistedGeneration(peerStore)
		Expect(sessionStatusesByID(peerState.Sessions)).To(Equal(map[string]storage.GenerationSessionStatus{
			"tapes-transient": storage.GenerationSessionPending,
			"llm-transient":   storage.GenerationSessionPending,
			"healthy":         storage.GenerationSessionEvaluated,
		}))
		Expect(peerState.Diagnostics).To(ContainElements(
			And(HaveField("Code", "transcript_unavailable"), HaveField("Retryable", true)),
			And(HaveField("Code", "candidate_generation_failed"), HaveField("Retryable", true)),
		))
		Expect(peerStore.FinalizationInputs()).To(HaveLen(1), "a viable peer must prevent generation-level retry")

		By("requesting a retry when every source failure is transient")
		retryStore, retryClaim := claimedGeneration(
			[]string{"retry-tapes", "retry-llm"}, "resume transient source work",
		)
		retryLoader := &fakeTranscriptLoader{
			transcripts: map[string]string{"retry-tapes": "restored tapes input", "retry-llm": "llm input"},
			errors:      map[string]error{"retry-tapes": transient},
		}
		retryGenerator := &fakeCandidateGenerator{
			generateErrors: map[string]error{"retry-llm": transient},
			synthesisError: errors.New("optional synthesis unavailable"),
		}
		retryProcessor := generation.NewProcessor(
			retryStore, retryLoader, retryGenerator,
			&fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}},
		)
		firstResult, firstErr := retryProcessor.Process(context.Background(), retryClaim.ID, retryClaim.ClaimToken)
		Expect(firstResult).To(BeNil())
		var processErr *generation.ProcessError
		Expect(errors.As(firstErr, &processErr)).To(BeTrue())
		Expect(processErr.Code).To(Equal("generation_source_temporarily_unavailable"))
		Expect(processErr.Retryable).To(BeTrue())
		firstState := persistedGeneration(retryStore)
		Expect(firstState.Generation.Status).To(Equal(storage.GenerationStatusGeneratingCandidates))
		for _, session := range firstState.Sessions {
			Expect(session.Status).To(Equal(storage.GenerationSessionPending))
		}
		Expect(retryStore.FinalizationInputs()).To(BeEmpty())

		delete(retryLoader.errors, "retry-tapes")
		delete(retryGenerator.generateErrors, "retry-llm")
		resumed, resumeErr := retryProcessor.Process(context.Background(), retryClaim.ID, retryClaim.ClaimToken)
		Expect(resumeErr).NotTo(HaveOccurred())
		Expect(resumed).NotTo(BeNil())
		Expect(resumed.InitialWinnerCandidate.SourceSessionIDs).To(HaveLen(1))
		Expect(retryStore.FinalizationInputs()).To(HaveLen(1))
	})

	It("rejects evaluator profile drift before persistence, reuse, ranking, or finalization", func() {
		store, claim := claimedGeneration(
			[]string{"wrong-profile", "wrong-version", "healthy"}, "enforce persisted rubric identity",
		)
		wrongVersion := candidateEvaluation(persistedEvaluatorProfile, 0.99, "pass")
		wrongVersion.ProfileVersion = "other-version"
		judge := &fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{
			"candidate-wrong-profile": {evaluation: candidateEvaluation("other-profile", 1, "pass")},
			"candidate-wrong-version": {evaluation: wrongVersion},
			"candidate-healthy":       {evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.8, "pass")},
		}}
		result, err := generation.NewProcessor(
			store,
			&fakeTranscriptLoader{transcripts: map[string]string{
				"wrong-profile": "profile", "wrong-version": "version", "healthy": "healthy",
			}, errors: map[string]error{}},
			&fakeCandidateGenerator{generateErrors: map[string]error{}, synthesisError: errors.New("optional synthesis unavailable")},
			judge,
		).Process(context.Background(), claim.ID, claim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.InitialWinnerCandidate.SourceSessionIDs).To(Equal([]string{"healthy"}))
		state := persistedGeneration(store)
		Expect(state.Evaluations).To(HaveLen(1), "mismatched evaluator responses must never be persisted")
		Expect(state.Diagnostics).To(ContainElements(
			HaveField("Code", "evaluation_profile_mismatch"),
			HaveField("Code", "evaluation_profile_mismatch"),
		))
		Expect(store.FinalizationInputs()).To(HaveLen(1))

		By("excluding a mismatched persisted evaluation from resumed selection")
		resumedStore, resumedClaim := claimedGeneration([]string{"persisted"}, "reject stale persisted policy")
		resumedStore.MutateClaimedState(func(state *storage.GenerationState) {
			if state.Generation.Status == storage.GenerationStatusSynthesizing && len(state.Evaluations) > 0 {
				state.Evaluations[0].ProfileVersion = "stale-version"
			}
		})
		resumedResult, resumedErr := generation.NewProcessor(
			resumedStore,
			&fakeTranscriptLoader{transcripts: map[string]string{"persisted": "persisted"}, errors: map[string]error{}},
			&fakeCandidateGenerator{generateErrors: map[string]error{}},
			&fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}},
		).Process(context.Background(), resumedClaim.ID, resumedClaim.ClaimToken)
		Expect(resumedResult).To(BeNil())
		var resumedProcessErr *generation.ProcessError
		Expect(errors.As(resumedErr, &resumedProcessErr)).To(BeTrue())
		Expect(resumedProcessErr.Code).To(Equal("no_viable_candidates"))
		Expect(resumedStore.FinalizationInputs()).To(BeEmpty())
		resumedState := persistedGeneration(resumedStore)
		Expect(resumedState.Diagnostics).To(ContainElement(HaveField("Code", "evaluation_profile_mismatch")))
	})

	It("classifies ordinary storage failures as retryable without weakening fencing", func() {
		for _, failure := range []struct {
			name          string
			candidateErr  error
			evaluationErr error
		}{
			{name: "candidate persistence", candidateErr: errors.New("database temporarily unavailable")},
			{name: "evaluation persistence", evaluationErr: errors.New("database temporarily unavailable")},
		} {
			store, claim := claimedGeneration([]string{"source"}, "retry storage failures")
			wrapped := &transientFailureStore{
				recordingGenerationStore: store,
				candidateErr:             failure.candidateErr,
				evaluationErr:            failure.evaluationErr,
			}
			_, err := generation.NewProcessor(wrapped, &fakeTranscriptLoader{
				transcripts: map[string]string{"source": "bounded source"}, errors: map[string]error{},
			}, &fakeCandidateGenerator{generateErrors: map[string]error{}, synthesisError: errors.New("skip")},
				&fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}}).
				Process(context.Background(), claim.ID, claim.ClaimToken)
			var processErr *generation.ProcessError
			Expect(errors.As(err, &processErr)).To(BeTrue(), failure.name)
			Expect(processErr.Retryable).To(BeTrue(), failure.name)
			Expect(processErr.Code).To(Equal("generation_storage_temporarily_unavailable"), failure.name)
			Expect(errors.Is(err, storage.ErrGenerationClaimLost)).To(BeFalse(), failure.name)
		}
	})

	It("processor_fails_without_appending_when_no_viable_candidates", func() {
		terminalFailures := map[string]error{
			"terminal-canceled": &evaluator.CallError{Code: "evaluator_canceled", Message: "raw canceled", Retryable: false},
			"terminal-rejected": &evaluator.CallError{Code: "evaluator_rejected", Message: "raw rejected", Retryable: false},
			"terminal-invalid":  &evaluator.CallError{Code: "evaluator_invalid_response", Message: "raw invalid", Retryable: false},
			"terminal-unknown":  errors.New("raw unknown evaluator failure"),
		}
		terminalSessions := sortedKeys(terminalFailures)
		terminalStore, terminalClaim := claimedGeneration(terminalSessions, "terminal evaluation coverage")
		terminalTranscripts := make(map[string]string, len(terminalSessions))
		terminalOutcomes := make(map[string]evaluationOutcome, len(terminalSessions))
		for _, sessionID := range terminalSessions {
			terminalTranscripts[sessionID] = "transcript for " + sessionID
			terminalOutcomes["candidate-"+sessionID] = evaluationOutcome{err: terminalFailures[sessionID]}
		}
		terminalResult, terminalErr := generation.NewProcessor(
			terminalStore,
			&fakeTranscriptLoader{transcripts: terminalTranscripts, errors: map[string]error{}},
			&fakeCandidateGenerator{generateErrors: map[string]error{}},
			&fakeCandidateEvaluator{outcomes: terminalOutcomes},
		).Process(context.Background(), terminalClaim.ID, terminalClaim.ClaimToken)
		Expect(terminalResult).To(BeNil())
		var terminalProcessError *generation.ProcessError
		Expect(errors.As(terminalErr, &terminalProcessError)).To(BeTrue())
		Expect(terminalProcessError.Code).To(Equal("no_viable_candidates"))
		Expect(terminalProcessError.Retryable).To(BeFalse())
		terminalState := persistedGeneration(terminalStore)
		Expect(terminalState.Generation.Status).To(Equal(storage.GenerationStatusEvaluatingCandidates),
			"Processor reports terminal classification; Worker owns the atomic failed transition")
		Expect(terminalState.Generation.ResultRevisionID).To(BeEmpty())
		Expect(terminalState.Candidates).To(HaveLen(len(terminalSessions)))
		Expect(terminalState.Evaluations).To(BeEmpty())
		Expect(terminalState.Diagnostics).To(HaveLen(len(terminalSessions)))
		for _, status := range sessionStatusesByID(terminalState.Sessions) {
			Expect(status).To(Equal(storage.GenerationSessionEvaluationFailed))
		}
		Expect(terminalStore.FinalizationInputs()).To(BeEmpty())
		expectNoRevisionMetadataMutation(terminalStore, terminalClaim)

		retryableFailures := map[string]error{
			"retryable-rate-limited": &evaluator.CallError{Code: "evaluator_rate_limited", Message: "raw rate limit", Retryable: true},
			"retryable-unavailable":  &evaluator.CallError{Code: "evaluator_unavailable", Message: "raw outage", Retryable: true},
		}
		retryableSessions := sortedKeys(retryableFailures)
		retryableStore, retryableClaim := claimedGeneration(retryableSessions, "retryable evaluation coverage")
		retryableTranscripts := make(map[string]string, len(retryableSessions))
		retryableOutcomes := make(map[string]evaluationOutcome, len(retryableSessions))
		for _, sessionID := range retryableSessions {
			retryableTranscripts[sessionID] = "transcript for " + sessionID
			retryableOutcomes["candidate-"+sessionID] = evaluationOutcome{err: retryableFailures[sessionID]}
		}
		retryableResult, retryableErr := generation.NewProcessor(
			retryableStore,
			&fakeTranscriptLoader{transcripts: retryableTranscripts, errors: map[string]error{}},
			&fakeCandidateGenerator{generateErrors: map[string]error{}},
			&fakeCandidateEvaluator{outcomes: retryableOutcomes},
		).Process(context.Background(), retryableClaim.ID, retryableClaim.ClaimToken)
		Expect(retryableResult).To(BeNil())
		var retryableProcessError *generation.ProcessError
		Expect(errors.As(retryableErr, &retryableProcessError)).To(BeTrue())
		Expect(retryableProcessError.Code).To(Equal("evaluator_temporarily_unavailable"))
		Expect(retryableProcessError.Retryable).To(BeTrue())
		retryableState := persistedGeneration(retryableStore)
		Expect(retryableState.Generation.Status).To(Equal(storage.GenerationStatusEvaluatingCandidates))
		Expect(retryableState.Generation.ResultRevisionID).To(BeEmpty())
		Expect(retryableState.Candidates).To(HaveLen(len(retryableSessions)))
		Expect(retryableState.Evaluations).To(BeEmpty())
		Expect(retryableState.Diagnostics).To(HaveLen(len(retryableSessions)))
		for _, diagnostic := range retryableState.Diagnostics {
			Expect(diagnostic.Retryable).To(BeTrue())
		}
		for _, status := range sessionStatusesByID(retryableState.Sessions) {
			Expect(status).To(Equal(storage.GenerationSessionCandidateReady), "retryable evaluation remains resumable")
		}
		Expect(retryableStore.FinalizationInputs()).To(BeEmpty())
		expectNoRevisionMetadataMutation(retryableStore, retryableClaim)
	})

	It("processor_synthesizes_once_and_keeps_higher_ranked_result", func() {
		By("bounding deterministic feedback for the largest accepted source set and reusing it after finalization retry")
		selected := make([]string, 100)
		transcripts := make(map[string]string, len(selected))
		outcomes := make(map[string]evaluationOutcome, len(selected)+1)
		for index := range selected {
			sessionID := fmt.Sprintf("source-%03d", index+1)
			selected[index] = sessionID
			transcripts[sessionID] = "RAW_TRANSCRIPT_" + sessionID
			score := 0.80
			if index == 0 {
				score = 0.90
			}
			outcomes["candidate-"+sessionID] = evaluationOutcome{
				evaluation: candidateEvaluation(persistedEvaluatorProfile, score, "pass"),
			}
		}
		outcomes["synthesis"] = evaluationOutcome{
			evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.95, "pass"),
		}
		oversizedStore, oversizedClaim := claimedGeneration(selected, "bounded deterministic synthesis context")
		finalizationInterruption := errors.New("injected finalization interruption")
		oversizedStore.FailFinalizationOnce(finalizationInterruption)
		oversizedGenerator := &fakeCandidateGenerator{
			generateErrors: map[string]error{}, synthesis: generatedCandidate("synthesis", "workflow", ""),
		}
		oversizedJudge := &fakeCandidateEvaluator{outcomes: outcomes}
		oversizedProcessor := generation.NewProcessor(
			oversizedStore,
			&fakeTranscriptLoader{transcripts: transcripts, errors: map[string]error{}},
			oversizedGenerator,
			oversizedJudge,
		)
		firstResult, firstErr := oversizedProcessor.Process(context.Background(), oversizedClaim.ID, oversizedClaim.ClaimToken)
		Expect(firstResult).To(BeNil())
		Expect(firstErr).To(MatchError(finalizationInterruption))
		oversizedResult, err := oversizedProcessor.Process(context.Background(), oversizedClaim.ID, oversizedClaim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(oversizedResult).NotTo(BeNil())
		Expect(oversizedResult.InitialWinnerCandidate.SourceSessionIDs).To(Equal([]string{"source-001"}))
		Expect(oversizedResult.ResultCandidate.Kind).To(Equal(storage.GenerationCandidateSynthesis))
		Expect(oversizedResult.ResultEvaluation.Score).To(HaveValue(Equal(0.95)))

		oversizedSyntheses := oversizedGenerator.Syntheses()
		Expect(oversizedSyntheses).To(HaveLen(1), "resume must not repeat synthesis generation")
		oversizedSynthesis := oversizedSyntheses[0]
		Expect(oversizedSynthesis.Winner.Skill.Name).To(Equal("candidate-source-001"))
		Expect(oversizedSynthesis.AuthorContext).To(Equal(oversizedClaim.AuthorContext))
		Expect(oversizedSynthesis.InputSnapshot).To(Equal(expectedCandidateInputSnapshot(oversizedClaim.Snapshot)),
			"synthesis must retain the persisted author snapshot intent without receiving transcripts")
		feedbackSummaries := make([]string, len(oversizedSynthesis.Feedback))
		for index, feedback := range oversizedSynthesis.Feedback {
			Expect(feedback.Insights).NotTo(BeEmpty())
			feedbackSummaries[index] = feedback.Insights[0].Summary
		}
		expectedFeedbackPrefix := make([]string, configuredSynthesisFeedbackLimit)
		for index := range expectedFeedbackPrefix {
			expectedFeedbackPrefix[index] = fmt.Sprintf("Reusable step from candidate-source-%03d", index+1)
		}
		Expect(feedbackSummaries[:min(len(feedbackSummaries), configuredSynthesisFeedbackLimit)]).
			To(Equal(expectedFeedbackPrefix[:min(len(feedbackSummaries), configuredSynthesisFeedbackLimit)]),
				"bounded feedback must follow stable source ordinal, not completion order")
		encodedSynthesis, marshalErr := json.Marshal(oversizedSynthesis)
		Expect(marshalErr).NotTo(HaveOccurred())
		Expect(string(encodedSynthesis)).NotTo(ContainSubstring("RAW_TRANSCRIPT_"))

		oversizedEvaluationRequests := oversizedJudge.Requests()
		Expect(oversizedEvaluationRequests).To(HaveLen(101), "100 source candidates plus one synthesis are each evaluated once")
		synthesisEvaluationCount := 0
		for _, request := range oversizedEvaluationRequests {
			if request.Candidate.Name == "synthesis" {
				synthesisEvaluationCount++
				expectExactEvaluationContract(request, *oversizedClaim, "synthesis", selected, len(selected))
				continue
			}
			sourceID := request.Candidate.SourceSessionIDs[0]
			expectExactEvaluationContract(request, *oversizedClaim, "candidate-"+sourceID, []string{sourceID}, slicesIndex(selected, sourceID))
		}
		Expect(synthesisEvaluationCount).To(Equal(1), "synthesis must cross exactly one evaluator call, even after resume")
		oversizedState := persistedGeneration(oversizedStore)
		Expect(oversizedState.Sessions).To(HaveLen(100))
		Expect(oversizedState.Candidates).To(HaveLen(101))
		Expect(oversizedState.Evaluations).To(HaveLen(101))
		synthesisCandidate := candidateByID(oversizedState.Candidates, oversizedResult.ResultCandidate.ID)
		Expect(synthesisCandidate).NotTo(BeNil())
		Expect(synthesisCandidate.SourceSessionIDs).To(Equal(selected), "all effective sources remain bounded by the configured 100-source limit")
		oversizedFinalizations := oversizedStore.FinalizationInputs()
		Expect(oversizedFinalizations).To(HaveLen(2))
		for _, input := range oversizedFinalizations {
			Expect(input.InitialWinnerCandidateID).To(Equal(oversizedResult.InitialWinnerCandidate.ID))
			Expect(input.ResultCandidateID).To(Equal(oversizedResult.ResultCandidate.ID))
		}

		By("keeping the deterministic initial winner when synthesis ranks lower")
		lowerStore, lowerClaim := claimedGeneration([]string{"lower-a", "lower-b"}, "lower synthesis fallback")
		lowerGenerator := &fakeCandidateGenerator{generateErrors: map[string]error{}}
		lowerJudge := &fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{
			"candidate-lower-a": {evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.90, "pass")},
			"candidate-lower-b": {evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.80, "pass")},
			"synthesis":         {evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.50, "pass")},
		}}
		lowerResult, err := generation.NewProcessor(
			lowerStore,
			&fakeTranscriptLoader{transcripts: map[string]string{"lower-a": "raw-a", "lower-b": "raw-b"}, errors: map[string]error{}},
			lowerGenerator,
			lowerJudge,
		).Process(context.Background(), lowerClaim.ID, lowerClaim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(lowerResult.ResultCandidate.ID).To(Equal(lowerResult.InitialWinnerCandidate.ID))
		Expect(lowerGenerator.Syntheses()).To(HaveLen(1))
		Expect(countEvaluationRequests(lowerJudge.Requests(), "synthesis")).To(Equal(1))

		By("falling back after synthesis generation failure without retrying the failed stage")
		generationFailureStore, generationFailureClaim := claimedGeneration(
			[]string{"generation-fallback-a", "generation-fallback-b"}, "generation failure fallback",
		)
		generationFailureStore.FailFinalizationOnce(finalizationInterruption)
		generationFailureGenerator := &fakeCandidateGenerator{
			generateErrors: map[string]error{}, synthesisError: errors.New("raw synthesis provider error"),
		}
		generationFailureJudge := &fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{
			"candidate-generation-fallback-a": {evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.90, "pass")},
			"candidate-generation-fallback-b": {evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.80, "pass")},
		}}
		generationFailureProcessor := generation.NewProcessor(
			generationFailureStore,
			&fakeTranscriptLoader{transcripts: map[string]string{
				"generation-fallback-a": "raw-a", "generation-fallback-b": "raw-b",
			}, errors: map[string]error{}},
			generationFailureGenerator,
			generationFailureJudge,
		)
		_, err = generationFailureProcessor.Process(context.Background(), generationFailureClaim.ID, generationFailureClaim.ClaimToken)
		Expect(err).To(MatchError(finalizationInterruption))
		generationFallbackResult, err := generationFailureProcessor.Process(context.Background(), generationFailureClaim.ID, generationFailureClaim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(generationFallbackResult.ResultCandidate.ID).To(Equal(generationFallbackResult.InitialWinnerCandidate.ID))
		Expect(generationFailureGenerator.Syntheses()).To(HaveLen(1))
		Expect(countEvaluationRequests(generationFailureJudge.Requests(), "synthesis")).To(BeZero())
		generationFailureState := persistedGeneration(generationFailureStore)
		Expect(generationFailureState.Diagnostics).To(ContainElement(And(
			HaveField("Code", "synthesis_generation_failed"),
			HaveField("Message", Not(ContainSubstring("raw synthesis provider error"))),
		)))

		By("falling back after one synthesis evaluation failure without regenerating or re-evaluating")
		for _, failureCase := range []struct {
			err       error
			code      string
			retryable bool
		}{
			{err: &evaluator.CallError{Code: "evaluator_rejected", Message: "raw rejection", Retryable: false}, code: "synthesis_evaluator_rejected"},
			{err: &evaluator.CallError{Code: "evaluator_unavailable", Message: "raw outage", Retryable: true}, code: "synthesis_evaluator_unavailable", retryable: true},
		} {
			evaluationFailureStore, evaluationFailureClaim := claimedGeneration(
				[]string{"evaluation-fallback-a", "evaluation-fallback-b"}, "evaluation failure fallback",
			)
			evaluationFailureStore.FailFinalizationOnce(finalizationInterruption)
			evaluationFailureGenerator := &fakeCandidateGenerator{generateErrors: map[string]error{}}
			evaluationFailureJudge := &fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{
				"candidate-evaluation-fallback-a": {evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.90, "pass")},
				"candidate-evaluation-fallback-b": {evaluation: candidateEvaluation(persistedEvaluatorProfile, 0.80, "pass")},
				"synthesis":                       {err: failureCase.err},
			}}
			evaluationFailureProcessor := generation.NewProcessor(
				evaluationFailureStore,
				&fakeTranscriptLoader{transcripts: map[string]string{
					"evaluation-fallback-a": "raw-a", "evaluation-fallback-b": "raw-b",
				}, errors: map[string]error{}},
				evaluationFailureGenerator,
				evaluationFailureJudge,
			)
			_, err = evaluationFailureProcessor.Process(context.Background(), evaluationFailureClaim.ID, evaluationFailureClaim.ClaimToken)
			Expect(err).To(MatchError(finalizationInterruption))
			evaluationFallbackResult, retryErr := evaluationFailureProcessor.Process(
				context.Background(), evaluationFailureClaim.ID, evaluationFailureClaim.ClaimToken,
			)
			Expect(retryErr).NotTo(HaveOccurred())
			Expect(evaluationFallbackResult.ResultCandidate.ID).To(Equal(evaluationFallbackResult.InitialWinnerCandidate.ID))
			Expect(evaluationFailureGenerator.Syntheses()).To(HaveLen(1))
			Expect(countEvaluationRequests(evaluationFailureJudge.Requests(), "synthesis")).To(Equal(1))
			Expect(evaluationFailureStore.FinalizationInputs()).To(HaveLen(2))
			for _, input := range evaluationFailureStore.FinalizationInputs() {
				Expect(input.ResultCandidateID).To(Equal(evaluationFallbackResult.InitialWinnerCandidate.ID))
			}
			evaluationFailureState := persistedGeneration(evaluationFailureStore)
			Expect(evaluationFailureState.Diagnostics).To(ContainElement(And(
				HaveField("Code", failureCase.code),
				HaveField("Retryable", failureCase.retryable),
				HaveField("Message", Not(Or(ContainSubstring("raw rejection"), ContainSubstring("raw outage")))),
			)))
		}

		// Keep this assertion last so every fallback and retry path executes while
		// the processor implementation is still RED on oversized feedback.
		Expect(len(oversizedSynthesis.Feedback)).To(BeNumerically("<=", configuredSynthesisFeedbackLimit),
			"synthesis feedback must fit the generator's configured bound")
	})
})

func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	Expect(err).NotTo(HaveOccurred())
	return encoded
}

func sortedKeys(values map[string]error) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func countEvaluationRequests(requests []evaluator.CandidateEvaluationRequest, candidateName string) int {
	count := 0
	for _, request := range requests {
		if request.Candidate.Name == candidateName {
			count++
		}
	}
	return count
}

func slicesIndex(values []string, target string) int {
	for index, value := range values {
		if value == target {
			return index
		}
	}
	return -1
}
