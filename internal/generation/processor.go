package generation

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

// Processor advances one fenced generation claim through independent source
// candidates, evaluation, deterministic selection, and optional synthesis.
const (
	maxSynthesisFeedbackInputs = 16
	maxGenerationConcurrency   = 100
)

// ProcessorConfig bounds external candidate-generation and evaluation calls.
// Persistence and deterministic ranking remain serially ordered even when the
// external work is later scheduled concurrently.
type ProcessorConfig struct {
	CandidateConcurrency int
}

// DefaultProcessorConfig returns the bounded processor defaults used by callers
// that do not need to share worker configuration explicitly.
func DefaultProcessorConfig() ProcessorConfig {
	return ProcessorConfig{CandidateConcurrency: 2}
}

type Processor struct {
	store      storage.GenerationStore
	transcript TranscriptLoader
	generator  CandidateGenerator
	evaluator  evaluator.CandidateEvaluator
	config     ProcessorConfig
	recorder   Recorder
}

// NewProcessor creates the deterministic orchestration core used by leased
// workers with bounded defaults.
func NewProcessor(store storage.GenerationStore, transcript TranscriptLoader, generator CandidateGenerator, candidateEvaluator evaluator.CandidateEvaluator) *Processor {
	return NewProcessorWithConfig(store, transcript, generator, candidateEvaluator, DefaultProcessorConfig(), nil)
}

// NewProcessorWithConfig injects the typed fixed-vocabulary stage recorder and
// exposes the external-call concurrency seam. The same cap governs candidate
// generation and candidate evaluation while deterministic artifact persistence
// and ranking remain serial and ordered.
func NewProcessorWithConfig(
	store storage.GenerationStore,
	transcript TranscriptLoader,
	generator CandidateGenerator,
	candidateEvaluator evaluator.CandidateEvaluator,
	config ProcessorConfig,
	recorder Recorder,
) *Processor {
	if config.CandidateConcurrency <= 0 {
		config.CandidateConcurrency = DefaultProcessorConfig().CandidateConcurrency
	}
	if config.CandidateConcurrency > maxGenerationConcurrency {
		config.CandidateConcurrency = maxGenerationConcurrency
	}
	return &Processor{
		store: store, transcript: transcript, generator: generator,
		evaluator: candidateEvaluator, config: config, recorder: recorder,
	}
}

// ProcessResult distinguishes the deterministic initial winner from the final
// evaluated candidate appended as a private revision.
type ProcessResult struct {
	InitialWinnerCandidate  storage.GenerationCandidateRecord
	InitialWinnerEvaluation storage.CandidateEvaluationRecord
	ResultCandidate         storage.GenerationCandidateRecord
	ResultEvaluation        storage.CandidateEvaluationRecord
	ResultRevision          storage.SkillRevisionRecord
	SynthesisAttempted      bool
}

// ProcessError classifies generation-level outcomes for worker retry policy.
type ProcessError struct {
	Code      string
	Message   string
	Retryable bool
	cause     error
}

func (e *ProcessError) Error() string { return e.Message }
func (e *ProcessError) Unwrap() error { return e.cause }

// Process resumes a claimed generation from its persisted artifacts.
func (p *Processor) Process(ctx context.Context, generationID, claimToken string) (*ProcessResult, error) {
	if p.store == nil || p.transcript == nil || p.generator == nil || p.evaluator == nil {
		return nil, &ProcessError{Code: "generation_not_configured", Message: "Generation processing is not configured."}
	}
	state, err := p.claimedState(ctx, generationID, claimToken)
	if err != nil {
		return nil, err
	}
	if state.Generation.Status == storage.GenerationStatusQueued {
		if err := p.transition(ctx, generationID, claimToken, storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates); err != nil {
			return nil, err
		}
		state, err = p.claimedState(ctx, generationID, claimToken)
		if err != nil {
			return nil, err
		}
	}
	if state.Generation.Status == storage.GenerationStatusGeneratingCandidates {
		candidateAvailable, retryableSourceFailure, generateErr := p.generateIndependentCandidates(ctx, claimToken, state)
		if generateErr != nil {
			return nil, generateErr
		}
		if !candidateAvailable && retryableSourceFailure {
			return nil, &ProcessError{
				Code: "generation_source_temporarily_unavailable", Message: "Generation sources are temporarily unavailable.", Retryable: true,
			}
		}
		if err := p.transition(ctx, generationID, claimToken, storage.GenerationStatusGeneratingCandidates, storage.GenerationStatusEvaluatingCandidates); err != nil {
			return nil, err
		}
		state, err = p.claimedState(ctx, generationID, claimToken)
		if err != nil {
			return nil, err
		}
	}
	if state.Generation.Status == storage.GenerationStatusEvaluatingCandidates {
		viable, retryable, evaluateErr := p.evaluateIndependentCandidates(ctx, claimToken, state)
		if evaluateErr != nil {
			return nil, evaluateErr
		}
		if len(viable) == 0 {
			if retryable {
				return nil, &ProcessError{Code: "evaluator_temporarily_unavailable", Message: "Candidate evaluation is temporarily unavailable.", Retryable: true}
			}
			return nil, &ProcessError{Code: "no_viable_candidates", Message: "No candidate could be evaluated."}
		}
		if err := p.transition(ctx, generationID, claimToken, storage.GenerationStatusEvaluatingCandidates, storage.GenerationStatusSynthesizing); err != nil {
			return nil, err
		}
		state, err = p.claimedState(ctx, generationID, claimToken)
		if err != nil {
			return nil, err
		}
	}
	if state.Generation.Status != storage.GenerationStatusSynthesizing {
		return nil, &ProcessError{Code: "invalid_generation_state", Message: "The generation cannot be processed from its current state."}
	}
	result, err := p.selectAndSynthesize(ctx, claimToken, state)
	if err != nil {
		return nil, err
	}
	finalizationStarted := time.Now()
	revision, err := p.store.AppendPrivateGenerationResult(ctx, storage.AppendPrivateGenerationResultInput{
		GenerationID: generationID, ClaimToken: claimToken,
		InitialWinnerCandidateID: result.InitialWinnerCandidate.ID,
		ResultCandidateID:        result.ResultCandidate.ID,
	})
	if err != nil {
		outcome := GenerationStageOutcomeFailure
		switch {
		case errors.Is(err, storage.ErrGenerationClaimLost):
			outcome = GenerationStageOutcomeClaimLost
		case errors.Is(err, storage.ErrGenerationCanceled), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			outcome = GenerationStageOutcomeCanceled
		}
		p.observeStage(GenerationStageFinalization, outcome,
			generationStageReasonForError(err), time.Since(finalizationStarted))
		p.observeRevisionAppend(RevisionAppendOriginGeneration, revisionAppendOutcomeForError(err))
		return nil, classifyStorageOperationError(err)
	}
	p.observeStage(GenerationStageFinalization, GenerationStageOutcomeSuccess,
		GenerationStageReasonNone, time.Since(finalizationStarted))
	p.observeRevisionAppend(RevisionAppendOriginGeneration, RevisionAppendOutcomeSuccess)
	result.ResultRevision = *revision
	return result, nil
}

type sourceCandidateResult struct {
	candidateAvailable bool
	retryableFailure   bool
	err                error
}

func (p *Processor) generateIndependentCandidates(ctx context.Context, claimToken string, state *storage.GenerationState) (bool, bool, error) {
	generation := state.Generation
	if len(generation.SelectedSessionIDs) == 0 {
		if candidateByKind(state.Candidates, storage.GenerationCandidateContext) != nil {
			return true, false, nil
		}
		started := time.Now()
		candidate, err := p.generator.GenerateCandidate(ctx, skill.CandidateRequest{
			InputSnapshot: candidateInputSnapshot(generation.Snapshot),
			Name:          generation.Snapshot.Name, SkillType: generation.Snapshot.Type,
			AuthorContext: generation.AuthorContext,
		})
		if ctx.Err() != nil {
			p.observeStage(GenerationStageCandidateGeneration, GenerationStageOutcomeCanceled,
				GenerationStageReasonCanceled, time.Since(started))
			return false, false, ctx.Err()
		}
		if err != nil {
			p.observeStage(GenerationStageCandidateGeneration, GenerationStageOutcomeFailure,
				GenerationStageReasonCandidateGenerationFailed, time.Since(started))
			retryable := skill.IsRetryableExternalError(err)
			if diagnosticErr := p.recordGenerationFailure(ctx, generation, claimToken, "context_candidate_failed", "A context-derived candidate could not be generated.", retryable); diagnosticErr != nil {
				return false, retryable, diagnosticErr
			}
			return false, retryable, nil
		}
		p.observeStage(GenerationStageCandidateGeneration, GenerationStageOutcomeSuccess,
			GenerationStageReasonNone, time.Since(started))
		_, err = p.persistCandidate(ctx, generation, claimToken, 0, storage.GenerationCandidateContext, nil, candidate)
		return err == nil, false, err
	}

	existing := candidatesByOrdinal(state.Candidates)
	candidateAvailable := len(existing) > 0
	jobs := make([]storage.GenerationSessionRecord, 0, len(state.Sessions))
	for _, session := range state.Sessions {
		if session.Status != storage.GenerationSessionPending {
			continue
		}
		if candidate := existing[session.Ordinal]; candidate != nil {
			if err := p.updateSession(ctx, generation.ID, claimToken, session, storage.GenerationSessionCandidateReady, candidate.ID, ""); err != nil {
				return candidateAvailable, false, err
			}
			continue
		}
		jobs = append(jobs, session)
	}

	results := make([]sourceCandidateResult, len(jobs))
	p.runBounded(len(jobs), func(index int) {
		session := jobs[index]
		started := time.Now()
		transcript, loadErr := p.transcript.LoadTranscript(ctx, session.SessionID)
		if ctx.Err() != nil {
			p.observeStage(GenerationStageTranscriptLoad, GenerationStageOutcomeCanceled,
				GenerationStageReasonCanceled, time.Since(started))
			results[index].err = ctx.Err()
			return
		}
		if loadErr != nil {
			p.observeStage(GenerationStageTranscriptLoad, GenerationStageOutcomeFailure,
				GenerationStageReasonTranscriptUnavailable, time.Since(started))
			retryable := skill.IsRetryableExternalError(loadErr)
			p.observePartialSource(PartialSourceStageTranscriptLoad, PartialSourceReasonTranscriptUnavailable)
			if !retryable {
				if err := p.updateSession(ctx, generation.ID, claimToken, session,
					storage.GenerationSessionTranscriptFailed, "", "transcript_unavailable"); err != nil {
					results[index].err = err
					return
				}
			}
			results[index].err = p.diagnostic(ctx, generation.ID, claimToken, session.SessionID, "",
				"transcript", "transcript_unavailable", "The selected session transcript could not be loaded.", retryable)
			results[index].retryableFailure = retryable
			return
		}
		p.observeStage(GenerationStageTranscriptLoad, GenerationStageOutcomeSuccess,
			GenerationStageReasonNone, time.Since(started))

		started = time.Now()
		candidate, generateErr := p.generator.GenerateCandidate(ctx, skill.CandidateRequest{
			InputSnapshot: candidateInputSnapshot(generation.Snapshot),
			Name:          generation.Snapshot.Name, SkillType: generation.Snapshot.Type,
			AuthorContext: generation.AuthorContext,
			Transcript:    transcript, SourceSessionID: session.SessionID,
		})
		if ctx.Err() != nil {
			p.observeStage(GenerationStageCandidateGeneration, GenerationStageOutcomeCanceled,
				GenerationStageReasonCanceled, time.Since(started))
			results[index].err = ctx.Err()
			return
		}
		if generateErr != nil {
			p.observeStage(GenerationStageCandidateGeneration, GenerationStageOutcomeFailure,
				GenerationStageReasonCandidateGenerationFailed, time.Since(started))
			retryable := skill.IsRetryableExternalError(generateErr)
			p.observePartialSource(PartialSourceStageCandidateGeneration, PartialSourceReasonCandidateGenerationFailed)
			if !retryable {
				if err := p.updateSession(ctx, generation.ID, claimToken, session,
					storage.GenerationSessionCandidateFailed, "", "candidate_generation_failed"); err != nil {
					results[index].err = err
					return
				}
			}
			results[index].err = p.diagnostic(ctx, generation.ID, claimToken, session.SessionID, "",
				"candidate", "candidate_generation_failed", "A candidate could not be generated from the selected session.", retryable)
			results[index].retryableFailure = retryable
			return
		}
		p.observeStage(GenerationStageCandidateGeneration, GenerationStageOutcomeSuccess,
			GenerationStageReasonNone, time.Since(started))
		persisted, err := p.persistCandidate(ctx, generation, claimToken, session.Ordinal,
			storage.GenerationCandidateSession, []string{session.SessionID}, candidate)
		if err == nil {
			err = p.updateSession(ctx, generation.ID, claimToken, session,
				storage.GenerationSessionCandidateReady, persisted.ID, "")
		}
		results[index].candidateAvailable = err == nil
		results[index].err = err
	})

	retryableSourceFailure := false
	for _, result := range results {
		candidateAvailable = candidateAvailable || result.candidateAvailable
		retryableSourceFailure = retryableSourceFailure || result.retryableFailure
		if result.err != nil {
			return candidateAvailable, retryableSourceFailure, result.err
		}
	}
	return candidateAvailable, retryableSourceFailure, nil
}

type candidateEvaluationResult struct {
	ranked           *RankedCandidate
	retryableFailure bool
	err              error
}

func (p *Processor) evaluateIndependentCandidates(ctx context.Context, claimToken string, state *storage.GenerationState) ([]RankedCandidate, bool, error) {
	generation := state.Generation
	evaluations := evaluationsByCandidate(state.Evaluations)
	sessions := sessionsByCandidate(state.Sessions)
	viable := make([]RankedCandidate, 0, len(state.Candidates))
	candidates := make([]storage.GenerationCandidateRecord, 0, len(state.Candidates))
	requests := make([]evaluator.CandidateEvaluationRequest, 0, len(state.Candidates))
	for _, candidate := range state.Candidates {
		if candidate.Kind == storage.GenerationCandidateSynthesis {
			continue
		}
		stored := evaluations[candidate.ID]
		if stored == nil {
			request, err := evaluationRequest(generation, candidate)
			if err != nil {
				return nil, false, err
			}
			candidates = append(candidates, candidate)
			requests = append(requests, request)
			continue
		}
		if !allEvaluationIdentitiesMatch(generation, state.Evaluations, candidate.ID) {
			if err := p.rejectEvaluationIdentity(ctx, generation, claimToken, candidate, sessions[candidate.ID], "evaluation"); err != nil {
				return nil, false, err
			}
			continue
		}
		ranked, err := p.rankPersistedEvaluation(ctx, generation, claimToken, candidate, *stored, sessions[candidate.ID])
		if err != nil {
			return nil, false, err
		}
		if ranked != nil {
			viable = append(viable, *ranked)
		}
	}

	results := make([]candidateEvaluationResult, len(candidates))
	p.runBounded(len(candidates), func(index int) {
		candidate := candidates[index]
		started := time.Now()
		evaluation, evaluateErr := p.evaluator.EvaluateCandidate(ctx, requests[index])
		if ctx.Err() != nil {
			p.observeStage(GenerationStageCandidateEvaluation, GenerationStageOutcomeCanceled,
				GenerationStageReasonCanceled, time.Since(started))
			results[index].err = ctx.Err()
			return
		}
		if evaluateErr != nil {
			p.observeStage(GenerationStageCandidateEvaluation, GenerationStageOutcomeFailure,
				GenerationStageReasonCandidateEvaluationFailed, time.Since(started))
			code, message, retryable := evaluatorFailure(evaluateErr)
			p.observePartialSource(PartialSourceStageCandidateEvaluation, PartialSourceReasonCandidateEvaluationFailed)
			if err := p.diagnostic(ctx, generation.ID, claimToken, sessionIDForCandidate(candidate), candidate.ID,
				"evaluation", code, message, retryable); err != nil {
				results[index].err = err
				return
			}
			if !retryable {
				if session := sessions[candidate.ID]; session != nil {
					results[index].err = p.updateSession(ctx, generation.ID, claimToken, *session,
						storage.GenerationSessionEvaluationFailed, candidate.ID, code)
				}
			}
			results[index].retryableFailure = retryable
			return
		}
		p.observeStage(GenerationStageCandidateEvaluation, GenerationStageOutcomeSuccess,
			GenerationStageReasonNone, time.Since(started))
		if !evaluationIdentityMatches(generation, evaluation.Profile, evaluation.ProfileVersion) {
			results[index].err = p.rejectEvaluationIdentity(
				ctx, generation, claimToken, candidate, sessions[candidate.ID], "evaluation",
			)
			return
		}
		persisted, err := p.persistEvaluation(ctx, generation, claimToken, candidate, evaluation)
		if err != nil {
			results[index].err = err
			return
		}
		results[index].ranked, results[index].err = p.rankPersistedEvaluation(
			ctx, generation, claimToken, candidate, *persisted, sessions[candidate.ID],
		)
	})

	retryableFailure := false
	for _, result := range results {
		retryableFailure = retryableFailure || result.retryableFailure
		if result.err != nil {
			return nil, retryableFailure, result.err
		}
		if result.ranked != nil {
			viable = append(viable, *result.ranked)
		}
	}
	return viable, retryableFailure, nil
}

func (p *Processor) rankPersistedEvaluation(
	ctx context.Context,
	generation storage.SkillGenerationRecord,
	claimToken string,
	candidate storage.GenerationCandidateRecord,
	evaluation storage.CandidateEvaluationRecord,
	session *storage.GenerationSessionRecord,
) (*RankedCandidate, error) {
	ranked := rankedCandidate(candidate, evaluation)
	if _, err := RankCandidates(generation.EvaluatorProfile, []RankedCandidate{ranked}); err != nil {
		if diagnosticErr := p.diagnostic(ctx, generation.ID, claimToken, sessionIDForCandidate(candidate), candidate.ID,
			"evaluation", "evaluation_unrankable", "The candidate evaluation could not be ranked.", false); diagnosticErr != nil {
			return nil, diagnosticErr
		}
		if session != nil {
			if updateErr := p.updateSession(ctx, generation.ID, claimToken, *session,
				storage.GenerationSessionEvaluationFailed, candidate.ID, "evaluation_unrankable"); updateErr != nil {
				return nil, updateErr
			}
		}
		return nil, nil
	}
	if session != nil && session.Status != storage.GenerationSessionEvaluated {
		if err := p.updateSession(ctx, generation.ID, claimToken, *session,
			storage.GenerationSessionEvaluated, candidate.ID, ""); err != nil {
			return nil, err
		}
	}
	return &ranked, nil
}

func (p *Processor) selectAndSynthesize(ctx context.Context, claimToken string, state *storage.GenerationState) (*ProcessResult, error) {
	if err := p.diagnosePersistedEvaluationIdentityMismatches(ctx, claimToken, state); err != nil {
		return nil, err
	}
	viable := rankedFromState(*state, false)
	winner, err := SelectWinner(state.Generation.EvaluatorProfile, viable)
	if err != nil {
		return nil, &ProcessError{Code: "no_viable_candidates", Message: "No candidate could be selected."}
	}
	winnerCandidate := candidateByID(state.Candidates, winner.ID)
	winnerEvaluation := evaluationByCandidateID(state.Evaluations, winner.ID)
	if winnerCandidate == nil || winnerEvaluation == nil {
		return nil, errors.New("selected candidate artifacts are incomplete")
	}
	result := &ProcessResult{
		InitialWinnerCandidate: *winnerCandidate, InitialWinnerEvaluation: *winnerEvaluation,
		ResultCandidate: *winnerCandidate, ResultEvaluation: *winnerEvaluation,
	}
	if winnerCandidate.Kind != storage.GenerationCandidateSession {
		return result, nil
	}
	result.SynthesisAttempted = true

	var (
		synthesisStarted time.Time
		synthesisWorked  bool
	)
	startSynthesis := func() {
		if !synthesisWorked {
			synthesisWorked = true
			synthesisStarted = time.Now()
		}
	}
	observeSynthesis := func(outcome GenerationStageOutcome, reason GenerationStageReason) {
		if synthesisWorked {
			p.observeStage(GenerationStageSynthesis, outcome, reason, time.Since(synthesisStarted))
			synthesisWorked = false
		}
	}

	synthesis := candidateByKind(state.Candidates, storage.GenerationCandidateSynthesis)
	if synthesis == nil {
		if hasSynthesisFailure(state.Diagnostics) {
			return result, nil
		}
		startSynthesis()
		request := synthesisRequest(state.Generation, *winnerCandidate, state.Candidates, state.Evaluations)
		generated, generateErr := p.generator.SynthesizeCandidate(ctx, request)
		if generateErr != nil {
			if ctx.Err() != nil {
				observeSynthesis(GenerationStageOutcomeCanceled, GenerationStageReasonCanceled)
				return nil, ctx.Err()
			}
			if err := p.diagnostic(ctx, state.Generation.ID, claimToken, "", winnerCandidate.ID,
				"synthesis", "synthesis_generation_failed", "The optional synthesis candidate could not be generated.", false); err != nil {
				observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonStorage)
				return nil, err
			}
			observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonSynthesisFailed)
			return result, nil
		}
		persisted, persistErr := p.persistCandidate(ctx, state.Generation, claimToken,
			nextCandidateOrdinal(state.Candidates), storage.GenerationCandidateSynthesis,
			effectiveEvaluatedSources(state.Generation, state.Candidates, state.Evaluations), generated)
		if persistErr != nil {
			observeSynthesis(GenerationStageOutcomeFailure, generationStageReasonForError(persistErr))
			return nil, persistErr
		}
		synthesis = persisted
	}

	synthesisEvaluation := evaluationByCandidateID(state.Evaluations, synthesis.ID)
	if synthesisEvaluation == nil {
		if hasSynthesisEvaluationFailure(state.Diagnostics, synthesis.ID) {
			observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonSynthesisFailed)
			return result, nil
		}
		startSynthesis()
		request, requestErr := evaluationRequest(state.Generation, *synthesis)
		if requestErr != nil {
			observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonSynthesisFailed)
			return nil, requestErr
		}
		evaluation, evaluateErr := p.evaluator.EvaluateCandidate(ctx, request)
		if evaluateErr != nil {
			if ctx.Err() != nil {
				observeSynthesis(GenerationStageOutcomeCanceled, GenerationStageReasonCanceled)
				return nil, ctx.Err()
			}
			code, message, retryable := evaluatorFailure(evaluateErr)
			if err := p.diagnostic(ctx, state.Generation.ID, claimToken, "", synthesis.ID,
				"synthesis", "synthesis_"+code, message, retryable); err != nil {
				observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonStorage)
				return nil, err
			}
			observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonSynthesisFailed)
			return result, nil
		}
		if !evaluationIdentityMatches(state.Generation, evaluation.Profile, evaluation.ProfileVersion) {
			if err := p.rejectEvaluationIdentity(ctx, state.Generation, claimToken, *synthesis, nil, "synthesis"); err != nil {
				observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonStorage)
				return nil, err
			}
			observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonSynthesisFailed)
			return result, nil
		}
		persisted, persistErr := p.persistEvaluation(ctx, state.Generation, claimToken, *synthesis, evaluation)
		if persistErr != nil {
			observeSynthesis(GenerationStageOutcomeFailure, generationStageReasonForError(persistErr))
			return nil, persistErr
		}
		synthesisEvaluation = persisted
		state.Evaluations = append(state.Evaluations, *persisted)
	}
	if !allEvaluationIdentitiesMatch(state.Generation, state.Evaluations, synthesis.ID) {
		if err := p.rejectEvaluationIdentity(ctx, state.Generation, claimToken, *synthesis, nil, "synthesis"); err != nil {
			observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonStorage)
			return nil, err
		}
		observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonSynthesisFailed)
		return result, nil
	}
	all := append(viable, rankedCandidate(*synthesis, *synthesisEvaluation))
	selected, err := SelectWinner(state.Generation.EvaluatorProfile, all)
	if err != nil {
		observeSynthesis(GenerationStageOutcomeFailure, GenerationStageReasonSynthesisFailed)
		return nil, err
	}
	if selected.ID == synthesis.ID {
		result.ResultCandidate = *synthesis
		result.ResultEvaluation = *synthesisEvaluation
	}
	observeSynthesis(GenerationStageOutcomeSuccess, GenerationStageReasonNone)
	return result, nil
}

func (p *Processor) persistCandidate(ctx context.Context, generation storage.SkillGenerationRecord, claimToken string, ordinal int, kind storage.GenerationCandidateKind, sources []string, candidate *skill.Candidate) (*storage.GenerationCandidateRecord, error) {
	insights, err := json.Marshal(candidate.Insights)
	if err != nil {
		return nil, err
	}
	snapshot := storage.GenerationCandidateSnapshot{
		Name: candidate.Skill.Name, Description: candidate.Skill.Description,
		Type: candidate.Skill.Type, Tags: append([]string(nil), candidate.Skill.Tags...), Content: candidate.Skill.Content,
		IsAIGenerated: true,
	}
	hash, err := storage.GenerationCandidateBundleSHA256(snapshot, sources, insights)
	if err != nil {
		return nil, err
	}
	persisted, err := p.store.PutGenerationCandidate(ctx, generation.ID, claimToken, storage.GenerationCandidateRecord{
		ID: deterministicID(generation.ID, "candidate", strconv.Itoa(ordinal)), Ordinal: ordinal, Kind: kind,
		SourceSessionIDs: append([]string(nil), sources...), Snapshot: snapshot, Insights: insights,
		BundleSHA256: hash,
	})
	if err != nil {
		return nil, classifyStorageOperationError(err)
	}
	if persisted == nil {
		return nil, &ProcessError{Code: "invalid_generation_artifact", Message: "Generation storage returned an invalid candidate."}
	}
	return persisted, nil
}

func (p *Processor) persistEvaluation(ctx context.Context, generation storage.SkillGenerationRecord, claimToken string, candidate storage.GenerationCandidateRecord, result evaluator.CandidateEvaluation) (*storage.CandidateEvaluationRecord, error) {
	if !evaluationIdentityMatches(generation, result.Profile, result.ProfileVersion) {
		return nil, &ProcessError{Code: "evaluation_profile_mismatch", Message: "The candidate evaluation did not match the generation rubric."}
	}
	requestHash, err := storage.GenerationCandidateEvaluationRequestSHA256(generation, candidate)
	if err != nil {
		return nil, err
	}
	persisted, err := p.store.PutCandidateEvaluation(ctx, generation.ID, claimToken, storage.CandidateEvaluationRecord{
		ID: deterministicID(generation.ID, "evaluation", candidate.ID), CandidateID: candidate.ID,
		RequestSHA256: requestHash, Profile: result.Profile, ProfileVersion: result.ProfileVersion,
		EvaluatorVersion: result.EvaluatorVersion, Score: result.Score, Decision: result.Decision,
		CriticalFindingCount: result.CriticalFindingCount, WarningFindingCount: result.WarningFindingCount,
		CriterionResults: result.CriterionResults, Findings: result.Findings, Strengths: result.Strengths,
	})
	if err != nil {
		return nil, classifyStorageOperationError(err)
	}
	if persisted == nil {
		return nil, &ProcessError{Code: "invalid_generation_artifact", Message: "Generation storage returned an invalid evaluation."}
	}
	return persisted, nil
}

func (p *Processor) rejectEvaluationIdentity(
	ctx context.Context,
	generation storage.SkillGenerationRecord,
	claimToken string,
	candidate storage.GenerationCandidateRecord,
	session *storage.GenerationSessionRecord,
	stage string,
) error {
	code := "evaluation_profile_mismatch"
	if stage == "synthesis" {
		code = "synthesis_evaluation_profile_mismatch"
	}
	if err := p.diagnostic(ctx, generation.ID, claimToken, sessionIDForCandidate(candidate), candidate.ID,
		stage, code, "The candidate evaluation did not match the generation rubric.", false); err != nil {
		return err
	}
	if session != nil && session.Status != storage.GenerationSessionEvaluationFailed {
		return p.updateSession(ctx, generation.ID, claimToken, *session,
			storage.GenerationSessionEvaluationFailed, candidate.ID, code)
	}
	return nil
}

func (p *Processor) diagnosePersistedEvaluationIdentityMismatches(
	ctx context.Context,
	claimToken string,
	state *storage.GenerationState,
) error {
	sessions := sessionsByCandidate(state.Sessions)
	for _, candidate := range state.Candidates {
		evaluation := evaluationByCandidateID(state.Evaluations, candidate.ID)
		if evaluation == nil || allEvaluationIdentitiesMatch(state.Generation, state.Evaluations, candidate.ID) {
			continue
		}
		stage := "evaluation"
		var session *storage.GenerationSessionRecord
		if candidate.Kind == storage.GenerationCandidateSynthesis {
			stage = "synthesis"
		} else {
			session = sessions[candidate.ID]
		}
		if err := p.rejectEvaluationIdentity(ctx, state.Generation, claimToken, candidate, session, stage); err != nil {
			return err
		}
	}
	return nil
}

func (p *Processor) diagnostic(ctx context.Context, generationID, claimToken, sessionID, candidateID, stage, code, message string, retryable bool) error {
	_, err := p.store.PutGenerationDiagnostic(ctx, generationID, claimToken, storage.GenerationDiagnosticRecord{
		ID:        deterministicID(generationID, "diagnostic", stage, sessionID, candidateID, code),
		SessionID: sessionID, CandidateID: candidateID, Stage: stage, Code: code,
		Message: message, Retryable: retryable,
	})
	return classifyStorageOperationError(err)
}

func (p *Processor) recordGenerationFailure(ctx context.Context, generation storage.SkillGenerationRecord, claimToken, code, message string, retryable bool) error {
	if err := p.diagnostic(ctx, generation.ID, claimToken, "", "", "candidate", code, message, retryable); err != nil {
		return err
	}
	return nil
}

func (p *Processor) updateSession(ctx context.Context, generationID, claimToken string, session storage.GenerationSessionRecord, status storage.GenerationSessionStatus, candidateID, diagnosticCode string) error {
	session.Status = status
	session.CandidateID = candidateID
	session.DiagnosticCode = diagnosticCode
	return classifyStorageOperationError(
		p.store.UpdateGenerationSession(ctx, generationID, claimToken, session),
	)
}

func (p *Processor) claimedState(ctx context.Context, generationID, claimToken string) (*storage.GenerationState, error) {
	state, err := p.store.GetClaimedGeneration(ctx, generationID, claimToken)
	if err != nil {
		return nil, classifyStorageOperationError(err)
	}
	if state == nil {
		return nil, &ProcessError{Code: "invalid_generation_state", Message: "Generation storage returned no claimed state."}
	}
	return state, nil
}

func (p *Processor) transition(ctx context.Context, generationID, claimToken string, from, to storage.GenerationStatus) error {
	return classifyStorageOperationError(
		p.store.UpdateGenerationStatus(ctx, generationID, claimToken, from, to),
	)
}

func evaluationRequest(generation storage.SkillGenerationRecord, candidate storage.GenerationCandidateRecord) (evaluator.CandidateEvaluationRequest, error) {
	var criteria []evaluator.Criterion
	if err := json.Unmarshal(generation.EvaluationCriteria, &criteria); err != nil || len(criteria) == 0 {
		return evaluator.CandidateEvaluationRequest{}, &ProcessError{
			Code: "invalid_evaluation_criteria", Message: "The generation ranking criteria are invalid.",
		}
	}
	baseline := evaluatorBundle(generation.Snapshot)
	return evaluator.CandidateEvaluationRequest{
		Ref: deterministicID(generation.ID, "ref", candidate.ID), Name: candidate.Snapshot.Name,
		Candidate: candidateEvaluatorBundle(candidate), Baseline: &baseline,
		AuthorContext: generation.AuthorContext, EvidenceSessionIDs: append([]string(nil), candidate.SourceSessionIDs...),
		OwnerSubject: generation.CreatorSubject, Profile: generation.EvaluatorProfile,
		ProfileVersion: generation.EvaluatorProfileVersion, Criteria: criteria,
	}, nil
}

func evaluatorBundle(snapshot storage.SkillRevisionSnapshot) evaluator.CandidateBundle {
	return evaluator.CandidateBundle{
		Name: snapshot.Name, Description: snapshot.Description, Type: snapshot.Type,
		Tags: append([]string(nil), snapshot.Tags...), Content: snapshot.Content,
		SourceSessionIDs: append([]string(nil), snapshot.SourceSessionIDs...),
	}
}

func candidateEvaluatorBundle(candidate storage.GenerationCandidateRecord) evaluator.CandidateBundle {
	return evaluator.CandidateBundle{
		Name: candidate.Snapshot.Name, Description: candidate.Snapshot.Description,
		Type: candidate.Snapshot.Type, Tags: append([]string(nil), candidate.Snapshot.Tags...),
		Content:          candidate.Snapshot.Content,
		SourceSessionIDs: append([]string(nil), candidate.SourceSessionIDs...),
	}
}

func synthesisRequest(generation storage.SkillGenerationRecord, winner storage.GenerationCandidateRecord, candidates []storage.GenerationCandidateRecord, evaluations []storage.CandidateEvaluationRecord) skill.SynthesisRequest {
	candidateByID := make(map[string]storage.GenerationCandidateRecord, len(candidates))
	rankable := make([]RankedCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Kind == storage.GenerationCandidateSynthesis {
			continue
		}
		evaluation := evaluationByCandidateID(evaluations, candidate.ID)
		if evaluation == nil || !allEvaluationIdentitiesMatch(generation, evaluations, candidate.ID) {
			continue
		}
		ranked := rankedCandidate(candidate, *evaluation)
		if _, err := RankCandidates(generation.EvaluatorProfile, []RankedCandidate{ranked}); err != nil {
			continue
		}
		candidateByID[candidate.ID] = candidate
		rankable = append(rankable, ranked)
	}
	ordered, err := RankCandidates(generation.EvaluatorProfile, rankable)
	if err != nil {
		ordered = nil
	}
	if len(ordered) > maxSynthesisFeedbackInputs {
		ordered = ordered[:maxSynthesisFeedbackInputs]
	}
	feedback := make([]skill.SynthesisFeedback, 0, len(ordered))
	for _, ranked := range ordered {
		candidate := candidateByID[ranked.ID]
		evaluation := evaluationByCandidateID(evaluations, candidate.ID)
		var insights []skill.CandidateInsight
		_ = json.Unmarshal(candidate.Insights, &insights)
		feedback = append(feedback, skill.SynthesisFeedback{
			Insights:  insights,
			Findings:  append(json.RawMessage(nil), evaluation.Findings...),
			Strengths: append(json.RawMessage(nil), evaluation.Strengths...),
		})
	}
	return skill.SynthesisRequest{
		InputSnapshot: candidateInputSnapshot(generation.Snapshot),
		Name:          generation.Snapshot.Name, SkillType: generation.Snapshot.Type,
		AuthorContext: generation.AuthorContext, Winner: skillCandidate(winner), Feedback: feedback,
	}
}

func candidateInputSnapshot(snapshot storage.SkillRevisionSnapshot) skill.CandidateInputSnapshot {
	return skill.CandidateInputSnapshot{
		Name: snapshot.Name, Description: snapshot.Description, Type: snapshot.Type,
		Tags: append([]string{}, snapshot.Tags...), Content: snapshot.Content,
		IsAIGenerated:    snapshot.IsAIGenerated,
		SourceSessionIDs: append([]string{}, snapshot.SourceSessionIDs...),
	}
}

func skillCandidate(candidate storage.GenerationCandidateRecord) skill.Candidate {
	var insights []skill.CandidateInsight
	_ = json.Unmarshal(candidate.Insights, &insights)
	return skill.Candidate{
		Skill: skill.Skill{
			Name: candidate.Snapshot.Name, Description: candidate.Snapshot.Description,
			Type: candidate.Snapshot.Type, Tags: append([]string(nil), candidate.Snapshot.Tags...),
			Content: candidate.Snapshot.Content, Sessions: append([]string(nil), candidate.SourceSessionIDs...),
		},
		Insights: insights,
	}
}

func rankedCandidate(candidate storage.GenerationCandidateRecord, evaluation storage.CandidateEvaluationRecord) RankedCandidate {
	return RankedCandidate{ID: candidate.ID, Ordinal: candidate.Ordinal, Evaluation: evaluator.CandidateEvaluation{
		Profile: evaluation.Profile, ProfileVersion: evaluation.ProfileVersion,
		Score: evaluation.Score, Decision: evaluation.Decision,
		CriticalFindingCount: evaluation.CriticalFindingCount, WarningFindingCount: evaluation.WarningFindingCount,
	}}
}

func rankedFromState(state storage.GenerationState, includeSynthesis bool) []RankedCandidate {
	result := make([]RankedCandidate, 0, len(state.Evaluations))
	for _, candidate := range state.Candidates {
		if !includeSynthesis && candidate.Kind == storage.GenerationCandidateSynthesis {
			continue
		}
		if evaluation := evaluationByCandidateID(state.Evaluations, candidate.ID); evaluation != nil &&
			allEvaluationIdentitiesMatch(state.Generation, state.Evaluations, candidate.ID) {
			ranked := rankedCandidate(candidate, *evaluation)
			if _, err := RankCandidates(state.Generation.EvaluatorProfile, []RankedCandidate{ranked}); err == nil {
				result = append(result, ranked)
			}
		}
	}
	return result
}

func evaluationIdentityMatches(generation storage.SkillGenerationRecord, profile, version string) bool {
	return generation.EvaluatorProfile != "" && generation.EvaluatorProfileVersion != "" &&
		profile == generation.EvaluatorProfile && version == generation.EvaluatorProfileVersion
}

func allEvaluationIdentitiesMatch(
	generation storage.SkillGenerationRecord,
	evaluations []storage.CandidateEvaluationRecord,
	candidateID string,
) bool {
	found := false
	for _, evaluation := range evaluations {
		if evaluation.CandidateID != candidateID {
			continue
		}
		found = true
		if !evaluationIdentityMatches(generation, evaluation.Profile, evaluation.ProfileVersion) {
			return false
		}
	}
	return found
}

func (p *Processor) runBounded(total int, run func(int)) {
	if total <= 0 {
		return
	}
	concurrency := p.config.CandidateConcurrency
	if concurrency <= 0 {
		concurrency = DefaultProcessorConfig().CandidateConcurrency
	}
	if concurrency > total {
		concurrency = total
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for range concurrency {
		go func() {
			defer workers.Done()
			for index := range jobs {
				run(index)
			}
		}()
	}
	for index := range total {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
}

func (p *Processor) observeStage(stage GenerationStage, outcome GenerationStageOutcome, reason GenerationStageReason, duration time.Duration) {
	if p.recorder != nil {
		p.recorder.ObserveStage(stage, outcome, reason, duration)
	}
}

func (p *Processor) observePartialSource(stage PartialSourceStage, reason PartialSourceReason) {
	if p.recorder != nil {
		p.recorder.ObservePartialSource(stage, reason)
	}
}

func (p *Processor) observeRevisionAppend(origin RevisionAppendOrigin, outcome RevisionAppendOutcome) {
	if p.recorder != nil {
		p.recorder.ObserveRevisionAppend(origin, outcome)
	}
}

func generationStageReasonForError(err error) GenerationStageReason {
	switch {
	case errors.Is(err, storage.ErrGenerationClaimLost):
		return GenerationStageReasonClaimLost
	case errors.Is(err, storage.ErrGenerationCanceled), errors.Is(err, context.Canceled):
		return GenerationStageReasonCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return GenerationStageReasonTimeout
	default:
		return GenerationStageReasonStorage
	}
}

func revisionAppendOutcomeForError(err error) RevisionAppendOutcome {
	switch {
	case errors.Is(err, storage.ErrRevisionConflict):
		return RevisionAppendOutcomeConflict
	case errors.Is(err, storage.ErrInvalidGenerationState), errors.Is(err, storage.ErrRevisionLineageInvalid),
		errors.Is(err, storage.ErrRevisionSnapshotInvalid):
		return RevisionAppendOutcomeInvalid
	case errors.Is(err, storage.ErrRevisionNotFound), errors.Is(err, storage.ErrSkillNotFound):
		return RevisionAppendOutcomeNotFound
	case errors.Is(err, storage.ErrGenerationClaimLost):
		return RevisionAppendOutcomeClaimLost
	default:
		return RevisionAppendOutcomeError
	}
}

// classifyStorageOperationError makes ordinary database/network failures
// resumable without weakening fencing or retrying deterministic invariants.
func classifyStorageOperationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrGenerationClaimLost) || errors.Is(err, storage.ErrGenerationCanceled) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, storage.ErrInvalidGenerationState),
		errors.Is(err, storage.ErrGenerationNotFound),
		errors.Is(err, storage.ErrSkillNotFound),
		errors.Is(err, storage.ErrRevisionNotFound),
		errors.Is(err, storage.ErrRevisionLineageInvalid),
		errors.Is(err, storage.ErrRevisionConflict),
		errors.Is(err, storage.ErrRevisionSnapshotInvalid):
		return &ProcessError{
			Code: "generation_storage_invariant", Message: "Generation storage rejected an invalid state transition.",
			cause: err,
		}
	default:
		return &ProcessError{
			Code:    "generation_storage_temporarily_unavailable",
			Message: "Generation storage is temporarily unavailable.", Retryable: true, cause: err,
		}
	}
}

func evaluatorFailure(err error) (string, string, bool) {
	var call *evaluator.CallError
	if errors.As(err, &call) {
		switch call.Code {
		case "evaluator_rate_limited":
			return call.Code, "Candidate evaluation is temporarily rate limited.", true
		case "evaluator_unavailable":
			return call.Code, "Candidate evaluation is temporarily unavailable.", true
		case "evaluator_canceled":
			return call.Code, "Candidate evaluation was canceled.", false
		case "evaluator_rejected":
			return call.Code, "The evaluator rejected this candidate.", false
		case "evaluator_invalid_response":
			return call.Code, "The evaluator returned an invalid candidate judgment.", false
		}
	}
	return "candidate_evaluation_failed", "The candidate could not be evaluated.", false
}

func deterministicID(parts ...string) string {
	encoded, _ := json.Marshal(parts)
	return uuid.NewSHA1(uuid.NameSpaceOID, encoded).String()
}

func candidatesByOrdinal(candidates []storage.GenerationCandidateRecord) map[int]*storage.GenerationCandidateRecord {
	result := make(map[int]*storage.GenerationCandidateRecord, len(candidates))
	for i := range candidates {
		candidate := candidates[i]
		result[candidate.Ordinal] = &candidate
	}
	return result
}

func evaluationsByCandidate(evaluations []storage.CandidateEvaluationRecord) map[string]*storage.CandidateEvaluationRecord {
	result := make(map[string]*storage.CandidateEvaluationRecord, len(evaluations))
	for i := range evaluations {
		evaluation := evaluations[i]
		result[evaluation.CandidateID] = &evaluation
	}
	return result
}

func sessionsByCandidate(sessions []storage.GenerationSessionRecord) map[string]*storage.GenerationSessionRecord {
	result := make(map[string]*storage.GenerationSessionRecord, len(sessions))
	for i := range sessions {
		session := sessions[i]
		if session.CandidateID != "" {
			result[session.CandidateID] = &session
		}
	}
	return result
}

func candidateByID(candidates []storage.GenerationCandidateRecord, id string) *storage.GenerationCandidateRecord {
	for i := range candidates {
		if candidates[i].ID == id {
			candidate := candidates[i]
			return &candidate
		}
	}
	return nil
}

func candidateByKind(candidates []storage.GenerationCandidateRecord, kind storage.GenerationCandidateKind) *storage.GenerationCandidateRecord {
	for i := range candidates {
		if candidates[i].Kind == kind {
			candidate := candidates[i]
			return &candidate
		}
	}
	return nil
}

func evaluationByCandidateID(evaluations []storage.CandidateEvaluationRecord, candidateID string) *storage.CandidateEvaluationRecord {
	for i := range evaluations {
		if evaluations[i].CandidateID == candidateID {
			evaluation := evaluations[i]
			return &evaluation
		}
	}
	return nil
}

func sessionIDForCandidate(candidate storage.GenerationCandidateRecord) string {
	if len(candidate.SourceSessionIDs) == 1 {
		return candidate.SourceSessionIDs[0]
	}
	return ""
}

func nextCandidateOrdinal(candidates []storage.GenerationCandidateRecord) int {
	next := 0
	for _, candidate := range candidates {
		if candidate.Ordinal >= next {
			next = candidate.Ordinal + 1
		}
	}
	return next
}

func effectiveEvaluatedSources(generation storage.SkillGenerationRecord, candidates []storage.GenerationCandidateRecord, evaluations []storage.CandidateEvaluationRecord) []string {
	sorted := append([]storage.GenerationCandidateRecord(nil), candidates...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Ordinal < sorted[j].Ordinal })
	seen := make(map[string]struct{})
	result := make([]string, 0)
	for _, candidate := range sorted {
		if candidate.Kind == storage.GenerationCandidateSynthesis {
			continue
		}
		evaluation := evaluationByCandidateID(evaluations, candidate.ID)
		if evaluation == nil || !allEvaluationIdentitiesMatch(generation, evaluations, candidate.ID) {
			continue
		}
		if _, err := RankCandidates(generation.EvaluatorProfile, []RankedCandidate{rankedCandidate(candidate, *evaluation)}); err != nil {
			continue
		}
		for _, sessionID := range candidate.SourceSessionIDs {
			if _, ok := seen[sessionID]; ok {
				continue
			}
			seen[sessionID] = struct{}{}
			result = append(result, sessionID)
		}
	}
	return result
}

func hasSynthesisFailure(diagnostics []storage.GenerationDiagnosticRecord) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Stage == "synthesis" && diagnostic.Code == "synthesis_generation_failed" {
			return true
		}
	}
	return false
}

func hasSynthesisEvaluationFailure(diagnostics []storage.GenerationDiagnosticRecord, candidateID string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Stage == "synthesis" && diagnostic.CandidateID == candidateID && diagnostic.Code != "synthesis_generation_failed" {
			return true
		}
	}
	return false
}
