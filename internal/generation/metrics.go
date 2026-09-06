package generation

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	MetricGenerationWorkerClaimsTotal        = "skills_generation_worker_claims_total"
	MetricGenerationStageTotal               = "skills_generation_stage_total"
	MetricGenerationStageDurationSeconds     = "skills_generation_stage_duration_seconds"
	MetricGenerationQueueDepth               = "skills_generation_queue_depth"
	MetricGenerationQueueLagSeconds          = "skills_generation_queue_lag_seconds"
	MetricGenerationQueueMonitorPollFailures = "skills_generation_queue_monitor_consecutive_poll_failures"
	MetricGenerationWorkerReady              = "skills_generation_worker_ready"
	MetricGenerationActiveWorkers            = "skills_generation_active_workers"
	MetricGenerationLeaseLostTotal           = "skills_generation_lease_lost_total"
	MetricGenerationPartialSourcesTotal      = "skills_generation_partial_sources_total"
	MetricGenerationConsecutivePollFailures  = "skills_generation_worker_consecutive_poll_failures"
	MetricRevisionAppendTotal                = "skills_revision_append_total"
	MetricRevisionVisibilityTotal            = "skills_revision_visibility_total"
	MetricLatestMutationTotal                = "skills_latest_mutation_total"
)

// WorkerClaimResult is the complete result-label vocabulary for
// skills_generation_worker_claims_total.
type WorkerClaimResult string

const (
	WorkerClaimResultClaimed WorkerClaimResult = "claimed"
	WorkerClaimResultEmpty   WorkerClaimResult = "empty"
	WorkerClaimResultError   WorkerClaimResult = "error"
	WorkerClaimResultUnknown WorkerClaimResult = "unknown"
)

// GenerationStage is the complete stage-label vocabulary shared by the stage
// count and duration families.
type GenerationStage string

const (
	GenerationStageTranscriptLoad      GenerationStage = "transcript_load"
	GenerationStageCandidateGeneration GenerationStage = "candidate_generation"
	GenerationStageCandidateEvaluation GenerationStage = "candidate_evaluation"
	GenerationStageSynthesis           GenerationStage = "synthesis"
	GenerationStageFinalization        GenerationStage = "finalization"
	GenerationStageUnknown             GenerationStage = "unknown"
)

// GenerationStageOutcome is the operation-specific outcome vocabulary for
// skills_generation_stage_total.
type GenerationStageOutcome string

const (
	GenerationStageOutcomeSuccess         GenerationStageOutcome = "success"
	GenerationStageOutcomeFailure         GenerationStageOutcome = "failure"
	GenerationStageOutcomeCanceled        GenerationStageOutcome = "canceled"
	GenerationStageOutcomeRequeued        GenerationStageOutcome = "requeued"
	GenerationStageOutcomeClaimLost       GenerationStageOutcome = "claim_lost"
	GenerationStageOutcomeResourceLimited GenerationStageOutcome = "resource_limited"
	GenerationStageOutcomeUnknown         GenerationStageOutcome = "unknown"
)

// GenerationStageReason is the finite reason-label vocabulary for generation
// stages. Raw errors and resource identities have no representation here.
type GenerationStageReason string

const (
	GenerationStageReasonNone                      GenerationStageReason = "none"
	GenerationStageReasonStorage                   GenerationStageReason = "storage"
	GenerationStageReasonTransient                 GenerationStageReason = "transient"
	GenerationStageReasonTerminal                  GenerationStageReason = "terminal"
	GenerationStageReasonTimeout                   GenerationStageReason = "timeout"
	GenerationStageReasonAttemptsExhausted         GenerationStageReason = "attempts_exhausted"
	GenerationStageReasonTranscriptUnavailable     GenerationStageReason = "transcript_unavailable"
	GenerationStageReasonCandidateGenerationFailed GenerationStageReason = "candidate_generation_failed"
	GenerationStageReasonCandidateEvaluationFailed GenerationStageReason = "candidate_evaluation_failed"
	GenerationStageReasonSynthesisFailed           GenerationStageReason = "synthesis_failed"
	GenerationStageReasonNoViableCandidates        GenerationStageReason = "no_viable_candidates"
	GenerationStageReasonCanceled                  GenerationStageReason = "canceled"
	GenerationStageReasonClaimLost                 GenerationStageReason = "claim_lost"
	GenerationStageReasonResourceLimited           GenerationStageReason = "resource_limited"
	GenerationStageReasonUnknown                   GenerationStageReason = "unknown"
)

// PartialSourceStage is the source-isolation stage vocabulary for
// skills_generation_partial_sources_total.
type PartialSourceStage string

const (
	PartialSourceStageTranscriptLoad      PartialSourceStage = "transcript_load"
	PartialSourceStageCandidateGeneration PartialSourceStage = "candidate_generation"
	PartialSourceStageCandidateEvaluation PartialSourceStage = "candidate_evaluation"
	PartialSourceStageUnknown             PartialSourceStage = "unknown"
)

// PartialSourceReason is the source-isolation reason vocabulary. It is
// intentionally narrower than GenerationStageReason.
type PartialSourceReason string

const (
	PartialSourceReasonTranscriptUnavailable     PartialSourceReason = "transcript_unavailable"
	PartialSourceReasonCandidateGenerationFailed PartialSourceReason = "candidate_generation_failed"
	PartialSourceReasonCandidateEvaluationFailed PartialSourceReason = "candidate_evaluation_failed"
	PartialSourceReasonUnknown                   PartialSourceReason = "unknown"
)

// RevisionAppendOrigin is the origin-label vocabulary for
// skills_revision_append_total.
type RevisionAppendOrigin string

const (
	RevisionAppendOriginManual     RevisionAppendOrigin = "manual"
	RevisionAppendOriginGeneration RevisionAppendOrigin = "generation"
	RevisionAppendOriginDuplicate  RevisionAppendOrigin = "duplicate"
	RevisionAppendOriginMigrated   RevisionAppendOrigin = "migrated"
	RevisionAppendOriginUnknown    RevisionAppendOrigin = "unknown"
)

// RevisionAppendOutcome is the outcome-label vocabulary for revision appends.
type RevisionAppendOutcome string

const (
	RevisionAppendOutcomeSuccess   RevisionAppendOutcome = "success"
	RevisionAppendOutcomeConflict  RevisionAppendOutcome = "conflict"
	RevisionAppendOutcomeInvalid   RevisionAppendOutcome = "invalid"
	RevisionAppendOutcomeNotFound  RevisionAppendOutcome = "not_found"
	RevisionAppendOutcomeClaimLost RevisionAppendOutcome = "claim_lost"
	RevisionAppendOutcomeError     RevisionAppendOutcome = "error"
	RevisionAppendOutcomeUnknown   RevisionAppendOutcome = "unknown"
)

// RevisionVisibilityTransition is the transition-label vocabulary for
// skills_revision_visibility_total.
type RevisionVisibilityTransition string

const (
	RevisionVisibilityTransitionPrivateToPublic RevisionVisibilityTransition = "private_to_public"
	RevisionVisibilityTransitionPublicToPrivate RevisionVisibilityTransition = "public_to_private"
	RevisionVisibilityTransitionUnchanged       RevisionVisibilityTransition = "unchanged"
	RevisionVisibilityTransitionUnknown         RevisionVisibilityTransition = "unknown"
)

// RevisionVisibilityOutcome is the outcome-label vocabulary for visibility
// mutations.
type RevisionVisibilityOutcome string

const (
	RevisionVisibilityOutcomeSuccess        RevisionVisibilityOutcome = "success"
	RevisionVisibilityOutcomeNotFound       RevisionVisibilityOutcome = "not_found"
	RevisionVisibilityOutcomeLatestConflict RevisionVisibilityOutcome = "latest_conflict"
	RevisionVisibilityOutcomeError          RevisionVisibilityOutcome = "error"
	RevisionVisibilityOutcomeUnknown        RevisionVisibilityOutcome = "unknown"
)

// LatestMutationOperation is the operation-label vocabulary for
// skills_latest_mutation_total.
type LatestMutationOperation string

const (
	LatestMutationOperationSet     LatestMutationOperation = "set"
	LatestMutationOperationClear   LatestMutationOperation = "clear"
	LatestMutationOperationUnknown LatestMutationOperation = "unknown"
)

// LatestMutationOutcome is the outcome-label vocabulary for explicit-latest
// mutations.
type LatestMutationOutcome string

const (
	LatestMutationOutcomeSuccess           LatestMutationOutcome = "success"
	LatestMutationOutcomeRevisionNotFound  LatestMutationOutcome = "revision_not_found"
	LatestMutationOutcomeRevisionNotPublic LatestMutationOutcome = "revision_not_public"
	LatestMutationOutcomeError             LatestMutationOutcome = "error"
	LatestMutationOutcomeUnknown           LatestMutationOutcome = "unknown"
)

type generationStageLabels struct {
	Stage   GenerationStage
	Outcome GenerationStageOutcome
	Reason  GenerationStageReason
}

type partialSourceLabels struct {
	Stage  PartialSourceStage
	Reason PartialSourceReason
}

type revisionAppendLabels struct {
	Origin  RevisionAppendOrigin
	Outcome RevisionAppendOutcome
}

type revisionVisibilityLabels struct {
	Transition RevisionVisibilityTransition
	Outcome    RevisionVisibilityOutcome
}

type latestMutationLabels struct {
	Operation LatestMutationOperation
	Outcome   LatestMutationOutcome
}

// Recorder is the fixed-cardinality lifecycle metric surface injected into
// workers, processors, and revision metadata handlers.
type Recorder interface {
	SetQueue(depth int64, lag time.Duration)
	SetQueueMonitorPollFailures(failures int64)
	SetWorkerReady(ready bool)
	SetActiveWorkers(active int64)
	ObserveWorkerClaim(result WorkerClaimResult)
	ObserveStage(stage GenerationStage, outcome GenerationStageOutcome, reason GenerationStageReason, duration time.Duration)
	ObserveLeaseLost()
	ObservePartialSource(stage PartialSourceStage, reason PartialSourceReason)
	SetConsecutivePollFailures(failures int64)
	ObserveRevisionAppend(origin RevisionAppendOrigin, outcome RevisionAppendOutcome)
	ObserveRevisionVisibility(transition RevisionVisibilityTransition, outcome RevisionVisibilityOutcome)
	ObserveLatestMutation(operation LatestMutationOperation, outcome LatestMutationOutcome)
}

// Metrics is the in-process fixed-cardinality registry.
type Metrics struct {
	mu sync.Mutex

	workerClaims             map[WorkerClaimResult]uint64
	stageTotals              map[generationStageLabels]uint64
	stageDuration            map[GenerationStage]time.Duration
	stageDurationCount       map[GenerationStage]uint64
	queueDepth               int64
	queueLag                 time.Duration
	queueMonitorPollFailures int64
	workerReady              int64
	activeWorkers            int64
	leaseLostTotal           uint64
	partialSources           map[partialSourceLabels]uint64
	consecutivePollFailures  int64
	revisionAppends          map[revisionAppendLabels]uint64
	revisionVisibility       map[revisionVisibilityLabels]uint64
	latestMutations          map[latestMutationLabels]uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		workerClaims:       make(map[WorkerClaimResult]uint64),
		stageTotals:        make(map[generationStageLabels]uint64),
		stageDuration:      make(map[GenerationStage]time.Duration),
		stageDurationCount: make(map[GenerationStage]uint64),
		partialSources:     make(map[partialSourceLabels]uint64),
		revisionAppends:    make(map[revisionAppendLabels]uint64),
		revisionVisibility: make(map[revisionVisibilityLabels]uint64),
		latestMutations:    make(map[latestMutationLabels]uint64),
	}
}

func (m *Metrics) SetQueue(depth int64, lag time.Duration) {
	if depth < 0 {
		depth = 0
	}
	if lag < 0 {
		lag = 0
	}
	m.mu.Lock()
	m.queueDepth = depth
	m.queueLag = lag
	m.mu.Unlock()
}

func (m *Metrics) SetQueueMonitorPollFailures(failures int64) {
	if failures < 0 {
		failures = 0
	}
	m.mu.Lock()
	m.queueMonitorPollFailures = failures
	m.mu.Unlock()
}

func (m *Metrics) SetWorkerReady(ready bool) {
	value := int64(0)
	if ready {
		value = 1
	}
	m.mu.Lock()
	m.workerReady = value
	m.mu.Unlock()
}

func (m *Metrics) SetActiveWorkers(active int64) {
	if active < 0 {
		active = 0
	}
	m.mu.Lock()
	m.activeWorkers = active
	m.mu.Unlock()
}

func (m *Metrics) ObserveWorkerClaim(result WorkerClaimResult) {
	result = normalizeWorkerClaimResult(result)
	m.mu.Lock()
	m.workerClaims[result]++
	m.mu.Unlock()
}

func (m *Metrics) ObserveStage(stage GenerationStage, outcome GenerationStageOutcome, reason GenerationStageReason, duration time.Duration) {
	stage = normalizeGenerationStage(stage)
	outcome = normalizeGenerationStageOutcome(outcome)
	reason = normalizeGenerationStageReason(reason)
	if duration < 0 {
		duration = 0
	}
	m.mu.Lock()
	m.stageTotals[generationStageLabels{Stage: stage, Outcome: outcome, Reason: reason}]++
	m.stageDuration[stage] += duration
	m.stageDurationCount[stage]++
	m.mu.Unlock()
}

func (m *Metrics) ObserveLeaseLost() {
	m.mu.Lock()
	m.leaseLostTotal++
	m.mu.Unlock()
}

func (m *Metrics) ObservePartialSource(stage PartialSourceStage, reason PartialSourceReason) {
	stage = normalizePartialSourceStage(stage)
	reason = normalizePartialSourceReason(reason)
	m.mu.Lock()
	m.partialSources[partialSourceLabels{Stage: stage, Reason: reason}]++
	m.mu.Unlock()
}

func (m *Metrics) SetConsecutivePollFailures(failures int64) {
	if failures < 0 {
		failures = 0
	}
	m.mu.Lock()
	m.consecutivePollFailures = failures
	m.mu.Unlock()
}

func (m *Metrics) ObserveRevisionAppend(origin RevisionAppendOrigin, outcome RevisionAppendOutcome) {
	origin = normalizeRevisionAppendOrigin(origin)
	outcome = normalizeRevisionAppendOutcome(outcome)
	m.mu.Lock()
	m.revisionAppends[revisionAppendLabels{Origin: origin, Outcome: outcome}]++
	m.mu.Unlock()
}

func (m *Metrics) ObserveRevisionVisibility(transition RevisionVisibilityTransition, outcome RevisionVisibilityOutcome) {
	transition = normalizeRevisionVisibilityTransition(transition)
	outcome = normalizeRevisionVisibilityOutcome(outcome)
	m.mu.Lock()
	m.revisionVisibility[revisionVisibilityLabels{Transition: transition, Outcome: outcome}]++
	m.mu.Unlock()
}

func (m *Metrics) ObserveLatestMutation(operation LatestMutationOperation, outcome LatestMutationOutcome) {
	operation = normalizeLatestMutationOperation(operation)
	outcome = normalizeLatestMutationOutcome(outcome)
	m.mu.Lock()
	m.latestMutations[latestMutationLabels{Operation: operation, Outcome: outcome}]++
	m.mu.Unlock()
}

var _ Recorder = (*Metrics)(nil)

// WritePrometheus writes exactly the documented metric families.
func (m *Metrics) WritePrometheus(writer io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var output strings.Builder

	output.WriteString("# TYPE " + MetricGenerationWorkerClaimsTotal + " counter\n")
	workerClaimKeys := sortedKeys(m.workerClaims, func(value WorkerClaimResult) string { return string(value) })
	for _, result := range workerClaimKeys {
		fmt.Fprintf(&output, "%s{result=%q} %d\n", MetricGenerationWorkerClaimsTotal, result, m.workerClaims[result])
	}

	output.WriteString("# TYPE " + MetricGenerationStageTotal + " counter\n")
	stageKeys := sortedKeys(m.stageTotals, func(value generationStageLabels) string {
		return string(value.Stage) + "\x00" + string(value.Outcome) + "\x00" + string(value.Reason)
	})
	for _, labels := range stageKeys {
		fmt.Fprintf(&output, "%s{stage=%q,outcome=%q,reason=%q} %d\n", MetricGenerationStageTotal,
			labels.Stage, labels.Outcome, labels.Reason, m.stageTotals[labels])
	}

	output.WriteString("# TYPE " + MetricGenerationStageDurationSeconds + " summary\n")
	durationKeys := sortedKeys(m.stageDurationCount, func(value GenerationStage) string { return string(value) })
	for _, stage := range durationKeys {
		fmt.Fprintf(&output, "%s_sum{stage=%q} %.9f\n", MetricGenerationStageDurationSeconds, stage, m.stageDuration[stage].Seconds())
		fmt.Fprintf(&output, "%s_count{stage=%q} %d\n", MetricGenerationStageDurationSeconds, stage, m.stageDurationCount[stage])
	}

	fmt.Fprintf(&output, "# TYPE %s gauge\n%s %d\n", MetricGenerationQueueDepth, MetricGenerationQueueDepth, m.queueDepth)
	fmt.Fprintf(&output, "# TYPE %s gauge\n%s %.9f\n", MetricGenerationQueueLagSeconds, MetricGenerationQueueLagSeconds, m.queueLag.Seconds())
	fmt.Fprintf(&output, "# TYPE %s gauge\n%s %d\n", MetricGenerationQueueMonitorPollFailures,
		MetricGenerationQueueMonitorPollFailures, m.queueMonitorPollFailures)
	fmt.Fprintf(&output, "# TYPE %s gauge\n%s %d\n", MetricGenerationWorkerReady, MetricGenerationWorkerReady, m.workerReady)
	fmt.Fprintf(&output, "# TYPE %s gauge\n%s %d\n", MetricGenerationActiveWorkers, MetricGenerationActiveWorkers, m.activeWorkers)
	fmt.Fprintf(&output, "# TYPE %s counter\n%s %d\n", MetricGenerationLeaseLostTotal, MetricGenerationLeaseLostTotal, m.leaseLostTotal)

	output.WriteString("# TYPE " + MetricGenerationPartialSourcesTotal + " counter\n")
	partialKeys := sortedKeys(m.partialSources, func(value partialSourceLabels) string {
		return string(value.Stage) + "\x00" + string(value.Reason)
	})
	for _, labels := range partialKeys {
		fmt.Fprintf(&output, "%s{stage=%q,reason=%q} %d\n", MetricGenerationPartialSourcesTotal,
			labels.Stage, labels.Reason, m.partialSources[labels])
	}

	fmt.Fprintf(&output, "# TYPE %s gauge\n%s %d\n", MetricGenerationConsecutivePollFailures,
		MetricGenerationConsecutivePollFailures, m.consecutivePollFailures)

	output.WriteString("# TYPE " + MetricRevisionAppendTotal + " counter\n")
	appendKeys := sortedKeys(m.revisionAppends, func(value revisionAppendLabels) string {
		return string(value.Origin) + "\x00" + string(value.Outcome)
	})
	for _, labels := range appendKeys {
		fmt.Fprintf(&output, "%s{origin=%q,outcome=%q} %d\n", MetricRevisionAppendTotal,
			labels.Origin, labels.Outcome, m.revisionAppends[labels])
	}

	output.WriteString("# TYPE " + MetricRevisionVisibilityTotal + " counter\n")
	visibilityKeys := sortedKeys(m.revisionVisibility, func(value revisionVisibilityLabels) string {
		return string(value.Transition) + "\x00" + string(value.Outcome)
	})
	for _, labels := range visibilityKeys {
		fmt.Fprintf(&output, "%s{transition=%q,outcome=%q} %d\n", MetricRevisionVisibilityTotal,
			labels.Transition, labels.Outcome, m.revisionVisibility[labels])
	}

	output.WriteString("# TYPE " + MetricLatestMutationTotal + " counter\n")
	latestKeys := sortedKeys(m.latestMutations, func(value latestMutationLabels) string {
		return string(value.Operation) + "\x00" + string(value.Outcome)
	})
	for _, labels := range latestKeys {
		fmt.Fprintf(&output, "%s{operation=%q,outcome=%q} %d\n", MetricLatestMutationTotal,
			labels.Operation, labels.Outcome, m.latestMutations[labels])
	}

	_, err := io.WriteString(writer, output.String())
	return err
}

func sortedKeys[K comparable, V any](values map[K]V, key func(K) string) []K {
	keys := make([]K, 0, len(values))
	for value := range values {
		keys = append(keys, value)
	}
	sort.Slice(keys, func(i, j int) bool { return key(keys[i]) < key(keys[j]) })
	return keys
}

func normalizeWorkerClaimResult(value WorkerClaimResult) WorkerClaimResult {
	switch value {
	case WorkerClaimResultClaimed, WorkerClaimResultEmpty, WorkerClaimResultError:
		return value
	default:
		return WorkerClaimResultUnknown
	}
}

func normalizeGenerationStage(value GenerationStage) GenerationStage {
	switch value {
	case GenerationStageTranscriptLoad, GenerationStageCandidateGeneration, GenerationStageCandidateEvaluation,
		GenerationStageSynthesis, GenerationStageFinalization:
		return value
	default:
		return GenerationStageUnknown
	}
}

func normalizeGenerationStageOutcome(value GenerationStageOutcome) GenerationStageOutcome {
	switch value {
	case GenerationStageOutcomeSuccess, GenerationStageOutcomeFailure, GenerationStageOutcomeCanceled,
		GenerationStageOutcomeRequeued, GenerationStageOutcomeClaimLost, GenerationStageOutcomeResourceLimited:
		return value
	default:
		return GenerationStageOutcomeUnknown
	}
}

func normalizeGenerationStageReason(value GenerationStageReason) GenerationStageReason {
	switch value {
	case GenerationStageReasonNone, GenerationStageReasonStorage, GenerationStageReasonTransient,
		GenerationStageReasonTerminal, GenerationStageReasonTimeout, GenerationStageReasonAttemptsExhausted,
		GenerationStageReasonTranscriptUnavailable, GenerationStageReasonCandidateGenerationFailed,
		GenerationStageReasonCandidateEvaluationFailed, GenerationStageReasonSynthesisFailed,
		GenerationStageReasonNoViableCandidates, GenerationStageReasonCanceled,
		GenerationStageReasonClaimLost, GenerationStageReasonResourceLimited:
		return value
	default:
		return GenerationStageReasonUnknown
	}
}

func normalizePartialSourceStage(value PartialSourceStage) PartialSourceStage {
	switch value {
	case PartialSourceStageTranscriptLoad, PartialSourceStageCandidateGeneration, PartialSourceStageCandidateEvaluation:
		return value
	default:
		return PartialSourceStageUnknown
	}
}

func normalizePartialSourceReason(value PartialSourceReason) PartialSourceReason {
	switch value {
	case PartialSourceReasonTranscriptUnavailable, PartialSourceReasonCandidateGenerationFailed,
		PartialSourceReasonCandidateEvaluationFailed:
		return value
	default:
		return PartialSourceReasonUnknown
	}
}

func normalizeRevisionAppendOrigin(value RevisionAppendOrigin) RevisionAppendOrigin {
	switch value {
	case RevisionAppendOriginManual, RevisionAppendOriginGeneration, RevisionAppendOriginDuplicate, RevisionAppendOriginMigrated:
		return value
	default:
		return RevisionAppendOriginUnknown
	}
}

func normalizeRevisionAppendOutcome(value RevisionAppendOutcome) RevisionAppendOutcome {
	switch value {
	case RevisionAppendOutcomeSuccess, RevisionAppendOutcomeConflict, RevisionAppendOutcomeInvalid,
		RevisionAppendOutcomeNotFound, RevisionAppendOutcomeClaimLost, RevisionAppendOutcomeError:
		return value
	default:
		return RevisionAppendOutcomeUnknown
	}
}

func normalizeRevisionVisibilityTransition(value RevisionVisibilityTransition) RevisionVisibilityTransition {
	switch value {
	case RevisionVisibilityTransitionPrivateToPublic, RevisionVisibilityTransitionPublicToPrivate,
		RevisionVisibilityTransitionUnchanged:
		return value
	default:
		return RevisionVisibilityTransitionUnknown
	}
}

func normalizeRevisionVisibilityOutcome(value RevisionVisibilityOutcome) RevisionVisibilityOutcome {
	switch value {
	case RevisionVisibilityOutcomeSuccess, RevisionVisibilityOutcomeNotFound,
		RevisionVisibilityOutcomeLatestConflict, RevisionVisibilityOutcomeError:
		return value
	default:
		return RevisionVisibilityOutcomeUnknown
	}
}

func normalizeLatestMutationOperation(value LatestMutationOperation) LatestMutationOperation {
	switch value {
	case LatestMutationOperationSet, LatestMutationOperationClear:
		return value
	default:
		return LatestMutationOperationUnknown
	}
}

func normalizeLatestMutationOutcome(value LatestMutationOutcome) LatestMutationOutcome {
	switch value {
	case LatestMutationOutcomeSuccess, LatestMutationOutcomeRevisionNotFound,
		LatestMutationOutcomeRevisionNotPublic, LatestMutationOutcomeError:
		return value
	default:
		return LatestMutationOutcomeUnknown
	}
}
