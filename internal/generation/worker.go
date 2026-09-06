package generation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/papercomputeco/skills-cassette/internal/storage"
)

// WorkerConfig bounds the cassette-local leased worker.
type WorkerConfig struct {
	WorkerID             string
	WorkerConcurrency    int
	PollInterval         time.Duration
	MaxPollInterval      time.Duration
	LeaseDuration        time.Duration
	HeartbeatInterval    time.Duration
	ProcessingTimeout    time.Duration
	DrainTimeout         time.Duration
	RetryBackoff         time.Duration
	MaxRetryBackoff      time.Duration
	MaxAttempts          int
	MaxSessions          int
	CandidateConcurrency int
	MaxTranscriptBytes   int
}

func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		WorkerConcurrency: 2,
		PollInterval:      250 * time.Millisecond, MaxPollInterval: 5 * time.Second,
		LeaseDuration: 2 * time.Minute, HeartbeatInterval: 30 * time.Second,
		ProcessingTimeout: 5 * time.Minute, DrainTimeout: 10 * time.Second,
		RetryBackoff: time.Second, MaxRetryBackoff: 30 * time.Second,
		MaxAttempts: 3, MaxSessions: 8, CandidateConcurrency: 2,
		MaxTranscriptBytes: 1 << 20,
	}
}

// EffectiveCandidateConcurrency applies defaults and deterministically caps
// candidate generation and evaluation concurrency to maxSessions.
func EffectiveCandidateConcurrency(configured, maxSessions int) int {
	defaults := DefaultWorkerConfig()
	if maxSessions <= 0 {
		maxSessions = defaults.MaxSessions
	}
	if configured <= 0 {
		configured = defaults.CandidateConcurrency
	}
	if configured > maxSessions {
		return maxSessions
	}
	return configured
}

// WorkerTimer is the injected one-shot timer used for polling, lease renewal,
// and bounded drain waits. Processing deadlines are independent context
// deadlines so a blocked lease renewal cannot prevent timeout cancellation.
type WorkerTimer interface {
	C() <-chan time.Time
	Stop() bool
}

// WorkerRuntime contains process-clock seams. Durable due and lease times stay
// storage-owned.
type WorkerRuntime struct {
	Clock       func() time.Time
	NewTimer    func(time.Duration) WorkerTimer
	RandomFloat func() float64
}

type systemWorkerTimer struct {
	timer *time.Timer
}

func (t *systemWorkerTimer) C() <-chan time.Time { return t.timer.C }
func (t *systemWorkerTimer) Stop() bool          { return t.timer.Stop() }

func defaultWorkerRuntime() WorkerRuntime {
	return WorkerRuntime{
		Clock:       time.Now,
		NewTimer:    func(delay time.Duration) WorkerTimer { return &systemWorkerTimer{timer: time.NewTimer(delay)} },
		RandomFloat: rand.Float64,
	}
}

// QueueStatsReader is the one storage capability required by the queue monitor.
type QueueStatsReader interface {
	GenerationQueueStats(context.Context) (storage.GenerationQueueStats, error)
}

// QueueMonitor samples durable queue state independently of claim processing.
// This lets servers expose queue health even when LLM, Tapes, or evaluator
// dependencies are absent and the generation worker cannot run.
type QueueMonitor struct {
	store   QueueStatsReader
	config  WorkerConfig
	runtime WorkerRuntime
	metrics *Metrics
	logger  *slog.Logger
}

func NewQueueMonitor(store QueueStatsReader, config WorkerConfig, metrics *Metrics, logger *slog.Logger) *QueueMonitor {
	return NewQueueMonitorWithRuntime(store, config, metrics, logger, defaultWorkerRuntime())
}

func NewQueueMonitorWithRuntime(store QueueStatsReader, config WorkerConfig, metrics *Metrics, logger *slog.Logger, runtime WorkerRuntime) *QueueMonitor {
	if metrics == nil {
		metrics = NewMetrics()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &QueueMonitor{
		store: store, config: normalizedWorkerConfig(config),
		runtime: normalizedWorkerRuntime(runtime), metrics: metrics, logger: logger,
	}
}

// Run samples until ctx is canceled.
func (m *QueueMonitor) Run(ctx context.Context) error {
	if m.store == nil {
		return errors.New("generation queue monitor requires a store")
	}
	m.run(ctx, nil)
	return nil
}

// ClaimProcessor advances one fenced generation claim.
type ClaimProcessor interface {
	Process(context.Context, string, string) (*ProcessResult, error)
}

// Worker polls GenerationStore directly; no external queue service is needed.
type Worker struct {
	store     storage.GenerationStore
	processor ClaimProcessor
	config    WorkerConfig
	runtime   WorkerRuntime
	metrics   *Metrics
	logger    *slog.Logger

	activeClaims sync.WaitGroup
	activeCount  atomic.Int64
}

func NewWorker(store storage.GenerationStore, processor ClaimProcessor, config WorkerConfig, metrics *Metrics, logger *slog.Logger) *Worker {
	return NewWorkerWithRuntime(store, processor, config, metrics, logger, defaultWorkerRuntime())
}

// NewWorkerWithRuntime exposes clock, timer, and random seams for deterministic
// polling, lease cancellation, persistent requeue jitter, and graceful drain.
func NewWorkerWithRuntime(store storage.GenerationStore, processor ClaimProcessor, config WorkerConfig, metrics *Metrics, logger *slog.Logger, runtime WorkerRuntime) *Worker {
	if metrics == nil {
		metrics = NewMetrics()
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		store: store, processor: processor, config: normalizedWorkerConfig(config),
		runtime: normalizedWorkerRuntime(runtime), metrics: metrics, logger: logger,
	}
}

func normalizedWorkerRuntime(runtime WorkerRuntime) WorkerRuntime {
	defaults := defaultWorkerRuntime()
	if runtime.Clock == nil {
		runtime.Clock = defaults.Clock
	}
	if runtime.NewTimer == nil {
		runtime.NewTimer = defaults.NewTimer
	}
	if runtime.RandomFloat == nil {
		runtime.RandomFloat = defaults.RandomFloat
	}
	return runtime
}

// Run maintains a bounded pool of independently leased generations until ctx
// is canceled, then waits no longer than DrainTimeout for cooperative shutdown.
func (w *Worker) Run(ctx context.Context) error {
	if w.store == nil || w.processor == nil {
		return errors.New("generation worker requires a store and processor")
	}
	if err := validateWorkerConfig(w.config); err != nil {
		return err
	}

	w.metrics.SetWorkerReady(true)
	defer w.metrics.SetWorkerReady(false)
	slots := make(chan struct{}, w.config.WorkerConcurrency)
	sampleRequests := make(chan struct{}, 1)
	queueMonitor := NewQueueMonitorWithRuntime(w.store, w.config, w.metrics, w.logger, w.runtime)
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		queueMonitor.run(monitorCtx, sampleRequests)
	}()
	defer func() {
		stopMonitor()
		<-monitorDone
	}()
	pollDelay := w.config.PollInterval
	var consecutivePollFailures int64
	for {
		select {
		case <-ctx.Done():
			w.drain()
			return nil
		case slots <- struct{}{}:
		}

		claim, err := w.store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
			WorkerID: w.config.WorkerID, LeaseDuration: w.config.LeaseDuration,
		})
		if err != nil {
			<-slots
			if ctx.Err() != nil {
				w.drain()
				return nil
			}
			consecutivePollFailures++
			w.metrics.ObserveWorkerClaim(WorkerClaimResultError)
			w.metrics.SetConsecutivePollFailures(consecutivePollFailures)
			w.logger.Warn("generation queue poll failed")
			if !w.wait(ctx, w.jitteredDelay(pollDelay)) {
				w.drain()
				return nil
			}
			pollDelay = boundedDouble(pollDelay, w.config.MaxPollInterval)
			continue
		}

		consecutivePollFailures = 0
		w.metrics.SetConsecutivePollFailures(0)
		if claim == nil {
			<-slots
			w.metrics.ObserveWorkerClaim(WorkerClaimResultEmpty)
			if !w.wait(ctx, w.jitteredDelay(pollDelay)) {
				w.drain()
				return nil
			}
			pollDelay = boundedDouble(pollDelay, w.config.MaxPollInterval)
			continue
		}

		w.metrics.ObserveWorkerClaim(WorkerClaimResultClaimed)
		w.requestQueueSample(sampleRequests)
		pollDelay = w.config.PollInterval
		w.activeClaims.Add(1)
		active := w.activeCount.Add(1)
		w.metrics.SetActiveWorkers(active)
		go func(claim storage.SkillGenerationRecord) {
			defer func() {
				active := w.activeCount.Add(-1)
				w.metrics.SetActiveWorkers(active)
				<-slots
				w.activeClaims.Done()
			}()
			w.handleClaim(ctx, claim)
		}(*claim)
	}
}

type claimMonitorResult struct {
	leaseLost bool
	canceled  bool
	completed bool
	timedOut  bool
}

func (w *Worker) handleClaim(parent context.Context, claim storage.SkillGenerationRecord) {
	started := w.runtime.Clock()
	if claim.AttemptCount > w.config.MaxAttempts {
		w.failClaim(parent, claim, storage.GenerationFailure{
			Code: "attempts_exhausted", Message: "Generation retry attempts were exhausted.",
		}, GenerationStageReasonAttemptsExhausted, started)
		return
	}
	if len(claim.SelectedSessionIDs) > w.config.MaxSessions {
		_, diagnosticErr := w.store.PutGenerationDiagnostic(parent, claim.ID, claim.ClaimToken, storage.GenerationDiagnosticRecord{
			ID:    uuid.NewSHA1(uuid.NameSpaceOID, []byte(claim.ID+":resource-limit")).String(),
			Stage: "candidate", Code: "session_limit_exceeded",
			Message: "The generation selected more sessions than this worker permits.",
		})
		if diagnosticErr != nil {
			w.observeClaimLossIfNeeded(diagnosticErr, started)
			return
		}
		_, failErr := w.store.FailGeneration(parent, storage.FailGenerationInput{
			GenerationID: claim.ID, ClaimToken: claim.ClaimToken,
			Failure: storage.GenerationFailure{
				Code: "resource_limit_exceeded", Message: "Generation exceeded a configured resource limit.",
			},
		})
		if failErr != nil {
			w.observeClaimLossIfNeeded(failErr, started)
			return
		}
		w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeResourceLimited,
			GenerationStageReasonResourceLimited, w.elapsed(started))
		return
	}

	// Keep the processing deadline independent from the heartbeat loop. In
	// particular, RenewGenerationLease may block until this context is canceled;
	// a heartbeat-owned timeout timer would then never be serviced.
	processingCtx, cancelProcessing := context.WithTimeout(parent, w.config.ProcessingTimeout)
	defer cancelProcessing()
	monitorDone := make(chan claimMonitorResult, 1)
	go func() {
		monitorDone <- w.monitorClaim(processingCtx, cancelProcessing, claim)
	}()
	type processorResult struct{ err error }
	processorDone := make(chan processorResult, 1)
	go func() {
		_, err := w.processor.Process(processingCtx, claim.ID, claim.ClaimToken)
		processorDone <- processorResult{err: err}
	}()

	var processErr error
	select {
	case processed := <-processorDone:
		processErr = processed.err
		deadlineExpired := errors.Is(processingCtx.Err(), context.DeadlineExceeded)
		cancelProcessing()
		monitor := <-monitorDone
		if monitor.leaseLost {
			w.metrics.ObserveLeaseLost()
			w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeClaimLost,
				GenerationStageReasonClaimLost, w.elapsed(started))
			return
		}
		if monitor.canceled {
			w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeCanceled,
				GenerationStageReasonCanceled, w.elapsed(started))
			return
		}
		if monitor.timedOut || deadlineExpired {
			w.persistTimeoutFailure(parent, claim, started)
			return
		}
	case monitor := <-monitorDone:
		if monitor.leaseLost {
			w.metrics.ObserveLeaseLost()
			w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeClaimLost,
				GenerationStageReasonClaimLost, w.elapsed(started))
		} else if monitor.timedOut {
			w.persistTimeoutFailure(parent, claim, started)
		} else if !monitor.completed {
			w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeCanceled,
				GenerationStageReasonCanceled, w.elapsed(started))
		}
		// A canceled dependency may ignore its context. Keep this claim's worker
		// slot occupied until Process actually exits so such calls remain bounded
		// by WorkerConcurrency even after shutdown's drain budget elapses.
		<-processorDone
		return
	}

	if parent.Err() != nil {
		w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeCanceled,
			GenerationStageReasonCanceled, w.elapsed(started))
		return
	}
	if errors.Is(processErr, storage.ErrGenerationCanceled) {
		w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeCanceled,
			GenerationStageReasonCanceled, w.elapsed(started))
		return
	}
	if errors.Is(processErr, storage.ErrGenerationClaimLost) {
		w.metrics.ObserveLeaseLost()
		w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeClaimLost,
			GenerationStageReasonClaimLost, w.elapsed(started))
		return
	}
	if processErr == nil {
		return
	}

	var classified *ProcessError
	if errors.As(processErr, &classified) && classified.Retryable {
		if claim.AttemptCount >= w.config.MaxAttempts {
			w.failClaim(parent, claim, storage.GenerationFailure{
				Code: "attempts_exhausted", Message: "Generation retry attempts were exhausted.",
			}, GenerationStageReasonAttemptsExhausted, started)
			return
		}
		delay := w.retryDelay(claim.AttemptCount)
		_, err := w.store.RequeueGeneration(parent, storage.RequeueGenerationInput{
			GenerationID: claim.ID, ClaimToken: claim.ClaimToken, RetryAfter: delay,
			Failure: storage.GenerationFailure{
				Code: "generation_retrying", Message: "Generation will be retried.",
			},
		})
		if err != nil {
			w.observeClaimLossIfNeeded(err, started)
			return
		}
		w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeRequeued,
			GenerationStageReasonTransient, w.elapsed(started))
		return
	}

	failure := storage.GenerationFailure{
		Code: "generation_failed", Message: "Generation could not be completed.",
	}
	reason := GenerationStageReasonTerminal
	if errors.As(processErr, &classified) && classified.Code == "no_viable_candidates" {
		failure = storage.GenerationFailure{
			Code: "no_viable_candidates", Message: "No candidate could be evaluated.",
		}
		reason = GenerationStageReasonNoViableCandidates
	}
	w.failClaim(parent, claim, failure, reason, started)
}

func (w *Worker) monitorClaim(ctx context.Context, cancel context.CancelFunc, claim storage.SkillGenerationRecord) claimMonitorResult {
	heartbeatTimer := w.runtime.NewTimer(w.config.HeartbeatInterval)
	defer heartbeatTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return claimContextResult(ctx)
		case <-heartbeatTimer.C():
			renewed, err := w.store.RenewGenerationLease(ctx, claim.ID, claim.ClaimToken, w.config.LeaseDuration)
			if ctx.Err() != nil {
				return claimContextResult(ctx)
			}
			if errors.Is(err, storage.ErrGenerationCanceled) {
				cancel()
				return claimMonitorResult{canceled: true}
			}
			if errors.Is(err, storage.ErrGenerationCompleted) {
				cancel()
				return claimMonitorResult{completed: true}
			}
			if err != nil || !renewed {
				cancel()
				return claimMonitorResult{leaseLost: true}
			}
			heartbeatTimer.Stop()
			heartbeatTimer = w.runtime.NewTimer(w.config.HeartbeatInterval)
		}
	}
}

func claimContextResult(ctx context.Context) claimMonitorResult {
	return claimMonitorResult{timedOut: errors.Is(ctx.Err(), context.DeadlineExceeded)}
}

func (w *Worker) persistTimeoutFailure(parent context.Context, claim storage.SkillGenerationRecord, started time.Time) {
	// Persisting terminal state releases the claim immediately, fencing a
	// provider call that ignores cancellation from all later writes. Detach from
	// the processing cancellation, but retain a strict operational deadline.
	mutationCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), w.config.DrainTimeout)
	defer cancel()
	w.failClaim(mutationCtx, claim, storage.GenerationFailure{
		Code: "generation_timeout", Message: "Generation processing timed out.",
	}, GenerationStageReasonTimeout, started)
}

func (w *Worker) failClaim(ctx context.Context, claim storage.SkillGenerationRecord, failure storage.GenerationFailure, reason GenerationStageReason, started time.Time) {
	_, err := w.store.FailGeneration(ctx, storage.FailGenerationInput{
		GenerationID: claim.ID, ClaimToken: claim.ClaimToken, Failure: failure,
	})
	if err != nil {
		w.observeClaimLossIfNeeded(err, started)
		return
	}
	w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeFailure, reason, w.elapsed(started))
}

func (w *Worker) observeClaimLossIfNeeded(err error, started time.Time) {
	if errors.Is(err, storage.ErrGenerationCanceled) {
		w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeCanceled,
			GenerationStageReasonCanceled, w.elapsed(started))
		return
	}
	if !errors.Is(err, storage.ErrGenerationClaimLost) {
		w.logger.Warn("generation fenced state persistence failed")
		w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeFailure,
			GenerationStageReasonStorage, w.elapsed(started))
		return
	}
	w.metrics.ObserveLeaseLost()
	w.metrics.ObserveStage(GenerationStageUnknown, GenerationStageOutcomeClaimLost,
		GenerationStageReasonClaimLost, w.elapsed(started))
}

func (m *QueueMonitor) run(ctx context.Context, requests <-chan struct{}) {
	failureBase := m.config.PollInterval
	warningSuppressed := false
	var consecutiveFailures int64
	sample := func() time.Duration {
		stats, err := m.store.GenerationQueueStats(ctx)
		if err == nil {
			m.metrics.SetQueue(stats.Depth, stats.Lag)
			m.metrics.SetQueueMonitorPollFailures(0)
			consecutiveFailures = 0
			failureBase = m.config.PollInterval
			warningSuppressed = false
			return m.config.PollInterval
		}
		if ctx.Err() != nil {
			return m.config.PollInterval
		}
		consecutiveFailures++
		m.metrics.SetQueueMonitorPollFailures(consecutiveFailures)
		if !warningSuppressed {
			m.logger.Warn("generation queue statistics failed")
			warningSuppressed = true
		}
		delay := jitteredDelay(m.runtime.RandomFloat, failureBase)
		failureBase = boundedDouble(failureBase, m.config.MaxPollInterval)
		return delay
	}

	timer := m.runtime.NewTimer(sample())
	defer func() { timer.Stop() }()
	for {
		select {
		case <-ctx.Done():
			return
		case <-requests:
			// A post-claim hint may accelerate healthy sampling, but it must not
			// defeat failure backoff and turn an unavailable store into a hot loop.
			if warningSuppressed {
				continue
			}
			timer.Stop()
			timer = m.runtime.NewTimer(sample())
		case <-timer.C():
			timer = m.runtime.NewTimer(sample())
		}
	}
}

func (w *Worker) requestQueueSample(requests chan<- struct{}) {
	select {
	case requests <- struct{}{}:
	default:
	}
}

func (w *Worker) retryDelay(attempt int) time.Duration {
	base := w.config.RetryBackoff
	for current := 1; current < attempt; current++ {
		base = boundedDouble(base, w.config.MaxRetryBackoff)
	}
	if base > w.config.MaxRetryBackoff {
		base = w.config.MaxRetryBackoff
	}
	return w.jitteredDelay(base)
}

// jitteredDelay applies bounded equal jitter in [base/2, base). The injected
// random seam is shared by durable retries and empty/error poll backoff.
func (w *Worker) jitteredDelay(base time.Duration) time.Duration {
	return jitteredDelay(w.runtime.RandomFloat, base)
}

func jitteredDelay(randomFloat func() float64, base time.Duration) time.Duration {
	random := randomFloat()
	if random < 0 {
		random = 0
	}
	if random >= 1 {
		random = 0.9999999999999999
	}
	half := base / 2
	return half + time.Duration(float64(base-half)*random)
}

func (w *Worker) elapsed(started time.Time) time.Duration {
	duration := w.runtime.Clock().Sub(started)
	if duration < 0 {
		return 0
	}
	return duration
}

func (w *Worker) wait(ctx context.Context, delay time.Duration) bool {
	timer := w.runtime.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C():
		return true
	}
}

func (w *Worker) drain() {
	done := make(chan struct{})
	go func() {
		w.activeClaims.Wait()
		close(done)
	}()
	timer := w.runtime.NewTimer(w.config.DrainTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C():
	}
}

func normalizedWorkerConfig(config WorkerConfig) WorkerConfig {
	defaults := DefaultWorkerConfig()
	if config.WorkerID == "" {
		config.WorkerID = uuid.NewString()
	}
	if config.WorkerConcurrency <= 0 {
		config.WorkerConcurrency = defaults.WorkerConcurrency
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.MaxPollInterval <= 0 {
		config.MaxPollInterval = defaults.MaxPollInterval
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = defaults.LeaseDuration
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if config.ProcessingTimeout <= 0 {
		config.ProcessingTimeout = defaults.ProcessingTimeout
	}
	if config.DrainTimeout <= 0 {
		config.DrainTimeout = defaults.DrainTimeout
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = defaults.RetryBackoff
	}
	if config.MaxRetryBackoff <= 0 {
		config.MaxRetryBackoff = defaults.MaxRetryBackoff
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = defaults.MaxAttempts
	}
	if config.MaxSessions <= 0 {
		config.MaxSessions = defaults.MaxSessions
	}
	config.CandidateConcurrency = EffectiveCandidateConcurrency(config.CandidateConcurrency, config.MaxSessions)
	if config.MaxTranscriptBytes <= 0 {
		config.MaxTranscriptBytes = defaults.MaxTranscriptBytes
	}
	return config
}

func validateWorkerConfig(config WorkerConfig) error {
	if config.HeartbeatInterval >= config.LeaseDuration {
		return errors.New("generation heartbeat interval must be shorter than its lease")
	}
	if config.MaxPollInterval < config.PollInterval || config.MaxRetryBackoff < config.RetryBackoff {
		return errors.New("generation backoff maxima must not be shorter than their initial delays")
	}
	if config.MaxAttempts > 10 || config.WorkerConcurrency > 64 || config.MaxSessions > 100 || config.CandidateConcurrency > config.MaxSessions {
		return fmt.Errorf("generation worker resource limits are invalid")
	}
	return nil
}

func boundedDouble(value, maximum time.Duration) time.Duration {
	if value >= maximum || value > maximum/2 {
		return maximum
	}
	return value * 2
}
