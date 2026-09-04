package generation_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
	"github.com/papercomputeco/skills-cassette/internal/generation"
	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/internal/storage/storagetest"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const (
	workerSkillID      = "00000000-0000-0000-0000-000000000011"
	workerGenerationID = "00000000-0000-0000-0000-000000000012"
	workerBaseID       = "00000000-0000-0000-0000-000000000013"
	workerOwner        = "worker-owner"
)

type claimProcessorFunc func(context.Context, string, string) (*generation.ProcessResult, error)

func (f claimProcessorFunc) Process(ctx context.Context, generationID, claimToken string) (*generation.ProcessResult, error) {
	return f(ctx, generationID, claimToken)
}

func generationCriteriaJSON() (evaluator.CriteriaSet, json.RawMessage) {
	criteria := evaluator.GenerationCandidateCriteria()
	encoded, err := json.Marshal(criteria.Criteria)
	Expect(err).NotTo(HaveOccurred())
	return criteria, encoded
}

func expectedGenerationArtifactID(parts ...string) string {
	encoded, err := json.Marshal(parts)
	Expect(err).NotTo(HaveOccurred())
	return uuid.NewSHA1(uuid.NameSpaceOID, encoded).String()
}

func expectedGenerationResultRevisionID(generationID string) string {
	namespace, err := uuid.Parse("08c91982-ee90-4ad4-af5e-182112abb48a")
	Expect(err).NotTo(HaveOccurred())
	return uuid.NewSHA1(namespace, []byte(generationID)).String()
}

func queuedGeneration(selected []string) *storage.MemoryStore {
	store := storage.NewMemoryStore()
	now := time.Now().UTC().Add(-time.Second)
	criteria, criteriaJSON := generationCriteriaJSON()
	skillRecord, err := store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
		ID: workerSkillID, Slug: "worker", CreatorSubject: workerOwner, CreatedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	baseRevision, err := store.AppendRevision(context.Background(), storage.AppendRevisionInput{
		ID: workerBaseID, SkillID: skillRecord.ID, CreatorSubject: workerOwner,
		Origin: storage.RevisionOriginManual,
		Snapshot: storage.SkillRevisionSnapshot{
			Name: "Worker base", Description: "Use when testing workers.",
			Type: "workflow", Content: "## Steps\n\n1. Start safely.",
		},
		CreatedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	_, err = store.CreateGeneration(context.Background(), storage.CreateGenerationInput{
		ID: workerGenerationID, SkillID: skillRecord.ID,
		BaseRevisionID: baseRevision.ID, CreatorSubject: workerOwner,
		Snapshot: storage.SkillRevisionSnapshot{
			Name: "Worker", Description: "Use when testing workers.",
			Type: "workflow", Content: "## Steps\n\n1. Work.",
		},
		AuthorContext: "preserve safe steps", SelectedSessionIDs: selected,
		EvaluatorProfile: criteria.Profile, EvaluatorProfileVersion: criteria.Version,
		EvaluationCriteria: criteriaJSON, CreatedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	return store
}

func addQueuedGeneration(store storage.GenerationStore, id, skillID, baseID, creator string, selected []string, createdAt time.Time) {
	criteria, criteriaJSON := generationCriteriaJSON()
	_, err := store.CreateGeneration(context.Background(), storage.CreateGenerationInput{
		ID: id, SkillID: skillID, BaseRevisionID: baseID, CreatorSubject: creator,
		Snapshot: storage.SkillRevisionSnapshot{
			Name: "Worker " + id, Description: "Independent durable work.",
			Type: "workflow", Content: "## Steps\n\n1. Work independently.",
		},
		AuthorContext: "preserve safe steps", SelectedSessionIDs: selected,
		EvaluatorProfile: criteria.Profile, EvaluatorProfileVersion: criteria.Version,
		EvaluationCriteria: criteriaJSON, CreatedAt: createdAt,
	})
	Expect(err).NotTo(HaveOccurred())
}

func fastWorkerConfig() generation.WorkerConfig {
	config := generation.DefaultWorkerConfig()
	config.WorkerID = "test-worker"
	config.WorkerConcurrency = 2
	config.PollInterval = 2 * time.Millisecond
	config.MaxPollInterval = 10 * time.Millisecond
	config.LeaseDuration = 200 * time.Millisecond
	config.HeartbeatInterval = 20 * time.Millisecond
	config.ProcessingTimeout = time.Second
	config.DrainTimeout = 50 * time.Millisecond
	config.RetryBackoff = 10 * time.Millisecond
	config.MaxRetryBackoff = 20 * time.Millisecond
	config.MaxAttempts = 3
	return config
}

func waitUntil(timeout time.Duration, condition func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	Expect(condition()).To(BeTrue(), "condition did not converge within %s", timeout)
}

func workerTestPostgresDSN() string {
	for _, name := range []string{"TEST_POSTGRES_DSN", "TAPES_TEST_POSTGRES_DSN", "TEST_DATABASE_URL"} {
		if dsn := os.Getenv(name); dsn != "" {
			return dsn
		}
	}
	return ""
}

func requireWorkerTestPostgresDSN() string {
	dsn := workerTestPostgresDSN()
	if dsn == "" {
		Skip("TEST_POSTGRES_DSN / TAPES_TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
	}
	return dsn
}

type workerPostgresFixture struct {
	store  *storage.PostgresStore
	admin  *pgxpool.Pool
	schema string
}

func openWorkerPostgresFixture(dsn, prefix string) *workerPostgresFixture {
	ctx := context.Background()
	schema := prefix + "_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	store, err := storage.OpenPostgresStore(ctx, dsn, schema)
	Expect(err).NotTo(HaveOccurred())
	admin, err := pgxpool.New(ctx, dsn)
	Expect(err).NotTo(HaveOccurred())
	fixture := &workerPostgresFixture{store: store, admin: admin, schema: schema}
	DeferCleanup(func() {
		store.Close()
		_ = storagetest.DropSchema(context.Background(), admin, schema)
		admin.Close()
	})
	return fixture
}

func seedEvaluatedCandidate(
	store storage.GenerationStore,
	claim *storage.SkillGenerationRecord,
	candidateID string,
) (*storage.GenerationCandidateRecord, *storage.CandidateEvaluationRecord, error) {
	kind := storage.GenerationCandidateContext
	var sourceSessionIDs []string
	if len(claim.SelectedSessionIDs) > 0 {
		kind = storage.GenerationCandidateSession
		sourceSessionIDs = []string{claim.SelectedSessionIDs[0]}
	}
	candidate, err := store.PutGenerationCandidate(context.Background(), claim.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
		ID: candidateID, Ordinal: 0, Kind: kind, SourceSessionIDs: sourceSessionIDs,
		Snapshot: storage.GenerationCandidateSnapshot{
			Name: "Retained candidate", Description: "Bounded prior history.",
			Type: "workflow", Content: "# Retained candidate", IsAIGenerated: true,
		},
		Insights: json.RawMessage(`[{"kind":"strength","summary":"retained","evidence":"bounded fixture"}]`), BundleSHA256: "retained-bundle",
	})
	if err != nil {
		return nil, nil, err
	}
	score := 0.9
	evaluation, err := store.PutCandidateEvaluation(context.Background(), claim.ID, claim.ClaimToken, storage.CandidateEvaluationRecord{
		ID: uuid.NewString(), CandidateID: candidate.ID, RequestSHA256: "retained-request",
		Profile: claim.EvaluatorProfile, ProfileVersion: claim.EvaluatorProfileVersion,
		EvaluatorVersion: "test", Score: &score, Decision: "pass",
		CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`),
		Strengths: json.RawMessage(`["retained"]`), Panel: json.RawMessage(`{}`),
	})
	return candidate, evaluation, err
}

type blockingCandidateGenerator struct {
	active  atomic.Int32
	maximum atomic.Int32
	entered chan struct{}
	release <-chan struct{}
}

func updateMaximum(maximum *atomic.Int32, active int32) {
	for {
		observed := maximum.Load()
		if active <= observed || maximum.CompareAndSwap(observed, active) {
			return
		}
	}
}

func (g *blockingCandidateGenerator) GenerateCandidate(ctx context.Context, request skill.CandidateRequest) (*skill.Candidate, error) {
	active := g.active.Add(1)
	defer g.active.Add(-1)
	updateMaximum(&g.maximum, active)
	g.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-g.release:
		return generatedCandidate("candidate-"+request.SourceSessionID, request.SkillType, request.SourceSessionID), nil
	}
}

func (g *blockingCandidateGenerator) SynthesizeCandidate(_ context.Context, request skill.SynthesisRequest) (*skill.Candidate, error) {
	return generatedCandidate("synthesis", request.SkillType, ""), nil
}

type blockingCandidateEvaluator struct {
	active  atomic.Int32
	maximum atomic.Int32
	entered chan struct{}
	release <-chan struct{}
}

func (e *blockingCandidateEvaluator) EvaluateCandidate(ctx context.Context, request evaluator.CandidateEvaluationRequest) (evaluator.CandidateEvaluation, error) {
	active := e.active.Add(1)
	defer e.active.Add(-1)
	updateMaximum(&e.maximum, active)
	e.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return evaluator.CandidateEvaluation{}, ctx.Err()
	case <-e.release:
		result := candidateEvaluation(request.Profile, 0.8, "pass")
		result.ProfileVersion = request.ProfileVersion
		return result, nil
	}
}

type workerTimer struct{ timer *time.Timer }

func (t *workerTimer) C() <-chan time.Time { return t.timer.C }
func (t *workerTimer) Stop() bool          { return t.timer.Stop() }

type controlledWorkerTimer struct {
	delay time.Duration
	tick  chan time.Time
}

func (t *controlledWorkerTimer) C() <-chan time.Time { return t.tick }
func (t *controlledWorkerTimer) Stop() bool          { return true }

type scriptedQueueStatsReader struct{ calls atomic.Int32 }

func (s *scriptedQueueStatsReader) GenerationQueueStats(context.Context) (storage.GenerationQueueStats, error) {
	switch s.calls.Add(1) {
	case 1, 2, 3, 5:
		return storage.GenerationQueueStats{}, errors.New("queue unavailable")
	default:
		return storage.GenerationQueueStats{Depth: 7, Lag: 3 * time.Second}, nil
	}
}

type fakeStoreClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeStoreClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeStoreClock) Advance(delay time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delay)
	c.mu.Unlock()
}

type recordingSchedulingStore struct {
	*storage.MemoryStore
	mu       sync.Mutex
	requeues []storage.RequeueGenerationInput
	failures []storage.FailGenerationInput
	advance  func(time.Duration)
}

func (s *recordingSchedulingStore) RequeueGeneration(ctx context.Context, input storage.RequeueGenerationInput) (*storage.SkillGenerationRecord, error) {
	s.mu.Lock()
	s.requeues = append(s.requeues, input)
	s.mu.Unlock()
	requeued, err := s.MemoryStore.RequeueGeneration(ctx, input)
	if err == nil && s.advance != nil {
		s.advance(input.RetryAfter)
	}
	return requeued, err
}

func (s *recordingSchedulingStore) FailGeneration(ctx context.Context, input storage.FailGenerationInput) (*storage.SkillGenerationRecord, error) {
	s.mu.Lock()
	s.failures = append(s.failures, input)
	s.mu.Unlock()
	return s.MemoryStore.FailGeneration(ctx, input)
}

func (s *recordingSchedulingStore) schedulingCalls() ([]storage.RequeueGenerationInput, []storage.FailGenerationInput) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]storage.RequeueGenerationInput(nil), s.requeues...), append([]storage.FailGenerationInput(nil), s.failures...)
}

type crashPoint struct {
	operation    string
	phase        string
	artifactKind storage.GenerationCandidateKind
	from         storage.GenerationStatus
	to           storage.GenerationStatus
}

type crashGenerationStore struct {
	storage.GenerationStore
	point   crashPoint
	reached chan struct{}
	once    sync.Once
}

func (s *crashGenerationStore) matchesStage(from, to storage.GenerationStatus) bool {
	return s.point.operation == "stage" && s.point.from == from && s.point.to == to
}

func (s *crashGenerationStore) matchesCandidate(kind storage.GenerationCandidateKind) bool {
	return s.point.operation == "candidate" && s.point.artifactKind == kind
}

func (s *crashGenerationStore) matchesEvaluation(generationID, candidateID string) bool {
	if s.point.operation != "evaluation" {
		return false
	}
	ordinal := 0
	if s.point.artifactKind == storage.GenerationCandidateSynthesis {
		ordinal = 1
	}
	return candidateID == expectedGenerationArtifactID(generationID, "candidate", strconv.Itoa(ordinal))
}

func (s *crashGenerationStore) crash(ctx context.Context) error {
	s.once.Do(func() { close(s.reached) })
	<-ctx.Done()
	return ctx.Err()
}

func (s *crashGenerationStore) UpdateGenerationStatus(ctx context.Context, generationID, claimToken string, from, to storage.GenerationStatus) error {
	if s.matchesStage(from, to) && s.point.phase == "before" {
		return s.crash(ctx)
	}
	err := s.GenerationStore.UpdateGenerationStatus(ctx, generationID, claimToken, from, to)
	if err == nil && s.matchesStage(from, to) && s.point.phase == "after" {
		return s.crash(ctx)
	}
	return err
}

func (s *crashGenerationStore) PutGenerationCandidate(ctx context.Context, generationID, claimToken string, candidate storage.GenerationCandidateRecord) (*storage.GenerationCandidateRecord, error) {
	if s.matchesCandidate(candidate.Kind) && s.point.phase == "before" {
		return nil, s.crash(ctx)
	}
	stored, err := s.GenerationStore.PutGenerationCandidate(ctx, generationID, claimToken, candidate)
	if err == nil && s.matchesCandidate(candidate.Kind) && s.point.phase == "after" {
		return nil, s.crash(ctx)
	}
	return stored, err
}

func (s *crashGenerationStore) PutCandidateEvaluation(ctx context.Context, generationID, claimToken string, evaluation storage.CandidateEvaluationRecord) (*storage.CandidateEvaluationRecord, error) {
	if s.matchesEvaluation(generationID, evaluation.CandidateID) && s.point.phase == "before" {
		return nil, s.crash(ctx)
	}
	stored, err := s.GenerationStore.PutCandidateEvaluation(ctx, generationID, claimToken, evaluation)
	if err == nil && s.matchesEvaluation(generationID, evaluation.CandidateID) && s.point.phase == "after" {
		return nil, s.crash(ctx)
	}
	return stored, err
}

func (s *crashGenerationStore) AppendPrivateGenerationResult(ctx context.Context, input storage.AppendPrivateGenerationResultInput) (*storage.SkillRevisionRecord, error) {
	if s.point.operation == "result" && s.point.phase == "before" {
		return nil, s.crash(ctx)
	}
	stored, err := s.GenerationStore.AppendPrivateGenerationResult(ctx, input)
	if err == nil && s.point.operation == "result" && s.point.phase == "after" {
		return nil, s.crash(ctx)
	}
	return stored, err
}

type queueSamplingStore struct {
	storage.GenerationStore
	calls atomic.Int32
}

type joiningQueueStore struct {
	storage.GenerationStore
	entered  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (s *joiningQueueStore) GenerationQueueStats(ctx context.Context) (storage.GenerationQueueStats, error) {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	close(s.canceled)
	<-s.release
	return storage.GenerationQueueStats{}, ctx.Err()
}

func (s *queueSamplingStore) GenerationQueueStats(ctx context.Context) (storage.GenerationQueueStats, error) {
	s.calls.Add(1)
	return s.GenerationStore.GenerationQueueStats(ctx)
}

type pollScriptStore struct {
	storage.GenerationStore
	calls       atomic.Int32
	blockedCall chan struct{}
	blockedOnce sync.Once
}

func (s *pollScriptStore) ClaimGeneration(ctx context.Context, _ storage.ClaimGenerationInput) (*storage.SkillGenerationRecord, error) {
	switch s.calls.Add(1) {
	case 1, 2:
		return nil, errors.New("temporary queue failure")
	case 3, 4:
		return nil, nil
	default:
		s.blockedOnce.Do(func() { close(s.blockedCall) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
}

type metricWorkerStore struct {
	*storage.MemoryStore
	claimGate       chan struct{}
	claimErrorSeen  chan struct{}
	processingEmpty chan struct{}
	leaseLostSeen   chan struct{}
	claimCalls      atomic.Int32
	errorOnce       sync.Once
	emptyOnce       sync.Once
	leaseOnce       sync.Once
}

func (s *metricWorkerStore) ClaimGeneration(ctx context.Context, input storage.ClaimGenerationInput) (*storage.SkillGenerationRecord, error) {
	call := s.claimCalls.Add(1)
	switch call {
	case 1:
		s.errorOnce.Do(func() { close(s.claimErrorSeen) })
		return nil, errors.New("injected queue read failure")
	case 2:
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.claimGate:
		}
	case 4:
		// Hold the poll loop after its first empty result. This makes the
		// worker's claimed/error/empty deltas exact without bypassing any
		// Worker call site or recording through Metrics directly.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	claim, err := s.MemoryStore.ClaimGeneration(ctx, input)
	if call == 3 && err == nil && claim == nil {
		s.emptyOnce.Do(func() { close(s.processingEmpty) })
	}
	return claim, err
}

func (s *metricWorkerStore) RenewGenerationLease(context.Context, string, string, time.Duration) (bool, error) {
	s.leaseOnce.Do(func() { close(s.leaseLostSeen) })
	return false, nil
}

type timeoutBlockingRenewalStore struct {
	storage.GenerationStore
	startedOnce  sync.Once
	canceledOnce sync.Once
	started      chan struct{}
	canceled     chan struct{}
}

func (s *timeoutBlockingRenewalStore) RenewGenerationLease(
	ctx context.Context,
	_ string,
	_ string,
	_ time.Duration,
) (bool, error) {
	s.startedOnce.Do(func() { close(s.started) })
	<-ctx.Done()
	s.canceledOnce.Do(func() { close(s.canceled) })
	return false, ctx.Err()
}

type prometheusSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

func metricText(metrics *generation.Metrics) string {
	var output strings.Builder
	Expect(metrics.WritePrometheus(&output)).To(Succeed())
	return output.String()
}

func metricsHTTPText(service *server.Server) string {
	recorder := httptest.NewRecorder()
	service.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	Expect(recorder.Code).To(Equal(http.StatusOK))
	return recorder.Body.String()
}

func parsePrometheus(text string) (map[string]string, []prometheusSample) {
	types := map[string]string{}
	var samples []prometheusSample
	for lineNumber, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "# HELP ") {
			continue
		}
		if strings.HasPrefix(line, "# TYPE ") {
			fields := strings.Fields(line)
			Expect(fields).To(HaveLen(4), "metric TYPE line %d: %q", lineNumber+1, line)
			types[fields[2]] = fields[3]
			continue
		}
		Expect(line).NotTo(HavePrefix("#"), "unknown Prometheus directive on line %d", lineNumber+1)
		fields := strings.Fields(line)
		Expect(fields).To(HaveLen(2), "metric sample line %d: %q", lineNumber+1, line)
		value, err := strconv.ParseFloat(fields[1], 64)
		Expect(err).NotTo(HaveOccurred(), "metric sample line %d", lineNumber+1)
		nameAndLabels := fields[0]
		name := nameAndLabels
		labels := map[string]string{}
		if open := strings.IndexByte(nameAndLabels, '{'); open >= 0 {
			Expect(nameAndLabels).To(HaveSuffix("}"), "metric sample line %d", lineNumber+1)
			name = nameAndLabels[:open]
			inside := strings.TrimSuffix(nameAndLabels[open+1:], "}")
			if inside != "" {
				for _, encoded := range strings.Split(inside, ",") {
					key, quoted, found := strings.Cut(encoded, "=")
					Expect(found).To(BeTrue(), "metric label on line %d", lineNumber+1)
					decoded, decodeErr := strconv.Unquote(quoted)
					Expect(decodeErr).NotTo(HaveOccurred(), "metric label on line %d", lineNumber+1)
					Expect(labels).NotTo(HaveKey(key), "duplicate metric label on line %d", lineNumber+1)
					labels[key] = decoded
				}
			}
		}
		samples = append(samples, prometheusSample{Name: name, Labels: labels, Value: value})
	}
	return types, samples
}

func sampleFamily(name string) string {
	for _, suffix := range []string{"_sum", "_count"} {
		if strings.HasSuffix(name, suffix) && strings.TrimSuffix(name, suffix) == generation.MetricGenerationStageDurationSeconds {
			return generation.MetricGenerationStageDurationSeconds
		}
	}
	return name
}

func sortedMapKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func stringSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func assertPrometheusContract(text string) {
	expectedTypes := map[string]string{
		generation.MetricGenerationWorkerClaimsTotal:        "counter",
		generation.MetricGenerationStageTotal:               "counter",
		generation.MetricGenerationStageDurationSeconds:     "summary",
		generation.MetricGenerationQueueDepth:               "gauge",
		generation.MetricGenerationQueueLagSeconds:          "gauge",
		generation.MetricGenerationQueueMonitorPollFailures: "gauge",
		generation.MetricGenerationWorkerReady:              "gauge",
		generation.MetricGenerationActiveWorkers:            "gauge",
		generation.MetricGenerationLeaseLostTotal:           "counter",
		generation.MetricGenerationPartialSourcesTotal:      "counter",
		generation.MetricGenerationConsecutivePollFailures:  "gauge",
		generation.MetricRevisionAppendTotal:                "counter",
		generation.MetricRevisionVisibilityTotal:            "counter",
		generation.MetricLatestMutationTotal:                "counter",
	}
	expectedLabels := map[string][]string{
		generation.MetricGenerationWorkerClaimsTotal:        {"result"},
		generation.MetricGenerationStageTotal:               {"outcome", "reason", "stage"},
		generation.MetricGenerationStageDurationSeconds:     {"stage"},
		generation.MetricGenerationQueueDepth:               {},
		generation.MetricGenerationQueueLagSeconds:          {},
		generation.MetricGenerationQueueMonitorPollFailures: {},
		generation.MetricGenerationWorkerReady:              {},
		generation.MetricGenerationActiveWorkers:            {},
		generation.MetricGenerationLeaseLostTotal:           {},
		generation.MetricGenerationPartialSourcesTotal:      {"reason", "stage"},
		generation.MetricGenerationConsecutivePollFailures:  {},
		generation.MetricRevisionAppendTotal:                {"origin", "outcome"},
		generation.MetricRevisionVisibilityTotal:            {"outcome", "transition"},
		generation.MetricLatestMutationTotal:                {"operation", "outcome"},
	}
	allowed := map[string]map[string]map[string]struct{}{
		generation.MetricGenerationWorkerClaimsTotal: {
			"result": stringSet("claimed", "empty", "error", "unknown"),
		},
		generation.MetricGenerationStageTotal: {
			"stage":   stringSet("transcript_load", "candidate_generation", "candidate_evaluation", "synthesis", "finalization", "unknown"),
			"outcome": stringSet("success", "failure", "canceled", "requeued", "claim_lost", "resource_limited", "unknown"),
			"reason":  stringSet("none", "storage", "transient", "terminal", "timeout", "attempts_exhausted", "transcript_unavailable", "candidate_generation_failed", "candidate_evaluation_failed", "synthesis_failed", "no_viable_candidates", "canceled", "claim_lost", "resource_limited", "unknown"),
		},
		generation.MetricGenerationStageDurationSeconds: {
			"stage": stringSet("transcript_load", "candidate_generation", "candidate_evaluation", "synthesis", "finalization", "unknown"),
		},
		generation.MetricGenerationPartialSourcesTotal: {
			"stage":  stringSet("transcript_load", "candidate_generation", "candidate_evaluation", "unknown"),
			"reason": stringSet("transcript_unavailable", "candidate_generation_failed", "candidate_evaluation_failed", "unknown"),
		},
		generation.MetricRevisionAppendTotal: {
			"origin":  stringSet("manual", "generation", "duplicate", "migrated", "unknown"),
			"outcome": stringSet("success", "conflict", "invalid", "not_found", "claim_lost", "error", "unknown"),
		},
		generation.MetricRevisionVisibilityTotal: {
			"transition": stringSet("private_to_public", "public_to_private", "unchanged", "unknown"),
			"outcome":    stringSet("success", "not_found", "latest_conflict", "error", "unknown"),
		},
		generation.MetricLatestMutationTotal: {
			"operation": stringSet("set", "clear", "unknown"),
			"outcome":   stringSet("success", "revision_not_found", "revision_not_public", "error", "unknown"),
		},
	}

	types, samples := parsePrometheus(text)
	Expect(types).To(Equal(expectedTypes), "only the fourteen documented metric families may be rendered")
	Expect(samples).NotTo(BeEmpty())
	seenSamples := map[string]struct{}{}
	for _, sample := range samples {
		labelParts := make([]string, 0, len(sample.Labels))
		for _, key := range sortedMapKeys(sample.Labels) {
			labelParts = append(labelParts, key+"="+strconv.Quote(sample.Labels[key]))
		}
		fingerprint := sample.Name + "{" + strings.Join(labelParts, ",") + "}"
		Expect(seenSamples).NotTo(HaveKey(fingerprint), "duplicate Prometheus sample %s", fingerprint)
		seenSamples[fingerprint] = struct{}{}

		family := sampleFamily(sample.Name)
		labels, known := expectedLabels[family]
		Expect(known).To(BeTrue(), "unexpected metric sample %s", sample.Name)
		Expect(sortedMapKeys(sample.Labels)).To(Equal(labels), "labels for %s", sample.Name)
		Expect(math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0)).To(BeFalse(), sample.Name)
		Expect(sample.Value).To(BeNumerically(">=", 0), sample.Name)
		for label, value := range sample.Labels {
			values, bounded := allowed[family][label]
			if bounded {
				Expect(values).To(HaveKey(value), "%s{%s=%q}", sample.Name, label, value)
			}
			for _, forbidden := range []string{"id", "subject", "transcript", "candidate", "generation", "revision", "error"} {
				Expect(strings.ToLower(label)).NotTo(ContainSubstring(forbidden), "%s label %q", sample.Name, label)
			}
		}
	}
}

func metricValue(text, name string, labels map[string]string) float64 {
	_, samples := parsePrometheus(text)
	var result float64
	for _, sample := range samples {
		labelsMatch := reflect.DeepEqual(sample.Labels, labels) || (len(sample.Labels) == 0 && len(labels) == 0)
		if sample.Name != name || !labelsMatch {
			continue
		}
		result += sample.Value
	}
	return result
}

func expectMetricDelta(before, after, name string, labels map[string]string, expected float64) {
	Expect(metricValue(after, name, labels)-metricValue(before, name, labels)).To(Equal(expected), "%s %#v", name, labels)
}

func workerDoJSON(service *server.Server, method, path, body, subject string) (map[string]any, int) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if subject != "" {
		request.Header.Set("x-paper-auth-subject", subject)
	}
	service.Handler().ServeHTTP(recorder, request)
	var response map[string]any
	Expect(json.Unmarshal(recorder.Body.Bytes(), &response)).To(Succeed(), recorder.Body.String())
	return response, recorder.Code
}

func evaluationResponseFromBody(body io.Reader) string {
	var request struct {
		Ref struct {
			ID string `json:"id"`
		} `json:"ref"`
		Criteria []evaluator.Criterion `json:"criteria"`
	}
	Expect(json.NewDecoder(body).Decode(&request)).To(Succeed())
	Expect(request.Criteria).To(Equal(evaluator.GenerationCandidateCriteria().Criteria))
	results := make([]map[string]any, len(request.Criteria))
	for i, criterion := range request.Criteria {
		results[i] = map[string]any{
			"criterion_id": criterion.ID, "weight": criterion.Weight,
			"passed": true, "rationale": "The criterion passed.",
		}
	}
	response, err := json.Marshal(map[string]any{
		"ref":     map[string]any{"source": "skills-cassette", "id": request.Ref.ID},
		"profile": "generation-candidate-v1", "profile_version": "1",
		"evaluator_version": "test", "score": 0.9, "decision": "pass",
		"criterion_results": results, "findings": []any{},
		"strengths": []string{"clear"}, "panel": map[string]any{},
	})
	Expect(err).NotTo(HaveOccurred())
	return string(response)
}

func createQueueGeneration(store interface {
	storage.SkillIdentityStore
	storage.GenerationStore
}, skillID, id string, createdAt time.Time) *storage.SkillGenerationRecord {
	criteria, criteriaJSON := generationCriteriaJSON()
	created, err := store.CreateGeneration(context.Background(), storage.CreateGenerationInput{
		ID: id, SkillID: skillID, CreatorSubject: workerOwner,
		Snapshot:      storage.SkillRevisionSnapshot{Name: "Queue " + id, Type: "workflow", Content: "# Queue"},
		AuthorContext: "queue semantics", EvaluatorProfile: criteria.Profile,
		EvaluatorProfileVersion: criteria.Version, EvaluationCriteria: criteriaJSON, CreatedAt: createdAt,
	})
	Expect(err).NotTo(HaveOccurred())
	return created
}

func completeClaimedGeneration(store storage.GenerationStore, claim *storage.SkillGenerationRecord) *storage.SkillRevisionRecord {
	ctx := context.Background()
	Expect(store.UpdateGenerationStatus(ctx, claim.ID, claim.ClaimToken,
		storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates)).To(Succeed())
	candidate, err := store.PutGenerationCandidate(ctx, claim.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
		ID: uuid.NewString(), Ordinal: 0, Kind: storage.GenerationCandidateContext,
		Snapshot: storage.GenerationCandidateSnapshot{
			Name: "Retained candidate", Description: "Bounded prior history.",
			Type: "workflow", Content: "# Retained candidate", IsAIGenerated: true,
		},
		Insights: json.RawMessage(`[]`), BundleSHA256: "retained-bundle",
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(store.UpdateGenerationStatus(ctx, claim.ID, claim.ClaimToken,
		storage.GenerationStatusGeneratingCandidates, storage.GenerationStatusEvaluatingCandidates)).To(Succeed())
	score := 0.9
	_, err = store.PutCandidateEvaluation(ctx, claim.ID, claim.ClaimToken, storage.CandidateEvaluationRecord{
		ID: uuid.NewString(), CandidateID: candidate.ID, RequestSHA256: "retained-request",
		Profile: claim.EvaluatorProfile, ProfileVersion: claim.EvaluatorProfileVersion,
		EvaluatorVersion: "test", Score: &score, Decision: "pass",
		CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`),
		Strengths: json.RawMessage(`["retained"]`), Panel: json.RawMessage(`{}`),
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(store.UpdateGenerationStatus(ctx, claim.ID, claim.ClaimToken,
		storage.GenerationStatusEvaluatingCandidates, storage.GenerationStatusSynthesizing)).To(Succeed())
	revision, err := store.AppendPrivateGenerationResult(ctx, storage.AppendPrivateGenerationResultInput{
		GenerationID: claim.ID, ClaimToken: claim.ClaimToken,
		InitialWinnerCandidateID: candidate.ID, ResultCandidateID: candidate.ID,
	})
	Expect(err).NotTo(HaveOccurred())
	return revision
}

var _ = Describe("leased generation worker", func() {
	It("joins its queue monitor before Run returns", func() {
		store := &joiningQueueStore{
			GenerationStore: storage.NewMemoryStore(), entered: make(chan struct{}),
			canceled: make(chan struct{}), release: make(chan struct{}),
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- generation.NewWorker(store, claimProcessorFunc(func(context.Context, string, string) (*generation.ProcessResult, error) {
				return nil, nil
			}), fastWorkerConfig(), nil, nil).Run(ctx)
		}()
		Eventually(store.entered).Should(BeClosed())
		cancel()
		Eventually(store.canceled).Should(BeClosed())
		Consistently(done).WithTimeout(30 * time.Millisecond).ShouldNot(Receive())
		close(store.release)
		Eventually(done).Should(Receive(Succeed()))
	})

	It("backs off queue-stat failures with jitter and suppresses warnings until recovery", func() {
		reader := &scriptedQueueStatsReader{}
		timers := make(chan *controlledWorkerTimer, 8)
		runtime := generation.WorkerRuntime{
			Clock:       time.Now,
			RandomFloat: func() float64 { return 0 },
			NewTimer: func(delay time.Duration) generation.WorkerTimer {
				timer := &controlledWorkerTimer{delay: delay, tick: make(chan time.Time, 1)}
				timers <- timer
				return timer
			},
		}
		config := generation.DefaultWorkerConfig()
		config.PollInterval = 10 * time.Millisecond
		config.MaxPollInterval = 40 * time.Millisecond
		metrics := generation.NewMetrics()
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- generation.NewQueueMonitorWithRuntime(reader, config, metrics, logger, runtime).Run(ctx)
		}()

		for _, expectedDelay := range []time.Duration{
			5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond,
			10 * time.Millisecond, 5 * time.Millisecond,
		} {
			var timer *controlledWorkerTimer
			Eventually(timers).Should(Receive(&timer))
			Expect(timer.delay).To(Equal(expectedDelay))
			if reader.calls.Load() < 5 {
				timer.tick <- time.Now()
			}
		}
		Expect(reader.calls.Load()).To(Equal(int32(5)))
		Expect(bytes.Count(logs.Bytes(), []byte("generation queue statistics failed"))).To(Equal(2),
			"one warning is emitted per failure streak and suppression resets after recovery")
		metricSnapshot := metricText(metrics)
		Expect(metricValue(metricSnapshot, generation.MetricGenerationQueueDepth, nil)).To(Equal(float64(7)))
		Expect(metricValue(metricSnapshot, generation.MetricGenerationQueueLagSeconds, nil)).To(Equal(float64(3)))
		Expect(metricValue(metricSnapshot, generation.MetricGenerationQueueMonitorPollFailures, nil)).To(Equal(float64(1)))

		cancel()
		Eventually(done).Should(Receive(Succeed()))
	})

	It("records creator cancellation without lease-loss metrics", func() {
		store := queuedGeneration(nil)
		processingStarted := make(chan struct{})
		processingCanceled := make(chan struct{})
		processor := claimProcessorFunc(func(ctx context.Context, _, _ string) (*generation.ProcessResult, error) {
			close(processingStarted)
			<-ctx.Done()
			close(processingCanceled)
			return nil, ctx.Err()
		})
		metrics := generation.NewMetrics()
		baseline := metricText(metrics)
		config := fastWorkerConfig()
		config.HeartbeatInterval = 5 * time.Millisecond
		config.LeaseDuration = time.Second
		ctx, stop := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- generation.NewWorker(store, processor, config, metrics, nil).Run(ctx) }()
		Eventually(processingStarted).Should(BeClosed())
		canceled, err := store.CancelSkillGeneration(context.Background(), workerOwner, workerSkillID, workerGenerationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(canceled.Generation.Status).To(Equal(storage.GenerationStatusCanceled))
		Eventually(processingCanceled).Should(BeClosed())
		stop()
		Eventually(done).Should(Receive(Succeed()))

		finalMetrics := metricText(metrics)
		expectMetricDelta(baseline, finalMetrics, generation.MetricGenerationLeaseLostTotal, nil, 0)
		expectMetricDelta(baseline, finalMetrics, generation.MetricGenerationStageTotal,
			map[string]string{"stage": "unknown", "outcome": "canceled", "reason": "canceled"}, 1)
		expectMetricDelta(baseline, finalMetrics, generation.MetricGenerationStageTotal,
			map[string]string{"stage": "unknown", "outcome": "claim_lost", "reason": "claim_lost"}, 0)
	})

	It("does not report lease loss when a heartbeat observes atomic completion", func() {
		store := queuedGeneration(nil)
		completed := make(chan struct{})
		processorCanceled := make(chan struct{})
		processor := claimProcessorFunc(func(ctx context.Context, generationID, claimToken string) (*generation.ProcessResult, error) {
			state, err := store.GetClaimedGeneration(context.Background(), generationID, claimToken)
			if err != nil {
				return nil, err
			}
			completeClaimedGeneration(store, &state.Generation)
			close(completed)
			<-ctx.Done()
			close(processorCanceled)
			return &generation.ProcessResult{}, nil
		})
		metrics := generation.NewMetrics()
		baseline := metricText(metrics)
		config := fastWorkerConfig()
		config.WorkerConcurrency = 1
		config.HeartbeatInterval = 5 * time.Millisecond
		config.LeaseDuration = time.Second
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- generation.NewWorker(store, processor, config, metrics, nil).Run(ctx) }()
		Eventually(completed).Should(BeClosed())
		Eventually(func() bool {
			state, err := store.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
			return err == nil && state.Generation.Status == storage.GenerationStatusCompleted
		}).Should(BeTrue())
		Eventually(processorCanceled).Should(BeClosed(), "the post-completion heartbeat must finish the monitor")
		cancel()
		Eventually(done).Should(Receive(Succeed()))
		finalMetrics := metricText(metrics)
		expectMetricDelta(baseline, finalMetrics, generation.MetricGenerationLeaseLostTotal, nil, 0)
		expectMetricDelta(baseline, finalMetrics, generation.MetricGenerationStageTotal,
			map[string]string{"stage": "unknown", "outcome": "claim_lost", "reason": "claim_lost"}, 0)
	})

	It("cancel_generation_fences_worker_and_retains_history", func() {
		fixture := openWorkerPostgresFixture(requireWorkerTestPostgresDSN(), "skills_cancel_worker")
		store := fixture.store
		ctx := context.Background()
		now := time.Now().UTC().Add(-time.Second)
		criteria, criteriaJSON := generationCriteriaJSON()
		skillRecord, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "cancel-blocked-processing", CreatorSubject: workerOwner, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		generationRecord, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
			ID: uuid.NewString(), SkillID: skillRecord.ID, CreatorSubject: workerOwner,
			Snapshot:      storage.SkillRevisionSnapshot{Name: "Cancelable", Type: "workflow", Content: "# Cancelable"},
			AuthorContext: "retain bounded progress", SelectedSessionIDs: []string{"session-cancel"},
			EvaluatorProfile: criteria.Profile, EvaluatorProfileVersion: criteria.Version,
			EvaluationCriteria: criteriaJSON, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		started := make(chan struct{})
		processingCanceled := make(chan struct{})
		claimToken := make(chan string, 1)
		processor := claimProcessorFunc(func(processingCtx context.Context, generationID, token string) (*generation.ProcessResult, error) {
			Expect(store.UpdateGenerationStatus(processingCtx, generationID, token,
				storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates)).To(Succeed())
			claim, claimErr := store.GetClaimedGeneration(processingCtx, generationID, token)
			if claimErr != nil {
				return nil, claimErr
			}
			candidate, _, seedErr := seedEvaluatedCandidate(store, &claim.Generation, uuid.NewString())
			if seedErr != nil {
				return nil, seedErr
			}
			if updateErr := store.UpdateGenerationSession(processingCtx, generationID, token, storage.GenerationSessionRecord{
				SessionID: "session-cancel", Status: storage.GenerationSessionEvaluated, CandidateID: candidate.ID,
			}); updateErr != nil {
				return nil, updateErr
			}
			if _, diagnosticErr := store.PutGenerationDiagnostic(processingCtx, generationID, token, storage.GenerationDiagnosticRecord{
				ID: uuid.NewString(), SessionID: "session-cancel", CandidateID: candidate.ID,
				Stage: "evaluation", Code: "evaluation_unrankable",
				Message: "The candidate evaluation could not be ranked.", Retryable: false,
			}); diagnosticErr != nil {
				return nil, diagnosticErr
			}
			claimToken <- token
			close(started)
			<-processingCtx.Done()
			close(processingCanceled)
			return nil, processingCtx.Err()
		})
		config := fastWorkerConfig()
		config.WorkerID = "postgres-cancel-worker"
		config.WorkerConcurrency = 1
		config.LeaseDuration = time.Second
		config.HeartbeatInterval = 10 * time.Millisecond
		workerCtx, stopWorker := context.WithCancel(context.Background())
		workerDone := make(chan error, 1)
		go func() { workerDone <- generation.NewWorker(store, processor, config, nil, nil).Run(workerCtx) }()
		Eventually(started).WithTimeout(3 * time.Second).Should(BeClosed())
		staleToken := <-claimToken

		canceled, err := store.CancelSkillGeneration(ctx, workerOwner, skillRecord.ID, generationRecord.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(canceled.Generation.Status).To(Equal(storage.GenerationStatusCanceled))
		renewed, err := store.RenewGenerationLease(ctx, generationRecord.ID, staleToken, config.LeaseDuration)
		Expect(renewed).To(BeFalse())
		Expect(err).To(MatchError(storage.ErrGenerationCanceled))
		Eventually(processingCanceled).WithTimeout(time.Second).Should(BeClosed())
		stopWorker()
		Eventually(workerDone).WithTimeout(time.Second).Should(Receive(Succeed()))

		retained, err := store.GetGenerationByID(ctx, workerOwner, generationRecord.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(retained.Generation.Status).To(Equal(storage.GenerationStatusCanceled))
		Expect(retained.Candidates).To(HaveLen(1))
		Expect(retained.Evaluations).To(HaveLen(1))
		Expect(retained.Diagnostics).To(HaveLen(1))
		Expect(retained.Generation.ClaimToken).To(BeEmpty())
		Expect(retained.Generation.ClaimOwner).To(BeEmpty())
		Expect(retained.Generation.LeaseExpiresAt).To(BeNil())

		renewed, err = store.RenewGenerationLease(ctx, generationRecord.ID, staleToken, time.Minute)
		Expect(renewed).To(BeFalse())
		Expect(err).To(MatchError(storage.ErrGenerationCanceled))
		_, err = store.GetClaimedGeneration(ctx, generationRecord.ID, staleToken)
		Expect(err).To(MatchError(storage.ErrGenerationCanceled))
		Expect(store.UpdateGenerationStatus(ctx, generationRecord.ID, staleToken,
			storage.GenerationStatusGeneratingCandidates, storage.GenerationStatusEvaluatingCandidates)).To(MatchError(storage.ErrGenerationCanceled))
		staleCandidate, err := store.PutGenerationCandidate(ctx, generationRecord.ID, staleToken, storage.GenerationCandidateRecord{
			ID: uuid.NewString(), Ordinal: 1, Kind: storage.GenerationCandidateContext,
			Snapshot: storage.GenerationCandidateSnapshot{Name: "Stale", Type: "workflow", Content: "# Stale"},
			Insights: json.RawMessage(`[]`), BundleSHA256: "stale",
		})
		Expect(staleCandidate).To(BeNil())
		Expect(err).To(MatchError(storage.ErrGenerationCanceled))
		staleEvaluation, err := store.PutCandidateEvaluation(ctx, generationRecord.ID, staleToken, storage.CandidateEvaluationRecord{
			ID: uuid.NewString(), CandidateID: retained.Candidates[0].ID, RequestSHA256: "stale",
			Profile: criteria.Profile, ProfileVersion: criteria.Version,
			CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`), Strengths: json.RawMessage(`[]`), Panel: json.RawMessage(`{}`),
		})
		Expect(staleEvaluation).To(BeNil())
		Expect(err).To(MatchError(storage.ErrGenerationCanceled))
		staleDiagnostic, err := store.PutGenerationDiagnostic(ctx, generationRecord.ID, staleToken, storage.GenerationDiagnosticRecord{
			ID: uuid.NewString(), Stage: "candidate", Code: "context_candidate_failed",
			Message: "A context-derived candidate could not be generated.", Retryable: false,
		})
		Expect(staleDiagnostic).To(BeNil())
		Expect(err).To(MatchError(storage.ErrGenerationCanceled))
		staleRevision, err := store.AppendPrivateGenerationResult(ctx, storage.AppendPrivateGenerationResultInput{
			GenerationID: generationRecord.ID, ClaimToken: staleToken,
			InitialWinnerCandidateID: retained.Candidates[0].ID, ResultCandidateID: retained.Candidates[0].ID,
		})
		Expect(staleRevision).To(BeNil())
		Expect(err).To(MatchError(storage.ErrGenerationCanceled))
		failed, err := store.FailGeneration(ctx, storage.FailGenerationInput{
			GenerationID: generationRecord.ID, ClaimToken: staleToken,
			Failure: storage.GenerationFailure{Code: "terminal", Message: "A safe terminal failure."},
		})
		Expect(failed).To(BeNil())
		Expect(err).To(MatchError(storage.ErrGenerationCanceled))
		requeued, err := store.RequeueGeneration(ctx, storage.RequeueGenerationInput{
			GenerationID: generationRecord.ID, ClaimToken: staleToken, RetryAfter: time.Second,
			Failure: storage.GenerationFailure{Code: "transient", Message: "A safe retryable failure."},
		})
		Expect(requeued).To(BeNil())
		Expect(err).To(MatchError(storage.ErrGenerationCanceled))

		after, err := store.GetGenerationByID(ctx, workerOwner, generationRecord.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(after).To(Equal(retained))
		revisions, err := store.ListRevisions(ctx, storage.RevisionListOpts{SkillID: skillRecord.ID, CallerSubject: workerOwner})
		Expect(err).NotTo(HaveOccurred())
		Expect(revisions).To(BeEmpty())
	})

	It("worker_recovers_expired_claim_without_duplicate_result_revision", func() {
		fixture := openWorkerPostgresFixture(requireWorkerTestPostgresDSN(), "skills_worker_recovery")
		store := fixture.store
		stage := func(from, to storage.GenerationStatus) crashPoint {
			return crashPoint{operation: "stage", from: from, to: to}
		}
		points := []crashPoint{
			stage(storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates),
			{operation: "candidate", artifactKind: storage.GenerationCandidateSession},
			stage(storage.GenerationStatusGeneratingCandidates, storage.GenerationStatusEvaluatingCandidates),
			{operation: "evaluation", artifactKind: storage.GenerationCandidateSession},
			stage(storage.GenerationStatusEvaluatingCandidates, storage.GenerationStatusSynthesizing),
			{operation: "candidate", artifactKind: storage.GenerationCandidateSynthesis},
			{operation: "evaluation", artifactKind: storage.GenerationCandidateSynthesis},
			{operation: "result"},
		}
		var cases []crashPoint
		for _, point := range points {
			for _, phase := range []string{"before", "after"} {
				point.phase = phase
				cases = append(cases, point)
			}
		}

		for ordinal, point := range cases {
			label := fmt.Sprintf("%02d-%s-%s-%s-%s-%s", ordinal, point.operation, point.artifactKind, point.phase, point.from, point.to)
			By("stopping and restarting the real Postgres worker at " + label)
			now := time.Now().UTC().Add(-time.Second)
			skillRecord, err := store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
				ID: uuid.NewString(), Slug: "worker-recovery-" + label, CreatorSubject: workerOwner, CreatedAt: now,
			})
			Expect(err).NotTo(HaveOccurred())
			base, err := store.AppendRevision(context.Background(), storage.AppendRevisionInput{
				ID: uuid.NewString(), SkillID: skillRecord.ID, CreatorSubject: workerOwner,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Recovery base", Type: "workflow", Content: "# Recovery base"},
				CreatedAt: now,
			})
			Expect(err).NotTo(HaveOccurred())
			criteria, criteriaJSON := generationCriteriaJSON()
			generationRecord, err := store.CreateGeneration(context.Background(), storage.CreateGenerationInput{
				ID: uuid.NewString(), SkillID: skillRecord.ID, BaseRevisionID: base.ID, CreatorSubject: workerOwner,
				Snapshot:      storage.SkillRevisionSnapshot{Name: "Recovery", Type: "workflow", Content: "# Recovery"},
				AuthorContext: "recover deterministic artifacts", SelectedSessionIDs: []string{"recovery-session"},
				EvaluatorProfile: criteria.Profile, EvaluatorProfileVersion: criteria.Version,
				EvaluationCriteria: criteriaJSON, CreatedAt: now,
			})
			Expect(err).NotTo(HaveOccurred())

			crashingStore := &crashGenerationStore{GenerationStore: store, point: point, reached: make(chan struct{})}
			firstProcessor := generation.NewProcessor(crashingStore,
				&fakeTranscriptLoader{transcripts: map[string]string{"recovery-session": "one bounded transcript"}, errors: map[string]error{}},
				&fakeCandidateGenerator{generateErrors: map[string]error{}},
				&fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}},
			)
			config := fastWorkerConfig()
			config.WorkerID = "crashing-worker-" + label
			config.WorkerConcurrency = 1
			config.LeaseDuration = 150 * time.Millisecond
			config.HeartbeatInterval = 25 * time.Millisecond
			config.ProcessingTimeout = 5 * time.Second
			firstCtx, stopFirst := context.WithCancel(context.Background())
			DeferCleanup(stopFirst)
			firstDone := make(chan error, 1)
			go func() {
				firstDone <- generation.NewWorker(crashingStore, firstProcessor, config, nil, nil).Run(firstCtx)
			}()
			Eventually(crashingStore.reached).WithTimeout(3 * time.Second).Should(BeClosed())
			stopFirst()
			Eventually(firstDone).WithTimeout(time.Second).Should(Receive(Succeed()))

			secondCalls := atomic.Int32{}
			secondProcessor := generation.NewProcessor(store,
				&fakeTranscriptLoader{transcripts: map[string]string{"recovery-session": "one bounded transcript"}, errors: map[string]error{}},
				&fakeCandidateGenerator{generateErrors: map[string]error{}},
				&fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}},
			)
			countingProcessor := claimProcessorFunc(func(ctx context.Context, generationID, claimToken string) (*generation.ProcessResult, error) {
				secondCalls.Add(1)
				return secondProcessor.Process(ctx, generationID, claimToken)
			})
			restartedConfig := config
			restartedConfig.WorkerID = "restarted-worker-" + label
			restartedConfig.LeaseDuration = 5 * time.Second
			restartedConfig.HeartbeatInterval = 250 * time.Millisecond
			restartedConfig.ProcessingTimeout = 10 * time.Second
			secondCtx, stopSecond := context.WithCancel(context.Background())
			DeferCleanup(stopSecond)
			secondDone := make(chan error, 1)
			go func() {
				secondDone <- generation.NewWorker(store, countingProcessor, restartedConfig, nil, nil).Run(secondCtx)
			}()
			waitUntil(4*time.Second, func() bool {
				state, getErr := store.GetGenerationByID(context.Background(), workerOwner, generationRecord.ID)
				return getErr == nil && state != nil && state.Generation.Status == storage.GenerationStatusCompleted
			})
			stopSecond()
			Eventually(secondDone).WithTimeout(time.Second).Should(Receive(Succeed()))

			state, err := store.GetGenerationByID(context.Background(), workerOwner, generationRecord.ID)
			Expect(err).NotTo(HaveOccurred())
			expectedSessionCandidateID := expectedGenerationArtifactID(generationRecord.ID, "candidate", "0")
			expectedSynthesisCandidateID := expectedGenerationArtifactID(generationRecord.ID, "candidate", "1")
			expectedSessionEvaluationID := expectedGenerationArtifactID(generationRecord.ID, "evaluation", expectedSessionCandidateID)
			expectedSynthesisEvaluationID := expectedGenerationArtifactID(generationRecord.ID, "evaluation", expectedSynthesisCandidateID)
			expectedRevisionID := expectedGenerationResultRevisionID(generationRecord.ID)
			Expect(state.Generation.Status).To(Equal(storage.GenerationStatusCompleted))
			Expect(state.Generation.WinnerCandidateID).To(Equal(expectedSessionCandidateID), label)
			Expect(state.Generation.ResultCandidateID).To(Equal(expectedSessionCandidateID), label)
			Expect(state.Generation.ResultRevisionID).To(Equal(expectedRevisionID), label)
			Expect(state.Sessions).To(ConsistOf(And(
				HaveField("SessionID", "recovery-session"),
				HaveField("Status", storage.GenerationSessionEvaluated),
				HaveField("CandidateID", expectedSessionCandidateID),
			)), label)
			Expect(state.Candidates).To(ConsistOf(
				And(
					HaveField("ID", expectedSessionCandidateID),
					HaveField("GenerationID", generationRecord.ID),
					HaveField("Ordinal", 0),
					HaveField("Kind", storage.GenerationCandidateSession),
					HaveField("SourceSessionIDs", []string{"recovery-session"}),
				),
				And(
					HaveField("ID", expectedSynthesisCandidateID),
					HaveField("GenerationID", generationRecord.ID),
					HaveField("Ordinal", 1),
					HaveField("Kind", storage.GenerationCandidateSynthesis),
					HaveField("SourceSessionIDs", []string{"recovery-session"}),
				),
			), label)
			Expect(state.Evaluations).To(ConsistOf(
				And(
					HaveField("ID", expectedSessionEvaluationID),
					HaveField("GenerationID", generationRecord.ID),
					HaveField("CandidateID", expectedSessionCandidateID),
				),
				And(
					HaveField("ID", expectedSynthesisEvaluationID),
					HaveField("GenerationID", generationRecord.ID),
					HaveField("CandidateID", expectedSynthesisCandidateID),
				),
			), label)
			revisions, err := store.ListRevisions(context.Background(), storage.RevisionListOpts{
				SkillID: skillRecord.ID, CallerSubject: workerOwner,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(revisions).To(HaveLen(2), "one base and exactly one result revision at %s", label)
			resultCount := 0
			for _, revision := range revisions {
				if revision.Revision.GenerationID == generationRecord.ID {
					resultCount++
					Expect(revision.Revision.ID).To(Equal(state.Generation.ResultRevisionID))
				}
			}
			Expect(resultCount).To(Equal(1), label)

			var candidateRows, evaluationRows, resultRevisionRows int
			schema := fmt.Sprintf("%q", fixture.schema)
			Expect(fixture.admin.QueryRow(context.Background(),
				"SELECT count(*) FROM "+schema+".generation_candidates WHERE generation_id=$1",
				generationRecord.ID).Scan(&candidateRows)).To(Succeed())
			Expect(fixture.admin.QueryRow(context.Background(),
				"SELECT count(*) FROM "+schema+".candidate_evaluations WHERE generation_id=$1",
				generationRecord.ID).Scan(&evaluationRows)).To(Succeed())
			Expect(fixture.admin.QueryRow(context.Background(),
				"SELECT count(*) FROM "+schema+".skill_revisions WHERE generation_id=$1 AND id=$2",
				generationRecord.ID, expectedRevisionID).Scan(&resultRevisionRows)).To(Succeed())
			Expect([]int{candidateRows, evaluationRows, resultRevisionRows}).To(Equal([]int{2, 2, 1}),
				"each crash boundary must converge to one session candidate/evaluation, one optional synthesis candidate/evaluation, and one result revision: %s", label)

			if point.operation == "result" && point.phase == "after" {
				Expect(state.Generation.AttemptCount).To(Equal(1), "committed finalization is terminal and not reclaimed")
				Expect(secondCalls.Load()).To(BeZero())
			} else {
				Expect(state.Generation.AttemptCount).To(Equal(2), "the restarted worker must reclaim the expired Postgres lease")
				Expect(secondCalls.Load()).To(Equal(int32(1)))
			}
		}
	})

	It("server_hosts_self_contained_worker_without_external_queue", func() {
		const natsSentinel = "nats://commit-seven-must-not-be-read.invalid:4222"
		for _, key := range []string{"NATS_URL", "CASSETTE_NATS_URL", "CASSETTE_GENERATION_NATS_URL"} {
			GinkgoT().Setenv(key, natsSentinel)
		}
		fromEnvironment := server.ConfigFromEnv()
		Expect(fmt.Sprintf("%#v", fromEnvironment)).NotTo(ContainSubstring(natsSentinel))
		var typeContainsNATS func(reflect.Type) bool
		typeContainsNATS = func(value reflect.Type) bool {
			if value.Kind() == reflect.Pointer || value.Kind() == reflect.Slice {
				return typeContainsNATS(value.Elem())
			}
			if value.Kind() != reflect.Struct {
				return false
			}
			for index := 0; index < value.NumField(); index++ {
				field := value.Field(index)
				if strings.Contains(strings.ToLower(field.Name), "nats") || typeContainsNATS(field.Type) {
					return true
				}
			}
			return false
		}
		Expect(typeContainsNATS(reflect.TypeOf(fromEnvironment))).To(BeFalse(), "ConfigFromEnv has no NATS transport field")

		type manifestContract struct {
			Config []struct {
				Key string `json:"key" toml:"key"`
			} `json:"config" toml:"config"`
		}
		var authored manifestContract
		_, err := toml.DecodeFile("../../cassette.toml", &authored)
		Expect(err).NotTo(HaveOccurred())
		manifestKeys := func(contract manifestContract) []string {
			keys := make([]string, 0, len(contract.Config))
			for _, entry := range contract.Config {
				Expect(strings.ToLower(entry.Key)).NotTo(ContainSubstring("nats"))
				keys = append(keys, entry.Key)
			}
			sort.Strings(keys)
			return keys
		}
		authoredKeys := manifestKeys(authored)
		Expect(authoredKeys).To(ContainElements("generation.worker_concurrency", "generation.candidate_concurrency"))

		store := queuedGeneration([]string{"session-a"})
		var llmCalls, evaluatorCalls atomic.Int32
		llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			llmCalls.Add(1)
			Expect(r.URL.Path).To(Equal("/v1/chat/completions"))
			w.Header().Set("Content-Type", "application/json")
			candidate := `{"skill":{"name":"Worker","description":"Use when testing workers.","tags":["worker"],"content":"## Steps\\n\\n1. Work safely."},"insights":[{"kind":"evidence","summary":"Safe work","evidence":"The source completed it."}]}`
			Expect(json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"content": candidate}}}})).To(Succeed())
		}))
		defer llm.Close()
		core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			evaluatorCalls.Add(1)
			Expect(r.URL.Path).To(Equal(evaluator.CandidateEvaluationsPath))
			w.Header().Set("Content-Type", "application/json")
			_, writeErr := io.WriteString(w, evaluationResponseFromBody(r.Body))
			Expect(writeErr).NotTo(HaveOccurred())
		}))
		defer core.Close()
		config := server.Config{
			Name: "skills", CoreURL: core.URL, Generation: fastWorkerConfig(),
			LLM: skill.LLMCallerConfig{Provider: "openai", APIKey: "test", BaseURL: llm.URL},
		}
		service := server.New(config, store, &workerQuerier{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		openAPI := httptest.NewRecorder()
		service.Handler().ServeHTTP(openAPI, httptest.NewRequest(http.MethodGet, "/openapi", nil))
		Expect(openAPI.Code).To(Equal(http.StatusOK))
		var document struct {
			Manifest manifestContract `json:"x-tapes-cassette"`
		}
		Expect(json.Unmarshal(openAPI.Body.Bytes(), &document)).To(Succeed())
		Expect(manifestKeys(document.Manifest)).To(Equal(authoredKeys), "both published manifest structs omit an external queue contract")

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- service.Serve(ctx, listener) }()
		waitUntil(2*time.Second, func() bool {
			state, getErr := store.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
			return getErr == nil && state != nil && state.Generation.Status == storage.GenerationStatusCompleted
		})
		state, err := store.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Generation.ResultRevisionID).NotTo(BeEmpty())
		Expect(llmCalls.Load()).To(BeNumerically(">=", 2))
		Expect(evaluatorCalls.Load()).To(BeNumerically(">=", 2))
		response, err := http.Get("http://" + listener.Addr().String() + "/metrics")
		Expect(err).NotTo(HaveOccurred())
		body, err := io.ReadAll(response.Body)
		Expect(err).NotTo(HaveOccurred())
		Expect(response.Body.Close()).To(Succeed())
		Expect(string(body)).To(ContainSubstring(generation.MetricGenerationWorkerClaimsTotal))
		cancel()
		Eventually(done).WithTimeout(config.Generation.DrainTimeout + 250*time.Millisecond).Should(Receive(Succeed()))
	})

	It("fails over-attempt and terminal processor outcomes through the worker", func() {
		By("failing a claim already beyond the durable attempt budget before processing")
		overAttemptStore := queuedGeneration(nil)
		firstClaim, err := overAttemptStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{
			WorkerID: "prior-worker", LeaseDuration: time.Second,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = overAttemptStore.RequeueGeneration(context.Background(), storage.RequeueGenerationInput{
			GenerationID: firstClaim.ID, ClaimToken: firstClaim.ClaimToken, RetryAfter: time.Nanosecond,
			Failure: storage.GenerationFailure{Code: "generation_retrying", Message: "Generation will be retried."},
		})
		Expect(err).NotTo(HaveOccurred())
		var processCalls atomic.Int32
		config := fastWorkerConfig()
		config.MaxAttempts = 1
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- generation.NewWorker(overAttemptStore, claimProcessorFunc(func(context.Context, string, string) (*generation.ProcessResult, error) {
				processCalls.Add(1)
				return &generation.ProcessResult{}, nil
			}), config, nil, nil).Run(ctx)
		}()
		waitUntil(time.Second, func() bool {
			state, getErr := overAttemptStore.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
			return getErr == nil && state.Generation.Status == storage.GenerationStatusFailed
		})
		cancel()
		Eventually(done).Should(Receive(Succeed()))
		state, err := overAttemptStore.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Generation.AttemptCount).To(Equal(2))
		Expect(state.Generation.ErrorCode).To(Equal("attempts_exhausted"))
		Expect(processCalls.Load()).To(BeZero())

		By("persisting every non-retryable processor outcome as a terminal failure")
		terminalStore := queuedGeneration(nil)
		terminalCtx, stopTerminal := context.WithCancel(context.Background())
		terminalDone := make(chan error, 1)
		go func() {
			terminalDone <- generation.NewWorker(terminalStore, claimProcessorFunc(func(context.Context, string, string) (*generation.ProcessResult, error) {
				return nil, &generation.ProcessError{Code: "no_viable_candidates", Message: "No candidate could be selected."}
			}), fastWorkerConfig(), nil, nil).Run(terminalCtx)
		}()
		waitUntil(time.Second, func() bool {
			state, getErr := terminalStore.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
			return getErr == nil && state.Generation.Status == storage.GenerationStatusFailed
		})
		stopTerminal()
		Eventually(terminalDone).Should(Receive(Succeed()))
		terminalState, err := terminalStore.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(terminalState.Generation.ErrorCode).To(Equal("no_viable_candidates"))
		Expect(terminalState.Generation.ErrorMessage).To(Equal("No candidate could be evaluated."))
	})

	It("samples queue metrics while all worker slots are occupied", func() {
		memory := queuedGeneration(nil)
		addQueuedGeneration(memory, uuid.NewString(), workerSkillID, workerBaseID, workerOwner, nil, time.Now().UTC())
		store := &queueSamplingStore{GenerationStore: memory}
		started := make(chan struct{})
		processor := claimProcessorFunc(func(ctx context.Context, _, _ string) (*generation.ProcessResult, error) {
			select {
			case <-started:
			default:
				close(started)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		})
		metrics := generation.NewMetrics()
		config := fastWorkerConfig()
		config.WorkerConcurrency = 1
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- generation.NewWorker(store, processor, config, metrics, nil).Run(ctx) }()
		Eventually(started).Should(BeClosed())
		Eventually(store.calls.Load).Should(BeNumerically(">=", 4),
			"queue sampling must continue even though the only processing slot is occupied")
		Eventually(func() float64 {
			return metricValue(metricText(metrics), generation.MetricGenerationQueueDepth, nil)
		}).Should(Equal(float64(1)), "the post-claim sample must report the remaining work")
		cancel()
		Eventually(done).Should(Receive(Succeed()))
	})

	It("applies exponential equal jitter to empty and failed queue polls", func() {
		memory := storage.NewMemoryStore()
		store := &pollScriptStore{GenerationStore: memory, blockedCall: make(chan struct{})}
		var (
			delaysMu sync.Mutex
			delays   []time.Duration
			random   atomic.Int32
		)
		runtime := generation.WorkerRuntime{
			Clock:       time.Now,
			RandomFloat: func() float64 { random.Add(1); return 0.25 },
			NewTimer: func(delay time.Duration) generation.WorkerTimer {
				delaysMu.Lock()
				delays = append(delays, delay)
				delaysMu.Unlock()
				return &workerTimer{timer: time.NewTimer(time.Millisecond)}
			},
		}
		config := fastWorkerConfig()
		config.PollInterval = 100 * time.Millisecond
		config.MaxPollInterval = 800 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- generation.NewWorkerWithRuntime(store, claimProcessorFunc(func(context.Context, string, string) (*generation.ProcessResult, error) {
				return nil, errors.New("must not process")
			}), config, nil, nil, runtime).Run(ctx)
		}()
		Eventually(store.blockedCall).Should(BeClosed())
		Expect(random.Load()).To(Equal(int32(4)))
		delaysMu.Lock()
		var pollDelays []time.Duration
		for _, delay := range delays {
			if delay != config.PollInterval { // queue sampler uses a fixed independent interval
				pollDelays = append(pollDelays, delay)
			}
		}
		delaysMu.Unlock()
		Expect(pollDelays).To(HaveExactElements(
			62500*time.Microsecond, 125*time.Millisecond, 250*time.Millisecond, 500*time.Millisecond,
		))
		cancel()
		Eventually(done).Should(Receive(Succeed()))
	})

	It("times out and persists failure while lease renewal is blocked", func() {
		memory := queuedGeneration(nil)
		store := &timeoutBlockingRenewalStore{
			GenerationStore: memory, started: make(chan struct{}), canceled: make(chan struct{}),
		}
		processorCanceled := make(chan struct{})
		processor := claimProcessorFunc(func(ctx context.Context, _, _ string) (*generation.ProcessResult, error) {
			<-ctx.Done()
			close(processorCanceled)
			return nil, ctx.Err()
		})
		config := fastWorkerConfig()
		config.WorkerConcurrency = 1
		config.LeaseDuration = 500 * time.Millisecond
		config.HeartbeatInterval = 5 * time.Millisecond
		config.ProcessingTimeout = 40 * time.Millisecond
		config.DrainTimeout = 100 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- generation.NewWorker(store, processor, config, nil, nil).Run(ctx) }()

		Eventually(store.started).WithTimeout(time.Second).Should(BeClosed(),
			"the heartbeat must enter the cancellation-aware blocking renewal")
		Eventually(store.canceled).WithTimeout(time.Second).Should(BeClosed(),
			"the independent processing deadline must release the blocked renewal")
		Eventually(processorCanceled).WithTimeout(time.Second).Should(BeClosed())
		Eventually(func() bool {
			state, err := memory.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
			return err == nil && state != nil && state.Generation.Status == storage.GenerationStatusFailed
		}).WithTimeout(time.Second).Should(BeTrue())
		state, err := memory.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Generation.ErrorCode).To(Equal("generation_timeout"))
		Expect(state.Generation.ErrorMessage).To(Equal("Generation processing timed out."))
		cancel()
		Eventually(done).WithTimeout(time.Second).Should(Receive(Succeed()),
			"a cancellation-aware blocked renewal must not deadlock the worker")
	})

	It("retains a worker slot until a canceled processor actually exits", func() {
		store := queuedGeneration(nil)
		addQueuedGeneration(store, uuid.NewString(), workerSkillID, workerBaseID, workerOwner, nil, time.Now().UTC())
		firstStarted := make(chan struct{})
		secondStarted := make(chan struct{})
		releaseFirst := make(chan struct{})
		var calls atomic.Int32
		processor := claimProcessorFunc(func(ctx context.Context, _, _ string) (*generation.ProcessResult, error) {
			if calls.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst // deliberately ignore cancellation until the test releases it
				return nil, ctx.Err()
			}
			close(secondStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		config := fastWorkerConfig()
		config.WorkerConcurrency = 1
		config.ProcessingTimeout = 20 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- generation.NewWorker(store, processor, config, nil, nil).Run(ctx) }()
		Eventually(firstStarted).Should(BeClosed())
		Consistently(secondStarted).WithTimeout(75*time.Millisecond).ShouldNot(BeClosed(),
			"timing out a dependency that ignores cancellation must not free its concurrency slot")
		close(releaseFirst)
		Eventually(secondStarted).Should(BeClosed())
		cancel()
		Eventually(done).Should(Receive(Succeed()))
	})

	It("worker_and_revision_reads_enforce_resource_bounds", func() {
		criteria, criteriaJSON := generationCriteriaJSON()
		Expect(generation.EffectiveCandidateConcurrency(0, 0)).To(Equal(2))
		Expect(generation.EffectiveCandidateConcurrency(64, 3)).To(Equal(3))
		Expect(generation.EffectiveCandidateConcurrency(1, 100)).To(Equal(1))

		By("failing an over-session generation before external processing")
		overSessionStore := queuedGeneration([]string{"one", "two", "three"})
		var externalCalls atomic.Int32
		processor := claimProcessorFunc(func(context.Context, string, string) (*generation.ProcessResult, error) {
			externalCalls.Add(1)
			return &generation.ProcessResult{}, nil
		})
		config := fastWorkerConfig()
		config.MaxSessions = 2
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- generation.NewWorker(overSessionStore, processor, config, nil, nil).Run(ctx) }()
		waitUntil(time.Second, func() bool {
			state, err := overSessionStore.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
			return err == nil && state.Generation.Status == storage.GenerationStatusFailed
		})
		cancel()
		Expect(<-done).To(Succeed())
		Expect(externalCalls.Load()).To(BeZero())

		By("running exactly the configured number of generation claims concurrently")
		concurrentStore := queuedGeneration(nil)
		now := time.Now().UTC()
		addQueuedGeneration(concurrentStore, "00000000-0000-0000-0000-000000000014", workerSkillID, workerBaseID, workerOwner, nil, now)
		addQueuedGeneration(concurrentStore, "00000000-0000-0000-0000-000000000015", workerSkillID, workerBaseID, workerOwner, nil, now.Add(time.Millisecond))
		var active, maximum atomic.Int32
		blockingProcessor := claimProcessorFunc(func(processingCtx context.Context, _, _ string) (*generation.ProcessResult, error) {
			current := active.Add(1)
			defer active.Add(-1)
			updateMaximum(&maximum, current)
			<-processingCtx.Done()
			return nil, processingCtx.Err()
		})
		concurrentConfig := fastWorkerConfig()
		concurrentConfig.WorkerConcurrency = 2
		concurrentCtx, stopConcurrent := context.WithCancel(context.Background())
		concurrentDone := make(chan error, 1)
		go func() {
			concurrentDone <- generation.NewWorker(concurrentStore, blockingProcessor, concurrentConfig, nil, nil).Run(concurrentCtx)
		}()
		Eventually(maximum.Load).WithTimeout(500 * time.Millisecond).Should(Equal(int32(2)))
		Consistently(maximum.Load).WithTimeout(30 * time.Millisecond).Should(BeNumerically("<=", 2))
		stopConcurrent()
		Eventually(concurrentDone).WithTimeout(concurrentConfig.DrainTimeout + 250*time.Millisecond).Should(Receive(Succeed()))

		By("independently blocking candidate generation and evaluation at the same configured cap")
		candidateStore := queuedGeneration([]string{"one", "two", "three"})
		claim, err := candidateStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{WorkerID: "candidate-cap", LeaseDuration: time.Minute})
		Expect(err).NotTo(HaveOccurred())
		generatorRelease := make(chan struct{})
		evaluatorRelease := make(chan struct{})
		var generatorReleaseOnce, evaluatorReleaseOnce sync.Once
		DeferCleanup(func() {
			generatorReleaseOnce.Do(func() { close(generatorRelease) })
			evaluatorReleaseOnce.Do(func() { close(evaluatorRelease) })
		})
		boundedGenerator := &blockingCandidateGenerator{entered: make(chan struct{}, 4), release: generatorRelease}
		boundedEvaluator := &blockingCandidateEvaluator{entered: make(chan struct{}, 4), release: evaluatorRelease}
		boundedProcessor := generation.NewProcessorWithConfig(
			candidateStore,
			&fakeTranscriptLoader{transcripts: map[string]string{"one": "one", "two": "two", "three": "three"}, errors: map[string]error{}},
			boundedGenerator, boundedEvaluator,
			generation.ProcessorConfig{CandidateConcurrency: 2}, nil,
		)
		processDone := make(chan error, 1)
		go func() {
			_, processErr := boundedProcessor.Process(context.Background(), claim.ID, claim.ClaimToken)
			processDone <- processErr
		}()
		Eventually(boundedGenerator.maximum.Load).WithTimeout(500 * time.Millisecond).Should(Equal(int32(2)))
		Consistently(boundedGenerator.maximum.Load).WithTimeout(30 * time.Millisecond).Should(Equal(int32(2)))
		Expect(len(boundedGenerator.entered)).To(Equal(2), "the third generation call remains blocked by the cap")
		generatorReleaseOnce.Do(func() { close(generatorRelease) })
		Eventually(boundedEvaluator.maximum.Load).WithTimeout(500 * time.Millisecond).Should(Equal(int32(2)))
		Consistently(boundedEvaluator.maximum.Load).WithTimeout(30 * time.Millisecond).Should(Equal(int32(2)))
		Expect(len(boundedEvaluator.entered)).To(Equal(2), "the third evaluator call is independently blocked by the cap")
		evaluatorReleaseOnce.Do(func() { close(evaluatorRelease) })
		Eventually(processDone).WithTimeout(time.Second).Should(Receive(Succeed()))
		Expect(boundedGenerator.maximum.Load()).To(Equal(int32(2)))
		Expect(boundedEvaluator.maximum.Load()).To(Equal(int32(2)))

		By("bounding transcript bytes before either local HTTP dependency is called")
		oversizedStore := queuedGeneration([]string{"session-a"})
		var oversizedLLMCalls, oversizedEvaluatorCalls atomic.Int32
		llm := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { oversizedLLMCalls.Add(1) }))
		defer llm.Close()
		core := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { oversizedEvaluatorCalls.Add(1) }))
		defer core.Close()
		oversizedConfig := server.Config{
			Name: "skills", CoreURL: core.URL, Generation: fastWorkerConfig(),
			LLM: skill.LLMCallerConfig{Provider: "openai", APIKey: "test", BaseURL: llm.URL},
		}
		oversizedConfig.Generation.MaxTranscriptBytes = 4096
		oversizedConfig.Generation.MaxAttempts = 1
		oversizedService := server.New(oversizedConfig, oversizedStore,
			&oversizedWorkerQuerier{payload: strings.Repeat("transcript-byte-", 1024)}, nil)
		oversizedListener, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		oversizedCtx, stopOversized := context.WithCancel(context.Background())
		oversizedDone := make(chan error, 1)
		go func() { oversizedDone <- oversizedService.Serve(oversizedCtx, oversizedListener) }()
		waitUntil(time.Second, func() bool {
			state, getErr := oversizedStore.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
			return getErr == nil && state.Generation.Status == storage.GenerationStatusFailed
		})
		stopOversized()
		Eventually(oversizedDone).WithTimeout(time.Second).Should(Receive(Succeed()))
		Expect(oversizedLLMCalls.Load()).To(BeZero())
		Expect(oversizedEvaluatorCalls.Load()).To(BeZero())

		By("capping revision and generation pages for direct storage callers")
		pageStore := storage.NewMemoryStore()
		pageSkill, err := pageStore.ResolveSkill(context.Background(), storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "bounded-pages", CreatorSubject: workerOwner, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		for ordinal := range 105 {
			_, err = pageStore.AppendRevision(context.Background(), storage.AppendRevisionInput{
				ID: uuid.NewString(), SkillID: pageSkill.ID, CreatorSubject: workerOwner,
				Origin:         storage.RevisionOriginManual,
				Snapshot:       storage.SkillRevisionSnapshot{Name: fmt.Sprintf("Revision %03d", ordinal), Type: "workflow", Content: "# bounded"},
				IdempotencyKey: fmt.Sprintf("revision-%03d", ordinal), CreatedAt: now.Add(time.Duration(ordinal) * time.Millisecond),
			})
			Expect(err).NotTo(HaveOccurred())
			addQueuedGeneration(pageStore, uuid.NewString(), pageSkill.ID, "", workerOwner, nil, now.Add(time.Duration(ordinal)*time.Millisecond))
		}
		revisionPage, err := pageStore.ListRevisions(context.Background(), storage.RevisionListOpts{
			SkillID: pageSkill.ID, CallerSubject: workerOwner, Limit: 1000,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(revisionPage).To(HaveLen(storage.MaxRevisionListLimit+1),
			"direct storage reads permit exactly one bounded HTTP lookahead row")
		generationPage, err := pageStore.ListSkillGenerations(context.Background(), storage.SkillGenerationListOpts{
			SkillID: pageSkill.ID, CallerSubject: workerOwner, Limit: 1000,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(generationPage.Generations).To(HaveLen(storage.MaxGenerationListLimit))

		By("bounding sessions, candidates, evaluations, and diagnostics on exact reads")
		artifactStore := storage.NewMemoryStore()
		artifactSkill, err := artifactStore.ResolveSkill(context.Background(), storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "bounded-exact-artifacts", CreatorSubject: workerOwner, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		sessions := make([]string, 100)
		for ordinal := range sessions {
			sessions[ordinal] = fmt.Sprintf("session-%03d", ordinal)
		}
		artifactGeneration, err := artifactStore.CreateGeneration(context.Background(), storage.CreateGenerationInput{
			ID: uuid.NewString(), SkillID: artifactSkill.ID, CreatorSubject: workerOwner,
			Snapshot:      storage.SkillRevisionSnapshot{Name: "Artifacts", Type: "workflow", Content: "# Artifacts"},
			AuthorContext: "bound every artifact", SelectedSessionIDs: sessions,
			EvaluatorProfile: criteria.Profile, EvaluatorProfileVersion: criteria.Version,
			EvaluationCriteria: criteriaJSON, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		artifactClaim, err := artifactStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{
			WorkerID: "artifact-bounds", LeaseDuration: time.Hour,
		})
		Expect(err).NotTo(HaveOccurred())
		candidateIDs := make([]string, 0, 101)
		for ordinal := range 101 {
			kind := storage.GenerationCandidateSession
			var sources []string
			if ordinal < len(sessions) {
				sources = []string{sessions[ordinal]}
			} else {
				kind = storage.GenerationCandidateSynthesis
				sources = append([]string(nil), sessions...)
			}
			candidate, putErr := artifactStore.PutGenerationCandidate(context.Background(), artifactGeneration.ID,
				artifactClaim.ClaimToken, storage.GenerationCandidateRecord{
					ID: uuid.NewString(), Ordinal: ordinal, Kind: kind, SourceSessionIDs: sources,
					Snapshot: storage.GenerationCandidateSnapshot{Name: fmt.Sprintf("Candidate %03d", ordinal), Type: "workflow", Content: "# Candidate"},
					Insights: json.RawMessage(`[]`), BundleSHA256: fmt.Sprintf("bundle-%03d", ordinal),
				})
			Expect(putErr).NotTo(HaveOccurred())
			candidateIDs = append(candidateIDs, candidate.ID)
			score := 0.5
			_, putErr = artifactStore.PutCandidateEvaluation(context.Background(), artifactGeneration.ID,
				artifactClaim.ClaimToken, storage.CandidateEvaluationRecord{
					ID: uuid.NewString(), CandidateID: candidate.ID, RequestSHA256: fmt.Sprintf("request-%03d", ordinal),
					Profile: criteria.Profile, ProfileVersion: criteria.Version, EvaluatorVersion: "test",
					Score: &score, Decision: "pass", CriterionResults: json.RawMessage(`[]`),
					Findings: json.RawMessage(`[]`), Strengths: json.RawMessage(`[]`), Panel: json.RawMessage(`{}`),
				})
			Expect(putErr).NotTo(HaveOccurred())
		}
		for ordinal := range 303 {
			diagnostic := storage.GenerationDiagnosticRecord{ID: uuid.NewString()}
			switch {
			case ordinal < 100:
				diagnostic.SessionID = sessions[ordinal]
				diagnostic.Stage, diagnostic.Code = "transcript", "transcript_unavailable"
				diagnostic.Message = "The selected session transcript could not be loaded."
			case ordinal < 200:
				diagnostic.SessionID = sessions[ordinal-100]
				diagnostic.Stage, diagnostic.Code = "candidate", "candidate_generation_failed"
				diagnostic.Message = "A candidate could not be generated from the selected session."
			case ordinal == 200:
				diagnostic.Stage, diagnostic.Code = "candidate", "context_candidate_failed"
				diagnostic.Message = "A context-derived candidate could not be generated."
			case ordinal < 302:
				diagnostic.CandidateID = candidateIDs[ordinal-201]
				diagnostic.Stage, diagnostic.Code = "evaluation", "evaluation_unrankable"
				diagnostic.Message = "The candidate evaluation could not be ranked."
			default:
				diagnostic.CandidateID = candidateIDs[100]
				diagnostic.Stage, diagnostic.Code = "synthesis", "synthesis_generation_failed"
				diagnostic.Message = "The optional synthesis candidate could not be generated."
			}
			diagnostic.CreatedAt = now.Add(time.Duration(ordinal) * time.Millisecond)
			_, putErr := artifactStore.PutGenerationDiagnostic(context.Background(), artifactGeneration.ID,
				artifactClaim.ClaimToken, diagnostic)
			Expect(putErr).NotTo(HaveOccurred())
		}
		boundedState, err := artifactStore.GetGenerationByID(context.Background(), workerOwner, artifactGeneration.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(boundedState.Sessions).To(HaveLen(100))
		Expect(boundedState.Candidates).To(HaveLen(101))
		Expect(boundedState.Evaluations).To(HaveLen(101))
		Expect(boundedState.Diagnostics).To(HaveLen(303))

		By("returning after the configured drain budget when processing ignores cancellation")
		drainStore := queuedGeneration(nil)
		drainStarted := make(chan struct{})
		releaseDrain := make(chan struct{})
		var releaseDrainOnce sync.Once
		DeferCleanup(func() { releaseDrainOnce.Do(func() { close(releaseDrain) }) })
		drainProcessor := claimProcessorFunc(func(context.Context, string, string) (*generation.ProcessResult, error) {
			close(drainStarted)
			<-releaseDrain
			return &generation.ProcessResult{}, nil
		})
		drainConfig := fastWorkerConfig()
		drainConfig.WorkerConcurrency = 1
		drainConfig.ProcessingTimeout = time.Hour
		drainConfig.DrainTimeout = 25 * time.Millisecond
		drainCtx, stopDrain := context.WithCancel(context.Background())
		drainDone := make(chan error, 1)
		go func() {
			drainDone <- generation.NewWorker(drainStore, drainProcessor, drainConfig, nil, nil).Run(drainCtx)
		}()
		Eventually(drainStarted).WithTimeout(time.Second).Should(BeClosed())
		stopDrain()
		Eventually(drainDone).WithTimeout(drainConfig.DrainTimeout + 150*time.Millisecond).Should(Receive(Succeed()))
		releaseDrainOnce.Do(func() { close(releaseDrain) })
	})

	It("metrics_report_queue_stage_revision_and_partial_failures", func() {
		By("driving queue, claim, poll-failure, active-worker, and lease-loss metrics through Worker rather than calling Recorder methods")
		queueClock := &fakeStoreClock{now: time.Date(2026, 9, 8, 16, 0, 0, 0, time.UTC)}
		memory := storage.NewMemoryStoreWithClock(queueClock.Now)
		criteria, criteriaJSON := generationCriteriaJSON()
		skillRecord, err := memory.ResolveSkill(context.Background(), storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "metric-worker", CreatorSubject: workerOwner, CreatedAt: queueClock.Now().Add(-5 * time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = memory.CreateGeneration(context.Background(), storage.CreateGenerationInput{
			ID: uuid.NewString(), SkillID: skillRecord.ID, CreatorSubject: workerOwner,
			Snapshot:      storage.SkillRevisionSnapshot{Name: "Metrics", Type: "workflow", Content: "# Metrics"},
			AuthorContext: "drive worker metrics", EvaluatorProfile: criteria.Profile,
			EvaluatorProfileVersion: criteria.Version, EvaluationCriteria: criteriaJSON,
			CreatedAt: queueClock.Now().Add(-5 * time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		queueClock.Advance(5 * time.Second)
		workerStore := &metricWorkerStore{
			MemoryStore: memory, claimGate: make(chan struct{}), claimErrorSeen: make(chan struct{}),
			processingEmpty: make(chan struct{}), leaseLostSeen: make(chan struct{}),
		}
		processingStarted := make(chan struct{})
		processor := claimProcessorFunc(func(ctx context.Context, _, _ string) (*generation.ProcessResult, error) {
			close(processingStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		workerMetrics := generation.NewMetrics()
		workerBaseline := metricText(workerMetrics)
		workerConfig := fastWorkerConfig()
		workerConfig.WorkerConcurrency = 1
		workerConfig.LeaseDuration = time.Minute
		// Leave enough observation time to assert the active gauge before the
		// intentionally failed renewal cancels this very short-lived fixture.
		workerConfig.HeartbeatInterval = 100 * time.Millisecond
		workerCtx, stopWorker := context.WithCancel(context.Background())
		workerDone := make(chan error, 1)
		go func() {
			workerDone <- generation.NewWorker(workerStore, processor, workerConfig, workerMetrics, nil).Run(workerCtx)
		}()
		Eventually(workerStore.claimErrorSeen).WithTimeout(time.Second).Should(BeClosed())
		Eventually(func() float64 {
			return metricValue(metricText(workerMetrics), generation.MetricGenerationWorkerReady, nil)
		}).Should(Equal(float64(1)))
		Eventually(func() float64 {
			return metricValue(metricText(workerMetrics), generation.MetricGenerationConsecutivePollFailures, nil)
		}).Should(Equal(float64(1)))
		afterPollFailure := metricText(workerMetrics)
		assertPrometheusContract(afterPollFailure)
		expectMetricDelta(workerBaseline, afterPollFailure, generation.MetricGenerationWorkerClaimsTotal, map[string]string{"result": "error"}, 1)
		Expect(metricValue(afterPollFailure, generation.MetricGenerationQueueDepth, nil)).To(Equal(float64(1)))
		Expect(metricValue(afterPollFailure, generation.MetricGenerationQueueLagSeconds, nil)).To(Equal(float64(5)))

		close(workerStore.claimGate)
		Eventually(processingStarted).WithTimeout(time.Second).Should(BeClosed())
		Eventually(func() float64 {
			return metricValue(metricText(workerMetrics), generation.MetricGenerationActiveWorkers, nil)
		}).Should(Equal(float64(1)))
		Eventually(workerStore.leaseLostSeen).WithTimeout(time.Second).Should(BeClosed())
		Eventually(workerStore.processingEmpty).WithTimeout(time.Second).Should(BeClosed())
		Eventually(func() float64 {
			return metricValue(metricText(workerMetrics), generation.MetricGenerationWorkerClaimsTotal,
				map[string]string{"result": "empty"})
		}).Should(Equal(float64(1)))
		stopWorker()
		Eventually(workerDone).WithTimeout(time.Second).Should(Receive(Succeed()))
		workerFinal := metricText(workerMetrics)
		assertPrometheusContract(workerFinal)
		expectMetricDelta(workerBaseline, workerFinal, generation.MetricGenerationWorkerClaimsTotal, map[string]string{"result": "error"}, 1)
		expectMetricDelta(workerBaseline, workerFinal, generation.MetricGenerationWorkerClaimsTotal, map[string]string{"result": "claimed"}, 1)
		expectMetricDelta(workerBaseline, workerFinal, generation.MetricGenerationWorkerClaimsTotal, map[string]string{"result": "empty"}, 1)
		expectMetricDelta(workerBaseline, workerFinal, generation.MetricGenerationLeaseLostTotal, nil, 1)
		expectMetricDelta(workerBaseline, workerFinal, generation.MetricGenerationStageTotal,
			map[string]string{"stage": "unknown", "outcome": "claim_lost", "reason": "claim_lost"}, 1)
		Expect(metricValue(workerFinal, generation.MetricGenerationQueueDepth, nil)).To(BeZero())
		Expect(metricValue(workerFinal, generation.MetricGenerationQueueLagSeconds, nil)).To(BeZero())
		Expect(metricValue(workerFinal, generation.MetricGenerationQueueMonitorPollFailures, nil)).To(BeZero())
		Expect(metricValue(workerFinal, generation.MetricGenerationWorkerReady, nil)).To(BeZero())
		Expect(metricValue(workerFinal, generation.MetricGenerationActiveWorkers, nil)).To(BeZero())
		Expect(metricValue(workerFinal, generation.MetricGenerationConsecutivePollFailures, nil)).To(BeZero())

		By("driving stage, duration, generation-append, and partial-source metrics through Processor rather than calling Recorder methods")
		processorStore := queuedGeneration([]string{"missing", "healthy"})
		claim, err := processorStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{WorkerID: "metric-processor", LeaseDuration: time.Minute})
		Expect(err).NotTo(HaveOccurred())
		processorMetrics := generation.NewMetrics()
		processorBaseline := metricText(processorMetrics)
		generationProcessor := generation.NewProcessorWithConfig(
			processorStore,
			&fakeTranscriptLoader{transcripts: map[string]string{"healthy": "bounded transcript"}, errors: map[string]error{"missing": errors.New("missing transcript")}},
			&fakeCandidateGenerator{generateErrors: map[string]error{}},
			&fakeCandidateEvaluator{outcomes: map[string]evaluationOutcome{}},
			generation.ProcessorConfig{CandidateConcurrency: 2}, processorMetrics,
		)
		result, err := generationProcessor.Process(context.Background(), claim.ID, claim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.ResultRevision.ID).NotTo(BeEmpty())
		processorFinal := metricText(processorMetrics)
		assertPrometheusContract(processorFinal)
		expectMetricDelta(processorBaseline, processorFinal, generation.MetricGenerationPartialSourcesTotal,
			map[string]string{"stage": "transcript_load", "reason": "transcript_unavailable"}, 1)
		expectMetricDelta(processorBaseline, processorFinal, generation.MetricRevisionAppendTotal,
			map[string]string{"origin": "generation", "outcome": "success"}, 1)
		expectMetricDelta(processorBaseline, processorFinal, generation.MetricGenerationStageTotal,
			map[string]string{"stage": "transcript_load", "outcome": "failure", "reason": "transcript_unavailable"}, 1)
		expectedDurationCounts := map[string]float64{
			"transcript_load": 2, "candidate_generation": 1, "candidate_evaluation": 1,
			"synthesis": 1, "finalization": 1,
		}
		for stageName, expectedCount := range expectedDurationCounts {
			expectMetricDelta(processorBaseline, processorFinal, generation.MetricGenerationStageDurationSeconds+"_count",
				map[string]string{"stage": stageName}, expectedCount)
			Expect(metricValue(processorFinal, generation.MetricGenerationStageDurationSeconds+"_sum",
				map[string]string{"stage": stageName})-
				metricValue(processorBaseline, generation.MetricGenerationStageDurationSeconds+"_sum",
					map[string]string{"stage": stageName})).To(BeNumerically(">", 0), stageName)
			expectMetricDelta(processorBaseline, processorFinal, generation.MetricGenerationStageTotal,
				map[string]string{"stage": stageName, "outcome": "success", "reason": "none"}, 1)
		}

		By("driving manual append, visibility, and latest deltas through actual HTTP handlers rather than storage mutators")
		routeStore := storage.NewMemoryStore()
		routeService := server.New(server.Config{}, routeStore, nil, nil)
		routeBaseline := metricsHTTPText(routeService)
		identity, status := workerDoJSON(routeService, http.MethodPost, "/api/skills", `{"slug":"metric-routes"}`, workerOwner)
		Expect(status).To(Equal(http.StatusOK), identity)
		skillID := identity["id"].(string)
		appendBody := `{"snapshot":{"name":"Metric revision","description":"bounded","type":"workflow","tags":[],"content":"# Metric","isAiGenerated":false,"sourceSessionIds":[]},"changeNote":"metric","idempotencyKey":"metric-append"}`
		revision, status := workerDoJSON(routeService, http.MethodPost, "/api/skills/"+skillID+"/revisions", appendBody, workerOwner)
		Expect(status).To(Equal(http.StatusCreated), revision)
		revisionID := revision["id"].(string)
		_, status = workerDoJSON(routeService, http.MethodPut, "/api/skills/"+skillID+"/revisions/"+revisionID+"/visibility", `{"isPublic":true}`, workerOwner)
		Expect(status).To(Equal(http.StatusOK))
		_, status = workerDoJSON(routeService, http.MethodPut, "/api/skills/"+skillID+"/revisions/"+revisionID+"/visibility", `{"isPublic":true}`, workerOwner)
		Expect(status).To(Equal(http.StatusOK))
		_, status = workerDoJSON(routeService, http.MethodPut, "/api/skills/"+skillID+"/latest", `{"revisionId":"`+revisionID+`"}`, workerOwner)
		Expect(status).To(Equal(http.StatusOK))
		_, status = workerDoJSON(routeService, http.MethodDelete, "/api/skills/"+skillID+"/latest", "", workerOwner)
		Expect(status).To(Equal(http.StatusOK))
		_, status = workerDoJSON(routeService, http.MethodPut, "/api/skills/"+skillID+"/revisions/"+revisionID+"/visibility", `{"isPublic":false}`, workerOwner)
		Expect(status).To(Equal(http.StatusOK))
		_, status = workerDoJSON(routeService, http.MethodPut, "/api/skills/"+skillID+"/revisions/"+revisionID+"/visibility", `{"isPublic":false}`, workerOwner)
		Expect(status).To(Equal(http.StatusOK))
		routeFinal := metricsHTTPText(routeService)
		assertPrometheusContract(routeFinal)
		expectMetricDelta(routeBaseline, routeFinal, generation.MetricRevisionAppendTotal,
			map[string]string{"origin": "manual", "outcome": "success"}, 1)
		expectMetricDelta(routeBaseline, routeFinal, generation.MetricRevisionVisibilityTotal,
			map[string]string{"transition": "private_to_public", "outcome": "success"}, 1)
		expectMetricDelta(routeBaseline, routeFinal, generation.MetricRevisionVisibilityTotal,
			map[string]string{"transition": "public_to_private", "outcome": "success"}, 1)
		expectMetricDelta(routeBaseline, routeFinal, generation.MetricRevisionVisibilityTotal,
			map[string]string{"transition": "unchanged", "outcome": "success"}, 2)
		expectMetricDelta(routeBaseline, routeFinal, generation.MetricLatestMutationTotal,
			map[string]string{"operation": "set", "outcome": "success"}, 1)
		expectMetricDelta(routeBaseline, routeFinal, generation.MetricLatestMutationTotal,
			map[string]string{"operation": "clear", "outcome": "success"}, 1)
	})

	It("diagnostics_and_metrics_exclude_sensitive_values", func() {
		const (
			rawProviderError  = "provider-stacktrace-secret"
			providerRequestID = "provider-request-id-secret"
			rawTranscript     = "raw-transcript-secret"
			requestSHA        = "request-sha-secret"
			panelSecret       = "provider-panel-secret"
			diagnosticID      = "00000000-0000-0000-0000-000000000099"
		)
		store := queuedGeneration([]string{"opaque-session-id"})
		var observedClaimToken atomic.Value
		processor := claimProcessorFunc(func(ctx context.Context, generationID, claimToken string) (*generation.ProcessResult, error) {
			observedClaimToken.Store(claimToken)
			if err := store.UpdateGenerationStatus(ctx, generationID, claimToken,
				storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates); err != nil {
				return nil, err
			}
			state, err := store.GetClaimedGeneration(ctx, generationID, claimToken)
			if err != nil {
				return nil, err
			}
			candidate, err := store.PutGenerationCandidate(ctx, generationID, claimToken, storage.GenerationCandidateRecord{
				ID: uuid.NewString(), Ordinal: 0, Kind: storage.GenerationCandidateSession,
				SourceSessionIDs: []string{"opaque-session-id"},
				Snapshot:         storage.GenerationCandidateSnapshot{Name: "Safe candidate", Type: "workflow", Content: "# Safe"},
				Insights:         json.RawMessage(`[]`), BundleSHA256: "bounded-bundle",
			})
			if err != nil {
				return nil, err
			}
			score := 0.5
			_, err = store.PutCandidateEvaluation(ctx, generationID, claimToken, storage.CandidateEvaluationRecord{
				ID: uuid.NewString(), CandidateID: candidate.ID, RequestSHA256: requestSHA,
				Profile: state.Generation.EvaluatorProfile, ProfileVersion: state.Generation.EvaluatorProfileVersion,
				EvaluatorVersion: "safe", Score: &score, Decision: "revise",
				CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`), Strengths: json.RawMessage(`[]`),
				Panel: json.RawMessage(`{"raw":"` + rawTranscript + `","provider":"` + panelSecret + `","providerRequestId":"` + providerRequestID + `"}`),
			})
			if err != nil {
				return nil, err
			}
			_, err = store.PutGenerationDiagnostic(ctx, generationID, claimToken, storage.GenerationDiagnosticRecord{
				ID: diagnosticID, SessionID: "opaque-session-id", Stage: "candidate", Code: "candidate_generation_failed",
				Message: "A candidate could not be generated from the selected session.", Retryable: false,
			})
			if err != nil {
				return nil, err
			}
			return nil, &generation.ProcessError{
				Code:      rawProviderError + providerRequestID + workerGenerationID,
				Message:   strings.Repeat(rawProviderError+providerRequestID+rawTranscript+workerOwner, 64),
				Retryable: false,
			}
		})
		metrics := generation.NewMetrics()
		baseline := metricText(metrics)
		config := fastWorkerConfig()
		config.MaxAttempts = 1
		config.WorkerConcurrency = 1
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- generation.NewWorker(store, processor, config, metrics, slog.New(slog.NewTextHandler(io.Discard, nil))).Run(ctx)
		}()
		waitUntil(time.Second, func() bool {
			state, err := store.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
			return err == nil && state.Generation.Status == storage.GenerationStatusFailed
		})
		cancel()
		Eventually(done).Should(Receive(Succeed()))

		const expectedCode = "generation_failed"
		const expectedMessage = "Generation could not be completed."
		state, err := store.GetGenerationByID(context.Background(), workerOwner, workerGenerationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Generation.ErrorCode).To(Equal(expectedCode))
		Expect(state.Generation.ErrorMessage).To(Equal(expectedMessage))
		Expect(state.Generation.ErrorCode).NotTo(BeEmpty())
		Expect(state.Generation.ErrorMessage).NotTo(BeEmpty())
		Expect(state.Diagnostics).To(ContainElement(And(
			HaveField("Code", "candidate_generation_failed"),
			HaveField("Message", "A candidate could not be generated from the selected session."),
		)))
		Expect(state.Evaluations).To(HaveLen(1))
		Expect(state.Evaluations[0].RequestSHA256).To(Equal(requestSHA), "private worker identity remains internal persistence")
		Expect(state.Evaluations[0].Panel).To(SatisfyAny(BeEmpty(), MatchJSON(`{}`)),
			"opaque evaluator panels are not generation-history persistence")
		persistedStateJSON, err := json.Marshal(state)
		Expect(err).NotTo(HaveOccurred())
		for _, forbidden := range []string{rawProviderError, providerRequestID, rawTranscript, panelSecret} {
			Expect(string(persistedStateJSON)).NotTo(ContainSubstring(forbidden), "persisted generation state exposed %q", forbidden)
		}
		claimToken, ok := observedClaimToken.Load().(string)
		Expect(ok).To(BeTrue())
		Expect(claimToken).NotTo(BeEmpty())

		metricsText := metricText(metrics)
		assertPrometheusContract(metricsText)
		expectMetricDelta(baseline, metricsText, generation.MetricGenerationStageTotal,
			map[string]string{"stage": "unknown", "outcome": "failure", "reason": "terminal"}, 1)

		service := server.New(server.Config{}, store, nil, nil)
		wireResponses := map[string]*httptest.ResponseRecorder{}
		for name, path := range map[string]string{
			"nested": "/api/skills/" + workerSkillID + "/generations/" + workerGenerationID,
			"direct": "/api/skills/generations/" + workerGenerationID,
			"list":   "/api/skills/" + workerSkillID + "/generations",
		} {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, path, nil)
			request.Header.Set("x-paper-auth-subject", workerOwner)
			service.Handler().ServeHTTP(recorder, request)
			Expect(recorder.Code).To(Equal(http.StatusOK), "%s: %s", name, recorder.Body.String())
			wireResponses[name] = recorder
		}
		for name, recorder := range wireResponses {
			body := recorder.Body.String()
			var decoded map[string]any
			Expect(json.Unmarshal(recorder.Body.Bytes(), &decoded)).To(Succeed(), name)
			failureOwner := decoded
			if name == "list" {
				generations, listOK := decoded["generations"].([]any)
				Expect(listOK).To(BeTrue(), "%s: %#v", name, decoded)
				Expect(generations).To(HaveLen(1))
				failureOwner, listOK = generations[0].(map[string]any)
				Expect(listOK).To(BeTrue(), "%s: %#v", name, generations[0])
			}
			failure, failureOK := failureOwner["failure"].(map[string]any)
			Expect(failureOK).To(BeTrue(), "%s: %#v", name, failureOwner)
			Expect(failure).To(Equal(map[string]any{"code": expectedCode, "message": expectedMessage}),
				"%s must return the same nonempty curated terminal failure persisted by Worker", name)
			for _, forbidden := range []string{
				rawProviderError, providerRequestID, rawTranscript, requestSHA, panelSecret,
				diagnosticID, workerOwner, claimToken, config.WorkerID,
				`"requestSHA256"`, `"panel"`, `"creatorSubject"`, `"claimToken"`, `"claimOwner"`,
			} {
				Expect(body).NotTo(ContainSubstring(forbidden), "%s exposed %q", name, forbidden)
			}
		}
		for _, forbidden := range []string{
			rawProviderError, providerRequestID, rawTranscript, requestSHA, panelSecret,
			diagnosticID, workerOwner, claimToken, config.WorkerID, workerGenerationID, workerSkillID,
		} {
			Expect(metricsText).NotTo(ContainSubstring(forbidden))
		}

		openAPI := httptest.NewRecorder()
		service.Handler().ServeHTTP(openAPI, httptest.NewRequest(http.MethodGet, "/openapi", nil))
		Expect(openAPI.Code).To(Equal(http.StatusOK))
		var document any
		Expect(json.Unmarshal(openAPI.Body.Bytes(), &document)).To(Succeed())
		keys := map[string]struct{}{}
		var collectKeys func(any)
		collectKeys = func(value any) {
			switch typed := value.(type) {
			case map[string]any:
				for key, child := range typed {
					keys[key] = struct{}{}
					collectKeys(child)
				}
			case []any:
				for _, child := range typed {
					collectKeys(child)
				}
			}
		}
		collectKeys(document)
		for _, forbidden := range []string{
			rawProviderError, providerRequestID, rawTranscript, requestSHA, panelSecret,
			diagnosticID, workerOwner, claimToken, config.WorkerID,
		} {
			Expect(openAPI.Body.String()).NotTo(ContainSubstring(forbidden))
		}
		for _, forbidden := range []string{
			"requestSHA256", "requestId", "providerRequestId", "panel", "creatorSubject",
			"claimToken", "claimOwner", "leaseExpiresAt", "lastHeartbeatAt", "diagnosticId",
			"rawError", "providerError", "transcript", "transcripts",
		} {
			Expect(keys).NotTo(HaveKey(forbidden))
		}
	})

	It("worker_backoff_is_bounded_and_generation_failures_are_isolated", func() {
		By("using one injected random value for each of at least three persistent requeues")
		clock := &fakeStoreClock{now: time.Date(2026, 9, 8, 14, 0, 0, 0, time.UTC)}
		memory := storage.NewMemoryStoreWithClock(clock.Now)
		store := &recordingSchedulingStore{MemoryStore: memory}
		store.advance = func(delay time.Duration) { clock.Advance(delay + time.Nanosecond) }
		criteria, criteriaJSON := generationCriteriaJSON()
		skillRecord, err := store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "persistent-jitter", CreatorSubject: workerOwner, CreatedAt: clock.Now().Add(-time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		badID := "00000000-0000-0000-0000-000000000021"
		healthyID := "00000000-0000-0000-0000-000000000022"
		for ordinal, id := range []string{badID, healthyID} {
			_, err = store.CreateGeneration(context.Background(), storage.CreateGenerationInput{
				ID: id, SkillID: skillRecord.ID, CreatorSubject: workerOwner,
				Snapshot:      storage.SkillRevisionSnapshot{Name: "Retry", Type: "workflow", Content: "# Retry"},
				AuthorContext: "isolate durable failures", EvaluatorProfile: criteria.Profile,
				EvaluatorProfileVersion: criteria.Version, EvaluationCriteria: criteriaJSON,
				CreatedAt: clock.Now().Add(time.Duration(ordinal-2) * time.Second),
			})
			Expect(err).NotTo(HaveOccurred())
		}
		var badCalls atomic.Int32
		healthyProcessed := make(chan struct{})
		var healthyOnce sync.Once
		processor := claimProcessorFunc(func(ctx context.Context, generationID, _ string) (*generation.ProcessResult, error) {
			if generationID == badID {
				badCalls.Add(1)
				return nil, &generation.ProcessError{Code: "raw-transient-provider-error", Message: "raw transient detail", Retryable: true}
			}
			_, cancelErr := store.CancelSkillGeneration(ctx, workerOwner, skillRecord.ID, healthyID)
			if cancelErr != nil {
				return nil, cancelErr
			}
			healthyOnce.Do(func() { close(healthyProcessed) })
			return &generation.ProcessResult{}, nil
		})
		randomValues := []float64{0, 0.5, 0.75}
		var randomMu sync.Mutex
		var randomCalls int
		runtime := generation.WorkerRuntime{
			Clock: clock.Now,
			RandomFloat: func() float64 {
				randomMu.Lock()
				defer randomMu.Unlock()
				value := 0.5
				if randomCalls < len(randomValues) {
					value = randomValues[randomCalls]
				}
				randomCalls++
				return value
			},
			NewTimer: func(time.Duration) generation.WorkerTimer {
				return &workerTimer{timer: time.NewTimer(time.Millisecond)}
			},
		}
		config := fastWorkerConfig()
		config.WorkerConcurrency = 1
		config.LeaseDuration = 2 * time.Hour
		config.HeartbeatInterval = time.Hour
		config.ProcessingTimeout = 3 * time.Hour
		config.RetryBackoff = time.Second
		config.MaxRetryBackoff = 3 * time.Second
		config.MaxAttempts = 4
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- generation.NewWorkerWithRuntime(store, processor, config, nil, nil, runtime).Run(ctx) }()
		Eventually(healthyProcessed).WithTimeout(time.Second).Should(BeClosed(), "requeued work must not starve an unrelated healthy generation")
		waitUntil(2*time.Second, func() bool {
			state, getErr := store.GetGenerationByID(context.Background(), workerOwner, badID)
			return getErr == nil && state.Generation.Status == storage.GenerationStatusFailed
		})
		cancel()
		Eventually(done).WithTimeout(time.Second).Should(Receive(Succeed()))
		requeues, failures := store.schedulingCalls()
		Expect(requeues).To(HaveLen(3))
		Expect(failures).To(HaveLen(1))
		Expect(badCalls.Load()).To(Equal(int32(4)))
		randomMu.Lock()
		Expect(randomCalls).To(BeNumerically(">=", 3))
		randomMu.Unlock()
		expectedDelays := []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond, 2625 * time.Millisecond}
		actualDelays := make([]time.Duration, len(requeues))
		for index, requeue := range requeues {
			actualDelays[index] = requeue.RetryAfter
			Expect(requeue.Failure.Code).To(Equal("generation_retrying"))
			Expect(requeue.Failure.Message).To(Equal("Generation will be retried."))
		}
		Expect(actualDelays).To(Equal(expectedDelays), "base*2^(attempt-1), capped before one [0.5,1.0) jitter sample")
		Expect(failures[0].Failure).To(Equal(storage.GenerationFailure{
			Code: "attempts_exhausted", Message: "Generation retry attempts were exhausted.",
		}))
		badState, err := store.GetGenerationByID(context.Background(), workerOwner, badID)
		Expect(err).NotTo(HaveOccurred())
		Expect(badState.Generation.AttemptCount).To(Equal(4))
		Expect(badState.Generation.ErrorCode).To(Equal("attempts_exhausted"))
		Expect(badState.Generation.ErrorMessage).To(Equal("Generation retry attempts were exhausted."))
		healthyState, err := store.GetGenerationByID(context.Background(), workerOwner, healthyID)
		Expect(err).NotTo(HaveOccurred())
		Expect(healthyState.Generation.Status).To(Equal(storage.GenerationStatusCanceled))

		By("enforcing exact due/future/active/expired/terminal/empty lag semantics with the memory storage clock")
		queueFinal := time.Date(2026, 9, 8, 18, 0, 0, 0, time.UTC)
		queueClock := &fakeStoreClock{now: queueFinal.Add(-2 * time.Minute)}
		queueStore := storage.NewMemoryStoreWithClock(queueClock.Now)
		queueSkill, err := queueStore.ResolveSkill(context.Background(), storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "memory-queue-stats", CreatorSubject: workerOwner, CreatedAt: queueClock.Now(),
		})
		Expect(err).NotTo(HaveOccurred())
		active := createQueueGeneration(queueStore, queueSkill.ID, uuid.NewString(), queueClock.Now())
		activeClaim, err := queueStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{WorkerID: "active", LeaseDuration: time.Hour})
		Expect(err).NotTo(HaveOccurred())
		Expect(activeClaim.ID).To(Equal(active.ID))
		failedGeneration := createQueueGeneration(queueStore, queueSkill.ID, uuid.NewString(), queueClock.Now().Add(-4*time.Second))
		failedClaim, err := queueStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{WorkerID: "failed", LeaseDuration: time.Hour})
		Expect(err).NotTo(HaveOccurred())
		Expect(failedClaim.ID).To(Equal(failedGeneration.ID))
		failedRecord, err := queueStore.FailGeneration(context.Background(), storage.FailGenerationInput{
			GenerationID: failedClaim.ID, ClaimToken: failedClaim.ClaimToken,
			Failure: storage.GenerationFailure{Code: "generation_failed", Message: "Generation could not be completed."},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(failedRecord.Status).To(Equal(storage.GenerationStatusFailed))
		canceledGeneration := createQueueGeneration(queueStore, queueSkill.ID, uuid.NewString(), queueClock.Now().Add(-3*time.Second))
		canceledState, err := queueStore.CancelSkillGeneration(context.Background(), workerOwner, queueSkill.ID, canceledGeneration.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(canceledState.Generation.Status).To(Equal(storage.GenerationStatusCanceled))
		completedGeneration := createQueueGeneration(queueStore, queueSkill.ID, uuid.NewString(), queueClock.Now().Add(-2*time.Second))
		completedClaim, err := queueStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{WorkerID: "completed", LeaseDuration: time.Hour})
		Expect(err).NotTo(HaveOccurred())
		Expect(completedClaim.ID).To(Equal(completedGeneration.ID))
		completeClaimedGeneration(queueStore, completedClaim)
		completedState, err := queueStore.GetGenerationByID(context.Background(), workerOwner, completedGeneration.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(completedState.Generation.Status).To(Equal(storage.GenerationStatusCompleted))
		expiredGeneration := createQueueGeneration(queueStore, queueSkill.ID, uuid.NewString(), queueClock.Now().Add(-time.Second))
		expiredClaim, err := queueStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{WorkerID: "expired", LeaseDuration: time.Minute})
		Expect(err).NotTo(HaveOccurred())
		Expect(expiredClaim.ID).To(Equal(expiredGeneration.ID))
		futureGeneration := createQueueGeneration(queueStore, queueSkill.ID, uuid.NewString(), queueClock.Now().Add(10*time.Minute))
		futureClaim, err := queueStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{WorkerID: "future", LeaseDuration: time.Hour})
		Expect(err).NotTo(HaveOccurred())
		Expect(futureClaim.ID).To(Equal(futureGeneration.ID))
		futureGeneration, err = queueStore.RequeueGeneration(context.Background(), storage.RequeueGenerationInput{
			GenerationID: futureClaim.ID, ClaimToken: futureClaim.ClaimToken, RetryAfter: 12 * time.Minute,
			Failure: storage.GenerationFailure{Code: "generation_retrying", Message: "Generation will be retried."},
		})
		Expect(err).NotTo(HaveOccurred())
		queueClock.Advance(2 * time.Minute)
		dueGeneration := createQueueGeneration(queueStore, queueSkill.ID, uuid.NewString(), queueFinal.Add(-10*time.Second))
		Expect(activeClaim.LeaseExpiresAt).To(HaveValue(BeTemporally(">", queueFinal)))
		Expect(expiredClaim.LeaseExpiresAt).To(HaveValue(BeTemporally("<", queueFinal)))
		Expect(dueGeneration.NextAttemptAt).To(BeTemporally("<=", queueFinal))
		Expect(futureGeneration.NextAttemptAt).To(BeTemporally(">", queueFinal))
		stats, err := queueStore.GenerationQueueStats(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(stats.Depth).To(Equal(int64(2)), "only due unclaimed and due expired-lease rows are queued")
		Expect(stats.Lag).To(Equal(queueFinal.Sub(expiredGeneration.NextAttemptAt)), "lag uses the oldest eligible next_attempt_at and one storage-clock sample")
		_, err = queueStore.CancelSkillGeneration(context.Background(), workerOwner, queueSkill.ID, dueGeneration.ID)
		Expect(err).NotTo(HaveOccurred())
		reclaimedExpired, err := queueStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{WorkerID: "reclaimed-expired", LeaseDuration: time.Hour})
		Expect(err).NotTo(HaveOccurred())
		Expect(reclaimedExpired.ID).To(Equal(expiredGeneration.ID))
		emptyStats, err := queueStore.GenerationQueueStats(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(emptyStats).To(Equal(storage.GenerationQueueStats{Depth: 0, Lag: 0}))

		dsn := workerTestPostgresDSN()
		if dsn == "" {
			return
		}

		By("running the real-Postgres queue statistics fixture when the integration gate provides its DSN")
		postgresFixture := openWorkerPostgresFixture(dsn, "skills_queue_stats")
		postgresStore := postgresFixture.store
		postgresSkill, err := postgresStore.ResolveSkill(context.Background(), storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "postgres-queue-stats", CreatorSubject: workerOwner, CreatedAt: time.Now().UTC(),
		})
		Expect(err).NotTo(HaveOccurred())
		ids := map[string]string{}
		for _, kind := range []string{"due", "future", "active", "expired", "failed", "canceled", "completed"} {
			ids[kind] = uuid.NewString()
			createQueueGeneration(postgresStore, postgresSkill.ID, ids[kind], time.Now().UTC())
		}
		table := fmt.Sprintf("%q.skill_generations", postgresFixture.schema)
		resetPostgresQueue := func() {
			_, resetErr := postgresFixture.admin.Exec(context.Background(), "UPDATE "+table+" SET status='canceled', next_attempt_at=clock_timestamp()-interval '1 hour', claim_token=NULL, claim_owner='', lease_expires_at=NULL")
			Expect(resetErr).NotTo(HaveOccurred())
		}
		configurePostgresRow := func(kind, assignment string) {
			_, configureErr := postgresFixture.admin.Exec(context.Background(),
				"UPDATE "+table+" SET "+assignment+" WHERE id=$1", ids[kind])
			Expect(configureErr).NotTo(HaveOccurred(), kind)
		}
		assertPostgresEmpty := func(kind string) {
			stats, statsErr := postgresStore.GenerationQueueStats(context.Background())
			Expect(statsErr).NotTo(HaveOccurred(), kind)
			Expect(stats).To(Equal(storage.GenerationQueueStats{Depth: 0, Lag: 0}), kind)
		}
		assertPostgresSingleDue := func(kind string, minimumAge time.Duration) {
			var before, after, nextAttempt time.Time
			Expect(postgresFixture.admin.QueryRow(context.Background(),
				"SELECT clock_timestamp(), next_attempt_at FROM "+table+" WHERE id=$1", ids[kind]).
				Scan(&before, &nextAttempt)).To(Succeed())
			stats, statsErr := postgresStore.GenerationQueueStats(context.Background())
			Expect(statsErr).NotTo(HaveOccurred(), kind)
			Expect(postgresFixture.admin.QueryRow(context.Background(), "SELECT clock_timestamp()").Scan(&after)).To(Succeed())
			Expect(stats.Depth).To(Equal(int64(1)), kind)
			Expect(before.Sub(nextAttempt)).To(BeNumerically(">=", minimumAge), kind)
			Expect(stats.Lag).To(BeNumerically(">=", before.Sub(nextAttempt)), "%s lag predates the queue-stats query", kind)
			Expect(stats.Lag).To(BeNumerically("<=", after.Sub(nextAttempt)), "%s lag postdates the queue-stats query", kind)
		}

		resetPostgresQueue()
		assertPostgresEmpty("zero")

		resetPostgresQueue()
		configurePostgresRow("due", "status='queued', next_attempt_at=clock_timestamp()-interval '10 seconds', claim_token=NULL, claim_owner='', lease_expires_at=NULL")
		assertPostgresSingleDue("due", 10*time.Second)

		resetPostgresQueue()
		configurePostgresRow("future", "status='queued', next_attempt_at=clock_timestamp()+interval '10 minutes', claim_token=NULL, claim_owner='', lease_expires_at=NULL")
		assertPostgresEmpty("future next_attempt_at")

		resetPostgresQueue()
		configurePostgresRow("active", "status='generating_candidates', next_attempt_at=clock_timestamp()-interval '30 seconds', claim_token='00000000-0000-0000-0000-000000000031'::uuid, claim_owner='active', lease_expires_at=clock_timestamp()+interval '10 minutes'")
		assertPostgresEmpty("active lease")

		resetPostgresQueue()
		configurePostgresRow("expired", "status='evaluating_candidates', next_attempt_at=clock_timestamp()-interval '20 seconds', claim_token='00000000-0000-0000-0000-000000000032'::uuid, claim_owner='expired', lease_expires_at=clock_timestamp()-interval '1 second'")
		assertPostgresSingleDue("expired", 20*time.Second)

		for _, terminal := range []string{"failed", "canceled", "completed"} {
			resetPostgresQueue()
			configurePostgresRow(terminal, "status='"+terminal+"', next_attempt_at=clock_timestamp()-interval '1 hour', claim_token=NULL, claim_owner='', lease_expires_at=NULL")
			assertPostgresEmpty("terminal " + terminal)
		}

		resetPostgresQueue()
		configurePostgresRow("due", "status='queued', next_attempt_at=clock_timestamp()-interval '10 seconds', claim_token=NULL, claim_owner='', lease_expires_at=NULL")
		configurePostgresRow("future", "status='queued', next_attempt_at=clock_timestamp()+interval '10 minutes', claim_token=NULL, claim_owner='', lease_expires_at=NULL")
		configurePostgresRow("active", "status='generating_candidates', next_attempt_at=clock_timestamp()-interval '30 seconds', claim_token='00000000-0000-0000-0000-000000000031'::uuid, claim_owner='active', lease_expires_at=clock_timestamp()+interval '10 minutes'")
		configurePostgresRow("expired", "status='evaluating_candidates', next_attempt_at=clock_timestamp()-interval '20 seconds', claim_token='00000000-0000-0000-0000-000000000032'::uuid, claim_owner='expired', lease_expires_at=clock_timestamp()-interval '1 second'")
		for _, terminal := range []string{"failed", "canceled", "completed"} {
			configurePostgresRow(terminal, "status='"+terminal+"', next_attempt_at=clock_timestamp()-interval '1 hour', claim_token=NULL, claim_owner='', lease_expires_at=NULL")
		}
		rows, err := postgresFixture.admin.Query(context.Background(), "SELECT id::text FROM "+table+" WHERE status IN ('queued','generating_candidates','evaluating_candidates','synthesizing') AND next_attempt_at <= clock_timestamp() AND (lease_expires_at IS NULL OR lease_expires_at <= clock_timestamp()) ORDER BY id")
		Expect(err).NotTo(HaveOccurred())
		var eligible []string
		for rows.Next() {
			var id string
			Expect(rows.Scan(&id)).To(Succeed())
			eligible = append(eligible, id)
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		rows.Close()
		Expect(eligible).To(ConsistOf(ids["due"], ids["expired"]), "the combined SQL fixture covers every queue category")
		var before, after, oldestDue time.Time
		Expect(postgresFixture.admin.QueryRow(context.Background(),
			"SELECT clock_timestamp(), next_attempt_at FROM "+table+" WHERE id=$1", ids["expired"]).
			Scan(&before, &oldestDue)).To(Succeed())
		postgresStats, err := postgresStore.GenerationQueueStats(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(postgresFixture.admin.QueryRow(context.Background(), "SELECT clock_timestamp()").Scan(&after)).To(Succeed())
		Expect(postgresStats.Depth).To(Equal(int64(2)))
		Expect(postgresStats.Lag).To(BeNumerically(">=", before.Sub(oldestDue)))
		Expect(postgresStats.Lag).To(BeNumerically("<=", after.Sub(oldestDue)))
		resetPostgresQueue()
		assertPostgresEmpty("zero after eligible rows become terminal")
	})
})

type workerQuerier struct{}

func (*workerQuerier) TraceSummaries(context.Context, string) ([]skill.TraceSummary, error) {
	return []skill.TraceSummary{{TraceID: "worker-trace", UserPrompt: "do safe work", StartedAt: time.Now()}}, nil
}

func (*workerQuerier) Trace(context.Context, string) (*skill.Trace, error) {
	return &skill.Trace{TraceID: "worker-trace", Spans: []skill.Span{{
		Kind: "llm", CallKind: "main", Output: []skill.ContentBlock{{Type: "text", Text: "done safely"}},
	}}}, nil
}

type oversizedWorkerQuerier struct{ payload string }

func (q *oversizedWorkerQuerier) TraceSummaries(context.Context, string) ([]skill.TraceSummary, error) {
	return []skill.TraceSummary{{TraceID: "oversized-trace", UserPrompt: q.payload, StartedAt: time.Now()}}, nil
}

func (q *oversizedWorkerQuerier) Trace(context.Context, string) (*skill.Trace, error) {
	return &skill.Trace{TraceID: "oversized-trace", Spans: []skill.Span{{
		Kind: "llm", CallKind: "main", Output: []skill.ContentBlock{{Type: "text", Text: q.payload}},
	}}}, nil
}
