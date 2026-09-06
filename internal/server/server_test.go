package server_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
)

func newTestServer() *server.Server {
	return server.New(server.Config{}, storage.NewMemoryStore(), nil, nil)
}

type generationCreationTestStore interface {
	storage.Store
	storage.SkillIdentityStore
	storage.RevisionStore
	storage.RevisionMetadataStore
	storage.GenerationStore
}

type generationListInspectionStore struct {
	generationCreationTestStore
	exactReads int
}

func (s *generationListInspectionStore) GetSkillGeneration(ctx context.Context, callerSubject, skillID, generationID string) (*storage.GenerationState, error) {
	s.exactReads++
	return s.generationCreationTestStore.GetSkillGeneration(ctx, callerSubject, skillID, generationID)
}

type writeHeaderInspectionRecorder struct {
	header        http.Header
	body          bytes.Buffer
	statusCode    int
	headerWrites  int
	onWriteHeader func(int)
}

func newWriteHeaderInspectionRecorder(onWriteHeader func(int)) *writeHeaderInspectionRecorder {
	return &writeHeaderInspectionRecorder{header: make(http.Header), onWriteHeader: onWriteHeader}
}

func (w *writeHeaderInspectionRecorder) Header() http.Header { return w.header }

func (w *writeHeaderInspectionRecorder) WriteHeader(statusCode int) {
	if w.headerWrites != 0 {
		return
	}
	w.headerWrites++
	w.statusCode = statusCode
	if w.onWriteHeader != nil {
		w.onWriteHeader(statusCode)
	}
}

func (w *writeHeaderInspectionRecorder) Write(body []byte) (int, error) {
	if w.headerWrites == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.body.Write(body)
}

type generationSessionHTTPResponse struct {
	SessionID      string                          `json:"sessionId"`
	Ordinal        int                             `json:"ordinal"`
	Status         storage.GenerationSessionStatus `json:"status"`
	CandidateID    *string                         `json:"candidateId"`
	DiagnosticCode *string                         `json:"diagnosticCode"`
}

type generationHTTPResponse struct {
	ID                      string                          `json:"id"`
	SkillID                 string                          `json:"skillId"`
	BaseRevisionID          *string                         `json:"baseRevisionId"`
	Status                  storage.GenerationStatus        `json:"status"`
	Snapshot                storage.SkillRevisionSnapshot   `json:"input"`
	AuthorContext           string                          `json:"authorContext"`
	SelectedSessionIDs      []string                        `json:"selectedSessionIds"`
	SourceSessionIDs        []string                        `json:"sourceSessionIds"`
	EvaluatorProfile        string                          `json:"evaluatorProfile"`
	EvaluatorProfileVersion string                          `json:"evaluatorProfileVersion"`
	EvaluationCriteria      json.RawMessage                 `json:"evaluationCriteria"`
	WinnerCandidateID       *string                         `json:"winnerCandidateId"`
	ResultCandidateID       *string                         `json:"resultCandidateId"`
	ResultRevisionID        *string                         `json:"resultRevisionId"`
	Failure                 *json.RawMessage                `json:"failure"`
	Sessions                []generationSessionHTTPResponse `json:"sessions"`
	Candidates              []json.RawMessage               `json:"candidates"`
	Evaluations             []json.RawMessage               `json:"evaluations"`
	Diagnostics             []json.RawMessage               `json:"diagnostics"`
	AttemptCount            int                             `json:"attemptCount"`
	CreatedAt               string                          `json:"createdAt"`
	UpdatedAt               string                          `json:"updatedAt"`
	StartedAt               *string                         `json:"startedAt"`
	CompletedAt             *string                         `json:"completedAt"`
}

type defensiveGenerationReadStore struct {
	*storage.MemoryStore
	mu       sync.RWMutex
	state    storage.GenerationState
	isPublic bool
}

func (s *defensiveGenerationReadStore) visible(caller string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return caller == s.state.Generation.CreatorSubject || s.isPublic
}

func (s *defensiveGenerationReadStore) setPublic(value bool) {
	s.mu.Lock()
	s.isPublic = value
	s.mu.Unlock()
}

func (s *defensiveGenerationReadStore) GetSkillGeneration(_ context.Context, callerSubject, skillID, generationID string) (*storage.GenerationState, error) {
	if skillID != s.state.Generation.SkillID || generationID != s.state.Generation.ID || !s.visible(callerSubject) {
		return nil, nil
	}
	state := s.state
	return &state, nil
}

func (s *defensiveGenerationReadStore) GetGenerationByID(_ context.Context, callerSubject, generationID string) (*storage.GenerationState, error) {
	if generationID != s.state.Generation.ID || !s.visible(callerSubject) {
		return nil, nil
	}
	state := s.state
	return &state, nil
}

func (s *defensiveGenerationReadStore) ListSkillGenerations(_ context.Context, opts storage.SkillGenerationListOpts) (*storage.SkillGenerationListPage, error) {
	if opts.SkillID != s.state.Generation.SkillID || !s.visible(opts.CallerSubject) {
		return &storage.SkillGenerationListPage{Generations: []storage.SkillGenerationSummaryRecord{}}, nil
	}
	generation := s.state.Generation
	return &storage.SkillGenerationListPage{Generations: []storage.SkillGenerationSummaryRecord{{
		ID: generation.ID, SkillID: generation.SkillID, BaseRevisionID: generation.BaseRevisionID,
		Status: generation.Status, ResultRevisionID: generation.ResultRevisionID,
		ErrorCode: generation.ErrorCode, ErrorMessage: generation.ErrorMessage,
		AttemptCount: generation.AttemptCount, CreatedAt: generation.CreatedAt,
		UpdatedAt: generation.UpdatedAt, StartedAt: generation.StartedAt, CompletedAt: generation.CompletedAt,
	}}}, nil
}

func collectJSONKeys(value any, keys map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			keys[key] = struct{}{}
			collectJSONKeys(child, keys)
		}
	case []any:
		for _, child := range typed {
			collectJSONKeys(child, keys)
		}
	}
}

var _ = Describe("skill-scoped generation creation", func() {
	It("create_generation_snapshots_skill_revision_inputs", func() {
		By("fencing memory writes with the injected store clock instead of artifact timestamps")
		clockNow := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
		clockStore := storage.NewMemoryStoreWithClock(func() time.Time { return clockNow })
		DeferCleanup(clockStore.Close)
		clockSkill, err := clockStore.ResolveSkill(context.Background(), storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "memory-clock-generation",
			CreatorSubject: "clock-creator", CreatedAt: clockNow,
		})
		Expect(err).NotTo(HaveOccurred())
		criteriaSet := evaluator.GenerationCandidateCriteria()
		clockCriteria, err := json.Marshal(criteriaSet.Criteria)
		Expect(err).NotTo(HaveOccurred())
		clockGeneration, err := clockStore.CreateGeneration(context.Background(), storage.CreateGenerationInput{
			ID: uuid.NewString(), SkillID: clockSkill.ID, CreatorSubject: "clock-creator",
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Clock input", Type: "workflow", Content: "# Clock input",
			},
			AuthorContext: "Use store time.", EvaluatorProfile: criteriaSet.Profile,
			EvaluatorProfileVersion: criteriaSet.Version,
			EvaluationCriteria:      clockCriteria, CreatedAt: clockNow,
		})
		Expect(err).NotTo(HaveOccurred())
		clockClaim, err := clockStore.ClaimGeneration(context.Background(), storage.ClaimGenerationInput{
			WorkerID: "clock-worker", LeaseDuration: time.Minute,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(clockClaim).NotTo(BeNil(), "a stale caller timestamp must not hide work due on the store clock")
		Expect(clockClaim.LastHeartbeatAt).To(HaveValue(BeTemporally("==", clockNow)))
		Expect(clockClaim.LeaseExpiresAt).To(HaveValue(BeTemporally("==", clockNow.Add(time.Minute))))

		renewed, err := clockStore.RenewGenerationLease(context.Background(), clockGeneration.ID,
			clockClaim.ClaimToken, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(renewed).To(BeTrue())
		renewedState, err := clockStore.GetClaimedGeneration(context.Background(), clockGeneration.ID,
			clockClaim.ClaimToken)
		Expect(err).NotTo(HaveOccurred(), "caller read time must not expire a claim before the store clock")
		Expect(renewedState.Generation.LastHeartbeatAt).To(HaveValue(BeTemporally("==", clockNow)))
		Expect(renewedState.Generation.LeaseExpiresAt).To(HaveValue(BeTemporally("==", clockNow.Add(2*time.Minute))))

		futureArtifact, err := clockStore.PutGenerationDiagnostic(context.Background(), clockGeneration.ID,
			clockClaim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: uuid.NewString(), Stage: "candidate", Code: "context_candidate_failed",
				Message: "A context-derived candidate could not be generated.", CreatedAt: clockNow.Add(24 * time.Hour),
			})
		Expect(err).NotTo(HaveOccurred())
		Expect(futureArtifact).NotTo(BeNil())
		clockNow = clockNow.Add(3 * time.Minute)
		renewed, err = clockStore.RenewGenerationLease(context.Background(), clockGeneration.ID,
			clockClaim.ClaimToken, time.Hour)
		Expect(err).NotTo(HaveOccurred())
		Expect(renewed).To(BeFalse(), "caller timestamps must not renew a store-expired lease")
		_, err = clockStore.GetClaimedGeneration(context.Background(), clockGeneration.ID,
			clockClaim.ClaimToken)
		Expect(err).To(MatchError(storage.ErrGenerationClaimLost))
		finalized, err := clockStore.AppendPrivateGenerationResult(context.Background(), storage.AppendPrivateGenerationResultInput{
			GenerationID: clockGeneration.ID, ClaimToken: clockClaim.ClaimToken,
			InitialWinnerCandidateID: uuid.NewString(), ResultCandidateID: uuid.NewString(),
		})
		Expect(finalized).To(BeNil())
		Expect(err).To(MatchError(storage.ErrGenerationClaimLost))
		staleArtifact, err := clockStore.PutGenerationDiagnostic(context.Background(), clockGeneration.ID,
			clockClaim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: uuid.NewString(), Stage: "candidate", Code: "context_candidate_failed",
				Message: "A context-derived candidate could not be generated.", CreatedAt: clockNow.Add(-90 * time.Second),
			})
		Expect(staleArtifact).To(BeNil())
		Expect(err).To(MatchError(storage.ErrGenerationClaimLost))

		memoryStore := storage.NewMemoryStore()
		DeferCleanup(memoryStore.Close)
		fixtures := []struct {
			name  string
			store generationCreationTestStore
		}{{name: "memory", store: memoryStore}}

		dsn := os.Getenv("TEST_POSTGRES_DSN")
		if dsn == "" {
			dsn = os.Getenv("TAPES_TEST_POSTGRES_DSN")
		}
		if dsn == "" {
			dsn = os.Getenv("TEST_DATABASE_URL")
		}
		if dsn != "" {
			schema := "skills_generation_http_" + uuid.NewString()[:8]
			postgresStore, err := storage.OpenPostgresStore(context.Background(), dsn, schema)
			Expect(err).NotTo(HaveOccurred())
			admin, err := pgxpool.New(context.Background(), dsn)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				postgresStore.Close()
				_, _ = admin.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
				admin.Close()
			})
			fixtures = append(fixtures, struct {
				name  string
				store generationCreationTestStore
			}{name: "postgres", store: postgresStore})
		}

		for _, fixture := range fixtures {
			By("exercising the actual HTTP handler and " + fixture.name + " generation store")
			ctx := context.Background()
			creator := "creator-a"
			identityTime := time.Date(2026, 9, 5, 10, 0, 0, 0, time.UTC)
			target, err := fixture.store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: uuid.NewString(), Slug: fixture.name + "-generation-target",
				CreatorSubject: creator, CreatedAt: identityTime,
			})
			Expect(err).NotTo(HaveOccurred())
			base, err := fixture.store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: uuid.NewString(), SkillID: target.ID, CreatorSubject: creator,
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Accessible base", Description: "Exact private base.", Type: "workflow",
					Tags: []string{"base"}, Content: "# Base", SourceSessionIDs: []string{"base-source"},
				},
				CreatedAt: identityTime.Add(time.Minute),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(base.SkillID).To(Equal(target.ID))
			Expect(base.CreatorSubject).To(Equal(creator))
			Expect(base.Snapshot).To(Equal(storage.SkillRevisionSnapshot{
				Name: "Accessible base", Description: "Exact private base.", Type: "workflow",
				Tags: []string{"base"}, Content: "# Base", SourceSessionIDs: []string{"base-source"},
			}))

			inaccessibleBase, err := fixture.store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: uuid.NewString(), SkillID: target.ID, CreatorSubject: "creator-b",
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Hidden base", Type: "workflow", Tags: []string{}, Content: "# Creator B private",
					SourceSessionIDs: []string{},
				},
				CreatedAt: identityTime.Add(2 * time.Minute),
			})
			Expect(err).NotTo(HaveOccurred())
			foreign, err := fixture.store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: uuid.NewString(), Slug: fixture.name + "-generation-foreign",
				CreatorSubject: creator, CreatedAt: identityTime,
			})
			Expect(err).NotTo(HaveOccurred())
			crossSkillBase, err := fixture.store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: uuid.NewString(), SkillID: foreign.ID, CreatorSubject: creator,
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Accessible but foreign", Type: "workflow", Tags: []string{}, Content: "# Foreign",
					SourceSessionIDs: []string{},
				},
				CreatedAt: identityTime.Add(time.Minute),
			})
			Expect(err).NotTo(HaveOccurred())

			generationSnapshot := storage.SkillRevisionSnapshot{
				Name: "Generated incident workflow", Description: "Complete immutable seed.", Type: "workflow",
				Tags: []string{"incident", "safe"}, Content: "# Generated seed\n\nPreserve every check.",
				IsAIGenerated: true, SourceSessionIDs: []string{"source-one", "source-two"},
			}
			selectedSessionIDs := []string{"selected-third", "selected-first", "selected-second"}
			requestBody, err := json.Marshal(map[string]any{
				"baseRevisionId": base.ID,
				"input": map[string]any{
					"name": generationSnapshot.Name, "description": generationSnapshot.Description,
					"type": generationSnapshot.Type, "tags": generationSnapshot.Tags,
					"content": generationSnapshot.Content, "isAiGenerated": generationSnapshot.IsAIGenerated,
					"sourceSessionIds": generationSnapshot.SourceSessionIDs,
				},
				"authorContext":      "Keep rollback and verification explicit.",
				"selectedSessionIds": selectedSessionIDs,
			})
			Expect(err).NotTo(HaveOccurred())
			criteriaSet := evaluator.GenerationCandidateCriteria()
			criteriaJSON, err := json.Marshal(criteriaSet.Criteria)
			Expect(err).NotTo(HaveOccurred())

			requestContext, cancelRequest := context.WithCancel(context.Background())
			DeferCleanup(cancelRequest)
			var committedState *storage.GenerationState
			var recorder *writeHeaderInspectionRecorder
			recorder = newWriteHeaderInspectionRecorder(func(statusCode int) {
				Expect(statusCode).To(Equal(http.StatusAccepted), "the first response write must be 202")
				Expect(recorder.body.Len()).To(BeZero(), "persistence must be inspectable before response bytes are written")
				// WriteHeader records the successful 202 before invoking this hook.
				// Canceling here proves request lifetime cannot own committed work.
				cancelRequest()
				Expect(requestContext.Err()).To(MatchError(context.Canceled))
				listed, listErr := fixture.store.ListSkillGenerations(ctx, storage.SkillGenerationListOpts{
					CallerSubject: creator, SkillID: target.ID,
				})
				Expect(listErr).NotTo(HaveOccurred())
				Expect(listed.Generations).To(HaveLen(1), "the generation row must commit before WriteHeader(202)")
				state, getErr := fixture.store.GetSkillGeneration(ctx, creator, target.ID, listed.Generations[0].ID)
				Expect(getErr).NotTo(HaveOccurred())
				Expect(state).NotTo(BeNil())
				committedState = state

				generation := state.Generation
				Expect(generation.ID).To(Equal(listed.Generations[0].ID))
				Expect(generation.SkillID).To(Equal(target.ID))
				Expect(generation.BaseRevisionID).To(Equal(base.ID))
				Expect(generation.CreatorSubject).To(Equal(creator))
				Expect(generation.Snapshot).To(Equal(generationSnapshot))
				Expect(generation.Status).To(Equal(storage.GenerationStatusQueued))
				Expect(generation.AuthorContext).To(Equal("Keep rollback and verification explicit."))
				Expect(generation.SelectedSessionIDs).To(Equal(selectedSessionIDs))
				Expect(generation.EvaluatorProfile).To(Equal(criteriaSet.Profile))
				Expect(generation.EvaluatorProfileVersion).To(Equal(criteriaSet.Version))
				Expect(generation.EvaluationCriteria).To(MatchJSON(criteriaJSON))
				Expect(generation.NextAttemptAt).To(BeTemporally("==", generation.CreatedAt))
				Expect(generation.UpdatedAt).To(BeTemporally("==", generation.CreatedAt))
				Expect(generation.StartedAt).To(BeNil())
				Expect(generation.CompletedAt).To(BeNil())
				Expect(state.Sessions).To(Equal([]storage.GenerationSessionRecord{
					{GenerationID: generation.ID, SessionID: "selected-third", Ordinal: 0, Status: storage.GenerationSessionPending, UpdatedAt: generation.CreatedAt},
					{GenerationID: generation.ID, SessionID: "selected-first", Ordinal: 1, Status: storage.GenerationSessionPending, UpdatedAt: generation.CreatedAt},
					{GenerationID: generation.ID, SessionID: "selected-second", Ordinal: 2, Status: storage.GenerationSessionPending, UpdatedAt: generation.CreatedAt},
				}))
				Expect(state.Candidates).To(BeEmpty())
				Expect(state.Evaluations).To(BeEmpty())
				Expect(state.Diagnostics).To(BeEmpty())
			})

			requestStartedAt := time.Now().UTC()
			request := httptest.NewRequest(http.MethodPost, "/api/skills/"+target.ID+"/generations", bytes.NewReader(requestBody)).WithContext(requestContext)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(authSubjectHeader, creator)
			newSkillsServer(fixture.store).Handler().ServeHTTP(recorder, request)
			requestFinishedAt := time.Now().UTC()

			Expect(request.Context().Err()).To(MatchError(context.Canceled), "the response writer must cancel the request after accepting durable work")
			Expect(recorder.headerWrites).To(Equal(1))
			Expect(recorder.statusCode).To(Equal(http.StatusAccepted), "%s response: %s", fixture.name, recorder.body.String())
			Expect(committedState).NotTo(BeNil(), "the WriteHeader callback must observe complete durable state")
			Expect(recorder.Header().Get("Content-Type")).To(Equal("application/json"))
			var createdObject map[string]any
			Expect(json.Unmarshal(recorder.body.Bytes(), &createdObject)).To(Succeed())
			Expect(createdObject).NotTo(HaveKey("creatorSubject"))
			Expect(createdObject).NotTo(HaveKey("claimToken"))
			Expect(createdObject).NotTo(HaveKey("claimOwner"))
			Expect(createdObject).NotTo(HaveKey("leaseExpiresAt"))
			var created generationHTTPResponse
			Expect(json.Unmarshal(recorder.body.Bytes(), &created)).To(Succeed())
			_, err = uuid.Parse(created.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(created.ID).To(Equal(committedState.Generation.ID))
			Expect(created.SkillID).To(Equal(target.ID))
			Expect(created.BaseRevisionID).To(HaveValue(Equal(base.ID)))
			Expect(created.Status).To(Equal(storage.GenerationStatusQueued))
			Expect(created.Snapshot).To(Equal(generationSnapshot))
			Expect(created.AuthorContext).To(Equal("Keep rollback and verification explicit."))
			Expect(created.SelectedSessionIDs).To(Equal(selectedSessionIDs))
			Expect(created.SourceSessionIDs).To(BeEmpty())
			Expect(created.EvaluatorProfile).To(Equal(criteriaSet.Profile))
			Expect(created.EvaluatorProfileVersion).To(Equal(criteriaSet.Version))
			Expect(created.EvaluationCriteria).To(MatchJSON(criteriaJSON))
			Expect(created.WinnerCandidateID).To(BeNil())
			Expect(created.ResultCandidateID).To(BeNil())
			Expect(created.ResultRevisionID).To(BeNil())
			Expect(created.Failure).To(BeNil())
			Expect(created.Sessions).To(Equal([]generationSessionHTTPResponse{
				{SessionID: "selected-third", Ordinal: 0, Status: storage.GenerationSessionPending},
				{SessionID: "selected-first", Ordinal: 1, Status: storage.GenerationSessionPending},
				{SessionID: "selected-second", Ordinal: 2, Status: storage.GenerationSessionPending},
			}))
			Expect(created.Candidates).To(BeEmpty())
			Expect(created.Evaluations).To(BeEmpty())
			Expect(created.Diagnostics).To(BeEmpty())
			Expect(created.AttemptCount).To(BeZero())
			createdAt, err := time.Parse(time.RFC3339Nano, created.CreatedAt)
			Expect(err).NotTo(HaveOccurred())
			updatedAt, err := time.Parse(time.RFC3339Nano, created.UpdatedAt)
			Expect(err).NotTo(HaveOccurred())
			Expect(createdAt).To(BeTemporally(">=", requestStartedAt))
			Expect(createdAt).To(BeTemporally("<=", requestFinishedAt))
			Expect(updatedAt).To(BeTemporally("==", createdAt))
			Expect(created.StartedAt).To(BeNil())
			Expect(created.CompletedAt).To(BeNil())

			for _, path := range []string{
				"/api/skills/" + target.ID + "/generations/" + created.ID,
				"/api/skills/generations/" + created.ID,
			} {
				readRecorder := httptest.NewRecorder()
				readRequest := httptest.NewRequest(http.MethodGet, path, nil)
				readRequest.Header.Set(authSubjectHeader, creator)
				newSkillsServer(fixture.store).Handler().ServeHTTP(readRecorder, readRequest)
				Expect(readRecorder.Code).To(Equal(http.StatusOK), "%s GET %s response: %s", fixture.name, path, readRecorder.Body.String())
				var readState generationHTTPResponse
				Expect(json.Unmarshal(readRecorder.Body.Bytes(), &readState)).To(Succeed())
				Expect(readState).To(Equal(created), "GET %s must return complete equivalent bounded state", path)
			}

			committedState.Generation.Snapshot.Tags[0] = "mutated"
			committedState.Generation.SelectedSessionIDs[0] = "mutated"
			committedState.Generation.EvaluationCriteria[0] = 'x'
			committedState.Sessions[0].SessionID = "mutated"
			reread, err := fixture.store.GetSkillGeneration(ctx, creator, target.ID, created.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(reread.Generation.SkillID).To(Equal(target.ID))
			Expect(reread.Generation.BaseRevisionID).To(Equal(base.ID))
			Expect(reread.Generation.CreatorSubject).To(Equal(creator))
			Expect(reread.Generation.Snapshot).To(Equal(generationSnapshot))
			Expect(reread.Generation.SelectedSessionIDs).To(Equal(selectedSessionIDs))
			Expect(reread.Generation.EvaluationCriteria).To(MatchJSON(criteriaJSON))
			Expect(reread.Sessions[0].SessionID).To(Equal("selected-third"))

			var claimed *storage.SkillGenerationRecord
			Eventually(func() error {
				var claimErr error
				claimed, claimErr = fixture.store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
					WorkerID: "worker-after-canceled-request", LeaseDuration: time.Minute,
				})
				if claimErr != nil {
					return claimErr
				}
				if claimed == nil {
					return errors.New("committed generation is not due on the storage clock yet")
				}
				return nil
			}).WithTimeout(time.Second).Should(Succeed(),
				"the committed generation must remain claimable after request cancellation")
			Expect(claimed.ID).To(Equal(created.ID))
			Expect(claimed.ClaimToken).NotTo(BeEmpty())
			claimedState, err := fixture.store.GetClaimedGeneration(ctx, created.ID, claimed.ClaimToken)
			Expect(err).NotTo(HaveOccurred())
			Expect(claimedState.Generation.SkillID).To(Equal(target.ID))
			Expect(claimedState.Generation.BaseRevisionID).To(Equal(base.ID))
			Expect(claimedState.Generation.CreatorSubject).To(Equal(creator))
			Expect(claimedState.Generation.Snapshot).To(Equal(generationSnapshot))
			Expect(claimedState.Generation.AuthorContext).To(Equal("Keep rollback and verification explicit."))
			Expect(claimedState.Generation.SelectedSessionIDs).To(Equal(selectedSessionIDs))
			Expect(claimedState.Generation.EvaluatorProfile).To(Equal(criteriaSet.Profile))
			Expect(claimedState.Generation.EvaluatorProfileVersion).To(Equal(criteriaSet.Version))
			Expect(claimedState.Generation.EvaluationCriteria).To(MatchJSON(criteriaJSON))
			Expect(claimedState.Sessions).To(HaveLen(3))

			modernInput := storage.CreateGenerationInput{
				ID: uuid.NewString(), SkillID: target.ID, BaseRevisionID: base.ID,
				CreatorSubject: creator, Snapshot: generationSnapshot,
				AuthorContext: "modern only", SelectedSessionIDs: []string{"modern-session"},
				EvaluatorProfile: criteriaSet.Profile, EvaluatorProfileVersion: criteriaSet.Version,
				EvaluationCriteria: criteriaJSON, CreatedAt: identityTime.Add(5 * time.Minute),
			}
			By("rejecting removed draft identity and lock fields at the HTTP boundary")
			deprecatedBody := map[string]any{}
			Expect(json.Unmarshal(requestBody, &deprecatedBody)).To(Succeed())
			deprecatedBody["expectedLockVersion"] = 1
			deprecatedJSON, err := json.Marshal(deprecatedBody)
			Expect(err).NotTo(HaveOccurred())
			failure, failedStatus := doJSON(newSkillsServer(fixture.store), http.MethodPost,
				"/api/skills/"+target.ID+"/generations", string(deprecatedJSON), creator)
			Expect(failedStatus).To(Equal(http.StatusBadRequest), "%s response: %#v", fixture.name, failure)

			for _, rejectedBase := range []string{crossSkillBase.ID, inaccessibleBase.ID} {
				failedBody, err := json.Marshal(map[string]any{
					"baseRevisionId": rejectedBase,
					"input": map[string]any{
						"name": generationSnapshot.Name, "description": generationSnapshot.Description,
						"type": generationSnapshot.Type, "tags": generationSnapshot.Tags,
						"content": generationSnapshot.Content, "isAiGenerated": generationSnapshot.IsAIGenerated,
						"sourceSessionIds": generationSnapshot.SourceSessionIDs,
					},
					"authorContext": "must not commit", "selectedSessionIds": []string{"must-not-exist"},
				})
				Expect(err).NotTo(HaveOccurred())
				failure, failedStatus := doJSON(newSkillsServer(fixture.store), http.MethodPost,
					"/api/skills/"+target.ID+"/generations", string(failedBody), creator)
				Expect(failedStatus).To(Equal(http.StatusNotFound), "%s response: %#v", fixture.name, failure)
				failureJSON, err := json.Marshal(failure)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(failureJSON)).NotTo(ContainSubstring(rejectedBase))
				listed, listErr := fixture.store.ListSkillGenerations(ctx, storage.SkillGenerationListOpts{
					CallerSubject: creator, SkillID: target.ID,
				})
				Expect(listErr).NotTo(HaveOccurred())
				Expect(listed.Generations).To(HaveLen(1), "a rejected creation must not commit a partial generation")
			}

			By("returning bounded keyset pages of summaries without exact-read N+1 loading")
			for ordinal := range 4 {
				input := modernInput
				input.ID = uuid.NewString()
				input.AuthorContext = fmt.Sprintf("page-%d", ordinal)
				input.CreatedAt = identityTime.Add(time.Duration(10+ordinal) * time.Minute)
				_, err = fixture.store.CreateGeneration(ctx, input)
				Expect(err).NotTo(HaveOccurred())
			}
			inspectionStore := &generationListInspectionStore{generationCreationTestStore: fixture.store}
			seenGenerationIDs := map[string]struct{}{}
			cursor := ""
			for _, expectedPageSize := range []int{2, 2, 1} {
				path := "/api/skills/" + target.ID + "/generations?limit=2"
				if cursor != "" {
					path += "&cursor=" + cursor
				}
				pageBody, pageStatus := doJSON(newSkillsServer(inspectionStore), http.MethodGet, path, "", creator)
				Expect(pageStatus).To(Equal(http.StatusOK), "%s page response: %#v", fixture.name, pageBody)
				pageGenerations := pageBody["generations"].([]any)
				Expect(pageGenerations).To(HaveLen(expectedPageSize))
				for _, value := range pageGenerations {
					summary := value.(map[string]any)
					generationID := summary["id"].(string)
					Expect(seenGenerationIDs).NotTo(HaveKey(generationID))
					seenGenerationIDs[generationID] = struct{}{}
					Expect(summary).NotTo(HaveKey("input"))
					Expect(summary).NotTo(HaveKey("selectedSessionIds"))
					Expect(summary).NotTo(HaveKey("sessions"))
					Expect(summary).NotTo(HaveKey("candidates"))
					Expect(summary).NotTo(HaveKey("evaluations"))
					Expect(summary).NotTo(HaveKey("diagnostics"))
				}
				cursor, _ = pageBody["nextCursor"].(string)
			}
			Expect(cursor).To(BeEmpty())
			Expect(seenGenerationIDs).To(HaveLen(5))
			Expect(inspectionStore.exactReads).To(BeZero(), "list must not issue an exact artifact read per summary")
			_, invalidCursorStatus := doJSON(newSkillsServer(inspectionStore), http.MethodGet,
				"/api/skills/"+target.ID+"/generations?cursor=not-base64!", "", creator)
			Expect(invalidCursorStatus).To(Equal(http.StatusBadRequest))
		}
	})

	It("memory_and_postgres_http_boundaries_match", func() {
		memoryStore := storage.NewMemoryStore()
		DeferCleanup(memoryStore.Close)
		fixtures := []struct {
			name  string
			store generationCreationTestStore
		}{{name: "memory", store: memoryStore}}

		dsn := os.Getenv("TEST_POSTGRES_DSN")
		if dsn == "" {
			dsn = os.Getenv("TAPES_TEST_POSTGRES_DSN")
		}
		if dsn == "" {
			dsn = os.Getenv("TEST_DATABASE_URL")
		}
		if dsn != "" {
			schema := "skills_http_bounds_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
			postgresStore, err := storage.OpenPostgresStore(context.Background(), dsn, schema)
			Expect(err).NotTo(HaveOccurred())
			admin, err := pgxpool.New(context.Background(), dsn)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				postgresStore.Close()
				_, _ = admin.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema))
				admin.Close()
			})
			fixtures = append(fixtures, struct {
				name  string
				store generationCreationTestStore
			}{name: "postgres", store: postgresStore})
		}

		marshalBody := func(value any) string {
			body, err := json.Marshal(value)
			Expect(err).NotTo(HaveOccurred())
			return string(body)
		}
		appendBody := func(mutate func(map[string]any, map[string]any)) string {
			snapshot := map[string]any{
				"name": "Boundary revision", "description": "Boundary description", "type": "workflow",
				"tags": []string{"boundary"}, "content": "# Boundary", "isAiGenerated": false,
				"sourceSessionIds": []string{"boundary-session"},
			}
			request := map[string]any{
				"snapshot": snapshot, "changeNote": "Boundary note", "idempotencyKey": uuid.NewString(),
			}
			if mutate != nil {
				mutate(request, snapshot)
			}
			return marshalBody(request)
		}
		generationBody := func(authorContext string, selected []string, mutate func(map[string]any)) string {
			input := map[string]any{
				"name": "Generation boundary", "description": "Complete seed", "type": "workflow",
				"tags": []string{"generation"}, "content": "# Generation", "isAiGenerated": true,
				"sourceSessionIds": []string{},
			}
			if mutate != nil {
				mutate(input)
			}
			return marshalBody(map[string]any{
				"baseRevisionId": nil, "input": input,
				"authorContext": authorContext, "selectedSessionIds": selected,
			})
		}

		for _, fixture := range fixtures {
			By("checking code-point, transport-byte, control, visibility, and list bounds through the " + fixture.name + " HTTP store")
			creator := "boundary-creator"
			srv := newSkillsServer(fixture.store)

			exactSlug := strings.Repeat("s", 128)
			identity, status := doJSON(srv, http.MethodPost, "/api/skills",
				marshalBody(map[string]any{"slug": exactSlug}), creator)
			Expect(status).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, identity)
			skillID := identity["id"].(string)

			By("accepting multibyte text at the documented Unicode code-point limits")
			unicodeIdentity, unicodeStatus := doJSON(srv, http.MethodPost, "/api/skills",
				marshalBody(map[string]any{"slug": strings.Repeat("界", 128)}), creator)
			Expect(unicodeStatus).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, unicodeIdentity)
			unicodeRevisionBody := marshalBody(map[string]any{
				"snapshot": map[string]any{
					"name": strings.Repeat("界", 1024), "description": strings.Repeat("界", 1<<20),
					"type": "workflow", "tags": []string{strings.Repeat("界", 256)},
					"content": strings.Repeat("界", 1<<20), "isAiGenerated": false,
					"sourceSessionIds": []string{strings.Repeat("界", 256)},
				},
				"changeNote":     strings.Repeat("界", 1024),
				"idempotencyKey": strings.Repeat("界", 256),
			})
			unicodeRevision, unicodeRevisionStatus := doJSON(srv, http.MethodPost,
				"/api/skills/"+unicodeIdentity["id"].(string)+"/revisions", unicodeRevisionBody, creator)
			Expect(unicodeRevisionStatus).To(Equal(http.StatusCreated),
				"%s response: %#v", fixture.name, unicodeRevision)

			exactRevisionBody := marshalBody(map[string]any{
				"snapshot": map[string]any{
					"name": strings.Repeat("n", 1024), "description": strings.Repeat("d", 1<<20),
					"type": "workflow", "tags": []string{strings.Repeat("t", 256)},
					"content": strings.Repeat("c", 1<<20), "isAiGenerated": false,
					"sourceSessionIds": []string{strings.Repeat("i", 256)},
				},
				"changeNote": strings.Repeat("m", 1024), "idempotencyKey": strings.Repeat("k", 256),
			})
			created, status := doJSON(srv, http.MethodPost,
				"/api/skills/"+skillID+"/revisions", exactRevisionBody, creator)
			Expect(status).To(Equal(http.StatusCreated), "%s response: %#v", fixture.name, created)
			revisionID := created["id"].(string)

			visible, status := doJSON(srv, http.MethodPut,
				"/api/skills/"+skillID+"/revisions/"+revisionID+"/visibility",
				`{"isPublic":true}`, creator)
			Expect(status).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, visible)
			Expect(mapKeys(visible)).To(ConsistOf("revisionId", "isPublic", "changedAt"))
			Expect(visible).To(And(
				HaveKeyWithValue("revisionId", revisionID),
				HaveKeyWithValue("isPublic", true),
			))

			By("allowing the noncreator audit actor to retry the exact public-to-private mutation")
			madePrivate, status := doJSON(srv, http.MethodPut,
				"/api/skills/"+skillID+"/revisions/"+revisionID+"/visibility",
				`{"isPublic":false}`, "visibility-editor")
			Expect(status).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, madePrivate)
			Expect(madePrivate).To(And(
				HaveKeyWithValue("revisionId", revisionID),
				HaveKeyWithValue("isPublic", false),
				HaveKey("changedAt"),
			))
			Expect(mapKeys(madePrivate)).To(ConsistOf("revisionId", "isPublic", "changedAt"))
			serialized, err := json.Marshal(madePrivate)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(serialized)).NotTo(ContainSubstring("Boundary revision"))
			Expect(string(serialized)).NotTo(ContainSubstring(strings.Repeat("c", 64)))
			retry, retryStatus := doJSON(srv, http.MethodPut,
				"/api/skills/"+skillID+"/revisions/"+revisionID+"/visibility",
				`{"isPublic":false}`, "visibility-editor")
			Expect(retryStatus).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, retry)
			Expect(retry).To(Equal(madePrivate), "an exact retry must preserve audit time")
			_, otherStatus := doJSON(srv, http.MethodPut,
				"/api/skills/"+skillID+"/revisions/"+revisionID+"/visibility",
				`{"isPublic":false}`, "different-editor")
			Expect(otherStatus).To(Equal(http.StatusNotFound))

			invalidRequests := []struct {
				name, method, path, body string
			}{
				{
					name: "whole request exceeds its hard byte limit", method: http.MethodPost,
					path: "/api/skills", body: `{"slug":"` + strings.Repeat("x", 14<<20) + `"}`,
				},
				{
					name: "whole request is invalid UTF-8", method: http.MethodPost,
					path: "/api/skills", body: "{\"slug\":\"bad\xffslug\"}",
				},
				{
					name: "raw slug exceeds its code-point limit after trimming", method: http.MethodPost,
					path: "/api/skills", body: marshalBody(map[string]any{"slug": strings.Repeat(" ", 128) + "x"}),
				},
				{
					name: "slug contains NUL", method: http.MethodPost,
					path: "/api/skills", body: marshalBody(map[string]any{"slug": "bad\x00slug"}),
				},
				{
					name: "raw name exceeds its code-point limit after trimming", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) {
						snapshot["name"] = strings.Repeat(" ", 1024) + "n"
					}),
				},
				{
					name: "raw description exceeds its code-point limit after trimming", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) {
						snapshot["description"] = strings.Repeat(" ", 1<<20) + "d"
					}),
				},
				{
					name: "raw type exceeds its code-point limit after trimming", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) {
						snapshot["type"] = strings.Repeat(" ", 256) + "workflow"
					}),
				},
				{
					name: "raw tag exceeds its code-point limit after trimming", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) {
						snapshot["tags"] = []string{strings.Repeat(" ", 256) + "t"}
					}),
				},
				{
					name: "raw session ID exceeds its code-point limit after trimming", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) {
						snapshot["sourceSessionIds"] = []string{strings.Repeat(" ", 256) + "s"}
					}),
				},
				{
					name: "raw change note exceeds its code-point limit after trimming", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(request, _ map[string]any) {
						request["changeNote"] = strings.Repeat(" ", 1024) + "n"
					}),
				},
				{
					name: "raw idempotency key exceeds its code-point limit after trimming", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(request, _ map[string]any) {
						request["idempotencyKey"] = strings.Repeat(" ", 256) + "k"
					}),
				},
				{
					name: "revision name contains NUL", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) { snapshot["name"] = "bad\x00name" }),
				},
				{
					name: "revision description contains a control", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) { snapshot["description"] = "bad\x01description" }),
				},
				{
					name: "revision content contains NUL", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) { snapshot["content"] = "bad\x00content" }),
				},
				{
					name: "tag contains a control", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) { snapshot["tags"] = []string{"bad\x01tag"} }),
				},
				{
					name: "source session contains NUL", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(_ map[string]any, snapshot map[string]any) {
						snapshot["sourceSessionIds"] = []string{"bad\x00session"}
					}),
				},
				{
					name: "change note contains a control", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(request, _ map[string]any) { request["changeNote"] = "bad\x01note" }),
				},
				{
					name: "idempotency key contains NUL", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(request, _ map[string]any) { request["idempotencyKey"] = "bad\x00key" }),
				},
				{
					name: "base revision raw identity exceeds its limit", method: http.MethodPost,
					path: "/api/skills/" + skillID + "/revisions",
					body: appendBody(func(request, _ map[string]any) {
						request["basedOnRevisionId"] = strings.Repeat(" ", 221) + revisionID
					}),
				},
				{
					name: "latest revision raw identity exceeds its limit", method: http.MethodPut,
					path: "/api/skills/" + skillID + "/latest",
					body: marshalBody(map[string]any{"revisionId": strings.Repeat(" ", 221) + revisionID}),
				},
				{
					name: "session query raw identity exceeds its limit", method: http.MethodGet,
					path: "/api/skills?session_id=" + url.QueryEscape(strings.Repeat(" ", 256)+"s"),
				},
				{
					name: "search query raw text exceeds its limit", method: http.MethodGet,
					path: "/api/skills?q=" + url.QueryEscape(strings.Repeat(" ", 1024)+"q"),
				},
			}
			for _, request := range invalidRequests {
				response, responseStatus := doJSON(srv, request.method, request.path, request.body, creator)
				Expect(responseStatus).To(Equal(http.StatusBadRequest),
					"%s %s response: %#v", fixture.name, request.name, response)
				Expect(response).To(HaveKey("error"), "%s %s", fixture.name, request.name)
			}

			exactGeneration, status := doJSON(srv, http.MethodPost,
				"/api/skills/"+skillID+"/generations",
				generationBody(strings.Repeat("界", 32<<10), []string{strings.Repeat("界", 256)}, nil), creator)
			Expect(status).To(Equal(http.StatusAccepted), "%s response: %#v", fixture.name, exactGeneration)
			invalidGenerations := []struct {
				name, body string
			}{
				{
					name: "raw author context exceeds its code-point limit after trimming",
					body: generationBody(strings.Repeat(" ", 32<<10)+"a", []string{}, nil),
				},
				{name: "author context contains NUL", body: generationBody("bad\x00context", []string{}, nil)},
				{
					name: "selected session raw identity exceeds its limit",
					body: generationBody("context", []string{strings.Repeat(" ", 256) + "s"}, nil),
				},
				{name: "selected session contains a control", body: generationBody("context", []string{"bad\x01session"}, nil)},
				{
					name: "generation input contains NUL",
					body: generationBody("context", []string{}, func(input map[string]any) { input["content"] = "bad\x00content" }),
				},
			}
			for _, request := range invalidGenerations {
				response, responseStatus := doJSON(srv, http.MethodPost,
					"/api/skills/"+skillID+"/generations", request.body, creator)
				Expect(responseStatus).To(Equal(http.StatusBadRequest),
					"%s %s response: %#v", fixture.name, request.name, response)
			}

			revisions, status := doJSON(srv, http.MethodGet,
				"/api/skills/"+skillID+"/revisions", "", creator)
			Expect(status).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, revisions)
			Expect(revisions["items"]).To(HaveLen(1), "invalid revision requests must not reach storage")
			generations, status := doJSON(srv, http.MethodGet,
				"/api/skills/"+skillID+"/generations", "", creator)
			Expect(status).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, generations)
			Expect(generations["generations"]).To(HaveLen(1), "invalid generation requests must not reach storage")

			By("bounding session reverse lookup before complete projections are loaded")
			baseTime := time.Now().UTC()
			for ordinal := range 101 {
				identity, err := fixture.store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
					ID: uuid.NewString(), Slug: fmt.Sprintf("%s-session-limit-%03d", fixture.name, ordinal),
					CreatorSubject: creator, CreatedAt: baseTime.Add(time.Duration(ordinal) * time.Millisecond),
				})
				Expect(err).NotTo(HaveOccurred())
				_, err = fixture.store.AppendRevision(context.Background(), storage.AppendRevisionInput{
					ID: uuid.NewString(), SkillID: identity.ID, CreatorSubject: creator,
					Origin: storage.RevisionOriginManual,
					Snapshot: storage.SkillRevisionSnapshot{
						Name: fmt.Sprintf("Session limit %03d", ordinal), Type: "workflow",
						Tags: []string{}, Content: "# Session limit", SourceSessionIDs: []string{"limit-session"},
					},
					IdempotencyKey: "session-limit", CreatedAt: baseTime.Add(time.Duration(ordinal) * time.Millisecond),
				})
				Expect(err).NotTo(HaveOccurred())
			}
			sessionPage, status := doJSON(srv, http.MethodGet,
				"/api/skills?session_id=limit-session", "", creator)
			Expect(status).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, sessionPage)
			Expect(sessionPage["items"]).To(HaveLen(100))

			By("retaining one internal lookahead row at the 100-revision HTTP page maximum")
			paginationSkill, err := fixture.store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
				ID: uuid.NewString(), Slug: fixture.name + "-revision-page-lookahead",
				CreatorSubject: creator, CreatedAt: baseTime,
			})
			Expect(err).NotTo(HaveOccurred())
			for ordinal := range 101 {
				_, err = fixture.store.AppendRevision(context.Background(), storage.AppendRevisionInput{
					ID: uuid.NewString(), SkillID: paginationSkill.ID, CreatorSubject: creator,
					Origin: storage.RevisionOriginManual,
					Snapshot: storage.SkillRevisionSnapshot{
						Name: fmt.Sprintf("Lookahead %03d", ordinal), Type: "workflow", Content: "# Lookahead",
					},
					IdempotencyKey: fmt.Sprintf("lookahead-%03d", ordinal),
					CreatedAt:      baseTime.Add(time.Duration(ordinal) * time.Millisecond),
				})
				Expect(err).NotTo(HaveOccurred())
			}
			directLookahead, err := fixture.store.ListRevisions(context.Background(), storage.RevisionListOpts{
				SkillID: paginationSkill.ID, CallerSubject: creator, Limit: 1000,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(directLookahead).To(HaveLen(storage.MaxRevisionListLimit+1),
				"%s storage must cap an oversized request at one row beyond the public maximum", fixture.name)

			firstPage, status := doJSON(srv, http.MethodGet,
				"/api/skills/"+paginationSkill.ID+"/revisions?limit=100", "", creator)
			Expect(status).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, firstPage)
			firstItems, ok := firstPage["items"].([]any)
			Expect(ok).To(BeTrue(), "%s response: %#v", fixture.name, firstPage)
			Expect(firstItems).To(HaveLen(storage.MaxRevisionListLimit))
			nextCursor, ok := firstPage["nextCursor"].(string)
			Expect(ok).To(BeTrue(), "%s response omitted lookahead cursor: %#v", fixture.name, firstPage)
			Expect(nextCursor).NotTo(BeEmpty())

			secondPage, status := doJSON(srv, http.MethodGet,
				"/api/skills/"+paginationSkill.ID+"/revisions?limit=100&cursor="+url.QueryEscape(nextCursor),
				"", creator)
			Expect(status).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, secondPage)
			Expect(secondPage["items"]).To(HaveLen(1))
			Expect(secondPage).NotTo(HaveKey("nextCursor"))
			_, status = doJSON(srv, http.MethodGet,
				"/api/skills/"+paginationSkill.ID+"/revisions?limit=101", "", creator)
			Expect(status).To(Equal(http.StatusBadRequest), "%s public limit must remain 100", fixture.name)

			By("canonicalizing uppercase and raw-hex UUIDs before every " + fixture.name + " store boundary")
			uppercaseUUID := func(id string) string {
				return strings.ToUpper(id)
			}
			noncanonicalUUID := func(id string) string {
				return strings.ReplaceAll(id, "-", "")
			}
			encodeCursor := func(value any) string {
				encoded, encodeErr := json.Marshal(value)
				Expect(encodeErr).NotTo(HaveOccurred())
				return base64.RawURLEncoding.EncodeToString(encoded)
			}
			canonicalTarget, err := fixture.store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
				ID: uuid.NewString(), Slug: fixture.name + "-canonical-target",
				CreatorSubject: creator, CreatedAt: time.Now().UTC(),
			})
			Expect(err).NotTo(HaveOccurred())
			canonicalBase, err := fixture.store.AppendRevision(context.Background(), storage.AppendRevisionInput{
				ID: uuid.NewString(), SkillID: canonicalTarget.ID, CreatorSubject: creator,
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Canonical base", Description: "Canonical UUID boundary base.", Type: "workflow",
					Tags: []string{}, Content: "# Canonical base", SourceSessionIDs: []string{},
				},
				IdempotencyKey: "canonical-base", CreatedAt: time.Now().UTC().Add(time.Millisecond),
			})
			Expect(err).NotTo(HaveOccurred())
			canonicalSourceSkill, err := fixture.store.ResolveSkill(context.Background(), storage.ResolveSkillInput{
				ID: uuid.NewString(), Slug: fixture.name + "-canonical-source",
				CreatorSubject: creator, CreatedAt: time.Now().UTC(),
			})
			Expect(err).NotTo(HaveOccurred())
			canonicalSource, err := fixture.store.AppendRevision(context.Background(), storage.AppendRevisionInput{
				ID: uuid.NewString(), SkillID: canonicalSourceSkill.ID, CreatorSubject: creator,
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Canonical source", Description: "Cross-skill source.", Type: "workflow",
					Tags: []string{}, Content: "# Canonical source", SourceSessionIDs: []string{},
				},
				IdempotencyKey: "canonical-source", CreatedAt: time.Now().UTC().Add(time.Millisecond),
			})
			Expect(err).NotTo(HaveOccurred())
			_, err = fixture.store.SetRevisionVisibility(context.Background(), storage.SetRevisionVisibilityInput{
				SkillID: canonicalSourceSkill.ID, RevisionID: canonicalSource.ID, CallerSubject: creator,
				IsPublic: true, ChangedAt: time.Now().UTC().Add(2 * time.Millisecond),
			})
			Expect(err).NotTo(HaveOccurred())

			uppercaseSkillID := uppercaseUUID(canonicalTarget.ID)
			noncanonicalSkillID := noncanonicalUUID(canonicalTarget.ID)
			uppercaseBaseID := uppercaseUUID(canonicalBase.ID)
			noncanonicalSourceID := noncanonicalUUID(canonicalSource.ID)
			detail, detailStatus := doJSON(srv, http.MethodGet,
				"/api/skills/"+uppercaseSkillID, "", creator)
			Expect(detailStatus).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, detail)
			Expect(detail).To(HaveKeyWithValue("id", canonicalTarget.ID))
			exactBase, exactBaseStatus := doJSON(srv, http.MethodGet,
				"/api/skills/"+noncanonicalSkillID+"/revisions/"+uppercaseBaseID, "", creator)
			Expect(exactBaseStatus).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, exactBase)
			Expect(exactBase).To(HaveKeyWithValue("id", canonicalBase.ID))

			canonicalAppendBody := marshalBody(map[string]any{
				"basedOnRevisionId": uppercaseBaseID,
				"sourceRevisionId":  noncanonicalSourceID,
				"snapshot": map[string]any{
					"name": "Canonical child", "description": "All parsed UUIDs become canonical.",
					"type": "workflow", "tags": []string{}, "content": "# Canonical child",
					"isAiGenerated": false, "sourceSessionIds": []string{},
				},
				"idempotencyKey": "canonical-child",
			})
			canonicalChild, childStatus := doJSON(srv, http.MethodPost,
				"/api/skills/"+uppercaseSkillID+"/revisions", canonicalAppendBody, creator)
			Expect(childStatus).To(Equal(http.StatusCreated), "%s response: %#v", fixture.name, canonicalChild)
			canonicalChildID := canonicalChild["id"].(string)
			Expect(canonicalChild).To(And(
				HaveKeyWithValue("skillId", canonicalTarget.ID),
				HaveKeyWithValue("basedOnRevisionId", canonicalBase.ID),
				HaveKeyWithValue("sourceRevisionId", canonicalSource.ID),
			))

			uppercaseChildID := uppercaseUUID(canonicalChildID)
			noncanonicalChildID := noncanonicalUUID(canonicalChildID)
			visibilityBody, visibilityStatus := doJSON(srv, http.MethodPut,
				"/api/skills/"+noncanonicalSkillID+"/revisions/"+uppercaseChildID+"/visibility",
				`{"isPublic":true}`, creator)
			Expect(visibilityStatus).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, visibilityBody)
			Expect(visibilityBody).To(HaveKeyWithValue("revisionId", canonicalChildID))
			latestBody, latestStatus := doJSON(srv, http.MethodPut,
				"/api/skills/"+uppercaseSkillID+"/latest",
				marshalBody(map[string]any{"revisionId": noncanonicalChildID}), creator)
			Expect(latestStatus).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, latestBody)
			Expect(latestBody).To(HaveKeyWithValue("explicitLatestRevisionId", canonicalChildID))

			resolvedIdentity, resolvedStatus := doJSON(srv, http.MethodPost, "/api/skills",
				marshalBody(map[string]any{"slug": canonicalTarget.Slug}), "another-creator")
			Expect(resolvedStatus).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, resolvedIdentity)
			Expect(resolvedIdentity).To(HaveKeyWithValue("explicitLatestRevisionId", canonicalChildID))
			Expect(mapKeys(resolvedIdentity)).To(ConsistOf(
				"id", "slug", "explicitLatestRevisionId", "createdAt",
			))

			markdownRecorder := httptest.NewRecorder()
			markdownRequest := httptest.NewRequest(http.MethodGet,
				"/api/skills/"+noncanonicalSkillID+"/skill.md", nil)
			markdownRequest.Header.Set(authSubjectHeader, creator)
			srv.Handler().ServeHTTP(markdownRecorder, markdownRequest)
			Expect(markdownRecorder.Code).To(Equal(http.StatusOK), markdownRecorder.Body.String())
			Expect(markdownRecorder.Body.String()).To(ContainSubstring("# Canonical child"))

			generationRequest := generationBody("Canonical generation.", []string{}, nil)
			var generationRequestObject map[string]any
			Expect(json.Unmarshal([]byte(generationRequest), &generationRequestObject)).To(Succeed())
			generationRequestObject["baseRevisionId"] = uppercaseChildID
			generationCreated, generationStatus := doJSON(srv, http.MethodPost,
				"/api/skills/"+noncanonicalSkillID+"/generations",
				marshalBody(generationRequestObject), creator)
			Expect(generationStatus).To(Equal(http.StatusAccepted),
				"%s response: %#v", fixture.name, generationCreated)
			canonicalGenerationID := generationCreated["id"].(string)
			Expect(generationCreated).To(And(
				HaveKeyWithValue("skillId", canonicalTarget.ID),
				HaveKeyWithValue("baseRevisionId", canonicalChildID),
			))
			uppercaseGenerationID := uppercaseUUID(canonicalGenerationID)
			noncanonicalGenerationID := noncanonicalUUID(canonicalGenerationID)

			generationList, generationListStatus := doJSON(srv, http.MethodGet,
				"/api/skills/"+uppercaseSkillID+"/generations", "", creator)
			Expect(generationListStatus).To(Equal(http.StatusOK),
				"%s response: %#v", fixture.name, generationList)
			Expect(generationList["generations"]).To(HaveLen(1))
			for _, path := range []string{
				"/api/skills/" + noncanonicalSkillID + "/generations/" + uppercaseGenerationID,
				"/api/skills/generations/" + noncanonicalGenerationID,
			} {
				generationRead, readStatus := doJSON(srv, http.MethodGet, path, "", creator)
				Expect(readStatus).To(Equal(http.StatusOK), "%s %s response: %#v", fixture.name, path, generationRead)
				Expect(generationRead).To(HaveKeyWithValue("id", canonicalGenerationID))
			}

			canonicalSkillCursor := encodeCursor(map[string]any{
				"ts": canonicalChild["createdAt"], "dc": 0, "id": canonicalTarget.ID,
			})
			noncanonicalSkillCursor := encodeCursor(map[string]any{
				"ts": canonicalChild["createdAt"], "dc": 0, "id": noncanonicalSkillID,
			})
			canonicalSkillPage, canonicalSkillPageStatus := doJSON(srv, http.MethodGet,
				"/api/skills?limit=100&cursor="+url.QueryEscape(canonicalSkillCursor), "", creator)
			noncanonicalSkillPage, noncanonicalSkillPageStatus := doJSON(srv, http.MethodGet,
				"/api/skills?limit=100&cursor="+url.QueryEscape(noncanonicalSkillCursor), "", creator)
			Expect(noncanonicalSkillPageStatus).To(Equal(canonicalSkillPageStatus))
			Expect(noncanonicalSkillPage).To(Equal(canonicalSkillPage))

			canonicalRevisionCursor := encodeCursor(map[string]any{
				"sequenceNumber": canonicalChild["sequenceNumber"], "revisionId": canonicalChildID,
			})
			noncanonicalRevisionCursor := encodeCursor(map[string]any{
				"sequenceNumber": canonicalChild["sequenceNumber"], "revisionId": noncanonicalChildID,
			})
			canonicalRevisionPage, canonicalRevisionPageStatus := doJSON(srv, http.MethodGet,
				"/api/skills/"+canonicalTarget.ID+"/revisions?cursor="+url.QueryEscape(canonicalRevisionCursor),
				"", creator)
			noncanonicalRevisionPage, noncanonicalRevisionPageStatus := doJSON(srv, http.MethodGet,
				"/api/skills/"+noncanonicalSkillID+"/revisions?cursor="+url.QueryEscape(noncanonicalRevisionCursor),
				"", creator)
			Expect(noncanonicalRevisionPageStatus).To(Equal(canonicalRevisionPageStatus))
			Expect(noncanonicalRevisionPage).To(Equal(canonicalRevisionPage))

			canonicalGenerationCursor := encodeCursor(map[string]any{
				"createdAt": generationCreated["createdAt"], "id": canonicalGenerationID,
			})
			noncanonicalGenerationCursor := encodeCursor(map[string]any{
				"createdAt": generationCreated["createdAt"], "id": noncanonicalGenerationID,
			})
			canonicalGenerationPage, canonicalGenerationPageStatus := doJSON(srv, http.MethodGet,
				"/api/skills/"+canonicalTarget.ID+"/generations?cursor="+url.QueryEscape(canonicalGenerationCursor),
				"", creator)
			noncanonicalGenerationPage, noncanonicalGenerationPageStatus := doJSON(srv, http.MethodGet,
				"/api/skills/"+noncanonicalSkillID+"/generations?cursor="+url.QueryEscape(noncanonicalGenerationCursor),
				"", creator)
			Expect(noncanonicalGenerationPageStatus).To(Equal(canonicalGenerationPageStatus))
			Expect(noncanonicalGenerationPage).To(Equal(canonicalGenerationPage))

			canceled, canceledStatus := doJSON(srv, http.MethodPost,
				"/api/skills/"+uppercaseSkillID+"/generations/"+uppercaseGenerationID+"/cancellations",
				"", creator)
			Expect(canceledStatus).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, canceled)
			Expect(canceled).To(HaveKeyWithValue("id", canonicalGenerationID))
			Expect(canceled).To(HaveKeyWithValue("status", "canceled"))

			cleared, clearedStatus := doJSON(srv, http.MethodDelete,
				"/api/skills/"+noncanonicalSkillID+"/latest", "", creator)
			Expect(clearedStatus).To(Equal(http.StatusOK), "%s response: %#v", fixture.name, cleared)
			Expect(cleared).To(HaveKeyWithValue("skillId", canonicalTarget.ID))
			Expect(cleared).To(HaveKeyWithValue("explicitLatestRevisionId", BeNil()))
		}
	})

})

var _ = Describe("cassette anchors", func() {
	It("serves GET /ping", func() {
		recorder := httptest.NewRecorder()
		newTestServer().Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ping", nil))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Body.String()).To(Equal("pong\n"))
	})

	It("serves nothing at the legacy core route", func() {
		recorder := httptest.NewRecorder()
		newTestServer().Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/skills", nil))
		Expect(recorder.Code).To(Equal(http.StatusNotFound))
	})

	It("serves an OpenAPI document that satisfies cassette admission", func() {
		recorder := httptest.NewRecorder()
		newTestServer().Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/openapi", nil))
		Expect(recorder.Code).To(Equal(http.StatusOK))
		Expect(recorder.Header().Get("Content-Type")).To(Equal("application/json"))

		var document map[string]any
		Expect(json.Unmarshal(recorder.Body.Bytes(), &document)).To(Succeed())

		// The manifest core admits the cassette on rides in the document.
		manifest, ok := document["x-tapes-cassette"].(map[string]any)
		Expect(ok).To(BeTrue(), "x-tapes-cassette root extension is required for admission")
		Expect(manifest).To(HaveKeyWithValue("kind", "cassette/v1alpha1"))
		identity, _ := manifest["cassette"].(map[string]any)
		Expect(identity).To(HaveKeyWithValue("name", "skills"))
		anchors, _ := manifest["api"].(map[string]any)
		Expect(anchors).To(HaveKeyWithValue("prefix_path", "api"))

		// Every declared path must be contained by /api/skills — a path outside
		// the prefix fails the whole document at admission.
		paths, _ := document["paths"].(map[string]any)
		Expect(paths).NotTo(BeEmpty())
		seenOperationIDs := map[string]bool{}
		for path, item := range paths {
			Expect(path == "/api/skills" || strings.HasPrefix(path, "/api/skills/")).
				To(BeTrue(), "path %q escapes the declared prefix", path)
			for method, op := range item.(map[string]any) {
				if method == "parameters" {
					continue
				}
				operation, ok := op.(map[string]any)
				Expect(ok).To(BeTrue())
				// Admission requires a response on every operation and
				// unique operation ids within the cassette.
				Expect(operation).To(HaveKey("responses"), "%s %s", method, path)
				id, _ := operation["operationId"].(string)
				Expect(id).NotTo(BeEmpty(), "%s %s", method, path)
				Expect(seenOperationIDs[id]).To(BeFalse(), "duplicate operationId %q", id)
				seenOperationIDs[id] = true
			}
		}
	})

	It("republishes under an installed name other than the default", func() {
		srv := server.New(server.Config{Name: "skills-two"}, storage.NewMemoryStore(), nil, nil)
		recorder := httptest.NewRecorder()
		srv.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/openapi", nil))
		var document map[string]any
		Expect(json.Unmarshal(recorder.Body.Bytes(), &document)).To(Succeed())
		paths, _ := document["paths"].(map[string]any)
		for path := range paths {
			Expect(strings.HasPrefix(path, "/api/skills-two")).To(BeTrue())
		}

		recorder = httptest.NewRecorder()
		srv.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/skills-two", nil))
		Expect(recorder.Code).To(Equal(http.StatusOK))
	})

	It("shuts down when its context is canceled", func() {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- newTestServer().Serve(ctx, listener) }()

		response, err := http.Get("http://" + listener.Addr().String() + "/ping")
		Expect(err).NotTo(HaveOccurred())
		_, _ = io.Copy(io.Discard, response.Body)
		Expect(response.Body.Close()).To(Succeed())
		cancel()
		Eventually(done).Should(Receive(Succeed()))
	})
})
