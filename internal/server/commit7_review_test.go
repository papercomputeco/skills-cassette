package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/evaluator"
	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/internal/storage/storagetest"
)

func commit7PostgresDSN() string {
	for _, key := range []string{"TEST_POSTGRES_DSN", "TAPES_TEST_POSTGRES_DSN", "TEST_DATABASE_URL"} {
		if dsn := os.Getenv(key); dsn != "" {
			return dsn
		}
	}
	return ""
}

func commit7GenerationCriteria() (evaluator.CriteriaSet, json.RawMessage) {
	criteria := evaluator.GenerationCandidateCriteria()
	encoded, err := json.Marshal(criteria.Criteria)
	Expect(err).NotTo(HaveOccurred())
	return criteria, encoded
}

func commit7GenerationIDs(page map[string]any) []string {
	values, ok := page["generations"].([]any)
	Expect(ok).To(BeTrue(), "%#v", page)
	ids := make([]string, len(values))
	for index, value := range values {
		record, recordOK := value.(map[string]any)
		Expect(recordOK).To(BeTrue(), "%#v", value)
		ids[index], recordOK = record["id"].(string)
		Expect(recordOK).To(BeTrue(), "%#v", record)
	}
	return ids
}

func runCommit7GenerationHistoryVisibilitySpec() {
	dsn := commit7PostgresDSN()
	if dsn == "" {
		Skip("TEST_POSTGRES_DSN / TAPES_TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set; this exact spec requires both Memory and real Postgres")
	}

	type fixture struct {
		name  string
		store generationCreationTestStore
	}
	memory := storage.NewMemoryStore()
	fixtures := []fixture{{name: "memory", store: memory}}
	DeferCleanup(memory.Close)

	schema := "skills_history_visibility_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	postgresStore, err := storage.OpenPostgresStore(context.Background(), dsn, schema)
	Expect(err).NotTo(HaveOccurred())
	admin, err := pgxpool.New(context.Background(), dsn)
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() {
		postgresStore.Close()
		_ = storagetest.DropSchema(context.Background(), admin, schema)
		admin.Close()
	})
	fixtures = append(fixtures, fixture{name: "postgres", store: postgresStore})

	for _, fixture := range fixtures {
		By("running private -> public -> private generation history through " + fixture.name)
		ctx := context.Background()
		const (
			creator = "generation-history-creator"
			member  = "generation-history-member"
		)
		now := time.Now().UTC().Add(-time.Minute)
		skillRecord, resolveErr := fixture.store.ResolveSkill(ctx, storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: fixture.name + "-generation-history-visibility",
			CreatorSubject: creator, CreatedAt: now,
		})
		Expect(resolveErr).NotTo(HaveOccurred())
		criteria, criteriaJSON := commit7GenerationCriteria()
		createGeneration := func(label string, createdAt time.Time, sessions []string) *storage.SkillGenerationRecord {
			created, createErr := fixture.store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: uuid.NewString(), SkillID: skillRecord.ID, CreatorSubject: creator,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: label, Description: "Visibility fixture.", Type: "workflow", Content: "# " + label,
				},
				AuthorContext: "preserve visibility", SelectedSessionIDs: sessions,
				EvaluatorProfile: criteria.Profile, EvaluatorProfileVersion: criteria.Version,
				EvaluationCriteria: criteriaJSON, CreatedAt: createdAt,
			})
			Expect(createErr).NotTo(HaveOccurred())
			return created
		}

		completed := createGeneration("completed", now.Add(time.Second), []string{"visibility-session"})
		completedClaim, claimErr := fixture.store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
			WorkerID: fixture.name + "-completion", LeaseDuration: time.Minute,
		})
		Expect(claimErr).NotTo(HaveOccurred())
		Expect(completedClaim.ID).To(Equal(completed.ID))
		Expect(fixture.store.UpdateGenerationStatus(ctx, completed.ID, completedClaim.ClaimToken,
			storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates)).To(Succeed())
		candidate, candidateErr := fixture.store.PutGenerationCandidate(ctx, completed.ID, completedClaim.ClaimToken,
			storage.GenerationCandidateRecord{
				ID: uuid.NewString(), Ordinal: 0, Kind: storage.GenerationCandidateSession,
				SourceSessionIDs: []string{"visibility-session"},
				Snapshot: storage.GenerationCandidateSnapshot{
					Name: "Visible result", Description: "Bounded result.", Type: "workflow",
					Content: "# Visible result", IsAIGenerated: true,
				},
				Insights: json.RawMessage(`[{"kind":"strength","summary":"bounded","evidence":"safe fixture"}]`), BundleSHA256: "visible-bundle",
			})
		Expect(candidateErr).NotTo(HaveOccurred())
		Expect(fixture.store.UpdateGenerationStatus(ctx, completed.ID, completedClaim.ClaimToken,
			storage.GenerationStatusGeneratingCandidates, storage.GenerationStatusEvaluatingCandidates)).To(Succeed())
		score := 0.9
		evaluation, evaluationErr := fixture.store.PutCandidateEvaluation(ctx, completed.ID, completedClaim.ClaimToken,
			storage.CandidateEvaluationRecord{
				ID: uuid.NewString(), CandidateID: candidate.ID, RequestSHA256: "visibility-request-sha",
				Profile: criteria.Profile, ProfileVersion: criteria.Version, EvaluatorVersion: "test",
				Score: &score, Decision: "pass", CriterionResults: json.RawMessage(`[]`),
				Findings: json.RawMessage(`[]`), Strengths: json.RawMessage(`["bounded"]`),
				Panel: json.RawMessage(`{"internal":"not-on-wire"}`),
			})
		Expect(evaluationErr).NotTo(HaveOccurred())
		Expect(fixture.store.UpdateGenerationSession(ctx, completed.ID, completedClaim.ClaimToken,
			storage.GenerationSessionRecord{
				SessionID: "visibility-session", Status: storage.GenerationSessionEvaluated, CandidateID: candidate.ID,
			})).To(Succeed())
		diagnostic, diagnosticErr := fixture.store.PutGenerationDiagnostic(ctx, completed.ID, completedClaim.ClaimToken,
			storage.GenerationDiagnosticRecord{
				ID: uuid.NewString(), SessionID: "visibility-session", CandidateID: candidate.ID,
				Stage: "evaluation", Code: "evaluation_unrankable",
				Message: "The candidate evaluation could not be ranked.", Retryable: false,
			})
		Expect(diagnosticErr).NotTo(HaveOccurred())
		Expect(fixture.store.UpdateGenerationStatus(ctx, completed.ID, completedClaim.ClaimToken,
			storage.GenerationStatusEvaluatingCandidates, storage.GenerationStatusSynthesizing)).To(Succeed())
		resultRevision, finalizeErr := fixture.store.AppendPrivateGenerationResult(ctx,
			storage.AppendPrivateGenerationResultInput{
				GenerationID: completed.ID, ClaimToken: completedClaim.ClaimToken,
				InitialWinnerCandidateID: candidate.ID, ResultCandidateID: candidate.ID,
			})
		Expect(finalizeErr).NotTo(HaveOccurred())

		failed := createGeneration("failed", now.Add(2*time.Second), nil)
		failedClaim, claimErr := fixture.store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
			WorkerID: fixture.name + "-failure", LeaseDuration: time.Minute,
		})
		Expect(claimErr).NotTo(HaveOccurred())
		Expect(failedClaim.ID).To(Equal(failed.ID))
		failedRecord, failErr := fixture.store.FailGeneration(ctx, storage.FailGenerationInput{
			GenerationID: failed.ID, ClaimToken: failedClaim.ClaimToken,
			Failure: storage.GenerationFailure{Code: "generation_failed", Message: "Generation could not be completed."},
		})
		Expect(failErr).NotTo(HaveOccurred())
		Expect(failedRecord.Status).To(Equal(storage.GenerationStatusFailed))
		canceled := createGeneration("canceled", now.Add(3*time.Second), nil)
		_, cancelErr := fixture.store.CancelSkillGeneration(ctx, creator, skillRecord.ID, canceled.ID)
		Expect(cancelErr).NotTo(HaveOccurred())
		inProgress := createGeneration("in-progress", now.Add(4*time.Second), nil)

		service := newSkillsServer(fixture.store)
		nestedPath := func(id string) string { return "/api/skills/" + skillRecord.ID + "/generations/" + id }
		directPath := func(id string) string { return "/api/skills/generations/" + id }
		listPath := "/api/skills/" + skillRecord.ID + "/generations?limit=100"
		read := func(path, subject string) (map[string]any, int) {
			return doJSON(service, http.MethodGet, path, "", subject)
		}
		expectedStatuses := map[string]string{
			completed.ID:  string(storage.GenerationStatusCompleted),
			failed.ID:     string(storage.GenerationStatusFailed),
			canceled.ID:   string(storage.GenerationStatusCanceled),
			inProgress.ID: string(storage.GenerationStatusQueued),
		}
		assertOwnerHistory := func() {
			ownerList, status := read(listPath, creator)
			Expect(status).To(Equal(http.StatusOK), "%s: %#v", fixture.name, ownerList)
			Expect(commit7GenerationIDs(ownerList)).To(ConsistOf(completed.ID, failed.ID, canceled.ID, inProgress.ID))
			for _, generationID := range []string{completed.ID, failed.ID, canceled.ID, inProgress.ID} {
				for _, path := range []string{nestedPath(generationID), directPath(generationID)} {
					body, exactStatus := read(path, creator)
					Expect(exactStatus).To(Equal(http.StatusOK), "%s owner GET %s", fixture.name, path)
					Expect(body).To(HaveKeyWithValue("status", expectedStatuses[generationID]))
					if generationID == completed.ID {
						Expect(body["sessions"]).To(HaveLen(1))
						Expect(body["candidates"]).To(HaveLen(1))
						Expect(body["evaluations"]).To(HaveLen(1))
						Expect(body["diagnostics"]).To(HaveLen(1))
					}
				}
			}
		}
		assertMemberHistory := func(public bool) {
			memberList, status := read(listPath, member)
			Expect(status).To(Equal(http.StatusOK), "%s: %#v", fixture.name, memberList)
			expectedIDs := []string{}
			if public {
				expectedIDs = []string{completed.ID}
			}
			Expect(commit7GenerationIDs(memberList)).To(ConsistOf(expectedIDs))
			for _, generationID := range []string{failed.ID, canceled.ID, inProgress.ID} {
				for _, path := range []string{nestedPath(generationID), directPath(generationID)} {
					_, exactStatus := read(path, member)
					Expect(exactStatus).To(Equal(http.StatusNotFound), "%s must conceal %s", fixture.name, generationID)
				}
			}
			for _, path := range []string{nestedPath(completed.ID), directPath(completed.ID)} {
				body, exactStatus := read(path, member)
				if !public {
					Expect(exactStatus).To(Equal(http.StatusNotFound), "%s private GET %s", fixture.name, path)
					continue
				}
				Expect(exactStatus).To(Equal(http.StatusOK), "%s public GET %s: %#v", fixture.name, path, body)
				Expect(body["sessions"]).To(HaveLen(1))
				Expect(body["candidates"]).To(HaveLen(1))
				Expect(body["evaluations"]).To(HaveLen(1))
				Expect(body["diagnostics"]).To(HaveLen(1))
				Expect(body["resultRevisionId"]).To(Equal(resultRevision.ID))
				serialized, marshalErr := json.Marshal(body)
				Expect(marshalErr).NotTo(HaveOccurred())
				Expect(string(serialized)).To(ContainSubstring(candidate.ID))
				Expect(string(serialized)).To(ContainSubstring(evaluation.ID))
				Expect(string(serialized)).To(ContainSubstring(diagnostic.Code))
			}
		}

		assertOwnerHistory()
		assertMemberHistory(false)
		_, visibilityErr := fixture.store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
			SkillID: skillRecord.ID, RevisionID: resultRevision.ID, CallerSubject: creator,
			IsPublic: true, ChangedAt: now.Add(5 * time.Second),
		})
		Expect(visibilityErr).NotTo(HaveOccurred())
		assertOwnerHistory()
		assertMemberHistory(true)
		_, visibilityErr = fixture.store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
			SkillID: skillRecord.ID, RevisionID: resultRevision.ID, CallerSubject: creator,
			IsPublic: false, ChangedAt: now.Add(6 * time.Second),
		})
		Expect(visibilityErr).NotTo(HaveOccurred())
		assertOwnerHistory()
		assertMemberHistory(false)
	}
}

func runCommit7SensitiveGenerationWireSpec() {
	const (
		creator           = "wire-creator-subject-secret"
		member            = "wire-member"
		claimOwner        = "claim-owner-secret"
		requestSHA        = "request-sha-secret"
		panelSecret       = "provider-panel-secret"
		providerError     = "provider-stacktrace-secret"
		providerRequestID = "provider-request-id-secret"
		rawTranscript     = "raw-transcript-secret"
		diagnosticID      = "00000000-0000-0000-0000-000000000088"
	)
	ctx := context.Background()
	store := storage.NewMemoryStore()
	DeferCleanup(store.Close)
	now := time.Now().UTC().Add(-time.Second)
	skillRecord, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
		ID: uuid.NewString(), Slug: "sensitive-generation-wire", CreatorSubject: creator, CreatedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	criteria, criteriaJSON := commit7GenerationCriteria()
	generationRecord, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
		ID: uuid.NewString(), SkillID: skillRecord.ID, CreatorSubject: creator,
		Snapshot:      storage.SkillRevisionSnapshot{Name: "Safe input", Type: "workflow", Content: "# Safe input"},
		AuthorContext: "safe", EvaluatorProfile: criteria.Profile,
		EvaluatorProfileVersion: criteria.Version, EvaluationCriteria: criteriaJSON, CreatedAt: now,
	})
	Expect(err).NotTo(HaveOccurred())
	claim, err := store.ClaimGeneration(ctx, storage.ClaimGenerationInput{WorkerID: claimOwner, LeaseDuration: time.Minute})
	Expect(err).NotTo(HaveOccurred())
	Expect(store.UpdateGenerationStatus(ctx, generationRecord.ID, claim.ClaimToken,
		storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates)).To(Succeed())
	candidate, err := store.PutGenerationCandidate(ctx, generationRecord.ID, claim.ClaimToken,
		storage.GenerationCandidateRecord{
			ID: uuid.NewString(), Ordinal: 0, Kind: storage.GenerationCandidateContext,
			Snapshot: storage.GenerationCandidateSnapshot{Name: "Safe result", Type: "workflow", Content: "# Safe result"},
			Insights: json.RawMessage(`[]`), BundleSHA256: "safe-bundle",
		})
	Expect(err).NotTo(HaveOccurred())
	Expect(store.UpdateGenerationStatus(ctx, generationRecord.ID, claim.ClaimToken,
		storage.GenerationStatusGeneratingCandidates, storage.GenerationStatusEvaluatingCandidates)).To(Succeed())
	score := 0.8
	opaquePanel := json.RawMessage(`{"transcript":"` + rawTranscript + `","provider":"` + panelSecret + `","providerError":"` + providerError + `","providerRequestId":"` + providerRequestID + `"}`)
	portableDetails := make([]map[string]any, 20)
	for index := range portableDetails {
		portableDetails[index] = map[string]any{
			"criterion_id": fmt.Sprintf("portable-%d", index), "weight": 1,
			"passed": true, "rationale": strings.Repeat("bounded", 500),
		}
	}
	portableCriterionResults, err := json.Marshal(portableDetails)
	Expect(err).NotTo(HaveOccurred())
	Expect(len(portableCriterionResults)).To(BeNumerically(">", 64<<10))
	evaluatorResult := evaluator.CandidateEvaluation{
		Profile: criteria.Profile, ProfileVersion: criteria.Version, EvaluatorVersion: "safe",
		Score: &score, Decision: "pass", CriterionResults: portableCriterionResults,
		Findings: json.RawMessage(`[]`), Strengths: json.RawMessage(`[]`), Panel: opaquePanel,
	}
	Expect(evaluatorResult.Panel).To(MatchJSON(opaquePanel), "the evaluator transport model may retain its opaque provider panel")
	evaluationRecord, err := store.PutCandidateEvaluation(ctx, generationRecord.ID, claim.ClaimToken,
		storage.CandidateEvaluationRecord{
			ID: uuid.NewString(), CandidateID: candidate.ID, RequestSHA256: requestSHA,
			Profile: evaluatorResult.Profile, ProfileVersion: evaluatorResult.ProfileVersion,
			EvaluatorVersion: evaluatorResult.EvaluatorVersion, Score: evaluatorResult.Score,
			Decision: evaluatorResult.Decision, CriterionResults: evaluatorResult.CriterionResults,
			Findings: evaluatorResult.Findings, Strengths: evaluatorResult.Strengths, Panel: evaluatorResult.Panel,
		})
	Expect(err).NotTo(HaveOccurred())
	persistedState, err := store.GetGenerationByID(ctx, creator, generationRecord.ID)
	Expect(err).NotTo(HaveOccurred())
	Expect(persistedState).NotTo(BeNil())
	Expect(persistedState.Evaluations).To(HaveLen(1))
	Expect(persistedState.Evaluations[0].Panel).To(SatisfyAny(BeEmpty(), MatchJSON(`{}`)),
		"generation history must drop or sanitize the evaluator-only opaque panel before persistence")
	persistedJSON, err := json.Marshal(persistedState)
	Expect(err).NotTo(HaveOccurred())
	for _, forbidden := range []string{rawTranscript, panelSecret, providerError, providerRequestID} {
		Expect(string(persistedJSON)).NotTo(ContainSubstring(forbidden), "persisted generation state exposed %q", forbidden)
	}
	_, err = store.PutGenerationDiagnostic(ctx, generationRecord.ID, claim.ClaimToken,
		storage.GenerationDiagnosticRecord{
			ID: diagnosticID, CandidateID: candidate.ID, Stage: "evaluation",
			Code: "evaluation_unrankable", Message: "The candidate evaluation could not be ranked.",
			Retryable: false,
		})
	Expect(err).NotTo(HaveOccurred())
	Expect(store.UpdateGenerationStatus(ctx, generationRecord.ID, claim.ClaimToken,
		storage.GenerationStatusEvaluatingCandidates, storage.GenerationStatusSynthesizing)).To(Succeed())
	resultRevision, err := store.AppendPrivateGenerationResult(ctx, storage.AppendPrivateGenerationResultInput{
		GenerationID: generationRecord.ID, ClaimToken: claim.ClaimToken,
		InitialWinnerCandidateID: candidate.ID, ResultCandidateID: candidate.ID,
	})
	Expect(err).NotTo(HaveOccurred())
	_, err = store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
		SkillID: skillRecord.ID, RevisionID: resultRevision.ID, CallerSubject: creator,
		IsPublic: true, ChangedAt: now.Add(time.Second),
	})
	Expect(err).NotTo(HaveOccurred())

	service := server.New(server.Config{}, store, nil, nil)
	paths := []string{
		"/api/skills/" + skillRecord.ID + "/generations/" + generationRecord.ID,
		"/api/skills/generations/" + generationRecord.ID,
		"/api/skills/" + skillRecord.ID + "/generations",
	}
	for _, subject := range []string{creator, member} {
		for _, path := range paths {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, path, nil)
			request.Header.Set(authSubjectHeader, subject)
			service.Handler().ServeHTTP(recorder, request)
			Expect(recorder.Code).To(Equal(http.StatusOK), "%s %s: %s", subject, path, recorder.Body.String())
			serialized := recorder.Body.String()
			for _, forbidden := range []string{
				creator, claimOwner, claim.ClaimToken, requestSHA, panelSecret, providerError, providerRequestID, rawTranscript, diagnosticID,
				`"creatorSubject"`, `"claimOwner"`, `"claimToken"`, `"leaseExpiresAt"`,
				`"lastHeartbeatAt"`, `"requestSHA256"`, `"panel"`,
			} {
				Expect(serialized).NotTo(ContainSubstring(forbidden), "%s exposed %q", path, forbidden)
			}
			if strings.Contains(path, "/generations/") {
				Expect(serialized).To(ContainSubstring(evaluationRecord.ID))
				Expect(serialized).To(ContainSubstring("evaluation_unrankable"))
				Expect(serialized).To(ContainSubstring("portable-19"),
					"legal structured evaluation details must never be replaced with an empty array")
			}
		}
	}

	openAPI := httptest.NewRecorder()
	service.Handler().ServeHTTP(openAPI, httptest.NewRequest(http.MethodGet, "/openapi", nil))
	Expect(openAPI.Code).To(Equal(http.StatusOK))
	var document any
	Expect(json.Unmarshal(openAPI.Body.Bytes(), &document)).To(Succeed())
	keys := map[string]struct{}{}
	collectJSONKeys(document, keys)
	for _, forbidden := range []string{
		"creatorSubject", "claimOwner", "claimToken", "leaseExpiresAt", "lastHeartbeatAt",
		"requestSHA256", "panel", "rawError", "providerError", "transcript", "transcripts",
	} {
		Expect(keys).NotTo(HaveKey(forbidden), "OpenAPI advertises sensitive field %q", forbidden)
	}
}
