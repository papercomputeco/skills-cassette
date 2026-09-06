package storagetest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck // Ginkgo's DSL is intentionally dot-imported in test contracts.
	. "github.com/onsi/gomega"    //nolint:staticcheck // Gomega's matchers form the same test DSL.

	"github.com/papercomputeco/skills-cassette/internal/storage"
)

// LifecycleStore is the generation capability shared by production stores.
type LifecycleStore interface {
	storage.GenerationStore
	storage.SkillIdentityStore
	storage.RevisionStore
	storage.RevisionMetadataStore
}

// LifecycleStoreFactory returns an isolated store and its cleanup callback.
type LifecycleStoreFactory func() (LifecycleStore, func())

// LifecycleStoreContract registers owner, conflict, cancellation, and cascade
// behavior that must match across memory and Postgres implementations.
func LifecycleStoreContract(name string, newStore LifecycleStoreFactory, newID func() string) bool {
	return Describe("LifecycleStore contract "+name, func() {
		var (
			ctx     context.Context
			store   LifecycleStore
			cleanup func()
		)

		BeforeEach(func() {
			ctx = context.Background()
			store, cleanup = newStore()
		})
		AfterEach(func() {
			if cleanup != nil {
				cleanup()
			}
		})

		It("ignores caller creation clocks and stamps the generation snapshot atomically", func() {
			creator := "storage-clock-owner"
			skill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: newID(), Slug: "generation-storage-clock", CreatorSubject: creator, CreatedAt: time.Now().UTC(),
			})
			Expect(err).NotTo(HaveOccurred())
			callerTime := time.Date(1999, 1, 2, 3, 4, 5, 0, time.UTC)
			before := time.Now().UTC().Add(-time.Second)
			generation, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: creator,
				Snapshot:           storage.SkillRevisionSnapshot{Name: "Clocked", Type: "workflow", Content: "# Clocked"},
				SelectedSessionIDs: []string{"clock-a", "clock-b"},
				EvaluatorProfile:   "generation-candidate-v1", EvaluatorProfileVersion: "1",
				EvaluationCriteria: json.RawMessage(`[{
					"id":"clock","kind":"content","description":"Clocked.","weight":1
				}]`),
				CreatedAt: callerTime,
			})
			after := time.Now().UTC().Add(time.Second)
			Expect(err).NotTo(HaveOccurred())
			Expect(generation.CreatedAt).NotTo(BeTemporally("==", callerTime))
			Expect(generation.CreatedAt).To(BeTemporally(">=", before))
			Expect(generation.CreatedAt).To(BeTemporally("<=", after))
			Expect(generation.UpdatedAt).To(BeTemporally("==", generation.CreatedAt))
			Expect(generation.NextAttemptAt).To(BeTemporally("==", generation.CreatedAt))
			state, err := store.GetGenerationByID(ctx, creator, generation.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(state.Sessions).To(HaveLen(2))
			for _, session := range state.Sessions {
				Expect(session.UpdatedAt).To(BeTemporally("==", generation.CreatedAt))
			}

			retry, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: generation.ID, SkillID: skill.ID, CreatorSubject: creator,
				Snapshot: generation.Snapshot, SelectedSessionIDs: []string{"clock-a", "clock-b"},
				EvaluatorProfile:        generation.EvaluatorProfile,
				EvaluatorProfileVersion: generation.EvaluatorProfileVersion,
				EvaluationCriteria:      generation.EvaluationCriteria, CreatedAt: callerTime.Add(100 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(retry).To(Equal(generation), "caller time does not participate in idempotent creation")
		})

		It("allows only the ordered nonterminal generation status chain", func() {
			creator := "status-chain-owner"
			skill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: newID(), Slug: "generation-status-chain", CreatorSubject: creator, CreatedAt: time.Now().UTC(),
			})
			Expect(err).NotTo(HaveOccurred())
			generation, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: creator,
				Snapshot:         storage.SkillRevisionSnapshot{Name: "Status chain", Type: "workflow", Content: "# Status chain"},
				EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
				EvaluationCriteria: json.RawMessage(`[{"id":"status","kind":"content","description":"Ordered.","weight":1}]`),
			})
			Expect(err).NotTo(HaveOccurred())
			claim, err := store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
				WorkerID: "status-chain-worker", LeaseDuration: time.Minute,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(claim).NotTo(BeNil())

			allStatuses := []storage.GenerationStatus{
				storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates,
				storage.GenerationStatusEvaluatingCandidates, storage.GenerationStatusSynthesizing,
				storage.GenerationStatusCompleted, storage.GenerationStatusCanceled, storage.GenerationStatusFailed,
			}
			assertOnlyTransition := func(from, allowed storage.GenerationStatus) {
				for _, to := range allStatuses {
					if to == allowed {
						continue
					}
					Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken, from, to)).To(
						MatchError(storage.ErrInvalidGenerationState), "%s -> %s must be rejected", from, to,
					)
				}
				Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken, from, allowed)).To(Succeed())
			}
			assertOnlyTransition(storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates)
			assertOnlyTransition(storage.GenerationStatusGeneratingCandidates, storage.GenerationStatusEvaluatingCandidates)
			assertOnlyTransition(storage.GenerationStatusEvaluatingCandidates, storage.GenerationStatusSynthesizing)
			for _, to := range allStatuses {
				Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
					storage.GenerationStatusSynthesizing, to)).To(MatchError(storage.ErrInvalidGenerationState))
			}
			canceled, err := store.CancelSkillGeneration(ctx, creator, skill.ID, generation.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(canceled.Generation.Status).To(Equal(storage.GenerationStatusCanceled))
			Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
				storage.GenerationStatusCanceled, storage.GenerationStatusCompleted)).To(MatchError(storage.ErrInvalidGenerationState))
		})

		It("enforces shared session and candidate artifact invariants before persistence", func() {
			creator := "artifact-invariant-owner"
			skill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: newID(), Slug: "generation-artifact-invariants", CreatorSubject: creator,
			})
			Expect(err).NotTo(HaveOccurred())
			generation, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: creator,
				Snapshot:           storage.SkillRevisionSnapshot{Name: "Artifact invariants", Type: "workflow", Content: "# Invariants"},
				SelectedSessionIDs: []string{"source-a"}, EvaluatorProfile: "generation-candidate-v1",
				EvaluatorProfileVersion: "1",
				EvaluationCriteria:      json.RawMessage(`[{"id":"artifact","kind":"content","description":"Bounded.","weight":1}]`),
			})
			Expect(err).NotTo(HaveOccurred())
			claim, err := store.ClaimGeneration(ctx, storage.ClaimGenerationInput{WorkerID: "artifact-worker", LeaseDuration: time.Minute})
			Expect(err).NotTo(HaveOccurred())

			invalidCandidates := []storage.GenerationCandidateRecord{
				{ID: newID(), GenerationID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateSession, SourceSessionIDs: []string{"source-a"}},
				{ID: newID(), Ordinal: -1, Kind: storage.GenerationCandidateSession, SourceSessionIDs: []string{"source-a"}},
				{ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateKind("legacy"), SourceSessionIDs: []string{"source-a"}},
				{ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateSession, SourceSessionIDs: []string{"wrong-source"}},
				{ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateContext},
			}
			for index := range invalidCandidates {
				invalidCandidates[index].Snapshot = storage.GenerationCandidateSnapshot{
					Name: "Invalid candidate", Type: "workflow", Content: "# Invalid",
				}
				invalidCandidates[index].Insights = json.RawMessage(`[]`)
				invalidCandidates[index].BundleSHA256 = "caller-value-is-recomputed"
				_, err = store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, invalidCandidates[index])
				Expect(err).To(HaveOccurred(), "invalid candidate %d", index)
			}

			candidateInput := storage.GenerationCandidateRecord{
				ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateSession,
				SourceSessionIDs: []string{"source-a"},
				Snapshot:         storage.GenerationCandidateSnapshot{Name: "Valid candidate", Type: "workflow", Content: "# Valid"},
				Insights:         json.RawMessage(`[{"kind":"fact","summary":" bounded ","evidence":" source "}]`),
				BundleSHA256:     strings.Repeat("A", 64),
			}
			candidate, err := store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, candidateInput)
			Expect(err).NotTo(HaveOccurred())
			Expect(candidate.Insights).To(MatchJSON(`[{"kind":"fact","summary":"bounded","evidence":"source"}]`))
			Expect(candidate.BundleSHA256).To(MatchRegexp(`^[0-9a-f]{64}$`))
			Expect(candidate.BundleSHA256).NotTo(Equal(candidateInput.BundleSHA256))
			_, err = store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
				ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateSession,
				SourceSessionIDs: []string{"source-a"},
				Snapshot:         storage.GenerationCandidateSnapshot{Name: "Conflict", Type: "workflow", Content: "# Conflict"},
				Insights:         json.RawMessage(`[]`), BundleSHA256: "conflict",
			})
			Expect(err).To(MatchError(ContainSubstring("conflicting idempotent artifact")))

			for _, session := range []storage.GenerationSessionRecord{
				{GenerationID: newID(), SessionID: "source-a", Status: storage.GenerationSessionPending},
				{SessionID: "source-a", Status: storage.GenerationSessionStatus("legacy")},
				{SessionID: "source-a", Status: storage.GenerationSessionCandidateReady},
				{SessionID: "source-a", Status: storage.GenerationSessionPending, CandidateID: candidate.ID},
			} {
				Expect(store.UpdateGenerationSession(ctx, generation.ID, claim.ClaimToken, session)).To(HaveOccurred())
			}
			Expect(store.UpdateGenerationSession(ctx, generation.ID, claim.ClaimToken, storage.GenerationSessionRecord{
				SessionID: "source-a", Status: storage.GenerationSessionCandidateReady, CandidateID: candidate.ID,
			})).To(Succeed())

			synthesis, err := store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
				ID: newID(), Ordinal: 1, Kind: storage.GenerationCandidateSynthesis,
				SourceSessionIDs: []string{"source-a"},
				Snapshot:         storage.GenerationCandidateSnapshot{Name: "Synthesis", Type: "workflow", Content: "# Synthesis"},
				Insights:         json.RawMessage(`[]`), BundleSHA256: "synthesis",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(synthesis.Ordinal).To(Equal(1))
			_, err = store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
				ID: newID(), Ordinal: 2, Kind: storage.GenerationCandidateSynthesis,
				SourceSessionIDs: []string{"source-a"},
				Snapshot:         storage.GenerationCandidateSnapshot{Name: "Second synthesis", Type: "workflow", Content: "# Second"},
				Insights:         json.RawMessage(`[]`), BundleSHA256: "second-synthesis",
			})
			Expect(err).To(MatchError(ContainSubstring("already has a synthesis candidate")))
		})

		It("validates diagnostic ownership and replaces logical source-stage slots", func() {
			creator := "diagnostic-contract-owner"
			skill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: newID(), Slug: "generation-diagnostic-contract", CreatorSubject: creator, CreatedAt: time.Now().UTC(),
			})
			Expect(err).NotTo(HaveOccurred())
			generation, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: creator,
				Snapshot:           storage.SkillRevisionSnapshot{Name: "Diagnostics", Type: "workflow", Content: "# Diagnostics"},
				SelectedSessionIDs: []string{"owned-session"}, EvaluatorProfile: "generation-candidate-v1",
				EvaluatorProfileVersion: "1",
				EvaluationCriteria:      json.RawMessage(`[{"id":"diagnostic","kind":"content","description":"Bounded.","weight":1}]`),
			})
			Expect(err).NotTo(HaveOccurred())
			claim, err := store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
				WorkerID: "diagnostic-contract-worker", LeaseDuration: time.Minute,
			})
			Expect(err).NotTo(HaveOccurred())
			candidate, err := store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken,
				storage.GenerationCandidateRecord{
					ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateSession,
					SourceSessionIDs: []string{"owned-session"},
					Snapshot:         storage.GenerationCandidateSnapshot{Name: "Diagnostic candidate", Type: "workflow", Content: "# Candidate"},
					Insights:         json.RawMessage(`[]`), BundleSHA256: "diagnostic-candidate",
				})
			Expect(err).NotTo(HaveOccurred())

			_, err = store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: newID(), GenerationID: newID(), CandidateID: candidate.ID,
				Stage: "evaluation", Code: "evaluation_unrankable",
				Message: "The candidate evaluation could not be ranked.",
			})
			Expect(err).To(MatchError(ContainSubstring("another generation")))
			_, err = store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: newID(), Stage: "arbitrary", Code: "provider_secret", Message: "raw provider detail",
			})
			Expect(err).To(MatchError(ContainSubstring("supported catalog")))
			_, err = store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: newID(), SessionID: "foreign-session", Stage: "transcript", Code: "transcript_unavailable",
				Message: "The selected session transcript could not be loaded.",
			})
			Expect(err).To(MatchError(ContainSubstring("not found")))
			_, err = store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: newID(), CandidateID: newID(), Stage: "evaluation", Code: "evaluation_unrankable",
				Message: "The candidate evaluation could not be ranked.",
			})
			Expect(err).To(MatchError(ContainSubstring("not found")))

			first, err := store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: newID(), SessionID: "owned-session", Stage: "transcript", Code: "transcript_unavailable",
				Message:   "The selected session transcript could not be loaded.",
				CreatedAt: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
			})
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(2 * time.Millisecond)
			replacement, err := store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: newID(), SessionID: "owned-session", Stage: "transcript", Code: "transcript_unavailable",
				Message: "The selected session transcript could not be loaded.", Retryable: true,
				CreatedAt: time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(replacement.CreatedAt).To(BeTemporally(">", first.CreatedAt), "caller diagnostic times are ignored")
			time.Sleep(2 * time.Millisecond)
			evaluationDiagnostic, err := store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: newID(), SessionID: "owned-session", CandidateID: candidate.ID,
				Stage: "evaluation", Code: "evaluation_unrankable",
				Message: "The candidate evaluation could not be ranked.",
			})
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(2 * time.Millisecond)
			synthesisDiagnostic, err := store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken, storage.GenerationDiagnosticRecord{
				ID: newID(), CandidateID: candidate.ID, Stage: "synthesis", Code: "synthesis_generation_failed",
				Message: "The optional synthesis candidate could not be generated.",
			})
			Expect(err).NotTo(HaveOccurred())

			state, err := store.GetGenerationByID(ctx, creator, generation.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(state.Diagnostics).To(HaveLen(3))
			Expect(state.Diagnostics[0].ID).To(Equal(synthesisDiagnostic.ID))
			Expect(state.Diagnostics[1].ID).To(Equal(evaluationDiagnostic.ID))
			Expect(state.Diagnostics[2].ID).To(Equal(replacement.ID))
			Expect(state.Diagnostics).NotTo(ContainElement(HaveField("ID", first.ID)))
		})

		It("validates evaluation identity, base access, and visibility-independent completed retries", func() {
			storeNow := time.Now().UTC().Add(-time.Minute)
			baseOwner := "generation-base-owner"
			creator := "generation-result-owner"
			skill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: newID(), Slug: "generation-finalization-contract", CreatorSubject: baseOwner, CreatedAt: storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			base, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: baseOwner,
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Generation base", Type: "workflow", Content: "# Generation base",
				},
				CreatedAt: storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			_, err = store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: base.ID, CallerSubject: baseOwner,
				IsPublic: true, ChangedAt: storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			generation, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: newID(), SkillID: skill.ID, BaseRevisionID: base.ID, CreatorSubject: creator,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Generation seed", Type: "workflow", Content: "# Generation seed",
				},
				SelectedSessionIDs: []string{"generation-source"},
				EvaluatorProfile:   "caller-profile", EvaluatorProfileVersion: "caller-version",
				EvaluationCriteria: json.RawMessage(`[{
					"id":"rankable","kind":"structure","description":"Complete.","weight":1
				}]`),
				CreatedAt: storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			claim, err := store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
				WorkerID: "finalization-contract-worker", LeaseDuration: time.Minute,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(claim).NotTo(BeNil())
			Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
				storage.GenerationStatusQueued, storage.GenerationStatusGeneratingCandidates)).To(Succeed())
			candidate, err := store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken,
				storage.GenerationCandidateRecord{
					ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateSession,
					SourceSessionIDs: []string{"generation-source"},
					Snapshot: storage.GenerationCandidateSnapshot{
						Name: "Generation result", Type: "workflow", Content: "# Generation result",
						IsAIGenerated: true,
					},
					Insights: json.RawMessage(`[]`), BundleSHA256: "generation-result-bundle",
				})
			Expect(err).NotTo(HaveOccurred())
			Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
				storage.GenerationStatusGeneratingCandidates, storage.GenerationStatusEvaluatingCandidates)).To(Succeed())
			score := 0.9
			evaluation := storage.CandidateEvaluationRecord{
				ID: newID(), CandidateID: candidate.ID, RequestSHA256: "generation-result-request",
				Profile: "other-profile", ProfileVersion: "caller-version", EvaluatorVersion: "contract",
				Score: &score, Decision: "pass", CriterionResults: json.RawMessage(`[]`),
				Findings: json.RawMessage(`[]`), Strengths: json.RawMessage(`[]`), Panel: json.RawMessage(`{}`),
			}
			storedEvaluation, err := store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, evaluation)
			Expect(storedEvaluation).To(BeNil())
			Expect(err).To(MatchError(storage.ErrInvalidGenerationState))
			evaluation.Profile = "caller-profile"
			evaluation.GenerationID = newID()
			_, err = store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, evaluation)
			Expect(err).To(MatchError(ContainSubstring("another generation")))
			evaluation.GenerationID = ""
			evaluation.ID = newID()
			storedEvaluation, err = store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, evaluation)
			Expect(err).NotTo(HaveOccurred())
			Expect(storedEvaluation.Profile).To(Equal(generation.EvaluatorProfile))
			Expect(storedEvaluation.ProfileVersion).To(Equal(generation.EvaluatorProfileVersion))
			Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
				storage.GenerationStatusEvaluatingCandidates, storage.GenerationStatusSynthesizing)).To(Succeed())

			_, err = store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: base.ID, CallerSubject: creator,
				IsPublic: false, ChangedAt: storeNow.Add(time.Second),
			})
			Expect(err).NotTo(HaveOccurred())
			input := storage.AppendPrivateGenerationResultInput{
				GenerationID: generation.ID, ClaimToken: claim.ClaimToken,
				InitialWinnerCandidateID: candidate.ID, ResultCandidateID: candidate.ID,
			}
			revision, err := store.AppendPrivateGenerationResult(ctx, input)
			Expect(revision).To(BeNil())
			Expect(err).To(MatchError(storage.ErrRevisionNotFound))

			_, err = store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: base.ID, CallerSubject: baseOwner,
				IsPublic: true, ChangedAt: storeNow.Add(2 * time.Second),
			})
			Expect(err).NotTo(HaveOccurred())
			revision, err = store.AppendPrivateGenerationResult(ctx, input)
			Expect(err).NotTo(HaveOccurred())
			Expect(revision).NotTo(BeNil())
			renewed, err := store.RenewGenerationLease(ctx, generation.ID, claim.ClaimToken, time.Minute)
			Expect(renewed).To(BeFalse())
			Expect(err).To(MatchError(storage.ErrGenerationCompleted),
				"the completing claim is distinct from expiry, reclaim, and creator cancellation")
			_, err = store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: creator,
				IsPublic: true, ChangedAt: storeNow.Add(3 * time.Second),
			})
			Expect(err).NotTo(HaveOccurred())
			_, err = store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: creator,
				ChangedAt: storeNow.Add(3 * time.Second),
			})
			Expect(err).NotTo(HaveOccurred())
			retry, err := store.AppendPrivateGenerationResult(ctx, input)
			Expect(err).NotTo(HaveOccurred())
			Expect(retry).To(Equal(revision), "completed retry must ignore mutable visibility and latest")
		})

		It("validates persisted snapshots and evaluator-owned detail JSON", func() {
			storeNow := time.Now().UTC().Add(-time.Minute)
			creator := "strict-storage-owner"
			skill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: newID(), Slug: "strict-storage-boundaries", CreatorSubject: creator, CreatedAt: storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			_, err = store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: creator, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: strings.Repeat("n", storage.MaxRevisionNameCodePoints+1),
					Type: "workflow", Content: "# Invalid",
				},
			})
			Expect(err).To(MatchError(ContainSubstring(storage.ErrRevisionSnapshotInvalid.Error())))
			_, err = store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: creator,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Oversized generation", Type: "workflow",
					Content: strings.Repeat("x", storage.MaxRevisionContentCodePoints+1),
				},
				EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
				EvaluationCriteria: json.RawMessage(`[{"id":"strict","kind":"content","description":"Strict.","weight":1}]`), CreatedAt: storeNow,
			})
			Expect(err).To(MatchError(ContainSubstring(storage.ErrRevisionSnapshotInvalid.Error())))

			generation, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: creator,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: " Strict generation ", Description: " Strict storage validation. ",
					Type: "workflow", Tags: []string{" strict "}, Content: " # Strict generation ",
				},
				AuthorContext:    " Strictly persist artifacts. ",
				EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
				EvaluationCriteria: json.RawMessage(`[{"id":"strict","kind":"content","description":"Strict.","weight":1}]`), CreatedAt: storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(generation.Snapshot.Name).To(Equal("Strict generation"))
			Expect(generation.Snapshot.Tags).To(Equal([]string{"strict"}))
			Expect(generation.AuthorContext).To(Equal("Strictly persist artifacts."))
			claim, err := store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
				WorkerID: "strict-storage-worker", LeaseDuration: time.Minute,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(claim).NotTo(BeNil())
			_, err = store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
				ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateContext,
				Snapshot: storage.GenerationCandidateSnapshot{
					Name: "Invalid candidate", Type: "workflow", Content: "# Invalid\x00",
				},
				Insights: json.RawMessage(`[]`), BundleSHA256: "invalid-candidate",
			})
			Expect(err).To(MatchError(ContainSubstring(storage.ErrRevisionSnapshotInvalid.Error())))
			tooManyInsights := make([]map[string]string, storage.MaxGenerationCandidateInsights+1)
			for index := range tooManyInsights {
				tooManyInsights[index] = map[string]string{"kind": "fact", "summary": "bounded", "evidence": "source"}
			}
			tooManyInsightsJSON, err := json.Marshal(tooManyInsights)
			Expect(err).NotTo(HaveOccurred())
			oversizedInsightJSON, err := json.Marshal([]map[string]string{{
				"kind": "fact", "summary": strings.Repeat("界", storage.MaxGenerationCandidateInsightRunes+1), "evidence": "source",
			}})
			Expect(err).NotTo(HaveOccurred())
			for _, malformedInsights := range []json.RawMessage{
				json.RawMessage(`[{"kind":"fact","summary":"bounded","evidence":"source","unsafe":"legacy"}]`),
				json.RawMessage(`[{"kind":"fact","summary":"bounded","evidence":"source","unsafe":"legacy","unsafe":"erased"}]`),
				json.RawMessage(`[{"kind":"fact","kind":"duplicate","summary":"bounded","evidence":"source"}]`),
				json.RawMessage(`[{"kind":"fact","summary":"bounded"}]`),
				tooManyInsightsJSON,
				oversizedInsightJSON,
			} {
				_, err = store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
					ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateContext,
					Snapshot: storage.GenerationCandidateSnapshot{Name: "Malformed insight", Type: "workflow", Content: "# Invalid"},
					Insights: malformedInsights, BundleSHA256: "untrusted",
				})
				Expect(err).To(MatchError(ContainSubstring("insight")))
			}

			maxInsights := make([]map[string]string, storage.MaxGenerationCandidateInsights)
			for index := range maxInsights {
				maxInsights[index] = map[string]string{
					"kind":     strings.Repeat("<", storage.MaxGenerationCandidateInsightRunes),
					"summary":  strings.Repeat("<", storage.MaxGenerationCandidateInsightRunes),
					"evidence": strings.Repeat("<", storage.MaxGenerationCandidateInsightRunes),
				}
			}
			maxInsightsJSON, err := json.Marshal(maxInsights)
			Expect(err).NotTo(HaveOccurred())
			Expect(maxInsightsJSON).To(HaveLen(storage.MaxGenerationCandidateInsightsJSONBytes),
				"the raw byte ceiling must admit the exact legal JSON-escaped maximum")
			candidate, err := store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
				ID: newID(), Ordinal: 0, Kind: storage.GenerationCandidateContext,
				Snapshot: storage.GenerationCandidateSnapshot{
					Name: "Strict candidate", Type: "workflow", Content: "# Strict candidate",
				},
				Insights: maxInsightsJSON, BundleSHA256: strings.Repeat("F", 64),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(candidate.Insights).To(HaveLen(storage.MaxGenerationCandidateInsightsJSONBytes))
			Expect(candidate.BundleSHA256).To(MatchRegexp(`^[0-9a-f]{64}$`))

			score := 0.8
			baseEvaluation := storage.CandidateEvaluationRecord{
				ID: newID(), CandidateID: candidate.ID, RequestSHA256: "strict-evaluation",
				Profile: generation.EvaluatorProfile, ProfileVersion: generation.EvaluatorProfileVersion,
				EvaluatorVersion: "contract", Score: &score, Decision: "pass",
				CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`),
				Strengths: json.RawMessage(`[]`), Panel: json.RawMessage(`{"must":"not persist"}`),
			}
			invalidEvaluationID := baseEvaluation
			invalidEvaluationID.ID = "not-a-uuid"
			invalidEvaluationID.RequestSHA256 = "invalid-evaluation-id"
			_, err = store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, invalidEvaluationID)
			Expect(err).To(MatchError(ContainSubstring("candidate evaluation id is invalid")),
				"memory and Postgres must reject malformed evaluation IDs identically before persistence")
			for index, invalidVersion := range []string{
				strings.Repeat("界", storage.MaxCandidateEvaluatorVersionCodePoints+1), "version\ncontrol",
			} {
				evaluation := baseEvaluation
				evaluation.ID = newID()
				evaluation.RequestSHA256 = fmt.Sprintf("invalid-version-%d", index)
				evaluation.EvaluatorVersion = invalidVersion
				_, err = store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, evaluation)
				Expect(err).To(MatchError(ContainSubstring("evaluator version")))
			}
			for index, malformed := range []struct {
				field string
				raw   json.RawMessage
			}{
				{field: "criteria", raw: json.RawMessage(`[{"criterion_id":"x","weight":1,"passed":true,"rationale":"ok","extra":true}]`)},
				{field: "criteria", raw: json.RawMessage(`[{"criterion_id":"x","weight":1,"passed":true,"rationale":"ok","extra":true,"extra":false}]`)},
				{field: "criteria", raw: json.RawMessage(`[{"criterion_id":"x","criterion_id":"x","weight":1,"passed":true,"rationale":"ok"}]`)},
				{field: "findings", raw: json.RawMessage(`{"not":"an array"}`)},
				{field: "findings", raw: json.RawMessage(`[{"severity":"warning","message":"bounded","extra":true,"extra":false}]`)},
				{field: "strengths", raw: json.RawMessage(`[{"value":"bounded","value":"erased"}]`)},
				{field: "panel", raw: json.RawMessage(`{"judge":{"score":1,"score":0}}`)},
			} {
				evaluation := baseEvaluation
				evaluation.ID = newID()
				evaluation.RequestSHA256 = fmt.Sprintf("malformed-%d", index)
				switch malformed.field {
				case "criteria":
					evaluation.CriterionResults = malformed.raw
				case "findings":
					evaluation.Findings = malformed.raw
				case "strengths":
					evaluation.Strengths = malformed.raw
				case "panel":
					evaluation.Panel = malformed.raw
				}
				_, err = store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, evaluation)
				Expect(err).To(HaveOccurred(), "structured %s artifact %d must be rejected", malformed.field, index)
			}
			largeCriteria := make([]map[string]any, 70)
			for index := range largeCriteria {
				largeCriteria[index] = map[string]any{
					"criterion_id": fmt.Sprintf("large-%d", index), "weight": 1,
					"passed": true, "rationale": strings.Repeat("x", 4000),
				}
			}
			oversizedCriteria, err := json.Marshal(largeCriteria)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(oversizedCriteria)).To(BeNumerically(">", 256<<10))
			oversizedEvaluation := baseEvaluation
			oversizedEvaluation.ID = newID()
			oversizedEvaluation.RequestSHA256 = "oversized-details"
			oversizedEvaluation.CriterionResults = oversizedCriteria
			_, err = store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, oversizedEvaluation)
			Expect(err).To(MatchError(ContainSubstring("criterion results")))

			portableCriteria, err := json.Marshal(largeCriteria[:20])
			Expect(err).NotTo(HaveOccurred())
			Expect(len(portableCriteria)).To(BeNumerically(">", 64<<10))
			Expect(len(portableCriteria)).To(BeNumerically("<", 256<<10))
			baseEvaluation.CriterionResults = portableCriteria
			baseEvaluation.Findings = json.RawMessage(`[{"severity":"warning","message":"bounded"}]`)
			baseEvaluation.CriticalFindingCount = 9
			baseEvaluation.WarningFindingCount = 9
			baseEvaluation.EvaluatorVersion = strings.Repeat("界", storage.MaxCandidateEvaluatorVersionCodePoints)
			persisted, err := store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, baseEvaluation)
			Expect(err).NotTo(HaveOccurred())
			Expect(persisted.CriticalFindingCount).To(Equal(0))
			Expect(persisted.WarningFindingCount).To(Equal(1))
			Expect(persisted.Panel).To(MatchJSON(`{}`), "provider panel payloads must be erased before persistence")
			Expect(persisted.EvaluatorVersion).To(Equal(strings.Repeat("界", storage.MaxCandidateEvaluatorVersionCodePoints)))
			Expect(persisted.CriterionResults).To(MatchJSON(portableCriteria))
			Expect(persisted.CriterionResults).NotTo(MatchJSON(`[]`))
			Expect(persisted.Findings).To(MatchJSON(`[{"severity":"warning","message":"bounded"}]`))
		})

		It("clears stale retry diagnostics when canceling queued work", func() {
			storeNow := time.Now().UTC().Add(-time.Minute)
			creator := "cancel-retry-owner"
			skill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: newID(), Slug: "cancel-retry-state", CreatorSubject: creator, CreatedAt: storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			generation, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: creator,
				Snapshot:      storage.SkillRevisionSnapshot{Name: "Cancelable", Type: "workflow", Content: "# Cancelable"},
				AuthorContext: "Cancel stale retries.", EvaluatorProfile: "generation-candidate-v1",
				EvaluatorProfileVersion: "1",
				EvaluationCriteria:      json.RawMessage(`[{"id":"cancel","kind":"content","description":"Cancelable.","weight":1}]`),
				CreatedAt:               storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			claim, err := store.ClaimGeneration(ctx, storage.ClaimGenerationInput{WorkerID: "retry-worker", LeaseDuration: time.Minute})
			Expect(err).NotTo(HaveOccurred())
			_, err = store.RequeueGeneration(ctx, storage.RequeueGenerationInput{
				GenerationID: generation.ID, ClaimToken: claim.ClaimToken,
				Failure:    storage.GenerationFailure{Code: "generation_retrying", Message: "Generation will be retried."},
				RetryAfter: time.Minute,
			})
			Expect(err).NotTo(HaveOccurred())
			canceled, err := store.CancelSkillGeneration(ctx, creator, skill.ID, generation.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(canceled.Generation.Status).To(Equal(storage.GenerationStatusCanceled))
			Expect(canceled.Generation.ErrorCode).To(BeEmpty())
			Expect(canceled.Generation.ErrorMessage).To(BeEmpty())
			Expect(canceled.Generation.CompletedAt).NotTo(BeNil())
			Expect(*canceled.Generation.CompletedAt).To(BeTemporally("==", canceled.Generation.UpdatedAt))
			Expect(canceled.Generation.NextAttemptAt).To(BeTemporally("==", canceled.Generation.UpdatedAt),
				"the returned retry and update timestamps must come from one storage clock sample")
		})

		It("retains_the_maximum_session_candidates_and_synthesis", func() {
			storeNow := time.Now().UTC().Add(-time.Minute)
			creator := "bounded-contract-owner"
			skill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
				ID: newID(), Slug: "bounded-contract", CreatorSubject: creator, CreatedAt: storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			sessions := make([]string, 100)
			for ordinal := range sessions {
				sessions[ordinal] = fmt.Sprintf("session-%03d", ordinal+1)
			}
			generation, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: creator,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Bounded contract", Type: "workflow", Content: "# Bounded contract",
				},
				AuthorContext: "Retain all bounded work.", SelectedSessionIDs: sessions,
				EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
				EvaluationCriteria: json.RawMessage(`[{"id":"rankable","weight":1}]`),
				CreatedAt:          storeNow,
			})
			Expect(err).NotTo(HaveOccurred())
			claim, err := store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
				WorkerID: "bounded-contract-worker", LeaseDuration: time.Minute,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(claim).NotTo(BeNil())

			candidateIDs := make([]string, 101)
			for ordinal := len(candidateIDs) - 1; ordinal >= 0; ordinal-- {
				candidateIDs[ordinal] = newID()
				kind := storage.GenerationCandidateSession
				var sources []string
				if ordinal < len(sessions) {
					sources = []string{sessions[ordinal]}
				} else {
					kind = storage.GenerationCandidateSynthesis
					sources = append([]string(nil), sessions...)
				}
				_, err = store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
					ID: candidateIDs[ordinal], Ordinal: ordinal, Kind: kind,
					SourceSessionIDs: sources,
					Snapshot: storage.GenerationCandidateSnapshot{
						Name: fmt.Sprintf("Candidate %03d", ordinal), Type: "workflow",
						Content: fmt.Sprintf("# Candidate %03d", ordinal),
					},
					Insights: json.RawMessage(`[]`), BundleSHA256: fmt.Sprintf("bundle-%03d", ordinal),
					CreatedAt: storeNow.Add(time.Duration(ordinal) * time.Second),
				})
				Expect(err).NotTo(HaveOccurred())
				score := 0.5
				switch ordinal {
				case 99:
					score = 0.99
				case 100:
					score = 0.98
				}
				_, err = store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, storage.CandidateEvaluationRecord{
					ID: newID(), CandidateID: candidateIDs[ordinal],
					RequestSHA256: fmt.Sprintf("request-%03d", ordinal),
					Profile:       "generation-candidate-v1", ProfileVersion: "1", EvaluatorVersion: "contract",
					Score: &score, Decision: "pass", CriterionResults: json.RawMessage(`[]`),
					Findings: json.RawMessage(`[]`), Strengths: json.RawMessage(`[]`), Panel: json.RawMessage(`{}`),
					CreatedAt: storeNow.Add(time.Duration(ordinal) * time.Second),
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Write the synthesis resume marker first so retention must preserve
			// it when more than 303 logical slots are subsequently observed.
			synthesisMarker, err := store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken,
				storage.GenerationDiagnosticRecord{
					ID: newID(), CandidateID: candidateIDs[100], Stage: "synthesis", Code: "synthesis_generation_failed",
					Message: "The optional synthesis candidate could not be generated.",
				})
			Expect(err).NotTo(HaveOccurred())
			for ordinal, sessionID := range sessions {
				_, err = store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken,
					storage.GenerationDiagnosticRecord{
						ID: newID(), SessionID: sessionID, Stage: "transcript", Code: "transcript_unavailable",
						Message: "The selected session transcript could not be loaded.",
					})
				Expect(err).NotTo(HaveOccurred())
				_, err = store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken,
					storage.GenerationDiagnosticRecord{
						ID: newID(), SessionID: sessionID, Stage: "candidate", Code: "candidate_generation_failed",
						Message: "A candidate could not be generated from the selected session.",
					})
				Expect(err).NotTo(HaveOccurred(), "source diagnostic %d", ordinal)
			}
			_, err = store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken,
				storage.GenerationDiagnosticRecord{
					ID: newID(), Stage: "candidate", Code: "context_candidate_failed",
					Message: "A context-derived candidate could not be generated.",
				})
			Expect(err).NotTo(HaveOccurred())
			for _, candidateID := range candidateIDs {
				_, err = store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken,
					storage.GenerationDiagnosticRecord{
						ID: newID(), CandidateID: candidateID, Stage: "evaluation", Code: "evaluation_unrankable",
						Message: "The candidate evaluation could not be ranked.",
					})
				Expect(err).NotTo(HaveOccurred())
			}
			_, err = store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken,
				storage.GenerationCandidateRecord{
					ID: newID(), Ordinal: 101, Kind: storage.GenerationCandidateContext,
					Snapshot: storage.GenerationCandidateSnapshot{Name: "Bounded extra", Type: "workflow", Content: "# Extra"},
					Insights: json.RawMessage(`[]`), BundleSHA256: "bounded-extra",
				})
			Expect(err).To(HaveOccurred(), "the write boundary must reject artifacts beyond the legal maximum")

			state, err := store.GetGenerationByID(ctx, creator, generation.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(state.Sessions).To(HaveLen(100))
			Expect(state.Candidates).To(HaveLen(101))
			Expect(state.Evaluations).To(HaveLen(101))
			Expect(state.Candidates[99].Ordinal).To(Equal(99))
			Expect(state.Candidates[99].SourceSessionIDs).To(Equal([]string{"session-100"}))
			Expect(state.Candidates[100].Ordinal).To(Equal(100))
			Expect(state.Candidates[100].Kind).To(Equal(storage.GenerationCandidateSynthesis))
			evaluationsByCandidate := make(map[string]storage.CandidateEvaluationRecord, len(state.Evaluations))
			for _, evaluation := range state.Evaluations {
				evaluationsByCandidate[evaluation.CandidateID] = evaluation
			}
			Expect(evaluationsByCandidate).To(HaveKey(candidateIDs[99]))
			Expect(evaluationsByCandidate[candidateIDs[99]].Score).To(HaveValue(Equal(0.99)))
			Expect(evaluationsByCandidate).To(HaveKey(candidateIDs[100]))
			Expect(evaluationsByCandidate[candidateIDs[100]].Score).To(HaveValue(Equal(0.98)))
			Expect(state.Diagnostics).To(HaveLen(storage.MaxGenerationDiagnostics))
			Expect(state.Diagnostics).To(ContainElement(HaveField("ID", synthesisMarker.ID)),
				"bounded reads must not hide the synthesis resume marker")
		})
	})
}
