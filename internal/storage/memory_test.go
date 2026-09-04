package storage_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/internal/storage/storagetest"
)

func seed(store *storage.MemoryStore, id string, updatedAt time.Time) storage.SkillRecord {
	rec := storage.SkillRecord{
		ID:          id,
		Slug:        id,
		Name:        "Skill " + id,
		Description: "about " + id,
		Type:        "workflow",
		Version:     "0.1.0",
		Visibility:  "private",
		Tags:        []string{"tag-" + id},
		Content:     "# " + id,
		CreatedAt:   updatedAt,
		UpdatedAt:   updatedAt,
	}
	saved, err := store.UpsertSkill(context.Background(), rec)
	Expect(err).NotTo(HaveOccurred())
	return *saved
}

var _ = storagetest.RevisionStoreContract("memory", func() storagetest.RevisionStoreFixture {
	store := storage.NewMemoryStore()
	return storagetest.RevisionStoreFixture{
		Store: store,
		MakePublic: func(ctx context.Context, revisionID, subject string, changedAt time.Time) error {
			return storage.MakeRevisionPublicForContract(ctx, store, revisionID, subject, changedAt)
		},
		Cleanup: store.Close,
	}
}, uuid.NewString)

var _ = storagetest.RevisionStoreContract("postgres", func() storagetest.RevisionStoreFixture {
	dsn := mandatoryPostgresTestDSN()
	if dsn == "" {
		Skip("TEST_POSTGRES_DSN / TAPES_TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	schema := "skills_revision_contract_" + uuid.NewString()[:8]
	store, err := storage.OpenPostgresStore(ctx, dsn, schema)
	Expect(err).NotTo(HaveOccurred())
	admin, err := pgxpool.New(ctx, dsn)
	Expect(err).NotTo(HaveOccurred())
	return storagetest.RevisionStoreFixture{
		Store: store,
		MakePublic: func(ctx context.Context, revisionID, subject string, changedAt time.Time) error {
			return storage.MakeRevisionPublicForContract(ctx, store, revisionID, subject, changedAt)
		},
		Cleanup: func() {
			store.Close()
			_ = storagetest.DropSchema(context.Background(), admin, schema)
			admin.Close()
		},
	}
}, uuid.NewString)

var _ = storagetest.LifecycleStoreContract("memory", func() (storagetest.LifecycleStore, func()) {
	store := storage.NewMemoryStore()
	return store, store.Close
}, uuid.NewString)

var _ = storagetest.LifecycleStoreContract("postgres", func() (storagetest.LifecycleStore, func()) {
	dsn := mandatoryPostgresTestDSN()
	if dsn == "" {
		Skip("TEST_POSTGRES_DSN / TAPES_TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	schema := "skills_lifecycle_" + uuid.NewString()[:8]
	store, err := storage.OpenPostgresStore(ctx, dsn, schema)
	Expect(err).NotTo(HaveOccurred())
	admin, err := pgxpool.New(ctx, dsn)
	Expect(err).NotTo(HaveOccurred())
	return store, func() {
		store.Close()
		_ = storagetest.DropSchema(context.Background(), admin, schema)
		admin.Close()
	}
}, uuid.NewString)

func mandatoryPostgresTestDSN() string {
	for _, name := range []string{"TEST_POSTGRES_DSN", "TAPES_TEST_POSTGRES_DSN", "TEST_DATABASE_URL"} {
		if dsn := os.Getenv(name); dsn != "" {
			return dsn
		}
	}
	return ""
}

var _ = Describe("MemoryStore", func() {
	ctx := context.Background()

	It("sorts bounded generation artifacts before applying limits", func() {
		clockNow := time.Date(2026, 9, 3, 8, 0, 0, 0, time.UTC)
		store := storage.NewMemoryStoreWithClock(func() time.Time { return clockNow })
		DeferCleanup(store.Close)
		creator := "bounded-artifact-creator"
		skill, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
			ID: uuid.NewString(), Slug: "bounded-artifacts", CreatorSubject: creator, CreatedAt: clockNow,
		})
		Expect(err).NotTo(HaveOccurred())
		sessions := make([]string, 100)
		for ordinal := range sessions {
			sessions[ordinal] = fmt.Sprintf("session-%03d", ordinal+1)
		}
		generation, err := store.CreateGeneration(ctx, storage.CreateGenerationInput{
			ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: creator,
			Snapshot: storage.SkillRevisionSnapshot{
				Name: "Bounded artifacts", Type: "workflow", Content: "# Bounded artifacts",
			},
			AuthorContext: "Retain every configured source.", SelectedSessionIDs: sessions,
			EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
			EvaluationCriteria: json.RawMessage(`[{"id":"rankable","weight":1}]`),
			CreatedAt:          clockNow,
		})
		Expect(err).NotTo(HaveOccurred())
		claim, err := store.ClaimGeneration(ctx, storage.ClaimGenerationInput{
			WorkerID: "bounded-artifact-worker", LeaseDuration: time.Hour,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(claim).NotTo(BeNil())

		candidateIDs := make([]string, 101)
		for ordinal := len(candidateIDs) - 1; ordinal >= 0; ordinal-- {
			clockNow = time.Date(2026, 9, 3, 8, 0, ordinal, 0, time.UTC)
			candidateIDs[ordinal] = uuid.NewString()
			kind := storage.GenerationCandidateSession
			sourceSessionIDs := []string{}
			if ordinal < len(sessions) {
				sourceSessionIDs = []string{sessions[ordinal]}
			} else if ordinal == len(sessions) {
				kind = storage.GenerationCandidateSynthesis
				sourceSessionIDs = append([]string(nil), sessions...)
			}
			_, err = store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, storage.GenerationCandidateRecord{
				ID: candidateIDs[ordinal], Ordinal: ordinal, Kind: kind,
				SourceSessionIDs: sourceSessionIDs,
				Snapshot: storage.GenerationCandidateSnapshot{
					Name: fmt.Sprintf("Candidate %03d", ordinal), Type: "workflow",
					Content: fmt.Sprintf("# Candidate %03d", ordinal),
				},
				Insights: json.RawMessage(`[]`), BundleSHA256: fmt.Sprintf("bundle-%03d", ordinal),
				CreatedAt: clockNow.Add(time.Duration(ordinal) * time.Second),
			})
			Expect(err).NotTo(HaveOccurred())
			score := float64(ordinal) / 101
			_, err = store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, storage.CandidateEvaluationRecord{
				ID: uuid.NewString(), CandidateID: candidateIDs[ordinal],
				RequestSHA256: fmt.Sprintf("request-%03d", ordinal), Profile: "generation-candidate-v1",
				ProfileVersion: "1", EvaluatorVersion: "test", Score: &score, Decision: "pass",
				CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`),
				Strengths: json.RawMessage(`[]`), Panel: json.RawMessage(`{}`),
				CreatedAt: clockNow.Add(time.Duration(ordinal) * time.Second),
			})
			Expect(err).NotTo(HaveOccurred())
		}
		diagnosticIDs := make([]string, 303)
		diagnosticFor := func(ordinal int) storage.GenerationDiagnosticRecord {
			record := storage.GenerationDiagnosticRecord{ID: uuid.NewString()}
			switch {
			case ordinal < 100:
				record.SessionID = sessions[ordinal]
				record.Stage, record.Code = "transcript", "transcript_unavailable"
				record.Message = "The selected session transcript could not be loaded."
			case ordinal < 200:
				record.SessionID = sessions[ordinal-100]
				record.Stage, record.Code = "candidate", "candidate_generation_failed"
				record.Message = "A candidate could not be generated from the selected session."
			case ordinal == 200:
				record.Stage, record.Code = "candidate", "context_candidate_failed"
				record.Message = "A context-derived candidate could not be generated."
			case ordinal < 302:
				record.CandidateID = candidateIDs[ordinal-201]
				record.Stage, record.Code = "evaluation", "evaluation_unrankable"
				record.Message = "The candidate evaluation could not be ranked."
			default:
				record.CandidateID = candidateIDs[100]
				record.Stage, record.Code = "synthesis", "synthesis_generation_failed"
				record.Message = "The optional synthesis candidate could not be generated."
			}
			return record
		}
		for ordinal := 302; ordinal >= 0; ordinal-- {
			clockNow = time.Date(2026, 9, 3, 8, 10, ordinal, 0, time.UTC)
			record := diagnosticFor(ordinal)
			diagnosticIDs[ordinal] = record.ID
			record.CreatedAt = clockNow.Add(time.Duration(ordinal) * time.Second)
			_, err = store.PutGenerationDiagnostic(ctx, generation.ID, claim.ClaimToken, record)
			Expect(err).NotTo(HaveOccurred())
		}

		state, err := store.GetGenerationByID(ctx, creator, generation.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Sessions).To(HaveLen(100))
		Expect(state.Sessions[0].Ordinal).To(Equal(0))
		Expect(state.Sessions[99].Ordinal).To(Equal(99))
		Expect(state.Candidates).To(HaveLen(101))
		Expect(state.Candidates[0].Ordinal).To(Equal(0))
		Expect(state.Candidates[99].SourceSessionIDs).To(Equal([]string{"session-100"}))
		Expect(state.Candidates[100].Ordinal).To(Equal(100))
		Expect(state.Candidates[100].Kind).To(Equal(storage.GenerationCandidateSynthesis))
		Expect(state.Evaluations).To(HaveLen(101))
		Expect(state.Evaluations[0].CandidateID).To(Equal(candidateIDs[0]))
		Expect(state.Evaluations[100].CandidateID).To(Equal(candidateIDs[100]))
		Expect(state.Diagnostics).To(HaveLen(303))
		Expect(state.Diagnostics[0].ID).To(Equal(diagnosticIDs[302]))
		Expect(state.Diagnostics).To(ContainElement(HaveField("ID", diagnosticIDs[0])))
	})

	It("preserves created_at, author, and downloads across upserts", func() {
		store := storage.NewMemoryStore()
		created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		_, err := store.UpsertSkill(ctx, storage.SkillRecord{
			ID: "s", Slug: "s", Name: "One", AuthorSubject: "user-a",
			CreatedAt: created, UpdatedAt: created,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(store.IncrementSkillDownloads(ctx, "s")).To(Succeed())

		later := created.Add(time.Hour)
		saved, err := store.UpsertSkill(ctx, storage.SkillRecord{
			ID: "s", Slug: "s", Name: "Renamed", AuthorSubject: "user-b",
			CreatedAt: later, UpdatedAt: later,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(saved.CreatedAt).To(Equal(created), "created_at is preserved on update")
		Expect(saved.AuthorSubject).To(Equal("user-a"), "the original creator stays authoritative")
		Expect(saved.DownloadCount).To(Equal(int64(1)), "downloads survive an update")
		Expect(saved.Name).To(Equal("Renamed"))
	})

	It("pages a tied updated_at set stably by id", func() {
		store := storage.NewMemoryStore()
		tied := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
		for i := range 5 {
			seed(store, fmt.Sprintf("skill-%d", i), tied)
		}

		var got []string
		opts := storage.SkillListOpts{Limit: 2}
		for {
			page, err := store.ListSkills(ctx, opts)
			Expect(err).NotTo(HaveOccurred())
			if len(page) == 0 {
				break
			}
			for _, rec := range page {
				got = append(got, rec.ID)
			}
			last := page[len(page)-1]
			ts := last.UpdatedAt
			opts.CursorTs = &ts
			opts.CursorID = last.ID
		}
		Expect(got).To(Equal([]string{"skill-4", "skill-3", "skill-2", "skill-1", "skill-0"}))
	})

	It("searches name, description, and tags case-insensitively", func() {
		store := storage.NewMemoryStore()
		now := time.Now().UTC()
		seed(store, "react-debug", now)
		seed(store, "sql-tuning", now.Add(time.Second))

		page, err := store.ListSkills(ctx, storage.SkillListOpts{Query: "REACT"})
		Expect(err).NotTo(HaveOccurred())
		Expect(page).To(HaveLen(1))
		Expect(page[0].ID).To(Equal("react-debug"))

		counts, err := store.CountSkills(ctx, storage.SkillCountOpts{Query: "tag-sql-tuning"})
		Expect(err).NotTo(HaveOccurred())
		Expect(counts.Total).To(Equal(int64(1)))
	})

	It("orders by downloads with its own keyset", func() {
		store := storage.NewMemoryStore()
		now := time.Now().UTC()
		seed(store, "a", now)
		seed(store, "b", now)
		for range 3 {
			Expect(store.IncrementSkillDownloads(ctx, "b")).To(Succeed())
		}

		page, err := store.ListSkills(ctx, storage.SkillListOpts{Sort: storage.SkillSortDownloads, Limit: 1})
		Expect(err).NotTo(HaveOccurred())
		Expect(page[0].ID).To(Equal("b"))

		dc := page[0].DownloadCount
		page, err = store.ListSkills(ctx, storage.SkillListOpts{
			Sort: storage.SkillSortDownloads, Limit: 1,
			CursorDownloads: &dc, CursorID: page[0].ID,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(page[0].ID).To(Equal("a"))
	})

	It("creates a skill with its initial version atomically", func() {
		store := storage.NewMemoryStore()
		now := time.Now().UTC()
		skill, err := store.CreatePublishedSkill(ctx, storage.SkillRecord{
			ID: "s", Slug: "s", Name: "Skill", CreatedAt: now, UpdatedAt: now,
		}, storage.SkillVersionRecord{
			SkillID: "s", VersionNumber: 1, Semver: "0.1.0", Content: "# Skill", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(skill.Content).To(Equal("# Skill"))
		versions, err := store.ListSkillVersions(ctx, "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(HaveLen(1))
		Expect(versions[0].Content).To(Equal("# Skill"))
	})

	It("refuses publication when the skill is missing", func() {
		store := storage.NewMemoryStore()
		_, err := store.PublishSkillVersion(ctx, storage.SkillVersionRecord{
			SkillID: "missing", VersionNumber: 1, Semver: "0.1.0", PublishedAt: time.Now().UTC(),
		})
		Expect(err).To(MatchError(storage.ErrSkillChanged))

		versions, err := store.ListSkillVersions(ctx, "missing")
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(BeEmpty())
	})

	It("refuses a duplicate version number with the typed conflict", func() {
		store := storage.NewMemoryStore()
		now := time.Now().UTC()
		seed(store, "s", now)
		_, err := store.PublishSkillVersion(ctx, storage.SkillVersionRecord{
			SkillID: "s", VersionNumber: 1, Semver: "0.1.0", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, storage.SkillVersionRecord{
			SkillID: "s", VersionNumber: 1, Semver: "0.1.0", PublishedAt: now,
		})
		Expect(err).To(MatchError(storage.ErrSkillVersionConflict))

		next, err := store.NextSkillVersionNumber(ctx, "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(next).To(Equal(2))
	})

	It("advances the head atomically and never lets an older publish regress it", func() {
		store := storage.NewMemoryStore()
		now := time.Now().UTC()
		seed(store, "s", now)

		// The newer publish lands first...
		_, err := store.PublishSkillVersion(ctx, storage.SkillVersionRecord{
			SkillID: "s", VersionNumber: 2, Semver: "0.1.1", Content: "# newer", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		head, err := store.GetSkill(ctx, "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(head.Version).To(Equal("0.1.1"))
		Expect(head.Content).To(Equal("# newer"))

		// ...then the older overlapping publish commits last: its history row
		// is kept but the head must not move backwards.
		_, err = store.PublishSkillVersion(ctx, storage.SkillVersionRecord{
			SkillID: "s", VersionNumber: 1, Semver: "0.1.0", Content: "# older", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		head, err = store.GetSkill(ctx, "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(head.Version).To(Equal("0.1.1"), "the head keeps the newer semver")
		Expect(head.Content).To(Equal("# newer"), "the head keeps the newer content")

		versions, err := store.ListSkillVersions(ctx, "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(HaveLen(2), "both snapshots survive in history")
	})

	It("deletes a skill together with its version history", func() {
		store := storage.NewMemoryStore()
		now := time.Now().UTC()
		seed(store, "s", now)
		_, err := store.PublishSkillVersion(ctx, storage.SkillVersionRecord{
			SkillID: "s", VersionNumber: 1, Semver: "0.1.0", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		deleted, err := store.DeleteSkill(ctx, "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeTrue())

		versions, err := store.ListSkillVersions(ctx, "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(versions).To(BeEmpty())

		deleted, err = store.DeleteSkill(ctx, "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(deleted).To(BeFalse(), "a second delete reports the id was already absent")
	})

	It("looks up skills by source session", func() {
		store := storage.NewMemoryStore()
		now := time.Now().UTC()
		rec := seed(store, "from-sess", now)
		rec.GeneratedFromSessionIDs = []string{"sess-1"}
		_, err := store.UpsertSkill(ctx, rec)
		Expect(err).NotTo(HaveOccurred())
		seed(store, "unrelated", now)

		found, err := store.ListSkillsBySession(ctx, "sess-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(HaveLen(1))
		Expect(found[0].ID).To(Equal("from-sess"))
	})
})
