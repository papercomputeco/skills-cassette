package storage_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/storage"
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

var _ = Describe("MemoryStore", func() {
	ctx := context.Background()

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

		counts, err := store.CountSkills(ctx, "tag-sql-tuning", "")
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

	It("refuses a duplicate version number with the typed conflict", func() {
		store := storage.NewMemoryStore()
		now := time.Now().UTC()
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
