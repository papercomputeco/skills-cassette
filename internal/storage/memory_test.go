package storage_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/storage"
)

var _ = Describe("MemoryStore drafts", func() {
	var store *storage.MemoryStore
	var now time.Time

	BeforeEach(func() {
		store = storage.NewMemoryStore()
		now = time.Now().UTC()
	})

	create := func(id string, sources []string, ai bool) storage.SkillDraftRecord {
		draft, err := store.CreateDraft(context.Background(), storage.SkillRecord{
			ID: id, AuthorSubject: "author", Visibility: "private", CreatedAt: now, UpdatedAt: now,
		}, storage.SkillDraftRecord{
			SkillID: id, Slug: id, Name: "Draft " + id, Type: "workflow", Content: "# draft",
			GeneratedFromSessionIDs: sources, IsAIGenerated: ai, CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		return *draft
	}

	It("keeps an unpublished draft out of published reads", func() {
		create("draft", []string{"sess-1"}, true)
		published, err := store.GetSkill(context.Background(), "draft")
		Expect(err).NotTo(HaveOccurred())
		Expect(published).To(BeNil())
		drafts, err := store.ListDrafts(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(drafts).To(HaveLen(1))
	})

	It("conditionally updates revisions without changing server metadata", func() {
		draft := create("s", []string{"sess-1"}, true)
		draft.Content = "# revised"
		draft.GeneratedFromSessionIDs = []string{"tampered"}
		updated, err := store.UpdateDraft(context.Background(), draft, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(updated.Revision).To(Equal(2))
		Expect(updated.GeneratedFromSessionIDs).To(Equal([]string{"sess-1"}))
		_, err = store.UpdateDraft(context.Background(), draft, 1)
		Expect(err).To(MatchError(storage.ErrDraftRevisionConflict))
	})

	It("publishes a complete snapshot and consumes the draft atomically", func() {
		create("s", []string{"sess-1"}, true)
		version, err := store.PublishDraft(context.Background(), "s", 1, "first", "publisher", now.Add(time.Minute))
		Expect(err).NotTo(HaveOccurred())
		Expect(version.VersionNumber).To(Equal(1))
		Expect(version.GeneratedFromSessionIDs).To(Equal([]string{"sess-1"}))
		Expect(version.IsAIGenerated).To(BeTrue())
		draft, err := store.GetDraft(context.Background(), "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(draft).To(BeNil())
		published, err := store.GetSkill(context.Background(), "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(published.Content).To(Equal("# draft"))
		Expect(published.Visibility).To(Equal("team"))
	})

	It("supports a published skill and a later working draft simultaneously", func() {
		create("s", nil, false)
		_, err := store.PublishDraft(context.Background(), "s", 1, "", "", now)
		Expect(err).NotTo(HaveOccurred())
		draft, err := store.CreateDraftFromPublished(context.Background(), "s", now.Add(time.Minute))
		Expect(err).NotTo(HaveOccurred())
		draft.Content = "# unpublished edit"
		_, err = store.UpdateDraft(context.Background(), *draft, draft.Revision)
		Expect(err).NotTo(HaveOccurred())
		published, err := store.GetSkill(context.Background(), "s")
		Expect(err).NotTo(HaveOccurred())
		Expect(published.Content).To(Equal("# draft"))
	})

	It("reverse lookup uses only current published provenance", func() {
		create("s", []string{"sess-1"}, true)
		_, err := store.PublishDraft(context.Background(), "s", 1, "", "", now)
		Expect(err).NotTo(HaveOccurred())
		draft, err := store.CreateDraftFromPublished(context.Background(), "s", now)
		Expect(err).NotTo(HaveOccurred())
		draft.GeneratedFromSessionIDs = []string{"sess-2"}
		_, err = store.UpdateDraft(context.Background(), *draft, 1)
		Expect(err).NotTo(HaveOccurred())
		fromOne, err := store.ListSkillsBySession(context.Background(), "sess-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(fromOne).To(HaveLen(1))
		fromTwo, err := store.ListSkillsBySession(context.Background(), "sess-2")
		Expect(err).NotTo(HaveOccurred())
		Expect(fromTwo).To(BeEmpty())
	})
})
