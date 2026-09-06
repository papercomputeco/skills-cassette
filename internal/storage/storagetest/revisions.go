package storagetest

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck // Ginkgo's DSL is intentionally dot-imported in test contracts.
	. "github.com/onsi/gomega"    //nolint:staticcheck // Gomega's matchers form the same test DSL.

	"github.com/papercomputeco/skills-cassette/internal/storage"
)

// RevisionLifecycleStore is the stable-identity and immutable-revision surface
// shared by the memory and Postgres drivers.
type RevisionLifecycleStore interface {
	storage.SkillIdentityStore
	storage.RevisionStore
}

// RevisionStoreFixture provides an isolated store plus the one setup hook the
// contract needs to create pre-existing public data. MakePublic is fixture
// setup, not the revision-metadata operation introduced by the next commit.
type RevisionStoreFixture struct {
	Store      RevisionLifecycleStore
	MakePublic func(context.Context, string, string, time.Time) error
	Cleanup    func()
}

// RevisionStoreFactory creates one isolated contract fixture.
type RevisionStoreFactory func() RevisionStoreFixture

// RevisionStoreContract registers the immutable append, access, lineage, and
// history behavior that must be identical in memory and real Postgres.
func RevisionStoreContract(name string, newStore RevisionStoreFactory, newID func() string) bool {
	return Describe("RevisionStore contract "+name, func() {
		var (
			ctx        context.Context
			fixture    RevisionStoreFixture
			store      RevisionLifecycleStore
			makePublic func(context.Context, string, string, time.Time) error
			now        time.Time
		)

		BeforeEach(func() {
			ctx = context.Background()
			fixture = newStore()
			store = fixture.Store
			makePublic = fixture.MakePublic
			now = time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
			Expect(store).NotTo(BeNil())
			Expect(makePublic).NotTo(BeNil())
		})

		AfterEach(func() {
			if fixture.Cleanup != nil {
				fixture.Cleanup()
			}
		})

		It("stores_append_from_any_accessible_revision", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "append-bases", "creator-a", now)
			baseInput := storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a",
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Original", Description: "The immutable base.", Type: "workflow",
					Tags: []string{"original"}, Content: "# Original",
					SourceSessionIDs: []string{"session-original"},
				},
				ChangeNote: "first save", CreatedAt: now,
			}
			base := appendRevision(ctx, store, baseInput)

			privateChild := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a",
				BasedOnRevisionID: base.ID, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Private child", Type: "workflow", Content: "# Private child",
				},
				CreatedAt: now.Add(time.Minute),
			})
			Expect(privateChild.BasedOnRevisionID).To(Equal(base.ID))
			Expect(privateChild.SequenceNumber).To(Equal(2))
			Expect(privateChild.Version).To(Equal("2"))

			baseInput.Snapshot.Content = "# Mutated caller buffer"
			baseInput.Snapshot.Tags[0] = "mutated"
			baseInput.Snapshot.SourceSessionIDs[0] = "mutated-session"
			original, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: base.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(original).NotTo(BeNil())
			Expect(original.Revision.Snapshot.Content).To(Equal("# Original"))
			Expect(original.Revision.Snapshot.Tags).To(Equal([]string{"original"}))
			Expect(original.Revision.Snapshot.SourceSessionIDs).To(Equal([]string{"session-original"}))
			Expect(original.Visibility.IsPublic).To(BeFalse())

			Expect(makePublic(ctx, base.ID, "creator-a", now.Add(2*time.Minute))).To(Succeed())
			publicChild := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-b",
				BasedOnRevisionID: base.ID, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Public-base child", Type: "workflow", Content: "# Public-base child",
				},
				CreatedAt: now.Add(3 * time.Minute),
			})
			Expect(publicChild.BasedOnRevisionID).To(Equal(base.ID))
			Expect(publicChild.CreatorSubject).To(Equal("creator-b"))
			Expect(publicChild.SequenceNumber).To(Equal(3))

			publicChildRead, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: publicChild.ID, CallerSubject: "creator-b",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(publicChildRead.Visibility.IsPublic).To(BeFalse(), "every descendant must still start private")

			foreignSkill := resolveRevisionSkill(ctx, store, newID, "foreign-base", "creator-a", now)
			foreignBase := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: foreignSkill.ID, CreatorSubject: "creator-a",
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Foreign base", Type: "workflow", Content: "# Foreign base",
				},
				CreatedAt: now.Add(4 * time.Minute),
			})
			crossSkillPrivate, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a",
				BasedOnRevisionID: foreignBase.ID, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Private foreign base", Type: "workflow", Content: "# Invalid",
				},
				CreatedAt: now.Add(5 * time.Minute),
			})
			Expect(crossSkillPrivate).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionLineageInvalid)).To(BeTrue(), "an owned private revision from another skill is not a valid base")

			Expect(makePublic(ctx, foreignBase.ID, "creator-a", now.Add(6*time.Minute))).To(Succeed())
			crossSkillPublic, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-b",
				BasedOnRevisionID: foreignBase.ID, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Public foreign base", Type: "workflow", Content: "# Invalid",
				},
				CreatedAt: now.Add(7 * time.Minute),
			})
			Expect(crossSkillPublic).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionLineageInvalid)).To(BeTrue(), "a public revision from another skill is not a valid base")

			notUUID, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a",
				BasedOnRevisionID: base.Version, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Version is not identity", Type: "workflow", Content: "# Invalid",
				},
				CreatedAt: now.Add(8 * time.Minute),
			})
			Expect(notUUID).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionLineageInvalid)).To(BeTrue(), "version labels must never be accepted as revision identity")
		})

		It("revision_reads_enforce_creator_or_public_visibility", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "visibility", "creator-a", now)
			private := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a",
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Private", Type: "workflow", Content: "# Creator A secret",
				},
				// Deliberately newer than later target-skill revisions: history must
				// follow sequence rather than caller-controlled timestamps.
				CreatedAt: now.Add(3 * time.Hour),
			})

			owned, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: private.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(owned.Revision.Snapshot.Content).To(Equal("# Creator A secret"))

			hidden, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: private.ID, CallerSubject: "creator-b",
			})
			Expect(hidden).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue())

			wrongSkill := resolveRevisionSkill(ctx, store, newID, "wrong-skill", "creator-a", now)
			hidden, err = store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: wrongSkill.ID, RevisionID: private.ID, CallerSubject: "creator-a",
			})
			Expect(hidden).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue(), "a UUID must remain scoped to the requested skill")

			rejected, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-b",
				BasedOnRevisionID: private.ID, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Inaccessible base", Type: "workflow", Content: "# Must not append",
				},
				CreatedAt: now.Add(time.Minute),
			})
			Expect(rejected).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionLineageInvalid)).To(BeTrue(), "private base validation must happen in storage")

			sourceSkill := resolveRevisionSkill(ctx, store, newID, "private-source", "creator-a", now)
			privateSource := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: sourceSkill.ID, CreatorSubject: "creator-a",
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Private source", Type: "workflow", Content: "# Private source",
				},
				CreatedAt: now.Add(2 * time.Minute),
			})
			rejected, err = store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-b",
				SourceRevisionID: privateSource.ID, Origin: storage.RevisionOriginDuplicate,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Inaccessible source", Type: "workflow", Content: "# Must not duplicate",
				},
				CreatedAt: now.Add(3 * time.Minute),
			})
			Expect(rejected).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionLineageInvalid)).To(BeTrue(), "private source validation must happen in storage")

			Expect(makePublic(ctx, private.ID, "creator-a", now.Add(4*time.Hour))).To(Succeed())
			creatorAPrivate := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a",
				BasedOnRevisionID: private.ID, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Creator A continuation", Type: "workflow", Content: "# A continuation",
				},
				CreatedAt: now.Add(2 * time.Hour),
			})
			creatorBPrivate := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-b",
				BasedOnRevisionID: private.ID, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Creator B continuation", Type: "workflow", Content: "# B continuation",
				},
				CreatedAt: now.Add(time.Hour),
			})
			Expect(creatorAPrivate.CreatedAt).To(BeTemporally("<", private.CreatedAt))
			Expect(creatorBPrivate.CreatedAt).To(BeTemporally("<", private.CreatedAt))

			assertRevisionHistory(ctx, store, skill.ID, "creator-a", []string{creatorAPrivate.ID, private.ID})
			assertRevisionHistory(ctx, store, skill.ID, "creator-b", []string{creatorBPrivate.ID, private.ID})
			assertRevisionHistory(ctx, store, skill.ID, "creator-c", []string{private.ID})
		})

		It("duplicate_appends_private_revision_with_source_uuid", func() {
			sourceSkill := resolveRevisionSkill(ctx, store, newID, "duplicate-source", "source-owner", now)
			source := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: sourceSkill.ID, CreatorSubject: "source-owner",
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Source", Type: "domain-knowledge", Tags: []string{"source"},
					Content: "# Source remains unchanged",
				},
				CreatedAt: now,
			})
			privateTargetSkill := resolveRevisionSkill(ctx, store, newID, "duplicate-private-target", "source-owner", now)
			privateDuplicate := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: privateTargetSkill.ID, CreatorSubject: "source-owner",
				SourceRevisionID: source.ID, Origin: storage.RevisionOriginDuplicate,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Own private duplicate", Type: "domain-knowledge", Content: "# Own private source is accessible",
				},
				CreatedAt: now.Add(time.Minute),
			})
			Expect(privateDuplicate.SourceRevisionID).To(Equal(source.ID))
			Expect(privateDuplicate.BasedOnRevisionID).To(BeEmpty())
			Expect(privateDuplicate.SkillID).To(Equal(privateTargetSkill.ID))
			Expect(privateDuplicate.SkillID).NotTo(Equal(source.SkillID))
			privateDuplicateRead, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: privateTargetSkill.ID, RevisionID: privateDuplicate.ID, CallerSubject: "source-owner",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(privateDuplicateRead.Visibility.IsPublic).To(BeFalse())

			targetSkill := resolveRevisionSkill(ctx, store, newID, "duplicate-target", "duplicator", now)

			rejected, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: newID(), SkillID: targetSkill.ID, CreatorSubject: "duplicator",
				SourceRevisionID: source.ID, Origin: storage.RevisionOriginDuplicate,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Hidden duplicate", Type: "domain-knowledge", Content: "# Must not append",
				},
				CreatedAt: now.Add(2 * time.Minute),
			})
			Expect(rejected).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionLineageInvalid)).To(BeTrue(), "another creator's private source must not be readable through append")

			Expect(makePublic(ctx, source.ID, "source-owner", now.Add(3*time.Minute))).To(Succeed())
			duplicateInput := storage.AppendRevisionInput{
				ID: newID(), SkillID: targetSkill.ID, CreatorSubject: "duplicator",
				SourceRevisionID: source.ID, Origin: storage.RevisionOriginDuplicate,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Duplicate", Description: "A separate skill history.", Type: "domain-knowledge",
					Tags: []string{"duplicate"}, Content: "# Duplicated and edited",
				},
				ChangeNote: "fork public source", CreatedAt: now.Add(4 * time.Minute),
			}
			duplicate := appendRevision(ctx, store, duplicateInput)
			Expect(duplicate.SkillID).To(Equal(targetSkill.ID))
			Expect(duplicate.SkillID).NotTo(Equal(source.SkillID))
			Expect(duplicate.SourceRevisionID).To(Equal(source.ID))
			Expect(duplicate.SourceRevisionID).NotTo(Equal(source.Version), "the shared version label is not lineage identity")
			Expect(duplicate.BasedOnRevisionID).To(BeEmpty(), "cross-skill lineage must not masquerade as a same-skill base")
			Expect(duplicate.Origin).To(Equal(storage.RevisionOriginDuplicate))
			Expect(duplicate.SequenceNumber).To(Equal(1), "a rejected private-source append must not consume a sequence")
			Expect(duplicate.Version).To(Equal("1"))
			Expect(source.Version).To(Equal(duplicate.Version), "equal presentation versions must not alias distinct UUID revisions")
			Expect(source.ID).NotTo(Equal(duplicate.ID))

			storedDuplicate, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: targetSkill.ID, RevisionID: duplicate.ID, CallerSubject: "duplicator",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(storedDuplicate.Visibility).To(Equal(storage.RevisionVisibilityRecord{
				RevisionID: duplicate.ID, IsPublic: false,
				ChangedBySubject: "duplicator", ChangedAt: duplicateInput.CreatedAt,
			}))
			hidden, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: targetSkill.ID, RevisionID: duplicate.ID, CallerSubject: "source-owner",
			})
			Expect(hidden).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue())

			storedSource, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: sourceSkill.ID, RevisionID: source.ID, CallerSubject: "source-owner",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(storedSource.Revision.Snapshot.Content).To(Equal("# Source remains unchanged"))
			Expect(storedSource.Revision.Snapshot.Tags).To(Equal([]string{"source"}))
			Expect(storedSource.Visibility.IsPublic).To(BeTrue())

			sameSkillSource, err := store.AppendRevision(ctx, storage.AppendRevisionInput{
				ID: newID(), SkillID: targetSkill.ID, CreatorSubject: "duplicator",
				SourceRevisionID: duplicate.ID, Origin: storage.RevisionOriginDuplicate,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Not cross-skill", Type: "domain-knowledge", Content: "# Invalid source",
				},
				CreatedAt: now.Add(5 * time.Minute),
			})
			Expect(sameSkillSource).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionLineageInvalid)).To(BeTrue(), "source UUID lineage is reserved for another skill")
		})

		It("memory_and_postgres_revision_lifecycle_contract_matches", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "parity", "creator", now)
			first := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator", Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "One", Type: "workflow", Tags: []string{"one"}, Content: "# One",
				},
				// Timestamps run opposite to sequence so only sequence-based history
				// ordering and pagination can satisfy the contract.
				CreatedAt: now.Add(3 * time.Hour),
			})
			second := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator",
				BasedOnRevisionID: first.ID, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Two", Type: "workflow", Tags: []string{"two"}, Content: "# Two",
				},
				CreatedAt: now.Add(2 * time.Hour),
			})
			third := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator",
				BasedOnRevisionID: second.ID, Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Three", Type: "workflow", Tags: []string{"three"}, Content: "# Three",
				},
				CreatedAt: now.Add(time.Hour),
			})
			Expect(third.CreatedAt).To(BeTemporally("<", second.CreatedAt))
			Expect(second.CreatedAt).To(BeTemporally("<", first.CreatedAt))
			Expect(makePublic(ctx, first.ID, "creator", now.Add(4*time.Hour))).To(Succeed())

			page, err := store.ListRevisions(ctx, storage.RevisionListOpts{
				SkillID: skill.ID, CallerSubject: "creator", Limit: 2,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(revisionIDs(page)).To(Equal([]string{third.ID, second.ID}))
			Expect(page[0].Revision.SequenceNumber).To(Equal(3))
			Expect(page[1].Revision.SequenceNumber).To(Equal(2))

			cursorSequence := page[len(page)-1].Revision.SequenceNumber
			nextPage, err := store.ListRevisions(ctx, storage.RevisionListOpts{
				SkillID: skill.ID, CallerSubject: "creator", Limit: 2,
				CursorSequenceNumber: &cursorSequence,
				CursorRevisionID:     page[len(page)-1].Revision.ID,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(revisionIDs(nextPage)).To(Equal([]string{first.ID}))
			Expect(nextPage[0].Visibility.IsPublic).To(BeTrue())

			publicOnly, err := store.ListRevisions(ctx, storage.RevisionListOpts{
				SkillID: skill.ID, CallerSubject: "another-creator",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(revisionIDs(publicOnly)).To(Equal([]string{first.ID}))

			page[0].Revision.Snapshot.Content = "# Mutated read"
			page[0].Revision.Snapshot.Tags[0] = "mutated-read"
			readAgain, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: third.ID, CallerSubject: "creator",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(readAgain.Revision.Snapshot.Content).To(Equal("# Three"))
			Expect(readAgain.Revision.Snapshot.Tags).To(Equal([]string{"three"}), "a returned memory value must not mutate persisted revision content")
		})

	})
}

func resolveRevisionSkill(
	ctx context.Context,
	store RevisionLifecycleStore,
	newID func() string,
	slug string,
	creator string,
	createdAt time.Time,
) *storage.SkillRecord {
	record, err := store.ResolveSkill(ctx, storage.ResolveSkillInput{
		ID: newID(), Slug: slug, CreatorSubject: creator, CreatedAt: createdAt,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(record).NotTo(BeNil())
	return record
}

func appendRevision(
	ctx context.Context,
	store RevisionLifecycleStore,
	input storage.AppendRevisionInput,
) *storage.SkillRevisionRecord {
	record, err := store.AppendRevision(ctx, input)
	Expect(err).NotTo(HaveOccurred())
	Expect(record).NotTo(BeNil())
	return record
}

func assertRevisionHistory(
	ctx context.Context,
	store RevisionLifecycleStore,
	skillID string,
	caller string,
	expectedIDs []string,
) {
	history, err := store.ListRevisions(ctx, storage.RevisionListOpts{
		SkillID: skillID, CallerSubject: caller,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(revisionIDs(history)).To(Equal(expectedIDs), fmt.Sprintf("accessible history for %s", caller))
}

func persistedVisibility(record *storage.RevisionVisibilityRecord) storage.RevisionVisibilityRecord {
	value := *record
	value.Changed = false
	value.PreviousIsPublic = false
	return value
}

func revisionIDs(revisions []storage.AccessibleRevisionRecord) []string {
	ids := make([]string, 0, len(revisions))
	for _, revision := range revisions {
		ids = append(ids, revision.Revision.ID)
	}
	return ids
}
