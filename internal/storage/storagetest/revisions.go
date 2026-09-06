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
	storage.SkillReader
	storage.RevisionStore
	storage.RevisionMetadataStore
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

		It("visibility_updates_metadata_without_changing_content", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "immutable-visibility", "creator-a", now)
			revision := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a",
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Immutable", Description: "Visibility is separate metadata.", Type: "workflow",
					Tags: []string{"immutable", "metadata"}, Content: "# Never rewritten",
					IsAIGenerated: true, SourceSessionIDs: []string{"session-immutable"},
				},
				ChangeNote: "original content", IdempotencyKey: "immutable:1",
				CreatedAt: now.Add(5 * time.Hour),
			})
			before, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())

			blocked, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				IsPublic: true, ChangedAt: now.Add(6 * time.Hour),
			})
			Expect(blocked).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue(), "a private revision must remain creator-only even for metadata writes")

			publishedAt := now.Add(7 * time.Hour)
			published, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
				IsPublic: true, ChangedAt: publishedAt,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(published).To(Equal(&storage.RevisionVisibilityRecord{
				RevisionID: revision.ID, IsPublic: true,
				ChangedBySubject: "creator-a", ChangedAt: publishedAt,
				Changed: true, PreviousIsPublic: false,
			}))

			publicRetry, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				IsPublic: true, ChangedAt: now.Add(8 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred(), "any member may idempotently address metadata after the revision is public")
			Expect(publicRetry.Changed).To(BeFalse())
			Expect(publicRetry.PreviousIsPublic).To(BeTrue())
			Expect(persistedVisibility(publicRetry)).To(Equal(persistedVisibility(published)),
				"an idempotent retry must not rewrite visibility audit time or actor")

			publicRead, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(publicRead.Revision).To(Equal(before.Revision), "visibility must not alter immutable content, hash, sequence, version, provenance, or created_at")
			Expect(publicRead.Visibility).To(Equal(persistedVisibility(published)))

			privateAt := now.Add(9 * time.Hour)
			madePrivate, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				IsPublic: false, ChangedAt: privateAt,
			})
			Expect(err).NotTo(HaveOccurred(), "an organization member may mutate metadata while its target is public")
			Expect(madePrivate).To(Equal(&storage.RevisionVisibilityRecord{
				RevisionID: revision.ID, IsPublic: false,
				ChangedBySubject: "creator-b", ChangedAt: privateAt,
				Changed: true, PreviousIsPublic: true,
			}))

			mutatorRetry, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				IsPublic: false, ChangedAt: now.Add(10 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred(), "the actor that made public content private must be able to retry that exact PUT")
			Expect(mutatorRetry.Changed).To(BeFalse())
			Expect(mutatorRetry.PreviousIsPublic).To(BeFalse())
			Expect(persistedVisibility(mutatorRetry)).To(Equal(persistedVisibility(madePrivate)),
				"an audit-authorized retry must not rewrite visibility metadata")

			differentState, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				IsPublic: true, ChangedAt: now.Add(11 * time.Hour),
			})
			Expect(differentState).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue(), "visibility audit metadata must authorize only an exact-state retry")

			unrelatedRetry, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-c",
				IsPublic: false, ChangedAt: now.Add(12 * time.Hour),
			})
			Expect(unrelatedRetry).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue(), "unrelated callers must not probe a private revision through an idempotent-looking PUT")

			hidden, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
			})
			Expect(hidden).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue())
			owned, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(owned.Revision).To(Equal(before.Revision))
			Expect(owned.Visibility).To(Equal(persistedVisibility(madePrivate)))

			privateRetry, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
				IsPublic: false, ChangedAt: now.Add(13 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(privateRetry.Changed).To(BeFalse())
			Expect(persistedVisibility(privateRetry)).To(Equal(persistedVisibility(madePrivate)),
				"a creator retry must not rewrite unchanged private metadata")
		})

		It("stores_clear_latest_without_changing_revisions", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "clear-latest", "creator-a", now)
			first := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", Origin: storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "First", Type: "workflow", Content: "# First"},
				CreatedAt: now.Add(4 * time.Hour),
			})
			second := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", BasedOnRevisionID: first.ID,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Second", Type: "workflow", Content: "# Second"},
				CreatedAt: now.Add(3 * time.Hour),
			})
			for index, revisionID := range []string{first.ID, second.ID} {
				_, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
					SkillID: skill.ID, RevisionID: revisionID, CallerSubject: "creator-a",
					IsPublic: true, ChangedAt: now.Add(time.Duration(5+index) * time.Hour),
				})
				Expect(err).NotTo(HaveOccurred())
			}
			before, err := store.ListRevisions(ctx, storage.RevisionListOpts{
				SkillID: skill.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())

			selected, err := store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: first.ID, CallerSubject: "creator-b",
				ChangedAt: now.Add(7 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(selected.ExplicitLatestRevisionID).To(Equal(first.ID))
			Expect(selected.EffectiveRevision).NotTo(BeNil())
			Expect(selected.EffectiveRevision.Revision.ID).To(Equal(first.ID))
			Expect(selected.EffectiveRevision.IsExplicitLatest).To(BeTrue())

			cleared, err := store.ClearExplicitLatestRevision(ctx, storage.ClearExplicitLatestRevisionInput{
				SkillID: skill.ID, CallerSubject: "creator-c", ChangedAt: now.Add(8 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(cleared.SkillID).To(Equal(skill.ID))
			Expect(cleared.ExplicitLatestRevisionID).To(BeEmpty())
			Expect(cleared.EffectiveRevision).NotTo(BeNil())
			Expect(cleared.EffectiveRevision.Revision.ID).To(Equal(second.ID), "clear must resolve the greatest public sequence")
			Expect(cleared.EffectiveRevision.IsExplicitLatest).To(BeFalse(), "fallback is never marked explicitly latest")

			clearedAgain, err := store.ClearExplicitLatestRevision(ctx, storage.ClearExplicitLatestRevisionInput{
				SkillID: skill.ID, CallerSubject: "creator-d", ChangedAt: now.Add(9 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(clearedAgain).To(Equal(cleared))

			after, err := store.ListRevisions(ctx, storage.RevisionListOpts{
				SkillID: skill.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(after).To(HaveLen(len(before)))
			for index := range before {
				Expect(after[index].Revision).To(Equal(before[index].Revision), "latest changes must not alter immutable revision fields")
				Expect(after[index].Visibility).To(Equal(before[index].Visibility), "latest changes must not alter visibility audit metadata")
				Expect(after[index].IsExplicitLatest).To(BeFalse())
			}
		})

		It("stores_resolve_explicit_or_newest_public_revision", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "effective-resolution", "creator-a", now)
			first := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", Origin: storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "First public", Type: "workflow", Content: "# Sequence 1"},
				CreatedAt: now.Add(30 * time.Hour),
			})
			privateMiddle := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", BasedOnRevisionID: first.ID,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Private middle", Type: "workflow", Content: "# Sequence 2"},
				CreatedAt: now.Add(20 * time.Hour),
			})
			newestPublic := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", BasedOnRevisionID: privateMiddle.ID,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Newest public", Type: "workflow", Content: "# Sequence 3"},
				CreatedAt: now.Add(10 * time.Hour),
			})
			Expect(newestPublic.CreatedAt).To(BeTemporally("<", privateMiddle.CreatedAt))
			Expect(privateMiddle.CreatedAt).To(BeTemporally("<", first.CreatedAt))

			noPublic, err := store.ResolveEffectiveRevision(ctx, storage.EffectiveRevisionReadOpts{SkillID: skill.ID})
			Expect(err).NotTo(HaveOccurred())
			Expect(noPublic).To(BeNil(), "effective resolution must never substitute even the owner's newest private revision")

			for index, revisionID := range []string{first.ID, newestPublic.ID} {
				_, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
					SkillID: skill.ID, RevisionID: revisionID, CallerSubject: "creator-a",
					IsPublic: true, ChangedAt: now.Add(time.Duration(31+index) * time.Hour),
				})
				Expect(err).NotTo(HaveOccurred())
			}

			fallback, err := store.ResolveEffectiveRevision(ctx, storage.EffectiveRevisionReadOpts{SkillID: skill.ID})
			Expect(err).NotTo(HaveOccurred())
			Expect(fallback).NotTo(BeNil())
			Expect(fallback.Revision.ID).To(Equal(newestPublic.ID), "sequence, not anti-correlated created_at, chooses fallback")
			Expect(fallback.Revision.SequenceNumber).To(Equal(3))
			Expect(fallback.IsExplicitLatest).To(BeFalse())

			_, err = store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: first.ID, CallerSubject: "creator-b",
				ChangedAt: now.Add(40 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			explicit, err := store.ResolveEffectiveRevision(ctx, storage.EffectiveRevisionReadOpts{SkillID: skill.ID})
			Expect(err).NotTo(HaveOccurred())
			Expect(explicit).NotTo(BeNil())
			Expect(explicit.Revision.ID).To(Equal(first.ID))
			Expect(explicit.Revision.SequenceNumber).To(Equal(1))
			Expect(explicit.IsExplicitLatest).To(BeTrue())
		})

		It("skill_list_returns_one_effective_entry_per_skill", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "one-effective-card", "creator-a", now)
			public := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Public card", Type: "workflow", Content: "# Public",
					SourceSessionIDs: []string{"shared-session"},
				},
				CreatedAt: now.Add(3 * time.Hour),
			})
			_, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: public.ID, CallerSubject: "creator-a",
				IsPublic: true, ChangedAt: now.Add(4 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			privateA := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", BasedOnRevisionID: public.ID,
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Creator A private", Type: "workflow", Content: "# Private A",
					SourceSessionIDs: []string{"shared-session", "private-a-session"},
				},
				CreatedAt: now.Add(2 * time.Hour),
			})
			privateB := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-b", BasedOnRevisionID: public.ID,
				Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Creator B private", Type: "workflow", Content: "# Private B",
					SourceSessionIDs: []string{"shared-session", "private-b-session"},
				},
				CreatedAt: now.Add(time.Hour),
			})

			listed, err := store.ListEffectiveSkills(ctx, storage.EffectiveSkillListOpts{CallerSubject: "creator-a"})
			Expect(err).NotTo(HaveOccurred())
			Expect(listed).To(HaveLen(1), "three revisions of one identity must produce one card")
			Expect(listed[0].Skill.ID).To(Equal(skill.ID))
			Expect(listed[0].EffectiveRevision).NotTo(BeNil())
			Expect(listed[0].EffectiveRevision.Revision.ID).To(Equal(public.ID))
			Expect(listed[0].CardRevision).NotTo(BeNil())
			Expect(listed[0].CardRevision.Revision.ID).To(Equal(public.ID))
			Expect(listed[0].NewestPrivateRevision).NotTo(BeNil())
			Expect(listed[0].NewestPrivateRevision.Revision.ID).To(Equal(privateA.ID))
			Expect(listed[0].NewestPrivateRevision.Revision.ID).NotTo(Equal(privateB.ID))

			counts, err := store.CountEffectiveSkills(ctx, storage.EffectiveSkillCountOpts{
				SkillCountOpts: storage.SkillCountOpts{Author: "creator-a"}, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(counts).To(Equal(storage.SkillCounts{Total: 1, Mine: 1}), "counts are per skill identity, not per accessible revision")

			bySharedSession, err := store.ListEffectiveSkillsBySession(ctx, storage.EffectiveSkillSessionListOpts{
				SessionID: "shared-session", CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(bySharedSession).To(HaveLen(1), "multiple matching accessible revisions must collapse to one skill")
			Expect(bySharedSession[0].Skill.ID).To(Equal(skill.ID))

			leakedSession, err := store.ListEffectiveSkillsBySession(ctx, storage.EffectiveSkillSessionListOpts{
				SessionID: "private-b-session", CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(leakedSession).To(BeEmpty(), "another creator's private session provenance must not reveal the skill")

			publicViewer, err := store.ListEffectiveSkills(ctx, storage.EffectiveSkillListOpts{CallerSubject: "creator-c"})
			Expect(err).NotTo(HaveOccurred())
			Expect(publicViewer).To(HaveLen(1))
			Expect(publicViewer[0].EffectiveRevision.Revision.ID).To(Equal(public.ID))
			Expect(publicViewer[0].NewestPrivateRevision).To(BeNil())
			Expect(publicViewer[0].HasNewerPrivateRevision).To(BeFalse())

			privateOnlySkill := resolveRevisionSkill(ctx, store, newID, "private-only-card", "creator-a", now)
			privateOnly := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: privateOnlySkill.ID, CreatorSubject: "creator-a", Origin: storage.RevisionOriginManual,
				Snapshot: storage.SkillRevisionSnapshot{
					Name: "Owner private only", Type: "workflow", Content: "# Private only",
					Tags: []string{"private-only-marker"}, SourceSessionIDs: []string{"private-only-owner-session"},
				},
				CreatedAt: now.Add(5 * time.Hour),
			})

			ownerPrivateList, err := store.ListEffectiveSkills(ctx, storage.EffectiveSkillListOpts{
				SkillListOpts: storage.SkillListOpts{Query: "private-only-marker"}, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(ownerPrivateList).To(HaveLen(1))
			Expect(ownerPrivateList[0].Skill.ID).To(Equal(privateOnlySkill.ID))
			Expect(ownerPrivateList[0].EffectiveRevision).To(BeNil())
			Expect(ownerPrivateList[0].NewestPrivateRevision).NotTo(BeNil())
			Expect(ownerPrivateList[0].NewestPrivateRevision.Revision.ID).To(Equal(privateOnly.ID))
			Expect(ownerPrivateList[0].CardRevision).NotTo(BeNil())
			Expect(ownerPrivateList[0].CardRevision.Revision.ID).To(Equal(privateOnly.ID))

			ownerPrivateCounts, err := store.CountEffectiveSkills(ctx, storage.EffectiveSkillCountOpts{
				SkillCountOpts: storage.SkillCountOpts{Query: "private-only-marker", Author: "creator-a"},
				CallerSubject:  "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(ownerPrivateCounts).To(Equal(storage.SkillCounts{Total: 1, Mine: 1}))

			ownerPrivateSession, err := store.ListEffectiveSkillsBySession(ctx, storage.EffectiveSkillSessionListOpts{
				SessionID: "private-only-owner-session", CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(ownerPrivateSession).To(HaveLen(1))
			Expect(ownerPrivateSession[0].Skill.ID).To(Equal(privateOnlySkill.ID))
			Expect(ownerPrivateSession[0].CardRevision.Revision.ID).To(Equal(privateOnly.ID))

			hiddenPrivateList, err := store.ListEffectiveSkills(ctx, storage.EffectiveSkillListOpts{
				SkillListOpts: storage.SkillListOpts{Query: "private-only-marker"}, CallerSubject: "creator-c",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(hiddenPrivateList).To(BeEmpty(), "another viewer must not receive a private-only skill card")

			hiddenPrivateCounts, err := store.CountEffectiveSkills(ctx, storage.EffectiveSkillCountOpts{
				SkillCountOpts: storage.SkillCountOpts{Query: "private-only-marker", Author: "creator-c"},
				CallerSubject:  "creator-c",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(hiddenPrivateCounts).To(Equal(storage.SkillCounts{}), "another viewer must not infer a private-only skill from counts")

			hiddenPrivateSession, err := store.ListEffectiveSkillsBySession(ctx, storage.EffectiveSkillSessionListOpts{
				SessionID: "private-only-owner-session", CallerSubject: "creator-c",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(hiddenPrivateSession).To(BeEmpty(), "another viewer must not infer private-only session provenance")
		})

		It("effective_list_and_count_treat_search_metacharacters_literally", func() {
			searchSkills := []struct {
				slug     string
				snapshot storage.SkillRevisionSnapshot
			}{
				{
					slug: "literal-percent",
					snapshot: storage.SkillRevisionSnapshot{
						Name: "Literal%Marker", Type: "workflow", Content: "# Percent",
					},
				},
				{
					slug: "literal-underscore",
					snapshot: storage.SkillRevisionSnapshot{
						Name: "Underscore", Description: "Literal_Marker", Type: "workflow", Content: "# Underscore",
					},
				},
				{
					slug: "literal-escape",
					snapshot: storage.SkillRevisionSnapshot{
						Name: "Escape", Type: "workflow", Tags: []string{`literal\marker`}, Content: "# Escape",
					},
				},
				{
					slug: "wildcard-impostor",
					snapshot: storage.SkillRevisionSnapshot{
						Name: "LiteralXMarker", Type: "workflow", Content: "# Not a literal match",
					},
				},
			}

			ids := make([]string, 0, len(searchSkills))
			for index, searchSkill := range searchSkills {
				skill := resolveRevisionSkill(ctx, store, newID, searchSkill.slug, "creator-a", now)
				revision := appendRevision(ctx, store, storage.AppendRevisionInput{
					ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a",
					Origin: storage.RevisionOriginManual, Snapshot: searchSkill.snapshot,
					CreatedAt: now.Add(time.Duration(index+1) * time.Minute),
				})
				_, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
					SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
					IsPublic: true, ChangedAt: now.Add(time.Duration(index+10) * time.Minute),
				})
				Expect(err).NotTo(HaveOccurred())
				ids = append(ids, skill.ID)
			}

			for _, searchCase := range []struct {
				query      string
				expectedID string
			}{
				{query: "LITERAL%MARKER", expectedID: ids[0]},
				{query: "LITERAL_MARKER", expectedID: ids[1]},
				{query: `LITERAL\MARKER`, expectedID: ids[2]},
			} {
				listed, err := store.ListEffectiveSkills(ctx, storage.EffectiveSkillListOpts{
					SkillListOpts: storage.SkillListOpts{Query: searchCase.query},
					CallerSubject: "viewer",
				})
				Expect(err).NotTo(HaveOccurred(), "literal list query %q", searchCase.query)
				Expect(listed).To(HaveLen(1), "literal list query %q", searchCase.query)
				Expect(listed[0].Skill.ID).To(Equal(searchCase.expectedID))

				counts, err := store.CountEffectiveSkills(ctx, storage.EffectiveSkillCountOpts{
					SkillCountOpts: storage.SkillCountOpts{Query: searchCase.query, Author: "creator-a"},
					CallerSubject:  "viewer",
				})
				Expect(err).NotTo(HaveOccurred(), "literal count query %q", searchCase.query)
				Expect(counts).To(Equal(storage.SkillCounts{Total: 1, Mine: 1}))
			}
		})

		It("revision_history_returns_public_and_viewer_private_only", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "exact-history", "creator-a", now)
			publicA := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", Origin: storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Public A", Type: "workflow", Content: "# Public A"},
				CreatedAt: now.Add(4 * time.Hour),
			})
			_, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: publicA.ID, CallerSubject: "creator-a",
				IsPublic: true, ChangedAt: now.Add(5 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			privateA := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", BasedOnRevisionID: publicA.ID,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Private A", Type: "workflow", Content: "# Secret A"},
				CreatedAt: now.Add(3 * time.Hour),
			})
			privateB := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-b", BasedOnRevisionID: publicA.ID,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Private B", Type: "workflow", Content: "# Secret B"},
				CreatedAt: now.Add(2 * time.Hour),
			})
			publicB := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-b", BasedOnRevisionID: privateB.ID,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Public B", Type: "workflow", Content: "# Public B"},
				CreatedAt: now.Add(time.Hour),
			})
			_, err = store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: publicB.ID, CallerSubject: "creator-b",
				IsPublic: true, ChangedAt: now.Add(6 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			_, err = store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: publicA.ID, CallerSubject: "creator-c",
				ChangedAt: now.Add(7 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())

			assertExactHistory := func(caller string, expectedIDs []string, expectedLatestID string) {
				history, historyErr := store.ListRevisions(ctx, storage.RevisionListOpts{
					SkillID: skill.ID, CallerSubject: caller,
				})
				Expect(historyErr).NotTo(HaveOccurred())
				Expect(revisionIDs(history)).To(Equal(expectedIDs))
				for _, item := range history {
					Expect(item.IsExplicitLatest).To(Equal(item.Revision.ID == expectedLatestID), "only exact stored-pointer equality earns Latest")
				}
			}
			assertExactHistory("creator-a", []string{publicB.ID, privateA.ID, publicA.ID}, publicA.ID)
			assertExactHistory("creator-b", []string{publicB.ID, privateB.ID, publicA.ID}, publicA.ID)
			assertExactHistory("creator-c", []string{publicB.ID, publicA.ID}, publicA.ID)

			moved, err := store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: publicB.ID, CallerSubject: "creator-c",
				ChangedAt: now.Add(8 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(moved.ExplicitLatestRevisionID).To(Equal(publicB.ID))
			Expect(moved.EffectiveRevision).NotTo(BeNil())
			Expect(moved.EffectiveRevision.Revision.ID).To(Equal(publicB.ID))
			Expect(moved.EffectiveRevision.IsExplicitLatest).To(BeTrue())
			assertExactHistory("creator-a", []string{publicB.ID, privateA.ID, publicA.ID}, publicB.ID)
			assertExactHistory("creator-b", []string{publicB.ID, privateB.ID, publicA.ID}, publicB.ID)
			assertExactHistory("creator-c", []string{publicB.ID, publicA.ID}, publicB.ID)

			hidden, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: privateB.ID, CallerSubject: "creator-a",
			})
			Expect(hidden).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue())
		})

		It("skill_detail_identifies_newest_private_continuation", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "private-continuation", "creator-a", now)
			public := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", Origin: storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Public", Type: "workflow", Content: "# Public"},
				CreatedAt: now.Add(40 * time.Hour),
			})
			_, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: public.ID, CallerSubject: "creator-a",
				IsPublic: true, ChangedAt: now.Add(41 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			olderPrivateA := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", BasedOnRevisionID: public.ID,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Older private A", Type: "workflow", Content: "# Older A"},
				CreatedAt: now.Add(30 * time.Hour),
			})
			privateB := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-b", BasedOnRevisionID: public.ID,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Private B", Type: "workflow", Content: "# Secret B"},
				CreatedAt: now.Add(20 * time.Hour),
			})
			newestPrivateA := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", BasedOnRevisionID: olderPrivateA.ID,
				Origin:    storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Newest private A", Type: "workflow", Content: "# Newest A"},
				CreatedAt: now.Add(10 * time.Hour),
			})
			Expect(newestPrivateA.CreatedAt).To(BeTemporally("<", olderPrivateA.CreatedAt), "newest continuation must be sequence-based")

			detailA, err := store.GetEffectiveSkill(ctx, storage.EffectiveSkillReadOpts{
				SkillID: skill.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(detailA.EffectiveRevision).NotTo(BeNil())
			Expect(detailA.EffectiveRevision.Revision.ID).To(Equal(public.ID))
			Expect(detailA.NewestPrivateRevision).NotTo(BeNil())
			Expect(detailA.NewestPrivateRevision.Revision.ID).To(Equal(newestPrivateA.ID))
			Expect(detailA.NewestPrivateRevision.Revision.ID).NotTo(Equal(privateB.ID))
			Expect(detailA.CardRevision.Revision.ID).To(Equal(public.ID), "public default remains the card while private work is separate continuation metadata")
			Expect(detailA.HasNewerPrivateRevision).To(BeTrue())

			detailB, err := store.GetEffectiveSkill(ctx, storage.EffectiveSkillReadOpts{
				SkillID: skill.ID, CallerSubject: "creator-b",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(detailB.NewestPrivateRevision).NotTo(BeNil())
			Expect(detailB.NewestPrivateRevision.Revision.ID).To(Equal(privateB.ID))
			Expect(detailB.NewestPrivateRevision.Revision.ID).NotTo(Or(Equal(olderPrivateA.ID), Equal(newestPrivateA.ID)))

			detailC, err := store.GetEffectiveSkill(ctx, storage.EffectiveSkillReadOpts{
				SkillID: skill.ID, CallerSubject: "creator-c",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(detailC.NewestPrivateRevision).To(BeNil())
			Expect(detailC.HasNewerPrivateRevision).To(BeFalse())

			privateOnlySkill := resolveRevisionSkill(ctx, store, newID, "private-only-detail", "creator-a", now)
			privateOnly := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: privateOnlySkill.ID, CreatorSubject: "creator-a", Origin: storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Private only", Type: "workflow", Content: "# Private only"},
				CreatedAt: now.Add(50 * time.Hour),
			})
			ownerDetail, err := store.GetEffectiveSkill(ctx, storage.EffectiveSkillReadOpts{
				SkillID: privateOnlySkill.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(ownerDetail.EffectiveRevision).To(BeNil())
			Expect(ownerDetail.NewestPrivateRevision).NotTo(BeNil())
			Expect(ownerDetail.NewestPrivateRevision.Revision.ID).To(Equal(privateOnly.ID))
			Expect(ownerDetail.CardRevision.Revision.ID).To(Equal(privateOnly.ID))

			hiddenDetail, err := store.GetEffectiveSkill(ctx, storage.EffectiveSkillReadOpts{
				SkillID: privateOnlySkill.ID, CallerSubject: "creator-b",
			})
			Expect(hiddenDetail).To(BeNil())
			Expect(errors.Is(err, storage.ErrSkillNotFound)).To(BeTrue(), "a private-only stable identity must not disclose its existence")
		})

		It("revision_metadata_operations_are_independent_and_idempotent", func() {
			skill := resolveRevisionSkill(ctx, store, newID, "independent-metadata", "creator-a", now)
			revision := appendRevision(ctx, store, storage.AppendRevisionInput{
				ID: newID(), SkillID: skill.ID, CreatorSubject: "creator-a", Origin: storage.RevisionOriginManual,
				Snapshot:  storage.SkillRevisionSnapshot{Name: "Saved first", Type: "workflow", Content: "# Saved"},
				CreatedAt: now.Add(time.Hour),
			})
			original, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())

			latest, err := store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
				ChangedAt: now.Add(2 * time.Hour),
			})
			Expect(latest).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotPublic)).To(BeTrue())
			stillSaved, readErr := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
			})
			Expect(readErr).NotTo(HaveOccurred())
			Expect(stillSaved.Revision).To(Equal(original.Revision), "failed latest must not roll back the saved revision")
			Expect(stillSaved.Visibility).To(Equal(original.Visibility))

			blocked, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				IsPublic: true, ChangedAt: now.Add(3 * time.Hour),
			})
			Expect(blocked).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue())

			published, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
				IsPublic: true, ChangedAt: now.Add(4 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			publishedRetry, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				IsPublic: true, ChangedAt: now.Add(5 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(published.Changed).To(BeTrue())
			Expect(publishedRetry.Changed).To(BeFalse())
			Expect(persistedVisibility(publishedRetry)).To(Equal(persistedVisibility(published)))

			selected, err := store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				ChangedAt: now.Add(6 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(selected.ExplicitLatestRevisionID).To(Equal(revision.ID))
			selectedAgain, err := store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-c",
				ChangedAt: now.Add(7 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(selectedAgain).To(Equal(selected))

			rejectedPrivate, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-c",
				IsPublic: false, ChangedAt: now.Add(8 * time.Hour),
			})
			Expect(rejectedPrivate).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionIsExplicitLatest)).To(BeTrue())
			stillPublic, readErr := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-c",
			})
			Expect(readErr).NotTo(HaveOccurred())
			Expect(stillPublic.Visibility).To(Equal(persistedVisibility(published)))
			Expect(stillPublic.Revision).To(Equal(original.Revision))

			cleared, err := store.ClearExplicitLatestRevision(ctx, storage.ClearExplicitLatestRevisionInput{
				SkillID: skill.ID, CallerSubject: "creator-c", ChangedAt: now.Add(9 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(cleared.ExplicitLatestRevisionID).To(BeEmpty())
			clearedAgain, err := store.ClearExplicitLatestRevision(ctx, storage.ClearExplicitLatestRevisionInput{
				SkillID: skill.ID, CallerSubject: "creator-b", ChangedAt: now.Add(10 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(clearedAgain).To(Equal(cleared))

			madePrivate, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				IsPublic: false, ChangedAt: now.Add(11 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(madePrivate.IsPublic).To(BeFalse())
			privateRetry, err := store.SetRevisionVisibility(ctx, storage.SetRevisionVisibilityInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
				IsPublic: false, ChangedAt: now.Add(12 * time.Hour),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(madePrivate.Changed).To(BeTrue())
			Expect(madePrivate.PreviousIsPublic).To(BeTrue())
			Expect(privateRetry.Changed).To(BeFalse())
			Expect(persistedVisibility(privateRetry)).To(Equal(persistedVisibility(madePrivate)))

			latest, err = store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-b",
				ChangedAt: now.Add(13 * time.Hour),
			})
			Expect(latest).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotFound)).To(BeTrue(), "a noncreator must not learn that the target is now private")
			latest, err = store.SetExplicitLatestRevision(ctx, storage.SetExplicitLatestRevisionInput{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
				ChangedAt: now.Add(14 * time.Hour),
			})
			Expect(latest).To(BeNil())
			Expect(errors.Is(err, storage.ErrRevisionNotPublic)).To(BeTrue())

			final, err := store.GetRevision(ctx, storage.RevisionReadOpts{
				SkillID: skill.ID, RevisionID: revision.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(final.Revision).To(Equal(original.Revision))
			history, err := store.ListRevisions(ctx, storage.RevisionListOpts{
				SkillID: skill.ID, CallerSubject: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(history).To(HaveLen(1), "metadata retries and failures must never append or delete revision history")
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
