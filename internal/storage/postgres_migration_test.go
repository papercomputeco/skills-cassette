package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Postgres snapshot migration", func() {
	It("postgres_migration_preserves_current_main_skills_versions_and_working_heads", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		schema := "skills_migration_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		pool, err := pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), pool, schema)
			pool.Close()
		})

		_, err = pool.Exec(ctx, fmt.Sprintf(`
			CREATE SCHEMA %s;
			CREATE TABLE %s.skills (
				id UUID PRIMARY KEY,
				slug TEXT NOT NULL,
				name TEXT NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				type TEXT NOT NULL DEFAULT 'workflow',
				version TEXT NOT NULL DEFAULT '0.1.0',
				visibility TEXT NOT NULL DEFAULT 'private',
				tags TEXT[] NOT NULL DEFAULT '{}',
				content TEXT NOT NULL DEFAULT '',
				is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE,
				generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}',
				parent_id UUID,
				author_subject TEXT NOT NULL DEFAULT '',
				download_count BIGINT NOT NULL DEFAULT 0,
				created_at TIMESTAMPTZ NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL
			);
			CREATE TABLE %s.skill_versions (
				skill_id UUID NOT NULL,
				version_number INT NOT NULL,
				semver TEXT NOT NULL,
				changelog TEXT NOT NULL DEFAULT '',
				content TEXT NOT NULL DEFAULT '',
				expected_content TEXT,
				author_subject TEXT NOT NULL DEFAULT '',
				published_at TIMESTAMPTZ NOT NULL,
				PRIMARY KEY (skill_id, version_number)
			);`, quotedSchema, quotedSchema, quotedSchema))
		Expect(err).NotTo(HaveOccurred())

		now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
		editedID := uuid.NewString()
		metadataOnlyID := uuid.NewString()
		unversionedID := uuid.NewString()
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.skills (
				id, slug, name, description, type, version, visibility, tags, content,
				is_ai_generated, generated_from_session_ids, author_subject, created_at, updated_at
			) VALUES
				($1, 'edited', 'Edited working head', 'draft metadata', 'workflow', '0.1.0', 'private', ARRAY['draft'], '# Edited after publish', TRUE, ARRAY['session-draft'], 'owner-1', $4, $4),
				($2, 'metadata-only', 'Metadata-only working head', 'unobservable divergence', 'domain-knowledge', '0.1.0', 'private', ARRAY['metadata'], '# Same content', TRUE, ARRAY['metadata-session'], 'owner-1', $4, $4 + INTERVAL '1 minute'),
				($3, 'new', 'Never versioned', 'published as recovered', 'workflow', '0.1.0', 'private', ARRAY['new'], '# Never versioned', FALSE, ARRAY[]::TEXT[], 'owner-1', $4, $4)`, quotedSchema), editedID, metadataOnlyID, unversionedID, now)
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.skill_versions (
				skill_id, version_number, semver, changelog, content, author_subject, published_at
			) VALUES
				($1, 1, '0.1.0', 'initial', '# Published content', 'owner-1', $3),
				($2, 1, '0.1.0', 'initial', '# Same content', 'owner-1', $3)`, quotedSchema), editedID, metadataOnlyID, now)
		Expect(err).NotTo(HaveOccurred())

		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(store.Close)

		var currentVersion int
		var publishedContent string
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT current_version_number, content FROM %s.skills WHERE id = $1`, quotedSchema), editedID).Scan(&currentVersion, &publishedContent)).To(Succeed())
		Expect(currentVersion).To(Equal(1))
		Expect(publishedContent).To(Equal("# Published content"), "the mutable working head must move into a draft, not remain publicly readable")

		var (
			versionName        string
			versionDescription string
			versionHash        string
		)
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT name, description, content_sha256
			FROM %s.skill_versions
			WHERE skill_id = $1 AND version_number = 1`, quotedSchema), editedID).Scan(&versionName, &versionDescription, &versionHash)).To(Succeed())
		Expect(versionName).To(Equal("Edited working head"))
		Expect(versionDescription).To(Equal("draft metadata"))
		Expect(versionHash).To(HaveLen(64))

		revisions, err := migratedRevisionRecords(ctx, store, editedID)
		Expect(err).NotTo(HaveOccurred())
		Expect(revisions).To(HaveLen(2))
		Expect(revisions[0].Snapshot.Content).To(Equal("# Published content"))
		Expect(revisions[0].IsPublic).To(BeTrue())
		Expect(revisions[1].Snapshot).To(Equal(SkillRevisionSnapshot{
			Name: "Edited working head", Description: "draft metadata", Type: "workflow",
			Tags: []string{"draft"}, Content: "# Edited after publish",
			IsAIGenerated: true, SourceSessionIDs: []string{"session-draft"},
		}))
		Expect(revisions[1].BasedOnRevisionID).To(Equal(revisions[0].ID))
		Expect(revisions[1].IsPublic).To(BeFalse())
		Expect(revisions[1].LegacyReference).To(HavePrefix("skill-working:" + editedID + ":"))
		Expect(revisions[1].ContentSHA256).To(HaveLen(64))

		metadataOnlyRevisions, err := migratedRevisionRecords(ctx, store, metadataOnlyID)
		Expect(err).NotTo(HaveOccurred())
		Expect(metadataOnlyRevisions).To(HaveLen(2), "content-only history cannot prove that mutable metadata matched a publication the head was written after")
		metadataOnlySnapshot := SkillRevisionSnapshot{
			Name: "Metadata-only working head", Description: "unobservable divergence",
			Type: "domain-knowledge", Tags: []string{"metadata"}, Content: "# Same content",
			IsAIGenerated: true, SourceSessionIDs: []string{"metadata-session"},
		}
		expectMigratedRevisionSnapshot(metadataOnlyRevisions[0], metadataOnlySnapshot)
		expectMigratedRevisionSnapshot(metadataOnlyRevisions[1], metadataOnlySnapshot)
		Expect(metadataOnlyRevisions[0].IsPublic).To(BeTrue())
		Expect(metadataOnlyRevisions[1].IsPublic).To(BeFalse())
		Expect(metadataOnlyRevisions[1].BasedOnRevisionID).To(Equal(metadataOnlyRevisions[0].ID))
		Expect(metadataOnlyRevisions[1].LegacyReference).To(HavePrefix("skill-working:" + metadataOnlyID + ":"))

		var recoveredVersions, recoveredDrafts int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_versions WHERE skill_id = $1`, quotedSchema), unversionedID).Scan(&recoveredVersions)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_drafts WHERE target_skill_id = $1`, quotedSchema), unversionedID).Scan(&recoveredDrafts)).To(Succeed())
		Expect(recoveredVersions).To(Equal(1))
		Expect(recoveredDrafts).To(BeZero())

		Expect(store.migrate(ctx)).To(Succeed())
		var draftCount, draftRevisionCount, immutableRevisionCount int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_drafts`, quotedSchema)).Scan(&draftCount)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.draft_revisions`, quotedSchema)).Scan(&draftRevisionCount)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions WHERE skill_id = $1`, quotedSchema), editedID).Scan(&immutableRevisionCount)).To(Succeed())
		Expect(draftCount).To(BeZero())
		Expect(draftRevisionCount).To(BeZero())
		Expect(immutableRevisionCount).To(Equal(2), "a rerun must reuse the content-addressed current-main source")
	})

	It("postgres_migration_keeps_unattributed_retained_heads_organization_visible", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		schema := "skills_migration_owner_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		pool, err := pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), pool, schema)
			pool.Close()
		})

		_, err = pool.Exec(ctx, fmt.Sprintf(`
			CREATE SCHEMA %[1]s;
			CREATE TABLE %[1]s.skills (
				id UUID PRIMARY KEY,
				slug TEXT NOT NULL,
				name TEXT NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				type TEXT NOT NULL DEFAULT 'workflow',
				version TEXT NOT NULL DEFAULT '0.1.0',
				visibility TEXT NOT NULL DEFAULT 'private',
				tags TEXT[] NOT NULL DEFAULT '{}',
				content TEXT NOT NULL DEFAULT '',
				is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE,
				generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}',
				parent_id UUID,
				author_subject TEXT NOT NULL DEFAULT '',
				download_count BIGINT NOT NULL DEFAULT 0,
				created_at TIMESTAMPTZ NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL
			);
			CREATE TABLE %[1]s.skill_versions (
				skill_id UUID NOT NULL,
				version_number INT NOT NULL,
				semver TEXT NOT NULL,
				changelog TEXT NOT NULL DEFAULT '',
				content TEXT NOT NULL DEFAULT '',
				expected_content TEXT,
				author_subject TEXT NOT NULL DEFAULT '',
				published_at TIMESTAMPTZ NOT NULL,
				PRIMARY KEY (skill_id, version_number)
			);`, quotedSchema))
		Expect(err).NotTo(HaveOccurred())

		now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
		unattributedID := uuid.NewString()
		attributedID := uuid.NewString()
		// Both heads were written after their publication; only one row remembers
		// who wrote it. Predecessor rows created before authorship existed carry
		// an empty author_subject.
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.skills (
				id, slug, name, description, type, version, visibility, tags, content,
				is_ai_generated, generated_from_session_ids, author_subject, created_at, updated_at
			) VALUES
				($1, 'unattributed', 'Unattributed head', 'edited by nobody in particular', 'workflow', '0.1.0', 'private', ARRAY['shared'], '# Edited without an author', FALSE, ARRAY[]::TEXT[], '', $3::timestamptz, $3::timestamptz + INTERVAL '1 minute'),
				($2, 'attributed', 'Attributed head', 'edited by its author', 'workflow', '0.1.0', 'private', ARRAY['owned'], '# Edited by owner', FALSE, ARRAY[]::TEXT[], 'owner-1', $3::timestamptz, $3::timestamptz + INTERVAL '1 minute')`, quotedSchema),
			unattributedID, attributedID, now)
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.skill_versions (
				skill_id, version_number, semver, changelog, content, author_subject, published_at
			) VALUES
				($1, 1, '0.1.0', 'initial', '# Published without an author', '', $3),
				($2, 1, '0.1.0', 'initial', '# Published by owner', 'owner-1', $3)`, quotedSchema),
			unattributedID, attributedID, now)
		Expect(err).NotTo(HaveOccurred())

		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(store.Close)

		assertOwnership := func() {
			unattributed, err := migratedRevisionRecords(ctx, store, unattributedID)
			Expect(err).NotTo(HaveOccurred())
			Expect(unattributed).To(HaveLen(2))
			Expect(unattributed[0].Snapshot.Content).To(Equal("# Published without an author"))
			Expect(unattributed[0].IsPublic).To(BeTrue())
			Expect(unattributed[1].Snapshot.Content).To(Equal("# Edited without an author"))
			Expect(unattributed[1].CreatorSubject).To(BeEmpty())
			Expect(unattributed[1].IsPublic).To(BeTrue(),
				"a retained head nobody owns stays organization-visible instead of becoming unreadable")
			Expect(unattributed[1].BasedOnRevisionID).To(Equal(unattributed[0].ID))
			Expect(unattributed[1].LegacyReference).To(HavePrefix("skill-working:" + unattributedID + ":"))

			attributed, err := migratedRevisionRecords(ctx, store, attributedID)
			Expect(err).NotTo(HaveOccurred())
			Expect(attributed).To(HaveLen(2))
			Expect(attributed[0].IsPublic).To(BeTrue())
			Expect(attributed[1].Snapshot.Content).To(Equal("# Edited by owner"))
			Expect(attributed[1].CreatorSubject).To(Equal("owner-1"))
			Expect(attributed[1].IsPublic).To(BeFalse(), "an attributed head stays private to its author")

			for skillID, revisions := range map[string][]migratedRevisionTestRecord{
				unattributedID: unattributed, attributedID: attributed,
			} {
				var latest string
				Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text
					FROM %s.skills WHERE id = $1`, quotedSchema), skillID).Scan(&latest)).To(Succeed())
				Expect(latest).To(Equal(revisions[0].ID), "latest stays on the published head")
			}

			var orphanedPrivate int
			Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*)
				FROM %s.skill_revisions revision
				JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
				WHERE NOT visibility.is_public AND revision.creator_subject = ''`, quotedSchema, quotedSchema)).
				Scan(&orphanedPrivate)).To(Succeed())
			Expect(orphanedPrivate).To(BeZero(), "no private revision may exist without a creator")
		}
		assertOwnership()

		// The unattributed head is readable through the ordinary organization
		// projection, and the attributed one only by its author.
		reader, err := store.GetRevision(ctx, RevisionReadOpts{SkillID: unattributedID, CallerSubject: "member-2",
			RevisionID: func() string {
				revisions, err := migratedRevisionRecords(ctx, store, unattributedID)
				Expect(err).NotTo(HaveOccurred())
				return revisions[1].ID
			}()})
		Expect(err).NotTo(HaveOccurred())
		Expect(reader).NotTo(BeNil())
		attributedRevisions, err := migratedRevisionRecords(ctx, store, attributedID)
		Expect(err).NotTo(HaveOccurred())
		hidden, err := store.GetRevision(ctx, RevisionReadOpts{
			SkillID: attributedID, RevisionID: attributedRevisions[1].ID, CallerSubject: "member-2",
		})
		Expect(err).To(MatchError(ErrRevisionNotFound))
		Expect(hidden).To(BeNil())

		Expect(store.migrate(ctx)).To(Succeed())
		assertOwnership()
	})

	It("postgres_migration_absorbs_published_initial_versions_and_drops_trigger", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		schema := "skills_migration_v060_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		pool, err := pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), pool, schema)
			pool.Close()
		})

		// The exact two-table shape a predecessor release left behind: content-only
		// version history, an AFTER INSERT trigger that publishes every new skill
		// as v0.1.0, and the startup backfill that published every head-only skill.
		_, err = pool.Exec(ctx, fmt.Sprintf(`
			CREATE SCHEMA %[1]s;
			CREATE TABLE %[1]s.skills (
				id UUID PRIMARY KEY,
				slug TEXT NOT NULL,
				name TEXT NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				type TEXT NOT NULL DEFAULT 'workflow',
				version TEXT NOT NULL DEFAULT '0.1.0',
				visibility TEXT NOT NULL DEFAULT 'private',
				tags TEXT[] NOT NULL DEFAULT '{}',
				content TEXT NOT NULL DEFAULT '',
				is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE,
				generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}',
				parent_id UUID,
				author_subject TEXT NOT NULL DEFAULT '',
				download_count BIGINT NOT NULL DEFAULT 0,
				created_at TIMESTAMPTZ NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL
			);
			CREATE INDEX skills_updated_idx ON %[1]s.skills (updated_at DESC, id DESC);
			CREATE TABLE %[1]s.skill_versions (
				skill_id UUID NOT NULL,
				version_number INT NOT NULL,
				semver TEXT NOT NULL,
				changelog TEXT NOT NULL DEFAULT '',
				content TEXT NOT NULL DEFAULT '',
				expected_content TEXT,
				author_subject TEXT NOT NULL DEFAULT '',
				published_at TIMESTAMPTZ NOT NULL,
				PRIMARY KEY (skill_id, version_number)
			);
			CREATE INDEX skill_versions_skill_idx ON %[1]s.skill_versions (skill_id, version_number DESC);`, quotedSchema))
		Expect(err).NotTo(HaveOccurred())

		now := time.Date(2026, 9, 5, 9, 0, 0, 0, time.UTC)
		backfilledID := uuid.NewString()
		insertSkill := fmt.Sprintf(`INSERT INTO %s.skills (
			id, slug, name, description, type, version, visibility, tags, content,
			is_ai_generated, generated_from_session_ids, author_subject, created_at, updated_at
		) VALUES ($1, $2, $3, $4, 'workflow', $5, 'private', $6, $7, FALSE, ARRAY[]::TEXT[], $8, $9, $9)`, quotedSchema)
		// A head-only skill that predates the trigger; the release backfilled it.
		_, err = pool.Exec(ctx, insertSkill, backfilledID, "backfilled", "Backfilled head",
			"published by the startup backfill", "0.1.0", []string{"legacy"}, "# Backfilled head", "owner-a", now)
		Expect(err).NotTo(HaveOccurred())

		_, err = pool.Exec(ctx, fmt.Sprintf(`
			CREATE OR REPLACE FUNCTION %[1]s.publish_initial_skill_version()
			RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				INSERT INTO %[1]s.skill_versions (
					skill_id, version_number, semver, changelog, content,
					author_subject, published_at
				) VALUES (NEW.id, 1, '0.1.0', '', NEW.content, NEW.author_subject, NEW.updated_at)
				ON CONFLICT DO NOTHING;
				RETURN NEW;
			END
			$$;
			CREATE TRIGGER publish_initial_skill_version
				AFTER INSERT ON %[1]s.skills
				FOR EACH ROW EXECUTE FUNCTION %[1]s.publish_initial_skill_version();
			INSERT INTO %[1]s.skill_versions (
				skill_id, version_number, semver, changelog, content, author_subject, published_at
			)
			SELECT id, 1, '0.1.0', '', content, author_subject, updated_at
			FROM %[1]s.skills skill
			WHERE NOT EXISTS (
				SELECT 1 FROM %[1]s.skill_versions version WHERE version.skill_id = skill.id
			)
			ON CONFLICT DO NOTHING;`, quotedSchema))
		Expect(err).NotTo(HaveOccurred())

		// Created after the release: the trigger publishes the initial version.
		createdID := uuid.NewString()
		_, err = pool.Exec(ctx, insertSkill, createdID, "created", "Created skill",
			"published atomically on insert", "0.1.0", []string{"initial"}, "# Created skill", "owner-b", now.Add(time.Minute))
		Expect(err).NotTo(HaveOccurred())

		// Created after the release and then published twice more through the
		// predecessor publish path, which advanced the head with each version.
		historyID := uuid.NewString()
		_, err = pool.Exec(ctx, insertSkill, historyID, "history", "History skill",
			"several published versions", "0.1.0", []string{"history"}, "# History v1", "owner-c", now.Add(2*time.Minute))
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_versions (
			skill_id, version_number, semver, changelog, content, author_subject, published_at
		) VALUES ($1, 2, '0.1.1', 'second', '# History v2', 'owner-c', $2),
		         ($1, 3, '0.1.2', 'third', '# History v3', 'owner-c', $3)`, quotedSchema),
			historyID, now.Add(3*time.Minute), now.Add(4*time.Minute))
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills
			SET version = '0.1.2', content = '# History v3', updated_at = $2 WHERE id = $1`, quotedSchema),
			historyID, now.Add(4*time.Minute))
		Expect(err).NotTo(HaveOccurred())

		// Created after the release and then edited in place without publishing.
		editedID := uuid.NewString()
		_, err = pool.Exec(ctx, insertSkill, editedID, "edited", "Edited skill",
			"head moved after the initial publication", "0.1.0", []string{"edited"}, "# Edited v1", "owner-d", now.Add(5*time.Minute))
		Expect(err).NotTo(HaveOccurred())
		_, err = pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills
			SET content = '# Edited after publish', updated_at = $2 WHERE id = $1`, quotedSchema),
			editedID, now.Add(6*time.Minute))
		Expect(err).NotTo(HaveOccurred())

		var versionRows int
		Expect(pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_versions`, quotedSchema)).Scan(&versionRows)).To(Succeed())
		Expect(versionRows).To(Equal(6), "the predecessor trigger and backfill published every skill at least once")

		triggerExists := func() bool {
			var exists bool
			Expect(pool.QueryRow(ctx, `SELECT EXISTS (
				SELECT 1 FROM pg_trigger trigger
				JOIN pg_class relation ON relation.oid = trigger.tgrelid
				JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
				WHERE namespace.nspname = $1 AND relation.relname = 'skills'
				  AND trigger.tgname = 'publish_initial_skill_version' AND NOT trigger.tgisinternal
			)`, schema).Scan(&exists)).To(Succeed())
			return exists
		}
		functionExists := func() bool {
			var exists bool
			Expect(pool.QueryRow(ctx, `SELECT EXISTS (
				SELECT 1 FROM pg_proc procedure
				JOIN pg_namespace namespace ON namespace.oid = procedure.pronamespace
				WHERE namespace.nspname = $1 AND procedure.proname = 'publish_initial_skill_version'
			)`, schema).Scan(&exists)).To(Succeed())
			return exists
		}
		Expect(triggerExists()).To(BeTrue())
		Expect(functionExists()).To(BeTrue())

		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(store.Close)

		Expect(triggerExists()).To(BeFalse(), "the predecessor insert trigger must not survive the unified migration")
		Expect(functionExists()).To(BeFalse(), "the predecessor trigger function must not survive the unified migration")

		type migratedSkillExpectation struct {
			id       string
			creator  string
			contents []string
			latest   int
		}
		expectations := []migratedSkillExpectation{
			{id: backfilledID, creator: "owner-a", contents: []string{"# Backfilled head"}, latest: 0},
			{id: createdID, creator: "owner-b", contents: []string{"# Created skill"}, latest: 0},
			{id: historyID, creator: "owner-c", contents: []string{"# History v1", "# History v2", "# History v3"}, latest: 2},
		}
		assertUnified := func() {
			for _, expectation := range expectations {
				revisions, err := migratedRevisionRecords(ctx, store, expectation.id)
				Expect(err).NotTo(HaveOccurred())
				Expect(revisions).To(HaveLen(len(expectation.contents)), expectation.id)
				for index, revision := range revisions {
					Expect(revision.SequenceNumber).To(Equal(index+1), expectation.id)
					Expect(revision.Version).To(Equal(fmt.Sprintf("%d", index+1)), expectation.id)
					Expect(revision.Snapshot.Content).To(Equal(expectation.contents[index]), expectation.id)
					Expect(revision.Origin).To(Equal(RevisionOriginMigrated))
					Expect(revision.CreatorSubject).To(Equal(expectation.creator))
					Expect(revision.LegacyReference).To(Equal(fmt.Sprintf("skill-version:%s:%d", expectation.id, index+1)))
					Expect(revision.IsPublic).To(BeTrue(), "every published version is a public revision")
					Expect(revision.ContentSHA256).To(Equal(canonicalRevisionDigestForTest(revision.Snapshot)))
					if index == 0 {
						Expect(revision.BasedOnRevisionID).To(BeEmpty())
					} else {
						Expect(revision.BasedOnRevisionID).To(Equal(revisions[index-1].ID))
					}
				}
				var latest string
				var nextSequence int
				Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text, next_sequence_number
					FROM %s.skills WHERE id = $1`, quotedSchema), expectation.id).Scan(&latest, &nextSequence)).To(Succeed())
				Expect(latest).To(Equal(revisions[expectation.latest].ID), "latest follows the predecessor head")
				Expect(nextSequence).To(Equal(len(expectation.contents) + 1))
			}

			editedRevisions, err := migratedRevisionRecords(ctx, store, editedID)
			Expect(err).NotTo(HaveOccurred())
			Expect(editedRevisions).To(HaveLen(2), "a head written after its publication is retained as private continuation")
			Expect(editedRevisions[0].Snapshot.Content).To(Equal("# Edited v1"))
			Expect(editedRevisions[0].IsPublic).To(BeTrue())
			Expect(editedRevisions[1].Snapshot.Content).To(Equal("# Edited after publish"))
			Expect(editedRevisions[1].IsPublic).To(BeFalse())
			Expect(editedRevisions[1].BasedOnRevisionID).To(Equal(editedRevisions[0].ID))
			Expect(editedRevisions[1].LegacyReference).To(HavePrefix("skill-working:" + editedID + ":"))
			var editedLatest string
			Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text
				FROM %s.skills WHERE id = $1`, quotedSchema), editedID).Scan(&editedLatest)).To(Succeed())
			Expect(editedLatest).To(Equal(editedRevisions[0].ID), "latest stays on the publication, never on unpublished work")

			var totalRevisions int
			Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions`, quotedSchema)).Scan(&totalRevisions)).To(Succeed())
			Expect(totalRevisions).To(Equal(7))
		}
		assertUnified()

		Expect(store.migrate(ctx)).To(Succeed(), "a rerun against the unified schema is a no-op")
		Expect(triggerExists()).To(BeFalse())
		Expect(functionExists()).To(BeFalse())
		assertUnified()

		// A skill resolved after cutover is an empty identity: nothing publishes it.
		resolved, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "resolved-after-cutover", CreatorSubject: "owner-e", CreatedAt: now.Add(time.Hour),
		})
		Expect(err).NotTo(HaveOccurred())
		var resolvedVersions int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_versions WHERE skill_id = $1`, quotedSchema), resolved.ID).Scan(&resolvedVersions)).To(Succeed())
		Expect(resolvedVersions).To(BeZero())
		Expect(store.migrate(ctx)).To(Succeed())
		resolvedRevisions, err := migratedRevisionRecords(ctx, store, resolved.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(resolvedRevisions).To(BeEmpty(), "an empty identity never becomes a publication on rerun")
	})

	It("postgres_migration_unifies_published_draft_and_working_history", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)

		By("migrating a current-main schema with published history and a distinct mutable head")
		mainSchema := "skills_main_history_" + uuid.NewString()[:8]
		quotedMainSchema := quoteIdentifier(mainSchema)
		mainPool, err := pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), mainPool, mainSchema)
			mainPool.Close()
		})
		_, err = mainPool.Exec(ctx, fmt.Sprintf(`
			CREATE SCHEMA %s;
			CREATE TABLE %s.skills (
				id UUID PRIMARY KEY,
				slug TEXT NOT NULL,
				name TEXT NOT NULL,
				description TEXT NOT NULL DEFAULT '',
				type TEXT NOT NULL DEFAULT 'workflow',
				version TEXT NOT NULL DEFAULT '0.1.0',
				visibility TEXT NOT NULL DEFAULT 'private',
				tags TEXT[] NOT NULL DEFAULT '{}',
				content TEXT NOT NULL DEFAULT '',
				is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE,
				generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}',
				parent_id UUID,
				author_subject TEXT NOT NULL DEFAULT '',
				download_count BIGINT NOT NULL DEFAULT 0,
				created_at TIMESTAMPTZ NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL
			);
			CREATE TABLE %s.skill_versions (
				skill_id UUID NOT NULL,
				version_number INT NOT NULL,
				semver TEXT NOT NULL,
				changelog TEXT NOT NULL DEFAULT '',
				content TEXT NOT NULL DEFAULT '',
				expected_content TEXT,
				author_subject TEXT NOT NULL DEFAULT '',
				published_at TIMESTAMPTZ NOT NULL,
				PRIMARY KEY (skill_id, version_number)
			);`, quotedMainSchema, quotedMainSchema, quotedMainSchema))
		Expect(err).NotTo(HaveOccurred())

		mainSkillID := uuid.NewString()
		unversionedSkillID := uuid.NewString()
		_, err = mainPool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.skills (
				id, slug, name, description, type, version, visibility, tags, content,
				is_ai_generated, generated_from_session_ids, author_subject, created_at, updated_at
			) VALUES
				($1, 'main-history', 'Main history', 'mutable metadata', 'workflow', '0.2.0', 'private', ARRAY['working'], '# Working head', TRUE, ARRAY['working-session'], 'main-owner', $3, $3),
				($2, 'main-unversioned', 'Unversioned', 'reachable head', 'domain-knowledge', '0.1.0', 'private', ARRAY['reachable'], '# Reachable head', FALSE, ARRAY[]::TEXT[], 'main-owner', $3, $3)`, quotedMainSchema),
			mainSkillID, unversionedSkillID, now)
		Expect(err).NotTo(HaveOccurred())
		_, err = mainPool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.skill_versions (
				skill_id, version_number, semver, changelog, content, author_subject, published_at
			) VALUES
				($1, 1, '0.1.0', 'first', '# Published one', 'main-owner', $2::timestamptz - INTERVAL '2 hours'),
				($1, 2, '0.2.0', 'second', '# Published two', 'main-owner', $2::timestamptz - INTERVAL '1 hour')`, quotedMainSchema),
			mainSkillID, now)
		Expect(err).NotTo(HaveOccurred())

		mainStore, err := OpenPostgresStore(ctx, dsn, mainSchema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(mainStore.Close)

		mainRevisions, err := migratedRevisionRecords(ctx, mainStore, mainSkillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(mainRevisions).To(HaveLen(3))
		publishedOne := SkillRevisionSnapshot{
			Name: "Main history", Description: "mutable metadata", Type: "workflow",
			Tags: []string{"working"}, Content: "# Published one", IsAIGenerated: true,
			SourceSessionIDs: []string{"working-session"},
		}
		expectMigratedRevisionSnapshot(mainRevisions[0], publishedOne)
		Expect(mainRevisions[0].SequenceNumber).To(Equal(1))
		Expect(mainRevisions[0].Version).To(Equal("1"))
		Expect(mainRevisions[0].CreatorSubject).To(Equal("main-owner"))
		Expect(mainRevisions[0].Origin).To(Equal(RevisionOriginMigrated))
		Expect(mainRevisions[0].BasedOnRevisionID).To(BeEmpty())
		Expect(mainRevisions[0].SourceRevisionID).To(BeEmpty())
		Expect(mainRevisions[0].ChangeNote).To(Equal("first"))
		Expect(mainRevisions[0].CreatedAt).To(BeTemporally("==", now.Add(-2*time.Hour)))
		Expect(mainRevisions[0].IsPublic).To(BeTrue())
		Expect(mainRevisions[0].ChangedBySubject).To(Equal("main-owner"))
		Expect(mainRevisions[0].ChangedAt).To(BeTemporally("==", mainRevisions[0].CreatedAt))

		publishedTwo := publishedOne
		publishedTwo.Content = "# Published two"
		expectMigratedRevisionSnapshot(mainRevisions[1], publishedTwo)
		Expect(mainRevisions[1].SequenceNumber).To(Equal(2))
		Expect(mainRevisions[1].Version).To(Equal("2"))
		Expect(mainRevisions[1].CreatorSubject).To(Equal("main-owner"))
		Expect(mainRevisions[1].Origin).To(Equal(RevisionOriginMigrated))
		Expect(mainRevisions[1].BasedOnRevisionID).To(Equal(mainRevisions[0].ID))
		Expect(mainRevisions[1].SourceRevisionID).To(BeEmpty())
		Expect(mainRevisions[1].ChangeNote).To(Equal("second"))
		Expect(mainRevisions[1].CreatedAt).To(BeTemporally("==", now.Add(-time.Hour)))
		Expect(mainRevisions[1].IsPublic).To(BeTrue())
		Expect(mainRevisions[1].ChangedBySubject).To(Equal("main-owner"))
		Expect(mainRevisions[1].ChangedAt).To(BeTemporally("==", mainRevisions[1].CreatedAt))

		workingHead := publishedTwo
		workingHead.Content = "# Working head"
		expectMigratedRevisionSnapshot(mainRevisions[2], workingHead)
		Expect(mainRevisions[2].SequenceNumber).To(Equal(3))
		Expect(mainRevisions[2].Version).To(Equal("3"))
		Expect(mainRevisions[2].CreatorSubject).To(Equal("main-owner"))
		Expect(mainRevisions[2].Origin).To(Equal(RevisionOriginMigrated))
		Expect(mainRevisions[2].BasedOnRevisionID).To(Equal(mainRevisions[1].ID))
		Expect(mainRevisions[2].SourceRevisionID).To(BeEmpty())
		Expect(mainRevisions[2].ChangeNote).To(BeEmpty())
		Expect(mainRevisions[2].CreatedAt).To(BeTemporally("==", now))
		Expect(mainRevisions[2].IsPublic).To(BeFalse())
		Expect(mainRevisions[2].ChangedBySubject).To(Equal("main-owner"))
		Expect(mainRevisions[2].ChangedAt).To(BeTemporally("==", mainRevisions[2].CreatedAt))
		for _, revision := range mainRevisions {
			Expect(revision.GenerationID).To(BeEmpty())
			Expect(revision.LegacyReference).NotTo(BeEmpty())
		}
		var mainLatest string
		var mainNextSequence int
		Expect(mainStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text, next_sequence_number FROM %s.skills WHERE id = $1`, quotedMainSchema), mainSkillID).Scan(&mainLatest, &mainNextSequence)).To(Succeed())
		Expect(mainLatest).To(Equal(mainRevisions[1].ID))
		Expect(mainNextSequence).To(Equal(4))

		unversionedRevisions, err := migratedRevisionRecords(ctx, mainStore, unversionedSkillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(unversionedRevisions).To(HaveLen(1))
		expectMigratedRevisionSnapshot(unversionedRevisions[0], SkillRevisionSnapshot{
			Name: "Unversioned", Description: "reachable head", Type: "domain-knowledge",
			Tags: []string{"reachable"}, Content: "# Reachable head",
			SourceSessionIDs: []string{},
		})
		Expect(unversionedRevisions[0].SequenceNumber).To(Equal(1))
		Expect(unversionedRevisions[0].Version).To(Equal("1"))
		Expect(unversionedRevisions[0].Origin).To(Equal(RevisionOriginMigrated))
		Expect(unversionedRevisions[0].IsPublic).To(BeTrue())
		Expect(unversionedRevisions[0].CreatorSubject).To(Equal("main-owner"))
		Expect(unversionedRevisions[0].ChangeNote).To(Equal("Recovered during cassette migration"))
		Expect(unversionedRevisions[0].GenerationID).To(BeEmpty())
		Expect(unversionedRevisions[0].LegacyReference).NotTo(BeEmpty())
		Expect(unversionedRevisions[0].CreatedAt).To(BeTemporally("==", now))
		var unversionedLatest string
		var unversionedNextSequence int
		Expect(mainStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text, next_sequence_number FROM %s.skills WHERE id = $1`, quotedMainSchema), unversionedSkillID).Scan(&unversionedLatest, &unversionedNextSequence)).To(Succeed())
		Expect(unversionedLatest).To(Equal(unversionedRevisions[0].ID))
		Expect(unversionedNextSequence).To(Equal(2))

		Expect(mainStore.migrate(ctx)).To(Succeed())
		mainRevisionsAfterRetry, err := migratedRevisionRecords(ctx, mainStore, mainSkillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(mainRevisionsAfterRetry).To(Equal(mainRevisions), "rerunning a named migration must retain deterministic revision mappings")

		By("migrating the implemented predecessor schema with draft and generation lineage")
		predecessorSchema := "skills_predecessor_history_" + uuid.NewString()[:8]
		quotedPredecessorSchema := quoteIdentifier(predecessorSchema)
		predecessorStore, err := OpenPostgresStore(ctx, dsn, predecessorSchema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), predecessorStore.pool, predecessorSchema)
			predecessorStore.Close()
		})
		Expect(removeDurableRevisionTargetForFixture(ctx, predecessorStore)).To(Succeed())

		var targetRelationCount, targetColumnCount int
		Expect(predecessorStore.pool.QueryRow(ctx, `
			SELECT count(*) FROM information_schema.tables
			WHERE table_schema = $1 AND table_name IN (
			  'skill_revisions', 'skill_revision_visibility',
			  'skill_revision_migration_sources'
			)`, predecessorSchema).Scan(&targetRelationCount)).To(Succeed())
		Expect(predecessorStore.pool.QueryRow(ctx, `
			SELECT count(*) FROM information_schema.columns
			WHERE table_schema = $1 AND (
			  (table_name = 'skills' AND column_name IN (
			    'explicit_latest_revision_id', 'next_sequence_number', 'created_by_subject',
			    'migration_alias_of_skill_id', 'migration_managed_latest_revision_id'
			  ))
			  OR (table_name = 'skill_generations' AND column_name IN (
			    'skill_id', 'creator_subject', 'base_revision_id', 'result_candidate_id', 'result_revision_id'
			  ))
			)`, predecessorSchema).Scan(&targetColumnCount)).To(Succeed())
		Expect(targetRelationCount).To(BeZero(), "the fixture must not seed through already-migrated revision tables")
		Expect(targetColumnCount).To(BeZero(), "the fixture must have the physical predecessor column shape")

		predecessorSkillID := uuid.NewString()
		_, err = predecessorStore.UpsertSkill(ctx, SkillRecord{
			ID: predecessorSkillID, Slug: "predecessor-history", Name: "Published predecessor",
			Description: "published metadata", Type: "workflow", Version: "0.1.0", Visibility: "private",
			Tags: []string{"published", "preserved"}, Content: "# Published predecessor",
			IsAIGenerated: false, GeneratedFromSessionIDs: []string{"published-session"},
			AuthorSubject: "predecessor-owner", CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = predecessorStore.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: predecessorSkillID, VersionNumber: 1, Semver: "0.1.0", Changelog: "published",
			Content: "# Published predecessor", AuthorSubject: "predecessor-owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		baseVersion := 1
		draftID := uuid.NewString()
		Expect(seedPredecessorDraft(ctx, predecessorStore, predecessorDraftFixture{
			id: draftID, targetSkillID: predecessorSkillID, ownerSubject: "predecessor-owner",
			baseSkillVersionNumber: &baseVersion,
			working: predecessorSkillSnapshot{
				Slug: "predecessor-history", Name: "Private checkpoint", Description: "private metadata",
				Type: "workflow", Tags: []string{"private", "checkpoint"}, Content: "# Private checkpoint",
				IsAIGenerated: true, SourceSessionIDs: []string{"draft-session"},
			},
			authorContext: "Improve the private revision.", selectedSessionIDs: []string{"generation-session"},
			createdAt: now.Add(time.Minute),
		})).To(Succeed())

		generationID := uuid.NewString()
		candidateID := uuid.NewString()
		evaluationID := uuid.NewString()
		candidateInsights := []byte(`[{"kind":"workflow","summary":"Preserve bounded candidate insight.","evidence":"Retained migration source."}]`)
		criterionResults := []byte(`[{"criterion_id":"complete","weight":1,"passed":true,"rationale":"Complete."}]`)
		findings := []byte(`[{"rule_id":"tighten-example","severity":"warning","message":"Tighten the example."}]`)
		strengths := []byte(`["The workflow is bounded."]`)
		panel := []byte(`{"summary":"Pass with one warning.","judgeCount":1}`)
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.skill_generations (
				id, draft_id, owner_subject, status, starting_lock_version,
				input_slug, input_name, input_description, input_type, input_tags,
				input_content, input_is_ai_generated, input_source_session_ids,
				author_context, selected_session_ids, evaluator_profile,
				evaluator_profile_version, evaluation_criteria, error_code, error_message,
				claim_owner, attempt_count, next_attempt_at, created_at, updated_at,
				started_at, completed_at
			) VALUES (
				$1, $2, 'predecessor-owner', 'completed', 1,
				'predecessor-history', 'Private checkpoint', 'private metadata', 'workflow', ARRAY['private', 'checkpoint'],
				'# Private checkpoint', TRUE, ARRAY['draft-session'],
				'Improve the private revision.', ARRAY['generation-session'], 'generation-candidate-v1',
				'1', $3::jsonb, '', '', '', 1, $4, $4, $5, $4, $5
			);
			INSERT INTO %s.generation_candidates (
				id, generation_id, ordinal, kind, source_session_ids, slug, name,
				description, type, tags, content, is_ai_generated, insights,
				bundle_sha256, created_at
			) VALUES (
				$6, $1, 0, 'session', ARRAY['generation-session'], 'predecessor-history', 'Generated result',
				'generated metadata', 'workflow', ARRAY['generated', 'winner'], '# Generated result', TRUE,
				$7::jsonb, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', $8
			);
			INSERT INTO %s.generation_sessions (
				generation_id, session_id, ordinal, status, candidate_id, updated_at
			) VALUES ($1, 'generation-session', 0, 'evaluated', $6, $9);
			INSERT INTO %s.candidate_evaluations (
				id, generation_id, candidate_id, request_sha256, profile, profile_version,
				evaluator_version, score, decision, critical_finding_count, warning_finding_count,
				criterion_results, findings, strengths, panel, created_at
			) VALUES (
				$10, $1, $6, 'migration-evaluation-request-sha256', 'generation-candidate-v1', '1',
				'fixture-v2', 0.9, 'pass', 0, 1, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb, $9
			);
			INSERT INTO %s.draft_revisions (
				draft_id, revision_number, origin, idempotency_key, generation_id,
				slug, name, description, type, tags, content, is_ai_generated,
				source_session_ids, content_sha256, created_at
			) VALUES (
				$2, 2, 'generation', 'generation:' || $1::text, $1,
				'predecessor-history', 'Generated result', 'generated metadata', 'workflow',
				ARRAY['generated', 'winner'], '# Generated result', TRUE,
				ARRAY['generation-session'], $15, $5
			);
			UPDATE %s.skill_generations SET
				winner_candidate_id = $6, proposed_candidate_id = $6,
				proposed_revision_number = 2, resolution = 'accept_generated'
			WHERE id = $1`, quotedPredecessorSchema, quotedPredecessorSchema,
			quotedPredecessorSchema, quotedPredecessorSchema, quotedPredecessorSchema,
			quotedPredecessorSchema),
			pgx.QueryExecModeSimpleProtocol, generationID, draftID,
			`[{"id":"complete","kind":"structure","description":"Complete.","weight":1}]`,
			now.Add(2*time.Minute), now.Add(9*time.Minute), candidateID, string(candidateInsights),
			now.Add(7*time.Minute), now.Add(8*time.Minute), evaluationID,
			string(criterionResults), string(findings), string(strengths), string(panel), strings.Repeat("a", 64))
		Expect(err).NotTo(HaveOccurred())

		// The predecessor stored the final synthesis in winner_candidate_id.
		// Retain it as result_candidate_id while reconstructing the deterministic
		// non-synthesis winner from the persisted ranking evidence.
		synthesisCandidateID := uuid.NewString()
		synthesisEvaluationID := uuid.NewString()
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.generation_candidates (
				id, generation_id, ordinal, kind, source_session_ids, slug, name,
				description, type, tags, content, is_ai_generated, insights,
				bundle_sha256, created_at
			) VALUES (
				$2, $1, 1, 'synthesis', ARRAY['generation-session'], 'predecessor-history',
				'Synthesized result', 'synthesized metadata', 'workflow',
				ARRAY['generated', 'synthesis'], '# Synthesized result', TRUE,
				'[{"kind":"legacy","opaque":"unsafe-result-detail"}]'::jsonb,
				'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', $4
			);
			INSERT INTO %s.candidate_evaluations (
				id, generation_id, candidate_id, request_sha256, profile, profile_version,
				evaluator_version, score, decision, critical_finding_count,
				warning_finding_count, criterion_results, findings, strengths, panel, created_at
			) VALUES (
				$3, $1, $2, 'migration-synthesis-evaluation', 'generation-candidate-v1', '1',
				'fixture-v2', 0.95, 'pass', 0, 0, '[]'::jsonb, '[]'::jsonb,
				'["synthesized"]'::jsonb, '{"judgeCount":1}'::jsonb, $4
			);
			UPDATE %s.draft_revisions SET
				name = 'Synthesized result', description = 'synthesized metadata',
				tags = ARRAY['generated', 'synthesis'], content = '# Synthesized result'
			WHERE generation_id = $1;
			UPDATE %s.skill_generations SET
				winner_candidate_id = $2, proposed_candidate_id = $2
			WHERE id = $1`, quotedPredecessorSchema, quotedPredecessorSchema,
			quotedPredecessorSchema, quotedPredecessorSchema),
			pgx.QueryExecModeSimpleProtocol, generationID, synthesisCandidateID,
			synthesisEvaluationID, now.Add(8*time.Minute+time.Second))
		Expect(err).NotTo(HaveOccurred())

		activeGenerationID := uuid.NewString()
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.skill_generations (
				id, draft_id, owner_subject, status, starting_lock_version,
				input_slug, input_name, input_description, input_type, input_tags,
				input_content, input_is_ai_generated, input_source_session_ids,
				author_context, selected_session_ids, evaluator_profile,
				evaluator_profile_version, evaluation_criteria, error_code, error_message,
				claim_owner, attempt_count, next_attempt_at, created_at, updated_at
			) VALUES (
				$1, $2, 'predecessor-owner', 'queued', 1,
				'predecessor-history', 'Private checkpoint', 'private metadata', 'workflow', ARRAY['private', 'checkpoint'],
				'# Private checkpoint', TRUE, ARRAY['draft-session'],
				'Continue the retained work.', ARRAY['active-session'], 'generation-candidate-v1',
				'1', $3::jsonb, '', '', '', 0, $4, $4, $4
			)`, quotedPredecessorSchema), activeGenerationID, draftID,
			`[{"id":"complete","kind":"structure","description":"Complete.","weight":1}]`,
			now.Add(10*time.Minute))
		Expect(err).NotTo(HaveOccurred())
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.generation_sessions (
			generation_id, session_id, ordinal, status, updated_at
		) VALUES ($1, 'active-session', 0, 'pending', $2)`, quotedPredecessorSchema),
			activeGenerationID, now.Add(10*time.Minute))
		Expect(err).NotTo(HaveOccurred())

		By("seeding malformed predecessor relationships and unsafe modern artifacts with test-only SQL")
		unsafeCandidateID := uuid.NewString()
		unsafeEvaluationID := uuid.NewString()
		unsafeSynthesisDiagnosticID := uuid.NewString()
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.generation_candidates (
				id, generation_id, ordinal, kind, source_session_ids, slug, name,
				description, type, tags, content, is_ai_generated, insights,
				bundle_sha256, created_at
			) VALUES (
				$2, $1, 0, 'session', ARRAY['active-session'], 'unsafe', 'Unsafe',
				'unsafe artifact', 'workflow', ARRAY[]::TEXT[], '# Unsafe', TRUE,
				'[{"kind":"legacy","summary":"missing evidence","provider":"secret"}]'::jsonb,
				'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc', $5
			);
			INSERT INTO %s.candidate_evaluations (
				id, generation_id, candidate_id, request_sha256, profile,
				profile_version, evaluator_version, score, decision, criterion_results,
				findings, strengths, panel, created_at
			) VALUES (
				$3, $1, $2, 'unsafe-request', 'generation-candidate-v1', '1',
				'unsafe', 0.5, 'pass', '[]'::jsonb, '[]'::jsonb, '[]'::jsonb,
				'{"provider":"secret"}'::jsonb, $5
			);
			INSERT INTO %s.generation_diagnostics (
				id, generation_id, candidate_id, stage, code, message, created_at
			) VALUES (
				$4, $1, $2, 'synthesis', 'legacy_resume_marker',
				'legacy diagnostics must not suppress synthesis', $5
			)`, quotedPredecessorSchema, quotedPredecessorSchema, quotedPredecessorSchema),
			pgx.QueryExecModeSimpleProtocol, activeGenerationID, unsafeCandidateID,
			unsafeEvaluationID, unsafeSynthesisDiagnosticID, now.Add(11*time.Minute))
		Expect(err).NotTo(HaveOccurred())

		malformedEvaluationID := uuid.NewString()
		malformedDiagnosticID := uuid.NewString()
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`
			UPDATE %s.generation_sessions SET
				status = 'candidate_ready', candidate_id = $2
			WHERE generation_id = $1 AND session_id = 'active-session';
			INSERT INTO %s.candidate_evaluations (
				id, generation_id, candidate_id, request_sha256, profile,
				profile_version, evaluator_version, score, decision, created_at
			) VALUES (
				$3, $1, $2, 'malformed-cross-generation', 'generation-candidate-v1',
				'1', 'malformed-fixture', 0.1, 'revise', $5
			);
			INSERT INTO %s.generation_diagnostics (
				id, generation_id, candidate_id, stage, code, message, created_at
			) VALUES (
				$4, $1, $2, 'evaluation', 'malformed_cross_generation',
				'test-only malformed predecessor link', $5
			)`, quotedPredecessorSchema, quotedPredecessorSchema,
			quotedPredecessorSchema), pgx.QueryExecModeSimpleProtocol,
			activeGenerationID, candidateID, malformedEvaluationID, malformedDiagnosticID,
			now.Add(11*time.Minute))
		Expect(err).NotTo(HaveOccurred(), "the predecessor schema deliberately constrained child UUIDs without composite ownership")

		By("repairing retained text evaluation IDs before UUID contract installation and dropping unsafe artifacts")
		seededEvaluationID := evaluationID
		seededUnsafeEvaluationID := unsafeEvaluationID
		const legacyEvaluationID = "legacy-retained-evaluation"
		const legacyUnsafeEvaluationID = "legacy-unsafe-evaluation"
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`
			ALTER TABLE %s.candidate_evaluations
				ALTER COLUMN id TYPE TEXT USING id::text;
			UPDATE %s.candidate_evaluations SET id = $2 WHERE id = $1;
			UPDATE %s.candidate_evaluations SET id = $4 WHERE id = $3`,
			quotedPredecessorSchema, quotedPredecessorSchema, quotedPredecessorSchema),
			pgx.QueryExecModeSimpleProtocol, seededEvaluationID, legacyEvaluationID,
			seededUnsafeEvaluationID, legacyUnsafeEvaluationID)
		Expect(err).NotTo(HaveOccurred())
		evaluationID = migratedCandidateEvaluationID(legacyEvaluationID, generationID,
			candidateID, "migration-evaluation-request-sha256", 0)
		unsafeEvaluationID = migratedCandidateEvaluationID(legacyUnsafeEvaluationID,
			activeGenerationID, unsafeCandidateID, "unsafe-request", 0)

		Expect(predecessorStore.migrate(ctx)).To(Succeed())
		predecessorRevisions, err := migratedRevisionRecords(ctx, predecessorStore, predecessorSkillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(predecessorRevisions).To(HaveLen(3))

		publishedSnapshot := SkillRevisionSnapshot{
			Name: "Published predecessor", Description: "published metadata", Type: "workflow",
			Tags: []string{"published", "preserved"}, Content: "# Published predecessor",
			SourceSessionIDs: []string{"published-session"},
		}
		expectMigratedRevisionSnapshot(predecessorRevisions[0], publishedSnapshot)
		Expect(predecessorRevisions[0].SequenceNumber).To(Equal(1))
		Expect(predecessorRevisions[0].Version).To(Equal("1"))
		Expect(predecessorRevisions[0].Origin).To(Equal(RevisionOriginMigrated))
		Expect(predecessorRevisions[0].ChangeNote).To(Equal("published"))
		Expect(predecessorRevisions[0].BasedOnRevisionID).To(BeEmpty())
		Expect(predecessorRevisions[0].SourceRevisionID).To(BeEmpty())
		Expect(predecessorRevisions[0].GenerationID).To(BeEmpty())
		Expect(predecessorRevisions[0].CreatedAt).To(BeTemporally("==", now))
		Expect(predecessorRevisions[0].IsPublic).To(BeTrue())

		privateSnapshot := SkillRevisionSnapshot{
			Name: "Private checkpoint", Description: "private metadata", Type: "workflow",
			Tags: []string{"private", "checkpoint"}, Content: "# Private checkpoint",
			IsAIGenerated: true, SourceSessionIDs: []string{"draft-session"},
		}
		expectMigratedRevisionSnapshot(predecessorRevisions[1], privateSnapshot)
		Expect(predecessorRevisions[1].SequenceNumber).To(Equal(2))
		Expect(predecessorRevisions[1].Version).To(Equal("2"))
		Expect(predecessorRevisions[1].Origin).To(Equal(RevisionOriginMigrated))
		Expect(predecessorRevisions[1].ChangeNote).To(BeEmpty())
		Expect(predecessorRevisions[1].BasedOnRevisionID).To(Equal(predecessorRevisions[0].ID))
		Expect(predecessorRevisions[1].SourceRevisionID).To(BeEmpty())
		Expect(predecessorRevisions[1].GenerationID).To(BeEmpty())
		Expect(predecessorRevisions[1].CreatedAt).To(BeTemporally("==", now.Add(time.Minute)))
		Expect(predecessorRevisions[1].IsPublic).To(BeFalse())

		generatedSnapshot := SkillRevisionSnapshot{
			Name: "Synthesized result", Description: "synthesized metadata", Type: "workflow",
			Tags: []string{"generated", "synthesis"}, Content: "# Synthesized result",
			IsAIGenerated: true, SourceSessionIDs: []string{"generation-session"},
		}
		expectMigratedRevisionSnapshot(predecessorRevisions[2], generatedSnapshot)
		Expect(predecessorRevisions[2].SequenceNumber).To(Equal(3))
		Expect(predecessorRevisions[2].Version).To(Equal("3"))
		Expect(predecessorRevisions[2].Origin).To(Equal(RevisionOriginGeneration))
		Expect(predecessorRevisions[2].ChangeNote).To(BeEmpty())
		Expect(predecessorRevisions[2].BasedOnRevisionID).To(Equal(predecessorRevisions[1].ID))
		Expect(predecessorRevisions[2].SourceRevisionID).To(BeEmpty())
		Expect(predecessorRevisions[2].GenerationID).To(Equal(generationID))
		Expect(predecessorRevisions[2].IdempotencyKey).To(Equal("generation:" + generationID))
		Expect(predecessorRevisions[2].CreatedAt).To(BeTemporally("==", now.Add(9*time.Minute)))
		Expect(predecessorRevisions[2].IsPublic).To(BeFalse())

		for _, revision := range predecessorRevisions {
			Expect(revision.CreatorSubject).To(Equal("predecessor-owner"))
			Expect(revision.ChangedBySubject).To(Equal("predecessor-owner"))
			Expect(revision.ChangedAt).To(BeTemporally("==", revision.CreatedAt))
			Expect(revision.LegacyReference).NotTo(BeEmpty())
		}
		var predecessorLatest string
		var predecessorNextSequence int
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text, next_sequence_number FROM %s.skills WHERE id = $1`, quotedPredecessorSchema), predecessorSkillID).Scan(&predecessorLatest, &predecessorNextSequence)).To(Succeed())
		Expect(predecessorLatest).To(Equal(predecessorRevisions[0].ID))
		Expect(predecessorNextSequence).To(Equal(4))

		var (
			generationSkillID, generationCreator, generationBaseRevisionID string
			generationStatus                                               string
			generationInput                                                SkillRevisionSnapshot
			authorContext                                                  string
			selectedSessionIDs                                             []string
			evaluatorProfile, evaluatorProfileVersion                      string
			evaluationCriteria                                             []byte
			winnerCandidateID, resultCandidateID, resultRevisionID         string
			generationCreatedAt, generationCompletedAt                     time.Time
			legacyLinksCleared                                             bool
		)
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT skill_id::text, creator_subject, base_revision_id::text, status,
			       input_name, input_description, input_type, input_tags,
			       input_content, input_is_ai_generated, input_source_session_ids,
			       author_context, selected_session_ids, evaluator_profile,
			       evaluator_profile_version, evaluation_criteria,
			       winner_candidate_id::text, result_candidate_id::text,
			       result_revision_id::text, created_at, completed_at,
			       draft_id IS NULL AND owner_subject IS NULL
			         AND starting_lock_version IS NULL AND proposed_candidate_id IS NULL
			         AND proposed_revision_number IS NULL AND resolution IS NULL
			FROM %s.skill_generations WHERE id = $1`, quotedPredecessorSchema), generationID).Scan(
			&generationSkillID, &generationCreator, &generationBaseRevisionID, &generationStatus,
			&generationInput.Name, &generationInput.Description, &generationInput.Type,
			&generationInput.Tags, &generationInput.Content, &generationInput.IsAIGenerated,
			&generationInput.SourceSessionIDs, &authorContext, &selectedSessionIDs,
			&evaluatorProfile, &evaluatorProfileVersion, &evaluationCriteria,
			&winnerCandidateID, &resultCandidateID, &resultRevisionID,
			&generationCreatedAt, &generationCompletedAt, &legacyLinksCleared,
		)).To(Succeed())
		Expect(generationSkillID).To(Equal(predecessorSkillID))
		Expect(generationCreator).To(Equal("predecessor-owner"))
		Expect(generationBaseRevisionID).To(Equal(predecessorRevisions[1].ID))
		Expect(generationStatus).To(Equal(string(GenerationStatusCompleted)))
		Expect(generationInput).To(Equal(privateSnapshot))
		Expect(legacyLinksCleared).To(BeTrue())
		Expect(authorContext).To(Equal("Improve the private revision."))
		Expect(selectedSessionIDs).To(Equal([]string{"generation-session"}))
		Expect(evaluatorProfile).To(Equal("generation-candidate-v1"))
		Expect(evaluatorProfileVersion).To(Equal("1"))
		Expect(evaluationCriteria).To(MatchJSON(`[{
			"id":"complete","kind":"structure","description":"Complete.","weight":1
		}]`))
		Expect(winnerCandidateID).To(Equal(candidateID), "migration must reconstruct the persisted non-synthesis winner")
		Expect(resultCandidateID).To(Equal(synthesisCandidateID), "migration must preserve the predecessor's final synthesis result")
		Expect(resultRevisionID).To(Equal(predecessorRevisions[2].ID))
		Expect(generationCreatedAt).To(BeTemporally("==", now.Add(2*time.Minute)))
		Expect(generationCompletedAt).To(BeTemporally("==", now.Add(9*time.Minute)))

		storedCandidate, err := scanGenerationCandidate(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT %s FROM %s.generation_candidates c WHERE c.id = $1`,
			generationCandidateColumns, quotedPredecessorSchema), candidateID))
		Expect(err).NotTo(HaveOccurred())
		Expect(storedCandidate.ID).To(Equal(candidateID))
		Expect(storedCandidate.GenerationID).To(Equal(generationID))
		Expect(storedCandidate.Ordinal).To(Equal(0))
		Expect(storedCandidate.Kind).To(Equal(GenerationCandidateSession))
		Expect(storedCandidate.SourceSessionIDs).To(Equal([]string{"generation-session"}))
		Expect(storedCandidate.Snapshot).To(Equal(GenerationCandidateSnapshot{
			Name: "Generated result", Description: "generated metadata", Type: "workflow",
			Tags: []string{"generated", "winner"}, Content: "# Generated result", IsAIGenerated: true,
		}))
		Expect(storedCandidate.Insights).To(MatchJSON(candidateInsights))
		Expect(storedCandidate.BundleSHA256).To(MatchRegexp(`^[0-9a-f]{64}$`))
		Expect(storedCandidate.BundleSHA256).NotTo(Equal("migration-candidate-bundle-sha256"),
			"migration recomputes rather than trusting predecessor hashes")
		Expect(storedCandidate.CreatedAt).To(BeTemporally("==", now.Add(7*time.Minute)))
		storedResultCandidate, err := scanGenerationCandidate(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT %s FROM %s.generation_candidates c WHERE c.id = $1`,
			generationCandidateColumns, quotedPredecessorSchema), synthesisCandidateID))
		Expect(err).NotTo(HaveOccurred())
		Expect(storedResultCandidate.ID).To(Equal(synthesisCandidateID))
		Expect(generationCandidateRevisionSnapshot(*storedResultCandidate)).To(Equal(predecessorRevisions[2].Snapshot),
			"the migrated result candidate must exactly match its generated result revision")
		Expect(storedResultCandidate.Insights).To(MatchJSON(`[]`),
			"unsafe optional completed-result details must be dropped without deleting lineage")
		Expect(storedResultCandidate.BundleSHA256).To(MatchRegexp(`^[0-9a-f]{64}$`))

		storedEvaluation, err := scanCandidateEvaluation(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT %s FROM %s.candidate_evaluations e WHERE e.id = $1`,
			candidateEvaluationColumns, quotedPredecessorSchema), evaluationID))
		Expect(err).NotTo(HaveOccurred())
		Expect(storedEvaluation.ID).To(Equal(evaluationID))
		Expect(storedEvaluation.ID).NotTo(Equal(seededEvaluationID),
			"non-UUID predecessor identity must receive its deterministic migrated UUID")
		var migratedEvaluationIDType string
		Expect(predecessorStore.pool.QueryRow(ctx, `SELECT udt_name FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = 'candidate_evaluations' AND column_name = 'id'`,
			predecessorSchema).Scan(&migratedEvaluationIDType)).To(Succeed())
		Expect(migratedEvaluationIDType).To(Equal("uuid"))
		Expect(storedEvaluation.GenerationID).To(Equal(generationID))
		Expect(storedEvaluation.CandidateID).To(Equal(candidateID))
		Expect(storedEvaluation.RequestSHA256).To(Equal("migration-evaluation-request-sha256"))
		Expect(storedEvaluation.Profile).To(Equal("generation-candidate-v1"))
		Expect(storedEvaluation.ProfileVersion).To(Equal("1"))
		Expect(storedEvaluation.EvaluatorVersion).To(Equal("fixture-v2"))
		Expect(storedEvaluation.Score).To(HaveValue(Equal(0.9)))
		Expect(storedEvaluation.Decision).To(Equal("pass"))
		Expect(storedEvaluation.CriticalFindingCount).To(BeZero())
		Expect(storedEvaluation.WarningFindingCount).To(Equal(1))
		Expect(storedEvaluation.CriterionResults).To(MatchJSON(criterionResults))
		Expect(storedEvaluation.Findings).To(MatchJSON(findings))
		Expect(storedEvaluation.Strengths).To(MatchJSON(strengths))
		Expect(storedEvaluation.Panel).To(MatchJSON(`{}`), "opaque predecessor panel data is sanitized")
		Expect(storedEvaluation.CreatedAt).To(BeTemporally("==", now.Add(8*time.Minute)))

		By("keeping predecessor active work modern and claimable after migration")
		activeState, err := predecessorStore.GetGenerationByID(ctx, "predecessor-owner", activeGenerationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(activeState).NotTo(BeNil())
		Expect(activeState.Generation.SkillID).To(Equal(predecessorSkillID))
		Expect(activeState.Generation.BaseRevisionID).To(Equal(predecessorRevisions[1].ID))
		Expect(activeState.Generation.CreatorSubject).To(Equal("predecessor-owner"))
		Expect(activeState.Generation.Status).To(Equal(GenerationStatusQueued))
		Expect(activeState.Generation.Snapshot).To(Equal(privateSnapshot))
		Expect(activeState.Generation.AuthorContext).To(Equal("Continue the retained work."))
		Expect(activeState.Generation.SelectedSessionIDs).To(Equal([]string{"active-session"}))
		Expect(activeState.Generation.WinnerCandidateID).To(BeEmpty())
		Expect(activeState.Generation.ResultCandidateID).To(BeEmpty())
		var activeLegacyLinksCleared bool
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT
			draft_id IS NULL AND owner_subject IS NULL AND starting_lock_version IS NULL
			  AND proposed_candidate_id IS NULL AND proposed_revision_number IS NULL AND resolution IS NULL
			FROM %s.skill_generations WHERE id = $1`, quotedPredecessorSchema), activeGenerationID).
			Scan(&activeLegacyLinksCleared)).To(Succeed())
		Expect(activeLegacyLinksCleared).To(BeTrue())
		Expect(activeState.Sessions).To(HaveLen(1))
		Expect(activeState.Sessions[0].Status).To(Equal(GenerationSessionPending))
		Expect(activeState.Sessions[0].CandidateID).To(BeEmpty(), "an unsafe cross-generation session outcome is reset deterministically")
		Expect(activeState.Candidates).To(HaveLen(1), "safe candidate identity and placement survive payload sanitization")
		Expect(activeState.Candidates[0].ID).To(Equal(unsafeCandidateID))
		Expect(activeState.Candidates[0].Insights).To(MatchJSON(`[]`), "unsafe optional insight details are discarded")
		Expect(activeState.Candidates[0].BundleSHA256).To(MatchRegexp(`^[0-9a-f]{64}$`))
		Expect(activeState.Evaluations).To(HaveLen(1), "a valid judgment remains linked to the normalized candidate")
		Expect(activeState.Evaluations[0].ID).To(Equal(unsafeEvaluationID))
		Expect(activeState.Evaluations[0].CandidateID).To(Equal(unsafeCandidateID))
		Expect(activeState.Evaluations[0].Panel).To(MatchJSON(`{}`))
		Expect(activeState.Diagnostics).To(BeEmpty(), "legacy diagnostics outside the runtime catalog are dropped")
		for table, artifactID := range map[string]string{
			"generation_candidates": unsafeCandidateID,
			"candidate_evaluations": unsafeEvaluationID,
		} {
			var retained bool
			Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (
				SELECT 1 FROM %s.%s WHERE id = $1
			)`, quotedPredecessorSchema, quoteIdentifier(table)), artifactID).Scan(&retained)).To(Succeed())
			Expect(retained).To(BeTrue(), table)
		}
		var unsafeDiagnosticRetained bool
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (
			SELECT 1 FROM %s.generation_diagnostics WHERE id = $1
		)`, quotedPredecessorSchema), unsafeSynthesisDiagnosticID).Scan(&unsafeDiagnosticRetained)).To(Succeed())
		Expect(unsafeDiagnosticRetained).To(BeFalse())
		malformedEvaluation, scanErr := scanCandidateEvaluation(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT %s FROM %s.candidate_evaluations e WHERE e.id = $1`,
			candidateEvaluationColumns, quotedPredecessorSchema), malformedEvaluationID))
		Expect(scanErr).NotTo(HaveOccurred())
		Expect(malformedEvaluation.GenerationID).To(Equal(generationID), "a required evaluation link must follow its candidate's generation")
		Expect(malformedEvaluation.CandidateID).To(Equal(candidateID))

		By("rejecting the same malformed writes after deterministic repair and constraint installation")
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_sessions
			SET candidate_id = $2 WHERE generation_id = $1 AND session_id = 'active-session'`,
			quotedPredecessorSchema), activeGenerationID, candidateID)
		expectPostgresConstraint(err, "generation_sessions_same_generation_candidate_fkey")
		var malformedDiagnosticRetained bool
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (
			SELECT 1 FROM %s.generation_diagnostics WHERE id = $1
		)`, quotedPredecessorSchema), malformedDiagnosticID).Scan(&malformedDiagnosticRetained)).To(Succeed())
		Expect(malformedDiagnosticRetained).To(BeFalse())
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.candidate_evaluations
			SET generation_id = $2 WHERE id = $1`, quotedPredecessorSchema), malformedEvaluationID, activeGenerationID)
		expectPostgresConstraint(err, "candidate_evaluations_same_generation_candidate_fkey")
		Expect(activeState.Generation.ClaimToken).To(BeEmpty())
		Expect(makeGenerationDue(ctx, predecessorStore, activeGenerationID)).To(Succeed())
		activeClaim, err := predecessorStore.ClaimGeneration(ctx, ClaimGenerationInput{
			WorkerID: "migrated-active-worker", LeaseDuration: time.Minute,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(activeClaim).NotTo(BeNil())
		Expect(activeClaim.ID).To(Equal(activeGenerationID))
		Expect(activeClaim.SkillID).To(Equal(predecessorSkillID))
		Expect(activeClaim.BaseRevisionID).To(Equal(predecessorRevisions[1].ID))
		Expect(activeClaim.CreatorSubject).To(Equal("predecessor-owner"))
		Expect(activeClaim.ClaimToken).NotTo(BeEmpty())
		claimedActiveState, err := predecessorStore.GetClaimedGeneration(ctx, activeGenerationID, activeClaim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(claimedActiveState.Generation.ID).To(Equal(activeGenerationID))
		_, err = predecessorStore.CancelSkillGeneration(ctx, "predecessor-owner", predecessorSkillID, activeGenerationID)
		Expect(err).NotTo(HaveOccurred())

		var generationCount, candidateCount, evaluationCount, generatedRevisionCount int
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_generations WHERE id = $1`, quotedPredecessorSchema), generationID).Scan(&generationCount)).To(Succeed())
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.generation_candidates WHERE generation_id = $1`, quotedPredecessorSchema), generationID).Scan(&candidateCount)).To(Succeed())
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.candidate_evaluations WHERE generation_id = $1`, quotedPredecessorSchema), generationID).Scan(&evaluationCount)).To(Succeed())
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions WHERE generation_id = $1`, quotedPredecessorSchema), generationID).Scan(&generatedRevisionCount)).To(Succeed())
		Expect([]int{generationCount, candidateCount, evaluationCount, generatedRevisionCount}).To(Equal([]int{1, 2, 3, 1}),
			"the malformed required evaluation must be repaired into its candidate's generation before constraints install")

		By("repairing a database that already ran the first durable-generation migration")
		lowerRankedCandidateID := uuid.NewString()
		lowerRankedEvaluationID := uuid.NewString()
		_, err = predecessorStore.pool.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s.generation_candidates (
				id, generation_id, ordinal, kind, source_session_ids, name,
				description, type, tags, content, is_ai_generated, insights,
				bundle_sha256, created_at
			) VALUES (
				$2, $1, 2, 'session', ARRAY['lower-ranked-session'], 'Lower-ranked candidate',
				'lower-ranked metadata', 'workflow', ARRAY['generated'], '# Lower-ranked',
				TRUE, '[]'::jsonb, 'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd', $4
			);
			INSERT INTO %s.candidate_evaluations (
				id, generation_id, candidate_id, request_sha256, profile, profile_version,
				evaluator_version, score, decision, critical_finding_count,
				warning_finding_count, criterion_results, findings, strengths, panel, created_at
			) VALUES (
				$3, $1, $2, 'migration-lower-ranked-evaluation', 'generation-candidate-v1', '1',
				'fixture-v2', 0.8, 'pass', 0, 0, '[]'::jsonb, '[]'::jsonb,
				'[]'::jsonb, '{}'::jsonb, $4
			);
			UPDATE %s.skill_generations SET winner_candidate_id = result_candidate_id
			WHERE id = $1`, quotedPredecessorSchema, quotedPredecessorSchema,
			quotedPredecessorSchema), pgx.QueryExecModeSimpleProtocol, generationID,
			lowerRankedCandidateID, lowerRankedEvaluationID, now.Add(12*time.Minute))
		Expect(err).NotTo(HaveOccurred())
		Expect(predecessorStore.migrate(ctx)).To(Succeed())
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT
			winner_candidate_id::text, result_candidate_id::text
			FROM %s.skill_generations WHERE id = $1`, quotedPredecessorSchema), generationID).
			Scan(&winnerCandidateID, &resultCandidateID)).To(Succeed())
		Expect(winnerCandidateID).To(Equal(candidateID), "rerun must derive the deterministic pre-synthesis winner")
		Expect(resultCandidateID).To(Equal(synthesisCandidateID), "rerun must preserve the selected synthesis result")

		predecessorRevisionsAfterRetry, err := migratedRevisionRecords(ctx, predecessorStore, predecessorSkillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(predecessorRevisionsAfterRetry).To(Equal(predecessorRevisions))
		candidateAfterRetry, err := scanGenerationCandidate(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT %s FROM %s.generation_candidates c WHERE c.id = $1`,
			generationCandidateColumns, quotedPredecessorSchema), candidateID))
		Expect(err).NotTo(HaveOccurred())
		Expect(candidateAfterRetry).To(Equal(storedCandidate))
		evaluationAfterRetry, err := scanCandidateEvaluation(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT %s FROM %s.candidate_evaluations e WHERE e.id = $1`,
			candidateEvaluationColumns, quotedPredecessorSchema), evaluationID))
		Expect(err).NotTo(HaveOccurred())
		Expect(evaluationAfterRetry).To(Equal(storedEvaluation))
		malformedEvaluationAfterRetry, err := scanCandidateEvaluation(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT %s FROM %s.candidate_evaluations e WHERE e.id = $1`,
			candidateEvaluationColumns, quotedPredecessorSchema), malformedEvaluationID))
		Expect(err).NotTo(HaveOccurred())
		Expect(malformedEvaluationAfterRetry).To(Equal(malformedEvaluation), "deterministic repair must be idempotent on migration retry")

		Expect(predecessorStore.migrate(ctx)).To(Succeed())
		var idempotentWinnerCandidateID, idempotentResultCandidateID string
		Expect(predecessorStore.pool.QueryRow(ctx, fmt.Sprintf(`SELECT
			winner_candidate_id::text, result_candidate_id::text
			FROM %s.skill_generations WHERE id = $1`, quotedPredecessorSchema), generationID).
			Scan(&idempotentWinnerCandidateID, &idempotentResultCandidateID)).To(Succeed())
		Expect(idempotentWinnerCandidateID).To(Equal(candidateID))
		Expect(idempotentResultCandidateID).To(Equal(synthesisCandidateID))
	})

	It("sanitizes generation history in bounded keyset pages without breaking lineage", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		schema := "skills_generation_sanitize_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})

		const creator = "migration-sanitize-owner"
		now := time.Now().UTC().Add(-time.Hour)
		skillRecord, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "generation-sanitize-pages", CreatorSubject: creator, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		criteria := json.RawMessage(`[{"id":"complete","weight":1}]`)
		type lineage struct {
			generationID string
			candidateID  string
			revisionID   string
			completed    bool
		}
		lineages := make([]lineage, 0, migrationGenerationSanitizePageSize+1)
		for index := 0; index < migrationGenerationSanitizePageSize+1; index++ {
			generationID := fmt.Sprintf("00000000-0000-4000-8000-%012x", index+1)
			candidateID := fmt.Sprintf("10000000-0000-4000-8000-%012x", index+1)
			generation, createErr := store.CreateGeneration(ctx, CreateGenerationInput{
				ID: generationID, SkillID: skillRecord.ID, CreatorSubject: creator,
				Snapshot: SkillRevisionSnapshot{
					Name: fmt.Sprintf("Input %02d", index), Description: "Persisted author intent.",
					Type: "workflow", Tags: []string{"input"}, Content: "# Input",
					SourceSessionIDs: []string{"prior-source"},
				},
				AuthorContext:    "Preserve the immutable input.",
				EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
				EvaluationCriteria: criteria, CreatedAt: now.Add(time.Duration(index) * time.Second),
			})
			Expect(createErr).NotTo(HaveOccurred())
			claim, claimErr := store.ClaimGeneration(ctx, ClaimGenerationInput{
				WorkerID: "migration-sanitize-worker", LeaseDuration: time.Minute,
			})
			Expect(claimErr).NotTo(HaveOccurred())
			Expect(claim).NotTo(BeNil())
			Expect(claim.ID).To(Equal(generation.ID))
			Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
				GenerationStatusQueued, GenerationStatusGeneratingCandidates)).To(Succeed())
			candidate, candidateErr := store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken,
				GenerationCandidateRecord{
					ID: candidateID, Ordinal: 0, Kind: GenerationCandidateContext,
					Snapshot: GenerationCandidateSnapshot{
						Name: fmt.Sprintf("Result %02d", index), Description: "Safe result.", Type: "workflow",
						Tags: []string{"result"}, Content: "# Result", IsAIGenerated: true,
					},
					Insights: json.RawMessage(`[]`), BundleSHA256: strings.Repeat("a", 64),
				})
			Expect(candidateErr).NotTo(HaveOccurred())
			Expect(candidate.ID).To(Equal(candidateID))
			Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
				GenerationStatusGeneratingCandidates, GenerationStatusEvaluatingCandidates)).To(Succeed())
			requestSHA256, hashErr := GenerationCandidateEvaluationRequestSHA256(*generation, *candidate)
			Expect(hashErr).NotTo(HaveOccurred())
			score := 0.9
			_, evaluationErr := store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken,
				CandidateEvaluationRecord{
					ID: fmt.Sprintf("20000000-0000-4000-8000-%012x", index+1), CandidateID: candidateID,
					RequestSHA256: requestSHA256, Profile: "generation-candidate-v1",
					ProfileVersion: "1", EvaluatorVersion: "fixture", Score: &score, Decision: "pass",
					CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`),
					Strengths: json.RawMessage(`[]`), Panel: json.RawMessage(`{}`),
				})
			Expect(evaluationErr).NotTo(HaveOccurred())
			lineage := lineage{generationID: generationID, candidateID: candidateID}
			if index < migrationGenerationSanitizePageSize {
				Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
					GenerationStatusEvaluatingCandidates, GenerationStatusSynthesizing)).To(Succeed())
				revision, appendErr := store.AppendPrivateGenerationResult(ctx, AppendPrivateGenerationResultInput{
					GenerationID: generation.ID, ClaimToken: claim.ClaimToken,
					InitialWinnerCandidateID: candidateID, ResultCandidateID: candidateID,
				})
				Expect(appendErr).NotTo(HaveOccurred())
				lineage.revisionID = revision.ID
				lineage.completed = true
			}
			lineages = append(lineages, lineage)
		}

		unsafeName := strings.Repeat("n", MaxRevisionNameCodePoints+1)
		unsafeDescription := " legacy\x01description "
		unsafeContent := "# Legacy\x01 content"
		unsafeTags := []string{" repeated ", "repeated", strings.Repeat("t", MaxRevisionIdentityCodePoints+1), "safe"}
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_candidates SET
			insights = '[{"kind":"legacy","opaque":"secret"}]'::jsonb,
			bundle_sha256 = $1`, quotedSchema), strings.Repeat("b", 64))
		Expect(err).NotTo(HaveOccurred())
		active := lineages[migrationGenerationSanitizePageSize]
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_candidates SET
			name = $2, description = $3, type = 'legacy-unknown', tags = $4,
			content = $5 WHERE id = $1`, quotedSchema), active.candidateID,
			unsafeName, unsafeDescription, unsafeTags, unsafeContent)
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.candidate_evaluations
			SET panel = '{"provider":"must-be-dropped"}'::jsonb`, quotedSchema))
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s.skill_generations
			DROP CONSTRAINT skill_generations_status_check;
			UPDATE %s.skill_generations SET status = 'awaiting_input'
			WHERE id = $1`, quotedSchema, quotedSchema), pgx.QueryExecModeSimpleProtocol,
			lineages[1].generationID)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.migrate(ctx)).To(Succeed())
		statesAfterFirstPass := make([]GenerationState, len(lineages))
		for index, expected := range lineages {
			state, readErr := store.GetGenerationByID(ctx, creator, expected.generationID)
			Expect(readErr).NotTo(HaveOccurred())
			Expect(state).NotTo(BeNil())
			Expect(state.Candidates).To(HaveLen(1))
			candidate := state.Candidates[0]
			Expect(candidate.ID).To(Equal(expected.candidateID), "candidate UUID lineage must survive sanitization")
			Expect(candidate.Snapshot.IsAIGenerated).To(BeTrue())
			Expect(candidate.SourceSessionIDs).To(BeEmpty())
			Expect(candidate.Insights).To(MatchJSON(`[]`))
			expectedHash, hashErr := GenerationCandidateBundleSHA256(
				candidate.Snapshot, candidate.SourceSessionIDs, candidate.Insights,
			)
			Expect(hashErr).NotTo(HaveOccurred())
			Expect(candidate.BundleSHA256).To(Equal(expectedHash))

			if expected.completed {
				Expect(state.Generation.Status).To(Equal(GenerationStatusCompleted))
				Expect(state.Generation.WinnerCandidateID).To(Equal(expected.candidateID))
				Expect(state.Generation.ResultCandidateID).To(Equal(expected.candidateID))
				Expect(state.Generation.ResultRevisionID).To(Equal(expected.revisionID))
				Expect(candidate.Snapshot).To(Equal(GenerationCandidateSnapshot{
					Name: fmt.Sprintf("Result %02d", index), Description: "Safe result.", Type: "workflow",
					Tags: []string{"result"}, Content: "# Result", IsAIGenerated: true,
				}))
				Expect(state.Evaluations).To(HaveLen(1), "unchanged result evaluations must be retained")
				Expect(state.Evaluations[0].CandidateID).To(Equal(expected.candidateID))
				expectedRequestHash, requestHashErr := GenerationCandidateEvaluationRequestSHA256(
					state.Generation, candidate,
				)
				Expect(requestHashErr).NotTo(HaveOccurred())
				Expect(state.Evaluations[0].RequestSHA256).To(Equal(expectedRequestHash),
					"retained evaluation content identity must remain provable")
				Expect(state.Evaluations[0].Panel).To(MatchJSON(`{}`))
				revision, revisionErr := persistedRevisionForTest(ctx, store, expected.revisionID)
				Expect(revisionErr).NotTo(HaveOccurred())
				Expect(generationCandidateRevisionSnapshot(candidate)).To(Equal(revision.Snapshot),
					"a completed result candidate must exactly match its generated revision")
			} else {
				Expect(state.Generation.Status).To(Equal(GenerationStatusEvaluatingCandidates))
				Expect(state.Generation.WinnerCandidateID).To(BeEmpty())
				Expect(state.Generation.ResultCandidateID).To(BeEmpty())
				Expect(state.Generation.ResultRevisionID).To(BeEmpty())
				Expect(candidate.Snapshot.Name).To(Equal(strings.Repeat("n", MaxRevisionNameCodePoints)))
				Expect(candidate.Snapshot.Description).To(Equal("legacydescription"))
				Expect(candidate.Snapshot.Type).To(Equal("workflow"))
				Expect(candidate.Snapshot.Tags).To(Equal([]string{
					"repeated", strings.Repeat("t", MaxRevisionIdentityCodePoints), "safe",
				}))
				Expect(candidate.Snapshot.Content).To(Equal("# Legacy content"))
				Expect(state.Evaluations).To(BeEmpty(),
					"normalizing executable non-result content must invalidate its stale evaluation")
			}
			statesAfterFirstPass[index] = *state
		}
		Expect(statesAfterFirstPass[migrationGenerationSanitizePageSize].Candidates[0].Snapshot.Content).
			To(Equal("# Legacy content"), "the first generation beyond the fixed page must be sanitized")

		Expect(store.migrate(ctx)).To(Succeed())
		for index, expected := range lineages {
			state, readErr := store.GetGenerationByID(ctx, creator, expected.generationID)
			Expect(readErr).NotTo(HaveOccurred())
			Expect(*state).To(Equal(statesAfterFirstPass[index]), "artifact sanitization must be idempotent")
		}

		broken := lineages[0]
		invalidEvaluationHash := strings.Repeat("0", 64)
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.candidate_evaluations
			SET request_sha256 = $2 WHERE candidate_id = $1`, quotedSchema),
			broken.candidateID, invalidEvaluationHash)
		Expect(err).NotTo(HaveOccurred())
		err = store.migrate(ctx)
		Expect(err).To(MatchError(ContainSubstring("migration_candidate_lineage_invalid")))
		var retainedEvaluationHash string
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT request_sha256
			FROM %s.candidate_evaluations WHERE candidate_id = $1`, quotedSchema), broken.candidateID).
			Scan(&retainedEvaluationHash)).To(Succeed())
		Expect(retainedEvaluationHash).To(Equal(invalidEvaluationHash),
			"an unprovable result evaluation must fail without being silently rebound")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.candidate_evaluations
			SET request_sha256 = $2 WHERE candidate_id = $1`, quotedSchema), broken.candidateID,
			statesAfterFirstPass[0].Evaluations[0].RequestSHA256)
		Expect(err).NotTo(HaveOccurred())

		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_candidates
			SET content = $2, bundle_sha256 = $3 WHERE id = $1`, quotedSchema),
			broken.candidateID, "# Different retained result", strings.Repeat("c", 64))
		Expect(err).NotTo(HaveOccurred())
		err = store.migrate(ctx)
		Expect(err).To(MatchError(ContainSubstring("migration_candidate_lineage_invalid")))
		var rolledBackContent, rolledBackHash string
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT content, bundle_sha256
			FROM %s.generation_candidates WHERE id = $1`, quotedSchema), broken.candidateID).
			Scan(&rolledBackContent, &rolledBackHash)).To(Succeed())
		Expect(rolledBackContent).To(Equal("# Different retained result"),
			"result mismatch must roll back sanitizer writes instead of changing revision semantics")
		Expect(rolledBackHash).To(Equal(strings.Repeat("c", 64)))
	})

	It("captures a complete current-main head independently of an open draft", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
		schema := "skills_complete_head_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})
		Expect(removeDurableRevisionTargetForFixture(ctx, store)).To(Succeed())

		parentID := uuid.NewString()
		_, err = store.UpsertSkill(ctx, SkillRecord{
			ID: parentID, Slug: "head-parent", Name: "Head parent", Type: "workflow",
			Version: "0.1.0", Visibility: "private", Content: "# Parent",
			AuthorSubject: "owner", CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: parentID, VersionNumber: 1, Semver: "0.1.0", Content: "# Parent",
			AuthorSubject: "owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		skillID := uuid.NewString()
		published := SkillRecord{
			ID: skillID, Slug: "complete-head", Name: "Published name",
			Description: "published description", Type: "workflow", Version: "0.1.0",
			Visibility: "private", Tags: []string{"published"}, Content: "# Same content",
			AuthorSubject: "owner", CreatedAt: now, UpdatedAt: now,
		}
		_, err = store.UpsertSkill(ctx, published)
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: skillID, VersionNumber: 1, Semver: "0.1.0", Content: published.Content,
			AuthorSubject: "owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		draftID := uuid.NewString()
		baseVersion := 1
		Expect(seedPredecessorDraft(ctx, store, predecessorDraftFixture{
			id: draftID, targetSkillID: skillID, ownerSubject: "owner",
			baseSkillVersionNumber: &baseVersion,
			working: predecessorSkillSnapshot{
				Slug: "complete-head", Name: "Independent open draft", Type: "workflow",
				Content: "# Draft content",
			},
			createdAt: now.Add(time.Minute),
		})).To(Succeed())

		mutableHead := published
		mutableHead.Name = "Mutable metadata"
		mutableHead.Description = "changed without changing content"
		mutableHead.Tags = []string{"private", "head"}
		mutableHead.IsAIGenerated = true
		mutableHead.GeneratedFromSessionIDs = []string{"head-session"}
		mutableHead.ParentID = parentID
		mutableHead.UpdatedAt = now.Add(2 * time.Minute)
		_, err = store.UpsertSkill(ctx, mutableHead)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.migrate(ctx)).To(Succeed())
		revisions, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(revisions).To(HaveLen(3), "published history, the open draft, and the independent mutable head must all survive")
		parentRevisions, err := migratedRevisionRecords(ctx, store, parentID)
		Expect(err).NotTo(HaveOccurred())
		Expect(parentRevisions).To(HaveLen(1))

		var publishedRevision, draftRevision, headRevision *migratedRevisionTestRecord
		for index := range revisions {
			revision := &revisions[index]
			switch {
			case revision.LegacyReference == "skill-version:"+skillID+":1":
				publishedRevision = revision
			case revision.LegacyReference == "draft-revision:"+draftID+":1":
				draftRevision = revision
			case len(revision.LegacyReference) > len("skill-working:"+skillID+":") &&
				revision.LegacyReference[:len("skill-working:"+skillID+":")] == "skill-working:"+skillID+":":
				headRevision = revision
			}
		}
		Expect(publishedRevision).NotTo(BeNil())
		Expect(draftRevision).NotTo(BeNil())
		Expect(headRevision).NotTo(BeNil())
		Expect(publishedRevision.IsPublic).To(BeTrue())
		Expect(draftRevision.Snapshot.Content).To(Equal("# Draft content"))
		Expect(headRevision.Snapshot).To(Equal(SkillRevisionSnapshot{
			Name: "Mutable metadata", Description: "changed without changing content",
			Type: "workflow", Tags: []string{"private", "head"}, Content: "# Same content",
			IsAIGenerated: true, SourceSessionIDs: []string{"head-session"},
		}))
		Expect(headRevision.BasedOnRevisionID).To(Equal(publishedRevision.ID))
		Expect(headRevision.SourceRevisionID).To(Equal(parentRevisions[0].ID))
		Expect(headRevision.IsPublic).To(BeFalse())

		persistedHead, err := store.GetSkill(ctx, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(persistedHead.Name).To(Equal(published.Name), "compatibility fields reset only after the distinct head is captured")
		Expect(persistedHead.Description).To(Equal(published.Description))
		Expect(persistedHead.Tags).To(Equal(published.Tags))
		Expect(persistedHead.Content).To(Equal(published.Content))
		Expect(persistedHead.IsAIGenerated).To(BeFalse())
		Expect(persistedHead.GeneratedFromSessionIDs).To(BeEmpty())
		Expect(persistedHead.ParentID).To(BeEmpty())
		var openDrafts, capturedHeads int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_drafts WHERE id = $1 AND status = 'open'`, quotedSchema), draftID).Scan(&openDrafts)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revision_migration_sources WHERE skill_id = $1`, quotedSchema), skillID).Scan(&capturedHeads)).To(Succeed())
		Expect(openDrafts).To(Equal(1))
		Expect(capturedHeads).To(Equal(1))
	})

	It("uses the pointed draft revision instead of a newer unresolved generated checkpoint", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
		schema := "skills_working_pointer_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})
		Expect(removeDurableRevisionTargetForFixture(ctx, store)).To(Succeed())

		skillID := uuid.NewString()
		_, err = store.UpsertSkill(ctx, SkillRecord{
			ID: skillID, Slug: "pointed-working", Name: "Published", Type: "workflow",
			Version: "0.1.0", Visibility: "private", Content: "# Published",
			AuthorSubject: "owner", CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: skillID, VersionNumber: 1, Semver: "0.1.0", Content: "# Published",
			AuthorSubject: "owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		baseVersion := 1
		draftID := uuid.NewString()
		selected := predecessorSkillSnapshot{
			Slug: "pointed-working", Name: "Selected working", Type: "workflow",
			Content: "# Selected working", SourceSessionIDs: []string{"selected-session"},
		}
		Expect(seedPredecessorDraft(ctx, store, predecessorDraftFixture{
			id: draftID, targetSkillID: skillID, ownerSubject: "owner",
			baseSkillVersionNumber: &baseVersion, working: selected,
			createdAt: now.Add(time.Minute),
		})).To(Succeed())
		generationID := uuid.NewString()
		candidateID := uuid.NewString()
		evaluationID := uuid.NewString()
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_generations (
			id, draft_id, owner_subject, status, starting_lock_version,
			input_slug, input_name, input_description, input_type, input_tags,
			input_content, input_is_ai_generated, input_source_session_ids,
			author_context, selected_session_ids, evaluator_profile,
			evaluator_profile_version, evaluation_criteria, error_code, error_message,
			claim_owner, attempt_count, next_attempt_at, created_at, updated_at
		) VALUES (
			$1, $2, 'owner', 'queued', 1,
			'pointed-working', 'Selected working', '', 'workflow', '{}',
			'# Selected working', FALSE, ARRAY['selected-session'],
			'', ARRAY['generated-session'], 'generation-candidate-v1', '1', '[]'::jsonb, '', '', '', 0, $3, $3, $3
		)`, quotedSchema), generationID, draftID, now.Add(2*time.Minute))
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.draft_revisions (
			draft_id, revision_number, origin, generation_id, idempotency_key,
			slug, name, description, type, tags, content, is_ai_generated,
			source_session_ids, content_sha256, created_at
		) VALUES ($1, 2, 'generation', $2, $3, 'pointed-working',
			'Unresolved generated', '', 'workflow', '{}', '# Unresolved generated',
			TRUE, ARRAY['generated-session'], '', $4)`, quotedSchema),
			draftID, generationID, "generation:"+generationID, now.Add(3*time.Minute))
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.generation_candidates (
			id, generation_id, ordinal, kind, source_session_ids, slug, name,
			description, type, tags, content, is_ai_generated, insights, bundle_sha256, created_at
		) VALUES (
			$2, $1, 0, 'session', ARRAY['generated-session'], 'pointed-working',
			'Unresolved generated', '', 'workflow', '{}', '# Unresolved generated',
			TRUE, '[{"kind":"legacy","opaque":"unsafe-awaiting-detail"}]'::jsonb,
			'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', $3
		)`, quotedSchema), generationID, candidateID, now.Add(3*time.Minute))
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.generation_sessions (
			generation_id, session_id, ordinal, status, candidate_id, updated_at
		) VALUES ($1, 'generated-session', 0, 'evaluated', $2, $3)`, quotedSchema),
			generationID, candidateID, now.Add(3*time.Minute))
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.candidate_evaluations (
			id, generation_id, candidate_id, request_sha256, profile, profile_version,
			evaluator_version, score, decision, criterion_results, findings, strengths,
			panel, created_at
		) VALUES (
			$4, $1, $2, 'awaiting-request', 'generation-candidate-v1', '1',
			'fixture', 0.91, 'pass', '[]'::jsonb, '[]'::jsonb, '["retained"]'::jsonb,
			'{}'::jsonb, $3
		)`, quotedSchema), generationID, candidateID, now.Add(3*time.Minute), evaluationID)
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations
			SET status = 'awaiting_input', winner_candidate_id = $2,
			    proposed_candidate_id = $2, proposed_revision_number = 2, updated_at = $3
			WHERE id = $1`, quotedSchema), generationID, candidateID, now.Add(3*time.Minute))
		Expect(err).NotTo(HaveOccurred())

		By("rolling back an invalid legacy result candidate instead of rewriting it beneath its evaluation and revision")
		invalidAwaitingContent := "# Unresolved\x01 generated"
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_candidates
			SET content = $2 WHERE id = $1`, quotedSchema), candidateID, invalidAwaitingContent)
		Expect(err).NotTo(HaveOccurred())
		err = store.migrate(ctx)
		Expect(err).To(MatchError(ContainSubstring("migration_candidate_lineage_invalid")))
		var retainedInvalidContent, retainedStatus string
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT candidate.content, generation.status
			FROM %s.generation_candidates candidate
			JOIN %s.skill_generations generation ON generation.id = candidate.generation_id
			WHERE candidate.id = $1`, quotedSchema, quotedSchema), candidateID).
			Scan(&retainedInvalidContent, &retainedStatus)).To(Succeed())
		Expect(retainedInvalidContent).To(Equal(invalidAwaitingContent))
		Expect(retainedStatus).To(Equal("awaiting_input"))
		var migratedTargetExists bool
		Expect(store.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`,
			schema+".skill_revisions").Scan(&migratedTargetExists)).To(Succeed())
		Expect(migratedTargetExists).To(BeFalse(), "failed conversion must roll back the complete migration transaction")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_candidates
			SET content = '# Unresolved generated' WHERE id = $1`, quotedSchema), candidateID)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.migrate(ctx)).To(Succeed())
		revisions, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(revisions).To(HaveLen(3), "the selected checkpoint must not be copied as a child of unresolved output")
		var publishedRevision, selectedRevision, generatedRevision *migratedRevisionTestRecord
		for index := range revisions {
			revision := &revisions[index]
			switch revision.LegacyReference {
			case "skill-version:" + skillID + ":1":
				publishedRevision = revision
			case "draft-revision:" + draftID + ":1":
				selectedRevision = revision
			case "draft-revision:" + draftID + ":2":
				generatedRevision = revision
			}
			Expect(revision.LegacyReference).NotTo(HavePrefix("draft-working:" + draftID + ":"))
		}
		Expect(publishedRevision).NotTo(BeNil())
		Expect(selectedRevision).NotTo(BeNil())
		Expect(generatedRevision).NotTo(BeNil())
		Expect(selectedRevision.Snapshot.Content).To(Equal(selected.Content))
		Expect(selectedRevision.BasedOnRevisionID).To(Equal(publishedRevision.ID))
		Expect(generatedRevision.BasedOnRevisionID).To(Equal(selectedRevision.ID))
		Expect(generatedRevision.GenerationID).To(Equal(generationID))
		Expect(generatedRevision.IsPublic).To(BeFalse())

		generationState, err := store.GetGenerationByID(ctx, "owner", generationID)
		Expect(err).NotTo(HaveOccurred())
		Expect(generationState).NotTo(BeNil())
		Expect(generationState.Generation.SkillID).To(Equal(skillID))
		Expect(generationState.Generation.BaseRevisionID).To(Equal(selectedRevision.ID))
		Expect(generationState.Generation.CreatorSubject).To(Equal("owner"))
		Expect(generationState.Generation.Status).To(Equal(GenerationStatusCompleted))
		Expect(generationState.Generation.WinnerCandidateID).To(Equal(candidateID))
		Expect(generationState.Generation.ResultCandidateID).To(Equal(candidateID))
		Expect(generationState.Generation.ResultRevisionID).To(Equal(generatedRevision.ID))
		Expect(generationState.Generation.CompletedAt).To(HaveValue(BeTemporally("==", now.Add(3*time.Minute))))
		Expect(generationState.Generation.ClaimToken).To(BeEmpty())
		Expect(generationState.Generation.ClaimOwner).To(BeEmpty())
		Expect(generationState.Generation.LeaseExpiresAt).To(BeNil())
		Expect(generationState.Candidates).To(HaveLen(1))
		Expect(generationState.Candidates[0].ID).To(Equal(candidateID))
		Expect(generationCandidateRevisionSnapshot(generationState.Candidates[0])).To(Equal(generatedRevision.Snapshot),
			"awaiting-input conversion must retain exact candidate/revision content")
		Expect(generationState.Candidates[0].Insights).To(MatchJSON(`[]`),
			"unsafe optional predecessor details must not delete converted awaiting-input lineage")
		Expect(generationState.Candidates[0].BundleSHA256).To(MatchRegexp(`^[0-9a-f]{64}$`))
		Expect(generationState.Evaluations).To(HaveLen(1))
		Expect(generationState.Evaluations[0].ID).To(Equal(evaluationID))
		Expect(generationState.Evaluations[0].CandidateID).To(Equal(candidateID))

		otherRead, err := store.GetRevision(ctx, RevisionReadOpts{
			SkillID: skillID, RevisionID: generatedRevision.ID, CallerSubject: "other-member",
		})
		Expect(otherRead).To(BeNil())
		Expect(err).To(MatchError(ErrRevisionNotFound), "retained awaiting output must become a private completed result")
	})

	It("reconciles predecessor writes on rerun without taking over user latest", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
		schema := "skills_migration_rerun_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})
		Expect(removeDurableRevisionTargetForFixture(ctx, store)).To(Succeed())

		skillID := uuid.NewString()
		_, err = store.UpsertSkill(ctx, SkillRecord{
			ID: skillID, Slug: "rerun-writes", Name: "Rerun writes", Type: "workflow",
			Version: "0.1.0", Visibility: "private", Content: "# Published one",
			AuthorSubject: "owner", CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: skillID, VersionNumber: 1, Semver: "0.1.0", Content: "# Published one",
			AuthorSubject: "owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		baseVersion := 1
		draftID := uuid.NewString()
		Expect(seedPredecessorDraft(ctx, store, predecessorDraftFixture{
			id: draftID, targetSkillID: skillID, ownerSubject: "owner",
			baseSkillVersionNumber: &baseVersion,
			working: predecessorSkillSnapshot{
				Slug: "rerun-writes", Name: "Initial draft", Type: "workflow",
				Content: "# Initial draft",
			},
			createdAt: now.Add(time.Minute),
		})).To(Succeed())
		Expect(updatePredecessorDraft(ctx, store, draftID, "owner", 1,
			predecessorSkillSnapshot{
				Slug: "rerun-writes", Name: "Autosave A", Type: "workflow",
				Content: "# Autosave A", SourceSessionIDs: []string{"source-a"},
			}, now.Add(2*time.Minute))).To(Succeed())
		Expect(store.migrate(ctx)).To(Succeed())

		firstPass, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		var versionOneID string
		workingReferences := make([]string, 0)
		for _, revision := range firstPass {
			if revision.LegacyReference == "skill-version:"+skillID+":1" {
				versionOneID = revision.ID
			}
			if len(revision.LegacyReference) > len("draft-working:"+draftID+":") &&
				revision.LegacyReference[:len("draft-working:"+draftID+":")] == "draft-working:"+draftID+":" {
				workingReferences = append(workingReferences, revision.LegacyReference)
			}
		}
		Expect(versionOneID).NotTo(BeEmpty())
		Expect(workingReferences).To(HaveLen(1))
		var latestID, managedLatestID string
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text, migration_managed_latest_revision_id::text FROM %s.skills WHERE id = $1`, quotedSchema), skillID).Scan(&latestID, &managedLatestID)).To(Succeed())
		Expect(latestID).To(Equal(versionOneID))
		Expect(managedLatestID).To(Equal(versionOneID))

		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: skillID, VersionNumber: 2, Semver: "0.1.1", Content: "# Published two",
			AuthorSubject: "owner", PublishedAt: now.Add(3 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(updatePredecessorDraft(ctx, store, draftID, "owner", 2,
			predecessorSkillSnapshot{
				Slug: "rerun-writes", Name: "Autosave B", Type: "workflow",
				Content: "# Autosave B", SourceSessionIDs: []string{"source-b"},
			}, now.Add(4*time.Minute))).To(Succeed())
		Expect(store.migrate(ctx)).To(Succeed())

		secondPass, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		var versionTwoID, publicWorkingID string
		workingReferences = workingReferences[:0]
		for _, revision := range secondPass {
			if revision.LegacyReference == "skill-version:"+skillID+":2" {
				versionTwoID = revision.ID
			}
			if len(revision.LegacyReference) > len("draft-working:"+draftID+":") &&
				revision.LegacyReference[:len("draft-working:"+draftID+":")] == "draft-working:"+draftID+":" {
				workingReferences = append(workingReferences, revision.LegacyReference)
				publicWorkingID = revision.ID
			}
		}
		Expect(versionTwoID).NotTo(BeEmpty())
		Expect(workingReferences).To(HaveLen(2))
		Expect(workingReferences[0]).NotTo(Equal(workingReferences[1]), "changed autosaves need content-addressed migration identities")
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text, migration_managed_latest_revision_id::text FROM %s.skills WHERE id = $1`, quotedSchema), skillID).Scan(&latestID, &managedLatestID)).To(Succeed())
		Expect(latestID).To(Equal(versionTwoID))
		Expect(managedLatestID).To(Equal(versionTwoID))

		visibilityChangedAt := now.Add(4500 * time.Millisecond)
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_revision_visibility SET
			is_public = FALSE, changed_by_subject = 'visibility-editor', changed_at = $2
			WHERE revision_id = $1`, quotedSchema), versionOneID, visibilityChangedAt)
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_revision_visibility SET
			is_public = TRUE, changed_by_subject = 'visibility-editor', changed_at = $2
			WHERE revision_id = $1`, quotedSchema), publicWorkingID, visibilityChangedAt)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.migrate(ctx)).To(Succeed(), "rerun verification must preserve valid post-migration visibility edits")
		visibilityPass, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		for _, revision := range visibilityPass {
			switch revision.ID {
			case versionOneID:
				Expect(revision.IsPublic).To(BeFalse())
				Expect(revision.ChangedBySubject).To(Equal("visibility-editor"))
				Expect(revision.ChangedAt).To(BeTemporally("==", visibilityChangedAt))
			case publicWorkingID:
				Expect(revision.IsPublic).To(BeTrue())
				Expect(revision.ChangedBySubject).To(Equal("visibility-editor"))
				Expect(revision.ChangedAt).To(BeTemporally("==", visibilityChangedAt))
			}
		}

		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills SET explicit_latest_revision_id = $2 WHERE id = $1`, quotedSchema), skillID, publicWorkingID)
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: skillID, VersionNumber: 3, Semver: "0.1.2", Content: "# Published three",
			AuthorSubject: "owner", PublishedAt: now.Add(5 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(store.migrate(ctx)).To(Succeed())

		thirdPass, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(thirdPass).To(HaveLen(len(secondPass)+1), "the new predecessor publication must append exactly once")
		var versionThreeID string
		for _, revision := range thirdPass {
			if revision.LegacyReference == "skill-version:"+skillID+":3" {
				versionThreeID = revision.ID
			}
		}
		Expect(versionThreeID).NotTo(BeEmpty())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text, COALESCE(migration_managed_latest_revision_id::text, '') FROM %s.skills WHERE id = $1`, quotedSchema), skillID).Scan(&latestID, &managedLatestID)).To(Succeed())
		Expect(latestID).To(Equal(publicWorkingID), "migration must not overwrite a later explicit user pin")
		Expect(managedLatestID).To(BeEmpty(), "a divergent user pin permanently releases migration management")

		Expect(store.migrate(ctx)).To(Succeed())
		finalPass, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(finalPass).To(Equal(thirdPass))
	})

	It("retains a synthetic generation input mapping across migration reruns", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
		schema := "skills_synthetic_base_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})
		Expect(removeDurableRevisionTargetForFixture(ctx, store)).To(Succeed())

		skillID := uuid.NewString()
		_, err = store.UpsertSkill(ctx, SkillRecord{
			ID: skillID, Slug: "synthetic-base", Name: "Published", Type: "workflow",
			Version: "0.1.0", Visibility: "private", Content: "# Published",
			AuthorSubject: "owner", CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: skillID, VersionNumber: 1, Semver: "0.1.0", Content: "# Published",
			AuthorSubject: "owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		baseVersion := 1
		draftID := uuid.NewString()
		Expect(seedPredecessorDraft(ctx, store, predecessorDraftFixture{
			id: draftID, targetSkillID: skillID, ownerSubject: "owner",
			baseSkillVersionNumber: &baseVersion,
			working: predecessorSkillSnapshot{
				Slug: "synthetic-base", Name: "Initial", Type: "workflow", Content: "# Initial",
			},
			createdAt: now.Add(time.Minute),
		})).To(Succeed())
		generationInput := predecessorSkillSnapshot{
			Slug: "synthetic-base", Name: "Transient generation input", Type: "workflow",
			Content: "# Transient generation input",
		}
		Expect(updatePredecessorDraft(ctx, store, draftID, "owner", 1,
			generationInput, now.Add(2*time.Minute))).To(Succeed())
		generationID := uuid.NewString()
		Expect(seedPredecessorGeneration(ctx, store, generationID, draftID, "owner", 2,
			generationInput, "fixture", now.Add(3*time.Minute))).To(Succeed())
		Expect(updatePredecessorDraft(ctx, store, draftID, "owner", 2,
			predecessorSkillSnapshot{
				Slug: "synthetic-base", Name: "Later autosave", Type: "workflow",
				Content: "# Later autosave",
			}, now.Add(4*time.Minute))).To(Succeed())

		Expect(store.migrate(ctx)).To(Succeed())
		revisions, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		var syntheticID string
		for _, revision := range revisions {
			if revision.LegacyReference == "generation-input:"+generationID {
				syntheticID = revision.ID
				expectMigratedRevisionSnapshot(revision, SkillRevisionSnapshot{
					Name: generationInput.Name, Type: generationInput.Type,
					Content: generationInput.Content, Tags: []string{}, SourceSessionIDs: []string{},
				})
			}
		}
		Expect(syntheticID).NotTo(BeEmpty())
		var firstBaseID string
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT base_revision_id::text
			FROM %s.skill_generations WHERE id = $1`, quotedSchema), generationID).
			Scan(&firstBaseID)).To(Succeed())
		Expect(firstBaseID).To(Equal(syntheticID))

		Expect(store.migrate(ctx)).To(Succeed(), "the retained synthetic input is absent from current predecessor rows but remains the exact base")
		var rerunBaseID string
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT base_revision_id::text
			FROM %s.skill_generations WHERE id = $1`, quotedSchema), generationID).
			Scan(&rerunBaseID)).To(Succeed())
		Expect(rerunBaseID).To(Equal(firstBaseID))
		afterRetry, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(afterRetry).To(Equal(revisions))
	})

	It("anchors a generated result to the generation input across a concurrent checkpoint", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
		schema := "skills_generation_base_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})
		Expect(removeDurableRevisionTargetForFixture(ctx, store)).To(Succeed())

		skillID := uuid.NewString()
		_, err = store.UpsertSkill(ctx, SkillRecord{
			ID: skillID, Slug: "generation-base", Name: "Generation base", Type: "workflow",
			Version: "0.1.0", Visibility: "private", Content: "# Published",
			AuthorSubject: "owner", CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: skillID, VersionNumber: 1, Semver: "0.1.0", Content: "# Published",
			AuthorSubject: "owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		baseVersion := 1
		draftID := uuid.NewString()
		generationInput := predecessorSkillSnapshot{
			Slug: "generation-base", Name: "Generation input", Type: "workflow",
			Content: "# Generation input",
		}
		Expect(seedPredecessorDraft(ctx, store, predecessorDraftFixture{
			id: draftID, targetSkillID: skillID, ownerSubject: "owner",
			baseSkillVersionNumber: &baseVersion, working: generationInput,
			createdAt: now.Add(time.Minute),
		})).To(Succeed())
		generationID := uuid.NewString()
		Expect(seedPredecessorGeneration(ctx, store, generationID, draftID, "owner", 1,
			generationInput, "fixture", now.Add(2*time.Minute))).To(Succeed())

		manualSnapshot := predecessorSkillSnapshot{
			Slug: "generation-base", Name: "Concurrent checkpoint", Type: "workflow",
			Content: "# Concurrent checkpoint",
		}
		Expect(updatePredecessorDraft(ctx, store, draftID, "owner", 1,
			manualSnapshot, now.Add(3*time.Minute))).To(Succeed())
		Expect(checkpointPredecessorDraft(ctx, store, draftID, 2,
			manualSnapshot, now.Add(4*time.Minute))).To(Succeed())

		candidateID := uuid.NewString()
		generatedResult := predecessorSkillSnapshot{
			Slug: "generation-base", Name: "Generated result", Type: "workflow",
			Content: "# Generated result", IsAIGenerated: true,
		}
		Expect(seedPredecessorGenerationOutput(ctx, store, generationID, draftID,
			candidateID, 3, generatedResult, now.Add(11*time.Minute))).To(Succeed())

		Expect(store.migrate(ctx)).To(Succeed())
		revisions, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(revisions).To(HaveLen(4))
		byLegacy := make(map[string]migratedRevisionTestRecord, len(revisions))
		for _, revision := range revisions {
			byLegacy[revision.LegacyReference] = revision
		}
		inputRevision := byLegacy["draft-revision:"+draftID+":1"]
		manualRevision := byLegacy["draft-revision:"+draftID+":2"]
		resultRevision := byLegacy["draft-revision:"+draftID+":3"]
		Expect(inputRevision.ID).NotTo(BeEmpty())
		Expect(manualRevision.ID).NotTo(BeEmpty())
		Expect(resultRevision.ID).NotTo(BeEmpty())
		Expect(resultRevision.GenerationID).To(Equal(generationID))
		Expect(resultRevision.BasedOnRevisionID).To(Equal(inputRevision.ID))
		Expect(resultRevision.BasedOnRevisionID).NotTo(Equal(manualRevision.ID))
		var generationBaseID, generationResultID string
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT base_revision_id::text,
			result_revision_id::text FROM %s.skill_generations WHERE id = $1`, quotedSchema),
			generationID).Scan(&generationBaseID, &generationResultID)).To(Succeed())
		Expect(generationBaseID).To(Equal(inputRevision.ID))
		Expect(generationResultID).To(Equal(resultRevision.ID))
		Expect(resultRevision.BasedOnRevisionID).To(Equal(generationBaseID))
	})

	It("distinguishes repeated draft working saves that revert to identical content", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
		schema := "skills_working_occurrence_" + uuid.NewString()[:8]
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})
		Expect(removeDurableRevisionTargetForFixture(ctx, store)).To(Succeed())

		skillID := uuid.NewString()
		_, err = store.UpsertSkill(ctx, SkillRecord{
			ID: skillID, Slug: "working-occurrence", Name: "Working occurrence", Type: "workflow",
			Version: "0.1.0", Visibility: "private", Content: "# Published",
			AuthorSubject: "owner", CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: skillID, VersionNumber: 1, Semver: "0.1.0", Content: "# Published",
			AuthorSubject: "owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		baseVersion := 1
		draftID := uuid.NewString()
		Expect(seedPredecessorDraft(ctx, store, predecessorDraftFixture{
			id: draftID, targetSkillID: skillID, ownerSubject: "owner",
			baseSkillVersionNumber: &baseVersion,
			working: predecessorSkillSnapshot{
				Slug: "working-occurrence", Name: "Initial", Type: "workflow", Content: "# Initial",
			},
			createdAt: now.Add(time.Minute),
		})).To(Succeed())

		snapshotA := predecessorSkillSnapshot{
			Slug: "working-occurrence", Name: "Saved A", Type: "workflow", Content: "# A",
		}
		Expect(updatePredecessorDraft(ctx, store, draftID, "owner", 1,
			snapshotA, now.Add(2*time.Minute))).To(Succeed())
		Expect(store.migrate(ctx)).To(Succeed())

		Expect(updatePredecessorDraft(ctx, store, draftID, "owner", 2,
			predecessorSkillSnapshot{
				Slug: "working-occurrence", Name: "Saved B", Type: "workflow", Content: "# B",
			}, now.Add(3*time.Minute))).To(Succeed())
		Expect(store.migrate(ctx)).To(Succeed())

		Expect(updatePredecessorDraft(ctx, store, draftID, "owner", 3,
			snapshotA, now.Add(4*time.Minute))).To(Succeed())
		Expect(store.migrate(ctx)).To(Succeed())

		revisions, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		working := make([]migratedRevisionTestRecord, 0, 3)
		reverted := make([]migratedRevisionTestRecord, 0, 2)
		prefix := "draft-working:" + draftID + ":"
		for _, revision := range revisions {
			if len(revision.LegacyReference) < len(prefix) || revision.LegacyReference[:len(prefix)] != prefix {
				continue
			}
			working = append(working, revision)
			if revision.Snapshot.Content == snapshotA.Content {
				reverted = append(reverted, revision)
			}
		}
		Expect(working).To(HaveLen(3))
		Expect(reverted).To(HaveLen(2))
		Expect(reverted[0].LegacyReference).NotTo(Equal(reverted[1].LegacyReference))
		Expect(reverted[0].ID).NotTo(Equal(reverted[1].ID))
		Expect([]int64{reverted[0].CreatedAt.UnixNano(), reverted[1].CreatedAt.UnixNano()}).To(ConsistOf(
			now.Add(2*time.Minute).UnixNano(), now.Add(4*time.Minute).UnixNano(),
		))

		Expect(store.migrate(ctx)).To(Succeed())
		afterRetry, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(afterRetry).To(Equal(revisions), "the same draft row occurrence must retain its deterministic mapping")
	})

	It("distinguishes repeated current-main heads that revert to identical content", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
		schema := "skills_head_occurrence_" + uuid.NewString()[:8]
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})
		Expect(removeDurableRevisionTargetForFixture(ctx, store)).To(Succeed())

		skillID := uuid.NewString()
		head := SkillRecord{
			ID: skillID, Slug: "head-occurrence", Name: "Published", Type: "workflow",
			Version: "0.1.0", Visibility: "private", Content: "# Published",
			AuthorSubject: "owner", CreatedAt: now, UpdatedAt: now,
		}
		_, err = store.UpsertSkill(ctx, head)
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: skillID, VersionNumber: 1, Semver: "0.1.0", Content: head.Content,
			AuthorSubject: "owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		head.Name, head.Content, head.UpdatedAt = "Saved A", "# A", now.Add(time.Minute)
		_, err = store.UpsertSkill(ctx, head)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.migrate(ctx)).To(Succeed())

		head.Name, head.Content, head.UpdatedAt = "Saved B", "# B", now.Add(2*time.Minute)
		_, err = store.UpsertSkill(ctx, head)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.migrate(ctx)).To(Succeed())

		head.Name, head.Content, head.UpdatedAt = "Saved A", "# A", now.Add(3*time.Minute)
		_, err = store.UpsertSkill(ctx, head)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.migrate(ctx)).To(Succeed())

		revisions, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(revisions).To(HaveLen(4))
		working := make([]migratedRevisionTestRecord, 0, 3)
		reverted := make([]migratedRevisionTestRecord, 0, 2)
		prefix := "skill-working:" + skillID + ":"
		for _, revision := range revisions {
			if !strings.HasPrefix(revision.LegacyReference, prefix) {
				continue
			}
			working = append(working, revision)
			if revision.Snapshot.Content == "# A" {
				reverted = append(reverted, revision)
			}
		}
		Expect(working).To(HaveLen(3))
		Expect(reverted).To(HaveLen(2))
		Expect(reverted[0].LegacyReference).NotTo(Equal(reverted[1].LegacyReference))
		Expect(reverted[0].ID).NotTo(Equal(reverted[1].ID))
		Expect([]int64{reverted[0].CreatedAt.UnixNano(), reverted[1].CreatedAt.UnixNano()}).To(ConsistOf(
			now.Add(time.Minute).UnixNano(), now.Add(3*time.Minute).UnixNano(),
		))

		Expect(store.migrate(ctx)).To(Succeed())
		afterRetry, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(afterRetry).To(Equal(revisions))
	})

	It("freezes an existing source revision mapping when the source publishes again", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
		schema := "skills_frozen_source_" + uuid.NewString()[:8]
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})
		Expect(removeDurableRevisionTargetForFixture(ctx, store)).To(Succeed())

		parentID := uuid.NewString()
		_, err = store.UpsertSkill(ctx, SkillRecord{
			ID: parentID, Slug: "frozen-parent", Name: "Frozen parent", Type: "workflow",
			Version: "0.1.0", Visibility: "private", Content: "# Parent one",
			AuthorSubject: "owner", CreatedAt: now, UpdatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: parentID, VersionNumber: 1, Semver: "0.1.0", Content: "# Parent one",
			AuthorSubject: "owner", PublishedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		childID := uuid.NewString()
		_, err = store.UpsertSkill(ctx, SkillRecord{
			ID: childID, Slug: "frozen-child", Name: "Frozen child", Type: "workflow",
			Version: "0.1.0", Visibility: "private", Content: "# Child",
			ParentID: parentID, AuthorSubject: "owner", CreatedAt: now.Add(time.Minute),
			UpdatedAt: now.Add(time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: childID, VersionNumber: 1, Semver: "0.1.0", Content: "# Child",
			AuthorSubject: "owner", PublishedAt: now.Add(time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(store.migrate(ctx)).To(Succeed())
		parentRevisions, err := migratedRevisionRecords(ctx, store, parentID)
		Expect(err).NotTo(HaveOccurred())
		Expect(parentRevisions).To(HaveLen(1))
		childRevisions, err := migratedRevisionRecords(ctx, store, childID)
		Expect(err).NotTo(HaveOccurred())
		Expect(childRevisions).To(HaveLen(1))
		childRevisionID := childRevisions[0].ID
		frozenSourceID := childRevisions[0].SourceRevisionID
		Expect(frozenSourceID).To(Equal(parentRevisions[0].ID))

		_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
			SkillID: parentID, VersionNumber: 2, Semver: "0.2.0", Content: "# Parent two",
			AuthorSubject: "owner", PublishedAt: now.Add(2 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(store.migrate(ctx)).To(Succeed())

		parentRevisions, err = migratedRevisionRecords(ctx, store, parentID)
		Expect(err).NotTo(HaveOccurred())
		Expect(parentRevisions).To(HaveLen(2))
		childRevisions, err = migratedRevisionRecords(ctx, store, childID)
		Expect(err).NotTo(HaveOccurred())
		Expect(childRevisions).To(HaveLen(1))
		Expect(childRevisions[0].ID).To(Equal(childRevisionID))
		Expect(childRevisions[0].SourceRevisionID).To(Equal(frozenSourceID))
		Expect(childRevisions[0].SourceRevisionID).NotTo(Equal(parentRevisions[1].ID))

		Expect(store.migrate(ctx)).To(Succeed())
		childAfterRetry, err := migratedRevisionRecords(ctx, store, childID)
		Expect(err).NotTo(HaveOccurred())
		Expect(childAfterRetry).To(Equal(childRevisions))
	})

	It("recovers an unversioned predecessor skill created after the initial migration", func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
		schema := "skills_late_predecessor_" + uuid.NewString()[:8]
		quotedSchema := quoteIdentifier(schema)
		store, err := OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = DropPrivateSchema(context.Background(), store.pool, schema)
			store.Close()
		})

		emptyID := uuid.NewString()
		_, err = store.ResolveSkill(ctx, ResolveSkillInput{
			ID: emptyID, Slug: "intentional-empty", CreatorSubject: "owner", CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		skillID := uuid.NewString()
		lateSnapshot := SkillRevisionSnapshot{
			Name: "Late predecessor", Description: "created through the retained writer",
			Type: "domain-knowledge", Tags: []string{"late", "complete"},
			Content: "# Late predecessor", IsAIGenerated: true,
			SourceSessionIDs: []string{"late-session"},
		}
		_, err = store.UpsertSkill(ctx, SkillRecord{
			ID: skillID, Slug: "late-predecessor", Name: lateSnapshot.Name,
			Description: lateSnapshot.Description, Type: lateSnapshot.Type,
			Version: "0.1.0", Visibility: "private", Tags: lateSnapshot.Tags,
			Content: lateSnapshot.Content, IsAIGenerated: lateSnapshot.IsAIGenerated,
			GeneratedFromSessionIDs: lateSnapshot.SourceSessionIDs,
			AuthorSubject:           "owner", CreatedAt: now.Add(time.Minute), UpdatedAt: now.Add(2 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		var predecessorVersions, predecessorRevisions int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_versions
			WHERE skill_id = $1`, quotedSchema), skillID).Scan(&predecessorVersions)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions
			WHERE skill_id = $1`, quotedSchema), skillID).Scan(&predecessorRevisions)).To(Succeed())
		Expect([]int{predecessorVersions, predecessorRevisions}).To(Equal([]int{0, 0}))

		Expect(store.migrate(ctx)).To(Succeed())
		revisions, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(revisions).To(HaveLen(1))
		expectMigratedRevisionSnapshot(revisions[0], lateSnapshot)
		Expect(revisions[0].LegacyReference).To(Equal("skill-version:" + skillID + ":1"))
		Expect(revisions[0].ChangeNote).To(Equal("Recovered during cassette migration"))
		Expect(revisions[0].CreatorSubject).To(Equal("owner"))
		Expect(revisions[0].CreatedAt).To(BeTemporally("==", now.Add(2*time.Minute)))
		Expect(revisions[0].IsPublic).To(BeTrue())
		var latestID, managedLatestID, createdBySubject string
		var currentVersionNumber, nextSequenceNumber int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT explicit_latest_revision_id::text,
			migration_managed_latest_revision_id::text, created_by_subject,
			current_version_number, next_sequence_number FROM %s.skills WHERE id = $1`, quotedSchema),
			skillID).Scan(&latestID, &managedLatestID, &createdBySubject,
			&currentVersionNumber, &nextSequenceNumber)).To(Succeed())
		Expect(latestID).To(Equal(revisions[0].ID))
		Expect(managedLatestID).To(Equal(revisions[0].ID))
		Expect(createdBySubject).To(Equal("owner"))
		Expect(currentVersionNumber).To(Equal(1))
		Expect(nextSequenceNumber).To(Equal(2))

		emptyRevisions, err := migratedRevisionRecords(ctx, store, emptyID)
		Expect(err).NotTo(HaveOccurred())
		Expect(emptyRevisions).To(BeEmpty(), "a revision-native empty identity must remain hidden")
		var emptyLatest string
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(explicit_latest_revision_id::text, '')
			FROM %s.skills WHERE id = $1`, quotedSchema), emptyID).Scan(&emptyLatest)).To(Succeed())
		Expect(emptyLatest).To(BeEmpty())

		Expect(store.migrate(ctx)).To(Succeed())
		afterRetry, err := migratedRevisionRecords(ctx, store, skillID)
		Expect(err).NotTo(HaveOccurred())
		Expect(afterRetry).To(Equal(revisions))
	})
})

func makeGenerationDue(ctx context.Context, store *PostgresStore, generationID string) error {
	_, err := store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations
		SET next_attempt_at = clock_timestamp() - interval '1 second'
		WHERE id = $1`, quoteIdentifier(store.schema)), generationID)
	return err
}

type migratedRevisionTestRecord struct {
	ID                string
	SequenceNumber    int
	Version           string
	CreatorSubject    string
	BasedOnRevisionID string
	SourceRevisionID  string
	Origin            RevisionOrigin
	Snapshot          SkillRevisionSnapshot
	ContentSHA256     string
	ChangeNote        string
	GenerationID      string
	IdempotencyKey    string
	LegacyReference   string
	CreatedAt         time.Time
	IsPublic          bool
	ChangedBySubject  string
	ChangedAt         time.Time
}

func migratedRevisionRecords(ctx context.Context, store *PostgresStore, skillID string) ([]migratedRevisionTestRecord, error) {
	rows, err := store.pool.Query(ctx, fmt.Sprintf(`
		SELECT revision.id::text, revision.sequence_number, revision.version,
		       revision.creator_subject, COALESCE(revision.based_on_revision_id::text, ''),
		       COALESCE(revision.source_revision_id::text, ''), revision.origin,
		       revision.name, revision.description, revision.type, revision.tags,
		       revision.content, revision.is_ai_generated, revision.source_session_ids,
		       revision.content_sha256, revision.change_note,
		       COALESCE(revision.generation_id::text, ''), revision.idempotency_key,
		       COALESCE(revision.legacy_reference, ''), revision.created_at,
		       visibility.is_public, visibility.changed_by_subject, visibility.changed_at
		FROM %s.skill_revisions revision
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
		WHERE revision.skill_id = $1
		ORDER BY revision.sequence_number, revision.id`, quoteIdentifier(store.schema), quoteIdentifier(store.schema)), skillID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	revisions := make([]migratedRevisionTestRecord, 0)
	for rows.Next() {
		var revision migratedRevisionTestRecord
		if err := rows.Scan(
			&revision.ID, &revision.SequenceNumber, &revision.Version, &revision.CreatorSubject,
			&revision.BasedOnRevisionID, &revision.SourceRevisionID, &revision.Origin,
			&revision.Snapshot.Name, &revision.Snapshot.Description, &revision.Snapshot.Type,
			&revision.Snapshot.Tags, &revision.Snapshot.Content, &revision.Snapshot.IsAIGenerated,
			&revision.Snapshot.SourceSessionIDs, &revision.ContentSHA256, &revision.ChangeNote,
			&revision.GenerationID, &revision.IdempotencyKey, &revision.LegacyReference,
			&revision.CreatedAt, &revision.IsPublic, &revision.ChangedBySubject, &revision.ChangedAt,
		); err != nil {
			return nil, err
		}
		revisions = append(revisions, revision)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return revisions, nil
}

func expectMigratedRevisionSnapshot(revision migratedRevisionTestRecord, expected SkillRevisionSnapshot) {
	Expect(revision.Snapshot).To(Equal(expected))
	Expect(revision.ContentSHA256).To(Equal(canonicalRevisionDigestForTest(expected)))
}

type predecessorDraftFixture struct {
	id                     string
	targetSkillID          string
	ownerSubject           string
	baseSkillVersionNumber *int
	working                predecessorSkillSnapshot
	authorContext          string
	selectedSessionIDs     []string
	createdAt              time.Time
}

func seedPredecessorDraft(ctx context.Context, store *PostgresStore, fixture predecessorDraftFixture) error {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schema := quoteIdentifier(store.schema)
	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_drafts (
		id, target_skill_id, owner_subject, status, base_skill_version_number,
		lock_version, working_revision_number, working_slug, working_name,
		working_description, working_type, working_tags, working_content,
		working_is_ai_generated, working_source_session_ids, working_parent_id,
		author_context, created_at, updated_at
	) VALUES (
		$1, NULLIF($2, '')::uuid, $3, 'open', $4, 1, 1, $5, $6, $7, $8,
		$9, $10, $11, $12, NULLIF($13, '')::uuid, $14, $15, $15
	)`, schema), fixture.id, fixture.targetSkillID, fixture.ownerSubject,
		fixture.baseSkillVersionNumber, fixture.working.Slug, fixture.working.Name,
		fixture.working.Description, fixture.working.Type, nonNilStrings(fixture.working.Tags),
		fixture.working.Content, fixture.working.IsAIGenerated,
		nonNilStrings(fixture.working.SourceSessionIDs), fixture.working.ParentID,
		fixture.authorContext, fixture.createdAt)
	if err != nil {
		return err
	}
	for ordinal, sessionID := range fixture.selectedSessionIDs {
		if _, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.draft_sessions (
			draft_id, session_id, ordinal
		) VALUES ($1, $2, $3)`, schema), fixture.id, sessionID, ordinal); err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.draft_revisions (
		draft_id, revision_number, origin, idempotency_key, slug, name,
		description, type, tags, content, is_ai_generated, source_session_ids,
		parent_id, content_sha256, created_at
	) VALUES (
		$1, 1, 'initial', '', $2, $3, $4, $5, $6, $7, $8, $9,
		NULLIF($10, '')::uuid, $11, $12
	)`, schema), fixture.id, fixture.working.Slug, fixture.working.Name,
		fixture.working.Description, fixture.working.Type, nonNilStrings(fixture.working.Tags),
		fixture.working.Content, fixture.working.IsAIGenerated,
		nonNilStrings(fixture.working.SourceSessionIDs), fixture.working.ParentID,
		predecessorSkillSnapshotSHA256(fixture.working), fixture.createdAt)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func updatePredecessorDraft(
	ctx context.Context,
	store *PostgresStore,
	draftID, ownerSubject string,
	expectedLockVersion int64,
	working predecessorSkillSnapshot,
	updatedAt time.Time,
) error {
	schema := quoteIdentifier(store.schema)
	tag, err := store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_drafts SET
		working_slug = $4, working_name = $5, working_description = $6,
		working_type = $7, working_tags = $8, working_content = $9,
		working_is_ai_generated = $10, working_source_session_ids = $11,
		working_parent_id = NULLIF($12, '')::uuid,
		lock_version = lock_version + 1, working_revision_number = NULL,
		updated_at = $13
		WHERE id = $1 AND owner_subject = $2 AND lock_version = $3`, schema),
		draftID, ownerSubject, expectedLockVersion, working.Slug, working.Name,
		working.Description, working.Type, nonNilStrings(working.Tags), working.Content,
		working.IsAIGenerated, nonNilStrings(working.SourceSessionIDs), working.ParentID, updatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("predecessor draft %s did not match lock %d", draftID, expectedLockVersion)
	}
	return nil
}

func checkpointPredecessorDraft(
	ctx context.Context,
	store *PostgresStore,
	draftID string,
	revisionNumber int,
	working predecessorSkillSnapshot,
	createdAt time.Time,
) error {
	schema := quoteIdentifier(store.schema)
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.draft_revisions (
		draft_id, revision_number, origin, idempotency_key, slug, name,
		description, type, tags, content, is_ai_generated, source_session_ids,
		parent_id, content_sha256, created_at
	) VALUES (
		$1, $2, 'manual', '', $3, $4, $5, $6, $7, $8, $9, $10,
		NULLIF($11, '')::uuid, $12, $13
	)`, schema), draftID, revisionNumber, working.Slug, working.Name,
		working.Description, working.Type, nonNilStrings(working.Tags), working.Content,
		working.IsAIGenerated, nonNilStrings(working.SourceSessionIDs), working.ParentID,
		predecessorSkillSnapshotSHA256(working), createdAt)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_drafts SET
		working_revision_number = $2, updated_at = $3 WHERE id = $1`, schema),
		draftID, revisionNumber, createdAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func seedPredecessorGeneration(
	ctx context.Context,
	store *PostgresStore,
	generationID, draftID, owner string,
	startingLockVersion int64,
	snapshot predecessorSkillSnapshot,
	evaluatorProfile string,
	createdAt time.Time,
) error {
	schema := quoteIdentifier(store.schema)
	var parentID any
	if snapshot.ParentID != "" {
		parentID = snapshot.ParentID
	}
	_, err := store.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_generations (
		id, draft_id, owner_subject, status, starting_lock_version,
		input_slug, input_name, input_description, input_type, input_tags,
		input_content, input_is_ai_generated, input_source_session_ids, input_parent_id,
		author_context, selected_session_ids, evaluator_profile,
		evaluator_profile_version, evaluation_criteria, error_code, error_message,
		claim_owner, attempt_count, next_attempt_at, created_at, updated_at
	) VALUES (
		$1, $2, $3, 'queued', $4,
		$5, $6, $7, $8, $9,
		$10, $11, $12, $13,
		'', '{}', $14, '', '[]'::jsonb, '', '', '', 0, $15, $15, $15
	)`, schema), generationID, draftID, owner, startingLockVersion,
		snapshot.Slug, snapshot.Name, snapshot.Description, snapshot.Type, nonNilStrings(snapshot.Tags),
		snapshot.Content, snapshot.IsAIGenerated, nonNilStrings(snapshot.SourceSessionIDs),
		parentID, evaluatorProfile, createdAt)
	return err
}

func seedPredecessorGenerationOutput(
	ctx context.Context,
	store *PostgresStore,
	generationID, draftID, candidateID string,
	revisionNumber int,
	snapshot predecessorSkillSnapshot,
	createdAt time.Time,
) error {
	schema := quoteIdentifier(store.schema)
	_, err := store.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.generation_candidates (
			id, generation_id, ordinal, kind, source_session_ids, slug, name,
			description, type, tags, content, is_ai_generated, insights,
			bundle_sha256, created_at
		) VALUES (
			$1, $2, 0, 'context', $3, $4, $5, $6, $7, $8, $9, $10,
			'[]'::jsonb, 'ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff', $11
		);
		INSERT INTO %s.draft_revisions (
			draft_id, revision_number, origin, idempotency_key, generation_id,
			slug, name, description, type, tags, content, is_ai_generated,
			source_session_ids, content_sha256, created_at
		) VALUES (
			$12, $13, 'generation', 'generation:' || $2::text, $2,
			$4, $5, $6, $7, $8, $9, $10, $3, $14, $11
		);
		UPDATE %s.skill_generations SET
			status = 'awaiting_input', winner_candidate_id = $1,
			proposed_candidate_id = $1, proposed_revision_number = $13,
			updated_at = $11, completed_at = $11
		WHERE id = $2`, schema, schema, schema), pgx.QueryExecModeSimpleProtocol,
		candidateID, generationID, nonNilStrings(snapshot.SourceSessionIDs), snapshot.Slug,
		snapshot.Name, snapshot.Description, snapshot.Type, nonNilStrings(snapshot.Tags),
		snapshot.Content, snapshot.IsAIGenerated, createdAt, draftID, revisionNumber,
		strings.Repeat("b", 64))
	return err
}

func removeDurableRevisionTargetForFixture(ctx context.Context, store *PostgresStore) error {
	schema := quoteIdentifier(store.schema)
	_, err := store.pool.Exec(ctx, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s.skill_revision_visibility CASCADE;
		DROP TABLE IF EXISTS %s.skill_revisions CASCADE;
		DROP TABLE IF EXISTS %s.skill_revision_migration_sources CASCADE;
		DROP INDEX IF EXISTS %s.skills_slug_key;
		ALTER TABLE %s.skills
			DROP COLUMN IF EXISTS explicit_latest_revision_id CASCADE,
			DROP COLUMN IF EXISTS next_sequence_number CASCADE,
			DROP COLUMN IF EXISTS created_by_subject CASCADE,
			DROP COLUMN IF EXISTS migration_alias_of_skill_id CASCADE,
			DROP COLUMN IF EXISTS migration_managed_latest_revision_id CASCADE;
		ALTER TABLE %s.skill_generations
			DROP CONSTRAINT IF EXISTS skill_generations_same_generation_winner_candidate_fkey,
			DROP CONSTRAINT IF EXISTS skill_generations_same_generation_result_candidate_fkey,
			DROP COLUMN IF EXISTS skill_id CASCADE,
			DROP COLUMN IF EXISTS creator_subject CASCADE,
			DROP COLUMN IF EXISTS base_revision_id CASCADE,
			DROP COLUMN IF EXISTS result_candidate_id CASCADE,
			DROP COLUMN IF EXISTS result_revision_id CASCADE,
			DROP CONSTRAINT IF EXISTS skill_generations_status_check;
		ALTER TABLE %s.generation_sessions
			DROP CONSTRAINT IF EXISTS generation_sessions_same_generation_candidate_fkey;
		ALTER TABLE %s.candidate_evaluations
			DROP CONSTRAINT IF EXISTS candidate_evaluations_same_generation_candidate_fkey,
			DROP CONSTRAINT IF EXISTS candidate_evaluations_candidate_fkey;
		ALTER TABLE %s.candidate_evaluations
			ADD CONSTRAINT candidate_evaluations_candidate_fkey
			FOREIGN KEY (candidate_id) REFERENCES %s.generation_candidates(id) ON DELETE CASCADE;
		ALTER TABLE %s.generation_diagnostics
			DROP CONSTRAINT IF EXISTS generation_diagnostics_same_generation_candidate_fkey;
		ALTER TABLE %s.skill_generations
			ADD CONSTRAINT skill_generations_status_check CHECK (status IN (
				'queued', 'generating_candidates', 'evaluating_candidates',
				'synthesizing', 'awaiting_input', 'completed', 'canceled', 'failed'
			));
	`, schema, schema, schema, schema, schema, schema, schema, schema, schema, schema, schema, schema))
	return err
}
