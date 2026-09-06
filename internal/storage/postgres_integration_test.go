package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPostgresConditionalPublish(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	schema := "skills_cas_" + uuid.NewString()[:8]
	store, err := OpenPostgresStore(ctx, dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = store.pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", quoteIdentifier(schema)))
		store.Close()
	}()

	now := time.Now().UTC()
	id := uuid.NewString()
	_, err = store.UpsertSkill(ctx, SkillRecord{
		ID: id, Slug: "cas", Name: "CAS", Content: "# original",
		Type: "workflow", Version: "0.1.0", Visibility: "private",
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	expected := "# original"
	_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
		SkillID: id, VersionNumber: 1, Semver: "0.1.0", Content: "# revised",
		ExpectedContent: &expected, PublishedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := store.ListSkillVersions(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0].ExpectedContent == nil || *versions[0].ExpectedContent != expected {
		t.Fatalf("stored expected content = %#v, want %q", versions, expected)
	}

	_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
		SkillID: id, VersionNumber: 2, Semver: "0.1.1", Content: "# overwrite",
		ExpectedContent: &expected, PublishedAt: now,
	})
	if !errors.Is(err, ErrSkillChanged) {
		t.Fatalf("stale conditional publish error = %v, want %v", err, ErrSkillChanged)
	}

	_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
		SkillID: uuid.NewString(), VersionNumber: 1, Semver: "0.1.0", Content: "# orphan",
		PublishedAt: now,
	})
	if !errors.Is(err, ErrSkillChanged) {
		t.Fatalf("missing-skill publish error = %v, want %v", err, ErrSkillChanged)
	}
}

func TestPostgresPublishedCreationRollsBack(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	schema := "skills_initial_" + uuid.NewString()[:8]
	store, err := OpenPostgresStore(ctx, dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = store.pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", quoteIdentifier(schema)))
		store.Close()
	}()

	now := time.Now().UTC()
	id := uuid.NewString()
	_, err = store.UpsertSkill(ctx, SkillRecord{
		ID: id, Slug: "legacy", Name: "Legacy", Content: "# Legacy",
		Type: "workflow", Version: "0.1.0", Visibility: "private",
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.CreatePublishedSkill(ctx, SkillRecord{
		ID: id, Slug: "duplicate", Name: "Duplicate", CreatedAt: now, UpdatedAt: now,
	}, SkillVersionRecord{
		SkillID: id, VersionNumber: 1, Semver: "0.1.0", Content: "# Duplicate", PublishedAt: now,
	})
	if err == nil {
		t.Fatal("duplicate skill creation succeeded")
	}
	versions, err := store.ListSkillVersions(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 0 {
		t.Fatalf("rolled-back initial versions = %d, want 0", len(versions))
	}

	created := uuid.NewString()
	skill, err := store.CreatePublishedSkill(ctx, SkillRecord{
		ID: created, Slug: "created", Name: "Created", Type: "workflow", Visibility: "private",
		CreatedAt: now, UpdatedAt: now,
	}, SkillVersionRecord{
		SkillID: created, VersionNumber: 1, Semver: "0.1.0", Content: "# Created", PublishedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if skill.Content != "# Created" || skill.Version != "0.1.0" {
		t.Fatalf("published skill head = %#v", skill)
	}
	versions, err = store.ListSkillVersions(ctx, created)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0].Content != "# Created" {
		t.Fatalf("initial versions = %#v", versions)
	}
}

var _ = Describe("Postgres durable revision identities", func() {
	var (
		ctx    context.Context
		store  *PostgresStore
		schema string
		now    time.Time
	)

	BeforeEach(func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx = context.Background()
		schema = "skills_revisions_" + uuid.NewString()[:8]
		var err error
		store, err = OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		now = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
		DeferCleanup(func() {
			_, _ = store.pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", quoteIdentifier(schema)))
			store.Close()
		})
	})

	It("stores_resolve_one_hidden_skill_per_slug", func() {
		owner, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "  Incident-Response  ", CreatorSubject: "creator-a", CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(owner).NotTo(BeNil())

		privateRevision, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: owner.ID, CreatorSubject: "creator-a", Origin: RevisionOriginManual,
			Snapshot: SkillRevisionSnapshot{
				Name: "Secret response process", Description: "creator-a private description", Type: "workflow",
				Tags: []string{"creator-a-private"}, Content: "# creator-a private content",
				IsAIGenerated: true, SourceSessionIDs: []string{"creator-a-private-session"},
			},
			ChangeNote: "creator-a private note", IdempotencyKey: "private-before-conflict", CreatedAt: now.Add(time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(privateRevision).NotTo(BeNil())

		var persistedPrivate bool
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT NOT v.is_public
			FROM %s.skill_revisions r
			JOIN %s.skill_revision_visibility v ON v.revision_id = r.id
			WHERE r.id = $1 AND r.creator_subject = 'creator-a' AND r.content = '# creator-a private content'`,
			quoteIdentifier(schema), quoteIdentifier(schema)), privateRevision.ID).Scan(&persistedPrivate)).To(Succeed())
		Expect(persistedPrivate).To(BeTrue(), "the conflicting resolve must happen after private content exists")

		resolved, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "incident-response", CreatorSubject: "creator-b", CreatedAt: now.Add(2 * time.Second),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(resolved).NotTo(BeNil())
		Expect(resolved.ID).To(Equal(owner.ID))
		Expect(resolved.Slug).To(Equal("incident-response"))
		Expect(resolved.ExplicitLatestRevisionID).To(BeEmpty())
		Expect(resolved.Name).To(BeEmpty())
		Expect(resolved.Description).To(BeEmpty())
		Expect(resolved.Type).To(BeEmpty())
		Expect(resolved.Tags).To(BeEmpty())
		Expect(resolved.Content).To(BeEmpty())
		Expect(resolved.IsAIGenerated).To(BeFalse())
		Expect(resolved.GeneratedFromSessionIDs).To(BeEmpty())
		Expect(resolved.ParentID).To(BeEmpty())

		var skillCount, revisionCount int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skills WHERE slug = 'incident-response'`, quoteIdentifier(schema))).Scan(&skillCount)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions WHERE skill_id = $1`, quoteIdentifier(schema)), owner.ID).Scan(&revisionCount)).To(Succeed())
		Expect(skillCount).To(Equal(1))
		Expect(revisionCount).To(Equal(1), "resolving the shared slug must neither copy nor disclose private content")
	})

	It("skill_lists_hide_zero_revision_containers", func() {
		identity, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "empty-container", CreatorSubject: "creator-a", CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(identity).NotTo(BeNil())

		listed, err := store.ListSkills(ctx, SkillListOpts{})
		Expect(err).NotTo(HaveOccurred())
		Expect(listed).To(BeEmpty(), "ordinary lists must omit stable identities with no saved revision")

		var count int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skills WHERE id = $1`, quoteIdentifier(schema)), identity.ID).Scan(&count)).To(Succeed())
		Expect(count).To(Equal(1), "the hidden identity must remain durably addressable")
	})

	It("postgres_predecessor_list_and_count_treat_search_metacharacters_literally", func() {
		records := []SkillRecord{
			{
				ID: uuid.NewString(), Slug: "literal-percent", Name: "Literal%Marker",
				Type: "workflow", AuthorSubject: "creator-a", CreatedAt: now, UpdatedAt: now,
			},
			{
				ID: uuid.NewString(), Slug: "literal-underscore", Name: "Underscore",
				Description: "Literal_Marker", Type: "workflow", AuthorSubject: "creator-a",
				CreatedAt: now.Add(time.Minute), UpdatedAt: now.Add(time.Minute),
			},
			{
				ID: uuid.NewString(), Slug: "literal-escape", Name: "Escape",
				Type: "workflow", Tags: []string{`literal\marker`}, AuthorSubject: "creator-a",
				CreatedAt: now.Add(2 * time.Minute), UpdatedAt: now.Add(2 * time.Minute),
			},
			{
				ID: uuid.NewString(), Slug: "wildcard-impostor", Name: "LiteralXMarker",
				Type: "workflow", AuthorSubject: "creator-a",
				CreatedAt: now.Add(3 * time.Minute), UpdatedAt: now.Add(3 * time.Minute),
			},
		}
		for _, record := range records {
			_, err := store.UpsertSkill(ctx, record)
			Expect(err).NotTo(HaveOccurred())
		}

		for _, searchCase := range []struct {
			query      string
			expectedID string
		}{
			{query: "LITERAL%MARKER", expectedID: records[0].ID},
			{query: "LITERAL_MARKER", expectedID: records[1].ID},
			{query: `LITERAL\MARKER`, expectedID: records[2].ID},
		} {
			listed, err := store.ListSkills(ctx, SkillListOpts{Query: searchCase.query})
			Expect(err).NotTo(HaveOccurred(), "literal list query %q", searchCase.query)
			Expect(listed).To(HaveLen(1), "literal list query %q", searchCase.query)
			Expect(listed[0].ID).To(Equal(searchCase.expectedID))

			counts, err := store.CountSkills(ctx, SkillCountOpts{
				Query: searchCase.query, Author: "creator-a",
			})
			Expect(err).NotTo(HaveOccurred(), "literal count query %q", searchCase.query)
			Expect(counts).To(Equal(SkillCounts{Total: 1, Mine: 1}))
		}
	})

	It("postgres_appends_uuid_revision_with_atomic_sequence", func() {
		skill, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "atomic-append", CreatorSubject: "creator-a", CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		first, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: "creator-a", Origin: RevisionOriginManual,
			Snapshot: SkillRevisionSnapshot{
				Name: "Atomic append", Description: "One complete transaction.", Type: "workflow",
				Tags: []string{"atomic", "revision"}, Content: "# First", SourceSessionIDs: []string{"session-a"},
			},
			ChangeNote: "first save", IdempotencyKey: "append:first", CreatedAt: now.Add(time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(first.ID).NotTo(Equal(skill.ID))
		_, err = uuid.Parse(first.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(first.SequenceNumber).To(Equal(1))
		Expect(first.Version).To(Equal("1"))
		Expect(first.ContentSHA256).To(HaveLen(64))

		failedID := uuid.NewString()
		_, err = store.AppendRevision(ctx, AppendRevisionInput{
			ID: failedID, SkillID: skill.ID, CreatorSubject: "creator-a",
			BasedOnRevisionID: uuid.NewString(), Origin: RevisionOriginManual,
			Snapshot:  SkillRevisionSnapshot{Name: "Invalid lineage", Type: "workflow", Content: "# Invalid"},
			CreatedAt: now.Add(2 * time.Minute),
		})
		Expect(errors.Is(err, ErrRevisionLineageInvalid)).To(BeTrue())

		second, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: "creator-a",
			BasedOnRevisionID: first.ID, Origin: RevisionOriginManual,
			Snapshot:  SkillRevisionSnapshot{Name: "Second", Type: "workflow", Content: "# Second"},
			CreatedAt: now.Add(3 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(second.SequenceNumber).To(Equal(2), "a rolled-back append must not consume a sequence")
		Expect(second.Version).To(Equal("2"))

		var (
			persistedID      string
			persistedSkillID string
			sequence         int
			version          string
			creator          string
			isPublic         bool
			nextSequence     int
		)
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT r.id::text, r.skill_id::text, r.sequence_number, r.version,
			       r.creator_subject, v.is_public, s.next_sequence_number
			FROM %s.skill_revisions r
			JOIN %s.skill_revision_visibility v ON v.revision_id = r.id
			JOIN %s.skills s ON s.id = r.skill_id
			WHERE r.id = $1`, quoteIdentifier(schema), quoteIdentifier(schema), quoteIdentifier(schema)), first.ID).
			Scan(&persistedID, &persistedSkillID, &sequence, &version, &creator, &isPublic, &nextSequence)).To(Succeed())
		Expect(persistedID).To(Equal(first.ID))
		Expect(persistedSkillID).To(Equal(skill.ID))
		Expect(sequence).To(Equal(1))
		Expect(version).To(Equal("1"))
		Expect(creator).To(Equal("creator-a"))
		Expect(isPublic).To(BeFalse())
		Expect(nextSequence).To(Equal(3))

		var revisionCount, visibilityCount, failedCount int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions WHERE skill_id = $1`, quoteIdentifier(schema)), skill.ID).Scan(&revisionCount)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revision_visibility v JOIN %s.skill_revisions r ON r.id = v.revision_id WHERE r.skill_id = $1`, quoteIdentifier(schema), quoteIdentifier(schema)), skill.ID).Scan(&visibilityCount)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions WHERE id = $1`, quoteIdentifier(schema)), failedID).Scan(&failedCount)).To(Succeed())
		Expect(revisionCount).To(Equal(2))
		Expect(visibilityCount).To(Equal(revisionCount), "revision and default-private visibility must commit together")
		Expect(failedCount).To(BeZero())
	})

	It("revision_relationships_use_uuid_identity", func() {
		relationshipColumns := []struct {
			table  string
			column string
		}{
			{table: "skills", column: "explicit_latest_revision_id"},
			{table: "skills", column: "migration_managed_latest_revision_id"},
			{table: "skill_revisions", column: "id"},
			{table: "skill_revisions", column: "skill_id"},
			{table: "skill_revisions", column: "based_on_revision_id"},
			{table: "skill_revisions", column: "source_revision_id"},
			{table: "skill_revisions", column: "generation_id"},
			{table: "skill_generations", column: "skill_id"},
			{table: "skill_generations", column: "base_revision_id"},
			{table: "skill_generations", column: "result_revision_id"},
		}
		for _, relationship := range relationshipColumns {
			var dataType string
			Expect(store.pool.QueryRow(ctx, `
				SELECT data_type FROM information_schema.columns
				WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`,
				schema, relationship.table, relationship.column).Scan(&dataType)).To(Succeed())
			Expect(dataType).To(Equal("uuid"), "%s.%s must carry UUID identity", relationship.table, relationship.column)
		}

		type foreignKeyColumn struct {
			SourceTable, SourceColumn, SourceType string
			TargetTable, TargetColumn, TargetType string
		}
		rows, err := store.pool.Query(ctx, `
			SELECT source_table.relname, source_column.attname,
			       format_type(source_column.atttypid, source_column.atttypmod),
			       target_table.relname, target_column.attname,
			       format_type(target_column.atttypid, target_column.atttypmod)
			FROM pg_constraint constraint_record
			JOIN pg_namespace namespace_record ON namespace_record.oid = constraint_record.connamespace
			JOIN pg_class source_table ON source_table.oid = constraint_record.conrelid
			JOIN pg_class target_table ON target_table.oid = constraint_record.confrelid
			JOIN LATERAL unnest(constraint_record.conkey) WITH ORDINALITY AS source_key(attnum, position) ON TRUE
			JOIN LATERAL unnest(constraint_record.confkey) WITH ORDINALITY AS target_key(attnum, position)
			  ON target_key.position = source_key.position
			JOIN pg_attribute source_column
			  ON source_column.attrelid = source_table.oid AND source_column.attnum = source_key.attnum
			JOIN pg_attribute target_column
			  ON target_column.attrelid = target_table.oid AND target_column.attnum = target_key.attnum
			WHERE namespace_record.nspname = $1 AND constraint_record.contype = 'f'
			ORDER BY source_table.relname, constraint_record.conname, source_key.position`, schema)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()
		foreignKeys := make([]foreignKeyColumn, 0)
		for rows.Next() {
			var foreignKey foreignKeyColumn
			Expect(rows.Scan(
				&foreignKey.SourceTable, &foreignKey.SourceColumn, &foreignKey.SourceType,
				&foreignKey.TargetTable, &foreignKey.TargetColumn, &foreignKey.TargetType,
			)).To(Succeed())
			Expect(foreignKey.SourceType).To(Equal("uuid"), "%s.%s must not use a presentation value as foreign identity", foreignKey.SourceTable, foreignKey.SourceColumn)
			Expect(foreignKey.TargetType).To(Equal("uuid"), "%s.%s must be UUID identity", foreignKey.TargetTable, foreignKey.TargetColumn)
			Expect(foreignKey.SourceColumn).NotTo(Or(ContainSubstring("version"), ContainSubstring("sequence_number")))
			Expect(foreignKey.TargetColumn).NotTo(Or(ContainSubstring("version"), ContainSubstring("sequence_number")))
			foreignKeys = append(foreignKeys, foreignKey)
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		rows.Close()

		expectForeignKey := func(sourceTable, sourceColumn, targetTable, targetColumn string) {
			Expect(foreignKeys).To(ContainElement(And(
				HaveField("SourceTable", sourceTable),
				HaveField("SourceColumn", sourceColumn),
				HaveField("TargetTable", targetTable),
				HaveField("TargetColumn", targetColumn),
			)), "%s.%s must reference %s.%s by UUID", sourceTable, sourceColumn, targetTable, targetColumn)
		}
		expectForeignKey("skills", "explicit_latest_revision_id", "skill_revisions", "id")
		expectForeignKey("skills", "migration_managed_latest_revision_id", "skill_revisions", "id")
		expectForeignKey("skill_revisions", "skill_id", "skills", "id")
		expectForeignKey("skill_revisions", "based_on_revision_id", "skill_revisions", "id")
		expectForeignKey("skill_revisions", "source_revision_id", "skill_revisions", "id")
		expectForeignKey("skill_revisions", "generation_id", "skill_generations", "id")
		expectForeignKey("skill_generations", "skill_id", "skills", "id")
		expectForeignKey("skill_generations", "base_revision_id", "skill_revisions", "id")
		expectForeignKey("skill_generations", "result_revision_id", "skill_revisions", "id")

		type compositeForeignKey struct {
			Name                         string
			SourceTable, TargetTable     string
			SourceColumns, TargetColumns []string
		}
		rows, err = store.pool.Query(ctx, `
			SELECT constraint_record.conname, source_table.relname, target_table.relname,
			       ARRAY(
			         SELECT source_column.attname::text
			         FROM unnest(constraint_record.conkey) WITH ORDINALITY AS source_key(attnum, position)
			         JOIN pg_attribute source_column
			           ON source_column.attrelid = source_table.oid AND source_column.attnum = source_key.attnum
			         ORDER BY source_key.position
			       ),
			       ARRAY(
			         SELECT target_column.attname::text
			         FROM unnest(constraint_record.confkey) WITH ORDINALITY AS target_key(attnum, position)
			         JOIN pg_attribute target_column
			           ON target_column.attrelid = target_table.oid AND target_column.attnum = target_key.attnum
			         ORDER BY target_key.position
			       )
			FROM pg_constraint constraint_record
			JOIN pg_namespace namespace_record ON namespace_record.oid = constraint_record.connamespace
			JOIN pg_class source_table ON source_table.oid = constraint_record.conrelid
			JOIN pg_class target_table ON target_table.oid = constraint_record.confrelid
			WHERE namespace_record.nspname = $1 AND constraint_record.contype = 'f'
			  AND cardinality(constraint_record.conkey) > 1
			ORDER BY constraint_record.conname`, schema)
		Expect(err).NotTo(HaveOccurred())
		compositeForeignKeys := make([]compositeForeignKey, 0)
		for rows.Next() {
			var foreignKey compositeForeignKey
			Expect(rows.Scan(&foreignKey.Name, &foreignKey.SourceTable, &foreignKey.TargetTable,
				&foreignKey.SourceColumns, &foreignKey.TargetColumns)).To(Succeed())
			compositeForeignKeys = append(compositeForeignKeys, foreignKey)
		}
		Expect(rows.Err()).NotTo(HaveOccurred())
		rows.Close()

		expectCompositeForeignKey := func(name, sourceTable string, sourceColumns []string, targetTable string, targetColumns []string) {
			Expect(compositeForeignKeys).To(ContainElement(And(
				HaveField("Name", name),
				HaveField("SourceTable", sourceTable),
				HaveField("SourceColumns", sourceColumns),
				HaveField("TargetTable", targetTable),
				HaveField("TargetColumns", targetColumns),
			)), "%s must enforce the complete parent/child identity", name)
		}
		expectCompositeForeignKey("skill_revisions_base_fkey", "skill_revisions",
			[]string{"skill_id", "based_on_revision_id"}, "skill_revisions", []string{"skill_id", "id"})
		expectCompositeForeignKey("skill_generations_same_skill_base_revision_fkey", "skill_generations",
			[]string{"skill_id", "base_revision_id"}, "skill_revisions", []string{"skill_id", "id"})
		expectCompositeForeignKey("skill_generations_same_generation_winner_candidate_fkey", "skill_generations",
			[]string{"id", "winner_candidate_id"}, "generation_candidates", []string{"generation_id", "id"})
		expectCompositeForeignKey("skill_generations_same_generation_result_candidate_fkey", "skill_generations",
			[]string{"id", "result_candidate_id"}, "generation_candidates", []string{"generation_id", "id"})
		expectCompositeForeignKey("generation_sessions_same_generation_candidate_fkey", "generation_sessions",
			[]string{"generation_id", "candidate_id"}, "generation_candidates", []string{"generation_id", "id"})
		expectCompositeForeignKey("candidate_evaluations_same_generation_candidate_fkey", "candidate_evaluations",
			[]string{"generation_id", "candidate_id"}, "generation_candidates", []string{"generation_id", "id"})
		expectCompositeForeignKey("generation_diagnostics_same_generation_candidate_fkey", "generation_diagnostics",
			[]string{"generation_id", "candidate_id"}, "generation_candidates", []string{"generation_id", "id"})
		expectCompositeForeignKey("skill_generations_same_generation_result_revision_fkey", "skill_generations",
			[]string{"id", "result_revision_id"}, "skill_revisions", []string{"generation_id", "id"})

		By("executing cross-skill and cross-generation writes against the installed constraints")
		createSkillAndBase := func(slug, creator string) (*SkillRecord, *SkillRevisionRecord) {
			skill, createErr := store.ResolveSkill(ctx, ResolveSkillInput{
				ID: uuid.NewString(), Slug: slug, CreatorSubject: creator, CreatedAt: now,
			})
			Expect(createErr).NotTo(HaveOccurred())
			base, appendErr := store.AppendRevision(ctx, AppendRevisionInput{
				ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: creator,
				Origin:    RevisionOriginManual,
				Snapshot:  SkillRevisionSnapshot{Name: slug, Type: "workflow", Content: "# " + slug},
				CreatedAt: now,
			})
			Expect(appendErr).NotTo(HaveOccurred())
			return skill, base
		}
		skillA, baseA := createSkillAndBase("fk-runtime-a", "creator-a")
		skillB, baseB := createSkillAndBase("fk-runtime-b", "creator-b")

		type generationFixture struct {
			generation *SkillGenerationRecord
			claim      *SkillGenerationRecord
			candidate  *GenerationCandidateRecord
		}
		createGenerationFixture := func(skill *SkillRecord, base *SkillRevisionRecord, creator, suffix string, createdAt time.Time) generationFixture {
			generation, createErr := store.CreateGeneration(ctx, CreateGenerationInput{
				ID: uuid.NewString(), SkillID: skill.ID, BaseRevisionID: base.ID,
				CreatorSubject:     creator,
				Snapshot:           SkillRevisionSnapshot{Name: "Generation " + suffix, Type: "workflow", Content: "# Generation " + suffix},
				SelectedSessionIDs: []string{"session-" + suffix},
				EvaluatorProfile:   "fk-profile", EvaluatorProfileVersion: "fk-v1",
				EvaluationCriteria: json.RawMessage(`[{"id":"fk","kind":"structure","description":"Exercise FK identity.","weight":1}]`),
				CreatedAt:          createdAt,
			})
			Expect(createErr).NotTo(HaveOccurred())
			claim, claimErr := store.ClaimGeneration(ctx, ClaimGenerationInput{
				WorkerID: "worker-" + suffix, LeaseDuration: time.Minute,
			})
			Expect(claimErr).NotTo(HaveOccurred())
			Expect(claim).NotTo(BeNil())
			Expect(claim.ID).To(Equal(generation.ID))
			Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
				GenerationStatusQueued, GenerationStatusGeneratingCandidates)).To(Succeed())
			candidate, putErr := store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, GenerationCandidateRecord{
				ID: uuid.NewString(), Ordinal: 0, Kind: GenerationCandidateSession,
				SourceSessionIDs: []string{"session-" + suffix},
				Snapshot:         GenerationCandidateSnapshot{Name: "Candidate " + suffix, Type: "workflow", Content: "# Candidate " + suffix},
				Insights:         json.RawMessage(`[]`), BundleSHA256: "bundle-" + suffix,
			})
			Expect(putErr).NotTo(HaveOccurred())
			Expect(store.UpdateGenerationStatus(ctx, generation.ID, claim.ClaimToken,
				GenerationStatusGeneratingCandidates, GenerationStatusEvaluatingCandidates)).To(Succeed())
			score := 0.9
			_, evaluationErr := store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, CandidateEvaluationRecord{
				ID: uuid.NewString(), CandidateID: candidate.ID, RequestSHA256: "request-" + suffix,
				Profile: "fk-profile", ProfileVersion: "fk-v1", EvaluatorVersion: "fk-test",
				Score: &score, Decision: "pass", CriterionResults: json.RawMessage(`[]`),
				Findings: json.RawMessage(`[]`), Strengths: json.RawMessage(`[]`), Panel: json.RawMessage(`{}`),
			})
			Expect(evaluationErr).NotTo(HaveOccurred())
			return generationFixture{generation: generation, claim: claim, candidate: candidate}
		}
		generationA := createGenerationFixture(skillA, baseA, "creator-a", "a", now.Add(-2*time.Minute))
		generationB := createGenerationFixture(skillB, baseB, "creator-b", "b", now.Add(-time.Minute))

		resultB, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: skillB.ID, CreatorSubject: "creator-b",
			BasedOnRevisionID: baseB.ID, Origin: RevisionOriginGeneration,
			Snapshot:     SkillRevisionSnapshot{Name: "Result B", Type: "workflow", Content: "# Result B"},
			GenerationID: generationB.generation.ID, IdempotencyKey: "generation:" + generationB.generation.ID,
			CreatedAt: now.Add(time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET
			winner_candidate_id = $2, result_candidate_id = $2, result_revision_id = $3
			WHERE id = $1`, quoteIdentifier(schema)), generationB.generation.ID, generationB.candidate.ID, resultB.ID)
		Expect(err).NotTo(HaveOccurred(), "same-generation relationships must be writable")

		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_revisions SET based_on_revision_id = $2 WHERE id = $1`, quoteIdentifier(schema)), baseA.ID, baseB.ID)
		expectPostgresConstraint(err, "skill_revisions_base_fkey")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET base_revision_id = $2 WHERE id = $1`, quoteIdentifier(schema)), generationA.generation.ID, baseB.ID)
		expectPostgresConstraint(err, "skill_generations_same_skill_base_revision_fkey")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET winner_candidate_id = $2 WHERE id = $1`, quoteIdentifier(schema)), generationA.generation.ID, generationB.candidate.ID)
		expectPostgresConstraint(err, "skill_generations_same_generation_winner_candidate_fkey")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET result_candidate_id = $2 WHERE id = $1`, quoteIdentifier(schema)), generationA.generation.ID, generationB.candidate.ID)
		expectPostgresConstraint(err, "skill_generations_same_generation_result_candidate_fkey")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_sessions SET candidate_id = $3
			WHERE generation_id = $1 AND session_id = $2`, quoteIdentifier(schema)), generationA.generation.ID, "session-a", generationB.candidate.ID)
		expectPostgresConstraint(err, "generation_sessions_same_generation_candidate_fkey")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.candidate_evaluations (
			id, generation_id, candidate_id, request_sha256, profile, created_at
		) VALUES ($1, $2, $3, 'cross-generation', 'fk-profile', $4)`, quoteIdentifier(schema)),
			uuid.NewString(), generationA.generation.ID, generationB.candidate.ID, now)
		expectPostgresConstraint(err, "candidate_evaluations_same_generation_candidate_fkey")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.generation_diagnostics (
			id, generation_id, candidate_id, stage, code, message, created_at
		) VALUES ($1, $2, $3, 'evaluation', 'cross_generation', 'must fail', $4)`, quoteIdentifier(schema)),
			uuid.NewString(), generationA.generation.ID, generationB.candidate.ID, now)
		expectPostgresConstraint(err, "generation_diagnostics_same_generation_candidate_fkey")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET result_revision_id = NULL WHERE id = $1`, quoteIdentifier(schema)), generationB.generation.ID)
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET result_revision_id = $2 WHERE id = $1`, quoteIdentifier(schema)), generationA.generation.ID, resultB.ID)
		expectPostgresConstraint(err, "skill_generations_same_generation_result_revision_fkey")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET result_revision_id = $2 WHERE id = $1`, quoteIdentifier(schema)), generationB.generation.ID, resultB.ID)
		Expect(err).NotTo(HaveOccurred(), "the exact same-generation result revision relationship must remain writable")

		By("executing delete attempts against retained immutable relationships")
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.skill_revisions WHERE id = $1`, quoteIdentifier(schema)), baseA.ID)
		expectPostgresForeignKeyViolation(err)
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.generation_candidates WHERE id = $1`, quoteIdentifier(schema)), generationB.candidate.ID)
		expectPostgresForeignKeyViolation(err)
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.skill_generations WHERE id = $1`, quoteIdentifier(schema)), generationB.generation.ID)
		expectPostgresForeignKeyViolation(err)
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.skills WHERE id = $1`, quoteIdentifier(schema)), skillB.ID)
		expectPostgresForeignKeyViolation(err)

		var retainedCounts []int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT ARRAY[
			(SELECT count(*)::int FROM %s.skills WHERE id IN ($1, $2)),
			(SELECT count(*)::int FROM %s.skill_revisions WHERE id IN ($3, $4, $5)),
			(SELECT count(*)::int FROM %s.skill_generations WHERE id IN ($6, $7)),
			(SELECT count(*)::int FROM %s.generation_candidates WHERE id IN ($8, $9))
		]`, quoteIdentifier(schema), quoteIdentifier(schema), quoteIdentifier(schema), quoteIdentifier(schema)),
			skillA.ID, skillB.ID, baseA.ID, baseB.ID, resultB.ID,
			generationA.generation.ID, generationB.generation.ID,
			generationA.candidate.ID, generationB.candidate.ID).Scan(&retainedCounts)).To(Succeed())
		Expect(retainedCounts).To(Equal([]int{2, 3, 2, 2}))
	})

	It("stores_persist_complete_immutable_revision", func() {
		resolve := func(slug, creator string) *SkillRecord {
			record, err := store.ResolveSkill(ctx, ResolveSkillInput{
				ID: uuid.NewString(), Slug: slug, CreatorSubject: creator, CreatedAt: now,
			})
			Expect(err).NotTo(HaveOccurred())
			return record
		}
		targetSkill := resolve("complete-target", "creator-a")
		sourceSkill := resolve("complete-source", "creator-a")

		source, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: sourceSkill.ID, CreatorSubject: "creator-a", Origin: RevisionOriginManual,
			Snapshot: SkillRevisionSnapshot{Name: "Source", Type: "workflow", Content: "# Source"}, CreatedAt: now.Add(time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		base, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: targetSkill.ID, CreatorSubject: "creator-a", Origin: RevisionOriginManual,
			Snapshot: SkillRevisionSnapshot{Name: "Base", Type: "workflow", Content: "# Base"}, CreatedAt: now.Add(2 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())

		input := AppendRevisionInput{
			ID: uuid.NewString(), SkillID: targetSkill.ID, CreatorSubject: "creator-a",
			BasedOnRevisionID: base.ID, SourceRevisionID: source.ID, Origin: RevisionOriginDuplicate,
			Snapshot: SkillRevisionSnapshot{
				Name: "Complete revision", Description: "Every publishable field is retained.", Type: "domain-knowledge",
				Tags: []string{"complete", "immutable"}, Content: "# Complete\n\nDo the work.",
				IsAIGenerated: true, SourceSessionIDs: []string{"session-one", "session-two"},
			},
			ChangeNote: "copied and refined", IdempotencyKey: "complete:2",
			LegacyReference: "draft:fixture:7", CreatedAt: now.Add(3 * time.Minute),
		}
		appended, err := store.AppendRevision(ctx, input)
		Expect(err).NotTo(HaveOccurred())
		Expect(appended).NotTo(BeNil())

		const expectedCanonicalDigest = "f9ec86df5b506331e1154ee02e90f43de5dd429da3e9ea87e26a80152096e212"
		Expect(appended.ContentSHA256).To(Equal(expectedCanonicalDigest), "the digest must cover the canonical complete SkillRevisionSnapshot")

		persistedBeforeConflict, err := persistedRevisionForTest(ctx, store, input.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(persistedBeforeConflict.CreatedAt).To(BeTemporally("==", input.CreatedAt))
		Expect(persistedBeforeConflict.ChangedAt).To(BeTemporally("==", input.CreatedAt))
		Expect(persistedBeforeConflict).To(Equal(persistedRevisionTestRecord{
			ID: input.ID, SkillID: input.SkillID, SequenceNumber: 2, Version: "2",
			CreatorSubject: input.CreatorSubject, BasedOnRevisionID: input.BasedOnRevisionID,
			SourceRevisionID: input.SourceRevisionID, Origin: input.Origin, Snapshot: input.Snapshot,
			ContentSHA256: expectedCanonicalDigest, ChangeNote: input.ChangeNote,
			IdempotencyKey: input.IdempotencyKey, LegacyReference: input.LegacyReference,
			CreatedAt: persistedBeforeConflict.CreatedAt, IsPublic: false, ChangedBySubject: input.CreatorSubject,
			ChangedAt: persistedBeforeConflict.ChangedAt,
		}))

		retryInput := input
		retryInput.ID = uuid.NewString()
		retried, err := store.AppendRevision(ctx, retryInput)
		Expect(err).NotTo(HaveOccurred())
		Expect(retried).NotTo(BeNil())
		Expect(retried.ID).To(Equal(appended.ID), "an identical idempotency-key retry must return the original immutable append")
		Expect(retried.ContentSHA256).To(Equal(expectedCanonicalDigest))
		persistedAfterRetry, err := persistedRevisionForTest(ctx, store, input.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(persistedAfterRetry).To(Equal(persistedBeforeConflict))

		conflicts := []struct {
			name   string
			mutate func(*AppendRevisionInput)
		}{
			{name: "name", mutate: func(conflict *AppendRevisionInput) { conflict.Snapshot.Name = "Changed name" }},
			{name: "description", mutate: func(conflict *AppendRevisionInput) { conflict.Snapshot.Description = "Changed description" }},
			{name: "type", mutate: func(conflict *AppendRevisionInput) { conflict.Snapshot.Type = "workflow" }},
			{name: "tags", mutate: func(conflict *AppendRevisionInput) { conflict.Snapshot.Tags = []string{"changed"} }},
			{name: "content", mutate: func(conflict *AppendRevisionInput) { conflict.Snapshot.Content = "# Changed" }},
			{name: "AI provenance", mutate: func(conflict *AppendRevisionInput) { conflict.Snapshot.IsAIGenerated = false }},
			{name: "source sessions", mutate: func(conflict *AppendRevisionInput) { conflict.Snapshot.SourceSessionIDs = []string{"changed-session"} }},
			{name: "base lineage", mutate: func(conflict *AppendRevisionInput) { conflict.BasedOnRevisionID = "" }},
			{name: "source lineage", mutate: func(conflict *AppendRevisionInput) { conflict.SourceRevisionID = "" }},
			{name: "origin", mutate: func(conflict *AppendRevisionInput) { conflict.Origin = RevisionOriginManual }},
			{name: "change note", mutate: func(conflict *AppendRevisionInput) { conflict.ChangeNote = "changed note" }},
			{name: "legacy provenance", mutate: func(conflict *AppendRevisionInput) { conflict.LegacyReference = "draft:fixture:8" }},
		}
		for _, conflictCase := range conflicts {
			By("rejecting an idempotency-key retry that changes " + conflictCase.name)
			conflict := input
			conflict.ID = uuid.NewString()
			conflictCase.mutate(&conflict)
			result, conflictErr := store.AppendRevision(ctx, conflict)
			Expect(result).To(BeNil())
			Expect(errors.Is(conflictErr, ErrRevisionConflict)).To(BeTrue(), "changed %s returned %v", conflictCase.name, conflictErr)

			persistedAfterConflict, readErr := persistedRevisionForTest(ctx, store, input.ID)
			Expect(readErr).NotTo(HaveOccurred())
			Expect(persistedAfterConflict).To(Equal(persistedBeforeConflict), "a conflict must not mutate any content, provenance, visibility, or audit column")
		}

		var revisionCount, visibilityCount, nextSequence int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions WHERE skill_id = $1`, quoteIdentifier(schema)), targetSkill.ID).Scan(&revisionCount)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT count(*) FROM %s.skill_revision_visibility visibility
			JOIN %s.skill_revisions revision ON revision.id = visibility.revision_id
			WHERE revision.skill_id = $1`, quoteIdentifier(schema), quoteIdentifier(schema)), targetSkill.ID).Scan(&visibilityCount)).To(Succeed())
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT next_sequence_number FROM %s.skills WHERE id = $1`, quoteIdentifier(schema)), targetSkill.ID).Scan(&nextSequence)).To(Succeed())
		Expect(revisionCount).To(Equal(2), "retries and conflicts must not append rows")
		Expect(visibilityCount).To(Equal(2), "retries and conflicts must not append metadata rows")
		Expect(nextSequence).To(Equal(3), "retries and conflicts must not consume sequence numbers")
	})

	It("postgres_concurrent_appends_receive_distinct_sequences", func() {
		skill, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "concurrent-append", CreatorSubject: "creator-a", CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		base, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: "creator-a", Origin: RevisionOriginManual,
			Snapshot: SkillRevisionSnapshot{Name: "Base", Type: "workflow", Content: "# Base"}, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())

		const writers = 8
		start := make(chan struct{})
		results := make(chan appendRevisionTestResult, writers)
		for ordinal := range writers {
			input := AppendRevisionInput{
				ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: "creator-a",
				BasedOnRevisionID: base.ID, Origin: RevisionOriginManual,
				Snapshot: SkillRevisionSnapshot{
					Name: fmt.Sprintf("Sibling %d", ordinal), Type: "workflow", Content: fmt.Sprintf("# Sibling %d", ordinal),
				},
				IdempotencyKey: fmt.Sprintf("sibling:%d", ordinal), CreatedAt: now.Add(time.Duration(ordinal+1) * time.Second),
			}
			go func() {
				<-start
				results <- appendRevisionForTest(ctx, store, input)
			}()
		}
		close(start)

		sequences := make([]int, 0, writers)
		ids := make(map[string]struct{}, writers)
		for range writers {
			result := <-results
			Expect(result.err).NotTo(HaveOccurred())
			Expect(result.revision).NotTo(BeNil())
			sequences = append(sequences, result.revision.SequenceNumber)
			ids[result.revision.ID] = struct{}{}
		}
		sort.Ints(sequences)
		Expect(sequences).To(Equal([]int{2, 3, 4, 5, 6, 7, 8, 9}))
		Expect(ids).To(HaveLen(writers))

		rows, err := store.pool.Query(ctx, fmt.Sprintf(`SELECT sequence_number FROM %s.skill_revisions WHERE skill_id = $1 ORDER BY sequence_number`, quoteIdentifier(schema)), skill.ID)
		Expect(err).NotTo(HaveOccurred())
		persistedSequences := make([]int, 0, writers+1)
		for rows.Next() {
			var sequence int
			Expect(rows.Scan(&sequence)).To(Succeed())
			persistedSequences = append(persistedSequences, sequence)
		}
		rows.Close()
		Expect(rows.Err()).NotTo(HaveOccurred())
		Expect(persistedSequences).To(Equal([]int{1, 2, 3, 4, 5, 6, 7, 8, 9}))

		var nextSequence int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT next_sequence_number FROM %s.skills WHERE id = $1`, quoteIdentifier(schema)), skill.ID).Scan(&nextSequence)).To(Succeed())
		Expect(nextSequence).To(Equal(10))
	})

})

// TestPostgresExternalAttachmentFilter pins the storage half of the
// deployment-configured external-filter capability: the probe, the
// EXISTS-per-value rendering against a fixture view of the canonical
// attachment shape, and the typed missing-relation error once the view is
// gone. The fixture is created by the test; no external product is installed
// or referenced.
var _ = Describe("Postgres durable generations", func() {
	var (
		ctx    context.Context
		store  *PostgresStore
		schema string
		now    time.Time
	)

	BeforeEach(func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx = context.Background()
		schema = "skills_generations_" + uuid.NewString()[:8]
		var err error
		store, err = OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		Expect(store.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)).To(Succeed())
		now = now.UTC()
		DeferCleanup(func() {
			_, _ = store.pool.Exec(context.Background(), fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", quoteIdentifier(schema)))
			store.Close()
		})
	})

	It("defense-sanitizes legacy artifacts on every generation load", func() {
		creator := "defense-sanitize-owner"
		skill, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "defense-sanitize-generation", CreatorSubject: creator,
		})
		Expect(err).NotTo(HaveOccurred())
		generation, err := store.CreateGeneration(ctx, CreateGenerationInput{
			ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: creator,
			Snapshot:      SkillRevisionSnapshot{Name: "Defense sanitize", Type: "workflow", Content: "# Defense"},
			AuthorContext: "Do not trust retained rows.", EvaluatorProfile: "generation-candidate-v1",
			EvaluatorProfileVersion: "1",
			EvaluationCriteria:      json.RawMessage(`[{"id":"safe","kind":"content","description":"Safe.","weight":1}]`),
		})
		Expect(err).NotTo(HaveOccurred())
		claim, err := store.ClaimGeneration(ctx, ClaimGenerationInput{WorkerID: "defense-worker", LeaseDuration: time.Minute})
		Expect(err).NotTo(HaveOccurred())
		candidate, err := store.PutGenerationCandidate(ctx, generation.ID, claim.ClaimToken, GenerationCandidateRecord{
			ID: uuid.NewString(), Ordinal: 0, Kind: GenerationCandidateContext,
			Snapshot:     GenerationCandidateSnapshot{Name: "Safe candidate", Type: "workflow", Content: "# Safe"},
			Insights:     json.RawMessage(`[{"kind":"fact","summary":"safe","evidence":"bounded"}]`),
			BundleSHA256: "recomputed",
		})
		Expect(err).NotTo(HaveOccurred())
		score := 0.8
		evaluation, err := store.PutCandidateEvaluation(ctx, generation.ID, claim.ClaimToken, CandidateEvaluationRecord{
			ID: uuid.NewString(), CandidateID: candidate.ID, RequestSHA256: "safe-request",
			Profile: generation.EvaluatorProfile, ProfileVersion: generation.EvaluatorProfileVersion,
			EvaluatorVersion: "safe", Score: &score, Decision: "pass",
			CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`), Strengths: json.RawMessage(`[]`),
		})
		Expect(err).NotTo(HaveOccurred())
		rogueDiagnosticID := uuid.NewString()
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.generation_diagnostics (
			id, generation_id, candidate_id, stage, code, message, retryable, created_at
		) VALUES ($1, $2, $3, 'synthesis', 'legacy_resume_marker',
			'legacy diagnostics must never suppress synthesis', FALSE, clock_timestamp())`, quoteIdentifier(schema)),
			rogueDiagnosticID, generation.ID, candidate.ID)
		Expect(err).NotTo(HaveOccurred())

		state, err := store.GetClaimedGeneration(ctx, generation.ID, claim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Candidates).To(HaveLen(1))
		Expect(state.Evaluations).To(HaveLen(1))
		Expect(state.Diagnostics).To(BeEmpty(), "an arbitrary synthesis row is not a runtime resume marker")

		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_candidates SET
			insights = '[{"kind":"legacy","summary":"missing evidence","unsafe":"secret"}]'::jsonb,
			bundle_sha256 = $2 WHERE id = $1`, quoteIdentifier(schema)), candidate.ID, strings.Repeat("a", 64))
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.candidate_evaluations SET
			criterion_results = '[{"criterion_id":"safe","weight":1,"passed":true,"rationale":"ok","unsafe":"secret"}]'::jsonb
			WHERE id = $1`, quoteIdentifier(schema)), evaluation.ID)
		Expect(err).NotTo(HaveOccurred())
		state, err = store.GetClaimedGeneration(ctx, generation.ID, claim.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(state.Candidates).To(BeEmpty())
		Expect(state.Evaluations).To(BeEmpty())
		Expect(state.Diagnostics).To(BeEmpty())
	})

	It("postgres_allows_concurrent_generations_per_skill", func() {
		creator := "creator-concurrent"
		skill, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "concurrent-generations",
			CreatorSubject: creator, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		base, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: creator,
			Origin: RevisionOriginManual,
			Snapshot: SkillRevisionSnapshot{
				Name: "Generation base", Description: "Shared exact base.", Type: "workflow",
				Tags: []string{"base"}, Content: "# Base", SourceSessionIDs: []string{"source-base"},
			},
			CreatedAt: now.Add(time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())

		const writers = 6
		type generationCreationResult struct {
			generation *SkillGenerationRecord
			err        error
		}
		start := make(chan struct{})
		results := make(chan generationCreationResult, writers)
		criteria := json.RawMessage(`[{"id":"actionable-workflow","kind":"structure","description":"Concrete steps.","weight":3}]`)
		expectedInputs := make(map[string]CreateGenerationInput, writers)
		for ordinal := range writers {
			input := CreateGenerationInput{
				ID: uuid.NewString(), SkillID: skill.ID, BaseRevisionID: base.ID,
				CreatorSubject: creator,
				Snapshot: SkillRevisionSnapshot{
					Name: fmt.Sprintf("Candidate seed %d", ordinal), Description: "Complete revision content.",
					Type: "workflow", Tags: []string{"seed", fmt.Sprintf("%d", ordinal)},
					Content: fmt.Sprintf("# Seed %d", ordinal), IsAIGenerated: true,
					SourceSessionIDs: []string{fmt.Sprintf("source-%d", ordinal)},
				},
				AuthorContext: fmt.Sprintf("context-%d", ordinal),
				SelectedSessionIDs: []string{
					fmt.Sprintf("selected-%d-b", ordinal), fmt.Sprintf("selected-%d-a", ordinal),
				},
				EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
				EvaluationCriteria: append(json.RawMessage(nil), criteria...),
				CreatedAt:          now.Add(time.Duration(ordinal+2) * time.Minute),
			}
			expectedInputs[input.ID] = input
			go func() {
				<-start
				generation, createErr := store.CreateGeneration(ctx, input)
				results <- generationCreationResult{generation: generation, err: createErr}
			}()
		}
		close(start)

		createdIDs := make(map[string]struct{}, writers)
		createdTimes := make(map[string]time.Time, writers)
		for range writers {
			result := <-results
			Expect(result.err).NotTo(HaveOccurred())
			Expect(result.generation).NotTo(BeNil())
			expected, exists := expectedInputs[result.generation.ID]
			Expect(exists).To(BeTrue(), "storage must return the caller-supplied generation UUID")
			Expect(result.generation.SkillID).To(Equal(skill.ID))
			Expect(result.generation.BaseRevisionID).To(Equal(base.ID))
			Expect(result.generation.CreatorSubject).To(Equal(creator))
			Expect(result.generation.Snapshot).To(Equal(expected.Snapshot))
			Expect(result.generation.Status).To(Equal(GenerationStatusQueued))
			Expect(result.generation.AuthorContext).To(Equal(expected.AuthorContext))
			Expect(result.generation.SelectedSessionIDs).To(Equal(expected.SelectedSessionIDs))
			Expect(result.generation.EvaluatorProfile).To(Equal(expected.EvaluatorProfile))
			Expect(result.generation.EvaluatorProfileVersion).To(Equal(expected.EvaluatorProfileVersion))
			Expect(result.generation.EvaluationCriteria).To(MatchJSON(criteria))
			Expect(result.generation.CreatedAt).NotTo(BeTemporally("==", expected.CreatedAt),
				"caller timestamps cannot schedule queue work")
			Expect(result.generation.UpdatedAt).To(BeTemporally("==", result.generation.CreatedAt))
			Expect(result.generation.NextAttemptAt).To(BeTemporally("==", result.generation.CreatedAt))
			createdIDs[result.generation.ID] = struct{}{}
			createdTimes[result.generation.ID] = result.generation.CreatedAt
		}
		Expect(createdIDs).To(HaveLen(writers), "each concurrent request must commit an independent generation")

		listed, err := store.ListSkillGenerations(ctx, SkillGenerationListOpts{
			CallerSubject: creator, SkillID: skill.ID,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(listed.Generations).To(HaveLen(writers))
		Expect(listed.NextCreatedAt).To(BeNil())
		Expect(listed.NextID).To(BeEmpty())
		for _, generation := range listed.Generations {
			_, exists := expectedInputs[generation.ID]
			Expect(exists).To(BeTrue())
			Expect(generation.Status).To(Equal(GenerationStatusQueued))
			Expect(generation.SkillID).To(Equal(skill.ID))
			Expect(generation.BaseRevisionID).To(Equal(base.ID))
			Expect(generation.CreatedAt).To(BeTemporally("==", createdTimes[generation.ID]))
			Expect(createdIDs).To(HaveKey(generation.ID))
		}

		next, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: creator,
			BasedOnRevisionID: base.ID, Origin: RevisionOriginManual,
			Snapshot: SkillRevisionSnapshot{
				Name: "After generations", Type: "workflow", Content: "# Sequence two",
			},
			CreatedAt: now.Add(20 * time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(next.SequenceNumber).To(Equal(2), "generation creation must not reserve a result revision sequence")
		Expect(next.Version).To(Equal("2"))
	})

	It("postgres_claims_are_exclusive_and_lease_fenced", func() {
		creator := "creator-claim"
		skill, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "claim-generation",
			CreatorSubject: creator, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		base, err := store.AppendRevision(ctx, AppendRevisionInput{
			ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: creator,
			Origin: RevisionOriginManual,
			Snapshot: SkillRevisionSnapshot{
				Name: "Claim base", Type: "workflow", Content: "# Claim base",
			},
			CreatedAt: now.Add(time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		generationSnapshot := SkillRevisionSnapshot{
			Name: "Claim input", Description: "Complete seed.", Type: "workflow",
			Tags: []string{"claim"}, Content: "# Claim input",
			SourceSessionIDs: []string{"source-claim"},
		}
		generation, err := store.CreateGeneration(ctx, CreateGenerationInput{
			ID: uuid.NewString(), SkillID: skill.ID, BaseRevisionID: base.ID,
			CreatorSubject: creator, Snapshot: generationSnapshot,
			AuthorContext: "Keep the lease fenced.", SelectedSessionIDs: []string{"session-claim"},
			EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
			EvaluationCriteria: json.RawMessage(`[{"id":"safe-boundaries","kind":"content","description":"Fence stale work.","weight":3}]`),
			CreatedAt:          now.Add(-time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(generation.SkillID).To(Equal(skill.ID))
		Expect(generation.BaseRevisionID).To(Equal(base.ID))
		Expect(generation.CreatorSubject).To(Equal(creator))
		Expect(generation.Snapshot).To(Equal(generationSnapshot))

		// Hold the oldest claimable row lock on a dedicated transaction. A real
		// FOR UPDATE SKIP LOCKED claim must return without waiting for this lock.
		lockTx, err := store.pool.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		lockReleased := false
		DeferCleanup(func() {
			if !lockReleased {
				_ = lockTx.Rollback(context.Background())
			}
		})
		var lockedGenerationID string
		Expect(lockTx.QueryRow(ctx, fmt.Sprintf(`SELECT id::text FROM %s.skill_generations WHERE id = $1 FOR UPDATE`, quoteIdentifier(schema)), generation.ID).Scan(&lockedGenerationID)).To(Succeed())
		Expect(lockedGenerationID).To(Equal(generation.ID))

		type claimResult struct {
			claim *SkillGenerationRecord
			err   error
		}
		skippedResult := make(chan claimResult, 1)
		go func() {
			claim, claimErr := store.ClaimGeneration(ctx, ClaimGenerationInput{
				WorkerID: "worker-skip-locked", LeaseDuration: time.Minute,
			})
			skippedResult <- claimResult{claim: claim, err: claimErr}
		}()
		var skipped claimResult
		Eventually(skippedResult).WithTimeout(time.Second).Should(Receive(&skipped), "SKIP LOCKED must not wait on the held row")
		Expect(skipped.err).NotTo(HaveOccurred())
		Expect(skipped.claim).To(BeNil())
		Expect(lockTx.Rollback(ctx)).To(Succeed())
		lockReleased = true

		start := make(chan struct{})
		claims := make(chan claimResult, 2)
		for _, workerID := range []string{"worker-a", "worker-b"} {
			go func(worker string) {
				<-start
				claim, claimErr := store.ClaimGeneration(ctx, ClaimGenerationInput{
					WorkerID: worker, LeaseDuration: time.Minute,
				})
				claims <- claimResult{claim: claim, err: claimErr}
			}(workerID)
		}
		close(start)
		var activeClaim *SkillGenerationRecord
		for range 2 {
			result := <-claims
			Expect(result.err).NotTo(HaveOccurred())
			if result.claim == nil {
				continue
			}
			Expect(activeClaim).To(BeNil(), "only one worker may claim one generation during a live lease")
			activeClaim = result.claim
		}
		Expect(activeClaim).NotTo(BeNil())
		Expect(activeClaim.ID).To(Equal(generation.ID))
		Expect(activeClaim.ClaimToken).NotTo(BeEmpty())
		Expect(activeClaim.AttemptCount).To(Equal(1))
		oldToken := activeClaim.ClaimToken
		var (
			databaseNow     time.Time
			persistedToken  string
			persistedStatus GenerationStatus
			persistedLease  time.Time
		)
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT clock_timestamp(),
			claim_token::text, status, lease_expires_at FROM %s.skill_generations
			WHERE id = $1`, quoteIdentifier(schema)), generation.ID).Scan(
			&databaseNow, &persistedToken, &persistedStatus, &persistedLease)).To(Succeed())
		By(fmt.Sprintf("database claim state now=%s lease=%s status=%s tokenMatches=%t",
			databaseNow, persistedLease, persistedStatus, persistedToken == oldToken))
		Expect(persistedToken).To(Equal(oldToken))
		Expect(persistedStatus).To(Equal(GenerationStatusQueued))
		Expect(persistedLease).To(BeTemporally(">", databaseNow))
		artifactTime := activeClaim.LeaseExpiresAt.Add(24 * time.Hour).UTC()

		candidate := GenerationCandidateRecord{
			ID: uuid.NewString(), GenerationID: generation.ID, Ordinal: 0,
			Kind: GenerationCandidateSession, SourceSessionIDs: []string{"session-claim"},
			Snapshot: GenerationCandidateSnapshot{
				Name: "Claim candidate", Description: "Retained work.",
				Type: "workflow", Tags: []string{"candidate"}, Content: "# Candidate",
				IsAIGenerated: true,
			},
			Insights:     json.RawMessage(`[{"kind":"strength","summary":"lease retained","evidence":"bounded fixture"}]`),
			BundleSHA256: "candidate-hash", CreatedAt: artifactTime.Add(time.Second),
		}
		Expect(store.UpdateGenerationStatus(ctx, generation.ID, oldToken,
			GenerationStatusQueued, GenerationStatusGeneratingCandidates)).To(Succeed())
		storedCandidate, err := store.PutGenerationCandidate(ctx, generation.ID, oldToken, candidate)
		Expect(err).NotTo(HaveOccurred())
		retriedCandidate, err := store.PutGenerationCandidate(ctx, generation.ID, oldToken, candidate)
		Expect(err).NotTo(HaveOccurred())
		Expect(retriedCandidate).To(Equal(storedCandidate), "a candidate retry must return the retained row")

		candidateReady := GenerationSessionRecord{
			GenerationID: generation.ID, SessionID: "session-claim", Ordinal: 0,
			Status: GenerationSessionCandidateReady, CandidateID: storedCandidate.ID,
			UpdatedAt: artifactTime.Add(2 * time.Second),
		}
		Expect(store.UpdateGenerationSession(ctx, generation.ID, oldToken, candidateReady)).To(Succeed())
		Expect(store.UpdateGenerationStatus(ctx, generation.ID, oldToken,
			GenerationStatusGeneratingCandidates, GenerationStatusEvaluatingCandidates)).To(Succeed())

		score := 0.9
		evaluation := CandidateEvaluationRecord{
			ID: uuid.NewString(), GenerationID: generation.ID, CandidateID: storedCandidate.ID,
			RequestSHA256: "request-hash", Profile: "generation-candidate-v1", ProfileVersion: "1",
			EvaluatorVersion: "test-evaluator", Score: &score, Decision: "pass",
			CriterionResults: json.RawMessage(`[{"criterion_id":"safe-boundaries","weight":2,"passed":true,"rationale":"Clear safety boundaries."}]`),
			Findings:         json.RawMessage(`[]`), Strengths: json.RawMessage(`["clear"]`),
			Panel: json.RawMessage(`{"judges":1}`), CreatedAt: artifactTime.Add(4 * time.Second),
		}
		storedEvaluation, err := store.PutCandidateEvaluation(ctx, generation.ID, oldToken, evaluation)
		Expect(err).NotTo(HaveOccurred())
		retriedEvaluation, err := store.PutCandidateEvaluation(ctx, generation.ID, oldToken, evaluation)
		Expect(err).NotTo(HaveOccurred())
		Expect(retriedEvaluation).To(Equal(storedEvaluation), "an evaluation retry must return the retained row")

		evaluatedSession := candidateReady
		evaluatedSession.Status = GenerationSessionEvaluated
		evaluatedSession.UpdatedAt = artifactTime.Add(5 * time.Second)
		Expect(store.UpdateGenerationSession(ctx, generation.ID, oldToken, evaluatedSession)).To(Succeed())
		diagnostic := GenerationDiagnosticRecord{
			ID: uuid.NewString(), GenerationID: generation.ID, SessionID: "session-claim",
			CandidateID: storedCandidate.ID, Stage: "evaluation", Code: "evaluation_unrankable",
			Message: "The candidate evaluation could not be ranked.", Retryable: false,
			CreatedAt: artifactTime.Add(6 * time.Second),
		}
		storedDiagnostic, err := store.PutGenerationDiagnostic(ctx, generation.ID, oldToken, diagnostic)
		Expect(err).NotTo(HaveOccurred())
		retriedDiagnostic, err := store.PutGenerationDiagnostic(ctx, generation.ID, oldToken, diagnostic)
		Expect(err).NotTo(HaveOccurred())
		Expect(retriedDiagnostic).To(Equal(storedDiagnostic), "a diagnostic retry must return the retained row")

		currentState, err := store.GetClaimedGeneration(ctx, generation.ID, oldToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(currentState.Generation.Status).To(Equal(GenerationStatusEvaluatingCandidates))
		Expect(currentState.Sessions).To(HaveLen(1))
		Expect(currentState.Sessions[0]).To(And(
			HaveField("GenerationID", generation.ID),
			HaveField("SessionID", "session-claim"),
			HaveField("Ordinal", 0),
			HaveField("Status", GenerationSessionEvaluated),
			HaveField("CandidateID", storedCandidate.ID),
			HaveField("DiagnosticCode", ""),
		))
		evaluatedSession = currentState.Sessions[0]
		Expect(evaluatedSession.UpdatedAt).To(BeTemporally("<", artifactTime), "storage time, not the caller fixture timestamp, is authoritative")
		Expect(currentState.Candidates).To(Equal([]GenerationCandidateRecord{*storedCandidate}))
		Expect(currentState.Evaluations).To(Equal([]CandidateEvaluationRecord{*storedEvaluation}))
		Expect(currentState.Diagnostics).To(Equal([]GenerationDiagnosticRecord{*storedDiagnostic}))

		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations
			SET lease_expires_at = clock_timestamp() - interval '1 second'
			WHERE id = $1`, quoteIdentifier(schema)), generation.ID)
		Expect(err).NotTo(HaveOccurred())
		reclaimAt := artifactTime.Add(time.Hour)
		reclaimed, err := store.ClaimGeneration(ctx, ClaimGenerationInput{
			WorkerID: "worker-recovery", LeaseDuration: time.Minute,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(reclaimed).NotTo(BeNil())
		Expect(reclaimed.ID).To(Equal(generation.ID))
		Expect(reclaimed.ClaimToken).NotTo(Equal(oldToken))
		Expect(reclaimed.AttemptCount).To(Equal(2))

		renewed, err := store.RenewGenerationLease(ctx, generation.ID, oldToken, time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(renewed).To(BeFalse())
		_, err = store.GetClaimedGeneration(ctx, generation.ID, oldToken)
		Expect(err).To(MatchError(ErrGenerationClaimLost))

		staleWriteAt := reclaimAt.Add(time.Second)
		Expect(store.UpdateGenerationStatus(ctx, generation.ID, oldToken,
			GenerationStatusEvaluatingCandidates, GenerationStatusSynthesizing)).To(MatchError(ErrGenerationClaimLost))
		Expect(store.UpdateGenerationSession(ctx, generation.ID, oldToken, GenerationSessionRecord{
			GenerationID: generation.ID, SessionID: "session-claim", Ordinal: 0,
			Status: GenerationSessionCandidateFailed, DiagnosticCode: "stale-write", UpdatedAt: staleWriteAt,
		})).To(MatchError(ErrGenerationClaimLost))

		staleCandidateID := uuid.NewString()
		staleCandidate, err := store.PutGenerationCandidate(ctx, generation.ID, oldToken, GenerationCandidateRecord{
			ID: staleCandidateID, GenerationID: generation.ID, Ordinal: 1, Kind: GenerationCandidateContext,
			Snapshot: GenerationCandidateSnapshot{Name: "Stale", Type: "workflow", Content: "# Stale"},
			Insights: json.RawMessage(`[{"kind":"stale"}]`), BundleSHA256: "stale-candidate-hash",
			CreatedAt: staleWriteAt,
		})
		Expect(staleCandidate).To(BeNil())
		Expect(err).To(MatchError(ErrGenerationClaimLost))

		staleScore := 0.99
		staleEvaluation, err := store.PutCandidateEvaluation(ctx, generation.ID, oldToken, CandidateEvaluationRecord{
			ID: uuid.NewString(), GenerationID: generation.ID, CandidateID: storedCandidate.ID,
			RequestSHA256: "stale-evaluation-hash", Profile: "generation-candidate-v1", ProfileVersion: "1",
			EvaluatorVersion: "stale-evaluator", Score: &staleScore, Decision: "pass",
			CriterionResults: json.RawMessage(`[]`), Findings: json.RawMessage(`[]`),
			Strengths: json.RawMessage(`[]`), Panel: json.RawMessage(`{}`), CreatedAt: staleWriteAt,
		})
		Expect(staleEvaluation).To(BeNil())
		Expect(err).To(MatchError(ErrGenerationClaimLost))

		staleDiagnostic, err := store.PutGenerationDiagnostic(ctx, generation.ID, oldToken, GenerationDiagnosticRecord{
			ID: uuid.NewString(), GenerationID: generation.ID,
			Stage: "candidate", Code: "context_candidate_failed",
			Message: "A context-derived candidate could not be generated.", CreatedAt: staleWriteAt,
		})
		Expect(staleDiagnostic).To(BeNil())
		Expect(err).To(MatchError(ErrGenerationClaimLost))

		staleFinalization, err := store.AppendPrivateGenerationResult(ctx, AppendPrivateGenerationResultInput{
			GenerationID: generation.ID, ClaimToken: oldToken,
			InitialWinnerCandidateID: storedCandidate.ID, ResultCandidateID: storedCandidate.ID,
		})
		Expect(staleFinalization).To(BeNil())
		if !errors.Is(err, ErrGenerationResultAppendUnimplemented) {
			Expect(err).To(MatchError(ErrGenerationClaimLost))
		}

		nested, err := store.GetSkillGeneration(ctx, creator, skill.ID, generation.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(nested).NotTo(BeNil())
		direct, err := store.GetGenerationByID(ctx, creator, generation.ID)
		Expect(err).NotTo(HaveOccurred())
		Expect(direct).To(Equal(nested))
		Expect(direct.Generation.ID).To(Equal(generation.ID))
		Expect(direct.Generation.SkillID).To(Equal(skill.ID))
		Expect(direct.Generation.BaseRevisionID).To(Equal(base.ID))
		Expect(direct.Generation.CreatorSubject).To(Equal(creator))
		Expect(direct.Generation.Snapshot).To(Equal(generationSnapshot))
		Expect(direct.Generation.Status).To(Equal(GenerationStatusEvaluatingCandidates))
		Expect(direct.Generation.WinnerCandidateID).To(BeEmpty())
		Expect(direct.Generation.ResultCandidateID).To(BeEmpty())
		Expect(direct.Generation.ResultRevisionID).To(BeEmpty())
		Expect(direct.Sessions).To(Equal([]GenerationSessionRecord{evaluatedSession}), "the stale session mutation must not be persisted")
		Expect(direct.Candidates).To(Equal([]GenerationCandidateRecord{*storedCandidate}), "the stale candidate must not be persisted")
		Expect(direct.Evaluations).To(Equal([]CandidateEvaluationRecord{*storedEvaluation}), "the stale evaluation must not be persisted")
		Expect(direct.Diagnostics).To(Equal([]GenerationDiagnosticRecord{*storedDiagnostic}), "the stale diagnostic must not be persisted")

		revisions, err := store.ListRevisions(ctx, RevisionListOpts{
			SkillID: skill.ID, CallerSubject: creator, Limit: DefaultRevisionListLimit,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(revisions).To(HaveLen(1), "stale finalization must not append a result revision")
		Expect(revisions[0].Revision.ID).To(Equal(base.ID))
		Expect(revisions[0].Revision.SkillID).To(Equal(skill.ID))
		Expect(revisions[0].Revision.CreatorSubject).To(Equal(creator))
		Expect(revisions[0].Revision.Snapshot).To(Equal(base.Snapshot))

		renewed, err = store.RenewGenerationLease(ctx, generation.ID, reclaimed.ClaimToken, 2*time.Minute)
		Expect(err).NotTo(HaveOccurred())
		Expect(renewed).To(BeTrue(), "the reclaimed worker must retain its current fenced claim")
		var renewedDatabaseNow, renewedLease time.Time
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT clock_timestamp(), lease_expires_at
			FROM %s.skill_generations WHERE id = $1`, quoteIdentifier(schema)), generation.ID).
			Scan(&renewedDatabaseNow, &renewedLease)).To(Succeed())
		Expect(renewedLease).To(BeTemporally(">", renewedDatabaseNow.Add(110*time.Second)))
		Expect(renewedLease).To(BeTemporally("<=", renewedDatabaseNow.Add(2*time.Minute)))

		resumed, err := store.GetClaimedGeneration(ctx, generation.ID, reclaimed.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(resumed.Generation.Status).To(Equal(GenerationStatusEvaluatingCandidates))
		Expect(resumed.Sessions).To(Equal([]GenerationSessionRecord{evaluatedSession}))
		Expect(resumed.Candidates).To(Equal([]GenerationCandidateRecord{*storedCandidate}))
		Expect(resumed.Evaluations).To(Equal([]CandidateEvaluationRecord{*storedEvaluation}))
		Expect(resumed.Diagnostics).To(Equal([]GenerationDiagnosticRecord{*storedDiagnostic}))

		retainedCandidate, err := store.PutGenerationCandidate(ctx, generation.ID, reclaimed.ClaimToken, candidate)
		Expect(err).NotTo(HaveOccurred())
		Expect(retainedCandidate).To(Equal(storedCandidate))
		retainedEvaluation, err := store.PutCandidateEvaluation(ctx, generation.ID, reclaimed.ClaimToken, evaluation)
		Expect(err).NotTo(HaveOccurred())
		Expect(retainedEvaluation).To(Equal(storedEvaluation))
		retainedDiagnostic, err := store.PutGenerationDiagnostic(ctx, generation.ID, reclaimed.ClaimToken, diagnostic)
		Expect(err).NotTo(HaveOccurred())
		Expect(retainedDiagnostic).To(Equal(storedDiagnostic))

		continueAt := reclaimAt.Add(2 * time.Second)
		Expect(store.UpdateGenerationStatus(ctx, generation.ID, reclaimed.ClaimToken,
			GenerationStatusEvaluatingCandidates, GenerationStatusSynthesizing)).To(Succeed())
		continuedSession := evaluatedSession
		continuedSession.UpdatedAt = continueAt.Add(time.Second)
		Expect(store.UpdateGenerationSession(ctx, generation.ID, reclaimed.ClaimToken, continuedSession)).To(Succeed())
		continued, err := store.GetClaimedGeneration(ctx, generation.ID, reclaimed.ClaimToken)
		Expect(err).NotTo(HaveOccurred())
		Expect(continued.Generation.Status).To(Equal(GenerationStatusSynthesizing))
		Expect(continued.Sessions).To(HaveLen(1))
		continuedSession = continued.Sessions[0]
		Expect(continuedSession.Status).To(Equal(GenerationSessionEvaluated))
		Expect(continuedSession.CandidateID).To(Equal(storedCandidate.ID))
		Expect(continued.Candidates).To(Equal([]GenerationCandidateRecord{*storedCandidate}))
		Expect(continued.Evaluations).To(Equal([]CandidateEvaluationRecord{*storedEvaluation}))
		Expect(continued.Diagnostics).To(Equal([]GenerationDiagnosticRecord{*storedDiagnostic}))

		By("using database time to reject an expired claim despite an old artifact timestamp")
		_, err = store.CancelSkillGeneration(ctx, creator, skill.ID, generation.ID)
		Expect(err).NotTo(HaveOccurred())
		createClaimedGeneration := func(name string, createdAt time.Time) (*SkillGenerationRecord, *SkillGenerationRecord) {
			created, createErr := store.CreateGeneration(ctx, CreateGenerationInput{
				ID: uuid.NewString(), SkillID: skill.ID, BaseRevisionID: base.ID,
				CreatorSubject: creator,
				Snapshot: SkillRevisionSnapshot{
					Name: name, Type: "workflow", Content: "# " + name,
				},
				AuthorContext:    "Exercise transactional fencing.",
				EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
				EvaluationCriteria: json.RawMessage(`[{"id":"fencing","kind":"content","description":"Fence writes.","weight":1}]`),
				CreatedAt:          now.Add(-time.Minute),
			})
			Expect(createErr).NotTo(HaveOccurred())
			claimed, claimErr := store.ClaimGeneration(ctx, ClaimGenerationInput{
				WorkerID: "worker-" + name, LeaseDuration: time.Minute,
			})
			Expect(claimErr).NotTo(HaveOccurred())
			Expect(claimed).NotTo(BeNil())
			Expect(claimed.ID).To(Equal(created.ID))
			return created, claimed
		}

		expiredGeneration, expiredClaim := createClaimedGeneration("expired-clock", now.Add(30*time.Minute))
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations
			SET lease_expires_at = clock_timestamp() - interval '1 second'
			WHERE id = $1`, quoteIdentifier(schema)), expiredGeneration.ID)
		Expect(err).NotTo(HaveOccurred())
		expiredWrite, err := store.PutGenerationDiagnostic(ctx, expiredGeneration.ID,
			expiredClaim.ClaimToken, GenerationDiagnosticRecord{
				ID: uuid.NewString(), Stage: "candidate", Code: "context_candidate_failed",
				Message:   "A context-derived candidate could not be generated.",
				CreatedAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			})
		Expect(expiredWrite).To(BeNil())
		Expect(err).To(MatchError(ErrGenerationClaimLost))
		var expiredDiagnosticCount int
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.generation_diagnostics
			WHERE generation_id = $1`, quoteIdentifier(schema)), expiredGeneration.ID).
			Scan(&expiredDiagnosticCount)).To(Succeed())
		Expect(expiredDiagnosticCount).To(BeZero())
		_, err = store.CancelSkillGeneration(ctx, creator, skill.ID, expiredGeneration.ID)
		Expect(err).NotTo(HaveOccurred())

		By("serializing an artifact commit before cancellation with the generation row lock")
		const artifactAdvisoryLock int64 = 7834291
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION %s.block_generation_candidate_write()
			RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				PERFORM pg_advisory_xact_lock(%d);
				RETURN NEW;
			END
			$$`, quoteIdentifier(schema), artifactAdvisoryLock))
		Expect(err).NotTo(HaveOccurred())
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER block_generation_candidate_write
			BEFORE INSERT ON %s.generation_candidates
			FOR EACH ROW EXECUTE FUNCTION %s.block_generation_candidate_write()`,
			quoteIdentifier(schema), quoteIdentifier(schema)))
		Expect(err).NotTo(HaveOccurred())

		waitingArtifactWrites := func() int {
			var waiting int
			queryErr := store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
				WHERE datname = current_database() AND wait_event_type = 'Lock'
				  AND query LIKE $1`, "%"+schema+"%generation_candidates%").Scan(&waiting)
			Expect(queryErr).NotTo(HaveOccurred())
			return waiting
		}
		type candidateWriteResult struct {
			candidate *GenerationCandidateRecord
			err       error
		}
		type cancelResult struct {
			state *GenerationState
			err   error
		}
		blockedCandidate := func(generationID, suffix string, createdAt time.Time) GenerationCandidateRecord {
			return GenerationCandidateRecord{
				ID: uuid.NewString(), GenerationID: generationID, Ordinal: 0,
				Kind: GenerationCandidateContext,
				Snapshot: GenerationCandidateSnapshot{
					Name: "Blocked " + suffix, Type: "workflow", Content: "# Blocked " + suffix,
				},
				Insights: json.RawMessage(`[]`), BundleSHA256: "blocked-" + suffix,
				CreatedAt: createdAt,
			}
		}

		cancelGeneration, cancelClaim := createClaimedGeneration("cancel-interleave", now.Add(40*time.Minute))
		cancelBlocker, err := store.pool.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = cancelBlocker.Rollback(context.Background()) })
		_, err = cancelBlocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, artifactAdvisoryLock)
		Expect(err).NotTo(HaveOccurred())
		cancelWriteResults := make(chan candidateWriteResult, 1)
		go func() {
			candidate, writeErr := store.PutGenerationCandidate(ctx, cancelGeneration.ID,
				cancelClaim.ClaimToken, blockedCandidate(cancelGeneration.ID, "cancel", now.Add(41*time.Minute)))
			cancelWriteResults <- candidateWriteResult{candidate: candidate, err: writeErr}
		}()
		Eventually(waitingArtifactWrites).WithTimeout(5 * time.Second).Should(BeNumerically(">=", 1))
		cancelStarted := make(chan struct{})
		cancelResults := make(chan cancelResult, 1)
		go func() {
			close(cancelStarted)
			state, cancelErr := store.CancelSkillGeneration(ctx, creator, skill.ID, cancelGeneration.ID)
			cancelResults <- cancelResult{state: state, err: cancelErr}
		}()
		<-cancelStarted
		Consistently(cancelResults).WithTimeout(100*time.Millisecond).ShouldNot(Receive(),
			"cancellation must wait for the already-observed artifact transaction")
		Expect(cancelBlocker.Commit(ctx)).To(Succeed())
		var committedWrite candidateWriteResult
		Eventually(cancelWriteResults).WithTimeout(5 * time.Second).Should(Receive(&committedWrite))
		Expect(committedWrite.err).NotTo(HaveOccurred())
		Expect(committedWrite.candidate).NotTo(BeNil())
		var committedCancel cancelResult
		Eventually(cancelResults).WithTimeout(5 * time.Second).Should(Receive(&committedCancel))
		Expect(committedCancel.err).NotTo(HaveOccurred())
		Expect(committedCancel.state.Generation.Status).To(Equal(GenerationStatusCanceled))
		Expect(committedCancel.state.Candidates).To(ContainElement(*committedWrite.candidate))

		By("making reclaim skip a generation until its in-flight artifact commits")
		reclaimGeneration, interleavedClaim := createClaimedGeneration("reclaim-interleave", now.Add(50*time.Minute))
		_, err = store.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations
			SET lease_expires_at = clock_timestamp() + interval '2 seconds'
			WHERE id = $1`, quoteIdentifier(schema)), reclaimGeneration.ID)
		Expect(err).NotTo(HaveOccurred())
		reclaimBlocker, err := store.pool.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = reclaimBlocker.Rollback(context.Background()) })
		_, err = reclaimBlocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, artifactAdvisoryLock)
		Expect(err).NotTo(HaveOccurred())
		reclaimWriteResults := make(chan candidateWriteResult, 1)
		go func() {
			candidate, writeErr := store.PutGenerationCandidate(ctx, reclaimGeneration.ID,
				interleavedClaim.ClaimToken, blockedCandidate(reclaimGeneration.ID, "reclaim", now.Add(51*time.Minute)))
			reclaimWriteResults <- candidateWriteResult{candidate: candidate, err: writeErr}
		}()
		Eventually(waitingArtifactWrites).WithTimeout(5 * time.Second).Should(BeNumerically(">=", 1))
		leaseExpired := func() bool {
			var expired bool
			queryErr := store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT clock_timestamp() >= lease_expires_at
				FROM %s.skill_generations WHERE id = $1`, quoteIdentifier(schema)), reclaimGeneration.ID).
				Scan(&expired)
			Expect(queryErr).NotTo(HaveOccurred())
			return expired
		}
		Eventually(leaseExpired).WithTimeout(3 * time.Second).Should(BeTrue())
		skippedReclaim, err := store.ClaimGeneration(ctx, ClaimGenerationInput{
			WorkerID: "worker-reclaim-race", LeaseDuration: time.Minute,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(skippedReclaim).To(BeNil(), "SKIP LOCKED must not reclaim while an artifact mutation owns the row")
		Expect(reclaimBlocker.Commit(ctx)).To(Succeed())
		var reclaimWrite candidateWriteResult
		Eventually(reclaimWriteResults).WithTimeout(5 * time.Second).Should(Receive(&reclaimWrite))
		Expect(reclaimWrite.err).NotTo(HaveOccurred())
		Expect(reclaimWrite.candidate).NotTo(BeNil())
		reclaimedAfterWrite, err := store.ClaimGeneration(ctx, ClaimGenerationInput{
			WorkerID: "worker-reclaim-after-write", LeaseDuration: time.Minute,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(reclaimedAfterWrite).NotTo(BeNil())
		Expect(reclaimedAfterWrite.ID).To(Equal(reclaimGeneration.ID))
		Expect(reclaimedAfterWrite.ClaimToken).NotTo(Equal(interleavedClaim.ClaimToken))
		lateDiagnostic, err := store.PutGenerationDiagnostic(ctx, reclaimGeneration.ID,
			interleavedClaim.ClaimToken, GenerationDiagnosticRecord{
				ID: uuid.NewString(), Stage: "candidate", Code: "context_candidate_failed",
				Message: "A context-derived candidate could not be generated.", CreatedAt: now.Add(52 * time.Minute),
			})
		Expect(lateDiagnostic).To(BeNil())
		Expect(err).To(MatchError(ErrGenerationClaimLost))
	})

	It("postgres_renewal_waiter_cannot_resurrect_expired_lease", func() {
		var databaseNow time.Time
		Expect(store.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow)).To(Succeed())
		creator := "creator-blocked-renewal"
		skill, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "blocked-renewal",
			CreatorSubject: creator, CreatedAt: databaseNow,
		})
		Expect(err).NotTo(HaveOccurred())
		generation, err := store.CreateGeneration(ctx, CreateGenerationInput{
			ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: creator,
			Snapshot: SkillRevisionSnapshot{
				Name: "Blocked renewal", Type: "workflow", Content: "# Blocked renewal",
			},
			AuthorContext:    "Do not resurrect an expired lease.",
			EvaluatorProfile: "generation-candidate-v1", EvaluatorProfileVersion: "1",
			EvaluationCriteria: json.RawMessage(`[{"id":"lease-fencing","kind":"content","description":"Fence expired work.","weight":1}]`),
			CreatedAt:          databaseNow.Add(-time.Minute),
		})
		Expect(err).NotTo(HaveOccurred())
		claim, err := store.ClaimGeneration(ctx, ClaimGenerationInput{
			WorkerID: "blocked-renewal-worker", LeaseDuration: time.Minute,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(claim).NotTo(BeNil())
		Expect(claim.ID).To(Equal(generation.ID))

		blocker, err := store.pool.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		blockerReleased := false
		DeferCleanup(func() {
			if !blockerReleased {
				_ = blocker.Rollback(context.Background())
			}
		})
		var lockedID string
		Expect(blocker.QueryRow(ctx, fmt.Sprintf(`SELECT id::text
			FROM %s.skill_generations WHERE id = $1 FOR UPDATE`,
			quoteIdentifier(schema)), generation.ID).Scan(&lockedID)).To(Succeed())
		Expect(lockedID).To(Equal(generation.ID))

		type renewalResult struct {
			renewed bool
			err     error
		}
		renewalResults := make(chan renewalResult, 1)
		go func() {
			renewed, renewErr := store.RenewGenerationLease(ctx, generation.ID, claim.ClaimToken, 2*time.Minute)
			renewalResults <- renewalResult{renewed: renewed, err: renewErr}
		}()

		renewalWaiters := func() int {
			var waiting int
			pattern := "%UPDATE " + quoteIdentifier(schema) + ".skill_generations AS generation SET%"
			queryErr := store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
				WHERE pid <> pg_backend_pid() AND datname = current_database()
				  AND state = 'active' AND wait_event_type = 'Lock' AND query LIKE $1`,
				pattern).Scan(&waiting)
			Expect(queryErr).NotTo(HaveOccurred())
			return waiting
		}
		Eventually(renewalWaiters).WithTimeout(5*time.Second).Should(BeNumerically(">=", 1),
			"renewal must be waiting on the target generation row")

		// Move the still-locked row's expiry just beyond the renewal's already
		// blocked start, then hold the lock until database time crosses it. An
		// UPDATE that sampled clock_timestamp before waiting would resurrect it.
		var crossedExpiry time.Time
		Expect(blocker.QueryRow(ctx, fmt.Sprintf(`UPDATE %s.skill_generations
			SET lease_expires_at = clock_timestamp() + interval '250 milliseconds'
			WHERE id = $1 RETURNING lease_expires_at`, quoteIdentifier(schema)),
			generation.ID).Scan(&crossedExpiry)).To(Succeed())
		Eventually(func() bool {
			var expired bool
			Expect(blocker.QueryRow(ctx, `SELECT clock_timestamp() >= $1`, crossedExpiry).
				Scan(&expired)).To(Succeed())
			return expired
		}).WithTimeout(2 * time.Second).Should(BeTrue())
		Expect(blocker.Commit(ctx)).To(Succeed())
		blockerReleased = true

		var result renewalResult
		Eventually(renewalResults).WithTimeout(5 * time.Second).Should(Receive(&result))
		Expect(result.err).NotTo(HaveOccurred())
		Expect(result.renewed).To(BeFalse(), "a waiter that crosses expiry must not renew stale work")
		var persistedLease, observedNow time.Time
		Expect(store.pool.QueryRow(ctx, fmt.Sprintf(`SELECT lease_expires_at, clock_timestamp()
			FROM %s.skill_generations WHERE id = $1`, quoteIdentifier(schema)), generation.ID).
			Scan(&persistedLease, &observedNow)).To(Succeed())
		Expect(persistedLease).To(BeTemporally("==", crossedExpiry), "failed renewal must not move the expired lease")
		Expect(persistedLease).To(BeTemporally("<=", observedNow))
	})

})

type persistedRevisionTestRecord struct {
	ID                string
	SkillID           string
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

func persistedRevisionForTest(ctx context.Context, store *PostgresStore, revisionID string) (persistedRevisionTestRecord, error) {
	var revision persistedRevisionTestRecord
	err := store.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT revision.id::text, revision.skill_id::text, revision.sequence_number,
		       revision.version, revision.creator_subject,
		       COALESCE(revision.based_on_revision_id::text, ''),
		       COALESCE(revision.source_revision_id::text, ''), revision.origin,
		       revision.name, revision.description, revision.type, revision.tags,
		       revision.content, revision.is_ai_generated, revision.source_session_ids,
		       revision.content_sha256, revision.change_note,
		       COALESCE(revision.generation_id::text, ''), revision.idempotency_key,
		       COALESCE(revision.legacy_reference, ''), revision.created_at,
		       visibility.is_public, visibility.changed_by_subject, visibility.changed_at
		FROM %s.skill_revisions revision
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
		WHERE revision.id = $1`, quoteIdentifier(store.schema), quoteIdentifier(store.schema)), revisionID).Scan(
		&revision.ID, &revision.SkillID, &revision.SequenceNumber, &revision.Version,
		&revision.CreatorSubject, &revision.BasedOnRevisionID, &revision.SourceRevisionID,
		&revision.Origin, &revision.Snapshot.Name, &revision.Snapshot.Description,
		&revision.Snapshot.Type, &revision.Snapshot.Tags, &revision.Snapshot.Content,
		&revision.Snapshot.IsAIGenerated, &revision.Snapshot.SourceSessionIDs,
		&revision.ContentSHA256, &revision.ChangeNote, &revision.GenerationID,
		&revision.IdempotencyKey, &revision.LegacyReference, &revision.CreatedAt,
		&revision.IsPublic, &revision.ChangedBySubject, &revision.ChangedAt,
	)
	return revision, err
}

func canonicalRevisionDigestForTest(snapshot SkillRevisionSnapshot) string {
	encoded, err := json.Marshal(snapshot)
	Expect(err).NotTo(HaveOccurred())
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// MakeRevisionPublicForContract seeds public visibility for the shared
// commit-2 revision contract without exercising the commit-3 metadata API.
// It lives in test code so production exposes no premature visibility writer.
func MakeRevisionPublicForContract(
	ctx context.Context,
	store RevisionStore,
	revisionID string,
	subject string,
	changedAt time.Time,
) error {
	switch concrete := store.(type) {
	case *MemoryStore:
		concrete.mu.Lock()
		defer concrete.mu.Unlock()
		visibility, ok := concrete.revisionVisibility[revisionID]
		if !ok {
			return ErrRevisionNotFound
		}
		visibility.IsPublic = true
		visibility.ChangedBySubject = subject
		visibility.ChangedAt = changedAt.UTC()
		concrete.revisionVisibility[revisionID] = visibility
		return nil
	case *PostgresStore:
		tag, err := concrete.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_revision_visibility
			SET is_public = TRUE, changed_by_subject = $2, changed_at = $3
			WHERE revision_id = $1`, quoteIdentifier(concrete.schema)), revisionID, subject, changedAt.UTC())
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrRevisionNotFound
		}
		return nil
	default:
		return fmt.Errorf("unsupported revision contract store %T", store)
	}
}

type revisionMetadataRaceResult struct {
	operation string
	err       error
}

type appendRevisionTestResult struct {
	revision *SkillRevisionRecord
	err      error
}

type generationFinalizationTestResult struct {
	revision *SkillRevisionRecord
	err      error
}

func finalizeGenerationForTest(
	ctx context.Context,
	store *PostgresStore,
	input AppendPrivateGenerationResultInput,
) (result generationFinalizationTestResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result.err = fmt.Errorf("append private generation result panicked: %v", recovered)
		}
	}()
	result.revision, result.err = store.AppendPrivateGenerationResult(ctx, input)
	return result
}

func appendRevisionForTest(ctx context.Context, store *PostgresStore, input AppendRevisionInput) (result appendRevisionTestResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result.err = fmt.Errorf("append revision panicked: %v", recovered)
		}
	}()
	result.revision, result.err = store.AppendRevision(ctx, input)
	return result
}

func postgresTestDSN() string {
	for _, name := range []string{"TEST_POSTGRES_DSN", "TAPES_TEST_POSTGRES_DSN", "TEST_DATABASE_URL"} {
		if dsn := os.Getenv(name); dsn != "" {
			return dsn
		}
	}
	return ""
}

func expectPostgresConstraint(err error, constraint string) {
	var postgresError *pgconn.PgError
	Expect(errors.As(err, &postgresError)).To(BeTrue(), "expected Postgres constraint %s, got %v", constraint, err)
	Expect(postgresError.Code).To(Equal("23503"), "constraint %s must reject the foreign-key write", constraint)
	Expect(postgresError.ConstraintName).To(Equal(constraint))
}

func expectPostgresForeignKeyViolation(err error) {
	var postgresError *pgconn.PgError
	Expect(errors.As(err, &postgresError)).To(BeTrue(), "expected Postgres foreign-key rejection, got %v", err)
	Expect(postgresError.Code).To(Equal("23503"))
}
