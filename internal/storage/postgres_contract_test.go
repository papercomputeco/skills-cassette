package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Postgres published identity contracts", func() {
	It("publishes only dynamic definer-rights identity mappings to the readers group", Serial, func() {
		dsn := postgresTestDSN()
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
		}
		ctx := context.Background()
		admin, err := pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(admin.Close)

		var readersRoleExists bool
		Expect(admin.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1
		)`, publishedIdentityContractReadRole).Scan(&readersRoleExists)).To(Succeed())
		if readersRoleExists {
			Skip(publishedIdentityContractReadRole + " already exists in the test cluster")
		}

		privateSchema := "skills_contract_source_" + uuid.NewString()[:8]
		futurePrivateSchema := privateSchema + "_future"
		quotedPrivateSchema := quoteIdentifier(privateSchema)
		quotedFuturePrivateSchema := quoteIdentifier(futurePrivateSchema)
		store, err := OpenPostgresStore(ctx, dsn, privateSchema)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			store.Close()
			_ = DropPrivateSchema(context.Background(), admin, privateSchema)
			_ = DropPrivateSchema(context.Background(), admin, futurePrivateSchema)
		})

		memberRole := "skills_contract_member_" + uuid.NewString()[:8]
		quotedReadersRole := quoteIdentifier(publishedIdentityContractReadRole)
		quotedMemberRole := quoteIdentifier(memberRole)
		var (
			contractLock       *pgxpool.Conn
			readersRoleCreated bool
		)
		DeferCleanup(func() {
			cleanupCtx := context.Background()
			var cleanupErr error
			if readersRoleCreated {
				_, _ = admin.Exec(cleanupCtx, "DROP OWNED BY "+quotedMemberRole)
				_, _ = admin.Exec(cleanupCtx, "DROP ROLE IF EXISTS "+quotedMemberRole)
				_, _ = admin.Exec(cleanupCtx, fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s
					REVOKE SELECT ON TABLES FROM %s`, quoteIdentifier(revisionIdentityContractSchema), quotedReadersRole))
				_, _ = admin.Exec(cleanupCtx, "DROP OWNED BY "+quotedReadersRole)
				_, cleanupErr = admin.Exec(cleanupCtx, "DROP ROLE "+quotedReadersRole)
			}
			if contractLock != nil {
				_, _ = contractLock.Exec(cleanupCtx,
					"SELECT pg_catalog.pg_advisory_unlock("+PublishedContractLockKey+")")
				contractLock.Release()
			}
			Expect(cleanupErr).NotTo(HaveOccurred())
		})

		By("skipping grants without creating the deployment-owned readers role")
		Expect(admin.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1
		)`, publishedIdentityContractReadRole).Scan(&readersRoleExists)).To(Succeed())
		Expect(readersRoleExists).To(BeFalse())

		type contractColumn struct {
			name       string
			dataType   string
			isNullable string
		}
		contractColumns := func(view string) []contractColumn {
			rows, queryErr := admin.Query(ctx, `SELECT column_name, data_type, is_nullable
				FROM information_schema.columns
				WHERE table_schema = $1 AND table_name = $2
				ORDER BY ordinal_position`, revisionIdentityContractSchema, view)
			Expect(queryErr).NotTo(HaveOccurred())
			defer rows.Close()
			var columns []contractColumn
			for rows.Next() {
				var column contractColumn
				Expect(rows.Scan(&column.name, &column.dataType, &column.isNullable)).To(Succeed())
				columns = append(columns, column)
			}
			Expect(rows.Err()).NotTo(HaveOccurred())
			return columns
		}

		type contractRelation struct {
			oid       int64
			kind      string
			options   []string
			schemaOID int64
		}
		contractRelations := func() map[string]contractRelation {
			rows, queryErr := admin.Query(ctx, `SELECT relation.relname, relation.oid::bigint,
				relation.relkind::text, COALESCE(relation.reloptions, ARRAY[]::text[]),
				namespace.oid::bigint
				FROM pg_catalog.pg_class AS relation
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				WHERE namespace.nspname = $1
				ORDER BY relation.relname`, revisionIdentityContractSchema)
			Expect(queryErr).NotTo(HaveOccurred())
			defer rows.Close()
			relations := make(map[string]contractRelation)
			for rows.Next() {
				var name string
				var relation contractRelation
				Expect(rows.Scan(&name, &relation.oid, &relation.kind, &relation.options, &relation.schemaOID)).To(Succeed())
				relations[name] = relation
			}
			Expect(rows.Err()).NotTo(HaveOccurred())
			return relations
		}
		By("granting through the fixed group only after deployment creates it")
		_, err = admin.Exec(ctx, "CREATE ROLE "+quotedReadersRole+" NOLOGIN")
		Expect(err).NotTo(HaveOccurred())
		readersRoleCreated = true
		_, err = admin.Exec(ctx, "CREATE ROLE "+quotedMemberRole+" NOLOGIN IN ROLE "+quotedReadersRole)
		Expect(err).NotTo(HaveOccurred())

		Expect(store.migrate(ctx)).To(Succeed())
		Expect(store.migrate(ctx)).To(Succeed())

		// Other packages use isolated private schemas in the same test database.
		// Hold the lock every production migration uses, then refresh through the
		// production statement builder so another package cannot repoint this fixed
		// namespace or drop a source schema out from under these assertions.
		contractLock, err = admin.Acquire(ctx)
		Expect(err).NotTo(HaveOccurred())
		_, err = contractLock.Exec(ctx,
			"SELECT pg_catalog.pg_advisory_lock("+PublishedContractLockKey+")")
		Expect(err).NotTo(HaveOccurred())
		for _, statement := range publishedIdentityContractStatements(quotedPrivateSchema) {
			_, err = contractLock.Exec(ctx, statement)
			Expect(err).NotTo(HaveOccurred())
		}

		By("installing exactly two narrow definer-rights views")
		Expect(contractColumns(skillIdentityContractView)).To(Equal([]contractColumn{
			{name: "predecessor_skill_id", dataType: "uuid", isNullable: "YES"},
			{name: "skill_id", dataType: "uuid", isNullable: "YES"},
		}))
		Expect(contractColumns(revisionIdentityContractView)).To(Equal([]contractColumn{
			{name: "skill_id", dataType: "uuid", isNullable: "YES"},
			{name: "revision_id", dataType: "uuid", isNullable: "YES"},
			{name: "predecessor_skill_id", dataType: "uuid", isNullable: "YES"},
			{name: "predecessor_version_number", dataType: "integer", isNullable: "YES"},
		}))
		beforeRetry := contractRelations()
		Expect(beforeRetry).To(HaveLen(2))
		Expect(beforeRetry).To(HaveKey(skillIdentityContractView))
		Expect(beforeRetry).To(HaveKey(revisionIdentityContractView))
		for name, relation := range beforeRetry {
			Expect(relation.kind).To(Equal("v"), name)
			Expect(relation.options).To(ContainElement("security_invoker=false"),
				"published view %s must evaluate private-schema access with its owner's rights", name)
		}
		for _, statement := range publishedIdentityContractStatements(quotedPrivateSchema) {
			_, err = contractLock.Exec(ctx, statement)
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(contractRelations()).To(Equal(beforeRetry),
			"idempotent retries must preserve the schema, views, and definer-rights options")

		var isMember bool
		Expect(admin.QueryRow(ctx, `SELECT pg_has_role($1, $2, 'MEMBER')`,
			memberRole, publishedIdentityContractReadRole).Scan(&isMember)).To(Succeed())
		Expect(isMember).To(BeTrue())
		for _, principal := range []string{publishedIdentityContractReadRole, memberRole} {
			var schemaUsage, skillSelect, revisionSelect bool
			Expect(admin.QueryRow(ctx, `SELECT
				has_schema_privilege($1, $2, 'USAGE'),
				has_table_privilege($1, to_regclass($2 || '.skill_identities'), 'SELECT'),
				has_table_privilege($1, to_regclass($2 || '.revision_identities'), 'SELECT')`,
				principal, revisionIdentityContractSchema).Scan(
				&schemaUsage, &skillSelect, &revisionSelect)).To(Succeed())
			Expect(schemaUsage).To(BeTrue(), principal)
			Expect(skillSelect).To(BeTrue(), principal)
			Expect(revisionSelect).To(BeTrue(), principal)
		}

		var memberDirectViewGrants int
		Expect(admin.QueryRow(ctx, `SELECT count(*)
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
			CROSS JOIN LATERAL aclexplode(COALESCE(relation.relacl, acldefault('r', relation.relowner))) AS privilege
			JOIN pg_catalog.pg_roles AS grantee ON grantee.oid = privilege.grantee
			WHERE namespace.nspname = $1
			  AND relation.relname IN ('skill_identities', 'revision_identities')
			  AND privilege.privilege_type = 'SELECT'
			  AND grantee.rolname = $2`, revisionIdentityContractSchema, memberRole).Scan(
			&memberDirectViewGrants)).To(Succeed())
		Expect(memberDirectViewGrants).To(BeZero(),
			"the restricted reader must receive access only through group membership")

		By("applying matching defaults only inside the published schema")
		futurePublishedView := "future_identity_probe_" + uuid.NewString()[:8]
		quotedFuturePublishedView := quoteIdentifier(futurePublishedView)
		_, err = admin.Exec(ctx, fmt.Sprintf(`CREATE VIEW %s.%s AS
			SELECT NULL::UUID AS identity WHERE FALSE`, quoteIdentifier(revisionIdentityContractSchema),
			quotedFuturePublishedView))
		Expect(err).NotTo(HaveOccurred())
		_, err = admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s;
			CREATE TABLE %s.private_identity_probe (identity UUID)`,
			quotedFuturePrivateSchema, quotedFuturePrivateSchema))
		Expect(err).NotTo(HaveOccurred())
		var futurePublishedSelect, futurePrivateUsage, futurePrivateSelect bool
		Expect(admin.QueryRow(ctx, `SELECT
			has_table_privilege($1, to_regclass($2), 'SELECT'),
			has_schema_privilege($1, $3, 'USAGE'),
			has_table_privilege($1, to_regclass($3 || '.private_identity_probe'), 'SELECT')`,
			memberRole, revisionIdentityContractSchema+"."+futurePublishedView, futurePrivateSchema).Scan(
			&futurePublishedSelect, &futurePrivateUsage, &futurePrivateSelect)).To(Succeed())
		Expect(futurePublishedSelect).To(BeTrue())
		Expect(futurePrivateUsage).To(BeFalse())
		Expect(futurePrivateSelect).To(BeFalse())
		_, err = admin.Exec(ctx, fmt.Sprintf("DROP VIEW %s.%s",
			quoteIdentifier(revisionIdentityContractSchema), quotedFuturePublishedView))
		Expect(err).NotTo(HaveOccurred())
		Expect(contractRelations()).To(Equal(beforeRetry),
			"the published namespace must contain no undeclared base relations")

		By("mapping canonical and slug-coalesced predecessor identities dynamically")
		const creator = "identity-contract-owner"
		now := time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)
		skill, err := store.ResolveSkill(ctx, ResolveSkillInput{
			ID: uuid.NewString(), Slug: "published-identity-contracts",
			CreatorSubject: creator, CreatedAt: now,
		})
		Expect(err).NotTo(HaveOccurred())
		aliasID := uuid.NewString()
		_, err = admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skills (
			id, slug, name, author_subject, created_by_subject, created_at, updated_at,
			migration_alias_of_skill_id
		) VALUES ($1, $2, 'Migrated alias', $3, $3, $4, $4, $5)`, quotedPrivateSchema),
			aliasID, skill.Slug, creator, now.Add(time.Minute), skill.ID)
		Expect(err).NotTo(HaveOccurred())

		identityRows, err := admin.Query(ctx, fmt.Sprintf(`SELECT predecessor_skill_id::text, skill_id::text
			FROM %s.%s WHERE skill_id = $1 ORDER BY predecessor_skill_id`,
			quoteIdentifier(revisionIdentityContractSchema), quoteIdentifier(skillIdentityContractView)), skill.ID)
		Expect(err).NotTo(HaveOccurred())
		skillIdentities := make(map[string]string)
		for identityRows.Next() {
			var predecessorSkillID, canonicalSkillID string
			Expect(identityRows.Scan(&predecessorSkillID, &canonicalSkillID)).To(Succeed())
			skillIdentities[predecessorSkillID] = canonicalSkillID
		}
		Expect(identityRows.Err()).NotTo(HaveOccurred())
		identityRows.Close()
		Expect(skillIdentities).To(Equal(map[string]string{
			skill.ID: skill.ID,
			aliasID:  skill.ID,
		}))

		appendRevision := func(legacyReference string, ordinal int) string {
			revision, appendErr := store.AppendRevision(ctx, AppendRevisionInput{
				ID: uuid.NewString(), SkillID: skill.ID, CreatorSubject: creator,
				Origin: RevisionOriginMigrated,
				Snapshot: SkillRevisionSnapshot{
					Name: fmt.Sprintf("Contract revision %d", ordinal), Type: "workflow",
					Content: fmt.Sprintf("# Contract revision %d", ordinal),
				},
				IdempotencyKey:  fmt.Sprintf("identity-contract-%d", ordinal),
				LegacyReference: legacyReference,
				CreatedAt:       now.Add(time.Duration(ordinal+1) * time.Minute),
			})
			Expect(appendErr).NotTo(HaveOccurred())
			return revision.ID
		}
		modernRevisionID := appendRevision("", 1)
		canonicalRevisionID := appendRevision("skill-version:"+skill.ID+":7", 2)
		aliasRevisionID := appendRevision("skill-version:"+strings.ToUpper(aliasID)+":8", 3)
		unmappedRevisionID := appendRevision("skill-version:"+uuid.NewString()+":9", 4)

		type revisionIdentity struct {
			skillID            string
			predecessorSkillID string
			predecessorVersion pgtype.Int4
		}
		revisionRows, err := admin.Query(ctx, fmt.Sprintf(`SELECT skill_id::text, revision_id::text,
			COALESCE(predecessor_skill_id::text, ''), predecessor_version_number
			FROM %s.%s WHERE skill_id = $1`, quoteIdentifier(revisionIdentityContractSchema),
			quoteIdentifier(revisionIdentityContractView)), skill.ID)
		Expect(err).NotTo(HaveOccurred())
		revisionIdentities := make(map[string]revisionIdentity)
		for revisionRows.Next() {
			var revisionID string
			var identity revisionIdentity
			Expect(revisionRows.Scan(&identity.skillID, &revisionID,
				&identity.predecessorSkillID, &identity.predecessorVersion)).To(Succeed())
			revisionIdentities[revisionID] = identity
		}
		Expect(revisionRows.Err()).NotTo(HaveOccurred())
		revisionRows.Close()
		Expect(revisionIdentities).To(HaveLen(4))
		Expect(revisionIdentities[modernRevisionID]).To(Equal(revisionIdentity{skillID: skill.ID}))
		Expect(revisionIdentities[canonicalRevisionID]).To(Equal(revisionIdentity{
			skillID: skill.ID, predecessorSkillID: skill.ID,
			predecessorVersion: pgtype.Int4{Int32: 7, Valid: true},
		}))
		Expect(revisionIdentities[aliasRevisionID]).To(Equal(revisionIdentity{
			skillID: skill.ID, predecessorSkillID: aliasID,
			predecessorVersion: pgtype.Int4{Int32: 8, Valid: true},
		}), "a slug-coalesced predecessor UUID must remain recoverable beside the canonical skill UUID")
		Expect(revisionIdentities[unmappedRevisionID]).To(Equal(revisionIdentity{skillID: skill.ID}),
			"an unrecognized predecessor identity must not become a trusted mapping")

		By("reading both contracts as a restricted group member without private access")
		restricted, err := admin.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		_, err = restricted.Exec(ctx, "SET LOCAL ROLE "+quotedMemberRole)
		Expect(err).NotTo(HaveOccurred())
		var restrictedCanonicalSkillID string
		Expect(restricted.QueryRow(ctx, fmt.Sprintf(`SELECT skill_id::text FROM %s.%s
			WHERE predecessor_skill_id = $1`, quoteIdentifier(revisionIdentityContractSchema),
			quoteIdentifier(skillIdentityContractView)), aliasID).Scan(&restrictedCanonicalSkillID)).To(Succeed())
		Expect(restrictedCanonicalSkillID).To(Equal(skill.ID))
		var restrictedPredecessorSkillID string
		var restrictedPredecessorVersion int
		Expect(restricted.QueryRow(ctx, fmt.Sprintf(`SELECT predecessor_skill_id::text,
			predecessor_version_number FROM %s.%s WHERE revision_id = $1`,
			quoteIdentifier(revisionIdentityContractSchema), quoteIdentifier(revisionIdentityContractView)),
			aliasRevisionID).Scan(&restrictedPredecessorSkillID, &restrictedPredecessorVersion)).To(Succeed())
		Expect(restrictedPredecessorSkillID).To(Equal(aliasID))
		Expect(restrictedPredecessorVersion).To(Equal(8))
		Expect(restricted.Commit(ctx)).To(Succeed())

		for _, principal := range []string{publishedIdentityContractReadRole, memberRole} {
			var privateSchemaUsage, privateTableSelect bool
			Expect(admin.QueryRow(ctx, `SELECT
				has_schema_privilege($1, $2, 'USAGE'),
				has_table_privilege($1, relation.oid, 'SELECT')
				FROM pg_catalog.pg_class AS relation
				JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = relation.relnamespace
				WHERE namespace.nspname = $2 AND relation.relname = 'skill_revisions'`,
				principal, privateSchema).Scan(&privateSchemaUsage, &privateTableSelect)).To(Succeed())
			Expect(privateSchemaUsage).To(BeFalse(), principal)
			Expect(privateTableSelect).To(BeFalse(), principal)
		}

		denied, err := admin.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		_, err = denied.Exec(ctx, "SET LOCAL ROLE "+quotedMemberRole)
		Expect(err).NotTo(HaveOccurred())
		_, err = denied.Exec(ctx, fmt.Sprintf("SELECT id FROM %s.skill_revisions LIMIT 1", quotedPrivateSchema))
		Expect(err).To(HaveOccurred())
		var pgErr *pgconn.PgError
		Expect(errors.As(err, &pgErr)).To(BeTrue())
		Expect(pgErr.Code).To(Equal("42501"))
		Expect(denied.Rollback(ctx)).To(Succeed())
	})
})
