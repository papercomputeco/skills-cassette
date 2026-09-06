package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// postgresMigration describes the additive DDL, data conversion, verification,
// and deferred constraints of a named migration.
type postgresMigration struct {
	name     string
	expand   []string
	backfill func(context.Context, pgx.Tx) error
	verify   func(context.Context, pgx.Tx) error
	contract []string
}

// migrate upgrades the cassette-owned schema while serializing concurrent
// cassette startups with a schema-scoped advisory transaction lock. Every
// statement is idempotent because installations can jump directly from any
// previously shipped schema to the current shape.
func (s *PostgresStore) migrate(ctx context.Context) (err error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin skills migration: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, s.schema); err != nil {
		return fmt.Errorf("lock skills migration: %w", err)
	}

	contentOnlyVersionShape, err := legacySkillVersionsHaveContentOnlyShape(ctx, tx, s.schema)
	if err != nil {
		return err
	}

	schema := quoteIdentifier(s.schema)
	// Predecessor tables and columns below are a migration-only compatibility
	// inbox. Current runtime APIs never address them, but a rolling-upgrade peer
	// may retain writes there until cutover; each advisory-locked idempotent rerun
	// ingests those writes before modern relationships are verified. Do not use
	// this physical compatibility shape as a second application model.
	statements := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + schema,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skills (
			id                         UUID NOT NULL,
			slug                       TEXT NOT NULL,
			name                       TEXT NOT NULL,
			description                TEXT NOT NULL DEFAULT '',
			type                       TEXT NOT NULL DEFAULT 'workflow',
			version                    TEXT NOT NULL DEFAULT '0.1.0',
			visibility                 TEXT NOT NULL DEFAULT 'private',
			tags                       TEXT[] NOT NULL DEFAULT '{}',
			content                    TEXT NOT NULL DEFAULT '',
			is_ai_generated            BOOLEAN NOT NULL DEFAULT FALSE,
			generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}',
			parent_id                  UUID,
			author_subject             TEXT NOT NULL DEFAULT '',
			download_count             BIGINT NOT NULL DEFAULT 0,
			current_version_number     INT,
			created_at                 TIMESTAMPTZ NOT NULL,
			updated_at                 TIMESTAMPTZ NOT NULL,
			CONSTRAINT skills_pkey PRIMARY KEY (id)
		)`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skills ADD COLUMN IF NOT EXISTS current_version_number INT`, schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skills_updated_idx ON %s.skills (updated_at DESC, id DESC)`, schema),
		// A predecessor published every inserted skill as v0.1.0 through an AFTER
		// INSERT trigger on skills. Under the unified model a skills row is an
		// empty identity, so that trigger would turn every resolved slug into a
		// bogus content-less publication and a rerun would promote it to a public
		// revision. Drop it before any statement below inserts into skills or
		// skill_versions; the trigger must go first because it owns the function.
		fmt.Sprintf(`DROP TRIGGER IF EXISTS publish_initial_skill_version ON %s.skills`, schema),
		fmt.Sprintf(`DROP FUNCTION IF EXISTS %s.publish_initial_skill_version()`, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skill_versions (
			skill_id                       UUID NOT NULL,
			version_number                 INT NOT NULL,
			semver                         TEXT NOT NULL,
			changelog                      TEXT NOT NULL DEFAULT '',
			slug                           TEXT NOT NULL DEFAULT '',
			name                           TEXT NOT NULL DEFAULT '',
			description                    TEXT NOT NULL DEFAULT '',
			type                           TEXT NOT NULL DEFAULT 'workflow',
			visibility                     TEXT NOT NULL DEFAULT 'private',
			tags                           TEXT[] NOT NULL DEFAULT '{}',
			content                        TEXT NOT NULL DEFAULT '',
			is_ai_generated                BOOLEAN NOT NULL DEFAULT FALSE,
			generated_from_session_ids     TEXT[] NOT NULL DEFAULT '{}',
			parent_id                      UUID,
			content_sha256                 TEXT NOT NULL DEFAULT '',
			expected_content               TEXT,
			author_subject                 TEXT NOT NULL DEFAULT '',
			published_at                   TIMESTAMPTZ NOT NULL,
			CONSTRAINT skill_versions_pkey PRIMARY KEY (skill_id, version_number)
		)`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS expected_content TEXT`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS slug TEXT NOT NULL DEFAULT ''`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT ''`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'workflow'`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS visibility TEXT NOT NULL DEFAULT 'private'`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}'`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}'`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS parent_id UUID`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS content_sha256 TEXT NOT NULL DEFAULT ''`, schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_versions_skill_idx ON %s.skill_versions (skill_id, version_number DESC)`, schema),
		// Current-main stored unsaved work in the mutable skills row. Keep each
		// distinguishable head in a migration-owned immutable source before the
		// compatibility head is reset to its selected published version. This is
		// deliberately independent of skill_drafts: an existing open target draft
		// must not make current-main recovery disappear behind its unique index.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skill_revision_migration_sources (
			legacy_reference           TEXT NOT NULL,
			skill_id                  UUID NOT NULL,
			base_skill_version_number INT NOT NULL,
			slug                      TEXT NOT NULL,
			name                      TEXT NOT NULL,
			description               TEXT NOT NULL DEFAULT '',
			type                      TEXT NOT NULL DEFAULT 'workflow',
			tags                      TEXT[] NOT NULL DEFAULT '{}',
			content                   TEXT NOT NULL DEFAULT '',
			is_ai_generated           BOOLEAN NOT NULL DEFAULT FALSE,
			source_session_ids        TEXT[] NOT NULL DEFAULT '{}',
			parent_id                 UUID,
			creator_subject           TEXT NOT NULL,
			snapshot_sha256           TEXT NOT NULL,
			created_at                TIMESTAMPTZ NOT NULL,
			CONSTRAINT skill_revision_migration_sources_pkey PRIMARY KEY (legacy_reference),
			CONSTRAINT skill_revision_migration_sources_skill_fkey FOREIGN KEY (skill_id) REFERENCES %s.skills(id) ON DELETE CASCADE
		)`, schema, schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_revision_migration_sources_skill_idx
			ON %s.skill_revision_migration_sources (skill_id, created_at, legacy_reference)`, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skill_drafts (
			id                         UUID NOT NULL,
			target_skill_id            UUID,
			owner_subject              TEXT NOT NULL,
			status                     TEXT NOT NULL DEFAULT 'open',
			base_skill_version_number  INT,
			lock_version               BIGINT NOT NULL DEFAULT 1,
			working_revision_number    INT,
			working_slug                TEXT NOT NULL,
			working_name                TEXT NOT NULL,
			working_description         TEXT NOT NULL DEFAULT '',
			working_type                TEXT NOT NULL DEFAULT 'workflow',
			working_tags                TEXT[] NOT NULL DEFAULT '{}',
			working_content             TEXT NOT NULL DEFAULT '',
			working_is_ai_generated     BOOLEAN NOT NULL DEFAULT FALSE,
			working_source_session_ids  TEXT[] NOT NULL DEFAULT '{}',
			working_parent_id           UUID,
			author_context              TEXT NOT NULL DEFAULT '',
			published_skill_id          UUID,
			published_version_number    INT,
			created_at                  TIMESTAMPTZ NOT NULL,
			updated_at                  TIMESTAMPTZ NOT NULL,
			published_at                TIMESTAMPTZ,
			CONSTRAINT skill_drafts_pkey PRIMARY KEY (id),
			CONSTRAINT skill_drafts_target_fkey FOREIGN KEY (target_skill_id) REFERENCES %s.skills(id) ON DELETE SET NULL,
			CONSTRAINT skill_drafts_published_fkey FOREIGN KEY (published_skill_id) REFERENCES %s.skills(id) ON DELETE SET NULL,
			CONSTRAINT skill_drafts_status_check CHECK (status IN ('open', 'published'))
		)`, schema, schema, schema),
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS one_open_draft_per_skill ON %s.skill_drafts (target_skill_id) WHERE status = 'open' AND target_skill_id IS NOT NULL`, schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_drafts_owner_idx ON %s.skill_drafts (owner_subject, updated_at DESC, id DESC)`, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.draft_sessions (
			draft_id UUID NOT NULL,
			session_id TEXT NOT NULL,
			ordinal INT NOT NULL,
			CONSTRAINT draft_sessions_pkey PRIMARY KEY (draft_id, session_id),
			CONSTRAINT draft_sessions_draft_fkey FOREIGN KEY (draft_id) REFERENCES %s.skill_drafts(id) ON DELETE CASCADE,
			CONSTRAINT draft_sessions_ordinal_key UNIQUE (draft_id, ordinal)
		)`, schema, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.draft_revisions (
			draft_id                    UUID NOT NULL,
			revision_number             INT NOT NULL,
			origin                      TEXT NOT NULL,
			generation_id               UUID,
			idempotency_key             TEXT NOT NULL DEFAULT '',
			slug                        TEXT NOT NULL,
			name                        TEXT NOT NULL,
			description                 TEXT NOT NULL DEFAULT '',
			type                        TEXT NOT NULL DEFAULT 'workflow',
			tags                        TEXT[] NOT NULL DEFAULT '{}',
			content                     TEXT NOT NULL DEFAULT '',
			is_ai_generated             BOOLEAN NOT NULL DEFAULT FALSE,
			source_session_ids          TEXT[] NOT NULL DEFAULT '{}',
			parent_id                   UUID,
			content_sha256              TEXT NOT NULL,
			created_at                  TIMESTAMPTZ NOT NULL,
			CONSTRAINT draft_revisions_pkey PRIMARY KEY (draft_id, revision_number),
			CONSTRAINT draft_revisions_draft_fkey FOREIGN KEY (draft_id) REFERENCES %s.skill_drafts(id) ON DELETE CASCADE,
			CONSTRAINT draft_revisions_origin_check CHECK (origin IN ('initial', 'manual', 'generation'))
		)`, schema, schema),
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS draft_revisions_idempotency_key ON %s.draft_revisions (draft_id, idempotency_key) WHERE idempotency_key <> ''`, schema),
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS draft_revisions_generation_id ON %s.draft_revisions (draft_id, generation_id) WHERE generation_id IS NOT NULL`, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skill_generations (
			id                         UUID NOT NULL,
			draft_id                   UUID NOT NULL,
			owner_subject              TEXT NOT NULL,
			status                     TEXT NOT NULL,
			starting_lock_version      BIGINT NOT NULL,
			input_slug                 TEXT NOT NULL,
			input_name                 TEXT NOT NULL,
			input_description          TEXT NOT NULL DEFAULT '',
			input_type                 TEXT NOT NULL DEFAULT 'workflow',
			input_tags                 TEXT[] NOT NULL DEFAULT '{}',
			input_content              TEXT NOT NULL DEFAULT '',
			input_is_ai_generated      BOOLEAN NOT NULL DEFAULT FALSE,
			input_source_session_ids   TEXT[] NOT NULL DEFAULT '{}',
			input_parent_id            UUID,
			author_context             TEXT NOT NULL DEFAULT '',
			selected_session_ids       TEXT[] NOT NULL DEFAULT '{}',
			evaluator_profile          TEXT NOT NULL,
			evaluator_profile_version  TEXT NOT NULL DEFAULT '',
			evaluation_criteria        JSONB NOT NULL DEFAULT '[]'::jsonb,
			winner_candidate_id        UUID,
			proposed_candidate_id      UUID,
			proposed_revision_number   INT,
			resolution                 TEXT NOT NULL DEFAULT '',
			error_code                 VARCHAR(128) NOT NULL DEFAULT '',
			error_message              VARCHAR(1024) NOT NULL DEFAULT '',
			claim_token                UUID,
			claim_owner                TEXT NOT NULL DEFAULT '',
			lease_expires_at           TIMESTAMPTZ,
			attempt_count              INT NOT NULL DEFAULT 0,
			next_attempt_at            TIMESTAMPTZ NOT NULL,
			last_heartbeat_at          TIMESTAMPTZ,
			created_at                 TIMESTAMPTZ NOT NULL,
			updated_at                 TIMESTAMPTZ NOT NULL,
			started_at                 TIMESTAMPTZ,
			completed_at               TIMESTAMPTZ,
			CONSTRAINT skill_generations_pkey PRIMARY KEY (id),
			CONSTRAINT skill_generations_draft_fkey FOREIGN KEY (draft_id) REFERENCES %s.skill_drafts(id) ON DELETE CASCADE,
			CONSTRAINT skill_generations_status_check CHECK (status IN (
				'queued', 'generating_candidates', 'evaluating_candidates',
				'synthesizing', 'awaiting_input', 'completed', 'canceled', 'failed'
			))
		)`, schema, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_generations
			ADD COLUMN IF NOT EXISTS evaluator_profile_version TEXT NOT NULL DEFAULT ''`, schema),
		fmt.Sprintf(`ALTER TABLE %s.skill_generations
			ADD COLUMN IF NOT EXISTS evaluation_criteria JSONB NOT NULL DEFAULT '[]'::jsonb`, schema),
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS one_nonterminal_generation_per_draft
			ON %s.skill_generations (draft_id)
			WHERE status IN ('queued', 'generating_candidates', 'evaluating_candidates', 'synthesizing', 'awaiting_input')`, schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_generations_claim_idx
			ON %s.skill_generations (next_attempt_at, lease_expires_at, created_at)
			WHERE status IN ('queued', 'generating_candidates', 'evaluating_candidates', 'synthesizing')`, schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_generations_owner_idx
			ON %s.skill_generations (owner_subject, draft_id, created_at DESC, id DESC)`, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.generation_sessions (
			generation_id  UUID NOT NULL,
			session_id     TEXT NOT NULL,
			ordinal        INT NOT NULL,
			status         TEXT NOT NULL DEFAULT 'pending',
			candidate_id   UUID,
			diagnostic_code VARCHAR(128) NOT NULL DEFAULT '',
			updated_at     TIMESTAMPTZ NOT NULL,
			CONSTRAINT generation_sessions_pkey PRIMARY KEY (generation_id, session_id),
			CONSTRAINT generation_sessions_generation_fkey FOREIGN KEY (generation_id) REFERENCES %s.skill_generations(id) ON DELETE CASCADE,
			CONSTRAINT generation_sessions_ordinal_key UNIQUE (generation_id, ordinal),
			CONSTRAINT generation_sessions_status_check CHECK (status IN (
				'pending', 'transcript_failed', 'candidate_failed', 'candidate_ready',
				'evaluation_failed', 'evaluated'
			))
		)`, schema, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.generation_candidates (
			id                     UUID NOT NULL,
			generation_id          UUID NOT NULL,
			ordinal                INT NOT NULL,
			kind                   TEXT NOT NULL,
			source_session_ids     TEXT[] NOT NULL DEFAULT '{}',
			slug                   TEXT NOT NULL,
			name                   TEXT NOT NULL,
			description            TEXT NOT NULL DEFAULT '',
			type                   TEXT NOT NULL DEFAULT 'workflow',
			tags                   TEXT[] NOT NULL DEFAULT '{}',
			content                TEXT NOT NULL DEFAULT '',
			is_ai_generated        BOOLEAN NOT NULL DEFAULT TRUE,
			parent_id              UUID,
			insights               JSONB NOT NULL DEFAULT '[]'::jsonb,
			bundle_sha256          TEXT NOT NULL,
			created_at             TIMESTAMPTZ NOT NULL,
			CONSTRAINT generation_candidates_pkey PRIMARY KEY (id),
			CONSTRAINT generation_candidates_generation_fkey FOREIGN KEY (generation_id) REFERENCES %s.skill_generations(id) ON DELETE CASCADE,
			CONSTRAINT generation_candidates_ordinal_key UNIQUE (generation_id, ordinal),
			CONSTRAINT generation_candidates_generation_id_id_key UNIQUE (generation_id, id),
			CONSTRAINT generation_candidates_kind_check CHECK (kind IN ('session', 'context', 'synthesis'))
		)`, schema, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.candidate_evaluations (
			id                       UUID NOT NULL,
			generation_id            UUID NOT NULL,
			candidate_id             UUID NOT NULL,
			request_sha256           TEXT NOT NULL,
			profile                  TEXT NOT NULL,
			profile_version          TEXT NOT NULL DEFAULT '',
			evaluator_version        TEXT NOT NULL DEFAULT '',
			score                    DOUBLE PRECISION,
			decision                 TEXT NOT NULL DEFAULT '',
			critical_finding_count   INT NOT NULL DEFAULT 0,
			warning_finding_count    INT NOT NULL DEFAULT 0,
			criterion_results        JSONB NOT NULL DEFAULT '[]'::jsonb,
			findings                 JSONB NOT NULL DEFAULT '[]'::jsonb,
			strengths                JSONB NOT NULL DEFAULT '[]'::jsonb,
			panel                    JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at               TIMESTAMPTZ NOT NULL,
			CONSTRAINT candidate_evaluations_pkey PRIMARY KEY (id),
			CONSTRAINT candidate_evaluations_generation_fkey FOREIGN KEY (generation_id) REFERENCES %s.skill_generations(id) ON DELETE CASCADE,
			CONSTRAINT candidate_evaluations_candidate_fkey FOREIGN KEY (candidate_id) REFERENCES %s.generation_candidates(id) ON DELETE CASCADE,
			CONSTRAINT candidate_evaluations_request_key UNIQUE (candidate_id, request_sha256)
		)`, schema, schema, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.generation_diagnostics (
			id             UUID NOT NULL,
			generation_id  UUID NOT NULL,
			session_id     TEXT NOT NULL DEFAULT '',
			candidate_id   UUID,
			stage          VARCHAR(64) NOT NULL,
			code           VARCHAR(128) NOT NULL,
			message        VARCHAR(1024) NOT NULL,
			retryable      BOOLEAN NOT NULL DEFAULT FALSE,
			created_at     TIMESTAMPTZ NOT NULL,
			CONSTRAINT generation_diagnostics_pkey PRIMARY KEY (id),
			CONSTRAINT generation_diagnostics_generation_fkey FOREIGN KEY (generation_id) REFERENCES %s.skill_generations(id) ON DELETE CASCADE
		)`, schema, schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS generation_diagnostics_generation_idx
			ON %s.generation_diagnostics (generation_id, created_at, id)`, schema),
	}
	for _, statement := range statements {
		if _, err = tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("migrating skills tables: %w", err)
		}
	}
	if err = s.normalizeCandidateEvaluationIDColumn(ctx, tx); err != nil {
		return err
	}

	var durableTargetExists bool
	if err = tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, s.schema+".skill_revisions").Scan(&durableTargetExists); err != nil {
		return fmt.Errorf("inspect durable revision migration target: %w", err)
	}
	// Only the first pass recovers old direct-create rows as published
	// versions. Every pass still snapshots and resets mutable predecessor
	// heads: an older binary may retain writes during rolling cutover, while a
	// deliberately empty stable identity must never become a publication.
	if err = s.backfillSnapshots(ctx, tx, !durableTargetExists, contentOnlyVersionShape); err != nil {
		return err
	}

	migration := s.durableRevisionIdentityMigration()
	if err = executePostgresMigration(ctx, tx, migration); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit skills migration: %w", err)
	}
	return nil
}

func legacySkillVersionsHaveContentOnlyShape(ctx context.Context, tx pgx.Tx, schema string) (bool, error) {
	var contentOnly bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL AND NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = $2 AND table_name = 'skill_versions' AND column_name = 'name'
	)`, schema+".skill_versions", schema).Scan(&contentOnly); err != nil {
		return false, fmt.Errorf("inspect predecessor skill version shape: %w", err)
	}
	return contentOnly, nil
}

// normalizeCandidateEvaluationIDColumn upgrades the brief predecessor shape
// that accepted arbitrary text evaluation IDs. Canonical UUID text is retained;
// every other ID receives a deterministic, collision-fenced UUID before the
// column becomes UUID-typed. Later artifact sanitization still drops rows whose
// candidate relationship or structured judgment is unsafe.
func (s *PostgresStore) normalizeCandidateEvaluationIDColumn(ctx context.Context, tx pgx.Tx) error {
	var dataType string
	if err := tx.QueryRow(ctx, `SELECT udt_name FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = 'candidate_evaluations' AND column_name = 'id'`, s.schema).
		Scan(&dataType); err != nil {
		return fmt.Errorf("inspect candidate evaluation id type: %w", err)
	}
	if dataType == "uuid" {
		return nil
	}
	if dataType != "text" && dataType != "varchar" {
		return fmt.Errorf("candidate evaluation id has unsupported predecessor type %q", dataType)
	}

	type predecessorEvaluationID struct {
		rawID, generationID, candidateID, requestSHA256 string
	}
	schema := quoteIdentifier(s.schema)
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT id::text, generation_id::text,
		candidate_id::text, request_sha256 FROM %s.candidate_evaluations
		ORDER BY id::text, generation_id, candidate_id, request_sha256`, schema))
	if err != nil {
		return fmt.Errorf("query predecessor candidate evaluation ids: %w", err)
	}
	var predecessorIDs []predecessorEvaluationID
	for rows.Next() {
		var evaluationID predecessorEvaluationID
		if err = rows.Scan(&evaluationID.rawID, &evaluationID.generationID,
			&evaluationID.candidateID, &evaluationID.requestSHA256); err != nil {
			rows.Close()
			return fmt.Errorf("scan predecessor candidate evaluation id: %w", err)
		}
		predecessorIDs = append(predecessorIDs, evaluationID)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate predecessor candidate evaluation ids: %w", err)
	}
	rows.Close()

	rawIDs := make(map[string]struct{}, len(predecessorIDs))
	reserved := make(map[string]struct{}, len(predecessorIDs))
	for _, evaluationID := range predecessorIDs {
		rawIDs[evaluationID.rawID] = struct{}{}
		if parsed, parseErr := uuid.Parse(evaluationID.rawID); parseErr == nil && evaluationID.rawID == parsed.String() {
			reserved[evaluationID.rawID] = struct{}{}
		}
	}
	for _, evaluationID := range predecessorIDs {
		parsed, parseErr := uuid.Parse(evaluationID.rawID)
		if parseErr == nil && evaluationID.rawID == parsed.String() {
			continue
		}
		targetID := ""
		if parseErr == nil {
			canonicalID := parsed.String()
			_, rawCollision := rawIDs[canonicalID]
			_, reservedCollision := reserved[canonicalID]
			if !rawCollision && !reservedCollision {
				targetID = canonicalID
			}
		}
		for collision := 0; targetID == ""; collision++ {
			candidateID := migratedCandidateEvaluationID(evaluationID.rawID,
				evaluationID.generationID, evaluationID.candidateID,
				evaluationID.requestSHA256, collision)
			_, rawCollision := rawIDs[candidateID]
			_, reservedCollision := reserved[candidateID]
			if !rawCollision && !reservedCollision {
				targetID = candidateID
			}
		}
		command, updateErr := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.candidate_evaluations
			SET id = $2 WHERE id::text = $1`, schema), evaluationID.rawID, targetID)
		if updateErr != nil {
			return fmt.Errorf("repair predecessor candidate evaluation id: %w", updateErr)
		}
		if command.RowsAffected() != 1 {
			return fmt.Errorf("repair predecessor candidate evaluation id %q: row disappeared", evaluationID.rawID)
		}
		reserved[targetID] = struct{}{}
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s.candidate_evaluations
		ALTER COLUMN id TYPE UUID USING id::uuid`, schema)); err != nil {
		return fmt.Errorf("convert candidate evaluation ids to UUID: %w", err)
	}
	return nil
}

func migratedCandidateEvaluationID(rawID, generationID, candidateID, requestSHA256 string, collision int) string {
	identity := fmt.Sprintf("candidate-evaluation:%s:%s:%s:%s:%d",
		rawID, generationID, candidateID, requestSHA256, collision)
	return uuid.NewSHA1(durableRevisionMigrationNamespace, []byte(identity)).String()
}

func executePostgresMigration(ctx context.Context, tx pgx.Tx, migration postgresMigration) error {
	for _, statement := range migration.expand {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("expand %s migration: %w", migration.name, err)
		}
	}
	if err := migration.backfill(ctx, tx); err != nil {
		return fmt.Errorf("backfill %s migration: %w", migration.name, err)
	}
	if err := migration.verify(ctx, tx); err != nil {
		return fmt.Errorf("verify %s migration: %w", migration.name, err)
	}
	for _, statement := range migration.contract {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("contract %s migration: %w", migration.name, err)
		}
	}
	return nil
}

// durableRevisionIdentityMigration describes the expand/backfill/verify/
// contract boundary for the unified history.
func (s *PostgresStore) durableRevisionIdentityMigration() postgresMigration {
	schema := quoteIdentifier(s.schema)
	var expectations *durableRevisionMigrationExpectations
	return postgresMigration{
		name: "durable-skill-revision-identities",
		expand: []string{
			fmt.Sprintf(`ALTER TABLE %s.skills
				ADD COLUMN IF NOT EXISTS explicit_latest_revision_id UUID`, schema),
			fmt.Sprintf(`ALTER TABLE %s.skills
				ADD COLUMN IF NOT EXISTS next_sequence_number INT NOT NULL DEFAULT 1`, schema),
			fmt.Sprintf(`ALTER TABLE %s.skills
				ADD COLUMN IF NOT EXISTS created_by_subject TEXT NOT NULL DEFAULT ''`, schema),
			fmt.Sprintf(`ALTER TABLE %s.skills
				ADD COLUMN IF NOT EXISTS migration_alias_of_skill_id UUID`, schema),
			fmt.Sprintf(`ALTER TABLE %s.skills
				ADD COLUMN IF NOT EXISTS migration_managed_latest_revision_id UUID`, schema),
			fmt.Sprintf(`DROP INDEX IF EXISTS %s.skills_slug_key`, schema),
			fmt.Sprintf(`ALTER TABLE %s.skill_generations
				ADD COLUMN IF NOT EXISTS skill_id UUID,
				ADD COLUMN IF NOT EXISTS creator_subject TEXT,
				ADD COLUMN IF NOT EXISTS base_revision_id UUID,
				ADD COLUMN IF NOT EXISTS result_candidate_id UUID,
				ADD COLUMN IF NOT EXISTS result_revision_id UUID,
				ADD COLUMN IF NOT EXISTS result_claim_token UUID`, schema),
			// Backfill clears the predecessor ownership and resolution columns.
			// Relax their constraints during expand so old rows can be converted
			// before the final contract is installed.
			fmt.Sprintf(`ALTER TABLE %s.skill_generations
				DROP CONSTRAINT IF EXISTS skill_generations_owner_subject_check,
				ALTER COLUMN draft_id DROP NOT NULL,
				ALTER COLUMN owner_subject DROP NOT NULL,
				ALTER COLUMN starting_lock_version DROP NOT NULL,
				ALTER COLUMN resolution DROP NOT NULL`, schema),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skill_revisions (
				id                       UUID NOT NULL,
				skill_id                 UUID NOT NULL,
				sequence_number          INT NOT NULL,
				version                   TEXT NOT NULL,
				creator_subject           TEXT NOT NULL,
				based_on_revision_id      UUID,
				source_revision_id        UUID,
				origin                    TEXT NOT NULL,
				name                      TEXT NOT NULL,
				description               TEXT NOT NULL DEFAULT '',
				type                      TEXT NOT NULL DEFAULT 'workflow',
				tags                      TEXT[] NOT NULL DEFAULT '{}',
				content                   TEXT NOT NULL DEFAULT '',
				is_ai_generated           BOOLEAN NOT NULL DEFAULT FALSE,
				source_session_ids        TEXT[] NOT NULL DEFAULT '{}',
				content_sha256            TEXT NOT NULL,
				change_note               TEXT NOT NULL DEFAULT '',
				generation_id             UUID,
				idempotency_key           TEXT NOT NULL DEFAULT '',
				legacy_reference          TEXT,
				created_at                TIMESTAMPTZ NOT NULL,
				CONSTRAINT skill_revisions_pkey PRIMARY KEY (id),
				CONSTRAINT skill_revisions_skill_sequence_key UNIQUE (skill_id, sequence_number),
				CONSTRAINT skill_revisions_skill_id_id_key UNIQUE (skill_id, id),
				CONSTRAINT skill_revisions_generation_key UNIQUE (generation_id),
				CONSTRAINT skill_revisions_generation_id_id_key UNIQUE (generation_id, id),
				CONSTRAINT skill_revisions_legacy_reference_key UNIQUE (legacy_reference),
				CONSTRAINT skill_revisions_skill_fkey FOREIGN KEY (skill_id) REFERENCES %s.skills(id) ON DELETE RESTRICT,
				CONSTRAINT skill_revisions_base_fkey FOREIGN KEY (skill_id, based_on_revision_id) REFERENCES %s.skill_revisions(skill_id, id) ON DELETE RESTRICT,
				CONSTRAINT skill_revisions_source_fkey FOREIGN KEY (source_revision_id) REFERENCES %s.skill_revisions(id) ON DELETE RESTRICT,
				CONSTRAINT skill_revisions_generation_fkey FOREIGN KEY (generation_id) REFERENCES %s.skill_generations(id) ON DELETE RESTRICT,
				CONSTRAINT skill_revisions_sequence_check CHECK (sequence_number > 0),
				CONSTRAINT skill_revisions_origin_check CHECK (origin IN ('manual', 'generation', 'duplicate', 'migrated'))
			)`, schema, schema, schema, schema, schema),
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skill_revision_visibility (
				revision_id        UUID NOT NULL,
				is_public          BOOLEAN NOT NULL DEFAULT FALSE,
				changed_by_subject TEXT NOT NULL,
				changed_at         TIMESTAMPTZ NOT NULL,
				CONSTRAINT skill_revision_visibility_pkey PRIMARY KEY (revision_id),
				CONSTRAINT skill_revision_visibility_revision_fkey FOREIGN KEY (revision_id) REFERENCES %s.skill_revisions(id) ON DELETE RESTRICT
			)`, schema, schema),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_revisions_history_idx
				ON %s.skill_revisions (skill_id, sequence_number DESC, id DESC)`, schema),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_revisions_creator_idx
				ON %s.skill_revisions (skill_id, creator_subject, sequence_number DESC, id DESC)`, schema),
			fmt.Sprintf(`DROP INDEX IF EXISTS %s.skill_revisions_idempotency_key`, schema),
			fmt.Sprintf(`UPDATE %s.skill_revisions SET idempotency_key = CASE
				WHEN generation_id IS NOT NULL THEN 'generation:' || generation_id::text
				ELSE ''
			END WHERE legacy_reference IS NOT NULL`, schema),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_revision_visibility_public_idx
				ON %s.skill_revision_visibility (is_public, revision_id)`, schema),
			fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS generation_candidates_generation_id_id_key
				ON %s.generation_candidates (generation_id, id)`, schema),
			fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS skill_revisions_generation_id_id_key
				ON %s.skill_revisions (generation_id, id)`, schema),
		},
		backfill: func(ctx context.Context, tx pgx.Tx) error {
			expectations = &durableRevisionMigrationExpectations{}
			if err := s.backfillDurableRevisionIdentities(ctx, tx, expectations); err != nil {
				return err
			}
			if err := s.backfillGenerationRelationshipIdentities(ctx, tx); err != nil {
				return err
			}
			if err := s.repairModernSynthesisWinnerIdentities(ctx, tx); err != nil {
				return err
			}
			// Sanitize public artifact projections before awaiting-input work is
			// converted to completed. Required candidate IDs survive payload repair,
			// so conversion can validate the exact retained result relationship.
			if err := s.sanitizeGenerationArtifacts(ctx, tx); err != nil {
				return err
			}
			if err := s.verifyCompletedGenerationLineage(ctx, tx); err != nil {
				return err
			}
			if err := s.completeRetainedAwaitingGenerationResults(ctx, tx); err != nil {
				return err
			}
			return s.verifyCompletedGenerationLineage(ctx, tx)
		},
		verify: func(ctx context.Context, tx pgx.Tx) error {
			if err := s.verifyDurableRevisionIdentities(ctx, tx, expectations); err != nil {
				return err
			}
			// Re-run after sanitization and awaiting conversion. A broken required
			// candidate or result-revision link aborts this migration transaction.
			return s.verifyCompletedGenerationLineage(ctx, tx)
		},
		contract: []string{
			fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS skills_slug_key ON %s.skills (slug)
				WHERE migration_alias_of_skill_id IS NULL`, schema),
			fmt.Sprintf(`ALTER TABLE %s.skill_generations
				DROP CONSTRAINT IF EXISTS skill_generations_status_check`, schema),
			fmt.Sprintf(`ALTER TABLE %s.skill_generations
				ADD CONSTRAINT skill_generations_status_check CHECK (status IN (
					'queued', 'generating_candidates', 'evaluating_candidates',
					'synthesizing', 'completed', 'canceled', 'failed'
				))`, schema),
			fmt.Sprintf(`ALTER TABLE %s.generation_sessions
				DROP CONSTRAINT IF EXISTS generation_sessions_status_check,
				DROP CONSTRAINT IF EXISTS generation_sessions_ordinal_check`, schema),
			fmt.Sprintf(`ALTER TABLE %s.generation_sessions
				ADD CONSTRAINT generation_sessions_status_check CHECK (status IN (
					'pending', 'transcript_failed', 'candidate_failed', 'candidate_ready',
					'evaluation_failed', 'evaluated'
				)),
				ADD CONSTRAINT generation_sessions_ordinal_check CHECK (ordinal >= 0 AND ordinal < %d)`,
				schema, maxGenerationStateSessions),
			fmt.Sprintf(`ALTER TABLE %s.generation_candidates
				DROP CONSTRAINT IF EXISTS generation_candidates_kind_check,
				DROP CONSTRAINT IF EXISTS generation_candidates_ordinal_check,
				DROP CONSTRAINT IF EXISTS generation_candidates_bundle_sha256_check`, schema),
			fmt.Sprintf(`ALTER TABLE %s.generation_candidates
				ADD CONSTRAINT generation_candidates_kind_check CHECK (kind IN ('session', 'context', 'synthesis')),
				ADD CONSTRAINT generation_candidates_ordinal_check CHECK (ordinal >= 0 AND ordinal < %d),
				ADD CONSTRAINT generation_candidates_bundle_sha256_check CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$')`,
				schema, maxGenerationStateCandidates),
			fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS one_context_candidate_per_generation
				ON %s.generation_candidates (generation_id, kind) WHERE kind = 'context'`, schema),
			fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS one_synthesis_candidate_per_generation
				ON %s.generation_candidates (generation_id, kind) WHERE kind = 'synthesis'`, schema),
			// Predecessor columns remain isolated as a migration-only rolling-upgrade
			// inbox. A rerun ingests retained old writes and clears their relationships;
			// current runtime code neither reads nor writes these columns.
			fmt.Sprintf(`DROP INDEX IF EXISTS %s.one_nonterminal_generation_per_draft`, schema),
			fmt.Sprintf(`ALTER TABLE %s.skill_generations
				ALTER COLUMN input_slug SET DEFAULT '',
				ALTER COLUMN resolution DROP DEFAULT,
				ALTER COLUMN skill_id SET NOT NULL,
				ALTER COLUMN creator_subject SET NOT NULL`, schema),
			fmt.Sprintf(`ALTER TABLE %s.generation_candidates
				ALTER COLUMN slug SET DEFAULT ''`, schema),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_generations_creator_idx
				ON %s.skill_generations (creator_subject, skill_id, created_at DESC, id DESC)`, schema),
			fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS skill_revisions_idempotency_key
				ON %s.skill_revisions (skill_id, idempotency_key)
				WHERE idempotency_key <> ''`, schema),
			fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS skill_generations_result_revision_key
				ON %s.skill_generations (result_revision_id) WHERE result_revision_id IS NOT NULL`, schema),
			migrationForeignKeyStatement(s.schema, "skills", "skills_explicit_latest_revision_fkey", "explicit_latest_revision_id", "skill_revisions", "id"),
			migrationForeignKeyStatement(s.schema, "skills", "skills_migration_managed_latest_revision_fkey", "migration_managed_latest_revision_id", "skill_revisions", "id"),
			migrationForeignKeyStatement(s.schema, "skills", "skills_migration_alias_fkey", "migration_alias_of_skill_id", "skills", "id"),
			migrationForeignKeyStatement(s.schema, "skill_generations", "skill_generations_skill_fkey", "skill_id", "skills", "id"),
			migrationCompositeForeignKeyStatement(s.schema, "skill_generations", "skill_generations_same_skill_base_revision_fkey",
				[]string{"skill_id", "base_revision_id"}, "skill_revisions", []string{"skill_id", "id"}),
			fmt.Sprintf(`ALTER TABLE %s.skill_generations
				DROP CONSTRAINT IF EXISTS skill_generations_winner_candidate_fkey,
				DROP CONSTRAINT IF EXISTS skill_generations_result_candidate_fkey,
				DROP CONSTRAINT IF EXISTS skill_generations_result_revision_fkey`, schema),
			fmt.Sprintf(`ALTER TABLE %s.generation_sessions
				DROP CONSTRAINT IF EXISTS generation_sessions_candidate_fkey`, schema),
			fmt.Sprintf(`ALTER TABLE %s.candidate_evaluations
				DROP CONSTRAINT IF EXISTS candidate_evaluations_candidate_fkey`, schema),
			fmt.Sprintf(`ALTER TABLE %s.generation_diagnostics
				DROP CONSTRAINT IF EXISTS generation_diagnostics_candidate_fkey`, schema),
			migrationCompositeForeignKeyStatement(s.schema, "skill_generations", "skill_generations_same_generation_winner_candidate_fkey",
				[]string{"id", "winner_candidate_id"}, "generation_candidates", []string{"generation_id", "id"}),
			migrationCompositeForeignKeyStatement(s.schema, "skill_generations", "skill_generations_same_generation_result_candidate_fkey",
				[]string{"id", "result_candidate_id"}, "generation_candidates", []string{"generation_id", "id"}),
			migrationCompositeForeignKeyStatement(s.schema, "generation_sessions", "generation_sessions_same_generation_candidate_fkey",
				[]string{"generation_id", "candidate_id"}, "generation_candidates", []string{"generation_id", "id"}),
			migrationCompositeForeignKeyStatement(s.schema, "candidate_evaluations", "candidate_evaluations_same_generation_candidate_fkey",
				[]string{"generation_id", "candidate_id"}, "generation_candidates", []string{"generation_id", "id"}),
			migrationCompositeForeignKeyStatement(s.schema, "generation_diagnostics", "generation_diagnostics_same_generation_candidate_fkey",
				[]string{"generation_id", "candidate_id"}, "generation_candidates", []string{"generation_id", "id"}),
			migrationCompositeForeignKeyStatement(s.schema, "skill_generations", "skill_generations_same_generation_result_revision_fkey",
				[]string{"id", "result_revision_id"}, "skill_revisions", []string{"generation_id", "id"}),
		},
	}
}

// backfillGenerationRelationshipIdentities reconciles relationships that
// predecessor schemas constrained only by globally unique child IDs. Required
// evaluation links follow their candidate; optional links are cleared when the
// child does not belong to the owning generation. Composite foreign keys then
// preserve that identity without making nullable candidate fields mandatory.
func (s *PostgresStore) backfillGenerationRelationshipIdentities(ctx context.Context, tx pgx.Tx) error {
	schema := quoteIdentifier(s.schema)
	statements := []struct {
		name string
		sql  string
	}{
		{name: "candidate evaluation generations", sql: fmt.Sprintf(`UPDATE %s.candidate_evaluations evaluation
			SET generation_id = candidate.generation_id
			FROM %s.generation_candidates candidate
			WHERE candidate.id = evaluation.candidate_id
			  AND evaluation.generation_id IS DISTINCT FROM candidate.generation_id`, schema, schema)},
		{name: "generation winner candidates", sql: fmt.Sprintf(`UPDATE %s.skill_generations generation
			SET winner_candidate_id = NULL
			WHERE generation.winner_candidate_id IS NOT NULL AND NOT EXISTS (
				SELECT 1 FROM %s.generation_candidates candidate
				WHERE candidate.generation_id = generation.id
				  AND candidate.id = generation.winner_candidate_id
			)`, schema, schema)},
		{name: "generation result candidates", sql: fmt.Sprintf(`UPDATE %s.skill_generations generation
			SET result_candidate_id = NULL
			WHERE generation.result_candidate_id IS NOT NULL AND NOT EXISTS (
				SELECT 1 FROM %s.generation_candidates candidate
				WHERE candidate.generation_id = generation.id
				  AND candidate.id = generation.result_candidate_id
			)`, schema, schema)},
		{name: "generation session candidates", sql: fmt.Sprintf(`UPDATE %s.generation_sessions session SET candidate_id = NULL
			WHERE session.candidate_id IS NOT NULL AND NOT EXISTS (
				SELECT 1 FROM %s.generation_candidates candidate
				WHERE candidate.generation_id = session.generation_id
				  AND candidate.id = session.candidate_id
			)`, schema, schema)},
		{name: "generation diagnostic candidates", sql: fmt.Sprintf(`UPDATE %s.generation_diagnostics diagnostic SET candidate_id = NULL
			WHERE diagnostic.candidate_id IS NOT NULL AND NOT EXISTS (
				SELECT 1 FROM %s.generation_candidates candidate
				WHERE candidate.generation_id = diagnostic.generation_id
				  AND candidate.id = diagnostic.candidate_id
			)`, schema, schema)},
		{name: "generation result revisions", sql: fmt.Sprintf(`UPDATE %s.skill_generations generation SET result_revision_id = NULL
			WHERE generation.result_revision_id IS NOT NULL AND NOT EXISTS (
				SELECT 1 FROM %s.skill_revisions revision
				WHERE revision.generation_id = generation.id
				  AND revision.id = generation.result_revision_id
			)`, schema, schema)},
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.sql); err != nil {
			return fmt.Errorf("backfill %s: %w", statement.name, err)
		}
	}
	return nil
}

const migrationGenerationSanitizePageSize = 8

// sanitizeGenerationArtifacts walks generation history in fixed keyset pages.
// Every child query is independently bounded, and SQL deletes rows outside the
// retained bounded projection without materializing their payloads. Unsafe
// optional details may be normalized in place. Candidate snapshot changes are
// allowed only for executable non-result work after stale judgments are
// invalidated; protected or terminal lineage aborts the migration transaction.
func (s *PostgresStore) sanitizeGenerationArtifacts(ctx context.Context, tx pgx.Tx) error {
	schema := quoteIdentifier(s.schema)
	cursor := ""
	for {
		rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT %s
			FROM %s.skill_generations g
			WHERE NULLIF($1::text, '') IS NULL OR g.id > NULLIF($1::text, '')::uuid
			ORDER BY g.id LIMIT %d`, generationColumns, schema, migrationGenerationSanitizePageSize), cursor)
		if err != nil {
			return fmt.Errorf("list generation artifact sanitization page: %w", err)
		}
		page := make([]SkillGenerationRecord, 0, migrationGenerationSanitizePageSize)
		for rows.Next() {
			generation, scanErr := scanGeneration(rows)
			if scanErr != nil {
				rows.Close()
				return scanErr
			}
			page = append(page, *generation)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate generation artifact sanitization page: %w", err)
		}
		rows.Close()
		if len(page) == 0 {
			return nil
		}
		for index := range page {
			if err = s.sanitizeOneGenerationArtifacts(ctx, tx, schema, page[index]); err != nil {
				return err
			}
		}
		cursor = page[len(page)-1].ID
	}
}

func (s *PostgresStore) sanitizeOneGenerationArtifacts(
	ctx context.Context,
	tx pgx.Tx,
	schema string,
	generation SkillGenerationRecord,
) error {
	// Predecessor source identifiers are part of the public projection. Repair
	// them with the same deterministic truncation used by candidate provenance,
	// then normalize child session keys before validating placement.
	generation.SelectedSessionIDs = migrationBoundedIdentities(
		generation.SelectedSessionIDs, MaxRevisionSourceSessionIDs,
	)
	state := GenerationState{Generation: generation}

	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT generation_id::text, session_id,
		ordinal, status, COALESCE(candidate_id::text, ''), diagnostic_code, updated_at
		FROM %s.generation_sessions
		WHERE generation_id = $1
		ORDER BY ordinal, session_id LIMIT %d`, schema, maxGenerationStateSessions), generation.ID)
	if err != nil {
		return fmt.Errorf("load bounded migration generation sessions: %w", err)
	}
	for rows.Next() {
		var session GenerationSessionRecord
		if err = rows.Scan(&session.GenerationID, &session.SessionID, &session.Ordinal,
			&session.Status, &session.CandidateID, &session.DiagnosticCode, &session.UpdatedAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan migration generation session: %w", err)
		}
		session.SessionID = migrationBoundedIdentity(session.SessionID, MaxRevisionIdentityCodePoints)
		session.UpdatedAt = session.UpdatedAt.UTC()
		state.Sessions = append(state.Sessions, session)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate migration generation sessions: %w", err)
	}
	rows.Close()

	protectedCandidateIDs := nonEmptyMigrationIDs(generation.WinnerCandidateID, generation.ResultCandidateID)
	rawCandidates := make(map[string]GenerationCandidateRecord)
	rows, err = tx.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s.generation_candidates c
		WHERE c.generation_id = $1
		ORDER BY CASE WHEN c.id = ANY($2::uuid[]) THEN 0 ELSE 1 END,
			c.ordinal, c.id LIMIT %d`, generationCandidateColumns, schema, maxGenerationStateCandidates),
		generation.ID, protectedCandidateIDs)
	if err != nil {
		return fmt.Errorf("load bounded migration generation candidates: %w", err)
	}
	for rows.Next() {
		candidate, scanErr := scanGenerationCandidate(rows)
		if scanErr != nil {
			rows.Close()
			return scanErr
		}
		rawCandidates[candidate.ID] = *candidate
		state.Candidates = append(state.Candidates,
			sanitizeMigratedGenerationCandidateProjection(*candidate, generation.Snapshot))
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate migration generation candidates: %w", err)
	}
	rows.Close()

	candidateIDs := make([]string, 0, len(state.Candidates))
	for _, candidate := range state.Candidates {
		candidateIDs = append(candidateIDs, candidate.ID)
	}
	rows, err = tx.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s.candidate_evaluations e
		WHERE e.generation_id = $1 AND e.candidate_id = ANY($2::uuid[])
		ORDER BY e.created_at, e.id LIMIT %d`, candidateEvaluationColumns, schema,
		maxGenerationStateEvaluations), generation.ID, candidateIDs)
	if err != nil {
		return fmt.Errorf("load bounded migration candidate evaluations: %w", err)
	}
	for rows.Next() {
		evaluation, scanErr := scanCandidateEvaluation(rows)
		if scanErr != nil {
			rows.Close()
			return scanErr
		}
		// Panel is evaluator-private opaque data. Discard it before validation so
		// an unsafe predecessor panel cannot delete an otherwise valid judgment.
		evaluation.Panel = json.RawMessage(`{}`)
		state.Evaluations = append(state.Evaluations, *evaluation)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate migration candidate evaluations: %w", err)
	}
	rows.Close()

	rows, err = tx.Query(ctx, fmt.Sprintf(`SELECT id::text, generation_id::text,
		session_id, COALESCE(candidate_id::text, ''), stage, code, message,
		retryable, created_at FROM %s.generation_diagnostics
		WHERE generation_id = $1
		ORDER BY (stage = 'synthesis') DESC, (NOT retryable) DESC,
			created_at DESC, id DESC LIMIT %d`, schema, maxGenerationStateDiagnostics), generation.ID)
	if err != nil {
		return fmt.Errorf("load bounded migration generation diagnostics: %w", err)
	}
	for rows.Next() {
		var diagnostic GenerationDiagnosticRecord
		if err = rows.Scan(&diagnostic.ID, &diagnostic.GenerationID,
			&diagnostic.SessionID, &diagnostic.CandidateID, &diagnostic.Stage,
			&diagnostic.Code, &diagnostic.Message, &diagnostic.Retryable,
			&diagnostic.CreatedAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan migration generation diagnostic: %w", err)
		}
		diagnostic.CreatedAt = diagnostic.CreatedAt.UTC()
		state.Diagnostics = append(state.Diagnostics, diagnostic)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate migration generation diagnostics: %w", err)
	}
	rows.Close()

	sanitizeGenerationStateArtifacts(&state)
	acceptedCandidateIDs := make([]string, 0, len(state.Candidates))
	acceptedCandidates := make(map[string]GenerationCandidateRecord, len(state.Candidates))
	for _, candidate := range state.Candidates {
		acceptedCandidateIDs = append(acceptedCandidateIDs, candidate.ID)
		acceptedCandidates[candidate.ID] = candidate
	}
	protectedCandidates := make(map[string]struct{}, len(protectedCandidateIDs))
	for _, candidateID := range protectedCandidateIDs {
		protectedCandidates[candidateID] = struct{}{}
		if _, accepted := acceptedCandidates[candidateID]; !accepted {
			return migrationCandidateLineageError("a winner or result candidate is invalid")
		}
	}

	reevaluateCandidates := make(map[string]struct{})
	markForReevaluation := func(candidateID string) error {
		if _, protected := protectedCandidates[candidateID]; protected {
			return migrationCandidateLineageError("a winner or result candidate would change")
		}
		if !generationIsExecutable(generation.Status) {
			return migrationCandidateLineageError("a retained candidate cannot be safely re-evaluated")
		}
		reevaluateCandidates[candidateID] = struct{}{}
		return nil
	}
	for _, candidate := range state.Candidates {
		raw, exists := rawCandidates[candidate.ID]
		if !exists {
			return migrationCandidateLineageError("a retained candidate lost its source snapshot")
		}
		if !equalRevisionSnapshots(generationCandidateRevisionSnapshot(raw),
			generationCandidateRevisionSnapshot(candidate)) {
			if err = markForReevaluation(candidate.ID); err != nil {
				return err
			}
		}
	}
	for _, evaluation := range state.Evaluations {
		if !migrationEvaluationRequestHashIsProvable(evaluation.RequestSHA256) {
			continue
		}
		candidate, exists := acceptedCandidates[evaluation.CandidateID]
		if !exists {
			continue
		}
		expectedHash, hashErr := GenerationCandidateEvaluationRequestSHA256(state.Generation, candidate)
		if hashErr == nil && strings.EqualFold(expectedHash, evaluation.RequestSHA256) {
			continue
		}
		if err = markForReevaluation(candidate.ID); err != nil {
			return err
		}
	}
	if len(reevaluateCandidates) > 0 {
		evaluations := state.Evaluations[:0]
		for _, evaluation := range state.Evaluations {
			if _, stale := reevaluateCandidates[evaluation.CandidateID]; !stale {
				evaluations = append(evaluations, evaluation)
			}
		}
		state.Evaluations = evaluations

		diagnostics := state.Diagnostics[:0]
		for _, diagnostic := range state.Diagnostics {
			if _, stale := reevaluateCandidates[diagnostic.CandidateID]; !stale {
				diagnostics = append(diagnostics, diagnostic)
			}
		}
		state.Diagnostics = diagnostics

		for index := range state.Sessions {
			if _, stale := reevaluateCandidates[state.Sessions[index].CandidateID]; !stale {
				continue
			}
			state.Sessions[index].Status = GenerationSessionCandidateReady
			state.Sessions[index].DiagnosticCode = ""
		}
		if state.Generation.Status == GenerationStatusSynthesizing {
			state.Generation.Status = GenerationStatusEvaluatingCandidates
		}
	}

	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET
		winner_candidate_id = NULLIF($2, '')::uuid,
		result_candidate_id = NULLIF($3, '')::uuid,
		status = $4, selected_session_ids = $5 WHERE id = $1`, schema), generation.ID,
		generation.WinnerCandidateID, generation.ResultCandidateID,
		state.Generation.Status, nonNilStrings(state.Generation.SelectedSessionIDs)); err != nil {
		return fmt.Errorf("sanitize generation candidate links: %w", err)
	}

	acceptedDiagnosticIDs := make([]string, 0, len(state.Diagnostics))
	for _, diagnostic := range state.Diagnostics {
		acceptedDiagnosticIDs = append(acceptedDiagnosticIDs, diagnostic.ID)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.generation_diagnostics
		WHERE generation_id = $1 AND NOT (id = ANY($2::uuid[]))`, schema),
		generation.ID, acceptedDiagnosticIDs); err != nil {
		return fmt.Errorf("drop unsafe or excess generation diagnostics: %w", err)
	}

	acceptedEvaluationIDs := make([]string, 0, len(state.Evaluations))
	for _, evaluation := range state.Evaluations {
		acceptedEvaluationIDs = append(acceptedEvaluationIDs, evaluation.ID)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.candidate_evaluations
		WHERE generation_id = $1 AND NOT (id = ANY($2::uuid[]))`, schema),
		generation.ID, acceptedEvaluationIDs); err != nil {
		return fmt.Errorf("drop unsafe or excess candidate evaluations: %w", err)
	}

	acceptedSessionIDs := make([]string, 0, len(state.Sessions))
	for _, session := range state.Sessions {
		acceptedSessionIDs = append(acceptedSessionIDs, session.SessionID)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.generation_sessions
		WHERE generation_id = $1 AND NOT (session_id = ANY($2::text[]))`, schema),
		generation.ID, acceptedSessionIDs); err != nil {
		return fmt.Errorf("drop unsafe or excess generation sessions: %w", err)
	}
	for _, session := range state.Sessions {
		if _, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.generation_sessions (
			generation_id, session_id, ordinal, status, candidate_id, diagnostic_code, updated_at
		) VALUES ($1, $2, $3, $4, NULLIF($5, '')::uuid, $6, $7)
		ON CONFLICT (generation_id, session_id) DO UPDATE SET
			ordinal = EXCLUDED.ordinal,
			status = EXCLUDED.status,
			candidate_id = EXCLUDED.candidate_id,
			diagnostic_code = EXCLUDED.diagnostic_code,
			updated_at = EXCLUDED.updated_at`, schema), generation.ID, session.SessionID,
			session.Ordinal, session.Status, session.CandidateID,
			session.DiagnosticCode, session.UpdatedAt); err != nil {
			return fmt.Errorf("sanitize generation session: %w", err)
		}
	}

	for _, candidate := range state.Candidates {
		if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_candidates SET
			ordinal = $2, kind = $3, source_session_ids = $4, name = $5,
			description = $6, type = $7, tags = $8, content = $9,
			is_ai_generated = $10, insights = $11::jsonb, bundle_sha256 = $12
			WHERE id = $1`, schema), candidate.ID, candidate.Ordinal, candidate.Kind,
			nonNilStrings(candidate.SourceSessionIDs), candidate.Snapshot.Name,
			candidate.Snapshot.Description, candidate.Snapshot.Type,
			nonNilStrings(candidate.Snapshot.Tags), candidate.Snapshot.Content,
			candidate.Snapshot.IsAIGenerated, candidate.Insights,
			candidate.BundleSHA256); err != nil {
			return fmt.Errorf("canonicalize generation candidate: %w", err)
		}
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.generation_candidates
		WHERE generation_id = $1 AND NOT (id = ANY($2::uuid[]))`, schema),
		generation.ID, acceptedCandidateIDs); err != nil {
		return fmt.Errorf("drop unsafe or excess generation candidates: %w", err)
	}

	for _, evaluation := range state.Evaluations {
		if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.candidate_evaluations SET
			request_sha256 = $2, profile = $3, profile_version = $4,
			evaluator_version = $5, score = $6, decision = $7,
			critical_finding_count = $8, warning_finding_count = $9,
			criterion_results = $10::jsonb, findings = $11::jsonb,
			strengths = $12::jsonb, panel = '{}'::jsonb WHERE id = $1`, schema),
			evaluation.ID, evaluation.RequestSHA256, evaluation.Profile,
			evaluation.ProfileVersion, evaluation.EvaluatorVersion,
			evaluation.Score, evaluation.Decision, evaluation.CriticalFindingCount,
			evaluation.WarningFindingCount, evaluation.CriterionResults,
			evaluation.Findings, evaluation.Strengths); err != nil {
			return fmt.Errorf("canonicalize candidate evaluation: %w", err)
		}
	}
	return nil
}

func nonEmptyMigrationIDs(values ...string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func migrationCandidateLineageError(message string) error {
	return fmt.Errorf("migration_candidate_lineage_invalid: %s", message)
}

func migrationEvaluationRequestHashIsProvable(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func sanitizeMigratedGenerationCandidateProjection(
	candidate GenerationCandidateRecord,
	fallback SkillRevisionSnapshot,
) GenerationCandidateRecord {
	candidate.Snapshot.Name = migrationBoundedIdentity(candidate.Snapshot.Name, MaxRevisionNameCodePoints)
	if candidate.Snapshot.Name == "" {
		candidate.Snapshot.Name = migrationBoundedIdentity(fallback.Name, MaxRevisionNameCodePoints)
	}
	if candidate.Snapshot.Name == "" {
		candidate.Snapshot.Name = "Migrated candidate"
	}
	candidate.Snapshot.Description = strings.TrimSpace(migrationBoundedText(
		candidate.Snapshot.Description, MaxRevisionDescriptionCodePoints, true,
	))
	candidate.Snapshot.Type = migrationBoundedIdentity(candidate.Snapshot.Type, MaxRevisionIdentityCodePoints)
	if candidate.Snapshot.Type != "workflow" && candidate.Snapshot.Type != "domain-knowledge" &&
		candidate.Snapshot.Type != "prompt-template" {
		candidate.Snapshot.Type = migrationBoundedIdentity(fallback.Type, MaxRevisionIdentityCodePoints)
	}
	if candidate.Snapshot.Type != "workflow" && candidate.Snapshot.Type != "domain-knowledge" &&
		candidate.Snapshot.Type != "prompt-template" {
		candidate.Snapshot.Type = "workflow"
	}
	candidate.Snapshot.Tags = migrationBoundedIdentities(candidate.Snapshot.Tags, MaxRevisionTags)
	candidate.Snapshot.Content = migrationBoundedText(candidate.Snapshot.Content, MaxRevisionContentCodePoints, true)
	candidate.SourceSessionIDs = migrationBoundedIdentities(
		candidate.SourceSessionIDs, MaxRevisionSourceSessionIDs,
	)
	insights, err := CanonicalGenerationCandidateInsights(candidate.Insights)
	if err != nil {
		insights = json.RawMessage(`[]`)
	}
	candidate.Insights = insights
	candidate.BundleSHA256 = ""
	return candidate
}

func migrationBoundedIdentities(values []string, maximum int) []string {
	result := make([]string, 0, min(len(values), maximum))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		if len(result) >= maximum {
			break
		}
		value := migrationBoundedIdentity(raw, MaxRevisionIdentityCodePoints)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func migrationBoundedIdentity(value string, maximum int) string {
	return strings.TrimSpace(migrationBoundedText(value, maximum, false))
}

func migrationBoundedText(value string, maximum int, allowWhitespaceControls bool) string {
	value = strings.ToValidUTF8(value, "")
	var result strings.Builder
	count := 0
	for _, character := range value {
		if count >= maximum {
			break
		}
		if unicode.IsControl(character) && (!allowWhitespaceControls ||
			(character != '\n' && character != '\r' && character != '\t')) {
			continue
		}
		result.WriteRune(character)
		count++
	}
	return result.String()
}

// verifyCompletedGenerationLineage is intentionally run both before and after
// awaiting-input conversion. Sanitization may discard malformed optional
// artifacts, but it may never leave completed work without its exact winner,
// result candidate, and generation-owned result revision.
func (s *PostgresStore) verifyCompletedGenerationLineage(ctx context.Context, tx pgx.Tx) error {
	schema := quoteIdentifier(s.schema)
	var generationID string
	err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT generation.id::text
		FROM %s.skill_generations generation
		LEFT JOIN %s.generation_candidates winner
		  ON winner.generation_id = generation.id
		 AND winner.id = generation.winner_candidate_id
		LEFT JOIN %s.generation_candidates result_candidate
		  ON result_candidate.generation_id = generation.id
		 AND result_candidate.id = generation.result_candidate_id
		LEFT JOIN %s.skill_revisions result_revision
		  ON result_revision.id = generation.result_revision_id
		 AND result_revision.generation_id = generation.id
		 AND result_revision.skill_id = generation.skill_id
		 AND result_revision.creator_subject = generation.creator_subject
		LEFT JOIN %s.skill_revision_visibility visibility
		  ON visibility.revision_id = result_revision.id
		WHERE generation.status = 'completed' AND (
			winner.id IS NULL OR result_candidate.id IS NULL OR
			result_revision.id IS NULL OR visibility.revision_id IS NULL
		)
		ORDER BY generation.id LIMIT 1`, schema, schema, schema, schema, schema)).Scan(&generationID)
	if err == nil {
		return migrationCandidateLineageError("a completed generation has broken result lineage")
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("verify completed generation lineage: %w", err)
	}
	return s.verifyGenerationResultSemantics(ctx, tx, GenerationStatusCompleted)
}

// verifyGenerationResultSemantics proves every available immutable relationship
// around a completed (or about-to-be-completed) result. Candidate UUIDs remain
// the lineage identity, while exact snapshot comparison prevents sanitization
// from changing the content beneath a generated revision. Opaque legacy request
// identities are retained; a syntactically valid SHA-256 is checked against the
// same canonical request function used by new runtime writes.
func (s *PostgresStore) verifyGenerationResultSemantics(ctx context.Context, tx pgx.Tx, status GenerationStatus) error {
	schema := quoteIdentifier(s.schema)
	var mismatch bool
	if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (
		SELECT 1 FROM %s.skill_generations generation
		LEFT JOIN %s.generation_candidates candidate
		  ON candidate.generation_id = generation.id
		 AND candidate.id = generation.result_candidate_id
		LEFT JOIN %s.skill_revisions revision
		  ON revision.id = generation.result_revision_id
		 AND revision.generation_id = generation.id
		WHERE generation.status = $1 AND (
			candidate.id IS NULL OR revision.id IS NULL OR
			candidate.name IS DISTINCT FROM revision.name OR
			candidate.description IS DISTINCT FROM revision.description OR
			candidate.type IS DISTINCT FROM revision.type OR
			candidate.tags IS DISTINCT FROM revision.tags OR
			candidate.content IS DISTINCT FROM revision.content OR
			candidate.is_ai_generated IS DISTINCT FROM revision.is_ai_generated OR
			candidate.source_session_ids IS DISTINCT FROM revision.source_session_ids
		)
	)`, schema, schema, schema), status).Scan(&mismatch); err != nil {
		return fmt.Errorf("verify generation result snapshots: %w", err)
	}
	if mismatch {
		return migrationCandidateLineageError("a result candidate does not match its generated revision")
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT
		generation.id::text, generation.creator_subject,
		generation.input_name, generation.input_description,
		generation.input_type, generation.input_tags, generation.input_content,
		generation.input_is_ai_generated, generation.input_source_session_ids,
		generation.author_context, generation.evaluator_profile,
		generation.evaluator_profile_version, generation.evaluation_criteria,
		candidate.id::text, candidate.ordinal, candidate.kind,
		candidate.source_session_ids, candidate.name, candidate.description,
		candidate.type, candidate.tags, candidate.content,
		candidate.is_ai_generated, evaluation.request_sha256
		FROM %s.skill_generations generation
		JOIN %s.generation_candidates candidate
		  ON candidate.generation_id = generation.id
		 AND candidate.id IN (generation.winner_candidate_id, generation.result_candidate_id)
		JOIN %s.candidate_evaluations evaluation
		  ON evaluation.generation_id = generation.id
		 AND evaluation.candidate_id = candidate.id
		WHERE generation.status = $1
		ORDER BY generation.id, candidate.id, evaluation.id`, schema, schema, schema), status)
	if err != nil {
		return fmt.Errorf("query generation result evaluation identities: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var generation SkillGenerationRecord
		var candidate GenerationCandidateRecord
		var requestSHA256 string
		if err = rows.Scan(&generation.ID, &generation.CreatorSubject,
			&generation.Snapshot.Name, &generation.Snapshot.Description,
			&generation.Snapshot.Type, &generation.Snapshot.Tags,
			&generation.Snapshot.Content, &generation.Snapshot.IsAIGenerated,
			&generation.Snapshot.SourceSessionIDs, &generation.AuthorContext,
			&generation.EvaluatorProfile, &generation.EvaluatorProfileVersion,
			&generation.EvaluationCriteria, &candidate.ID, &candidate.Ordinal,
			&candidate.Kind, &candidate.SourceSessionIDs, &candidate.Snapshot.Name,
			&candidate.Snapshot.Description, &candidate.Snapshot.Type,
			&candidate.Snapshot.Tags, &candidate.Snapshot.Content,
			&candidate.Snapshot.IsAIGenerated, &requestSHA256); err != nil {
			return fmt.Errorf("scan generation result evaluation identity: %w", err)
		}
		if !migrationEvaluationRequestHashIsProvable(requestSHA256) {
			continue
		}
		expected, hashErr := GenerationCandidateEvaluationRequestSHA256(generation, candidate)
		if hashErr != nil || !strings.EqualFold(expected, requestSHA256) {
			return migrationCandidateLineageError("a retained evaluation does not match its candidate request")
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("iterate generation result evaluation identities: %w", err)
	}
	return nil
}

// completeRetainedAwaitingGenerationResults converts predecessor output that
// was waiting on draft resolution into completed work whose exact migrated
// result revision remains creator-private.
func (s *PostgresStore) completeRetainedAwaitingGenerationResults(ctx context.Context, tx pgx.Tx) error {
	schema := quoteIdentifier(s.schema)
	var invalidGenerationID string
	err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT generation.id::text
		FROM %s.skill_generations generation
		LEFT JOIN %s.generation_candidates winner
		  ON winner.generation_id = generation.id
		 AND winner.id = generation.winner_candidate_id
		LEFT JOIN %s.generation_candidates result_candidate
		  ON result_candidate.generation_id = generation.id
		 AND result_candidate.id = generation.result_candidate_id
		LEFT JOIN %s.skill_revisions result_revision
		  ON result_revision.id = generation.result_revision_id
		 AND result_revision.generation_id = generation.id
		 AND result_revision.skill_id = generation.skill_id
		 AND result_revision.creator_subject = generation.creator_subject
		LEFT JOIN %s.skill_revision_visibility visibility
		  ON visibility.revision_id = result_revision.id
		WHERE generation.status = 'awaiting_input'
		  AND (winner.id IS NULL OR result_candidate.id IS NULL
		    OR result_revision.id IS NULL OR visibility.revision_id IS NULL
		    OR visibility.is_public)
		ORDER BY generation.id LIMIT 1`, schema, schema, schema, schema, schema)).
		Scan(&invalidGenerationID)
	if err == nil {
		return migrationCandidateLineageError("an awaiting generation has no exact private result")
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("verify awaiting generation results: %w", err)
	}
	if err = s.verifyGenerationResultSemantics(ctx, tx, GenerationStatus("awaiting_input")); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET
		status = 'completed', completed_at = COALESCE(completed_at, updated_at),
		claim_token = NULL, claim_owner = '', lease_expires_at = NULL
		WHERE status = 'awaiting_input'`, schema)); err != nil {
		return fmt.Errorf("complete migrated awaiting generations: %w", err)
	}
	return nil
}

func migrationForeignKeyStatement(schemaName, table, constraint, column, targetTable, targetColumn string) string {
	schema := quoteIdentifier(schemaName)
	return fmt.Sprintf(`DO $migration$
	BEGIN
		IF NOT EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conname = '%s'
			  AND conrelid = '%s.%s'::regclass
		) THEN
			ALTER TABLE %s.%s
				ADD CONSTRAINT %s FOREIGN KEY (%s)
				REFERENCES %s.%s(%s) ON DELETE RESTRICT;
		END IF;
	END
	$migration$`, constraint, schemaName, table, schema, quoteIdentifier(table),
		quoteIdentifier(constraint), quoteIdentifier(column), schema,
		quoteIdentifier(targetTable), quoteIdentifier(targetColumn))
}

// migrationCompositeForeignKeyStatement declares relationships whose identity
// includes the stable parent UUID. Generation base lineage uses
// (skill_id, base_revision_id), preventing a cross-skill revision UUID from
// satisfying the database contract.
func migrationCompositeForeignKeyStatement(schemaName, table, constraint string, columns []string, targetTable string, targetColumns []string) string {
	schema := quoteIdentifier(schemaName)
	quotedColumns := make([]string, len(columns))
	for i, column := range columns {
		quotedColumns[i] = quoteIdentifier(column)
	}
	quotedTargetColumns := make([]string, len(targetColumns))
	for i, column := range targetColumns {
		quotedTargetColumns[i] = quoteIdentifier(column)
	}
	return fmt.Sprintf(`DO $migration$
	BEGIN
		IF NOT EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conname = '%s'
			  AND conrelid = '%s.%s'::regclass
		) THEN
			ALTER TABLE %s.%s
				ADD CONSTRAINT %s FOREIGN KEY (%s)
				REFERENCES %s.%s(%s) ON DELETE RESTRICT;
		END IF;
	END
	$migration$`, constraint, schemaName, table, schema, quoteIdentifier(table),
		quoteIdentifier(constraint), strings.Join(quotedColumns, ", "), schema,
		quoteIdentifier(targetTable), strings.Join(quotedTargetColumns, ", "))
}

var durableRevisionMigrationNamespace = uuid.MustParse("5982ac1a-70d1-4f40-ae3f-0fb9d196136d")

type legacySkillForRevisionMigration struct {
	id                   string
	slug                 string
	authorSubject        string
	currentVersionNumber int
	createdAt            time.Time
	migrationAliasOf     string
}

type legacyDraftForRevisionMigration struct {
	id                        string
	targetSkillID             string
	publishedSkillID          string
	ownerSubject              string
	baseVersionNumber         int
	workingRevisionNumber     int
	workingSlug               string
	working                   SkillRevisionSnapshot
	workingParentID           string
	createdAt                 time.Time
	updatedAt                 time.Time
	targetUnifiedSkillID      string
	baseLegacyReference       string
	latestLegacyReference     string
	latestSourceLegacy        string
	latestRevisionSnapshot    SkillRevisionSnapshot
	latestRevisionIsGenerated bool
}

type durableRevisionMigrationCandidate struct {
	legacyReference    string
	legacyDraftID      string
	id                 string
	skillID            string
	creatorSubject     string
	basedOnLegacy      string
	sourceLegacy       string
	sourceLegacySkill  string
	origin             RevisionOrigin
	snapshot           SkillRevisionSnapshot
	changeNote         string
	generationID       string
	idempotencyKey     string
	createdAt          time.Time
	isPublic           bool
	orderGroup         int
	orderSource        string
	orderOrdinal       int
	inserted           bool
	repairBasedOn      bool
	assignedRevisionID string
}

type legacyGenerationForRevisionMigration struct {
	id                            string
	draftID                       string
	ownerSubject                  string
	input                         SkillRevisionSnapshot
	inputSourceLegacy             string
	existingBaseRevisionID        string
	existingBaseLegacyReference   string
	baseLegacyReference           string
	proposedCandidateID           string
	existingWinnerCandidateID     string
	existingResultCandidateID     string
	existingResultRevisionID      string
	existingResultLegacyReference string
	createdAt                     time.Time
}

type durableRevisionWorkingExpectation struct {
	draftID              string
	mappedLegacy         string
	sourceLegacy         string
	snapshot             SkillRevisionSnapshot
	updatedAt            time.Time
	allowGeneratedRepair bool
}

type durableRevisionGenerationExpectation struct {
	id                    string
	skillID               string
	creatorSubject        string
	baseRevisionID        string
	baseLegacyReference   string
	winnerCandidateID     string
	resultCandidateID     string
	resultRevisionID      string
	resultLegacyReference string
	input                 SkillRevisionSnapshot
	inputSourceLegacy     string
}

type durableRevisionLatestExpectation struct {
	explicitRevisionID string
	managedRevisionID  string
}

type durableRevisionMigrationExpectations struct {
	candidates         []*durableRevisionMigrationCandidate
	working            []durableRevisionWorkingExpectation
	generations        []durableRevisionGenerationExpectation
	latest             map[string]durableRevisionLatestExpectation
	aliases            map[string]string
	revisionIDByLegacy map[string]string
}

// backfillDurableRevisionIdentities constructs one deterministic append stream
// per stable skill. Legacy references provide the migration identity; obsolete
// draft-local idempotency keys are not promoted into the unified namespace.
func (s *PostgresStore) backfillDurableRevisionIdentities(ctx context.Context, tx pgx.Tx, expectations *durableRevisionMigrationExpectations) error {
	schema := quoteIdentifier(s.schema)

	skills, err := loadLegacyMigrationSkills(ctx, tx, schema)
	if err != nil {
		return err
	}
	canonicalBySlug := make(map[string]string, len(skills))
	targetByLegacySkill := make(map[string]string, len(skills))
	skillByID := make(map[string]legacySkillForRevisionMigration, len(skills))
	for index := range skills {
		skills[index].slug = normalizeSkillSlug(skills[index].slug)
		skillByID[skills[index].id] = skills[index]
	}
	// Canonical identities are selected only from explicitly non-alias rows in
	// stable (created_at, id) order. Additional predecessor rows retain their
	// real normalized slug and carry an explicit mapping; the partial slug index
	// excludes them, so no user slug is ever parsed as or rewritten to an alias.
	for index := range skills {
		skill := &skills[index]
		if skill.migrationAliasOf != "" {
			continue
		}
		if canonicalID := canonicalBySlug[skill.slug]; canonicalID == "" {
			canonicalBySlug[skill.slug] = skill.id
		} else {
			skill.migrationAliasOf = canonicalID
			skillByID[skill.id] = *skill
		}
	}
	for index := range skills {
		skill := &skills[index]
		canonicalID := skill.id
		if skill.migrationAliasOf != "" {
			canonicalID = skill.migrationAliasOf
			canonical, exists := skillByID[canonicalID]
			if !exists || canonical.migrationAliasOf != "" || canonical.slug != skill.slug {
				return fmt.Errorf("legacy skill %s has invalid canonical mapping %s", skill.id, canonicalID)
			}
		}
		targetByLegacySkill[skill.id] = canonicalID
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills SET
			slug = $2,
			created_by_subject = CASE WHEN created_by_subject = '' THEN author_subject ELSE created_by_subject END,
			migration_alias_of_skill_id = NULLIF($3, '')::uuid
			WHERE id = $1`, schema), skill.id, skill.slug, skill.migrationAliasOf); err != nil {
			return fmt.Errorf("normalize migrated skill identity: %w", err)
		}
	}

	drafts, err := loadLegacyMigrationDrafts(ctx, tx, schema)
	if err != nil {
		return err
	}
	for index := range drafts {
		draft := &drafts[index]
		legacyTarget := draft.targetSkillID
		if legacyTarget == "" {
			legacyTarget = draft.publishedSkillID
		}
		if legacyTarget != "" {
			draft.targetUnifiedSkillID = targetByLegacySkill[legacyTarget]
		}
		if draft.targetUnifiedSkillID == "" {
			slug := normalizeSkillSlug(draft.workingSlug)
			draft.targetUnifiedSkillID = canonicalBySlug[slug]
			if draft.targetUnifiedSkillID == "" {
				draft.targetUnifiedSkillID = uuid.NewSHA1(durableRevisionMigrationNamespace, []byte("skill:draft:"+draft.id)).String()
				canonicalBySlug[slug] = draft.targetUnifiedSkillID
				if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skills (
					id, slug, name, description, type, tags, content,
					is_ai_generated, generated_from_session_ids, author_subject,
					created_by_subject, created_at, updated_at
				) VALUES ($1, $2, '', '', 'workflow', '{}', '', FALSE, '{}', $3, $3, $4, $5)`, schema),
					draft.targetUnifiedSkillID, slug, draft.ownerSubject, draft.createdAt, draft.updatedAt); err != nil {
					return fmt.Errorf("create migrated greenfield skill identity: %w", err)
				}
			}
		}
	}

	candidates := make([]*durableRevisionMigrationCandidate, 0)
	versionReferences := make(map[string]map[int]string)
	latestVersionReference := make(map[string]string)
	desiredLatestReference := make(map[string]string)
	versionRows, err := tx.Query(ctx, fmt.Sprintf(`SELECT
		v.skill_id::text, v.version_number, v.changelog, v.name, v.description,
		v.type, v.tags, v.content, v.is_ai_generated, v.generated_from_session_ids,
		COALESCE(v.parent_id::text, ''), v.author_subject, v.published_at
		FROM %s.skill_versions v
		ORDER BY v.skill_id, v.version_number`, schema))
	if err != nil {
		return fmt.Errorf("query legacy published revisions: %w", err)
	}
	previousVersionReference := make(map[string]string)
	for versionRows.Next() {
		var skillID, parentID, authorSubject, changeNote string
		var versionNumber int
		var createdAt time.Time
		var snapshot SkillRevisionSnapshot
		if err := versionRows.Scan(&skillID, &versionNumber, &changeNote,
			&snapshot.Name, &snapshot.Description, &snapshot.Type, &snapshot.Tags,
			&snapshot.Content, &snapshot.IsAIGenerated, &snapshot.SourceSessionIDs,
			&parentID, &authorSubject, &createdAt); err != nil {
			versionRows.Close()
			return fmt.Errorf("scan legacy published revision: %w", err)
		}
		legacyReference := fmt.Sprintf("skill-version:%s:%d", skillID, versionNumber)
		if versionReferences[skillID] == nil {
			versionReferences[skillID] = make(map[int]string)
		}
		versionReferences[skillID][versionNumber] = legacyReference
		latestVersionReference[skillID] = legacyReference
		if authorSubject == "" {
			authorSubject = skillByID[skillID].authorSubject
		}
		candidate := &durableRevisionMigrationCandidate{
			legacyReference: legacyReference, skillID: targetByLegacySkill[skillID],
			creatorSubject: authorSubject, basedOnLegacy: previousVersionReference[skillID],
			origin: RevisionOriginMigrated, snapshot: canonicalSkillRevisionSnapshot(snapshot),
			changeNote: changeNote, createdAt: createdAt.UTC(), isPublic: true,
			orderGroup: 0, orderSource: skillID, orderOrdinal: versionNumber,
		}
		if parentID != "" {
			candidate.sourceLegacySkill = parentID
		}
		candidates = append(candidates, candidate)
		previousVersionReference[skillID] = legacyReference
	}
	if err := versionRows.Err(); err != nil {
		versionRows.Close()
		return fmt.Errorf("iterate legacy published revisions: %w", err)
	}
	versionRows.Close()
	for _, candidate := range candidates {
		if candidate.sourceLegacySkill != "" {
			candidate.sourceLegacy = latestVersionReference[candidate.sourceLegacySkill]
		}
	}

	// Mutable current-main heads were captured before compatibility fields were
	// reset. They are independent migration sources rather than synthetic
	// drafts, so an already-open draft for the same skill cannot hide them.
	mutableHeadRows, err := tx.Query(ctx, fmt.Sprintf(`SELECT
		legacy_reference, skill_id::text, base_skill_version_number,
		slug, name, description, type, tags, content, is_ai_generated,
		source_session_ids, COALESCE(parent_id::text, ''), creator_subject,
		snapshot_sha256, created_at
		FROM %s.skill_revision_migration_sources
		ORDER BY skill_id, created_at, legacy_reference`, schema))
	if err != nil {
		return fmt.Errorf("query captured mutable skill heads: %w", err)
	}
	for mutableHeadRows.Next() {
		var legacyReference, skillID, parentID, creatorSubject, snapshotSHA256 string
		var baseVersionNumber int
		var createdAt time.Time
		var snapshot predecessorSkillSnapshot
		if err := mutableHeadRows.Scan(&legacyReference, &skillID, &baseVersionNumber,
			&snapshot.Slug, &snapshot.Name, &snapshot.Description, &snapshot.Type,
			&snapshot.Tags, &snapshot.Content, &snapshot.IsAIGenerated,
			&snapshot.SourceSessionIDs, &parentID, &creatorSubject,
			&snapshotSHA256, &createdAt); err != nil {
			mutableHeadRows.Close()
			return fmt.Errorf("scan captured mutable skill head: %w", err)
		}
		snapshot.ParentID = parentID
		if snapshotSHA256 != predecessorSkillSnapshotSHA256(snapshot) ||
			legacyReference != currentMainWorkingLegacyReference(skillID, baseVersionNumber, snapshot, createdAt) {
			mutableHeadRows.Close()
			return fmt.Errorf("captured mutable skill head %s has invalid content identity", legacyReference)
		}
		basedOn := versionReferences[skillID][baseVersionNumber]
		if basedOn == "" {
			mutableHeadRows.Close()
			return fmt.Errorf("captured mutable skill head %s has no selected published base", legacyReference)
		}
		// A predecessor head had no ownership: every organization member could
		// read it. An attributed head stays private to its author, but a head
		// with an unknown author is retained as a public revision because it was
		// already organization-visible and a private revision with an empty
		// creator could never be read again. Latest stays on the published head
		// either way, so default consumers see no change.
		retainedHeadIsPublic := creatorSubject == ""
		candidates = append(candidates, &durableRevisionMigrationCandidate{
			legacyReference: legacyReference,
			skillID:         targetByLegacySkill[skillID],
			creatorSubject:  creatorSubject,
			basedOnLegacy:   basedOn,
			sourceLegacy:    latestVersionReference[parentID],
			origin:          RevisionOriginMigrated,
			snapshot: canonicalSkillRevisionSnapshot(SkillRevisionSnapshot{
				Name: snapshot.Name, Description: snapshot.Description,
				Type: snapshot.Type, Tags: snapshot.Tags, Content: snapshot.Content,
				IsAIGenerated:    snapshot.IsAIGenerated,
				SourceSessionIDs: snapshot.SourceSessionIDs,
			}),
			createdAt: createdAt.UTC(), isPublic: retainedHeadIsPublic,
			orderGroup: 2, orderSource: "skill-working:" + skillID,
		})
	}
	if err := mutableHeadRows.Err(); err != nil {
		mutableHeadRows.Close()
		return fmt.Errorf("iterate captured mutable skill heads: %w", err)
	}
	mutableHeadRows.Close()

	for _, skill := range skills {
		targetID := targetByLegacySkill[skill.id]
		if targetID != skill.id {
			continue
		}
		latestReference := latestVersionReference[skill.id]
		if skill.currentVersionNumber > 0 {
			latestReference = versionReferences[skill.id][skill.currentVersionNumber]
		}
		if latestReference != "" {
			desiredLatestReference[targetID] = latestReference
		}
	}

	draftByID := make(map[string]*legacyDraftForRevisionMigration, len(drafts))
	draftRevisionReferences := make(map[string]map[int]string, len(drafts))
	draftCandidateByLegacy := make(map[string]*durableRevisionMigrationCandidate)
	for index := range drafts {
		draft := &drafts[index]
		draftByID[draft.id] = draft
		legacyTarget := draft.targetSkillID
		if legacyTarget == "" {
			legacyTarget = draft.publishedSkillID
		}
		if draft.baseVersionNumber > 0 && versionReferences[legacyTarget] != nil {
			draft.baseLegacyReference = versionReferences[legacyTarget][draft.baseVersionNumber]
		}
		if draft.baseLegacyReference == "" {
			draft.baseLegacyReference = latestVersionReference[legacyTarget]
		}
	}

	draftRows, err := tx.Query(ctx, fmt.Sprintf(`SELECT
		r.draft_id::text, r.revision_number, r.origin,
		COALESCE(r.generation_id::text, ''), r.idempotency_key,
		r.name, r.description, r.type, r.tags, r.content, r.is_ai_generated,
		r.source_session_ids, COALESCE(r.parent_id::text, ''), r.created_at
		FROM %s.draft_revisions r
		ORDER BY r.draft_id, r.revision_number`, schema))
	if err != nil {
		return fmt.Errorf("query legacy draft revisions: %w", err)
	}
	for draftRows.Next() {
		var draftID, legacyOrigin, generationID, idempotencyKey, parentID string
		var revisionNumber int
		var createdAt time.Time
		var snapshot SkillRevisionSnapshot
		if err := draftRows.Scan(&draftID, &revisionNumber, &legacyOrigin,
			&generationID, &idempotencyKey, &snapshot.Name, &snapshot.Description,
			&snapshot.Type, &snapshot.Tags, &snapshot.Content, &snapshot.IsAIGenerated,
			&snapshot.SourceSessionIDs, &parentID, &createdAt); err != nil {
			draftRows.Close()
			return fmt.Errorf("scan legacy draft revision: %w", err)
		}
		draft := draftByID[draftID]
		if draft == nil {
			draftRows.Close()
			return fmt.Errorf("draft revision %s:%d has no draft", draftID, revisionNumber)
		}
		legacyReference := fmt.Sprintf("draft-revision:%s:%d", draftID, revisionNumber)
		basedOn := draft.latestLegacyReference
		if basedOn == "" {
			basedOn = draft.baseLegacyReference
		}
		origin := RevisionOriginMigrated
		if legacyOrigin == "generation" {
			origin = RevisionOriginGeneration
		}
		migratedIdempotencyKey := ""
		if generationID != "" {
			migratedIdempotencyKey = "generation:" + generationID
		}
		candidate := &durableRevisionMigrationCandidate{
			legacyReference: legacyReference, legacyDraftID: draftID,
			skillID:        draft.targetUnifiedSkillID,
			creatorSubject: draft.ownerSubject, basedOnLegacy: basedOn,
			origin: origin, snapshot: canonicalSkillRevisionSnapshot(snapshot),
			generationID: generationID, idempotencyKey: migratedIdempotencyKey,
			createdAt: createdAt.UTC(), isPublic: false,
			orderGroup: 1, orderSource: draftID, orderOrdinal: revisionNumber,
		}
		if parentID != "" {
			candidate.sourceLegacy = latestVersionReference[parentID]
		}
		candidates = append(candidates, candidate)
		if draftRevisionReferences[draftID] == nil {
			draftRevisionReferences[draftID] = make(map[int]string)
		}
		draftRevisionReferences[draftID][revisionNumber] = legacyReference
		draftCandidateByLegacy[legacyReference] = candidate
		draft.latestLegacyReference = legacyReference
		draft.latestSourceLegacy = candidate.sourceLegacy
		draft.latestRevisionSnapshot = candidate.snapshot
		draft.latestRevisionIsGenerated = origin == RevisionOriginGeneration
	}
	if err := draftRows.Err(); err != nil {
		draftRows.Close()
		return fmt.Errorf("iterate legacy draft revisions: %w", err)
	}
	draftRows.Close()
	if err := freezeExistingMigratedCandidateLineage(ctx, tx, schema, candidates); err != nil {
		return err
	}

	workingExpectations := make([]durableRevisionWorkingExpectation, 0, len(drafts))
	for _, draft := range drafts {
		working := canonicalSkillRevisionSnapshot(draft.working)
		workingSourceLegacy := latestVersionReference[draft.workingParentID]

		// A non-null predecessor pointer is the selected checkpoint, even when a
		// newer generated checkpoint is awaiting resolution. Only an autosave
		// with no pointer follows the newest checkpoint.
		selectedLegacy := draft.latestLegacyReference
		if draft.workingRevisionNumber > 0 {
			selectedLegacy = draftRevisionReferences[draft.id][draft.workingRevisionNumber]
			if selectedLegacy == "" {
				return fmt.Errorf("draft %s points to missing working revision %d", draft.id, draft.workingRevisionNumber)
			}
		}
		selected := draftCandidateByLegacy[selectedLegacy]
		// A later publication of the same source skill cannot change the source
		// revision selected by an already-migrated checkpoint.
		if selected != nil && legacySkillVersionReferenceMatches(selected.sourceLegacy, draft.workingParentID) {
			workingSourceLegacy = selected.sourceLegacy
		}
		workingMatchesSelected := selected != nil &&
			selected.sourceLegacy == workingSourceLegacy &&
			skillRevisionSnapshotSHA256(working) == skillRevisionSnapshotSHA256(selected.snapshot)
		generatedRepair := false
		// The predecessor finalizer copied the winning content into Working but
		// accidentally omitted the candidate's source-session slice. Repair is
		// valid only for the explicitly selected generated checkpoint (or the
		// newest checkpoint when this is an uncheckpointed autosave).
		if !workingMatchesSelected && selected != nil &&
			selected.origin == RevisionOriginGeneration && selected.sourceLegacy == workingSourceLegacy {
			workingWithoutSources := working
			selectedWithoutSources := selected.snapshot
			workingWithoutSources.SourceSessionIDs = []string{}
			selectedWithoutSources.SourceSessionIDs = []string{}
			workingMatchesSelected = skillRevisionSnapshotSHA256(workingWithoutSources) ==
				skillRevisionSnapshotSHA256(selectedWithoutSources)
			generatedRepair = workingMatchesSelected
		}
		if workingMatchesSelected {
			workingExpectations = append(workingExpectations, durableRevisionWorkingExpectation{
				draftID: draft.id, mappedLegacy: selectedLegacy,
				sourceLegacy: workingSourceLegacy, snapshot: working,
				updatedAt:            draft.updatedAt.UTC(),
				allowGeneratedRepair: generatedRepair,
			})
			continue
		}
		basedOn := selectedLegacy
		if basedOn == "" {
			basedOn = draft.baseLegacyReference
		}
		legacyReference := draftWorkingLegacyReference(draft.id, working, draft.updatedAt)
		workingCandidate := &durableRevisionMigrationCandidate{
			legacyReference: legacyReference, legacyDraftID: draft.id,
			skillID:        draft.targetUnifiedSkillID,
			creatorSubject: draft.ownerSubject, basedOnLegacy: basedOn,
			origin: RevisionOriginMigrated, snapshot: working, sourceLegacy: workingSourceLegacy,
			createdAt: draft.updatedAt.UTC(), isPublic: false, orderGroup: 2,
			orderSource: draft.id,
		}
		if err := freezeExistingMigratedCandidateLineage(ctx, tx, schema,
			[]*durableRevisionMigrationCandidate{workingCandidate}); err != nil {
			return err
		}
		candidates = append(candidates, workingCandidate)
		draftCandidateByLegacy[legacyReference] = workingCandidate
		workingExpectations = append(workingExpectations, durableRevisionWorkingExpectation{
			draftID: draft.id, mappedLegacy: legacyReference,
			sourceLegacy: workingCandidate.sourceLegacy, snapshot: working,
			updatedAt: draft.updatedAt.UTC(),
		})
		draft.latestLegacyReference = legacyReference
		draft.latestRevisionSnapshot = working
	}

	generations, err := loadLegacyMigrationGenerations(ctx, tx, schema, latestVersionReference)
	if err != nil {
		return err
	}
	for index := range generations {
		generation := &generations[index]
		draft := draftByID[generation.draftID]
		if draft == nil {
			return fmt.Errorf("generation %s has no draft", generation.id)
		}
		if generation.existingBaseRevisionID != "" {
			if generation.existingBaseLegacyReference == "" {
				return fmt.Errorf("generation %s existing base revision has no retained migration identity", generation.id)
			}
			generation.baseLegacyReference = generation.existingBaseLegacyReference
		} else {
			generation.baseLegacyReference = draft.baseLegacyReference
			latestPriorLegacy := draft.baseLegacyReference
			var latestPriorAt, exactMatchAt time.Time
			foundExactMatch := false
			for _, candidate := range candidates {
				relevantBase := candidate.legacyReference == draft.baseLegacyReference
				relevantDraft := candidate.legacyDraftID == generation.draftID
				if (!relevantBase && !relevantDraft) || candidate.createdAt.After(generation.createdAt) {
					continue
				}
				if relevantDraft && (latestPriorAt.IsZero() || !candidate.createdAt.Before(latestPriorAt)) {
					latestPriorLegacy = candidate.legacyReference
					latestPriorAt = candidate.createdAt
				}
				if candidate.sourceLegacy == generation.inputSourceLegacy &&
					skillRevisionSnapshotSHA256(candidate.snapshot) == skillRevisionSnapshotSHA256(generation.input) &&
					(!foundExactMatch || !candidate.createdAt.Before(exactMatchAt)) {
					generation.baseLegacyReference = candidate.legacyReference
					exactMatchAt = candidate.createdAt
					foundExactMatch = true
				}
			}
			if !foundExactMatch {
				// A predecessor generation could snapshot an autosaved working copy
				// that was never checkpointed and was later replaced. Preserve that
				// exact reachable input rather than silently retargeting its base.
				legacyReference := "generation-input:" + generation.id
				inputCandidate := &durableRevisionMigrationCandidate{
					legacyReference: legacyReference, legacyDraftID: generation.draftID,
					skillID: draft.targetUnifiedSkillID, creatorSubject: generation.ownerSubject,
					basedOnLegacy: latestPriorLegacy, sourceLegacy: generation.inputSourceLegacy,
					origin: RevisionOriginMigrated, snapshot: generation.input,
					createdAt: generation.createdAt.UTC(), isPublic: false,
					orderGroup: 1, orderSource: generation.draftID,
				}
				if err := freezeExistingMigratedCandidateLineage(ctx, tx, schema,
					[]*durableRevisionMigrationCandidate{inputCandidate}); err != nil {
					return err
				}
				candidates = append(candidates, inputCandidate)
				generation.baseLegacyReference = legacyReference
			}
		}

		// Generated output is a sibling of any checkpoint saved after the
		// generation began. Its immutable parent is the exact input revision,
		// never whichever draft checkpoint happened to precede finalization.
		for _, candidate := range candidates {
			if candidate.generationID == generation.id {
				candidate.basedOnLegacy = generation.baseLegacyReference
				candidate.repairBasedOn = true
			}
		}
	}

	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].skillID != candidates[right].skillID {
			return candidates[left].skillID < candidates[right].skillID
		}
		if candidates[left].orderGroup != candidates[right].orderGroup {
			return candidates[left].orderGroup < candidates[right].orderGroup
		}
		if candidates[left].orderGroup > 0 && !candidates[left].createdAt.Equal(candidates[right].createdAt) {
			return candidates[left].createdAt.Before(candidates[right].createdAt)
		}
		if candidates[left].orderSource != candidates[right].orderSource {
			return candidates[left].orderSource < candidates[right].orderSource
		}
		if candidates[left].orderOrdinal != candidates[right].orderOrdinal {
			return candidates[left].orderOrdinal < candidates[right].orderOrdinal
		}
		return candidates[left].legacyReference < candidates[right].legacyReference
	})

	hadUnifiedRevisions := make(map[string]bool)
	for _, candidate := range candidates {
		if _, checked := hadUnifiedRevisions[candidate.skillID]; checked {
			continue
		}
		var exists bool
		if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (
			SELECT 1 FROM %s.skill_revisions WHERE skill_id = $1
		)`, schema), candidate.skillID).Scan(&exists); err != nil {
			return fmt.Errorf("inspect existing unified revisions: %w", err)
		}
		hadUnifiedRevisions[candidate.skillID] = exists
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills skill SET next_sequence_number = GREATEST(
			skill.next_sequence_number,
			COALESCE((SELECT max(revision.sequence_number) + 1 FROM %s.skill_revisions revision WHERE revision.skill_id = skill.id), 1)
		) WHERE skill.id = $1`, schema, schema), candidate.skillID); err != nil {
			return fmt.Errorf("initialize migrated revision sequence: %w", err)
		}
	}

	revisionIDByLegacy, generationResultLegacy, err := loadRetainedMigratedRevisionMappings(ctx, tx, schema)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		candidate.id = uuid.NewSHA1(durableRevisionMigrationNamespace, []byte(candidate.legacyReference)).String()
		var existingID, existingSkillID string
		err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT id::text, skill_id::text
			FROM %s.skill_revisions WHERE legacy_reference = $1`, schema), candidate.legacyReference).Scan(&existingID, &existingSkillID)
		if err == nil {
			if existingSkillID != candidate.skillID {
				return fmt.Errorf("legacy revision %s was mapped to skill %s, expected %s", candidate.legacyReference, existingSkillID, candidate.skillID)
			}
			candidate.assignedRevisionID = existingID
			revisionIDByLegacy[candidate.legacyReference] = existingID
			if candidate.generationID != "" {
				if retainedLegacy := generationResultLegacy[candidate.generationID]; retainedLegacy != "" && retainedLegacy != candidate.legacyReference {
					return fmt.Errorf("generation %s has conflicting migrated result mappings", candidate.generationID)
				}
				generationResultLegacy[candidate.generationID] = candidate.legacyReference
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read migrated revision identity: %w", err)
		}

		// Private revisions are readable only by their creator, and no
		// authenticated caller has an empty subject: such a row would be lost
		// forever. Predecessor drafts and generations always carried an owner,
		// so this is a migration bug, not a data shape to tolerate.
		if !candidate.isPublic && candidate.creatorSubject == "" {
			return fmt.Errorf("migrated revision %s would be private without a creator", candidate.legacyReference)
		}
		var sequence int
		if err := tx.QueryRow(ctx, fmt.Sprintf(`UPDATE %s.skills SET next_sequence_number = next_sequence_number + 1
			WHERE id = $1 RETURNING next_sequence_number - 1`, schema), candidate.skillID).Scan(&sequence); err != nil {
			return fmt.Errorf("allocate migrated revision sequence: %w", err)
		}
		candidate.assignedRevisionID = candidate.id
		_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_revisions (
			id, skill_id, sequence_number, version, creator_subject, origin,
			name, description, type, tags, content, is_ai_generated,
			source_session_ids, content_sha256, change_note, generation_id,
			idempotency_key, legacy_reference, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, NULLIF($16, '')::uuid, $17, $18, $19)`, schema),
			candidate.id, candidate.skillID, sequence, fmt.Sprintf("%d", sequence),
			candidate.creatorSubject, candidate.origin, candidate.snapshot.Name,
			candidate.snapshot.Description, candidate.snapshot.Type,
			nonNilStrings(candidate.snapshot.Tags), candidate.snapshot.Content,
			candidate.snapshot.IsAIGenerated, nonNilStrings(candidate.snapshot.SourceSessionIDs),
			skillRevisionSnapshotSHA256(candidate.snapshot), candidate.changeNote,
			candidate.generationID, candidate.idempotencyKey,
			candidate.legacyReference, candidate.createdAt)
		if err != nil {
			return fmt.Errorf("insert migrated revision %s: %w", candidate.legacyReference, err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_revision_visibility (
			revision_id, is_public, changed_by_subject, changed_at
		) VALUES ($1, $2, $3, $4)`, schema), candidate.id, candidate.isPublic,
			candidate.creatorSubject, candidate.createdAt); err != nil {
			return fmt.Errorf("insert migrated revision visibility: %w", err)
		}
		candidate.inserted = true
		revisionIDByLegacy[candidate.legacyReference] = candidate.id
		if candidate.generationID != "" {
			if retainedLegacy := generationResultLegacy[candidate.generationID]; retainedLegacy != "" && retainedLegacy != candidate.legacyReference {
				return fmt.Errorf("generation %s has conflicting migrated result mappings", candidate.generationID)
			}
			generationResultLegacy[candidate.generationID] = candidate.legacyReference
		}
	}

	for _, candidate := range candidates {
		basedOnRevisionID := revisionIDByLegacy[candidate.basedOnLegacy]
		sourceRevisionID := revisionIDByLegacy[candidate.sourceLegacy]
		switch {
		case candidate.inserted:
			if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_revisions SET
				based_on_revision_id = NULLIF($2, '')::uuid,
				source_revision_id = NULLIF($3, '')::uuid
				WHERE id = $1`, schema), candidate.assignedRevisionID, basedOnRevisionID, sourceRevisionID); err != nil {
				return fmt.Errorf("link migrated revision lineage: %w", err)
			}
		case candidate.repairBasedOn:
			if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_revisions SET
				based_on_revision_id = NULLIF($2, '')::uuid
				WHERE id = $1 AND generation_id IS NOT NULL`, schema),
				candidate.assignedRevisionID, basedOnRevisionID); err != nil {
				return fmt.Errorf("repair migrated generation result lineage: %w", err)
			}
		}
	}

	generationExpectations := make([]durableRevisionGenerationExpectation, 0, len(generations))
	for _, generation := range generations {
		draft := draftByID[generation.draftID]
		baseRevisionID := generation.existingBaseRevisionID
		if baseRevisionID == "" {
			baseRevisionID = revisionIDByLegacy[generation.baseLegacyReference]
		}
		resultLegacy := generation.existingResultLegacyReference
		if resultLegacy == "" {
			resultLegacy = generationResultLegacy[generation.id]
		}
		resultCandidateID := generation.existingResultCandidateID
		if resultCandidateID == "" {
			resultCandidateID = generation.proposedCandidateID
		}
		if resultCandidateID == "" {
			resultCandidateID = generation.existingWinnerCandidateID
		}
		winnerCandidateID, winnerErr := migratedInitialWinnerCandidateID(
			ctx, tx, schema, generation.id, generation.existingWinnerCandidateID,
		)
		if winnerErr != nil {
			return winnerErr
		}
		resultRevisionID := generation.existingResultRevisionID
		if resultRevisionID == "" {
			resultRevisionID = revisionIDByLegacy[resultLegacy]
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET
			skill_id = COALESCE(skill_id, $2::uuid),
			creator_subject = COALESCE(creator_subject, $3),
			base_revision_id = COALESCE(base_revision_id, NULLIF($4, '')::uuid),
			winner_candidate_id = NULLIF($5, '')::uuid,
			result_candidate_id = COALESCE(result_candidate_id, NULLIF($6, '')::uuid),
			result_revision_id = COALESCE(result_revision_id, NULLIF($7, '')::uuid),
			draft_id = NULL, owner_subject = NULL, starting_lock_version = NULL,
			proposed_candidate_id = NULL, proposed_revision_number = NULL,
			resolution = NULL
			WHERE id = $1`, schema), generation.id, draft.targetUnifiedSkillID,
			generation.ownerSubject, baseRevisionID, winnerCandidateID, resultCandidateID, resultRevisionID); err != nil {
			return fmt.Errorf("link migrated generation %s: %w", generation.id, err)
		}
		generationExpectations = append(generationExpectations, durableRevisionGenerationExpectation{
			id: generation.id, skillID: draft.targetUnifiedSkillID,
			creatorSubject:        generation.ownerSubject,
			baseRevisionID:        baseRevisionID,
			baseLegacyReference:   generation.baseLegacyReference,
			winnerCandidateID:     winnerCandidateID,
			resultCandidateID:     resultCandidateID,
			resultRevisionID:      resultRevisionID,
			resultLegacyReference: resultLegacy,
			input:                 generation.input, inputSourceLegacy: generation.inputSourceLegacy,
		})
	}
	// A prior staged release could already have written skill-scoped rows while
	// the predecessor columns still had defaults. Canonicalize every modern row
	// so claims can require an unambiguous post-migration shape.
	if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET
		draft_id = NULL, owner_subject = NULL, starting_lock_version = NULL,
		proposed_candidate_id = NULL, proposed_revision_number = NULL,
		resolution = NULL
		WHERE skill_id IS NOT NULL AND creator_subject IS NOT NULL`, schema)); err != nil {
		return fmt.Errorf("clear migrated generation legacy links: %w", err)
	}

	latestExpectations := make(map[string]durableRevisionLatestExpectation)
	for skillID, latestLegacy := range desiredLatestReference {
		latestRevisionID := revisionIDByLegacy[latestLegacy]
		if !hadUnifiedRevisions[skillID] {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills SET
				explicit_latest_revision_id = $2,
				migration_managed_latest_revision_id = $2
				WHERE id = $1`, schema), skillID, latestRevisionID); err != nil {
				return fmt.Errorf("set initial migrated explicit latest revision: %w", err)
			}
			continue
		}

		// Advance only a pointer still equal to the alias recorded by the prior
		// migration pass. A user move or clear makes the values diverge; clear
		// the marker and leave that explicit choice untouched forever.
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills SET
			explicit_latest_revision_id = $2,
			migration_managed_latest_revision_id = $2
			WHERE id = $1
			  AND migration_managed_latest_revision_id IS NOT NULL
			  AND explicit_latest_revision_id IS NOT DISTINCT FROM migration_managed_latest_revision_id`, schema),
			skillID, latestRevisionID); err != nil {
			return fmt.Errorf("advance migration-managed explicit latest revision: %w", err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills SET
			migration_managed_latest_revision_id = NULL
			WHERE id = $1
			  AND migration_managed_latest_revision_id IS NOT NULL
			  AND explicit_latest_revision_id IS DISTINCT FROM migration_managed_latest_revision_id`, schema), skillID); err != nil {
			return fmt.Errorf("release user-managed explicit latest revision: %w", err)
		}
	}
	for skillID := range hadUnifiedRevisions {
		var latest durableRevisionLatestExpectation
		if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT
			COALESCE(explicit_latest_revision_id::text, ''),
			COALESCE(migration_managed_latest_revision_id::text, '')
			FROM %s.skills WHERE id = $1`, schema), skillID).Scan(
			&latest.explicitRevisionID, &latest.managedRevisionID,
		); err != nil {
			return fmt.Errorf("read migrated explicit latest metadata: %w", err)
		}
		latestExpectations[skillID] = latest
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills skill SET next_sequence_number = GREATEST(
		skill.next_sequence_number,
		COALESCE((SELECT max(revision.sequence_number) + 1 FROM %s.skill_revisions revision WHERE revision.skill_id = skill.id), 1)
	)`, schema, schema)); err != nil {
		return fmt.Errorf("finalize migrated revision sequences: %w", err)
	}
	aliases := make(map[string]string)
	for sourceSkillID, targetSkillID := range targetByLegacySkill {
		if sourceSkillID != targetSkillID {
			aliases[sourceSkillID] = targetSkillID
		}
	}
	expectations.candidates = candidates
	expectations.working = workingExpectations
	expectations.generations = generationExpectations
	expectations.latest = latestExpectations
	expectations.aliases = aliases
	expectations.revisionIDByLegacy = revisionIDByLegacy
	return nil
}

// verifyDurableRevisionIdentities runs before any deferred constraint is
// installed. A failed count, hash, lineage, pointer, or allocation invariant
// aborts the whole migration transaction and leaves the predecessor schema
// untouched.
func (s *PostgresStore) verifyDurableRevisionIdentities(ctx context.Context, tx pgx.Tx, expectations *durableRevisionMigrationExpectations) error {
	if expectations == nil {
		return errors.New("durable revision migration did not retain source expectations")
	}
	schema := quoteIdentifier(s.schema)
	checks := []struct {
		name  string
		query string
	}{
		{name: "published mappings", query: fmt.Sprintf(`SELECT count(*) FROM %s.skill_versions legacy
			LEFT JOIN %s.skill_revisions revision ON revision.legacy_reference =
				'skill-version:' || legacy.skill_id::text || ':' || legacy.version_number::text
			WHERE revision.id IS NULL`, schema, schema)},
		{name: "draft mappings", query: fmt.Sprintf(`SELECT count(*) FROM %s.draft_revisions legacy
			LEFT JOIN %s.skill_revisions revision ON revision.legacy_reference =
				'draft-revision:' || legacy.draft_id::text || ':' || legacy.revision_number::text
			WHERE revision.id IS NULL`, schema, schema)},
		{name: "visibility rows", query: fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions revision
			LEFT JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
			WHERE visibility.revision_id IS NULL`, schema, schema)},
		{name: "same-skill base lineage", query: fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions child
			JOIN %s.skill_revisions base ON base.id = child.based_on_revision_id
			WHERE child.skill_id <> base.skill_id`, schema, schema)},
		{name: "latest pointers", query: fmt.Sprintf(`SELECT count(*) FROM %s.skills skill
			LEFT JOIN %s.skill_revisions revision ON revision.id = skill.explicit_latest_revision_id
			LEFT JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
			WHERE skill.explicit_latest_revision_id IS NOT NULL AND (
				revision.id IS NULL OR visibility.revision_id IS NULL OR
				revision.skill_id <> skill.id OR NOT visibility.is_public
			)`, schema, schema, schema)},
		{name: "migration-managed latest pointers", query: fmt.Sprintf(`SELECT count(*) FROM %s.skills skill
			LEFT JOIN %s.skill_revisions revision ON revision.id = skill.migration_managed_latest_revision_id
			LEFT JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
			WHERE skill.migration_managed_latest_revision_id IS NOT NULL AND (
				skill.explicit_latest_revision_id IS DISTINCT FROM skill.migration_managed_latest_revision_id OR
				revision.id IS NULL OR visibility.revision_id IS NULL OR
				revision.skill_id <> skill.id OR NOT visibility.is_public
			)`, schema, schema, schema)},
		{name: "generation skill identities", query: fmt.Sprintf(`SELECT count(*) FROM %s.skill_generations
			WHERE skill_id IS NULL OR creator_subject IS NULL OR creator_subject = ''`, schema)},
		{name: "generation same-skill base lineage", query: fmt.Sprintf(`SELECT count(*) FROM %s.skill_generations generation
			LEFT JOIN %s.skill_revisions base ON base.id = generation.base_revision_id
			WHERE generation.base_revision_id IS NOT NULL AND (
				base.id IS NULL OR base.skill_id IS DISTINCT FROM generation.skill_id
			)`, schema, schema)},
		{name: "generation result lineage", query: fmt.Sprintf(`SELECT count(*) FROM %s.skill_generations generation
			LEFT JOIN %s.skill_revisions revision ON revision.id = generation.result_revision_id
			WHERE generation.result_revision_id IS NOT NULL AND (
				revision.id IS NULL OR revision.generation_id IS DISTINCT FROM generation.id
				OR revision.skill_id IS DISTINCT FROM generation.skill_id
			)`, schema, schema)},
		{name: "duplicate canonical normalized slugs", query: fmt.Sprintf(`SELECT count(*) FROM (
			SELECT slug FROM %s.skills WHERE migration_alias_of_skill_id IS NULL
			GROUP BY slug HAVING count(*) > 1
		) duplicate`, schema)},
		{name: "invalid migration aliases", query: fmt.Sprintf(`SELECT count(*) FROM %s.skills alias
			LEFT JOIN %s.skills canonical ON canonical.id = alias.migration_alias_of_skill_id
			WHERE alias.migration_alias_of_skill_id IS NOT NULL AND (
				canonical.id IS NULL OR canonical.migration_alias_of_skill_id IS NOT NULL OR
				canonical.id = alias.id OR canonical.slug <> alias.slug
			)`, schema, schema)},
		{name: "revision mapped to migration alias", query: fmt.Sprintf(`SELECT count(*) FROM %s.skill_revisions revision
			JOIN %s.skills skill ON skill.id = revision.skill_id
			WHERE skill.migration_alias_of_skill_id IS NOT NULL`, schema, schema)},
		{name: "duplicate revision idempotency identities", query: fmt.Sprintf(`SELECT count(*) FROM (
			SELECT skill_id, idempotency_key FROM %s.skill_revisions
			WHERE idempotency_key <> '' GROUP BY skill_id, idempotency_key HAVING count(*) > 1
		) duplicate`, schema)},
	}
	for _, check := range checks {
		var violations int
		if err := tx.QueryRow(ctx, check.query).Scan(&violations); err != nil {
			return fmt.Errorf("check %s: %w", check.name, err)
		}
		if violations != 0 {
			return fmt.Errorf("check %s: found %d violations", check.name, violations)
		}
	}
	if err := verifyDurableRevisionSourceMappings(ctx, tx, schema, expectations); err != nil {
		return err
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT id::text, name, description, type,
		tags, content, is_ai_generated, source_session_ids, content_sha256
		FROM %s.skill_revisions`, schema))
	if err != nil {
		return fmt.Errorf("query migrated revision hashes: %w", err)
	}
	for rows.Next() {
		var id, contentSHA256 string
		var snapshot SkillRevisionSnapshot
		if err := rows.Scan(&id, &snapshot.Name, &snapshot.Description, &snapshot.Type,
			&snapshot.Tags, &snapshot.Content, &snapshot.IsAIGenerated,
			&snapshot.SourceSessionIDs, &contentSHA256); err != nil {
			rows.Close()
			return fmt.Errorf("scan migrated revision hash: %w", err)
		}
		if contentSHA256 != skillRevisionSnapshotSHA256(snapshot) {
			rows.Close()
			return fmt.Errorf("revision %s has a non-canonical content hash", id)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate migrated revision hashes: %w", err)
	}
	rows.Close()

	sequenceRows, err := tx.Query(ctx, fmt.Sprintf(`SELECT skill.id::text,
		count(revision.id)::int, COALESCE(max(revision.sequence_number), 0)::int,
		skill.next_sequence_number
		FROM %s.skills skill
		LEFT JOIN %s.skill_revisions revision ON revision.skill_id = skill.id
		GROUP BY skill.id, skill.next_sequence_number`, schema, schema))
	if err != nil {
		return fmt.Errorf("query migrated revision sequences: %w", err)
	}
	for sequenceRows.Next() {
		var skillID string
		var count, maximum, next int
		if err := sequenceRows.Scan(&skillID, &count, &maximum, &next); err != nil {
			sequenceRows.Close()
			return fmt.Errorf("scan migrated revision sequences: %w", err)
		}
		if count != maximum || next != maximum+1 {
			sequenceRows.Close()
			return fmt.Errorf("skill %s has non-gapless revision allocation count=%d max=%d next=%d", skillID, count, maximum, next)
		}
	}
	if err := sequenceRows.Err(); err != nil {
		sequenceRows.Close()
		return fmt.Errorf("iterate migrated revision sequences: %w", err)
	}
	sequenceRows.Close()

	return verifyDistinctDraftWorkingCopies(ctx, tx, schema, expectations)
}

func verifyDurableRevisionSourceMappings(ctx context.Context, tx pgx.Tx, schema string, expectations *durableRevisionMigrationExpectations) error {
	revisionIDByLegacy := expectations.revisionIDByLegacy
	if revisionIDByLegacy == nil {
		return errors.New("durable revision migration did not retain legacy revision mappings")
	}
	for _, expected := range expectations.candidates {
		actual, err := migratedRevisionByLegacyReference(ctx, tx, schema, expected.legacyReference)
		if err != nil {
			return fmt.Errorf("verify source %s: %w", expected.legacyReference, err)
		}
		expectedBaseID, err := requiredMappedRevisionID(expected.basedOnLegacy, revisionIDByLegacy)
		if err != nil {
			return fmt.Errorf("verify source %s base: %w", expected.legacyReference, err)
		}
		expectedSourceID, err := requiredMappedRevisionID(expected.sourceLegacy, revisionIDByLegacy)
		if err != nil {
			return fmt.Errorf("verify source %s lineage: %w", expected.legacyReference, err)
		}
		if actual.ID != expected.assignedRevisionID || actual.SkillID != expected.skillID ||
			actual.CreatorSubject != expected.creatorSubject ||
			actual.BasedOnRevisionID != expectedBaseID ||
			actual.SourceRevisionID != expectedSourceID || actual.Origin != expected.origin ||
			!equalRevisionSnapshots(actual.Snapshot, expected.snapshot) ||
			actual.ContentSHA256 != skillRevisionSnapshotSHA256(expected.snapshot) ||
			actual.ChangeNote != expected.changeNote || actual.GenerationID != expected.generationID ||
			actual.IdempotencyKey != expected.idempotencyKey ||
			actual.LegacyReference != expected.legacyReference ||
			!actual.CreatedAt.Equal(expected.createdAt) ||
			actual.SequenceNumber < 1 || actual.Version != fmt.Sprintf("%d", actual.SequenceNumber) {
			return fmt.Errorf("source %s does not match its complete migrated revision", expected.legacyReference)
		}

		if expected.inserted {
			var isPublic bool
			var changedBy string
			var changedAt time.Time
			if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT is_public, changed_by_subject, changed_at
				FROM %s.skill_revision_visibility WHERE revision_id = $1`, schema), actual.ID).
				Scan(&isPublic, &changedBy, &changedAt); err != nil {
				return fmt.Errorf("verify source %s visibility: %w", expected.legacyReference, err)
			}
			if isPublic != expected.isPublic || changedBy != expected.creatorSubject ||
				!changedAt.Equal(expected.createdAt) {
				return fmt.Errorf("source %s does not match migrated visibility and audit metadata", expected.legacyReference)
			}
		}
	}

	for _, expected := range expectations.working {
		actual, err := migratedRevisionByLegacyReference(ctx, tx, schema, expected.mappedLegacy)
		if err != nil {
			return fmt.Errorf("verify draft working source %s: %w", expected.draftID, err)
		}
		snapshotMatches := equalRevisionSnapshots(actual.Snapshot, expected.snapshot)
		if !snapshotMatches && expected.allowGeneratedRepair {
			actualWithoutSources := actual.Snapshot
			expectedWithoutSources := expected.snapshot
			actualWithoutSources.SourceSessionIDs = []string{}
			expectedWithoutSources.SourceSessionIDs = []string{}
			snapshotMatches = equalRevisionSnapshots(actualWithoutSources, expectedWithoutSources)
		}
		expectedSourceID, mapErr := requiredMappedRevisionID(expected.sourceLegacy, revisionIDByLegacy)
		if mapErr != nil {
			return fmt.Errorf("verify draft working source %s lineage: %w", expected.draftID, mapErr)
		}
		if !snapshotMatches || actual.SourceRevisionID != expectedSourceID {
			return fmt.Errorf("draft working source %s does not match mapped revision %s", expected.draftID, expected.mappedLegacy)
		}
	}

	for _, expected := range expectations.generations {
		expectedBaseID := expected.baseRevisionID
		if expectedBaseID == "" {
			var err error
			expectedBaseID, err = requiredMappedRevisionID(expected.baseLegacyReference, revisionIDByLegacy)
			if err != nil {
				return fmt.Errorf("verify generation %s base: %w", expected.id, err)
			}
		}
		expectedResultID := expected.resultRevisionID
		if expectedResultID == "" {
			var err error
			expectedResultID, err = requiredMappedRevisionID(expected.resultLegacyReference, revisionIDByLegacy)
			if err != nil {
				return fmt.Errorf("verify generation %s result: %w", expected.id, err)
			}
		}
		var skillID, creatorSubject, baseRevisionID, winnerCandidateID, resultCandidateID, resultRevisionID string
		if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT
			COALESCE(skill_id::text, ''), COALESCE(creator_subject, ''),
			COALESCE(base_revision_id::text, ''), COALESCE(winner_candidate_id::text, ''),
			COALESCE(result_candidate_id::text, ''), COALESCE(result_revision_id::text, '')
			FROM %s.skill_generations WHERE id = $1`, schema), expected.id).Scan(
			&skillID, &creatorSubject, &baseRevisionID, &winnerCandidateID,
			&resultCandidateID, &resultRevisionID,
		); err != nil {
			return fmt.Errorf("verify generation %s relationships: %w", expected.id, err)
		}
		if skillID != expected.skillID || creatorSubject != expected.creatorSubject ||
			baseRevisionID != expectedBaseID || winnerCandidateID != expected.winnerCandidateID ||
			resultCandidateID != expected.resultCandidateID ||
			resultRevisionID != expectedResultID {
			return fmt.Errorf("generation %s does not match source skill/base/result relationships", expected.id)
		}
		if expectedResultID != "" {
			result, resultErr := migratedRevisionByID(ctx, tx, schema, expectedResultID)
			if resultErr != nil {
				return fmt.Errorf("verify generation %s result revision: %w", expected.id, resultErr)
			}
			if result.GenerationID != expected.id || result.BasedOnRevisionID != expectedBaseID {
				return fmt.Errorf("generation %s result does not retain its exact generation/input lineage", expected.id)
			}
		}
		base, err := migratedRevisionByID(ctx, tx, schema, expectedBaseID)
		if err != nil {
			return fmt.Errorf("verify generation %s input revision: %w", expected.id, err)
		}
		expectedInputSourceID, err := requiredMappedRevisionID(expected.inputSourceLegacy, revisionIDByLegacy)
		if err != nil {
			return fmt.Errorf("verify generation %s input lineage: %w", expected.id, err)
		}
		if !equalRevisionSnapshots(base.Snapshot, expected.input) ||
			base.ContentSHA256 != skillRevisionSnapshotSHA256(expected.input) ||
			base.SourceRevisionID != expectedInputSourceID {
			return fmt.Errorf("generation %s input does not match its exact mapped revision", expected.id)
		}
	}

	for skillID, expected := range expectations.latest {
		var actual durableRevisionLatestExpectation
		if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT
			COALESCE(explicit_latest_revision_id::text, ''),
			COALESCE(migration_managed_latest_revision_id::text, '')
			FROM %s.skills WHERE id = $1`, schema), skillID).Scan(
			&actual.explicitRevisionID, &actual.managedRevisionID,
		); err != nil {
			return fmt.Errorf("verify skill %s latest metadata: %w", skillID, err)
		}
		if actual != expected {
			return fmt.Errorf("skill %s explicit latest metadata changed during migration verification", skillID)
		}
	}
	for aliasID, canonicalID := range expectations.aliases {
		var actualCanonicalID string
		if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(migration_alias_of_skill_id::text, '')
			FROM %s.skills WHERE id = $1`, schema), aliasID).Scan(&actualCanonicalID); err != nil {
			return fmt.Errorf("verify legacy skill alias %s: %w", aliasID, err)
		}
		if actualCanonicalID != canonicalID {
			return fmt.Errorf("legacy skill alias %s maps to %s, expected %s", aliasID, actualCanonicalID, canonicalID)
		}
	}
	return nil
}

func migratedRevisionByLegacyReference(ctx context.Context, tx pgx.Tx, schema, legacyReference string) (SkillRevisionRecord, error) {
	if legacyReference == "" {
		return SkillRevisionRecord{}, errors.New("mapped legacy reference is empty")
	}
	row := tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM %s.skill_revisions r
		WHERE r.legacy_reference = $1`, skillRevisionColumns, schema), legacyReference)
	revision, err := scanSkillRevision(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return SkillRevisionRecord{}, fmt.Errorf("mapped revision %s not found", legacyReference)
	}
	return revision, err
}

func migratedRevisionByID(ctx context.Context, tx pgx.Tx, schema, revisionID string) (SkillRevisionRecord, error) {
	if revisionID == "" {
		return SkillRevisionRecord{}, errors.New("mapped revision id is empty")
	}
	row := tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM %s.skill_revisions r
		WHERE r.id = $1`, skillRevisionColumns, schema), revisionID)
	revision, err := scanSkillRevision(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return SkillRevisionRecord{}, fmt.Errorf("mapped revision %s not found", revisionID)
	}
	return revision, err
}

// loadRetainedMigratedRevisionMappings includes historical migration outputs
// that no longer have a row in mutable predecessor state. Generations can keep
// pointing at an older draft-working occurrence or synthetic generation input
// across later reconciliation passes only when those identities remain in the
// lookup used to link and verify this pass.
func loadRetainedMigratedRevisionMappings(ctx context.Context, tx pgx.Tx, schema string) (map[string]string, map[string]string, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT legacy_reference, id::text,
		COALESCE(generation_id::text, '')
		FROM %s.skill_revisions
		WHERE legacy_reference IS NOT NULL
		ORDER BY legacy_reference`, schema))
	if err != nil {
		return nil, nil, fmt.Errorf("query retained migrated revision mappings: %w", err)
	}
	defer rows.Close()

	revisionIDByLegacy := make(map[string]string)
	generationResultLegacy := make(map[string]string)
	for rows.Next() {
		var legacyReference, revisionID, generationID string
		if err := rows.Scan(&legacyReference, &revisionID, &generationID); err != nil {
			return nil, nil, fmt.Errorf("scan retained migrated revision mapping: %w", err)
		}
		revisionIDByLegacy[legacyReference] = revisionID
		if generationID == "" {
			continue
		}
		if retainedLegacy := generationResultLegacy[generationID]; retainedLegacy != "" && retainedLegacy != legacyReference {
			return nil, nil, fmt.Errorf("generation %s has conflicting retained result mappings", generationID)
		}
		generationResultLegacy[generationID] = legacyReference
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate retained migrated revision mappings: %w", err)
	}
	return revisionIDByLegacy, generationResultLegacy, nil
}

func requiredMappedRevisionID(legacyReference string, revisionIDByLegacy map[string]string) (string, error) {
	if legacyReference == "" {
		return "", nil
	}
	id := revisionIDByLegacy[legacyReference]
	if id == "" {
		return "", fmt.Errorf("legacy reference %s has no migration mapping", legacyReference)
	}
	return id, nil
}

func equalRevisionSnapshots(left, right SkillRevisionSnapshot) bool {
	return left.Name == right.Name && left.Description == right.Description &&
		left.Type == right.Type && equalStrings(left.Tags, right.Tags) &&
		left.Content == right.Content && left.IsAIGenerated == right.IsAIGenerated &&
		equalStrings(left.SourceSessionIDs, right.SourceSessionIDs)
}

func currentMainWorkingLegacyReference(skillID string, baseVersionNumber int, snapshot predecessorSkillSnapshot, updatedAt time.Time) string {
	identity := fmt.Sprintf("current-main-working:%s:%d:%s:%s",
		skillID, baseVersionNumber, predecessorSkillSnapshotSHA256(snapshot),
		updatedAt.UTC().Format(time.RFC3339Nano))
	return "skill-working:" + skillID + ":" +
		uuid.NewSHA1(durableRevisionMigrationNamespace, []byte(identity)).String()
}

func draftWorkingLegacyReference(draftID string, snapshot SkillRevisionSnapshot, updatedAt time.Time) string {
	identity := fmt.Sprintf("draft-working:%s:%s:%s", draftID,
		skillRevisionSnapshotSHA256(snapshot), updatedAt.UTC().Format(time.RFC3339Nano))
	return "draft-working:" + draftID + ":" +
		uuid.NewSHA1(durableRevisionMigrationNamespace, []byte(identity)).String()
}

func legacySkillVersionReferenceMatches(legacyReference, skillID string) bool {
	return skillID == "" && legacyReference == "" ||
		strings.HasPrefix(legacyReference, "skill-version:"+skillID+":")
}

// freezeExistingMigratedCandidateLineage keeps an already-mapped legacy row's
// ancestry stable across reconciliation passes. In particular, publishing a
// newer revision of a source skill must not retarget an existing child's
// source_revision_id during expectation construction or verification.
func freezeExistingMigratedCandidateLineage(ctx context.Context, tx pgx.Tx, schema string, candidates []*durableRevisionMigrationCandidate) error {
	for _, candidate := range candidates {
		var basedOnLegacy, sourceLegacy string
		err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT
			COALESCE(base.legacy_reference, ''), COALESCE(source.legacy_reference, '')
			FROM %s.skill_revisions child
			LEFT JOIN %s.skill_revisions base ON base.id = child.based_on_revision_id
			LEFT JOIN %s.skill_revisions source ON source.id = child.source_revision_id
			WHERE child.legacy_reference = $1`, schema, schema, schema), candidate.legacyReference).
			Scan(&basedOnLegacy, &sourceLegacy)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read frozen lineage for %s: %w", candidate.legacyReference, err)
		}
		candidate.basedOnLegacy = basedOnLegacy
		candidate.sourceLegacy = sourceLegacy
	}
	return nil
}

func loadLegacyMigrationSkills(ctx context.Context, tx pgx.Tx, schema string) ([]legacySkillForRevisionMigration, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT id::text, slug, author_subject,
		COALESCE(current_version_number, 0), created_at,
		COALESCE(migration_alias_of_skill_id::text, '')
		FROM %s.skills ORDER BY created_at, id`, schema))
	if err != nil {
		return nil, fmt.Errorf("query legacy skills: %w", err)
	}
	defer rows.Close()
	skills := make([]legacySkillForRevisionMigration, 0)
	for rows.Next() {
		var skill legacySkillForRevisionMigration
		if err := rows.Scan(&skill.id, &skill.slug, &skill.authorSubject,
			&skill.currentVersionNumber, &skill.createdAt, &skill.migrationAliasOf); err != nil {
			return nil, fmt.Errorf("scan legacy skill: %w", err)
		}
		skills = append(skills, skill)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy skills: %w", err)
	}
	return skills, nil
}

func loadLegacyMigrationDrafts(ctx context.Context, tx pgx.Tx, schema string) ([]legacyDraftForRevisionMigration, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT id::text,
		COALESCE(target_skill_id::text, ''), COALESCE(published_skill_id::text, ''),
		owner_subject, COALESCE(base_skill_version_number, 0),
		COALESCE(working_revision_number, 0), working_slug, working_name,
		working_description, working_type, working_tags, working_content,
		working_is_ai_generated, working_source_session_ids,
		COALESCE(working_parent_id::text, ''), created_at, updated_at
		FROM %s.skill_drafts ORDER BY created_at, id`, schema))
	if err != nil {
		return nil, fmt.Errorf("query legacy drafts: %w", err)
	}
	defer rows.Close()
	drafts := make([]legacyDraftForRevisionMigration, 0)
	for rows.Next() {
		var draft legacyDraftForRevisionMigration
		if err := rows.Scan(&draft.id, &draft.targetSkillID, &draft.publishedSkillID,
			&draft.ownerSubject, &draft.baseVersionNumber, &draft.workingRevisionNumber,
			&draft.workingSlug, &draft.working.Name, &draft.working.Description,
			&draft.working.Type, &draft.working.Tags, &draft.working.Content,
			&draft.working.IsAIGenerated, &draft.working.SourceSessionIDs,
			&draft.workingParentID, &draft.createdAt, &draft.updatedAt); err != nil {
			return nil, fmt.Errorf("scan legacy draft: %w", err)
		}
		draft.working = canonicalSkillRevisionSnapshot(draft.working)
		drafts = append(drafts, draft)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy drafts: %w", err)
	}
	return drafts, nil
}

// migratedInitialWinnerCandidateID preserves a final result separately from its
// deterministic pre-synthesis winner. Older finalizers stored the selected
// synthesis candidate in winner_candidate_id; ranking the persisted
// matching-profile non-synthesis evaluations reconstructs the only truthful
// initial lineage without another model call.
func migratedInitialWinnerCandidateID(
	ctx context.Context,
	tx pgx.Tx,
	schema, generationID, storedWinnerCandidateID string,
) (string, error) {
	if storedWinnerCandidateID == "" {
		return "", nil
	}
	var kind GenerationCandidateKind
	err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT kind
		FROM %s.generation_candidates
		WHERE generation_id = $1 AND id = $2`, schema), generationID,
		storedWinnerCandidateID).Scan(&kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read generation %s winner: %w", generationID, err)
	}
	if kind != GenerationCandidateSynthesis {
		return storedWinnerCandidateID, nil
	}

	var winnerCandidateID string
	err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT candidate.id::text
		FROM %s.generation_candidates candidate
		JOIN %s.candidate_evaluations evaluation
		  ON evaluation.generation_id = candidate.generation_id
		 AND evaluation.candidate_id = candidate.id
		JOIN %s.skill_generations generation ON generation.id = candidate.generation_id
		WHERE candidate.generation_id = $1
		  AND candidate.kind <> 'synthesis'
		  AND evaluation.profile = generation.evaluator_profile
		  AND evaluation.profile_version = generation.evaluator_profile_version
		  AND evaluation.score BETWEEN 0 AND 1
		  AND evaluation.decision IN ('pass', 'revise')
		  AND evaluation.critical_finding_count >= 0
		  AND evaluation.warning_finding_count >= 0
		  AND NOT EXISTS (
			SELECT 1 FROM %s.candidate_evaluations mismatch
			WHERE mismatch.generation_id = candidate.generation_id
			  AND mismatch.candidate_id = candidate.id
			  AND (mismatch.profile <> generation.evaluator_profile
			    OR mismatch.profile_version <> generation.evaluator_profile_version
			    OR mismatch.profile = '' OR mismatch.profile_version = '')
		  )
		ORDER BY evaluation.score DESC,
		  CASE evaluation.decision WHEN 'pass' THEN 0 ELSE 1 END,
		  evaluation.critical_finding_count,
		  evaluation.warning_finding_count,
		  candidate.ordinal,
		  candidate.id::text
		LIMIT 1`, schema, schema, schema, schema), generationID).Scan(&winnerCandidateID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("generation %s synthesis result has no rankable persisted initial winner", generationID)
	}
	if err != nil {
		return "", fmt.Errorf("derive generation %s initial winner: %w", generationID, err)
	}
	return winnerCandidateID, nil
}

// repairModernSynthesisWinnerIdentities fixes rows already converted by the
// first durable-generation release. That release copied its final synthesis
// result into both winner and result; only winner changes here, so the selected
// result and its revision lineage remain untouched. Once repaired, the row no
// longer matches and every later migration pass is a no-op.
func (s *PostgresStore) repairModernSynthesisWinnerIdentities(ctx context.Context, tx pgx.Tx) error {
	schema := quoteIdentifier(s.schema)
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT generation.id::text,
		generation.winner_candidate_id::text
		FROM %s.skill_generations generation
		JOIN %s.generation_candidates synthesis
		  ON synthesis.generation_id = generation.id
		 AND synthesis.id = generation.winner_candidate_id
		WHERE generation.draft_id IS NULL
		  AND generation.winner_candidate_id IS NOT NULL
		  AND generation.winner_candidate_id = generation.result_candidate_id
		  AND synthesis.kind = 'synthesis'
		ORDER BY generation.id`, schema, schema))
	if err != nil {
		return fmt.Errorf("query modern synthesis winners: %w", err)
	}
	type synthesisWinner struct {
		generationID string
		candidateID  string
	}
	matches := make([]synthesisWinner, 0)
	for rows.Next() {
		var match synthesisWinner
		if err := rows.Scan(&match.generationID, &match.candidateID); err != nil {
			rows.Close()
			return fmt.Errorf("scan modern synthesis winner: %w", err)
		}
		matches = append(matches, match)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate modern synthesis winners: %w", err)
	}
	rows.Close()

	for _, match := range matches {
		winnerCandidateID, err := migratedInitialWinnerCandidateID(
			ctx, tx, schema, match.generationID, match.candidateID,
		)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations
			SET winner_candidate_id = $3
			WHERE id = $1
			  AND draft_id IS NULL
			  AND winner_candidate_id = $2
			  AND result_candidate_id = $2`, schema),
			match.generationID, match.candidateID, winnerCandidateID); err != nil {
			return fmt.Errorf("repair generation %s initial winner: %w", match.generationID, err)
		}
	}
	return nil
}

func loadLegacyMigrationGenerations(ctx context.Context, tx pgx.Tx, schema string, latestVersionReference map[string]string) ([]legacyGenerationForRevisionMigration, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT generation.id::text, generation.draft_id::text,
		generation.owner_subject, generation.input_name, generation.input_description,
		generation.input_type, generation.input_tags, generation.input_content,
		generation.input_is_ai_generated, generation.input_source_session_ids,
		COALESCE(generation.input_parent_id::text, ''),
		COALESCE(generation.proposed_candidate_id::text, ''), generation.created_at,
		COALESCE(generation.base_revision_id::text, ''),
		COALESCE(base.legacy_reference, ''), COALESCE(source.legacy_reference, ''),
		COALESCE(generation.winner_candidate_id::text, ''),
		COALESCE(generation.result_candidate_id::text, ''),
		COALESCE(generation.result_revision_id::text, ''),
		COALESCE(result.legacy_reference, '')
		FROM %s.skill_generations generation
		LEFT JOIN %s.skill_revisions base ON base.id = generation.base_revision_id
		LEFT JOIN %s.skill_revisions source ON source.id = base.source_revision_id
		LEFT JOIN %s.skill_revisions result ON result.id = generation.result_revision_id
		WHERE generation.draft_id IS NOT NULL
		ORDER BY generation.created_at, generation.id`, schema, schema, schema, schema))
	if err != nil {
		return nil, fmt.Errorf("query legacy generations: %w", err)
	}
	defer rows.Close()
	generations := make([]legacyGenerationForRevisionMigration, 0)
	for rows.Next() {
		var generation legacyGenerationForRevisionMigration
		var inputParentID string
		if err := rows.Scan(&generation.id, &generation.draftID,
			&generation.ownerSubject, &generation.input.Name,
			&generation.input.Description, &generation.input.Type,
			&generation.input.Tags, &generation.input.Content,
			&generation.input.IsAIGenerated, &generation.input.SourceSessionIDs,
			&inputParentID, &generation.proposedCandidateID, &generation.createdAt,
			&generation.existingBaseRevisionID,
			&generation.existingBaseLegacyReference, &generation.inputSourceLegacy,
			&generation.existingWinnerCandidateID, &generation.existingResultCandidateID,
			&generation.existingResultRevisionID,
			&generation.existingResultLegacyReference); err != nil {
			return nil, fmt.Errorf("scan legacy generation: %w", err)
		}
		generation.input = canonicalSkillRevisionSnapshot(generation.input)
		if generation.existingBaseLegacyReference == "" {
			generation.inputSourceLegacy = latestVersionReference[inputParentID]
		}
		generations = append(generations, generation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy generations: %w", err)
	}
	return generations, nil
}

func verifyDistinctDraftWorkingCopies(ctx context.Context, tx pgx.Tx, schema string, expectations *durableRevisionMigrationExpectations) error {
	for _, expected := range expectations.working {
		prefix := "draft-working:" + expected.draftID + ":"
		if !strings.HasPrefix(expected.mappedLegacy, prefix) {
			continue
		}
		expectedLegacy := draftWorkingLegacyReference(
			expected.draftID, expected.snapshot, expected.updatedAt,
		)
		if expected.mappedLegacy != expectedLegacy {
			return fmt.Errorf("draft %s working copy has an unstable migration identity", expected.draftID)
		}
		var contentSHA256 string
		if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT content_sha256
			FROM %s.skill_revisions WHERE legacy_reference = $1`, schema), expectedLegacy).
			Scan(&contentSHA256); err != nil {
			return fmt.Errorf("verify migrated draft working copy %s: %w", expectedLegacy, err)
		}
		if contentSHA256 != skillRevisionSnapshotSHA256(expected.snapshot) {
			return fmt.Errorf("draft %s working copy does not match occurrence %s", expected.draftID, expectedLegacy)
		}
	}
	return nil
}

func (s *PostgresStore) backfillSnapshots(ctx context.Context, tx pgx.Tx, recoverUnversioned, contentOnlyVersionShape bool) error {
	schema := quoteIdentifier(s.schema)
	unversionedPredicate := ""
	if !recoverUnversioned {
		// ResolveSkill stamps created_by_subject and intentionally leaves an empty
		// container. The retained predecessor UpsertSkill path does not stamp it,
		// so a content row created after the first migration remains recoverable
		// without turning deliberate empty identities into publications.
		unversionedPredicate = " AND s.created_by_subject = ''"
	}
	initializeCurrentVersion := fmt.Sprintf(`UPDATE %s.skills s SET current_version_number = latest.version_number
		FROM (
			SELECT DISTINCT ON (skill_id) skill_id, version_number
			FROM %s.skill_versions ORDER BY skill_id, version_number DESC
		) latest
		WHERE s.id = latest.skill_id AND s.current_version_number IS NULL`, schema, schema)
	if _, err := tx.Exec(ctx, initializeCurrentVersion); err != nil {
		return fmt.Errorf("select predecessor skill snapshots: %w", err)
	}

	// A historical current-main skill_versions row persisted content but none of
	// the descriptive/provenance columns. Capture the complete mutable head before
	// those newly added columns are enriched from it; equal content cannot prove
	// that the unpublished metadata occurrence was equal to the publication.
	if err := s.captureLegacyMutableSkillHeads(ctx, tx, contentOnlyVersionShape); err != nil {
		return err
	}

	statements := []string{
		fmt.Sprintf(`INSERT INTO %s.skill_versions (
			skill_id, version_number, semver, changelog, slug, name, description,
			type, visibility, tags, content, is_ai_generated,
			generated_from_session_ids, parent_id, author_subject, published_at
		)
		SELECT id, 1, version, 'Recovered during cassette migration', slug, name,
			description, type, visibility, tags, content, is_ai_generated,
			generated_from_session_ids, parent_id, author_subject, updated_at
		FROM %s.skills s
		WHERE NOT EXISTS (SELECT 1 FROM %s.skill_versions v WHERE v.skill_id = s.id)%s
		ON CONFLICT DO NOTHING`, schema, schema, schema, unversionedPredicate),
		fmt.Sprintf(`UPDATE %s.skill_versions v SET
			slug = s.slug,
			name = s.name,
			description = s.description,
			type = s.type,
			visibility = s.visibility,
			tags = s.tags,
			is_ai_generated = s.is_ai_generated,
			generated_from_session_ids = s.generated_from_session_ids,
			parent_id = s.parent_id
		FROM %s.skills s
		WHERE v.skill_id = s.id AND v.slug = '' AND v.name = ''`, schema, schema),
		initializeCurrentVersion,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("prepare predecessor skill snapshots: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills s SET
		slug = v.slug,
		name = v.name,
		description = v.description,
		type = v.type,
		version = v.semver,
		visibility = v.visibility,
		tags = v.tags,
		content = v.content,
		is_ai_generated = v.is_ai_generated,
		generated_from_session_ids = v.generated_from_session_ids,
		parent_id = v.parent_id
		FROM %s.skill_versions v
		WHERE s.id = v.skill_id AND v.version_number = s.current_version_number`, schema, schema)); err != nil {
		return fmt.Errorf("reset predecessor skill heads after capture: %w", err)
	}
	if err := s.backfillVersionHashes(ctx, tx); err != nil {
		return err
	}
	if err := s.backfillDraftRevisionHashes(ctx, tx); err != nil {
		return err
	}
	return nil
}

func (s *PostgresStore) captureLegacyMutableSkillHeads(ctx context.Context, tx pgx.Tx, captureEqualContentOnlyHeads bool) error {
	schema := quoteIdentifier(s.schema)
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT
		s.id::text, s.current_version_number, s.slug, s.name, s.description,
		s.type, s.tags, s.content, s.is_ai_generated,
		s.generated_from_session_ids, COALESCE(s.parent_id::text, ''),
		s.author_subject, s.updated_at,
		v.slug, v.name, v.description, v.type, v.tags, v.content,
		v.is_ai_generated, v.generated_from_session_ids,
		COALESCE(v.parent_id::text, ''), v.published_at
		FROM %s.skills s
		JOIN %s.skill_versions v
		  ON v.skill_id = s.id AND v.version_number = s.current_version_number
		ORDER BY s.id`, schema, schema))
	if err != nil {
		return fmt.Errorf("query mutable predecessor skill heads: %w", err)
	}
	type capturedHead struct {
		skillID, creatorSubject string
		baseVersionNumber       int
		snapshot                predecessorSkillSnapshot
		createdAt               time.Time
	}
	captures := make([]capturedHead, 0)
	for rows.Next() {
		var capture capturedHead
		var published predecessorSkillSnapshot
		var publishedAt time.Time
		if err := rows.Scan(&capture.skillID, &capture.baseVersionNumber,
			&capture.snapshot.Slug, &capture.snapshot.Name,
			&capture.snapshot.Description, &capture.snapshot.Type,
			&capture.snapshot.Tags, &capture.snapshot.Content,
			&capture.snapshot.IsAIGenerated, &capture.snapshot.SourceSessionIDs,
			&capture.snapshot.ParentID, &capture.creatorSubject, &capture.createdAt,
			&published.Slug, &published.Name, &published.Description,
			&published.Type, &published.Tags, &published.Content,
			&published.IsAIGenerated, &published.SourceSessionIDs,
			&published.ParentID, &publishedAt); err != nil {
			rows.Close()
			return fmt.Errorf("scan mutable predecessor skill head: %w", err)
		}
		// The canonical predecessorSkillSnapshot digest covers descriptive metadata,
		// content, AI attribution, source sessions, slug, and parent lineage.
		if !captureEqualContentOnlyHeads &&
			predecessorSkillSnapshotSHA256(capture.snapshot) == predecessorSkillSnapshotSHA256(published) {
			continue
		}
		// A content-only history row cannot prove its metadata matched the head,
		// except when the head was never written after publication: every
		// predecessor publisher, backfill, and insert trigger stamped updated_at
		// with published_at, and every later head write moved it. An untouched
		// head therefore is the publication and needs no private duplicate.
		if captureEqualContentOnlyHeads && capture.snapshot.Content == published.Content &&
			capture.createdAt.Equal(publishedAt) {
			continue
		}
		capture.snapshot = canonicalPredecessorSkillSnapshot(capture.snapshot)
		capture.createdAt = capture.createdAt.UTC()
		captures = append(captures, capture)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate mutable predecessor skill heads: %w", err)
	}
	rows.Close()

	for _, capture := range captures {
		legacyReference := currentMainWorkingLegacyReference(
			capture.skillID, capture.baseVersionNumber, capture.snapshot, capture.createdAt,
		)
		if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_revision_migration_sources (
			legacy_reference, skill_id, base_skill_version_number,
			slug, name, description, type, tags, content, is_ai_generated,
			source_session_ids, parent_id, creator_subject, snapshot_sha256, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			NULLIF($12, '')::uuid, $13, $14, $15)
		ON CONFLICT (legacy_reference) DO NOTHING`, schema),
			legacyReference, capture.skillID, capture.baseVersionNumber,
			capture.snapshot.Slug, capture.snapshot.Name, capture.snapshot.Description,
			capture.snapshot.Type, nonNilStrings(capture.snapshot.Tags),
			capture.snapshot.Content, capture.snapshot.IsAIGenerated,
			nonNilStrings(capture.snapshot.SourceSessionIDs), capture.snapshot.ParentID,
			capture.creatorSubject, predecessorSkillSnapshotSHA256(capture.snapshot),
			capture.createdAt); err != nil {
			return fmt.Errorf("capture mutable predecessor skill head: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) backfillVersionHashes(ctx context.Context, tx pgx.Tx) error {
	schema := quoteIdentifier(s.schema)
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT skill_id::text, version_number,
		slug, name, description, type, tags, content, is_ai_generated,
		generated_from_session_ids, COALESCE(parent_id::text, '')
		FROM %s.skill_versions WHERE content_sha256 = ''`, schema))
	if err != nil {
		return fmt.Errorf("query version hashes: %w", err)
	}
	type hashUpdate struct {
		skillID       string
		version       int
		contentSHA256 string
	}
	updates := make([]hashUpdate, 0)
	for rows.Next() {
		var update hashUpdate
		var snapshot predecessorSkillSnapshot
		if err := rows.Scan(&update.skillID, &update.version, &snapshot.Slug, &snapshot.Name,
			&snapshot.Description, &snapshot.Type, &snapshot.Tags, &snapshot.Content,
			&snapshot.IsAIGenerated, &snapshot.SourceSessionIDs, &snapshot.ParentID); err != nil {
			rows.Close()
			return fmt.Errorf("scan version hash: %w", err)
		}
		update.contentSHA256 = predecessorSkillSnapshotSHA256(snapshot)
		updates = append(updates, update)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate version hashes: %w", err)
	}
	rows.Close()
	for _, update := range updates {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_versions SET content_sha256 = $3 WHERE skill_id = $1 AND version_number = $2`, schema), update.skillID, update.version, update.contentSHA256); err != nil {
			return fmt.Errorf("update version hash: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) backfillDraftRevisionHashes(ctx context.Context, tx pgx.Tx) error {
	schema := quoteIdentifier(s.schema)
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT draft_id::text, revision_number,
		slug, name, description, type, tags, content, is_ai_generated,
		source_session_ids, COALESCE(parent_id::text, '')
		FROM %s.draft_revisions WHERE content_sha256 = ''`, schema))
	if err != nil {
		return fmt.Errorf("query draft revision hashes: %w", err)
	}
	type hashUpdate struct {
		draftID       string
		revision      int
		contentSHA256 string
	}
	updates := make([]hashUpdate, 0)
	for rows.Next() {
		var update hashUpdate
		var snapshot predecessorSkillSnapshot
		if err := rows.Scan(&update.draftID, &update.revision, &snapshot.Slug, &snapshot.Name,
			&snapshot.Description, &snapshot.Type, &snapshot.Tags, &snapshot.Content,
			&snapshot.IsAIGenerated, &snapshot.SourceSessionIDs, &snapshot.ParentID); err != nil {
			rows.Close()
			return fmt.Errorf("scan draft revision hash: %w", err)
		}
		update.contentSHA256 = predecessorSkillSnapshotSHA256(snapshot)
		updates = append(updates, update)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate draft revision hashes: %w", err)
	}
	rows.Close()
	for _, update := range updates {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.draft_revisions SET content_sha256 = $3 WHERE draft_id = $1 AND revision_number = $2`, schema), update.draftID, update.revision, update.contentSHA256); err != nil {
			return fmt.Errorf("update draft revision hash: %w", err)
		}
	}
	return nil
}
