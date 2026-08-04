package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgUniqueViolation is the Postgres SQLSTATE for a unique-constraint breach
// (23505), used to turn a duplicate skill-version insert into a typed conflict.
const pgUniqueViolation = "23505"

// PostgresStore owns the cassette's two tables in the cassette's own schema
// and creates both itself. The deployment provisions the role, credential,
// and grants; what goes inside the schema is the cassette's business and
// core never sees the DDL.
type PostgresStore struct {
	pool   *pgxpool.Pool
	schema string
}

var _ Store = (*PostgresStore)(nil)

// OpenPostgresStore connects to dsn, runs the cassette-owned migrations in
// the schema named after the cassette, and returns the store.
func OpenPostgresStore(ctx context.Context, dsn, schema string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	s := &PostgresStore{pool: pool, schema: schema}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Kind names the backing store.
func (s *PostgresStore) Kind() string { return "postgres" }

// Close releases the connection pool.
func (s *PostgresStore) Close() { s.pool.Close() }

// migrate is the cassette's own migration, run at startup. This is the final
// two-table form of the four historical Tapes migration steps (skills,
// skill_versions + author_subject, download_count, id keys), re-homed without
// org_id and without core foreign keys. Identifiers are quoted because a
// cassette name may legally contain a hyphen.
func (s *PostgresStore) migrate(ctx context.Context) error {
	schema := quoteIdentifier(s.schema)
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
			created_at                 TIMESTAMPTZ NOT NULL,
			updated_at                 TIMESTAMPTZ NOT NULL,

			CONSTRAINT skills_pkey PRIMARY KEY (id)
		)`, schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skills_updated_idx
			ON %s.skills (updated_at DESC, id DESC)`, schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skill_versions (
			skill_id       UUID NOT NULL,
			version_number INT  NOT NULL,
			semver         TEXT NOT NULL,
			changelog      TEXT NOT NULL DEFAULT '',
			content        TEXT NOT NULL DEFAULT '',
			author_subject TEXT NOT NULL DEFAULT '',
			published_at   TIMESTAMPTZ NOT NULL,

			CONSTRAINT skill_versions_pkey PRIMARY KEY (skill_id, version_number)
		)`, schema),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_versions_skill_idx
			ON %s.skill_versions (skill_id, version_number DESC)`, schema),
	}
	for _, statement := range statements {
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("migrating skills tables: %w", err)
		}
	}
	return nil
}

// skillColumns is the SELECT list every skill read shares. UUIDs are projected
// as text (parent_id coalesced to ”) so rows scan into plain strings.
const skillColumns = `id::text, slug, name, description, type, version, visibility, tags, content,
	is_ai_generated, generated_from_session_ids, COALESCE(parent_id::text, ''), author_subject,
	download_count, created_at, updated_at`

// validUUID reports whether id parses as a UUID. A malformed or empty id is
// simply "not found" from the caller's view, mirroring the pre-cutover driver.
func validUUID(id string) bool {
	_, err := uuid.Parse(id)
	return err == nil
}

// UpsertSkill inserts or replaces a skill keyed by id and returns the
// persisted record. Create/generate/duplicate pass a freshly minted id (a
// plain insert); PUT/publish pass the existing id (an update). created_at,
// author_subject, and download_count are preserved on conflict.
func (s *PostgresStore) UpsertSkill(ctx context.Context, rec SkillRecord) (*SkillRecord, error) {
	if !validUUID(rec.ID) {
		return nil, errors.New("upsert skill: id is required")
	}
	if rec.ParentID != "" && !validUUID(rec.ParentID) {
		return nil, fmt.Errorf("upsert skill: invalid parent id %q", rec.ParentID)
	}
	query := fmt.Sprintf(`INSERT INTO %s.skills (
			id, slug, name, description, type, version, visibility, tags, content,
			is_ai_generated, generated_from_session_ids, parent_id, author_subject,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, '')::uuid, $13, $14, $15)
		ON CONFLICT (id) DO UPDATE
		SET slug                       = EXCLUDED.slug,
		    name                       = EXCLUDED.name,
		    description                = EXCLUDED.description,
		    type                       = EXCLUDED.type,
		    version                    = EXCLUDED.version,
		    visibility                 = EXCLUDED.visibility,
		    tags                       = EXCLUDED.tags,
		    content                    = EXCLUDED.content,
		    is_ai_generated            = EXCLUDED.is_ai_generated,
		    generated_from_session_ids = EXCLUDED.generated_from_session_ids,
		    parent_id                  = EXCLUDED.parent_id,
		    updated_at                 = EXCLUDED.updated_at
		RETURNING `+skillColumns, quoteIdentifier(s.schema))
	row := s.pool.QueryRow(ctx, query,
		rec.ID, rec.Slug, rec.Name, rec.Description, rec.Type, rec.Version, rec.Visibility,
		nonNilStrings(rec.Tags), rec.Content, rec.IsAIGenerated,
		nonNilStrings(rec.GeneratedFromSessionIDs), rec.ParentID, rec.AuthorSubject,
		rec.CreatedAt, rec.UpdatedAt)
	out, err := scanSkill(row)
	if err != nil {
		return nil, fmt.Errorf("upsert skill: %w", err)
	}
	return &out, nil
}

// GetSkill returns a single skill by id, or nil if not found.
func (s *PostgresStore) GetSkill(ctx context.Context, id string) (*SkillRecord, error) {
	if !validUUID(id) {
		return nil, nil
	}
	query := fmt.Sprintf(`SELECT %s FROM %s.skills WHERE id = $1`,
		skillColumns, quoteIdentifier(s.schema))
	out, err := scanSkill(s.pool.QueryRow(ctx, query, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get skill: %w", err)
	}
	return &out, nil
}

// DeleteSkill removes a skill and its published history by id in one
// transaction: skill_versions has no FK cascade to skills, so two separate
// statements could destroy version history and then fail to remove the skill.
func (s *PostgresStore) DeleteSkill(ctx context.Context, id string) (bool, error) {
	if !validUUID(id) {
		return false, nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin delete skill tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	schema := quoteIdentifier(s.schema)
	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s.skill_versions WHERE skill_id = $1`, schema), id); err != nil {
		return false, fmt.Errorf("delete skill versions: %w", err)
	}
	tag, err := tx.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s.skills WHERE id = $1`, schema), id)
	if err != nil {
		return false, fmt.Errorf("delete skill: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit delete skill tx: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// searchPredicate mirrors the pre-cutover ILIKE search over name, description,
// and tags. $1 is the nullable query text in every list/count statement.
const searchPredicate = `($1::text IS NULL
	OR name ILIKE '%' || $1::text || '%'
	OR description ILIKE '%' || $1::text || '%'
	OR EXISTS (SELECT 1 FROM unnest(tags) tag WHERE tag ILIKE '%' || $1::text || '%'))`

// ListSkills returns one keyset page honoring the optional search/scope
// filters, the requested sort, and the cursor in opts.
func (s *PostgresStore) ListSkills(ctx context.Context, opts SkillListOpts) ([]SkillRecord, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	schema := quoteIdentifier(s.schema)

	selectHead := fmt.Sprintf(`SELECT %s FROM %s.skills WHERE `, skillColumns, schema)

	var rows pgx.Rows
	var err error
	if opts.Sort == SkillSortDownloads {
		query := selectHead + searchPredicate + `
			  AND ($2::text IS NULL OR author_subject = $2::text)
			  AND ($3::text IS NULL OR author_subject <> $3::text)
			  AND (
			    $4::bigint IS NULL
			    OR download_count < $4::bigint
			    OR (download_count = $4::bigint AND id < $5::uuid)
			  )
			ORDER BY download_count DESC, id DESC
			LIMIT $6`
		rows, err = s.pool.Query(ctx, query,
			nullText(opts.Query), nullText(opts.Author), nullText(opts.NotAuthor),
			opts.CursorDownloads, nullText(opts.CursorID), limit)
	} else {
		query := selectHead + searchPredicate + `
			  AND ($2::text IS NULL OR author_subject = $2::text)
			  AND ($3::text IS NULL OR author_subject <> $3::text)
			  AND (
			    $4::timestamptz IS NULL
			    OR updated_at < $4::timestamptz
			    OR (updated_at = $4::timestamptz AND id < $5::uuid)
			  )
			ORDER BY updated_at DESC, id DESC
			LIMIT $6`
		rows, err = s.pool.Query(ctx, query,
			nullText(opts.Query), nullText(opts.Author), nullText(opts.NotAuthor),
			opts.CursorTs, nullText(opts.CursorID), limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list skills: %w", err)
	}
	defer rows.Close()
	return collectSkills(rows)
}

// ListSkillsBySession returns the skills generated from a given session
// (reverse lookup over the provenance array), newest-edited first.
func (s *PostgresStore) ListSkillsBySession(ctx context.Context, sessionID string) ([]SkillRecord, error) {
	query := fmt.Sprintf(`SELECT %s FROM %s.skills
		WHERE $1::text = ANY(generated_from_session_ids)
		ORDER BY updated_at DESC, id DESC`, skillColumns, quoteIdentifier(s.schema))
	rows, err := s.pool.Query(ctx, query, sessionID)
	if err != nil {
		return nil, fmt.Errorf("list session skills: %w", err)
	}
	defer rows.Close()
	return collectSkills(rows)
}

// CountSkills returns the per-tab totals for a search (ignoring scope and
// cursor): every matching skill and how many the caller authored.
func (s *PostgresStore) CountSkills(ctx context.Context, query, author string) (SkillCounts, error) {
	statement := fmt.Sprintf(`SELECT
			COUNT(*)::bigint AS total,
			COUNT(*) FILTER (WHERE author_subject = $2)::bigint AS mine
		FROM %s.skills
		WHERE `, quoteIdentifier(s.schema)) + searchPredicate
	var counts SkillCounts
	if err := s.pool.QueryRow(ctx, statement, nullText(query), author).
		Scan(&counts.Total, &counts.Mine); err != nil {
		return SkillCounts{}, fmt.Errorf("count skills: %w", err)
	}
	return counts, nil
}

// NextSkillVersionNumber returns the next monotonic version number for a
// skill (1 when nothing is published yet).
func (s *PostgresStore) NextSkillVersionNumber(ctx context.Context, skillID string) (int, error) {
	if !validUUID(skillID) {
		return 0, fmt.Errorf("next skill version: invalid id %q", skillID)
	}
	query := fmt.Sprintf(`SELECT COALESCE(MAX(version_number), 0)::int
		FROM %s.skill_versions WHERE skill_id = $1`, quoteIdentifier(s.schema))
	var maxN int
	if err := s.pool.QueryRow(ctx, query, skillID).Scan(&maxN); err != nil {
		return 0, fmt.Errorf("next skill version: %w", err)
	}
	return maxN + 1, nil
}

// PublishSkillVersion appends an immutable published snapshot and advances
// the skill head (version, content, updated_at) in one transaction. The head
// update is guarded so it only lands while this is the highest published
// number: an older overlapping publish that commits last inserts its history
// row but leaves the newer head alone. Under read committed, the loser's
// guard re-evaluates against the winner's committed row after any lock wait,
// so the ordering holds without a stricter isolation level.
func (s *PostgresStore) PublishSkillVersion(ctx context.Context, rec SkillVersionRecord) (*SkillVersionRecord, error) {
	if !validUUID(rec.SkillID) {
		return nil, fmt.Errorf("publish skill version: invalid id %q", rec.SkillID)
	}
	schema := quoteIdentifier(s.schema)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin publish skill tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	insert := fmt.Sprintf(`INSERT INTO %s.skill_versions (
			skill_id, version_number, semver, changelog, content, author_subject, published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING skill_id::text, version_number, semver, changelog, content, author_subject, published_at`,
		schema)
	var out SkillVersionRecord
	err = tx.QueryRow(ctx, insert,
		rec.SkillID, rec.VersionNumber, rec.Semver, rec.Changelog, rec.Content,
		rec.AuthorSubject, rec.PublishedAt).
		Scan(&out.SkillID, &out.VersionNumber, &out.Semver, &out.Changelog,
			&out.Content, &out.AuthorSubject, &out.PublishedAt)
	if err != nil {
		// A concurrent publish already claimed this version number; surface a
		// typed conflict so the handler can recompute and retry instead of 500.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return nil, ErrSkillVersionConflict
		}
		return nil, fmt.Errorf("publish skill version: %w", err)
	}

	bump := fmt.Sprintf(`UPDATE %s.skills
		SET version = $1, content = $2, updated_at = $3
		WHERE id = $4
		  AND NOT EXISTS (
		    SELECT 1 FROM %s.skill_versions
		    WHERE skill_id = $4 AND version_number > $5
		  )`, schema, schema)
	if _, err := tx.Exec(ctx, bump,
		rec.Semver, rec.Content, rec.PublishedAt, rec.SkillID, rec.VersionNumber); err != nil {
		return nil, fmt.Errorf("advance skill head: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit publish skill tx: %w", err)
	}
	return &out, nil
}

// ListSkillVersions returns a skill's published history, newest first.
func (s *PostgresStore) ListSkillVersions(ctx context.Context, skillID string) ([]SkillVersionRecord, error) {
	if !validUUID(skillID) {
		return []SkillVersionRecord{}, nil
	}
	query := fmt.Sprintf(`SELECT skill_id::text, version_number, semver, changelog, content,
			author_subject, published_at
		FROM %s.skill_versions WHERE skill_id = $1
		ORDER BY version_number DESC`, quoteIdentifier(s.schema))
	rows, err := s.pool.Query(ctx, query, skillID)
	if err != nil {
		return nil, fmt.Errorf("list skill versions: %w", err)
	}
	defer rows.Close()

	out := make([]SkillVersionRecord, 0)
	for rows.Next() {
		var rec SkillVersionRecord
		if err := rows.Scan(&rec.SkillID, &rec.VersionNumber, &rec.Semver, &rec.Changelog,
			&rec.Content, &rec.AuthorSubject, &rec.PublishedAt); err != nil {
			return nil, fmt.Errorf("list skill versions: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// IncrementSkillDownloads bumps the real download counter for a skill.
func (s *PostgresStore) IncrementSkillDownloads(ctx context.Context, id string) error {
	if !validUUID(id) {
		return nil
	}
	query := fmt.Sprintf(`UPDATE %s.skills SET download_count = download_count + 1 WHERE id = $1`,
		quoteIdentifier(s.schema))
	if _, err := s.pool.Exec(ctx, query, id); err != nil {
		return fmt.Errorf("increment skill downloads: %w", err)
	}
	return nil
}

func scanSkill(row pgx.Row) (SkillRecord, error) {
	var rec SkillRecord
	err := row.Scan(&rec.ID, &rec.Slug, &rec.Name, &rec.Description, &rec.Type, &rec.Version,
		&rec.Visibility, &rec.Tags, &rec.Content, &rec.IsAIGenerated,
		&rec.GeneratedFromSessionIDs, &rec.ParentID, &rec.AuthorSubject,
		&rec.DownloadCount, &rec.CreatedAt, &rec.UpdatedAt)
	return rec, err
}

func collectSkills(rows pgx.Rows) ([]SkillRecord, error) {
	out := make([]SkillRecord, 0)
	for rows.Next() {
		rec, err := scanSkill(rows)
		if err != nil {
			return nil, fmt.Errorf("scan skill: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// nullText maps the empty string to SQL NULL so the optional query/scope
// predicates disable, mirroring the pre-cutover pgtype.Text behavior.
func nullText(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// nonNilStrings returns a non-nil empty slice for nil input. The tags and
// generated_from_session_ids columns are NOT NULL, and an explicit INSERT
// supplying nil would write NULL (the column DEFAULT only applies when the
// column is omitted), so guard against it here.
func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

// quoteIdentifier renders a SQL identifier safely. Cassette names are already
// validated against a strict pattern upstream, so this is belt and braces —
// but a schema name reaching SQL unquoted is exactly the kind of thing that is
// fine until the day it is not.
func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
