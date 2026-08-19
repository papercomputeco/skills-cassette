package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool   *pgxpool.Pool
	schema string
}

func OpenPostgresStore(ctx context.Context, dsn, schema string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	s := &PostgresStore{pool: pool, schema: schema}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *PostgresStore) Kind() string { return "postgres" }
func (s *PostgresStore) Close()       { s.pool.Close() }

// migrate upgrades the legacy mutable-head schema in place. Existing skills
// remain published; an unpublished head differing from the latest historical
// version is preserved as a draft.
func (s *PostgresStore) migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, s.schema+":skills-cassette-migration"); err != nil {
		return fmt.Errorf("lock migration: %w", err)
	}

	q := quoteIdentifier(s.schema)
	statements := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + q,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skills (
			id UUID PRIMARY KEY, slug TEXT NOT NULL, name TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '', type TEXT NOT NULL DEFAULT 'workflow',
			version TEXT NOT NULL DEFAULT '0.1.0', visibility TEXT NOT NULL DEFAULT 'private',
			tags TEXT[] NOT NULL DEFAULT '{}', content TEXT NOT NULL DEFAULT '',
			is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE,
			generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}', parent_id UUID,
			author_subject TEXT NOT NULL DEFAULT '', download_count BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL,
			current_version_number INT
		)`, q),
		fmt.Sprintf(`ALTER TABLE %s.skills ADD COLUMN IF NOT EXISTS current_version_number INT`, q),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skill_versions (
			skill_id UUID NOT NULL, version_number INT NOT NULL, semver TEXT NOT NULL,
			changelog TEXT NOT NULL DEFAULT '', content TEXT NOT NULL DEFAULT '',
			author_subject TEXT NOT NULL DEFAULT '', published_at TIMESTAMPTZ NOT NULL,
			slug TEXT NOT NULL DEFAULT '', name TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '',
			type TEXT NOT NULL DEFAULT 'workflow', tags TEXT[] NOT NULL DEFAULT '{}',
			is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE,
			generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}',
			PRIMARY KEY (skill_id, version_number)
		)`, q),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS slug TEXT NOT NULL DEFAULT ''`, q),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''`, q),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT ''`, q),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS type TEXT NOT NULL DEFAULT 'workflow'`, q),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}'`, q),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE`, q),
		fmt.Sprintf(`ALTER TABLE %s.skill_versions ADD COLUMN IF NOT EXISTS generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}'`, q),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.skill_drafts (
			skill_id UUID PRIMARY KEY REFERENCES %s.skills(id) ON DELETE CASCADE,
			revision INT NOT NULL, slug TEXT NOT NULL, name TEXT NOT NULL,
			description TEXT NOT NULL DEFAULT '', type TEXT NOT NULL DEFAULT 'workflow',
			tags TEXT[] NOT NULL DEFAULT '{}', content TEXT NOT NULL DEFAULT '',
			is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE,
			generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}',
			created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
		)`, q, q),
		// A prerelease build briefly stored draft lifecycle on skills.status.
		// Preserve those rows as drafts when upgrading that schema; released
		// legacy schemas have no status column and skip this block.
		fmt.Sprintf(`DO $body$ BEGIN
			IF EXISTS (SELECT 1 FROM information_schema.columns
				WHERE table_schema=%s AND table_name='skills' AND column_name='status') THEN
				EXECUTE format('INSERT INTO %%I.skill_drafts (
					skill_id,revision,slug,name,description,type,tags,content,is_ai_generated,
					generated_from_session_ids,created_at,updated_at)
					SELECT id,1,slug,name,description,type,tags,content,is_ai_generated,
					generated_from_session_ids,created_at,updated_at FROM %%I.skills
					WHERE status=''draft'' AND current_version_number IS NULL
					ON CONFLICT (skill_id) DO NOTHING', %s, %s);
			END IF;
		END $body$`, quoteLiteral(s.schema), quoteLiteral(s.schema), quoteLiteral(s.schema)),
		// Legacy versions only stored content. Current row metadata is the only
		// recoverable best-effort backfill for those historical snapshots.
		fmt.Sprintf(`UPDATE %s.skill_versions v SET
			slug=s.slug, name=s.name, description=s.description, type=s.type, tags=s.tags,
			is_ai_generated=s.is_ai_generated, generated_from_session_ids=s.generated_from_session_ids
			FROM %s.skills s WHERE v.skill_id=s.id AND v.name=''`, q, q),
		// Rows with no version were previously served as ordinary skills. Preserve
		// that behavior by synthesizing their first immutable publication.
		fmt.Sprintf(`INSERT INTO %s.skill_versions (
			skill_id, version_number, semver, slug, name, description, type, tags, content,
			is_ai_generated, generated_from_session_ids, changelog, author_subject, published_at)
			SELECT s.id, 1, s.version, s.slug, s.name, s.description, s.type, s.tags, s.content,
				s.is_ai_generated, s.generated_from_session_ids, '', s.author_subject, s.updated_at
			FROM %s.skills s
			WHERE NOT EXISTS (SELECT 1 FROM %s.skill_versions v WHERE v.skill_id=s.id)
			  AND NOT EXISTS (SELECT 1 FROM %s.skill_drafts d WHERE d.skill_id=s.id)
			ON CONFLICT (skill_id, version_number) DO NOTHING`, q, q, q, q),
		fmt.Sprintf(`UPDATE %s.skills s SET current_version_number=x.n FROM (
			SELECT skill_id, MAX(version_number) n FROM %s.skill_versions GROUP BY skill_id
		) x WHERE s.id=x.skill_id AND s.current_version_number IS NULL`, q, q),
		// Preserve a legacy working head only when its content differs from the
		// current immutable publication.
		fmt.Sprintf(`INSERT INTO %s.skill_drafts (
			skill_id, revision, slug, name, description, type, tags, content,
			is_ai_generated, generated_from_session_ids, created_at, updated_at)
			SELECT s.id, 1, s.slug, s.name, s.description, s.type, s.tags, s.content,
				s.is_ai_generated, s.generated_from_session_ids, s.updated_at, s.updated_at
			FROM %s.skills s JOIN %s.skill_versions v
			  ON v.skill_id=s.id AND v.version_number=s.current_version_number
			WHERE s.updated_at>v.published_at OR s.content<>v.content OR s.slug<>v.slug OR s.name<>v.name
			   OR s.description<>v.description OR s.type<>v.type OR s.tags<>v.tags
			   OR s.is_ai_generated<>v.is_ai_generated
			   OR s.generated_from_session_ids<>v.generated_from_session_ids
			ON CONFLICT (skill_id) DO NOTHING`, q, q, q),
		// Orphaned legacy versions were unreachable through the API and cannot
		// satisfy the new identity FK; discard them before enforcing integrity.
		fmt.Sprintf(`DELETE FROM %s.skill_versions v WHERE NOT EXISTS (
			SELECT 1 FROM %s.skills s WHERE s.id=v.skill_id)`, q, q),
		fmt.Sprintf(`DO $body$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='skill_versions_skill_fk'
				AND conrelid='%s.skill_versions'::regclass) THEN
				ALTER TABLE %s.skill_versions ADD CONSTRAINT skill_versions_skill_fk
					FOREIGN KEY (skill_id) REFERENCES %s.skills(id) ON DELETE CASCADE;
			END IF;
		END $body$`, q, q, q),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skills_updated_idx ON %s.skills (updated_at DESC, id DESC)`, q),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_drafts_updated_idx ON %s.skill_drafts (updated_at DESC, skill_id DESC)`, q),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS skill_versions_skill_idx ON %s.skill_versions (skill_id, version_number DESC)`, q),
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("migrating skills tables: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}

func validUUID(id string) bool { _, err := uuid.Parse(id); return err == nil }

func (s *PostgresStore) CreateDraft(ctx context.Context, identity SkillRecord, draft SkillDraftRecord) (*SkillDraftRecord, error) {
	if !validUUID(identity.ID) {
		return nil, errors.New("create draft: valid skill id required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := quoteIdentifier(s.schema)
	visibility := defaultString(identity.Visibility, "private")
	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skills (
		id, slug, name, description, type, version, visibility, tags, content,
		is_ai_generated, generated_from_session_ids, parent_id, author_subject,
		created_at, updated_at, current_version_number)
		VALUES ($1,$2,$3,$4,$5,'0.1.0',$6,$7,$8,$9,$10,NULLIF($11,'')::uuid,$12,$13,$14,NULL)`, q),
		identity.ID, draft.Slug, draft.Name, draft.Description, draft.Type, visibility,
		nonNilStrings(draft.Tags), draft.Content, draft.IsAIGenerated,
		nonNilStrings(draft.GeneratedFromSessionIDs), identity.ParentID, identity.AuthorSubject,
		identity.CreatedAt, identity.UpdatedAt)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return nil, ErrSkillExists
		}
		return nil, fmt.Errorf("create skill identity: %w", err)
	}
	draft.SkillID, draft.Revision = identity.ID, 1
	row := tx.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %s.skill_drafts (
		skill_id, revision, slug, name, description, type, tags, content,
		is_ai_generated, generated_from_session_ids, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING %s`, q, draftColumns),
		draft.SkillID, draft.Revision, draft.Slug, draft.Name, draft.Description, draft.Type,
		nonNilStrings(draft.Tags), draft.Content, draft.IsAIGenerated,
		nonNilStrings(draft.GeneratedFromSessionIDs), draft.CreatedAt, draft.UpdatedAt)
	out, err := scanDraft(row)
	if err != nil {
		return nil, fmt.Errorf("create skill draft: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *PostgresStore) CreateDraftFromPublished(ctx context.Context, skillID string, now time.Time) (*SkillDraftRecord, error) {
	if !validUUID(skillID) {
		return nil, ErrDraftNotFound
	}
	q := quoteIdentifier(s.schema)
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %s.skill_drafts (
		skill_id, revision, slug, name, description, type, tags, content,
		is_ai_generated, generated_from_session_ids, created_at, updated_at)
		SELECT s.id,1,v.slug,v.name,v.description,v.type,v.tags,v.content,
			v.is_ai_generated,v.generated_from_session_ids,$2,$2
		FROM %s.skills s JOIN %s.skill_versions v
		  ON v.skill_id=s.id AND v.version_number=s.current_version_number
		WHERE s.id=$1 RETURNING %s`, q, q, q, draftColumns), skillID, now)
	out, err := scanDraft(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDraftNotFound
	}
	if err != nil && strings.Contains(err.Error(), "duplicate key") {
		return nil, ErrDraftExists
	}
	if err != nil {
		return nil, fmt.Errorf("create draft from published: %w", err)
	}
	return &out, nil
}

const draftColumns = `skill_id::text, revision, slug, name, description, type, tags, content,
	is_ai_generated, generated_from_session_ids, created_at, updated_at`

func (s *PostgresStore) GetDraft(ctx context.Context, skillID string) (*SkillDraftRecord, error) {
	if !validUUID(skillID) {
		return nil, nil
	}
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM %s.skill_drafts WHERE skill_id=$1`, draftColumns, quoteIdentifier(s.schema)), skillID)
	out, err := scanDraft(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get draft: %w", err)
	}
	return &out, nil
}

func (s *PostgresStore) ListDrafts(ctx context.Context) ([]SkillDraftRecord, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s.skill_drafts ORDER BY updated_at DESC, skill_id DESC`, draftColumns, quoteIdentifier(s.schema)))
	if err != nil {
		return nil, fmt.Errorf("list drafts: %w", err)
	}
	defer rows.Close()
	out := []SkillDraftRecord{}
	for rows.Next() {
		d, err := scanDraft(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *PostgresStore) UpdateDraft(ctx context.Context, draft SkillDraftRecord, expectedRevision int) (*SkillDraftRecord, error) {
	q := quoteIdentifier(s.schema)
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`UPDATE %s.skill_drafts SET
		revision=revision+1, slug=$3, name=$4, description=$5, type=$6, tags=$7,
		content=$8, is_ai_generated=$9, updated_at=$10
		WHERE skill_id=$1 AND revision=$2 RETURNING %s`, q, draftColumns),
		draft.SkillID, expectedRevision, draft.Slug, draft.Name, draft.Description, draft.Type,
		nonNilStrings(draft.Tags), draft.Content, draft.IsAIGenerated, draft.UpdatedAt)
	out, err := scanDraft(row)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, getErr := s.GetDraft(ctx, draft.SkillID)
		if getErr != nil {
			return nil, getErr
		}
		if existing == nil {
			return nil, ErrDraftNotFound
		}
		return nil, ErrDraftRevisionConflict
	}
	if err != nil {
		return nil, fmt.Errorf("update draft: %w", err)
	}
	return &out, nil
}

func (s *PostgresStore) PublishDraft(ctx context.Context, skillID string, expectedRevision int, changelog, author string, publishedAt time.Time) (*SkillVersionRecord, error) {
	if !validUUID(skillID) {
		return nil, ErrDraftNotFound
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := quoteIdentifier(s.schema)
	draft, err := scanDraft(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM %s.skill_drafts WHERE skill_id=$1 FOR UPDATE`, draftColumns, q), skillID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDraftNotFound
	}
	if err != nil {
		return nil, err
	}
	if draft.Revision != expectedRevision {
		return nil, ErrDraftRevisionConflict
	}
	var current int
	if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(current_version_number,0) FROM %s.skills WHERE id=$1 FOR UPDATE`, q), skillID).Scan(&current); err != nil {
		return nil, ErrDraftNotFound
	}
	number := current + 1
	semver := fmt.Sprintf("0.1.%d", number-1)
	row := tx.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %s.skill_versions (
		skill_id,version_number,semver,slug,name,description,type,tags,content,
		is_ai_generated,generated_from_session_ids,changelog,author_subject,published_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING %s`, q, versionColumns),
		skillID, number, semver, draft.Slug, draft.Name, draft.Description, draft.Type,
		nonNilStrings(draft.Tags), draft.Content, draft.IsAIGenerated,
		nonNilStrings(draft.GeneratedFromSessionIDs), changelog, author, publishedAt)
	version, err := scanVersion(row)
	if err != nil {
		return nil, fmt.Errorf("insert version: %w", err)
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills SET
		current_version_number=$2, visibility='team', updated_at=$3,
		slug=$4,name=$5,description=$6,type=$7,version=$8,tags=$9,content=$10,
		is_ai_generated=$11,generated_from_session_ids=$12 WHERE id=$1`, q),
		skillID, number, publishedAt, draft.Slug, draft.Name, draft.Description, draft.Type,
		semver, nonNilStrings(draft.Tags), draft.Content, draft.IsAIGenerated,
		nonNilStrings(draft.GeneratedFromSessionIDs))
	if err != nil {
		return nil, fmt.Errorf("advance published version: %w", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.skill_drafts WHERE skill_id=$1 AND revision=$2`, q), skillID, expectedRevision); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &version, nil
}

const publishedColumns = `s.id::text, COALESCE(s.parent_id::text,''), s.author_subject, s.visibility,
	s.download_count, s.current_version_number, s.created_at, s.updated_at,
	v.slug,v.name,v.description,v.type,v.semver,v.tags,v.content,v.is_ai_generated,v.generated_from_session_ids`

func (s *PostgresStore) GetSkillIdentity(ctx context.Context, id string) (*SkillRecord, error) {
	if !validUUID(id) {
		return nil, nil
	}
	var out SkillRecord
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT id::text,COALESCE(parent_id::text,''),author_subject,visibility,
		download_count,COALESCE(current_version_number,0),created_at,updated_at FROM %s.skills WHERE id=$1`, quoteIdentifier(s.schema)), id).Scan(
		&out.ID, &out.ParentID, &out.AuthorSubject, &out.Visibility, &out.DownloadCount, &out.CurrentVersionNumber, &out.CreatedAt, &out.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *PostgresStore) GetSkill(ctx context.Context, id string) (*SkillRecord, error) {
	if !validUUID(id) {
		return nil, nil
	}
	q := quoteIdentifier(s.schema)
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM %s.skills s JOIN %s.skill_versions v
		ON v.skill_id=s.id AND v.version_number=s.current_version_number WHERE s.id=$1`, publishedColumns, q, q), id)
	out, err := scanPublished(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get skill: %w", err)
	}
	return &out, nil
}

func (s *PostgresStore) DeleteSkill(ctx context.Context, id string) (bool, error) {
	if !validUUID(id) {
		return false, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := quoteIdentifier(s.schema)
	if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.skill_versions WHERE skill_id=$1`, q), id); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.skill_drafts WHERE skill_id=$1`, q), id); err != nil {
		return false, err
	}
	result, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.skills WHERE id=$1`, q), id)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func (s *PostgresStore) ListSkills(ctx context.Context, opts SkillListOpts) ([]SkillRecord, error) {
	q := quoteIdentifier(s.schema)
	where := []string{"s.current_version_number IS NOT NULL"}
	args := []any{}
	add := func(expr string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(expr, len(args)))
	}
	if opts.Query != "" {
		args = append(args, opts.Query)
		n := len(args)
		where = append(where, fmt.Sprintf(`(strpos(lower(v.name),lower($%d))>0 OR strpos(lower(v.description),lower($%d))>0 OR EXISTS (SELECT 1 FROM unnest(v.tags) t WHERE strpos(lower(t),lower($%d))>0))`, n, n, n))
	}
	if opts.Author != "" {
		add(`s.author_subject=$%d`, opts.Author)
	}
	if opts.NotAuthor != "" {
		add(`s.author_subject<>$%d`, opts.NotAuthor)
	}
	byDownloads := opts.Sort == SkillSortDownloads
	if opts.CursorID != "" && byDownloads && opts.CursorDownloads != nil {
		args = append(args, *opts.CursorDownloads, opts.CursorID)
		where = append(where, fmt.Sprintf(`(s.download_count,s.id)<($%d,$%d::uuid)`, len(args)-1, len(args)))
	} else if opts.CursorID != "" && opts.CursorTs != nil {
		args = append(args, *opts.CursorTs, opts.CursorID)
		where = append(where, fmt.Sprintf(`(s.updated_at,s.id)<($%d,$%d::uuid)`, len(args)-1, len(args)))
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	args = append(args, limit)
	order := "s.updated_at DESC,s.id DESC"
	if byDownloads {
		order = "s.download_count DESC,s.id DESC"
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s.skills s JOIN %s.skill_versions v
		ON v.skill_id=s.id AND v.version_number=s.current_version_number WHERE %s ORDER BY %s LIMIT $%d`, publishedColumns, q, q, strings.Join(where, " AND "), order, len(args)), args...)
	if err != nil {
		return nil, fmt.Errorf("list skills: %w", err)
	}
	defer rows.Close()
	return collectPublished(rows)
}

func (s *PostgresStore) ListSkillsBySession(ctx context.Context, sessionID string) ([]SkillRecord, error) {
	q := quoteIdentifier(s.schema)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s.skills s JOIN %s.skill_versions v
		ON v.skill_id=s.id AND v.version_number=s.current_version_number
		WHERE $1=ANY(v.generated_from_session_ids) ORDER BY s.updated_at DESC,s.id DESC`, publishedColumns, q, q), sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectPublished(rows)
}

func (s *PostgresStore) CountSkills(ctx context.Context, query, author string) (SkillCounts, error) {
	q := quoteIdentifier(s.schema)
	var out SkillCounts
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*),COUNT(*) FILTER (WHERE s.author_subject=$2)
		FROM %s.skills s JOIN %s.skill_versions v ON v.skill_id=s.id AND v.version_number=s.current_version_number
		WHERE ($1='' OR strpos(lower(v.name),lower($1))>0 OR strpos(lower(v.description),lower($1))>0
			OR EXISTS (SELECT 1 FROM unnest(v.tags) t WHERE strpos(lower(t),lower($1))>0))`, q, q), query, author).Scan(&out.Total, &out.Mine)
	return out, err
}

const versionColumns = `skill_id::text,version_number,semver,slug,name,description,type,tags,content,
	is_ai_generated,generated_from_session_ids,changelog,author_subject,published_at`

func (s *PostgresStore) ListSkillVersions(ctx context.Context, skillID string) ([]SkillVersionRecord, error) {
	if !validUUID(skillID) {
		return []SkillVersionRecord{}, nil
	}
	q := quoteIdentifier(s.schema)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s.skill_versions WHERE skill_id=$1 ORDER BY version_number DESC`, versionColumns, q), skillID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SkillVersionRecord{}
	for rows.Next() {
		v, e := scanVersion(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *PostgresStore) IncrementSkillDownloads(ctx context.Context, id string) error {
	if !validUUID(id) {
		return nil
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills SET download_count=download_count+1 WHERE id=$1 AND current_version_number IS NOT NULL`, quoteIdentifier(s.schema)), id)
	return err
}

func scanDraft(row pgx.Row) (SkillDraftRecord, error) {
	var d SkillDraftRecord
	err := row.Scan(&d.SkillID, &d.Revision, &d.Slug, &d.Name, &d.Description, &d.Type, &d.Tags, &d.Content, &d.IsAIGenerated, &d.GeneratedFromSessionIDs, &d.CreatedAt, &d.UpdatedAt)
	return d, err
}
func scanVersion(row pgx.Row) (SkillVersionRecord, error) {
	var v SkillVersionRecord
	err := row.Scan(&v.SkillID, &v.VersionNumber, &v.Semver, &v.Slug, &v.Name, &v.Description, &v.Type, &v.Tags, &v.Content, &v.IsAIGenerated, &v.GeneratedFromSessionIDs, &v.Changelog, &v.AuthorSubject, &v.PublishedAt)
	return v, err
}
func scanPublished(row pgx.Row) (SkillRecord, error) {
	var r SkillRecord
	err := row.Scan(&r.ID, &r.ParentID, &r.AuthorSubject, &r.Visibility, &r.DownloadCount, &r.CurrentVersionNumber, &r.CreatedAt, &r.UpdatedAt, &r.Slug, &r.Name, &r.Description, &r.Type, &r.Version, &r.Tags, &r.Content, &r.IsAIGenerated, &r.GeneratedFromSessionIDs)
	return r, err
}
func collectPublished(rows pgx.Rows) ([]SkillRecord, error) {
	out := []SkillRecord{}
	for rows.Next() {
		r, e := scanPublished(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func nonNilStrings(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}
func quoteIdentifier(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }
func quoteLiteral(value string) string    { return `'` + strings.ReplaceAll(value, `'`, `''`) + `'` }
