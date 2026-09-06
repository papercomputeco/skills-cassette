package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
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

type postgresQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

var (
	_ Store                 = (*PostgresStore)(nil)
	_ SkillIdentityStore    = (*PostgresStore)(nil)
	_ SkillReader           = (*PostgresStore)(nil)
	_ RevisionStore         = (*PostgresStore)(nil)
	_ RevisionMetadataStore = (*PostgresStore)(nil)
)

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}

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

// ResolveSkill normalizes a slug and atomically resolves it to one stable
// identity. The conflict branch projects identity columns only: resolving a
// slug owned by another creator cannot materialize that creator's revisions.
func (s *PostgresStore) ResolveSkill(ctx context.Context, input ResolveSkillInput) (*SkillRecord, error) {
	if !validUUID(input.ID) {
		return nil, errors.New("resolve skill: id is required")
	}
	slug := normalizeSkillSlug(input.Slug)
	if slug == "" {
		return nil, errors.New("resolve skill: slug is required")
	}
	if input.CreatorSubject == "" {
		return nil, errors.New("resolve skill: creator subject is required")
	}

	schema := quoteIdentifier(s.schema)
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %s.skills (
			id, slug, name, author_subject, created_by_subject, created_at, updated_at
		) VALUES ($1, $2, '', $3, $3, $4, $4)
		ON CONFLICT (slug) WHERE migration_alias_of_skill_id IS NULL
		DO UPDATE SET slug = EXCLUDED.slug
		RETURNING id::text, slug, COALESCE(explicit_latest_revision_id::text, ''),
			next_sequence_number,
			CASE WHEN created_by_subject = $3 THEN created_by_subject ELSE '' END,
			created_at, updated_at`, schema),
		input.ID, slug, input.CreatorSubject, input.CreatedAt.UTC())

	var record SkillRecord
	if err := row.Scan(&record.ID, &record.Slug, &record.ExplicitLatestRevisionID,
		&record.NextSequenceNumber, &record.CreatedBySubject, &record.CreatedAt,
		&record.UpdatedAt); err != nil {
		return nil, fmt.Errorf("resolve skill: %w", err)
	}
	return &record, nil
}

// AppendRevision allocates the next per-skill sequence and inserts one
// complete immutable snapshot plus its creator-private visibility row in the
// same short transaction. The skill-row lock also serializes idempotency-key
// checks, so a retry never consumes another sequence.
func (s *PostgresStore) AppendRevision(ctx context.Context, input AppendRevisionInput) (*SkillRevisionRecord, error) {
	if !validUUID(input.ID) {
		return nil, errors.New("append revision: id is required")
	}
	if !validUUID(input.SkillID) {
		return nil, ErrSkillNotFound
	}
	if input.CreatorSubject == "" {
		return nil, errors.New("append revision: creator subject is required")
	}
	if input.BasedOnRevisionID != "" && !validUUID(input.BasedOnRevisionID) {
		return nil, ErrRevisionLineageInvalid
	}
	if input.SourceRevisionID != "" && !validUUID(input.SourceRevisionID) {
		return nil, ErrRevisionLineageInvalid
	}
	if input.GenerationID != "" && !validUUID(input.GenerationID) {
		return nil, ErrRevisionLineageInvalid
	}
	switch input.Origin {
	case RevisionOriginManual, RevisionOriginGeneration, RevisionOriginDuplicate, RevisionOriginMigrated:
	default:
		return nil, errors.New("append revision: invalid origin")
	}
	var err error
	input.Snapshot, err = normalizeAppendRevisionSnapshot(input.Origin, input.Snapshot)
	if err != nil {
		return nil, fmt.Errorf("append revision: %w", err)
	}
	createdAt := input.CreatedAt.UTC()
	schema := quoteIdentifier(s.schema)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin append revision: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var lockedSkillID string
	if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT id::text FROM %s.skills
		WHERE id = $1 AND migration_alias_of_skill_id IS NULL FOR UPDATE`, schema), input.SkillID).Scan(&lockedSkillID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSkillNotFound
		}
		return nil, fmt.Errorf("lock skill for revision append: %w", err)
	}

	existing, err := findAppendIdentity(ctx, tx, schema, input)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if !revisionMatchesAppend(*existing, input) {
			return nil, ErrRevisionConflict
		}
		return existing, nil
	}

	if input.BasedOnRevisionID != "" {
		exists, lineageErr := revisionIsAccessibleLineage(ctx, tx, schema,
			input.BasedOnRevisionID, input.SkillID, input.CreatorSubject, true)
		if lineageErr != nil {
			return nil, lineageErr
		}
		if !exists {
			return nil, ErrRevisionLineageInvalid
		}
	}
	if input.SourceRevisionID != "" {
		exists, lineageErr := revisionIsAccessibleLineage(ctx, tx, schema,
			input.SourceRevisionID, input.SkillID, input.CreatorSubject, false)
		if lineageErr != nil {
			return nil, lineageErr
		}
		if !exists {
			return nil, ErrRevisionLineageInvalid
		}
	}

	var sequence int
	if err := tx.QueryRow(ctx, fmt.Sprintf(`UPDATE %s.skills
		SET next_sequence_number = next_sequence_number + 1, updated_at = GREATEST(updated_at, $2)
		WHERE id = $1 RETURNING next_sequence_number - 1`, schema), input.SkillID, createdAt).Scan(&sequence); err != nil {
		return nil, fmt.Errorf("allocate revision sequence: %w", err)
	}

	record := SkillRevisionRecord{
		ID: input.ID, SkillID: input.SkillID, SequenceNumber: sequence,
		Version: fmt.Sprintf("%d", sequence), CreatorSubject: input.CreatorSubject,
		BasedOnRevisionID: input.BasedOnRevisionID, SourceRevisionID: input.SourceRevisionID,
		Origin: input.Origin, Snapshot: input.Snapshot,
		ContentSHA256: skillRevisionSnapshotSHA256(input.Snapshot), ChangeNote: input.ChangeNote,
		GenerationID: input.GenerationID, IdempotencyKey: input.IdempotencyKey,
		LegacyReference: input.LegacyReference, CreatedAt: createdAt,
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_revisions (
			id, skill_id, sequence_number, version, creator_subject,
			based_on_revision_id, source_revision_id, origin, name, description,
			type, tags, content, is_ai_generated, source_session_ids,
			content_sha256, change_note, generation_id, idempotency_key,
			legacy_reference, created_at
		) VALUES (
			$1, $2, $3, $4, $5, NULLIF($6, '')::uuid, NULLIF($7, '')::uuid,
			$8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
			NULLIF($18, '')::uuid, $19, NULLIF($20, ''), $21
		)`, schema), record.ID, record.SkillID, record.SequenceNumber, record.Version,
		record.CreatorSubject, record.BasedOnRevisionID, record.SourceRevisionID,
		record.Origin, record.Snapshot.Name, record.Snapshot.Description,
		record.Snapshot.Type, nonNilStrings(record.Snapshot.Tags), record.Snapshot.Content,
		record.Snapshot.IsAIGenerated, nonNilStrings(record.Snapshot.SourceSessionIDs),
		record.ContentSHA256, record.ChangeNote, record.GenerationID,
		record.IdempotencyKey, record.LegacyReference, record.CreatedAt)
	if err != nil {
		return nil, appendRevisionError(err)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_revision_visibility (
			revision_id, is_public, changed_by_subject, changed_at
		) VALUES ($1, FALSE, $2, $3)`, schema), record.ID, record.CreatorSubject, record.CreatedAt); err != nil {
		return nil, fmt.Errorf("insert revision visibility: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit revision append: %w", err)
	}
	return &record, nil
}

func normalizeSkillSlug(slug string) string {
	return strings.ToLower(strings.TrimSpace(slug))
}

const skillRevisionColumns = `r.id::text, r.skill_id::text, r.sequence_number, r.version,
	r.creator_subject, COALESCE(r.based_on_revision_id::text, ''),
	r.source_revision_id::text, r.origin, r.name, r.description, r.type, r.tags,
	r.content, r.is_ai_generated, r.source_session_ids, r.content_sha256,
	r.change_note, COALESCE(r.generation_id::text, ''), r.idempotency_key,
	COALESCE(r.legacy_reference, ''), r.created_at`

func findAppendIdentity(ctx context.Context, tx pgx.Tx, schema string, input AppendRevisionInput) (*SkillRevisionRecord, error) {
	// Identity lookup deliberately has no visibility predicate. UUID,
	// generation, legacy, and per-skill idempotency identities must conflict
	// even when the existing revision is private to another creator. Only the
	// bounded identity/access columns are read until accessibility is known.
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT r.id::text, r.creator_subject, visibility.is_public
		FROM %s.skill_revisions r
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
		WHERE r.id = $1
		   OR ($2 <> '' AND r.skill_id = $3 AND r.idempotency_key = $2)
		   OR ($4 <> '' AND r.generation_id = $4::uuid)
		   OR ($5 <> '' AND r.legacy_reference = $5)
		FOR UPDATE OF r`, schema, schema), input.ID, input.IdempotencyKey,
		input.SkillID, input.GenerationID, input.LegacyReference)
	if err != nil {
		return nil, fmt.Errorf("find revision append identity: %w", err)
	}
	defer rows.Close()

	var foundID string
	var accessible bool
	for rows.Next() {
		var id, creator string
		var isPublic bool
		if err := rows.Scan(&id, &creator, &isPublic); err != nil {
			return nil, fmt.Errorf("scan revision append identity: %w", err)
		}
		if foundID != "" && foundID != id {
			return nil, ErrRevisionConflict
		}
		foundID = id
		accessible = isPublic || creator == input.CreatorSubject
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("find revision append identity: %w", err)
	}
	rows.Close()
	if foundID == "" {
		return nil, nil
	}
	if !accessible {
		return nil, ErrRevisionConflict
	}

	row := tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM %s.skill_revisions r
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
		WHERE r.id = $1 AND (visibility.is_public OR r.creator_subject = $2)`,
		skillRevisionColumns, schema, schema), foundID, input.CreatorSubject)
	record, err := scanSkillRevision(row)
	if err != nil {
		return nil, fmt.Errorf("read accessible revision append identity: %w", err)
	}
	return &record, nil
}

func revisionIsAccessibleLineage(
	ctx context.Context,
	tx pgx.Tx,
	schema string,
	revisionID string,
	targetSkillID string,
	callerSubject string,
	sameSkill bool,
) (bool, error) {
	skillPredicate := "r.skill_id = $2::uuid"
	if !sameSkill {
		skillPredicate = "r.skill_id <> $2::uuid"
	}
	var exists bool
	if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (
		SELECT 1 FROM %s.skill_revisions r
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
		WHERE r.id = $1
		  AND %s
		  AND (visibility.is_public OR r.creator_subject = $3)
	)`, schema, schema, skillPredicate), revisionID, targetSkillID, callerSubject).Scan(&exists); err != nil {
		return false, fmt.Errorf("verify accessible revision lineage: %w", err)
	}
	return exists, nil
}

func scanSkillRevision(row interface{ Scan(...any) error }) (SkillRevisionRecord, error) {
	var record SkillRevisionRecord
	var sourceRevisionID pgtype.Text
	err := row.Scan(&record.ID, &record.SkillID, &record.SequenceNumber, &record.Version,
		&record.CreatorSubject, &record.BasedOnRevisionID, &sourceRevisionID,
		&record.Origin, &record.Snapshot.Name, &record.Snapshot.Description,
		&record.Snapshot.Type, &record.Snapshot.Tags, &record.Snapshot.Content,
		&record.Snapshot.IsAIGenerated, &record.Snapshot.SourceSessionIDs,
		&record.ContentSHA256, &record.ChangeNote, &record.GenerationID,
		&record.IdempotencyKey, &record.LegacyReference, &record.CreatedAt)
	if sourceRevisionID.Valid {
		record.SourceRevisionID = sourceRevisionID.String
	}
	if err == nil {
		record.Snapshot = canonicalSkillRevisionSnapshot(record.Snapshot)
		record.CreatedAt = record.CreatedAt.UTC()
	}
	return record, err
}

func revisionMatchesAppend(record SkillRevisionRecord, input AppendRevisionInput) bool {
	return record.SkillID == input.SkillID &&
		record.CreatorSubject == input.CreatorSubject &&
		record.BasedOnRevisionID == input.BasedOnRevisionID &&
		record.SourceRevisionID == input.SourceRevisionID &&
		record.Origin == input.Origin &&
		record.Snapshot.Name == input.Snapshot.Name &&
		record.Snapshot.Description == input.Snapshot.Description &&
		record.Snapshot.Type == input.Snapshot.Type &&
		equalStrings(record.Snapshot.Tags, input.Snapshot.Tags) &&
		record.Snapshot.Content == input.Snapshot.Content &&
		record.Snapshot.IsAIGenerated == input.Snapshot.IsAIGenerated &&
		equalStrings(record.Snapshot.SourceSessionIDs, input.Snapshot.SourceSessionIDs) &&
		record.ChangeNote == input.ChangeNote && record.GenerationID == input.GenerationID &&
		record.IdempotencyKey == input.IdempotencyKey &&
		record.LegacyReference == input.LegacyReference &&
		record.ContentSHA256 == skillRevisionSnapshotSHA256(input.Snapshot)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func appendRevisionError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgUniqueViolation:
			return ErrRevisionConflict
		case "23503":
			return ErrRevisionLineageInvalid
		}
	}
	return fmt.Errorf("insert revision: %w", err)
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

// CreatePublishedSkill inserts a skill and its first immutable version in one
// transaction, so readers never observe a skill without version history. It is
// part of the retained predecessor surface: the row it creates is a legacy
// content head that the next migration pass recovers as a public revision.
func (s *PostgresStore) CreatePublishedSkill(ctx context.Context, rec SkillRecord, version SkillVersionRecord) (*SkillRecord, error) {
	if !validUUID(rec.ID) || version.SkillID != rec.ID || version.VersionNumber != 1 {
		return nil, errors.New("create published skill: invalid initial version")
	}
	if rec.ParentID != "" && !validUUID(rec.ParentID) {
		return nil, fmt.Errorf("create published skill: invalid parent id %q", rec.ParentID)
	}
	rec.Version = version.Semver
	rec.Content = version.Content
	rec.UpdatedAt = version.PublishedAt
	snapshot := canonicalPredecessorSkillSnapshot(predecessorSkillSnapshot{
		Slug: rec.Slug, Name: rec.Name, Description: rec.Description, Type: rec.Type,
		Tags: rec.Tags, Content: rec.Content, IsAIGenerated: rec.IsAIGenerated,
		SourceSessionIDs: rec.GeneratedFromSessionIDs, ParentID: rec.ParentID,
	})
	schema := quoteIdentifier(s.schema)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin create published skill tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// skill_versions deliberately has no cross-table foreign key, so the version
	// is inserted first and a duplicate skill id rolls both rows back together.
	insertVersion := fmt.Sprintf(`INSERT INTO %s.skill_versions (
			skill_id, version_number, semver, changelog, slug, name, description,
			type, visibility, tags, content, is_ai_generated,
			generated_from_session_ids, parent_id, content_sha256, expected_content,
			author_subject, published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, NULLIF($14, '')::uuid, $15, $16, $17, $18)`, schema)
	if _, err := tx.Exec(ctx, insertVersion,
		version.SkillID, version.VersionNumber, version.Semver, version.Changelog,
		snapshot.Slug, snapshot.Name, snapshot.Description, snapshot.Type,
		rec.Visibility, nonNilStrings(snapshot.Tags), version.Content,
		snapshot.IsAIGenerated, nonNilStrings(snapshot.SourceSessionIDs),
		snapshot.ParentID, predecessorSkillSnapshotSHA256(snapshot), version.ExpectedContent,
		version.AuthorSubject, version.PublishedAt); err != nil {
		return nil, fmt.Errorf("insert initial skill version: %w", err)
	}

	insertSkill := fmt.Sprintf(`INSERT INTO %s.skills (
			id, slug, name, description, type, version, visibility, tags, content,
			is_ai_generated, generated_from_session_ids, parent_id, author_subject,
			current_version_number, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, '')::uuid, $13, $14, $15, $16)
		RETURNING `+skillColumns, schema)
	out, err := scanSkill(tx.QueryRow(ctx, insertSkill,
		rec.ID, rec.Slug, rec.Name, rec.Description, rec.Type, rec.Version, rec.Visibility,
		nonNilStrings(rec.Tags), rec.Content, rec.IsAIGenerated,
		nonNilStrings(rec.GeneratedFromSessionIDs), rec.ParentID, rec.AuthorSubject,
		version.VersionNumber, rec.CreatedAt, rec.UpdatedAt))
	if err != nil {
		return nil, fmt.Errorf("insert published skill: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit create published skill tx: %w", err)
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
	var lockedID string
	err = tx.QueryRow(ctx,
		fmt.Sprintf(`SELECT id FROM %s.skills WHERE id = $1 FOR UPDATE`, schema), id,
	).Scan(&lockedID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock skill for delete: %w", err)
	}
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

// searchPredicate mirrors the pre-cutover case-insensitive substring search
// over name, description, and tags. $1 is a nullable literal ILIKE pattern;
// callers escape wildcard and escape characters before binding it.
const searchPredicate = `($1::text IS NULL
	OR name ILIKE $1::text ESCAPE E'\\'
	OR description ILIKE $1::text ESCAPE E'\\'
	OR EXISTS (SELECT 1 FROM unnest(tags) tag WHERE tag ILIKE $1::text ESCAPE E'\\'))`

// ListSkills returns one keyset page honoring the optional search/scope
// filters, any armed external attachment-view filters, the requested sort,
// and the cursor in opts. External predicates render inside this one
// paginating query — never as a post-fetch filter.
func (s *PostgresStore) ListSkills(ctx context.Context, opts SkillListOpts) ([]SkillRecord, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}
	schema := quoteIdentifier(s.schema)

	selectHead := fmt.Sprintf(`SELECT %s FROM %s.skills WHERE
		migration_alias_of_skill_id IS NULL AND
		(created_by_subject = '' OR EXISTS (
			SELECT 1 FROM %s.skill_revisions revision WHERE revision.skill_id = skills.id
		)) AND `, skillColumns, schema, schema)

	var where, orderBy string
	args := []any{literalILikePattern(opts.Query), nullText(opts.Author), nullText(opts.NotAuthor)}
	if opts.Sort == SkillSortDownloads {
		where = searchPredicate + `
			  AND ($2::text IS NULL OR author_subject = $2::text)
			  AND ($3::text IS NULL OR author_subject <> $3::text)
			  AND (
			    $4::bigint IS NULL
			    OR download_count < $4::bigint
			    OR (download_count = $4::bigint AND id < $5::uuid)
			  )`
		orderBy = `ORDER BY download_count DESC, id DESC`
		args = append(args, opts.CursorDownloads, nullText(opts.CursorID))
	} else {
		where = searchPredicate + `
			  AND ($2::text IS NULL OR author_subject = $2::text)
			  AND ($3::text IS NULL OR author_subject <> $3::text)
			  AND (
			    $4::timestamptz IS NULL
			    OR updated_at < $4::timestamptz
			    OR (updated_at = $4::timestamptz AND id < $5::uuid)
			  )`
		orderBy = `ORDER BY updated_at DESC, id DESC`
		args = append(args, opts.CursorTs, nullText(opts.CursorID))
	}
	where, args = appendExternalFilterPredicates(where, args, schema, opts.External)
	args = append(args, limit)
	query := selectHead + where + fmt.Sprintf(`
			%s
			LIMIT $%d`, orderBy, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, listSkillsError(err, opts)
	}
	defer rows.Close()
	recs, err := collectSkills(rows)
	if err != nil {
		// pgx can defer query-execution errors (a dropped external view
		// included) to row iteration, so classify here as well.
		return nil, listSkillsError(err, opts)
	}
	return recs, nil
}

// listSkillsError wraps a list failure, surfacing the typed
// ErrExternalViewUnavailable when an armed external filter is the cause so
// the handler can apply the missing-relation convention instead of a plain
// 500 — and never a silently unfiltered page.
func listSkillsError(err error, opts SkillListOpts) error {
	if len(opts.External) > 0 && isExternalRelationError(err) {
		return fmt.Errorf("list skills: %w: %v", ErrExternalViewUnavailable, err)
	}
	return fmt.Errorf("list skills: %w", err)
}

// appendExternalFilterPredicates renders one EXISTS per configured filter
// value against the deployment-granted external view, ANDed onto the where
// clause. Attachments to predecessor IDs coalesced by migration also match the
// one canonical outer identity; aliases themselves remain excluded by each
// caller's outer predicate. The view identifier passes through the quoting
// helper (config validation constrains its grammar upstream; quoting is belt
// and braces), and all configured data binds as positional arguments.
func appendExternalFilterPredicates(
	where string,
	args []any,
	schema string,
	filters []ExternalAttachmentFilter,
) (string, []any) {
	for _, filter := range filters {
		view := quoteQualifiedIdentifier(filter.View)
		for _, value := range filter.Values {
			args = append(args, filter.TypeValue, value)
			where += fmt.Sprintf(`
			  AND EXISTS (
			    SELECT 1 FROM %s ext
			    WHERE ext.primitive_type = $%d
			      AND ext.value = $%d
			      AND (
			        ext.primitive_id = skills.id::text
			        OR EXISTS (
			          SELECT 1 FROM %s.skills attachment_alias
			          WHERE attachment_alias.id::text = ext.primitive_id
			            AND attachment_alias.migration_alias_of_skill_id = skills.id
			        )
			      )
			  )`, view, len(args)-1, len(args), schema)
		}
	}
	return where, args
}

// ProbeExternalView checks that a configured external attachment view is
// readable and serves the canonical shape (primitive_type, primitive_id,
// value). The server runs it once at startup: a failing probe leaves the
// filter unarmed (absence is cheap), while a view that breaks later surfaces
// as ErrExternalViewUnavailable from ListSkills (breakage is loud).
func (s *PostgresStore) ProbeExternalView(ctx context.Context, view string) error {
	query := fmt.Sprintf(`SELECT primitive_type, primitive_id, value FROM %s WHERE FALSE`,
		quoteQualifiedIdentifier(view))
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("probe external view %s: %w", view, err)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("probe external view %s: %w", view, err)
	}
	return nil
}

var _ ExternalViewProber = (*PostgresStore)(nil)

// isExternalRelationError reports whether err is Postgres undefined_table
// (42P01) or insufficient_privilege (42501) — the two ways a configured
// external view stops being readable after its startup probe passed.
func isExternalRelationError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "42P01" || pgErr.Code == "42501"
}

// quoteQualifiedIdentifier renders a schema-qualified relation name safely,
// quoting each dot-separated segment independently.
func quoteQualifiedIdentifier(value string) string {
	segments := strings.Split(value, ".")
	for i, segment := range segments {
		segments[i] = quoteIdentifier(segment)
	}
	return strings.Join(segments, ".")
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

// CountSkills returns the per-tab totals for a search (ignoring cursor):
// every matching skill and how many the caller authored. Any armed external
// filters render into the same WHERE clause the page uses, so the totals
// describe exactly the filtered set — and a view that breaks mid-count
// surfaces as ErrExternalViewUnavailable, the same loud degradation as the
// page query, never silently unfiltered totals.
func (s *PostgresStore) CountSkills(ctx context.Context, opts SkillCountOpts) (SkillCounts, error) {
	schema := quoteIdentifier(s.schema)
	where := searchPredicate
	args := []any{literalILikePattern(opts.Query), opts.Author}
	where, args = appendExternalFilterPredicates(where, args, schema, opts.External)
	statement := fmt.Sprintf(`SELECT
			COUNT(*)::bigint AS total,
			COUNT(*) FILTER (WHERE author_subject = $2)::bigint AS mine
		FROM %s.skills
		WHERE migration_alias_of_skill_id IS NULL AND
		(created_by_subject = '' OR EXISTS (
			SELECT 1 FROM %s.skill_revisions revision WHERE revision.skill_id = skills.id
		)) AND `, schema, schema) + where
	var counts SkillCounts
	if err := s.pool.QueryRow(ctx, statement, args...).
		Scan(&counts.Total, &counts.Mine); err != nil {
		if len(opts.External) > 0 && isExternalRelationError(err) {
			return SkillCounts{}, fmt.Errorf("count skills: %w: %v", ErrExternalViewUnavailable, err)
		}
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

	var (
		current    string
		snapshot   predecessorSkillSnapshot
		visibility string
	)
	err = tx.QueryRow(ctx,
		fmt.Sprintf(`SELECT slug, name, description, type, visibility, tags, content,
			is_ai_generated, generated_from_session_ids, COALESCE(parent_id::text, '')
			FROM %s.skills WHERE id = $1 FOR UPDATE`, schema),
		rec.SkillID,
	).Scan(&snapshot.Slug, &snapshot.Name, &snapshot.Description, &snapshot.Type,
		&visibility, &snapshot.Tags, &current, &snapshot.IsAIGenerated,
		&snapshot.SourceSessionIDs, &snapshot.ParentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrSkillChanged
	}
	if err != nil {
		return nil, fmt.Errorf("lock skill head for publish: %w", err)
	}
	compareContent := rec.ExpectedContent
	if rec.CASContent != nil {
		compareContent = rec.CASContent
	}
	if compareContent != nil && current != *compareContent {
		return nil, ErrSkillChanged
	}

	snapshot.Content = rec.Content
	insert := fmt.Sprintf(`INSERT INTO %s.skill_versions (
			skill_id, version_number, semver, changelog, slug, name, description,
			type, visibility, tags, content, is_ai_generated,
			generated_from_session_ids, parent_id, content_sha256, expected_content,
			author_subject, published_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, NULLIF($14, '')::uuid, $15, $16, $17, $18)
		RETURNING skill_id::text, version_number, semver, changelog, content,
			expected_content, author_subject, published_at`, schema)
	var out SkillVersionRecord
	var expectedContent pgtype.Text
	err = tx.QueryRow(ctx, insert,
		rec.SkillID, rec.VersionNumber, rec.Semver, rec.Changelog,
		snapshot.Slug, snapshot.Name, snapshot.Description, snapshot.Type,
		visibility, nonNilStrings(snapshot.Tags), rec.Content,
		snapshot.IsAIGenerated, nonNilStrings(snapshot.SourceSessionIDs),
		snapshot.ParentID, predecessorSkillSnapshotSHA256(snapshot), rec.ExpectedContent,
		rec.AuthorSubject, rec.PublishedAt).
		Scan(&out.SkillID, &out.VersionNumber, &out.Semver, &out.Changelog,
			&out.Content, &expectedContent, &out.AuthorSubject, &out.PublishedAt)
	if err != nil {
		// A concurrent publish already claimed this version number; surface a
		// typed conflict so the handler can recompute and retry instead of 500.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return nil, ErrSkillVersionConflict
		}
		return nil, fmt.Errorf("publish skill version: %w", err)
	}
	if expectedContent.Valid {
		out.ExpectedContent = &expectedContent.String
	}

	bump := fmt.Sprintf(`UPDATE %s.skills
		SET version = $1, content = $2, updated_at = $3, current_version_number = $5
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
			expected_content, author_subject, published_at
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
		var expectedContent pgtype.Text
		if err := rows.Scan(&rec.SkillID, &rec.VersionNumber, &rec.Semver, &rec.Changelog,
			&rec.Content, &expectedContent, &rec.AuthorSubject, &rec.PublishedAt); err != nil {
			return nil, fmt.Errorf("list skill versions: %w", err)
		}
		if expectedContent.Valid {
			rec.ExpectedContent = &expectedContent.String
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

// literalILikePattern turns a user query into a case-insensitive literal
// substring pattern. Percent, underscore, and the escape character itself
// must be escaped so Postgres matches the same strings.Contains semantics as
// the memory store. An empty query remains SQL NULL and disables the filter.
func literalILikePattern(query string) *string {
	if query == "" {
		return nil
	}
	escaped := strings.NewReplacer(
		`\`, `\\`,
		`%`, `\%`,
		`_`, `\_`,
	).Replace(query)
	pattern := "%" + escaped + "%"
	return &pattern
}

// nullText maps the empty string to SQL NULL so optional scope predicates
// disable, mirroring the pre-cutover pgtype.Text behavior.
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
