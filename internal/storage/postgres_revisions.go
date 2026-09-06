package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const accessibleSkillRevisionColumns = skillRevisionColumns + `,
	visibility.revision_id::text, visibility.is_public,
	visibility.changed_by_subject, visibility.changed_at,
	COALESCE(r.id = skill.explicit_latest_revision_id, FALSE)`

// GetRevision applies the skill, UUID, and public-or-creator predicates in SQL
// before immutable content is materialized.
func (s *PostgresStore) GetRevision(ctx context.Context, opts RevisionReadOpts) (*AccessibleRevisionRecord, error) {
	if !validUUID(opts.SkillID) || !validUUID(opts.RevisionID) {
		return nil, ErrRevisionNotFound
	}

	schema := quoteIdentifier(s.schema)
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_revisions r
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
		JOIN %s.skills skill ON skill.id = r.skill_id
		WHERE r.skill_id = $1
		  AND r.id = $2
		  AND (visibility.is_public OR r.creator_subject = $3)`,
		accessibleSkillRevisionColumns, schema, schema, schema),
		opts.SkillID, opts.RevisionID, opts.CallerSubject)
	record, err := scanAccessibleRevision(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRevisionNotFound
		}
		return nil, fmt.Errorf("get revision: %w", err)
	}
	return &record, nil
}

// ListRevisions applies the public-or-creator predicate and newest-first
// sequence/UUID keyset in the database query.
func (s *PostgresStore) ListRevisions(ctx context.Context, opts RevisionListOpts) ([]AccessibleRevisionRecord, error) {
	limit, err := revisionListLimit(opts)
	if err != nil {
		return nil, err
	}
	if !validUUID(opts.SkillID) {
		return []AccessibleRevisionRecord{}, nil
	}

	schema := quoteIdentifier(s.schema)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_revisions r
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
		JOIN %s.skills skill ON skill.id = r.skill_id
		WHERE r.skill_id = $1
		  AND (visibility.is_public OR r.creator_subject = $2)
		  AND ($3::int IS NULL OR (r.sequence_number, r.id) < ($3::int, $4::uuid))
		ORDER BY r.sequence_number DESC, r.id DESC
		LIMIT $5`, accessibleSkillRevisionColumns, schema, schema, schema),
		opts.SkillID, opts.CallerSubject, opts.CursorSequenceNumber,
		nullText(opts.CursorRevisionID), limit)
	if err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}
	defer rows.Close()

	out := make([]AccessibleRevisionRecord, 0, limit)
	for rows.Next() {
		record, scanErr := scanAccessibleRevision(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan revision: %w", scanErr)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}
	return out, nil
}

// SetRevisionVisibility changes only mutable visibility/audit metadata. The
// implementation must lock the revision and skill invariants in one short
// transaction and treat an already-requested state as success.
func (s *PostgresStore) SetRevisionVisibility(
	ctx context.Context,
	input SetRevisionVisibilityInput,
) (*RevisionVisibilityRecord, error) {
	if !validUUID(input.SkillID) || !validUUID(input.RevisionID) || input.CallerSubject == "" {
		return nil, ErrRevisionNotFound
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin revision visibility mutation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	schema := quoteIdentifier(s.schema)
	var explicitLatestRevisionID string
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(explicit_latest_revision_id::text, '')
		FROM %s.skills
		WHERE id = $1 AND migration_alias_of_skill_id IS NULL
		FOR UPDATE`, schema), input.SkillID).Scan(&explicitLatestRevisionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRevisionNotFound
		}
		return nil, fmt.Errorf("lock skill for revision visibility: %w", err)
	}

	var visibility RevisionVisibilityRecord
	// The audit predicate grants only an exact retry of the currently stored
	// state to the actor that committed it. It selects bounded metadata rather
	// than immutable revision content, while unrelated private callers still
	// receive no row and therefore cannot probe revision IDs.
	err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT
		visibility.revision_id::text, visibility.is_public,
		visibility.changed_by_subject, visibility.changed_at
		FROM %s.skill_revisions r
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
		WHERE r.skill_id = $1 AND r.id = $2
		  AND (visibility.is_public OR r.creator_subject = $3 OR
		    (visibility.is_public = $4 AND visibility.changed_by_subject = $3))
		FOR UPDATE OF r, visibility`, schema, schema), input.SkillID,
		input.RevisionID, input.CallerSubject, input.IsPublic).Scan(
		&visibility.RevisionID, &visibility.IsPublic,
		&visibility.ChangedBySubject, &visibility.ChangedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRevisionNotFound
		}
		return nil, fmt.Errorf("lock accessible revision visibility: %w", err)
	}
	visibility.ChangedAt = visibility.ChangedAt.UTC()
	visibility.PreviousIsPublic = visibility.IsPublic
	visibility.Changed = visibility.IsPublic != input.IsPublic
	if !input.IsPublic && explicitLatestRevisionID == input.RevisionID {
		return nil, &RevisionIsExplicitLatestError{RevisionID: input.RevisionID}
	}
	if !visibility.Changed {
		if err = tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit idempotent revision visibility: %w", err)
		}
		return &visibility, nil
	}

	err = tx.QueryRow(ctx, fmt.Sprintf(`UPDATE %s.skill_revision_visibility
		SET is_public = $2, changed_by_subject = $3, changed_at = $4
		WHERE revision_id = $1
		RETURNING revision_id::text, is_public, changed_by_subject, changed_at`, schema),
		input.RevisionID, input.IsPublic, input.CallerSubject, input.ChangedAt.UTC()).Scan(
		&visibility.RevisionID, &visibility.IsPublic,
		&visibility.ChangedBySubject, &visibility.ChangedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("update revision visibility: %w", err)
	}
	visibility.ChangedAt = visibility.ChangedAt.UTC()
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit revision visibility mutation: %w", err)
	}
	return &visibility, nil
}

// SetExplicitLatestRevision atomically sets or moves explicit latest to a
// public revision of the same skill.
func (s *PostgresStore) SetExplicitLatestRevision(
	ctx context.Context,
	input SetExplicitLatestRevisionInput,
) (*SkillLatestRecord, error) {
	if !validUUID(input.SkillID) || !validUUID(input.RevisionID) || input.CallerSubject == "" {
		return nil, ErrRevisionNotFound
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin explicit latest mutation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	schema := quoteIdentifier(s.schema)
	var previousLatestRevisionID string
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(explicit_latest_revision_id::text, '')
		FROM %s.skills
		WHERE id = $1 AND migration_alias_of_skill_id IS NULL
		FOR UPDATE`, schema), input.SkillID).Scan(&previousLatestRevisionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRevisionNotFound
		}
		return nil, fmt.Errorf("lock skill for explicit latest: %w", err)
	}

	var (
		creatorSubject string
		isPublic       bool
	)
	err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT r.creator_subject, visibility.is_public
		FROM %s.skill_revisions r
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
		WHERE r.skill_id = $1 AND r.id = $2
		  AND (visibility.is_public OR r.creator_subject = $3)
		FOR UPDATE OF r, visibility`, schema, schema), input.SkillID,
		input.RevisionID, input.CallerSubject).Scan(&creatorSubject, &isPublic)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRevisionNotFound
		}
		return nil, fmt.Errorf("lock explicit latest target: %w", err)
	}
	_ = creatorSubject // Access was enforced in SQL before target state was returned.
	if !isPublic {
		return nil, &RevisionNotPublicError{RevisionID: input.RevisionID}
	}

	// Clearing the migration marker makes even an idempotent user pin
	// authoritative. A later migration pass may only advance pointers it still
	// owns, so predecessor writes on restart cannot overwrite this choice.
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills SET
		explicit_latest_revision_id = $2,
		migration_managed_latest_revision_id = NULL,
		updated_at = CASE WHEN explicit_latest_revision_id IS DISTINCT FROM $2::uuid
			THEN GREATEST(updated_at, $3) ELSE updated_at END
		WHERE id = $1`, schema), input.SkillID, input.RevisionID, input.ChangedAt.UTC()); err != nil {
		return nil, fmt.Errorf("set explicit latest revision: %w", err)
	}

	effective, err := getAccessibleRevisionWithQuerier(
		ctx, tx, schema, input.SkillID, input.RevisionID, input.CallerSubject,
	)
	if err != nil {
		return nil, err
	}
	if !effective.IsExplicitLatest {
		return nil, errors.New("set explicit latest revision: target was not marked explicit")
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit explicit latest mutation: %w", err)
	}
	return &SkillLatestRecord{
		SkillID: input.SkillID, ExplicitLatestRevisionID: input.RevisionID,
		EffectiveRevision: effective,
	}, nil
}

// ClearExplicitLatestRevision atomically clears explicit latest and resolves
// the newest-public fallback without marking that fallback explicit.
func (s *PostgresStore) ClearExplicitLatestRevision(
	ctx context.Context,
	input ClearExplicitLatestRevisionInput,
) (*SkillLatestRecord, error) {
	if !validUUID(input.SkillID) || input.CallerSubject == "" {
		return nil, ErrSkillNotFound
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin clear explicit latest: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	schema := quoteIdentifier(s.schema)
	var (
		previousLatestRevisionID string
		createdBySubject         string
	)
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT
		COALESCE(explicit_latest_revision_id::text, ''), created_by_subject
		FROM %s.skills
		WHERE id = $1 AND migration_alias_of_skill_id IS NULL
		FOR UPDATE`, schema), input.SkillID).Scan(
		&previousLatestRevisionID, &createdBySubject,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSkillNotFound
		}
		return nil, fmt.Errorf("lock skill to clear explicit latest: %w", err)
	}

	accessible := createdBySubject == input.CallerSubject
	if !accessible {
		if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (
			SELECT 1
			FROM %s.skill_revisions r
			JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
			WHERE r.skill_id = $1
			  AND (visibility.is_public OR r.creator_subject = $2)
		)`, schema, schema), input.SkillID, input.CallerSubject).Scan(&accessible); err != nil {
			return nil, fmt.Errorf("verify skill access to clear explicit latest: %w", err)
		}
	}
	if !accessible {
		return nil, ErrSkillNotFound
	}

	// Clear the migration marker even when the explicit pointer is already
	// empty. An idempotent user clear is still an authoritative choice that a
	// restart migration must not replace with a predecessor head.
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skills SET
		explicit_latest_revision_id = NULL,
		migration_managed_latest_revision_id = NULL,
		updated_at = CASE WHEN explicit_latest_revision_id IS NOT NULL
			THEN GREATEST(updated_at, $2) ELSE updated_at END
		WHERE id = $1`, schema), input.SkillID, input.ChangedAt.UTC()); err != nil {
		return nil, fmt.Errorf("clear explicit latest revision: %w", err)
	}

	effective, err := newestPublicRevisionWithQuerier(ctx, tx, schema, input.SkillID)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit clear explicit latest: %w", err)
	}
	return &SkillLatestRecord{SkillID: input.SkillID, EffectiveRevision: effective}, nil
}

func getAccessibleRevisionWithQuerier(
	ctx context.Context,
	querier interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	schema string,
	skillID string,
	revisionID string,
	callerSubject string,
) (*AccessibleRevisionRecord, error) {
	row := querier.QueryRow(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_revisions r
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
		JOIN %s.skills skill ON skill.id = r.skill_id
		WHERE r.skill_id = $1 AND r.id = $2
		  AND (visibility.is_public OR r.creator_subject = $3)`,
		accessibleSkillRevisionColumns, schema, schema, schema),
		skillID, revisionID, callerSubject)
	record, err := scanAccessibleRevision(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRevisionNotFound
		}
		return nil, fmt.Errorf("get accessible revision: %w", err)
	}
	return &record, nil
}

func newestPublicRevisionWithQuerier(
	ctx context.Context,
	querier interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	schema string,
	skillID string,
) (*AccessibleRevisionRecord, error) {
	row := querier.QueryRow(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_revisions r
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = r.id
		JOIN %s.skills skill ON skill.id = r.skill_id
		WHERE r.skill_id = $1 AND visibility.is_public
		ORDER BY r.sequence_number DESC, r.id DESC
		LIMIT 1`, accessibleSkillRevisionColumns, schema, schema, schema), skillID)
	record, err := scanAccessibleRevision(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("resolve newest public revision: %w", err)
	}
	return &record, nil
}

func scanAccessibleRevision(row interface{ Scan(...any) error }) (AccessibleRevisionRecord, error) {
	var (
		record           AccessibleRevisionRecord
		sourceRevisionID pgtype.Text
	)
	err := row.Scan(
		&record.Revision.ID,
		&record.Revision.SkillID,
		&record.Revision.SequenceNumber,
		&record.Revision.Version,
		&record.Revision.CreatorSubject,
		&record.Revision.BasedOnRevisionID,
		&sourceRevisionID,
		&record.Revision.Origin,
		&record.Revision.Snapshot.Name,
		&record.Revision.Snapshot.Description,
		&record.Revision.Snapshot.Type,
		&record.Revision.Snapshot.Tags,
		&record.Revision.Snapshot.Content,
		&record.Revision.Snapshot.IsAIGenerated,
		&record.Revision.Snapshot.SourceSessionIDs,
		&record.Revision.ContentSHA256,
		&record.Revision.ChangeNote,
		&record.Revision.GenerationID,
		&record.Revision.IdempotencyKey,
		&record.Revision.LegacyReference,
		&record.Revision.CreatedAt,
		&record.Visibility.RevisionID,
		&record.Visibility.IsPublic,
		&record.Visibility.ChangedBySubject,
		&record.Visibility.ChangedAt,
		&record.IsExplicitLatest,
	)
	if sourceRevisionID.Valid {
		record.Revision.SourceRevisionID = sourceRevisionID.String
	}
	if err == nil {
		record.Revision.CreatedAt = record.Revision.CreatedAt.UTC()
		record.Visibility.ChangedAt = record.Visibility.ChangedAt.UTC()
	}
	return record, err
}
