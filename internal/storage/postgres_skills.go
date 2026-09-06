package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const effectiveRevisionSearchPredicate = `($2::text IS NULL
	OR card.name ILIKE $2::text ESCAPE E'\\'
	OR card.description ILIKE $2::text ESCAPE E'\\'
	OR EXISTS (SELECT 1 FROM unnest(card.tags) tag WHERE tag ILIKE $2::text ESCAPE E'\\'))`

// ResolveEffectiveRevision resolves explicit latest or, when no pointer is
// stored, the deterministic newest-public fallback.
func (s *PostgresStore) ResolveEffectiveRevision(
	ctx context.Context,
	opts EffectiveRevisionReadOpts,
) (*AccessibleRevisionRecord, error) {
	if !validUUID(opts.SkillID) {
		return nil, ErrSkillNotFound
	}

	tx, err := s.pool.BeginTx(ctx, effectiveReadTxOptions())
	if err != nil {
		return nil, fmt.Errorf("begin effective revision read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	schema := quoteIdentifier(s.schema)
	var explicitLatestRevisionID string
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(explicit_latest_revision_id::text, '')
		FROM %s.skills
		WHERE id = $1 AND migration_alias_of_skill_id IS NULL`, schema), opts.SkillID).
		Scan(&explicitLatestRevisionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSkillNotFound
		}
		return nil, fmt.Errorf("read skill for effective revision: %w", err)
	}

	var effective *AccessibleRevisionRecord
	if explicitLatestRevisionID != "" {
		effective, err = getAccessibleRevisionWithQuerier(
			ctx, tx, schema, opts.SkillID, explicitLatestRevisionID, "",
		)
		if errors.Is(err, ErrRevisionNotFound) || (err == nil && !effective.Visibility.IsPublic) {
			return nil, errors.New("resolve effective revision: explicit latest is not a public revision of the skill")
		}
	} else {
		effective, err = newestPublicRevisionWithQuerier(ctx, tx, schema, opts.SkillID)
	}
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit effective revision read: %w", err)
	}
	return effective, nil
}

// GetEffectiveSkill returns one viewer-aware projection for a stable skill.
func (s *PostgresStore) GetEffectiveSkill(
	ctx context.Context,
	opts EffectiveSkillReadOpts,
) (*EffectiveSkillRecord, error) {
	if !validUUID(opts.SkillID) {
		return nil, ErrSkillNotFound
	}

	tx, err := s.pool.BeginTx(ctx, effectiveReadTxOptions())
	if err != nil {
		return nil, fmt.Errorf("begin effective skill read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	projection, err := loadEffectiveSkillWithQuerier(
		ctx, tx, quoteIdentifier(s.schema), opts.SkillID, opts.CallerSubject, true,
	)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit effective skill read: %w", err)
	}
	return projection, nil
}

// ListEffectiveSkills returns at most one viewer-aware projection per stable
// skill.
func (s *PostgresStore) ListEffectiveSkills(
	ctx context.Context,
	opts EffectiveSkillListOpts,
) ([]EffectiveSkillRecord, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}

	tx, err := s.pool.BeginTx(ctx, effectiveReadTxOptions())
	if err != nil {
		return nil, fmt.Errorf("begin effective skill list: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	schema := quoteIdentifier(s.schema)
	where := effectiveRevisionSearchPredicate + `
		AND ($3::text IS NULL OR card.creator_subject = $3)
		AND ($4::text IS NULL OR card.creator_subject <> $4)`
	args := []any{
		opts.CallerSubject, literalILikePattern(opts.Query), nullText(opts.Author),
		nullText(opts.NotAuthor),
	}
	var orderBy string
	if opts.Sort == SkillSortDownloads {
		where += `
			AND ($5::bigint IS NULL OR skills.download_count < $5
				OR (skills.download_count = $5 AND skills.id < $6::uuid))`
		args = append(args, opts.CursorDownloads, nullText(opts.CursorID))
		orderBy = `ORDER BY skills.download_count DESC, skills.id DESC`
	} else {
		where += `
			AND ($5::timestamptz IS NULL OR card.created_at < $5
				OR (card.created_at = $5 AND skills.id < $6::uuid))`
		args = append(args, opts.CursorTs, nullText(opts.CursorID))
		orderBy = `ORDER BY card.created_at DESC, skills.id DESC`
	}
	where, args = appendExternalFilterPredicates(where, args, schema, opts.External)
	args = append(args, limit)

	query := fmt.Sprintf(`SELECT skills.id::text
		FROM %s.skills skills
		%s
		WHERE skills.migration_alias_of_skill_id IS NULL AND %s
		%s
		LIMIT $%d`, schema, effectiveSkillCardJoins(schema, 1), where, orderBy, len(args))
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, listSkillsError(err, opts.SkillListOpts)
	}
	skillIDs := make([]string, 0, limit)
	for rows.Next() {
		var skillID string
		if err = rows.Scan(&skillID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan effective skill identity: %w", err)
		}
		skillIDs = append(skillIDs, skillID)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, listSkillsError(err, opts.SkillListOpts)
	}

	out := make([]EffectiveSkillRecord, 0, len(skillIDs))
	for _, skillID := range skillIDs {
		projection, loadErr := loadEffectiveSkillWithQuerier(
			ctx, tx, schema, skillID, opts.CallerSubject, false,
		)
		if loadErr != nil {
			return nil, fmt.Errorf("load listed effective skill: %w", loadErr)
		}
		out = append(out, *projection)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit effective skill list: %w", err)
	}
	return out, nil
}

// CountEffectiveSkills counts the same one-entry-per-skill projection selected
// by ListEffectiveSkills.
func (s *PostgresStore) CountEffectiveSkills(
	ctx context.Context,
	opts EffectiveSkillCountOpts,
) (SkillCounts, error) {
	schema := quoteIdentifier(s.schema)
	where := effectiveRevisionSearchPredicate
	args := []any{opts.CallerSubject, literalILikePattern(opts.Query), nullText(opts.Author)}
	where, args = appendExternalFilterPredicates(where, args, schema, opts.External)
	query := fmt.Sprintf(`SELECT
		COUNT(*)::bigint,
		COUNT(*) FILTER (WHERE $3::text IS NOT NULL AND card.creator_subject = $3)::bigint
		FROM %s.skills skills
		%s
		WHERE skills.migration_alias_of_skill_id IS NULL AND %s`,
		schema, effectiveSkillCardJoins(schema, 1), where)

	var counts SkillCounts
	if err := s.pool.QueryRow(ctx, query, args...).Scan(&counts.Total, &counts.Mine); err != nil {
		if len(opts.External) > 0 && isExternalRelationError(err) {
			return SkillCounts{}, fmt.Errorf("count effective skills: %w: %v", ErrExternalViewUnavailable, err)
		}
		return SkillCounts{}, fmt.Errorf("count effective skills: %w", err)
	}
	return counts, nil
}

// ListEffectiveSkillsBySession returns at most one viewer-aware projection per
// stable skill with accessible provenance for the requested session.
func (s *PostgresStore) ListEffectiveSkillsBySession(
	ctx context.Context,
	opts EffectiveSkillSessionListOpts,
) ([]EffectiveSkillRecord, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = DefaultListLimit
	}

	tx, err := s.pool.BeginTx(ctx, effectiveReadTxOptions())
	if err != nil {
		return nil, fmt.Errorf("begin effective session skill list: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	schema := quoteIdentifier(s.schema)
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT skills.id::text
		FROM %s.skills skills
		%s
		WHERE skills.migration_alias_of_skill_id IS NULL
		  AND EXISTS (
			SELECT 1
			FROM %s.skill_revisions source_revision
			JOIN %s.skill_revision_visibility source_visibility
			  ON source_visibility.revision_id = source_revision.id
			WHERE source_revision.skill_id = skills.id
			  AND $2::text = ANY(source_revision.source_session_ids)
			  AND (source_visibility.is_public OR source_revision.creator_subject = $1)
		  )
		ORDER BY card.created_at DESC, skills.id DESC
		LIMIT $3`, schema,
		effectiveSkillCardJoins(schema, 1), schema, schema),
		opts.CallerSubject, opts.SessionID, limit)
	if err != nil {
		return nil, fmt.Errorf("list effective session skills: %w", err)
	}
	skillIDs := make([]string, 0, limit)
	for rows.Next() {
		var skillID string
		if err = rows.Scan(&skillID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan effective session skill identity: %w", err)
		}
		skillIDs = append(skillIDs, skillID)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate effective session skills: %w", err)
	}

	out := make([]EffectiveSkillRecord, 0, len(skillIDs))
	for _, skillID := range skillIDs {
		projection, loadErr := loadEffectiveSkillWithQuerier(
			ctx, tx, schema, skillID, opts.CallerSubject, false,
		)
		if loadErr != nil {
			return nil, fmt.Errorf("load effective session skill: %w", loadErr)
		}
		out = append(out, *projection)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit effective session skill list: %w", err)
	}
	return out, nil
}

func effectiveReadTxOptions() pgx.TxOptions {
	return pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
}

func effectiveSkillCardJoins(schema string, callerArgument int) string {
	return fmt.Sprintf(`LEFT JOIN LATERAL (
		SELECT revision.id, revision.created_at
		FROM %s.skill_revisions revision
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
		WHERE revision.skill_id = skills.id AND visibility.is_public
		  AND (skills.explicit_latest_revision_id IS NULL
			OR revision.id = skills.explicit_latest_revision_id)
		ORDER BY revision.sequence_number DESC, revision.id DESC
		LIMIT 1
	) effective_revision ON TRUE
	LEFT JOIN LATERAL (
		SELECT revision.id, revision.created_at
		FROM %s.skill_revisions revision
		JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
		WHERE revision.skill_id = skills.id
		  AND revision.creator_subject = $%d
		  AND NOT visibility.is_public
		ORDER BY revision.sequence_number DESC, revision.id DESC
		LIMIT 1
	) newest_private_revision ON TRUE
	JOIN %s.skill_revisions card
	  ON card.id = COALESCE(effective_revision.id, newest_private_revision.id)`,
		schema, schema, schema, schema, callerArgument, schema)
}

func loadEffectiveSkillWithQuerier(
	ctx context.Context,
	querier interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	schema string,
	skillID string,
	callerSubject string,
	includeOwnedEmpty bool,
) (*EffectiveSkillRecord, error) {
	var (
		skill           SkillRecord
		effectiveID     pgtype.Text
		newestPrivateID pgtype.Text
		actualCreatedBy string
	)
	err := querier.QueryRow(ctx, fmt.Sprintf(`SELECT
		skills.id::text, skills.slug,
		COALESCE(skills.explicit_latest_revision_id::text, ''),
		0, skills.created_by_subject,
		skills.download_count, skills.created_at, skills.updated_at,
		CASE
			WHEN skills.explicit_latest_revision_id IS NOT NULL
				THEN skills.explicit_latest_revision_id::text
			ELSE (
				SELECT revision.id::text
				FROM %s.skill_revisions revision
				JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
				WHERE revision.skill_id = skills.id AND visibility.is_public
				ORDER BY revision.sequence_number DESC, revision.id DESC
				LIMIT 1
			)
		END,
		(
			SELECT revision.id::text
			FROM %s.skill_revisions revision
			JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = revision.id
			WHERE revision.skill_id = skills.id
			  AND revision.creator_subject = $2
			  AND NOT visibility.is_public
			ORDER BY revision.sequence_number DESC, revision.id DESC
			LIMIT 1
		)
		FROM %s.skills skills
		WHERE skills.id = $1 AND skills.migration_alias_of_skill_id IS NULL`,
		schema, schema, schema, schema, schema), skillID, callerSubject).Scan(
		&skill.ID, &skill.Slug, &skill.ExplicitLatestRevisionID,
		&skill.NextSequenceNumber, &actualCreatedBy, &skill.DownloadCount,
		&skill.CreatedAt, &skill.UpdatedAt, &effectiveID, &newestPrivateID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSkillNotFound
		}
		return nil, fmt.Errorf("read effective skill identity: %w", err)
	}
	if actualCreatedBy == callerSubject && callerSubject != "" {
		skill.CreatedBySubject = actualCreatedBy
	}
	skill.CreatedAt = skill.CreatedAt.UTC()
	skill.UpdatedAt = skill.UpdatedAt.UTC()

	var effective, newestPrivate *AccessibleRevisionRecord
	if effectiveID.Valid {
		effective, err = getAccessibleRevisionWithQuerier(
			ctx, querier, schema, skill.ID, effectiveID.String, callerSubject,
		)
		if err != nil {
			return nil, fmt.Errorf("load effective public revision: %w", err)
		}
		if !effective.Visibility.IsPublic {
			return nil, errors.New("load effective skill: explicit revision is private")
		}
	}
	if newestPrivateID.Valid {
		newestPrivate, err = getAccessibleRevisionWithQuerier(
			ctx, querier, schema, skill.ID, newestPrivateID.String, callerSubject,
		)
		if err != nil {
			return nil, fmt.Errorf("load newest private revision: %w", err)
		}
		if newestPrivate.Visibility.IsPublic || newestPrivate.Revision.CreatorSubject != callerSubject {
			return nil, errors.New("load effective skill: private continuation is not caller-owned")
		}
	}

	card := effective
	if card == nil {
		card = newestPrivate
	}
	if card == nil && (!includeOwnedEmpty || actualCreatedBy != callerSubject || callerSubject == "") {
		return nil, ErrSkillNotFound
	}
	if card != nil {
		skill.UpdatedAt = card.Revision.CreatedAt
	}
	projection := &EffectiveSkillRecord{
		Skill: skill, EffectiveRevision: effective,
		NewestPrivateRevision: newestPrivate, CardRevision: card,
	}
	if effective != nil && newestPrivate != nil {
		projection.HasNewerPrivateRevision =
			newestPrivate.Revision.SequenceNumber > effective.Revision.SequenceNumber
	}
	return projection, nil
}
