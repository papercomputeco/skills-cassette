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
