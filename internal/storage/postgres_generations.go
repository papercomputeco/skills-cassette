package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ GenerationStore = (*PostgresStore)(nil)

const generationColumns = `g.id::text, g.skill_id::text,
	COALESCE(g.base_revision_id::text, ''), g.creator_subject, g.status,
	g.input_name, g.input_description, g.input_type, g.input_tags,
	g.input_content, g.input_is_ai_generated, g.input_source_session_ids,
	g.author_context, g.selected_session_ids, g.evaluator_profile,
	g.evaluator_profile_version, g.evaluation_criteria,
	COALESCE(g.winner_candidate_id::text, ''),
	COALESCE(g.result_candidate_id::text, ''),
	COALESCE(g.result_revision_id::text, ''), g.error_code, g.error_message,
	COALESCE(g.claim_token::text, ''), g.claim_owner, g.lease_expires_at,
	g.attempt_count, g.next_attempt_at, g.last_heartbeat_at, g.created_at,
	g.updated_at, g.started_at, g.completed_at`

const generationCandidateColumns = `c.id::text, c.generation_id::text, c.ordinal, c.kind,
	c.source_session_ids, c.name, c.description, c.type, c.tags, c.content,
	c.is_ai_generated, c.insights, c.bundle_sha256, c.created_at`

const candidateEvaluationColumns = `e.id::text, e.generation_id::text,
	e.candidate_id::text, e.request_sha256, e.profile, e.profile_version,
	e.evaluator_version, e.score, e.decision, e.critical_finding_count,
	e.warning_finding_count, e.criterion_results, e.findings, e.strengths,
	e.panel, e.created_at`

const generationSummaryColumns = `g.id::text, g.skill_id::text,
	COALESCE(g.base_revision_id::text, ''), g.status,
	COALESCE(g.result_revision_id::text, ''), g.error_code, g.error_message,
	g.attempt_count, g.created_at, g.updated_at, g.started_at, g.completed_at`

// CreateGeneration commits a complete skill/revision generation seed and its
// ordered sessions in one transaction.
func (s *PostgresStore) CreateGeneration(ctx context.Context, input CreateGenerationInput) (*SkillGenerationRecord, error) {
	if err := validateSkillGenerationInput(input); err != nil {
		return nil, err
	}
	input.AuthorContext = strings.TrimSpace(input.AuthorContext)
	input.SelectedSessionIDs, _ = normalizedSnapshotIdentities(
		input.SelectedSessionIDs, MaxRevisionSourceSessionIDs, "selected session ids",
	)
	var err error
	input.Snapshot, err = normalizeSkillRevisionSnapshot(input.Snapshot)
	if err != nil {
		return nil, fmt.Errorf("create generation: %w", err)
	}
	criteria, err := normalizedJSON(input.EvaluationCriteria, nil)
	if err != nil {
		return nil, fmt.Errorf("create generation: %w", err)
	}
	schema := quoteIdentifier(s.schema)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin generation creation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var stableSkillID string
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT id::text FROM %s.skills
		WHERE id = $1 AND migration_alias_of_skill_id IS NULL
		FOR KEY SHARE`, schema), input.SkillID).Scan(&stableSkillID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSkillNotFound
		}
		return nil, fmt.Errorf("verify generation skill: %w", err)
	}
	// Serialize only retries of the same caller-provided generation UUID. This
	// does not serialize independent generations for one skill or reserve a
	// revision sequence.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, input.ID); err != nil {
		return nil, fmt.Errorf("lock generation idempotency key: %w", err)
	}
	if input.BaseRevisionID != "" {
		var accessibleBaseRevisionID string
		if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT revision.id::text
			FROM %s.skill_revisions revision
			JOIN %s.skill_revision_visibility visibility
			  ON visibility.revision_id = revision.id
			WHERE revision.id = $1 AND revision.skill_id = $2
			  AND (visibility.is_public OR revision.creator_subject = $3)
			FOR SHARE OF revision, visibility`, schema, schema),
			input.BaseRevisionID, input.SkillID, input.CreatorSubject).
			Scan(&accessibleBaseRevisionID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrRevisionNotFound
			}
			return nil, fmt.Errorf("verify generation base revision: %w", err)
		}
	}

	existing, findErr := scanGeneration(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_generations g WHERE g.id = $1 FOR UPDATE`, generationColumns, schema), input.ID))
	if findErr == nil {
		if !generationMatchesCreateInput(*existing, input) {
			return nil, errors.New("create generation: id already exists")
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit idempotent generation creation: %w", err)
		}
		return existing, nil
	}
	if !errors.Is(findErr, pgx.ErrNoRows) {
		return nil, fmt.Errorf("find generation creation retry: %w", findErr)
	}

	// Sample database time exactly once for the new row and every ordered
	// session. Caller-provided timestamps never control queue eligibility.
	var createdAt time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&createdAt); err != nil {
		return nil, fmt.Errorf("read generation creation clock: %w", err)
	}
	createdAt = createdAt.UTC()
	row := tx.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %s.skill_generations AS g (
		id, skill_id, base_revision_id, creator_subject, status,
		input_name, input_description, input_type, input_tags,
		input_content, input_is_ai_generated, input_source_session_ids,
		author_context, selected_session_ids, evaluator_profile,
		evaluator_profile_version, evaluation_criteria,
		next_attempt_at, created_at, updated_at
	) VALUES ($1, $2, NULLIF($3, '')::uuid, $4, 'queued', $5, $6,
		$7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb,
		$17, $17, $17)
	RETURNING %s`, schema, generationColumns),
		input.ID, input.SkillID, input.BaseRevisionID, input.CreatorSubject,
		input.Snapshot.Name, input.Snapshot.Description, input.Snapshot.Type,
		nonNilStrings(input.Snapshot.Tags), input.Snapshot.Content,
		input.Snapshot.IsAIGenerated, nonNilStrings(input.Snapshot.SourceSessionIDs),
		input.AuthorContext, nonNilStrings(input.SelectedSessionIDs),
		input.EvaluatorProfile, input.EvaluatorProfileVersion, criteria, createdAt)
	generation, err := scanGeneration(row)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, errors.New("create generation: id already exists")
		}
		return nil, fmt.Errorf("insert generation: %w", err)
	}
	if err = insertGenerationSessions(ctx, tx, schema, input.ID, input.SelectedSessionIDs, createdAt); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit generation creation: %w", err)
	}
	return generation, nil
}

func insertGenerationSessions(ctx context.Context, tx pgx.Tx, schema, generationID string, sessionIDs []string, createdAt time.Time) error {
	for ordinal, sessionID := range sessionIDs {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.generation_sessions (
			generation_id, session_id, ordinal, status, updated_at
		) VALUES ($1, $2, $3, 'pending', $4)`, schema), generationID,
			sessionID, ordinal, createdAt); err != nil {
			return fmt.Errorf("snapshot generation session: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) ListSkillGenerations(ctx context.Context, opts SkillGenerationListOpts) (*SkillGenerationListPage, error) {
	limit, err := normalizeSkillGenerationListOpts(opts)
	if err != nil {
		return nil, err
	}
	schema := quoteIdentifier(s.schema)
	var exists bool
	if err = s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (
		SELECT 1 FROM %s.skills WHERE id = $1 AND migration_alias_of_skill_id IS NULL
	)`, schema), opts.SkillID).Scan(&exists); err != nil {
		return nil, fmt.Errorf("verify generation skill: %w", err)
	}
	if !exists {
		return nil, ErrSkillNotFound
	}

	query := fmt.Sprintf(`SELECT %s FROM %s.skill_generations g
		WHERE g.skill_id = $1 AND (
			g.creator_subject = $2 OR (
				g.status = 'completed' AND EXISTS (
					SELECT 1 FROM %s.skill_revisions result_revision
					JOIN %s.skill_revision_visibility result_visibility
					  ON result_visibility.revision_id = result_revision.id
					WHERE result_revision.id = g.result_revision_id
					  AND result_revision.skill_id = g.skill_id
					  AND result_revision.generation_id = g.id
					  AND result_visibility.is_public
				)
			)
		)`, generationSummaryColumns, schema, schema, schema)
	args := []any{opts.SkillID, opts.CallerSubject}
	if opts.CursorCreatedAt != nil {
		query += ` AND (g.created_at < $3 OR (g.created_at = $3 AND g.id < $4::uuid))`
		args = append(args, opts.CursorCreatedAt.UTC(), opts.CursorID)
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY g.created_at DESC, g.id DESC LIMIT $%d", len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list skill generations: %w", err)
	}
	defer rows.Close()
	summaries := make([]SkillGenerationSummaryRecord, 0, limit+1)
	for rows.Next() {
		summary, scanErr := scanGenerationSummary(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		summaries = append(summaries, *summary)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate skill generations: %w", err)
	}
	return generationListPage(summaries, limit), nil
}

func (s *PostgresStore) GetSkillGeneration(ctx context.Context, callerSubject, skillID, generationID string) (*GenerationState, error) {
	if !validUUID(skillID) || !validUUID(generationID) || callerSubject == "" {
		return nil, nil
	}
	return s.getAuthorizedGenerationState(ctx, callerSubject, skillID, generationID)
}

func (s *PostgresStore) GetGenerationByID(ctx context.Context, callerSubject, generationID string) (*GenerationState, error) {
	if !validUUID(generationID) || callerSubject == "" {
		return nil, nil
	}
	return s.getAuthorizedGenerationState(ctx, callerSubject, "", generationID)
}

// getAuthorizedGenerationState selects the generation and every bounded
// artifact collection from one repeatable-read transaction. A non-creator read
// also takes a SHARE lock on the exact result visibility row, preventing a
// public-to-private transition from committing between authorization and
// artifact materialization.
func (s *PostgresStore) getAuthorizedGenerationState(
	ctx context.Context,
	callerSubject string,
	skillID string,
	generationID string,
) (*GenerationState, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("begin generation read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	generation, err := s.authorizeGenerationRead(ctx, tx, callerSubject, skillID, generationID)
	if err != nil || generation == nil {
		return nil, err
	}
	state, err := s.loadGenerationState(ctx, tx, *generation)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit exact generation read: %w", err)
	}
	return state, nil
}

func (s *PostgresStore) authorizeGenerationRead(
	ctx context.Context,
	tx pgx.Tx,
	callerSubject string,
	skillID string,
	generationID string,
) (*SkillGenerationRecord, error) {
	schema := quoteIdentifier(s.schema)
	skillPredicate := ""
	args := []any{generationID, callerSubject}
	if skillID != "" {
		skillPredicate = " AND g.skill_id = $3"
		args = append(args, skillID)
	}
	generation, err := scanGeneration(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_generations g
		WHERE g.id = $1 AND g.creator_subject = $2%s
		FOR SHARE OF g`, generationColumns, schema, skillPredicate), args...))
	if errors.Is(err, pgx.ErrNoRows) {
		generation, err = scanGeneration(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s
			FROM %s.skill_generations g
			JOIN %s.skill_revisions result_revision
			  ON result_revision.id = g.result_revision_id
			 AND result_revision.skill_id = g.skill_id
			 AND result_revision.generation_id = g.id
			JOIN %s.skill_revision_visibility result_visibility
			  ON result_visibility.revision_id = result_revision.id
			WHERE g.id = $1 AND $2 <> '' AND g.status = 'completed'
			  AND result_visibility.is_public%s
			FOR SHARE OF g, result_revision, result_visibility`, generationColumns,
			schema, schema, schema, skillPredicate), args...))
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("authorize exact generation read: %w", err)
	}
	return generation, nil
}

func (s *PostgresStore) ClaimGeneration(ctx context.Context, input ClaimGenerationInput) (*SkillGenerationRecord, error) {
	if input.WorkerID == "" || input.LeaseDuration <= 0 {
		return nil, errors.New("claim generation: worker and positive lease duration are required")
	}
	token := uuid.NewString()
	leaseMicroseconds := postgresLeaseMicroseconds(input.LeaseDuration)
	schema := quoteIdentifier(s.schema)
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`WITH queue_clock AS MATERIALIZED (
		SELECT clock_timestamp() AS now
	), claimable AS (
		SELECT generation.id, queue_clock.now
		FROM %s.skill_generations AS generation
		CROSS JOIN queue_clock
		WHERE generation.status IN ('queued', 'generating_candidates', 'evaluating_candidates', 'synthesizing')
		  AND generation.next_attempt_at <= queue_clock.now
		  AND (generation.lease_expires_at IS NULL OR generation.lease_expires_at <= queue_clock.now)
		ORDER BY generation.next_attempt_at, generation.created_at, generation.id
		FOR UPDATE OF generation SKIP LOCKED
		LIMIT 1
	)
	UPDATE %s.skill_generations AS g SET
		claim_token = $1, claim_owner = $2, error_code = '', error_message = '',
		lease_expires_at = claimable.now + ($3::bigint * interval '1 microsecond'),
		attempt_count = g.attempt_count + 1, last_heartbeat_at = claimable.now,
		started_at = COALESCE(g.started_at, claimable.now), updated_at = claimable.now
	FROM claimable WHERE g.id = claimable.id
	RETURNING %s`, schema, schema, generationColumns), token, input.WorkerID,
		leaseMicroseconds)
	generation, err := scanGeneration(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim generation: %w", err)
	}
	return generation, nil
}

func postgresLeaseMicroseconds(duration time.Duration) int64 {
	microseconds := duration / time.Microsecond
	if duration%time.Microsecond != 0 {
		microseconds++
	}
	return int64(microseconds)
}

func (s *PostgresStore) RenewGenerationLease(ctx context.Context, generationID, claimToken string, leaseDuration time.Duration) (bool, error) {
	if leaseDuration <= 0 {
		return false, errors.New("renew generation lease: positive lease duration is required")
	}
	tag, err := s.pool.Exec(ctx, fmt.Sprintf(`WITH locked_generation AS MATERIALIZED (
		SELECT generation.id
		FROM %s.skill_generations AS generation
		WHERE generation.id = $1
		FOR UPDATE OF generation
	), lease_clock AS MATERIALIZED (
		SELECT clock_timestamp() AS now
		FROM locked_generation
	)
	UPDATE %s.skill_generations AS generation SET
		lease_expires_at = lease_clock.now + ($3::bigint * interval '1 microsecond'),
		last_heartbeat_at = lease_clock.now, updated_at = lease_clock.now
	FROM lease_clock
	WHERE generation.id = $1 AND generation.claim_token = $2
	  AND generation.lease_expires_at > lease_clock.now
	  AND generation.status IN ('queued', 'generating_candidates', 'evaluating_candidates', 'synthesizing')`,
		quoteIdentifier(s.schema), quoteIdentifier(s.schema)), generationID, claimToken,
		postgresLeaseMicroseconds(leaseDuration))
	if err != nil {
		return false, fmt.Errorf("renew generation lease: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	terminalErr, resolveErr := s.generationClaimTerminalError(ctx, generationID, claimToken)
	if resolveErr != nil {
		return false, fmt.Errorf("resolve generation lease renewal: %w", resolveErr)
	}
	if terminalErr != nil {
		return false, terminalErr
	}
	return false, nil
}

// FailGeneration locks and validates a live claim before committing its
// curated terminal failure, timestamps, and claim release atomically.
func (s *PostgresStore) FailGeneration(ctx context.Context, input FailGenerationInput) (*SkillGenerationRecord, error) {
	if err := validateGenerationFailure(input.Failure); err != nil {
		return nil, err
	}
	tx, _, now, err := s.beginGenerationMutation(ctx, input.GenerationID, input.ClaimToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	row := tx.QueryRow(ctx, fmt.Sprintf(`UPDATE %s.skill_generations AS g SET
		status = 'failed', error_code = $2, error_message = $3,
		updated_at = $4, completed_at = $4,
		claim_token = NULL, claim_owner = '', lease_expires_at = NULL
		WHERE g.id = $1 RETURNING %s`, quoteIdentifier(s.schema), generationColumns),
		input.GenerationID, input.Failure.Code, input.Failure.Message, now)
	failed, err := scanGeneration(row)
	if err != nil {
		return nil, fmt.Errorf("fail generation: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit generation failure: %w", err)
	}
	return failed, nil
}

// RequeueGeneration locks and validates a live claim before committing its
// curated retry summary, storage-clock due time, and claim release atomically.
func (s *PostgresStore) RequeueGeneration(ctx context.Context, input RequeueGenerationInput) (*SkillGenerationRecord, error) {
	if input.RetryAfter <= 0 {
		return nil, errors.New("requeue generation: positive retry delay is required")
	}
	if err := validateGenerationFailure(input.Failure); err != nil {
		return nil, err
	}
	tx, _, now, err := s.beginGenerationMutation(ctx, input.GenerationID, input.ClaimToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	retryAt := now.Add(time.Duration(postgresLeaseMicroseconds(input.RetryAfter)) * time.Microsecond)
	row := tx.QueryRow(ctx, fmt.Sprintf(`UPDATE %s.skill_generations AS g SET
		error_code = $2, error_message = $3, next_attempt_at = $4,
		updated_at = $5, claim_token = NULL, claim_owner = '', lease_expires_at = NULL
		WHERE g.id = $1 RETURNING %s`, quoteIdentifier(s.schema), generationColumns),
		input.GenerationID, input.Failure.Code, input.Failure.Message, retryAt, now)
	requeued, err := scanGeneration(row)
	if err != nil {
		return nil, fmt.Errorf("requeue generation: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit generation requeue: %w", err)
	}
	return requeued, nil
}

// GenerationQueueStats uses one database-clock snapshot and the exact claim
// eligibility predicate. Active, future, and terminal rows contribute neither
// depth nor lag.
func (s *PostgresStore) GenerationQueueStats(ctx context.Context) (GenerationQueueStats, error) {
	var (
		stats      GenerationQueueStats
		lagSeconds float64
	)
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`WITH queue_clock AS MATERIALIZED (
		SELECT clock_timestamp() AS now
	), claimable AS MATERIALIZED (
		SELECT generation.next_attempt_at, queue_clock.now
		FROM %s.skill_generations generation
		CROSS JOIN queue_clock
		WHERE generation.status IN ('queued', 'generating_candidates', 'evaluating_candidates', 'synthesizing')
		  AND generation.next_attempt_at <= queue_clock.now
		  AND (generation.lease_expires_at IS NULL OR generation.lease_expires_at <= queue_clock.now)
	)
	SELECT count(*)::bigint,
		COALESCE(GREATEST(0, EXTRACT(EPOCH FROM max(now) - min(next_attempt_at))), 0)::double precision
	FROM claimable`, quoteIdentifier(s.schema))).Scan(&stats.Depth, &lagSeconds)
	if err != nil {
		return GenerationQueueStats{}, fmt.Errorf("read generation queue statistics: %w", err)
	}
	if lagSeconds > 0 {
		stats.Lag = time.Duration(lagSeconds * float64(time.Second))
	}
	return stats, nil
}

func (s *PostgresStore) GetClaimedGeneration(ctx context.Context, generationID, claimToken string) (*GenerationState, error) {
	row := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM %s.skill_generations g
		WHERE g.id = $1 AND g.claim_token = $2
		  AND g.lease_expires_at > clock_timestamp()
		  AND g.status IN ('queued', 'generating_candidates', 'evaluating_candidates', 'synthesizing')`,
		generationColumns, quoteIdentifier(s.schema)), generationID, claimToken)
	generation, err := scanGeneration(row)
	if errors.Is(err, pgx.ErrNoRows) {
		canceled, canceledErr := s.generationWasCanceled(ctx, generationID)
		if canceledErr != nil {
			return nil, fmt.Errorf("resolve claimed generation: %w", canceledErr)
		}
		if canceled {
			return nil, ErrGenerationCanceled
		}
		return nil, ErrGenerationClaimLost
	}
	if err != nil {
		return nil, fmt.Errorf("get claimed generation: %w", err)
	}
	return s.loadGenerationState(ctx, s.pool, *generation)
}

func (s *PostgresStore) generationClaimTerminalError(ctx context.Context, generationID, claimToken string) (error, error) {
	var (
		status           GenerationStatus
		resultClaimToken string
	)
	if err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT status, COALESCE(result_claim_token::text, '')
		FROM %s.skill_generations WHERE id = $1`, quoteIdentifier(s.schema)), generationID).
		Scan(&status, &resultClaimToken); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if status == GenerationStatusCanceled {
		return ErrGenerationCanceled, nil
	}
	if status == GenerationStatusCompleted && claimToken != "" && resultClaimToken == claimToken {
		return ErrGenerationCompleted, nil
	}
	return nil, nil
}

func (s *PostgresStore) generationWasCanceled(ctx context.Context, generationID string) (bool, error) {
	var canceled bool
	if err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT status = 'canceled'
		FROM %s.skill_generations WHERE id = $1`, quoteIdentifier(s.schema)), generationID).Scan(&canceled); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return canceled, nil
}

// beginGenerationMutation locks the queue row before consulting the current
// token, lifecycle state, and lease. Database time is sampled only after the
// lock is acquired, so cancellation or reclaim either commits before the
// validation or waits until this mutation commits.
func (s *PostgresStore) beginGenerationMutation(ctx context.Context, generationID, claimToken string) (pgx.Tx, GenerationStatus, time.Time, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, "", time.Time{}, fmt.Errorf("begin generation mutation: %w", err)
	}
	var (
		storedToken    string
		status         GenerationStatus
		leaseExpiresAt *time.Time
	)
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(claim_token::text, ''),
		status, lease_expires_at FROM %s.skill_generations
		WHERE id = $1 FOR UPDATE`, quoteIdentifier(s.schema)), generationID).
		Scan(&storedToken, &status, &leaseExpiresAt); err != nil {
		_ = tx.Rollback(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", time.Time{}, ErrGenerationClaimLost
		}
		return nil, "", time.Time{}, fmt.Errorf("lock generation mutation: %w", err)
	}
	var databaseNow time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		_ = tx.Rollback(ctx)
		return nil, "", time.Time{}, fmt.Errorf("read generation mutation clock: %w", err)
	}
	if status == GenerationStatusCanceled {
		_ = tx.Rollback(ctx)
		return nil, "", time.Time{}, ErrGenerationCanceled
	}
	if claimToken == "" || storedToken != claimToken || leaseExpiresAt == nil ||
		!leaseExpiresAt.After(databaseNow) || !generationIsExecutable(status) {
		_ = tx.Rollback(ctx)
		return nil, "", time.Time{}, ErrGenerationClaimLost
	}
	return tx, status, databaseNow.UTC(), nil
}

func (s *PostgresStore) UpdateGenerationStatus(ctx context.Context, generationID, claimToken string, from, to GenerationStatus) error {
	if !validGenerationStatusTransition(from, to) {
		return ErrInvalidGenerationState
	}
	tx, status, now, err := s.beginGenerationMutation(ctx, generationID, claimToken)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if status != from {
		return ErrInvalidGenerationState
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET
		status = $2, updated_at = $3 WHERE id = $1`, quoteIdentifier(s.schema)),
		generationID, to, now); err != nil {
		return fmt.Errorf("update generation status: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit generation status: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateGenerationSession(ctx context.Context, generationID, claimToken string, session GenerationSessionRecord) error {
	if err := validateGenerationArtifactOwner(session.GenerationID, generationID); err != nil {
		return err
	}
	tx, _, _, err := s.beginGenerationMutation(ctx, generationID, claimToken)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schema := quoteIdentifier(s.schema)
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT ordinal FROM %s.generation_sessions
		WHERE generation_id = $1 AND session_id = $2`, schema), generationID, session.SessionID).
		Scan(&session.Ordinal); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("generation session not found")
		}
		return fmt.Errorf("read generation session: %w", err)
	}
	session.GenerationID = generationID
	if err = validateGenerationSessionArtifact(session); err != nil {
		return err
	}
	if session.CandidateID != "" {
		candidate, candidateErr := scanGenerationCandidate(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s
			FROM %s.generation_candidates c WHERE c.generation_id = $1 AND c.id = $2`,
			generationCandidateColumns, schema), generationID, session.CandidateID))
		if candidateErr != nil || candidate.Kind != GenerationCandidateSession ||
			candidate.Ordinal != session.Ordinal || len(candidate.SourceSessionIDs) != 1 ||
			candidate.SourceSessionIDs[0] != session.SessionID {
			return errors.New("generation session candidate not found")
		}
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.generation_sessions SET
		status = $3, candidate_id = NULLIF($4, '')::uuid,
		diagnostic_code = $5, updated_at = clock_timestamp()
		WHERE generation_id = $1 AND session_id = $2`, schema), generationID,
		session.SessionID, session.Status, session.CandidateID, session.DiagnosticCode); err != nil {
		return fmt.Errorf("update generation session: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit generation session: %w", err)
	}
	return nil
}

func (s *PostgresStore) PutGenerationCandidate(ctx context.Context, generationID, claimToken string, candidate GenerationCandidateRecord) (*GenerationCandidateRecord, error) {
	if err := validateGenerationArtifactOwner(candidate.GenerationID, generationID); err != nil {
		return nil, err
	}
	tx, _, _, err := s.beginGenerationMutation(ctx, generationID, claimToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	candidate, err = normalizeGenerationCandidateArtifact(candidate, false)
	if err != nil {
		return nil, err
	}
	candidate.GenerationID = generationID
	schema := quoteIdentifier(s.schema)
	generation, err := scanGeneration(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_generations g WHERE g.id = $1`, generationColumns, schema), generationID))
	if err != nil {
		return nil, fmt.Errorf("read generation candidate owner: %w", err)
	}
	sessions, err := loadGenerationSessions(ctx, tx, schema, generationID)
	if err != nil {
		return nil, err
	}
	candidates, err := loadGenerationCandidates(ctx, tx, schema, generationID)
	if err != nil {
		return nil, err
	}
	for index := range candidates {
		candidates[index], err = normalizeGenerationCandidateArtifact(candidates[index], true)
		if err != nil {
			return nil, ErrInvalidGenerationState
		}
	}
	for _, existing := range candidates {
		if existing.ID != candidate.ID && existing.Ordinal != candidate.Ordinal {
			continue
		}
		if sameGenerationCandidateArtifact(existing, candidate) {
			if err = tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit idempotent generation candidate: %w", err)
			}
			stored := cloneGenerationCandidate(existing)
			return &stored, nil
		}
		return nil, errors.New("store generation candidate: conflicting idempotent artifact")
	}
	if err = validateGenerationCandidatePlacement(*generation, sessions, candidates, candidate); err != nil {
		return nil, err
	}
	row := tx.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %s.generation_candidates AS c (
		id, generation_id, ordinal, kind, source_session_ids, name,
		description, type, tags, content, is_ai_generated, insights,
		bundle_sha256, created_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
		$12::jsonb, $13, clock_timestamp()) RETURNING %s`, schema, generationCandidateColumns),
		candidate.ID, generationID, candidate.Ordinal, candidate.Kind,
		nonNilStrings(candidate.SourceSessionIDs), candidate.Snapshot.Name,
		candidate.Snapshot.Description, candidate.Snapshot.Type,
		nonNilStrings(candidate.Snapshot.Tags), candidate.Snapshot.Content,
		candidate.Snapshot.IsAIGenerated, candidate.Insights, candidate.BundleSHA256)
	stored, err := scanGenerationCandidate(row)
	if err != nil {
		return nil, fmt.Errorf("store generation candidate: %w", err)
	}
	canonicalStored, err := normalizeGenerationCandidateArtifact(*stored, true)
	if err != nil {
		return nil, fmt.Errorf("store generation candidate: %w", err)
	}
	stored = &canonicalStored
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit generation candidate: %w", err)
	}
	return stored, nil
}

func (s *PostgresStore) PutCandidateEvaluation(ctx context.Context, generationID, claimToken string, evaluation CandidateEvaluationRecord) (*CandidateEvaluationRecord, error) {
	if err := validateGenerationArtifactOwner(evaluation.GenerationID, generationID); err != nil {
		return nil, err
	}
	tx, _, _, err := s.beginGenerationMutation(ctx, generationID, claimToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	evaluation, err = canonicalCandidateEvaluationDetails(evaluation)
	if err != nil {
		return nil, fmt.Errorf("store candidate evaluation: %w", err)
	}
	var expectedProfile, expectedProfileVersion string
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT evaluator_profile, evaluator_profile_version
		FROM %s.skill_generations WHERE id = $1`, quoteIdentifier(s.schema)), generationID).
		Scan(&expectedProfile, &expectedProfileVersion); err != nil {
		return nil, fmt.Errorf("read generation evaluation identity: %w", err)
	}
	if evaluation.Profile == "" || evaluation.ProfileVersion == "" ||
		evaluation.Profile != expectedProfile || evaluation.ProfileVersion != expectedProfileVersion {
		return nil, ErrInvalidGenerationState
	}
	row := tx.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %s.candidate_evaluations AS e (
		id, generation_id, candidate_id, request_sha256, profile,
		profile_version, evaluator_version, score, decision,
		critical_finding_count, warning_finding_count, criterion_results,
		findings, strengths, panel, created_at
	)
	SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
		$12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, clock_timestamp()
	WHERE EXISTS (
		SELECT 1 FROM %s.generation_candidates candidate
		WHERE candidate.generation_id = $2 AND candidate.id = $3
	)
	ON CONFLICT (candidate_id, request_sha256) DO UPDATE SET id = e.id
	RETURNING %s`, quoteIdentifier(s.schema), quoteIdentifier(s.schema),
		candidateEvaluationColumns), evaluation.ID, generationID,
		evaluation.CandidateID, evaluation.RequestSHA256, evaluation.Profile,
		evaluation.ProfileVersion, evaluation.EvaluatorVersion, evaluation.Score,
		evaluation.Decision, evaluation.CriticalFindingCount,
		evaluation.WarningFindingCount, evaluation.CriterionResults,
		evaluation.Findings, evaluation.Strengths, evaluation.Panel)
	stored, err := scanCandidateEvaluation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errors.New("store candidate evaluation: candidate not found")
	}
	if err != nil {
		return nil, fmt.Errorf("store candidate evaluation: %w", err)
	}
	canonicalStored, err := canonicalCandidateEvaluationDetails(*stored)
	if err != nil {
		return nil, ErrInvalidGenerationState
	}
	stored = &canonicalStored
	if stored.Profile != expectedProfile || stored.ProfileVersion != expectedProfileVersion ||
		stored.Profile == "" || stored.ProfileVersion == "" {
		return nil, ErrInvalidGenerationState
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit candidate evaluation: %w", err)
	}
	return stored, nil
}

func (s *PostgresStore) PutGenerationDiagnostic(ctx context.Context, generationID, claimToken string, diagnostic GenerationDiagnosticRecord) (*GenerationDiagnosticRecord, error) {
	if err := validateGenerationArtifactOwner(diagnostic.GenerationID, generationID); err != nil {
		return nil, err
	}
	if err := validateGenerationDiagnostic(diagnostic); err != nil {
		return nil, err
	}
	tx, _, now, err := s.beginGenerationMutation(ctx, generationID, claimToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schema := quoteIdentifier(s.schema)

	var referencesValid bool
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT
		($2 = '' OR EXISTS (
			SELECT 1 FROM %s.generation_sessions session
			WHERE session.generation_id = $1 AND session.session_id = $2
		)) AND
		($3 = '' OR EXISTS (
			SELECT 1 FROM %s.generation_candidates candidate
			WHERE candidate.generation_id = $1 AND candidate.id = $3::uuid
		))`, schema, schema), generationID, diagnostic.SessionID,
		diagnostic.CandidateID).Scan(&referencesValid); err != nil {
		return nil, fmt.Errorf("validate generation diagnostic references: %w", err)
	}
	if !referencesValid {
		return nil, errors.New("store generation diagnostic: session or candidate not found")
	}

	var existing GenerationDiagnosticRecord
	existingErr := tx.QueryRow(ctx, fmt.Sprintf(`SELECT id::text, generation_id::text,
		session_id, COALESCE(candidate_id::text, ''), stage, code, message,
		retryable, created_at FROM %s.generation_diagnostics WHERE id = $1`, schema), diagnostic.ID).
		Scan(&existing.ID, &existing.GenerationID, &existing.SessionID,
			&existing.CandidateID, &existing.Stage, &existing.Code, &existing.Message,
			&existing.Retryable, &existing.CreatedAt)
	if existingErr == nil {
		existing.CreatedAt = existing.CreatedAt.UTC()
		if sameGenerationDiagnosticArtifact(existing, generationID, diagnostic) {
			if err = tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit idempotent generation diagnostic: %w", err)
			}
			return &existing, nil
		}
		if existing.GenerationID != generationID ||
			generationDiagnosticSlot(existing) != generationDiagnosticSlot(diagnostic) {
			return nil, errors.New("store generation diagnostic: conflicting idempotent artifact")
		}
	} else if !errors.Is(existingErr, pgx.ErrNoRows) {
		return nil, fmt.Errorf("find generation diagnostic retry: %w", existingErr)
	}

	// Replace the single bounded logical source+stage slot. The generation row
	// lock held by beginGenerationMutation serializes this delete/insert pair.
	if _, err = tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.generation_diagnostics d
		WHERE d.generation_id = $1 AND d.stage = $4 AND (
			($4 IN ('transcript', 'candidate') AND d.session_id = $2) OR
			($4 = 'evaluation' AND d.candidate_id = NULLIF($3, '')::uuid) OR
			$4 = 'synthesis'
		)`, schema), generationID, diagnostic.SessionID, diagnostic.CandidateID,
		diagnostic.Stage); err != nil {
		return nil, fmt.Errorf("replace generation diagnostic slot: %w", err)
	}

	row := tx.QueryRow(ctx, fmt.Sprintf(`INSERT INTO %s.generation_diagnostics (
		id, generation_id, session_id, candidate_id, stage, code, message,
		retryable, created_at
	) VALUES ($1, $2, $3, NULLIF($4, '')::uuid, $5, $6, $7, $8, $9)
	RETURNING id::text, generation_id::text, session_id,
		COALESCE(candidate_id::text, ''), stage, code, message, retryable, created_at`,
		schema), diagnostic.ID, generationID, diagnostic.SessionID,
		diagnostic.CandidateID, diagnostic.Stage, diagnostic.Code,
		diagnostic.Message, diagnostic.Retryable, now)
	var stored GenerationDiagnosticRecord
	if err = row.Scan(&stored.ID, &stored.GenerationID, &stored.SessionID,
		&stored.CandidateID, &stored.Stage, &stored.Code, &stored.Message,
		&stored.Retryable, &stored.CreatedAt); err != nil {
		if isUniqueViolation(err) {
			return nil, errors.New("store generation diagnostic: conflicting idempotent artifact")
		}
		return nil, fmt.Errorf("store generation diagnostic: %w", err)
	}
	stored.CreatedAt = stored.CreatedAt.UTC()

	// Defensive migration-era pruning keeps physical retention bounded even if
	// predecessor rows did not use logical slots. New finite-catalog writes fit
	// in 303 slots without pruning.
	if _, err = tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.generation_diagnostics
		WHERE id IN (
			SELECT id FROM %s.generation_diagnostics
			WHERE generation_id = $1
			ORDER BY (stage = 'synthesis') DESC, (NOT retryable) DESC,
				created_at DESC, id DESC
			OFFSET %d
		)`, schema, schema, maxGenerationStateDiagnostics), generationID); err != nil {
		return nil, fmt.Errorf("prune generation diagnostics: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit generation diagnostic: %w", err)
	}
	return &stored, nil
}

func (s *PostgresStore) CancelSkillGeneration(ctx context.Context, callerSubject, skillID, generationID string) (*GenerationState, error) {
	if !validUUID(skillID) || !validUUID(generationID) || callerSubject == "" {
		return nil, ErrGenerationNotFound
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin generation cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schema := quoteIdentifier(s.schema)
	generation, err := scanGeneration(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_generations g
		WHERE g.id = $1 AND g.skill_id = $2 AND g.creator_subject = $3
		FOR UPDATE`, generationColumns, schema), generationID, skillID, callerSubject))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrGenerationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock generation cancellation: %w", err)
	}
	if !generationIsExecutable(generation.Status) {
		return nil, ErrInvalidGenerationState
	}
	var canceledAt time.Time
	if err = tx.QueryRow(ctx, fmt.Sprintf(`WITH cancel_clock AS MATERIALIZED (
		SELECT clock_timestamp() AS now
	)
	UPDATE %s.skill_generations SET
		status = 'canceled', error_code = '', error_message = '',
		claim_token = NULL, claim_owner = '', lease_expires_at = NULL,
		next_attempt_at = cancel_clock.now,
		updated_at = cancel_clock.now, completed_at = cancel_clock.now
		FROM cancel_clock WHERE id = $1 RETURNING cancel_clock.now`, schema), generationID).Scan(&canceledAt); err != nil {
		return nil, fmt.Errorf("cancel generation: %w", err)
	}
	canceledAt = canceledAt.UTC()
	generation.Status = GenerationStatusCanceled
	generation.ErrorCode = ""
	generation.ErrorMessage = ""
	generation.ClaimToken = ""
	generation.ClaimOwner = ""
	generation.LeaseExpiresAt = nil
	generation.NextAttemptAt = canceledAt
	generation.UpdatedAt = canceledAt
	generation.CompletedAt = cloneTime(&canceledAt)
	state, err := s.loadGenerationState(ctx, tx, *generation)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit generation cancellation: %w", err)
	}
	return state, nil
}

func loadGenerationSessions(ctx context.Context, q postgresQuerier, schema, generationID string) ([]GenerationSessionRecord, error) {
	rows, err := q.Query(ctx, fmt.Sprintf(`SELECT generation_id::text, session_id,
		ordinal, status, COALESCE(candidate_id::text, ''), diagnostic_code, updated_at
		FROM %s.generation_sessions WHERE generation_id = $1 ORDER BY ordinal`, schema), generationID)
	if err != nil {
		return nil, fmt.Errorf("load generation sessions: %w", err)
	}
	defer rows.Close()
	var sessions []GenerationSessionRecord
	for rows.Next() {
		var session GenerationSessionRecord
		if err = rows.Scan(&session.GenerationID, &session.SessionID, &session.Ordinal,
			&session.Status, &session.CandidateID, &session.DiagnosticCode, &session.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan generation session: %w", err)
		}
		session.UpdatedAt = session.UpdatedAt.UTC()
		sessions = append(sessions, session)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate generation sessions: %w", err)
	}
	return sessions, nil
}

func loadGenerationCandidates(ctx context.Context, q postgresQuerier, schema, generationID string) ([]GenerationCandidateRecord, error) {
	rows, err := q.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s.generation_candidates c
		WHERE c.generation_id = $1 ORDER BY c.ordinal, c.id`, generationCandidateColumns, schema), generationID)
	if err != nil {
		return nil, fmt.Errorf("load generation candidates: %w", err)
	}
	defer rows.Close()
	var candidates []GenerationCandidateRecord
	for rows.Next() {
		candidate, scanErr := scanGenerationCandidate(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		candidates = append(candidates, *candidate)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate generation candidates: %w", err)
	}
	return candidates, nil
}

func (s *PostgresStore) loadGenerationState(ctx context.Context, q postgresQuerier, generation SkillGenerationRecord) (*GenerationState, error) {
	state := &GenerationState{Generation: generation}
	schema := quoteIdentifier(s.schema)
	rows, err := q.Query(ctx, fmt.Sprintf(`SELECT generation_id::text, session_id,
		ordinal, status, COALESCE(candidate_id::text, ''), diagnostic_code, updated_at
		FROM %s.generation_sessions WHERE generation_id = $1
		ORDER BY ordinal LIMIT %d`, schema, maxGenerationStateSessions), generation.ID)
	if err != nil {
		return nil, fmt.Errorf("load generation sessions: %w", err)
	}
	for rows.Next() {
		var session GenerationSessionRecord
		if err = rows.Scan(&session.GenerationID, &session.SessionID, &session.Ordinal,
			&session.Status, &session.CandidateID, &session.DiagnosticCode,
			&session.UpdatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan generation session: %w", err)
		}
		session.UpdatedAt = session.UpdatedAt.UTC()
		state.Sessions = append(state.Sessions, session)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate generation sessions: %w", err)
	}
	rows.Close()

	rows, err = q.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s.generation_candidates c
		WHERE c.generation_id = $1 ORDER BY c.ordinal LIMIT %d`,
		generationCandidateColumns, schema, maxGenerationStateCandidates), generation.ID)
	if err != nil {
		return nil, fmt.Errorf("load generation candidates: %w", err)
	}
	for rows.Next() {
		candidate, scanErr := scanGenerationCandidate(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		state.Candidates = append(state.Candidates, *candidate)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate generation candidates: %w", err)
	}
	rows.Close()

	rows, err = q.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s.candidate_evaluations e
		WHERE e.generation_id = $1 ORDER BY e.created_at, e.id LIMIT %d`,
		candidateEvaluationColumns, schema, maxGenerationStateEvaluations), generation.ID)
	if err != nil {
		return nil, fmt.Errorf("load candidate evaluations: %w", err)
	}
	for rows.Next() {
		evaluation, scanErr := scanCandidateEvaluation(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		// Legacy schemas may contain opaque evaluator panels. Never materialize
		// them into generation history; new writes persist the same empty value.
		evaluation.Panel = json.RawMessage(`{}`)
		state.Evaluations = append(state.Evaluations, *evaluation)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate candidate evaluations: %w", err)
	}
	rows.Close()

	rows, err = q.Query(ctx, fmt.Sprintf(`SELECT id::text, generation_id::text,
		session_id, COALESCE(candidate_id::text, ''), stage, code, message,
		retryable, created_at FROM (
			SELECT * FROM %s.generation_diagnostics
			WHERE generation_id = $1
			ORDER BY (stage = 'synthesis') DESC, (NOT retryable) DESC,
				created_at DESC, id DESC LIMIT %d
		) retained ORDER BY created_at DESC, id DESC`,
		schema, maxGenerationStateDiagnostics), generation.ID)
	if err != nil {
		return nil, fmt.Errorf("load generation diagnostics: %w", err)
	}
	for rows.Next() {
		var diagnostic GenerationDiagnosticRecord
		if err = rows.Scan(&diagnostic.ID, &diagnostic.GenerationID,
			&diagnostic.SessionID, &diagnostic.CandidateID, &diagnostic.Stage,
			&diagnostic.Code, &diagnostic.Message, &diagnostic.Retryable,
			&diagnostic.CreatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan generation diagnostic: %w", err)
		}
		diagnostic.CreatedAt = diagnostic.CreatedAt.UTC()
		state.Diagnostics = append(state.Diagnostics, diagnostic)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate generation diagnostics: %w", err)
	}
	rows.Close()
	sanitizeGenerationStateArtifacts(state)
	state.Sessions = nonNilGenerationSessions(state.Sessions)
	state.Candidates = nonNilGenerationCandidates(state.Candidates)
	state.Evaluations = nonNilCandidateEvaluations(state.Evaluations)
	state.Diagnostics = nonNilGenerationDiagnostics(state.Diagnostics)
	return state, nil
}

func scanGenerationSummary(row interface{ Scan(...any) error }) (*SkillGenerationSummaryRecord, error) {
	var summary SkillGenerationSummaryRecord
	if err := row.Scan(&summary.ID, &summary.SkillID, &summary.BaseRevisionID,
		&summary.Status, &summary.ResultRevisionID, &summary.ErrorCode,
		&summary.ErrorMessage, &summary.AttemptCount, &summary.CreatedAt,
		&summary.UpdatedAt, &summary.StartedAt, &summary.CompletedAt); err != nil {
		return nil, fmt.Errorf("scan generation summary: %w", err)
	}
	summary.CreatedAt = summary.CreatedAt.UTC()
	summary.UpdatedAt = summary.UpdatedAt.UTC()
	return &summary, nil
}

func scanGeneration(row interface{ Scan(...any) error }) (*SkillGenerationRecord, error) {
	var generation SkillGenerationRecord
	var criteria json.RawMessage
	if err := row.Scan(
		&generation.ID, &generation.SkillID, &generation.BaseRevisionID,
		&generation.CreatorSubject, &generation.Status,
		&generation.Snapshot.Name, &generation.Snapshot.Description,
		&generation.Snapshot.Type, &generation.Snapshot.Tags,
		&generation.Snapshot.Content, &generation.Snapshot.IsAIGenerated,
		&generation.Snapshot.SourceSessionIDs, &generation.AuthorContext,
		&generation.SelectedSessionIDs, &generation.EvaluatorProfile,
		&generation.EvaluatorProfileVersion, &criteria,
		&generation.WinnerCandidateID, &generation.ResultCandidateID,
		&generation.ResultRevisionID, &generation.ErrorCode, &generation.ErrorMessage,
		&generation.ClaimToken, &generation.ClaimOwner, &generation.LeaseExpiresAt,
		&generation.AttemptCount, &generation.NextAttemptAt,
		&generation.LastHeartbeatAt, &generation.CreatedAt, &generation.UpdatedAt,
		&generation.StartedAt, &generation.CompletedAt,
	); err != nil {
		return nil, fmt.Errorf("scan generation: %w", err)
	}
	generation.Snapshot = canonicalSkillRevisionSnapshot(generation.Snapshot)
	generation.EvaluationCriteria = cloneRawJSON(criteria)
	generation.NextAttemptAt = generation.NextAttemptAt.UTC()
	generation.CreatedAt = generation.CreatedAt.UTC()
	generation.UpdatedAt = generation.UpdatedAt.UTC()
	return &generation, nil
}

func scanGenerationCandidate(row interface{ Scan(...any) error }) (*GenerationCandidateRecord, error) {
	var candidate GenerationCandidateRecord
	if err := row.Scan(&candidate.ID, &candidate.GenerationID, &candidate.Ordinal,
		&candidate.Kind, &candidate.SourceSessionIDs, &candidate.Snapshot.Name,
		&candidate.Snapshot.Description, &candidate.Snapshot.Type,
		&candidate.Snapshot.Tags, &candidate.Snapshot.Content,
		&candidate.Snapshot.IsAIGenerated, &candidate.Insights,
		&candidate.BundleSHA256, &candidate.CreatedAt); err != nil {
		return nil, fmt.Errorf("scan generation candidate: %w", err)
	}
	candidate.SourceSessionIDs = cloneStrings(candidate.SourceSessionIDs)
	candidate.Snapshot = canonicalGenerationCandidateSnapshot(candidate.Snapshot)
	candidate.CreatedAt = candidate.CreatedAt.UTC()
	return &candidate, nil
}

func scanCandidateEvaluation(row interface{ Scan(...any) error }) (*CandidateEvaluationRecord, error) {
	var evaluation CandidateEvaluationRecord
	if err := row.Scan(&evaluation.ID, &evaluation.GenerationID,
		&evaluation.CandidateID, &evaluation.RequestSHA256, &evaluation.Profile,
		&evaluation.ProfileVersion, &evaluation.EvaluatorVersion, &evaluation.Score,
		&evaluation.Decision, &evaluation.CriticalFindingCount,
		&evaluation.WarningFindingCount, &evaluation.CriterionResults,
		&evaluation.Findings, &evaluation.Strengths, &evaluation.Panel,
		&evaluation.CreatedAt); err != nil {
		return nil, fmt.Errorf("scan candidate evaluation: %w", err)
	}
	evaluation.CreatedAt = evaluation.CreatedAt.UTC()
	return &evaluation, nil
}

// AppendPrivateGenerationResult locks the generation, validates its live lease
// against database time, and commits the private revision and terminal state as
// one transaction. ResultClaimToken is retained privately so an exact retry can
// return the committed revision while an older reclaimed token remains fenced.
func (s *PostgresStore) AppendPrivateGenerationResult(ctx context.Context, input AppendPrivateGenerationResultInput) (*SkillRevisionRecord, error) {
	if !validUUID(input.GenerationID) || !validUUID(input.ClaimToken) {
		return nil, ErrGenerationClaimLost
	}
	if !validUUID(input.InitialWinnerCandidateID) || !validUUID(input.ResultCandidateID) {
		return nil, ErrInvalidGenerationState
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin generation result append: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	schema := quoteIdentifier(s.schema)
	generation, err := scanGeneration(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_generations g WHERE g.id = $1 FOR UPDATE`, generationColumns, schema), input.GenerationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrGenerationClaimLost
	}
	if err != nil {
		return nil, fmt.Errorf("lock generation result append: %w", err)
	}
	var resultClaimToken string
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT COALESCE(result_claim_token::text, '')
		FROM %s.skill_generations WHERE id = $1`, schema), generation.ID).Scan(&resultClaimToken); err != nil {
		return nil, fmt.Errorf("read generation result claim: %w", err)
	}

	if generation.Status == GenerationStatusCanceled {
		return nil, ErrGenerationCanceled
	}
	if generation.Status == GenerationStatusCompleted {
		if resultClaimToken != input.ClaimToken {
			return nil, ErrGenerationClaimLost
		}
		if generation.WinnerCandidateID != input.InitialWinnerCandidateID ||
			generation.ResultCandidateID != input.ResultCandidateID || generation.ResultRevisionID == "" {
			return nil, ErrInvalidGenerationState
		}
		initialWinner, resultCandidate, candidateErr := loadEvaluatedGenerationResultCandidates(
			ctx, tx, schema, *generation, input.InitialWinnerCandidateID, input.ResultCandidateID,
		)
		if candidateErr != nil {
			return nil, candidateErr
		}
		revision, revisionErr := loadExactGenerationResult(ctx, tx, schema, *generation, resultCandidate)
		if revisionErr != nil {
			return nil, revisionErr
		}
		if initialWinner.ID != generation.WinnerCandidateID {
			return nil, ErrInvalidGenerationState
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit generation result retry: %w", err)
		}
		return revision, nil
	}

	var databaseNow time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return nil, fmt.Errorf("read generation result clock: %w", err)
	}
	if generation.ClaimToken != input.ClaimToken || generation.LeaseExpiresAt == nil ||
		!generation.LeaseExpiresAt.After(databaseNow) || !generationIsExecutable(generation.Status) {
		return nil, ErrGenerationClaimLost
	}
	if generation.Status != GenerationStatusSynthesizing {
		return nil, ErrInvalidGenerationState
	}

	// Lock the stable skill before the optional base/visibility rows. Metadata
	// mutations use the same order, so access cannot change between this check,
	// sequence allocation, and result append.
	var lockedSkillID string
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT id::text FROM %s.skills
		WHERE id = $1 AND migration_alias_of_skill_id IS NULL
		FOR UPDATE`, schema), generation.SkillID).Scan(&lockedSkillID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSkillNotFound
		}
		return nil, fmt.Errorf("lock generation result skill: %w", err)
	}
	if generation.BaseRevisionID != "" {
		var accessibleBaseRevisionID string
		if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT base.id::text
			FROM %s.skill_revisions base
			JOIN %s.skill_revision_visibility visibility ON visibility.revision_id = base.id
			WHERE base.id = $1 AND base.skill_id = $2
			  AND (visibility.is_public OR base.creator_subject = $3)
			FOR SHARE OF base, visibility`, schema, schema), generation.BaseRevisionID,
			generation.SkillID, generation.CreatorSubject).Scan(&accessibleBaseRevisionID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrRevisionNotFound
			}
			return nil, fmt.Errorf("recheck generation base revision access: %w", err)
		}
	}
	_, resultCandidate, err := loadEvaluatedGenerationResultCandidates(
		ctx, tx, schema, *generation, input.InitialWinnerCandidateID, input.ResultCandidateID,
	)
	if err != nil {
		return nil, err
	}

	revisionID := generationResultRevisionID(generation.ID)
	var identityExists bool
	if err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (
		SELECT 1 FROM %s.skill_revisions revision
		WHERE revision.id = $1 OR revision.generation_id = $2
		   OR (revision.skill_id = $3 AND revision.idempotency_key = $4)
	)`, schema), revisionID, generation.ID, generation.SkillID,
		"generation:"+generation.ID).Scan(&identityExists); err != nil {
		return nil, fmt.Errorf("inspect generation result identity: %w", err)
	}
	if identityExists {
		return nil, ErrRevisionConflict
	}

	snapshot, err := normalizeSkillRevisionSnapshot(generationCandidateRevisionSnapshot(resultCandidate))
	if err != nil {
		return nil, fmt.Errorf("append generation result: %w", err)
	}
	var sequence int
	if err = tx.QueryRow(ctx, fmt.Sprintf(`UPDATE %s.skills SET
		next_sequence_number = next_sequence_number + 1,
		updated_at = GREATEST(updated_at, $2)
		WHERE id = $1 AND migration_alias_of_skill_id IS NULL
		RETURNING next_sequence_number - 1`, schema), generation.SkillID, databaseNow).Scan(&sequence); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrSkillNotFound
		}
		return nil, fmt.Errorf("allocate generation result sequence: %w", err)
	}
	revision := SkillRevisionRecord{
		ID: revisionID, SkillID: generation.SkillID, SequenceNumber: sequence,
		Version: fmt.Sprintf("%d", sequence), CreatorSubject: generation.CreatorSubject,
		BasedOnRevisionID: generation.BaseRevisionID, Origin: RevisionOriginGeneration,
		Snapshot: snapshot, ContentSHA256: skillRevisionSnapshotSHA256(snapshot),
		ChangeNote: generationResultChangeNote, GenerationID: generation.ID,
		IdempotencyKey: "generation:" + generation.ID, CreatedAt: databaseNow.UTC(),
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_revisions (
		id, skill_id, sequence_number, version, creator_subject,
		based_on_revision_id, source_revision_id, origin, name, description,
		type, tags, content, is_ai_generated, source_session_ids,
		content_sha256, change_note, generation_id, idempotency_key,
		legacy_reference, created_at
	) VALUES (
		$1, $2, $3, $4, $5, NULLIF($6, '')::uuid, NULL, $7, $8, $9,
		$10, $11, $12, $13, $14, $15, $16, $17, $18, NULL, $19
	)`, schema), revision.ID, revision.SkillID, revision.SequenceNumber,
		revision.Version, revision.CreatorSubject, revision.BasedOnRevisionID,
		revision.Origin, revision.Snapshot.Name, revision.Snapshot.Description,
		revision.Snapshot.Type, nonNilStrings(revision.Snapshot.Tags),
		revision.Snapshot.Content, revision.Snapshot.IsAIGenerated,
		nonNilStrings(revision.Snapshot.SourceSessionIDs), revision.ContentSHA256,
		revision.ChangeNote, revision.GenerationID, revision.IdempotencyKey,
		revision.CreatedAt); err != nil {
		return nil, appendRevisionError(err)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_revision_visibility (
		revision_id, is_public, changed_by_subject, changed_at
	) VALUES ($1, FALSE, $2, $3)`, schema), revision.ID,
		revision.CreatorSubject, revision.CreatedAt); err != nil {
		return nil, fmt.Errorf("insert generation result visibility: %w", err)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.skill_generations SET
		status = 'completed', winner_candidate_id = $2,
		result_candidate_id = $3, result_revision_id = $4,
		result_claim_token = $5, claim_token = NULL, claim_owner = '',
		lease_expires_at = NULL, updated_at = $6, completed_at = $6
		WHERE id = $1`, schema), generation.ID, input.InitialWinnerCandidateID,
		input.ResultCandidateID, revision.ID, input.ClaimToken, revision.CreatedAt); err != nil {
		return nil, fmt.Errorf("complete generation result append: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit generation result append: %w", err)
	}
	return &revision, nil
}

func loadEvaluatedGenerationResultCandidates(
	ctx context.Context,
	tx pgx.Tx,
	schema string,
	generation SkillGenerationRecord,
	initialWinnerCandidateID, resultCandidateID string,
) (GenerationCandidateRecord, GenerationCandidateRecord, error) {
	if generation.EvaluatorProfile == "" || generation.EvaluatorProfileVersion == "" {
		return GenerationCandidateRecord{}, GenerationCandidateRecord{}, ErrInvalidGenerationState
	}
	load := func(candidateID string) (GenerationCandidateRecord, error) {
		candidate, err := scanGenerationCandidate(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s
			FROM %s.generation_candidates c
			WHERE c.generation_id = $1 AND c.id = $2 AND EXISTS (
				SELECT 1 FROM %s.candidate_evaluations evaluation
				WHERE evaluation.generation_id = c.generation_id
				  AND evaluation.candidate_id = c.id
				  AND evaluation.profile = $3
				  AND evaluation.profile_version = $4
			) AND NOT EXISTS (
				SELECT 1 FROM %s.candidate_evaluations evaluation
				WHERE evaluation.generation_id = c.generation_id
				  AND evaluation.candidate_id = c.id
				  AND (evaluation.profile <> $3 OR evaluation.profile_version <> $4
				    OR evaluation.profile = '' OR evaluation.profile_version = '')
			)`, generationCandidateColumns, schema, schema, schema), generation.ID, candidateID,
			generation.EvaluatorProfile, generation.EvaluatorProfileVersion))
		if errors.Is(err, pgx.ErrNoRows) {
			return GenerationCandidateRecord{}, errors.New("append generation result: matching evaluated candidate not found")
		}
		if err != nil {
			return GenerationCandidateRecord{}, fmt.Errorf("load evaluated generation result candidate: %w", err)
		}
		return *candidate, nil
	}
	initialWinner, err := load(initialWinnerCandidateID)
	if err != nil {
		return GenerationCandidateRecord{}, GenerationCandidateRecord{}, err
	}
	if initialWinner.Kind == GenerationCandidateSynthesis {
		return GenerationCandidateRecord{}, GenerationCandidateRecord{}, ErrInvalidGenerationState
	}
	if initialWinnerCandidateID == resultCandidateID {
		return initialWinner, initialWinner, nil
	}
	resultCandidate, err := load(resultCandidateID)
	if err != nil {
		return GenerationCandidateRecord{}, GenerationCandidateRecord{}, err
	}
	return initialWinner, resultCandidate, nil
}

func loadExactGenerationResult(
	ctx context.Context,
	tx pgx.Tx,
	schema string,
	generation SkillGenerationRecord,
	resultCandidate GenerationCandidateRecord,
) (*SkillRevisionRecord, error) {
	revision, err := scanSkillRevision(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s
		FROM %s.skill_revisions r
		WHERE r.id = $1 AND r.generation_id = $2 AND r.skill_id = $3
		  AND r.creator_subject = $4`,
		skillRevisionColumns, schema), generation.ResultRevisionID,
		generation.ID, generation.SkillID, generation.CreatorSubject))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrInvalidGenerationState
	}
	if err != nil {
		return nil, fmt.Errorf("load generation result revision: %w", err)
	}
	expected := AppendRevisionInput{
		ID: generationResultRevisionID(generation.ID), SkillID: generation.SkillID,
		CreatorSubject: generation.CreatorSubject, BasedOnRevisionID: generation.BaseRevisionID,
		Origin: RevisionOriginGeneration, Snapshot: generationCandidateRevisionSnapshot(resultCandidate),
		ChangeNote: generationResultChangeNote, GenerationID: generation.ID,
		IdempotencyKey: "generation:" + generation.ID,
	}
	if revision.ID != expected.ID || !revisionMatchesAppend(revision, expected) {
		return nil, ErrInvalidGenerationState
	}
	return &revision, nil
}

func nonNilGenerationSessions(value []GenerationSessionRecord) []GenerationSessionRecord {
	if value == nil {
		return []GenerationSessionRecord{}
	}
	return value
}

func nonNilGenerationCandidates(value []GenerationCandidateRecord) []GenerationCandidateRecord {
	if value == nil {
		return []GenerationCandidateRecord{}
	}
	return value
}

func nonNilCandidateEvaluations(value []CandidateEvaluationRecord) []CandidateEvaluationRecord {
	if value == nil {
		return []CandidateEvaluationRecord{}
	}
	return value
}

func nonNilGenerationDiagnostics(value []GenerationDiagnosticRecord) []GenerationDiagnosticRecord {
	if value == nil {
		return []GenerationDiagnosticRecord{}
	}
	return value
}
