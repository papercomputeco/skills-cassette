// Package storage owns the skills cassette's persistence: the skills and
// skill_versions tables in the cassette's own Postgres schema, plus an
// in-memory driver so the cassette runs (and its handlers test) without a
// database.
//
// The records mirror the shapes the Tapes skills API persisted before the
// cutover. There is no org_id: Tapes itself removed the organization concept
// (tapes#276, v0.30.1) and is dropping the storage columns behind it, so this
// schema is born without one. Tenancy is gateway-owned; a cassette process
// serves exactly one installation.
package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

// ErrSkillVersionConflict is returned by CreateSkillVersion when the
// (skill_id, version_number) pair already exists — i.e. a concurrent publish
// claimed the same number. Callers translate it into a retry rather than a 500.
var ErrSkillVersionConflict = errors.New("skill version number already exists")

// ErrSkillChanged means a conditional publish no longer matches the skill head.
var ErrSkillChanged = errors.New("skill content changed")

var (
	// ErrSkillNotFound means a stable skill identity does not exist.
	ErrSkillNotFound = errors.New("skill not found")
	// ErrRevisionNotFound deliberately covers absent revisions, revisions
	// outside the requested skill, and private revisions owned by another
	// subject. Callers must not disclose which condition failed.
	ErrRevisionNotFound = errors.New("skill revision not found")
	// ErrRevisionConflict means an append identity was already used for
	// different immutable content or lineage.
	ErrRevisionConflict = errors.New("skill revision conflict")
	// ErrRevisionSnapshotInvalid marks a runtime append whose complete snapshot
	// does not satisfy the shared bounded storage contract.
	ErrRevisionSnapshotInvalid = errors.New("skill revision snapshot is invalid")
	// ErrRevisionLineageInvalid means a base or source UUID is not valid for
	// the requested append.
	ErrRevisionLineageInvalid = errors.New("invalid skill revision lineage")
	// ErrRevisionNotPublic rejects selecting a private revision as explicit
	// latest. It remains distinct from not-found so callers can retry the
	// independent visibility mutation before moving latest.
	ErrRevisionNotPublic = errors.New("skill revision is not public")
	// ErrRevisionIsExplicitLatest rejects making the current explicit latest
	// revision private. Clearing or moving latest is an independent operation.
	ErrRevisionIsExplicitLatest = errors.New("skill revision is explicit latest")
	// ErrGenerationNotFound deliberately covers absent generations and
	// generations owned by another subject.
	ErrGenerationNotFound = errors.New("skill generation not found")
	// ErrGenerationClaimLost fences writes after lease expiry or reclaim.
	ErrGenerationClaimLost = errors.New("skill generation claim lost")
	// ErrGenerationCanceled distinguishes creator cancellation from lease loss
	// when a worker heartbeat or fenced write observes the terminal row.
	ErrGenerationCanceled = errors.New("skill generation was canceled")
	// ErrGenerationCompleted tells a concurrent heartbeat that its own claim
	// already committed the atomic result; it is neither cancellation nor lease
	// expiry/reclaim and must not increment claim-loss metrics.
	ErrGenerationCompleted = errors.New("skill generation was completed by this claim")
	// ErrInvalidGenerationState rejects lifecycle operations outside their
	// explicitly allowed source states.
	ErrInvalidGenerationState = errors.New("invalid skill generation state")
	// ErrGenerationResultAppendUnimplemented is retained as the predecessor
	// designer-phase sentinel used by migration-era test fixtures.
	ErrGenerationResultAppendUnimplemented = errors.New("private generation result append is not implemented")
)

// RevisionNotPublicError carries the safe revision identity associated with
// ErrRevisionNotPublic while preserving a stable errors.Is classification.
type RevisionNotPublicError struct {
	RevisionID string
}

func (e *RevisionNotPublicError) Error() string { return ErrRevisionNotPublic.Error() }
func (e *RevisionNotPublicError) Unwrap() error { return ErrRevisionNotPublic }

// RevisionIsExplicitLatestError carries the safe revision identity associated
// with ErrRevisionIsExplicitLatest while preserving a stable errors.Is
// classification.
type RevisionIsExplicitLatestError struct {
	RevisionID string
}

func (e *RevisionIsExplicitLatestError) Error() string { return ErrRevisionIsExplicitLatest.Error() }
func (e *RevisionIsExplicitLatestError) Unwrap() error { return ErrRevisionIsExplicitLatest }

const (
	// DefaultListLimit bounds a skills page when the caller supplies no limit.
	DefaultListLimit = 24
	// DefaultRevisionListLimit bounds a revision-history page when the caller
	// supplies no limit.
	DefaultRevisionListLimit = 24
	// MaxRevisionListLimit is the client-visible revision-history page cap.
	// Storage admits one additional row solely for bounded next-page lookahead.
	MaxRevisionListLimit        = 100
	maxRevisionListStorageLimit = MaxRevisionListLimit + 1

	// Revision snapshot limits are shared by both storage drivers. HTTP and
	// OpenAPI alias these values so direct manual and generated appends cannot
	// bypass the public contract.
	MaxRevisionNameCodePoints        = 1024
	MaxRevisionDescriptionCodePoints = 1 << 20
	MaxRevisionContentCodePoints     = 1 << 20
	MaxRevisionTags                  = 64
	MaxRevisionSourceSessionIDs      = 100
	MaxRevisionIdentityCodePoints    = 256
	MaxGenerationAuthorContextRunes  = 32 << 10

	// Generation candidate insights are a closed, canonical JSON array. Six
	// bytes is the largest encoding/json expansion of one legal rune (for
	// example '<' becomes "\\u003c"). The byte ceiling therefore admits the
	// exact maximum legal canonical array instead of silently dropping it at the
	// wire boundary.
	MaxGenerationCandidateInsights           = 16
	MaxGenerationCandidateInsightRunes       = 2048
	maxGenerationCandidateInsightRuneBytes   = 6
	maxGenerationCandidateInsightObjectBytes = len(`{"kind":"","summary":"","evidence":""}`) +
		3*MaxGenerationCandidateInsightRunes*maxGenerationCandidateInsightRuneBytes
	MaxGenerationCandidateInsightsJSONBytes = 2 + (MaxGenerationCandidateInsights - 1) +
		MaxGenerationCandidateInsights*maxGenerationCandidateInsightObjectBytes
)

// RevisionOrigin identifies the workflow that appended an immutable revision.
type RevisionOrigin string

const (
	RevisionOriginManual     RevisionOrigin = "manual"
	RevisionOriginGeneration RevisionOrigin = "generation"
	RevisionOriginDuplicate  RevisionOrigin = "duplicate"
	RevisionOriginMigrated   RevisionOrigin = "migrated"
)

// SkillRevisionSnapshot is the exact immutable content and source provenance
// stored by skill_revisions. Slug belongs to the stable SkillRecord identity;
// revision ancestry is expressed by revision UUIDs rather than the predecessor
// snapshot's parent ID.
type SkillRevisionSnapshot struct {
	Name             string
	Description      string
	Type             string
	Tags             []string
	Content          string
	IsAIGenerated    bool
	SourceSessionIDs []string
}

// canonicalSkillRevisionSnapshot makes the two collection fields explicit so
// equivalent complete snapshots never hash differently merely because a Go
// caller used nil instead of an empty slice.
func canonicalSkillRevisionSnapshot(snapshot SkillRevisionSnapshot) SkillRevisionSnapshot {
	snapshot.Tags = append([]string{}, snapshot.Tags...)
	snapshot.SourceSessionIDs = append([]string{}, snapshot.SourceSessionIDs...)
	return snapshot
}

// normalizeSkillRevisionSnapshot applies the same canonicalization and bounds
// as the HTTP revision contract. Migration paths and readers intentionally use
// only canonicalSkillRevisionSnapshot so historical rows remain byte-for-byte
// reachable even when they predate these runtime write constraints.
func normalizeSkillRevisionSnapshot(snapshot SkillRevisionSnapshot) (SkillRevisionSnapshot, error) {
	rawName := snapshot.Name
	rawDescription := snapshot.Description
	rawType := snapshot.Type
	snapshot.Name = strings.TrimSpace(rawName)
	snapshot.Description = strings.TrimSpace(rawDescription)
	snapshot.Type = strings.TrimSpace(rawType)
	if snapshot.Name == "" || !validSnapshotIdentity(rawName, MaxRevisionNameCodePoints) ||
		!validSnapshotIdentity(snapshot.Name, MaxRevisionNameCodePoints) {
		return SkillRevisionSnapshot{}, fmt.Errorf("%w: name is invalid or too long", ErrRevisionSnapshotInvalid)
	}
	if !validSnapshotText(rawDescription, MaxRevisionDescriptionCodePoints) ||
		!validSnapshotText(snapshot.Description, MaxRevisionDescriptionCodePoints) {
		return SkillRevisionSnapshot{}, fmt.Errorf("%w: description is invalid or too long", ErrRevisionSnapshotInvalid)
	}
	if !validSnapshotIdentity(rawType, MaxRevisionIdentityCodePoints) ||
		!validSnapshotIdentity(snapshot.Type, MaxRevisionIdentityCodePoints) {
		return SkillRevisionSnapshot{}, fmt.Errorf("%w: type is invalid", ErrRevisionSnapshotInvalid)
	}
	switch snapshot.Type {
	case "workflow", "domain-knowledge", "prompt-template":
	default:
		return SkillRevisionSnapshot{}, fmt.Errorf("%w: type is invalid", ErrRevisionSnapshotInvalid)
	}
	if !validSnapshotText(snapshot.Content, MaxRevisionContentCodePoints) {
		return SkillRevisionSnapshot{}, fmt.Errorf("%w: content is invalid or too long", ErrRevisionSnapshotInvalid)
	}
	var err error
	if snapshot.Tags, err = normalizedSnapshotIdentities(snapshot.Tags, MaxRevisionTags, "tags"); err != nil {
		return SkillRevisionSnapshot{}, err
	}
	if snapshot.SourceSessionIDs, err = normalizedSnapshotIdentities(
		snapshot.SourceSessionIDs, MaxRevisionSourceSessionIDs, "source session ids",
	); err != nil {
		return SkillRevisionSnapshot{}, err
	}
	return canonicalSkillRevisionSnapshot(snapshot), nil
}

func normalizeAppendRevisionSnapshot(origin RevisionOrigin, snapshot SkillRevisionSnapshot) (SkillRevisionSnapshot, error) {
	switch origin {
	case RevisionOriginManual, RevisionOriginGeneration, RevisionOriginDuplicate:
		return normalizeSkillRevisionSnapshot(snapshot)
	case RevisionOriginMigrated:
		// Migration origins preserve complete predecessor snapshots,
		// including historical values that predate the runtime write bounds.
		return canonicalSkillRevisionSnapshot(snapshot), nil
	default:
		return SkillRevisionSnapshot{}, errors.New("invalid revision origin")
	}
}

func normalizedSnapshotIdentities(values []string, maximum int, field string) ([]string, error) {
	if len(values) > maximum {
		return nil, fmt.Errorf("%w: %s contain too many values", ErrRevisionSnapshotInvalid, field)
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" || !validSnapshotIdentity(raw, MaxRevisionIdentityCodePoints) ||
			!validSnapshotIdentity(value, MaxRevisionIdentityCodePoints) {
			return nil, fmt.Errorf("%w: %s contain an invalid value", ErrRevisionSnapshotInvalid, field)
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("%w: %s contain duplicate values", ErrRevisionSnapshotInvalid, field)
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result, nil
}

func validSnapshotText(value string, maximum int) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > maximum {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) && character != '\n' && character != '\r' && character != '\t' {
			return false
		}
	}
	return true
}

func validSnapshotIdentity(value string, maximum int) bool {
	if !validSnapshotText(value, maximum) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

// skillRevisionSnapshotSHA256 hashes the canonical JSON representation of the
// complete immutable snapshot. The concrete struct (rather than a map) fixes
// field order across writers and migration retries.
func skillRevisionSnapshotSHA256(snapshot SkillRevisionSnapshot) string {
	encoded, _ := json.Marshal(canonicalSkillRevisionSnapshot(snapshot))
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// predecessorSkillSnapshot is the complete snapshot shape read only from the
// predecessor skill, version, draft, and migration-source tables. It remains
// unexported so removed draft APIs cannot become a runtime capability again.
type predecessorSkillSnapshot struct {
	Slug             string
	Name             string
	Description      string
	Type             string
	Tags             []string
	Content          string
	IsAIGenerated    bool
	SourceSessionIDs []string
	ParentID         string
}

func canonicalPredecessorSkillSnapshot(snapshot predecessorSkillSnapshot) predecessorSkillSnapshot {
	snapshot.Tags = append([]string{}, snapshot.Tags...)
	snapshot.SourceSessionIDs = append([]string{}, snapshot.SourceSessionIDs...)
	return snapshot
}

func predecessorSkillSnapshotSHA256(snapshot predecessorSkillSnapshot) string {
	encoded, _ := json.Marshal(canonicalPredecessorSkillSnapshot(snapshot))
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// SkillRevisionRecord is one immutable, complete skill revision. ID is the
// canonical identity for every durable relationship; SequenceNumber orders
// revisions only within SkillID, and Version is presentation metadata.
type SkillRevisionRecord struct {
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
	// LegacyReference is a migration-only canonical key for an old composite
	// skill-version, draft-revision, or mutable-head identity. Empty means the
	// revision was created in the unified model.
	LegacyReference string
	CreatedAt       time.Time
}

// RevisionVisibilityRecord is mutable access metadata stored separately from
// immutable revision content and provenance.
type RevisionVisibilityRecord struct {
	RevisionID       string
	IsPublic         bool
	ChangedBySubject string
	ChangedAt        time.Time
	// Changed and PreviousIsPublic describe the authoritative mutation decided
	// while storage held its metadata lock. They are not persisted or exposed on
	// the wire; metrics use them instead of racing a pre-read.
	Changed          bool
	PreviousIsPublic bool
}

// AccessibleRevisionRecord is the storage projection returned only after the
// public-or-creator predicate has been applied. Keeping visibility nested
// prevents mutable metadata from becoming part of SkillRevisionRecord.
type AccessibleRevisionRecord struct {
	Revision   SkillRevisionRecord
	Visibility RevisionVisibilityRecord
	// IsExplicitLatest is true only when this revision equals the skill's
	// stored explicit-latest pointer. Selecting the newest public revision as
	// the effective fallback never sets this field.
	IsExplicitLatest bool
}

// ResolveSkillInput contains the identity attributes committed when a
// normalized slug is first observed. ID is supplied by the caller so retries
// can retain identity; a slug conflict resolves to the existing SkillRecord.
type ResolveSkillInput struct {
	ID             string
	Slug           string
	CreatorSubject string
	CreatedAt      time.Time
}

// AppendRevisionInput contains one complete immutable append. SequenceNumber
// and Version are deliberately absent: storage allocates both atomically with
// the insert. Visibility is also absent because every append starts private.
// BasedOnRevisionID, when present, must name an accessible revision of SkillID.
// SourceRevisionID, when present, must name an accessible revision of another
// skill and records duplicate/fork lineage rather than same-skill ancestry.
type AppendRevisionInput struct {
	ID                string
	SkillID           string
	CreatorSubject    string
	BasedOnRevisionID string
	SourceRevisionID  string
	Origin            RevisionOrigin
	Snapshot          SkillRevisionSnapshot
	ChangeNote        string
	GenerationID      string
	IdempotencyKey    string
	LegacyReference   string
	CreatedAt         time.Time
}

// RevisionReadOpts identifies one exact revision read. SkillID remains part of
// the lookup even though revision UUIDs are globally unique so route-scoped
// reads cannot return a revision belonging to another skill.
type RevisionReadOpts struct {
	SkillID       string
	RevisionID    string
	CallerSubject string
}

// RevisionListOpts controls one newest-first accessible history page. The
// cursor is the final (sequence, UUID) pair from the preceding page.
type RevisionListOpts struct {
	SkillID              string
	CallerSubject        string
	CursorSequenceNumber *int
	CursorRevisionID     string
	Limit                int
}

// SetRevisionVisibilityInput changes only a revision's mutable visibility and
// audit metadata. Repeating the requested state is an idempotent success.
type SetRevisionVisibilityInput struct {
	SkillID       string
	RevisionID    string
	CallerSubject string
	IsPublic      bool
	ChangedAt     time.Time
}

// SetExplicitLatestRevisionInput sets or moves the one optional explicit
// latest pointer. RevisionID must identify a public revision of SkillID.
type SetExplicitLatestRevisionInput struct {
	SkillID       string
	RevisionID    string
	CallerSubject string
	ChangedAt     time.Time
}

// ClearExplicitLatestRevisionInput clears the optional explicit latest
// pointer. Repeating a clear is an idempotent success.
type ClearExplicitLatestRevisionInput struct {
	SkillID       string
	CallerSubject string
	ChangedAt     time.Time
}

// EffectiveSkillReadOpts identifies one viewer-aware stable skill projection.
type EffectiveSkillReadOpts struct {
	SkillID       string
	CallerSubject string
}

// EffectiveRevisionReadOpts identifies shared default resolution. Effective
// resolution considers public revisions only and therefore needs no viewer.
type EffectiveRevisionReadOpts struct {
	SkillID string
}

// SkillRecord is the stable identity row for one normalized slug. During the
// route cutover it also retains predecessor content-head fields so existing
// readers continue to compile; new revision relationships must use revision
// UUIDs rather than Version or sequence values.
type SkillRecord struct {
	ID                       string
	Slug                     string
	ExplicitLatestRevisionID string
	NextSequenceNumber       int
	CreatedBySubject         string
	// The fields below describe the predecessor mutable content head. They are
	// retained only until revision-backed readers replace the old API surface.
	Name                    string
	Description             string
	Type                    string // "workflow" | "domain-knowledge" | "prompt-template"
	Version                 string // semver, e.g. "0.1.0"
	Visibility              string // "private" | "team"
	Tags                    []string
	Content                 string // markdown body
	IsAIGenerated           bool
	GeneratedFromSessionIDs []string
	ParentID                string // empty when not a fork
	// AuthorSubject is the gateway-trusted user id (JWT sub) of the creator,
	// stamped from the x-paper-auth-subject header. Empty when no header was
	// present.
	AuthorSubject string
	// DownloadCount is a real usage signal — how many times the SKILL.md has
	// been downloaded.
	DownloadCount int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// SkillLatestRecord is the result of setting, moving, or clearing explicit
// latest. EffectiveRevision is the explicit public target when the pointer is
// set, otherwise the newest-public fallback; its IsExplicitLatest field makes
// those cases unambiguous.
type SkillLatestRecord struct {
	SkillID                  string
	ExplicitLatestRevisionID string
	EffectiveRevision        *AccessibleRevisionRecord
}

// EffectiveSkillRecord is one viewer-aware card/detail projection for a stable
// skill. EffectiveRevision is always public. CardRevision selects it when
// present, otherwise the viewer's newest private revision. Other creators'
// private revision metadata is never represented.
type EffectiveSkillRecord struct {
	Skill                   SkillRecord
	EffectiveRevision       *AccessibleRevisionRecord
	NewestPrivateRevision   *AccessibleRevisionRecord
	CardRevision            *AccessibleRevisionRecord
	HasNewerPrivateRevision bool
}

// GenerationStatus is the persisted public lifecycle and durable queue state.
type GenerationStatus string

const (
	GenerationStatusQueued               GenerationStatus = "queued"
	GenerationStatusGeneratingCandidates GenerationStatus = "generating_candidates"
	GenerationStatusEvaluatingCandidates GenerationStatus = "evaluating_candidates"
	GenerationStatusSynthesizing         GenerationStatus = "synthesizing"
	GenerationStatusCompleted            GenerationStatus = "completed"
	GenerationStatusCanceled             GenerationStatus = "canceled"
	GenerationStatusFailed               GenerationStatus = "failed"
)

// GenerationSessionStatus records one selected source's durable progress.
type GenerationSessionStatus string

const (
	GenerationSessionPending          GenerationSessionStatus = "pending"
	GenerationSessionTranscriptFailed GenerationSessionStatus = "transcript_failed"
	GenerationSessionCandidateFailed  GenerationSessionStatus = "candidate_failed"
	GenerationSessionCandidateReady   GenerationSessionStatus = "candidate_ready"
	GenerationSessionEvaluationFailed GenerationSessionStatus = "evaluation_failed"
	GenerationSessionEvaluated        GenerationSessionStatus = "evaluated"
)

// GenerationCandidateKind distinguishes independent source, context-only, and
// bounded synthesis candidates.
type GenerationCandidateKind string

const (
	GenerationCandidateSession   GenerationCandidateKind = "session"
	GenerationCandidateContext   GenerationCandidateKind = "context"
	GenerationCandidateSynthesis GenerationCandidateKind = "synthesis"
)

// SkillGenerationRecord is both the public asynchronous resource and its
// private durable queue record. SkillID is the stable target and
// BaseRevisionID, when non-empty, is the exact creator-accessible revision of
// SkillID snapshotted at creation; a revision from any other skill is invalid.
// Claim fields are private transport metadata.
type SkillGenerationRecord struct {
	ID             string
	SkillID        string
	BaseRevisionID string
	CreatorSubject string
	Status         GenerationStatus
	// Snapshot is the immutable generation seed consumed by runtime execution.
	// Stable slug identity and revision ancestry live on SkillID and
	// BaseRevisionID; Snapshot never carries either legacy value.
	Snapshot                SkillRevisionSnapshot
	AuthorContext           string
	SelectedSessionIDs      []string
	EvaluatorProfile        string
	EvaluatorProfileVersion string
	EvaluationCriteria      json.RawMessage
	WinnerCandidateID       string
	ResultCandidateID       string
	ResultRevisionID        string
	ErrorCode               string
	ErrorMessage            string
	ClaimToken              string
	ClaimOwner              string
	LeaseExpiresAt          *time.Time
	AttemptCount            int
	NextAttemptAt           time.Time
	LastHeartbeatAt         *time.Time
	CreatedAt               time.Time
	UpdatedAt               time.Time
	StartedAt               *time.Time
	CompletedAt             *time.Time
}

// GenerationSessionRecord is one immutable selected source plus its mutable
// bounded outcome.
type GenerationSessionRecord struct {
	GenerationID   string
	SessionID      string
	Ordinal        int
	Status         GenerationSessionStatus
	CandidateID    string
	DiagnosticCode string
	UpdatedAt      time.Time
}

// GenerationCandidateSnapshot is one complete generated skill bundle. Source
// sessions are intentionally absent: GenerationCandidateRecord owns the sole
// source-reference collection used by evaluation, synthesis, and revision
// append.
type GenerationCandidateSnapshot struct {
	Name          string
	Description   string
	Type          string
	Tags          []string
	Content       string
	IsAIGenerated bool
}

func canonicalGenerationCandidateSnapshot(snapshot GenerationCandidateSnapshot) GenerationCandidateSnapshot {
	snapshot.Tags = append([]string{}, snapshot.Tags...)
	return snapshot
}

type generationCandidateInsight struct {
	Kind     string `json:"kind"`
	Summary  string `json:"summary"`
	Evidence string `json:"evidence"`
}

// CanonicalGenerationCandidateInsights validates the closed, bounded insight
// schema and returns its unique encoding for storage, hashing, and HTTP output.
func CanonicalGenerationCandidateInsights(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		raw = json.RawMessage(`[]`)
	}
	if len(raw) > MaxGenerationCandidateInsightsJSONBytes {
		return nil, errors.New("generation candidate insights are too large")
	}
	if err := ValidateUniqueJSONObjectFields(raw); err != nil {
		return nil, errors.New("generation candidate insights are invalid")
	}
	items, err := strictRawJSONArray(raw)
	if err != nil || len(items) > MaxGenerationCandidateInsights {
		return nil, errors.New("generation candidate insights are invalid")
	}
	insights := make([]generationCandidateInsight, len(items))
	for index, item := range items {
		var wire struct {
			Kind     *string `json:"kind"`
			Summary  *string `json:"summary"`
			Evidence *string `json:"evidence"`
		}
		if strictDecodeJSON(item, &wire) != nil || wire.Kind == nil || wire.Summary == nil || wire.Evidence == nil {
			return nil, errors.New("generation candidate insight is invalid")
		}
		insight := generationCandidateInsight{
			Kind: strings.TrimSpace(*wire.Kind), Summary: strings.TrimSpace(*wire.Summary),
			Evidence: strings.TrimSpace(*wire.Evidence),
		}
		if insight.Kind == "" || insight.Summary == "" || insight.Evidence == "" ||
			!validSnapshotText(insight.Kind, MaxGenerationCandidateInsightRunes) ||
			!validSnapshotText(insight.Summary, MaxGenerationCandidateInsightRunes) ||
			!validSnapshotText(insight.Evidence, MaxGenerationCandidateInsightRunes) {
			return nil, errors.New("generation candidate insight is invalid or too long")
		}
		insights[index] = insight
	}
	encoded, err := json.Marshal(insights)
	if err != nil || len(encoded) > MaxGenerationCandidateInsightsJSONBytes {
		return nil, errors.New("generation candidate insights are too large")
	}
	return encoded, nil
}

type generationCandidateBundleHashInput struct {
	Snapshot         GenerationCandidateSnapshot `json:"snapshot"`
	SourceSessionIDs []string                    `json:"sourceSessionIds"`
	Insights         json.RawMessage             `json:"insights"`
}

// These hash-only mirrors deliberately preserve the field names and order of
// evaluator.CandidateEvaluationRequest without making storage depend on the
// higher-level evaluator package. Migration uses the same function as runtime
// persistence so a retained SHA-256 can prove which candidate snapshot was
// actually judged.
type generationCandidateEvaluationHashCriterion struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Description string `json:"description"`
	Weight      int    `json:"weight"`
}

type generationCandidateEvaluationHashBundle struct {
	Slug             string
	Name             string
	Description      string
	Type             string
	Tags             []string
	Content          string
	SourceSessionIDs []string
	ParentID         string
}

type generationCandidateEvaluationHashRequest struct {
	Ref                string
	Name               string
	Candidate          generationCandidateEvaluationHashBundle
	Baseline           *generationCandidateEvaluationHashBundle
	AuthorContext      string
	EvidenceSessionIDs []string
	OwnerSubject       string
	Profile            string
	ProfileVersion     string
	Criteria           []generationCandidateEvaluationHashCriterion
}

func canonicalGenerationCandidateBundleSHA256(candidate GenerationCandidateRecord) string {
	encoded, _ := json.Marshal(generationCandidateBundleHashInput{
		Snapshot:         canonicalGenerationCandidateSnapshot(candidate.Snapshot),
		SourceSessionIDs: append([]string{}, candidate.SourceSessionIDs...),
		Insights:         append(json.RawMessage(nil), candidate.Insights...),
	})
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

// GenerationCandidateBundleSHA256 returns the canonical digest a runtime
// candidate must supply. It applies the same snapshot and closed-insight
// normalization used by both storage drivers.
func GenerationCandidateBundleSHA256(snapshot GenerationCandidateSnapshot, sourceSessionIDs []string, insights json.RawMessage) (string, error) {
	candidate, err := normalizeGenerationCandidateArtifact(GenerationCandidateRecord{
		ID: uuid.Nil.String(), Ordinal: 0, Kind: GenerationCandidateContext,
		Snapshot: snapshot, SourceSessionIDs: sourceSessionIDs, Insights: insights,
	}, false)
	if err != nil {
		return "", err
	}
	return candidate.BundleSHA256, nil
}

// GenerationCandidateEvaluationRequestSHA256 returns the exact digest persisted
// beside a runtime candidate evaluation. Besides making new writes and
// migration verification share one contract, this lets migration distinguish
// opaque predecessor request identities from hashes whose candidate content can
// be proven without another evaluator call.
func GenerationCandidateEvaluationRequestSHA256(generation SkillGenerationRecord, candidate GenerationCandidateRecord) (string, error) {
	var criteria []generationCandidateEvaluationHashCriterion
	if err := json.Unmarshal(generation.EvaluationCriteria, &criteria); err != nil || len(criteria) == 0 {
		return "", errors.New("generation evaluation criteria are invalid")
	}
	bundle := func(snapshot SkillRevisionSnapshot) generationCandidateEvaluationHashBundle {
		return generationCandidateEvaluationHashBundle{
			Name: snapshot.Name, Description: snapshot.Description, Type: snapshot.Type,
			Tags: append([]string(nil), snapshot.Tags...), Content: snapshot.Content,
			SourceSessionIDs: append([]string(nil), snapshot.SourceSessionIDs...),
		}
	}
	baseline := bundle(generation.Snapshot)
	candidateBundle := bundle(generationCandidateRevisionSnapshot(candidate))
	refParts, _ := json.Marshal([]string{generation.ID, "ref", candidate.ID})
	request := generationCandidateEvaluationHashRequest{
		Ref: uuid.NewSHA1(uuid.NameSpaceOID, refParts).String(), Name: candidate.Snapshot.Name,
		Candidate: candidateBundle, Baseline: &baseline,
		AuthorContext:      generation.AuthorContext,
		EvidenceSessionIDs: append([]string(nil), candidate.SourceSessionIDs...),
		OwnerSubject:       generation.CreatorSubject, Profile: generation.EvaluatorProfile,
		ProfileVersion: generation.EvaluatorProfileVersion, Criteria: criteria,
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func normalizeGenerationCandidateArtifact(candidate GenerationCandidateRecord, requireHash bool) (GenerationCandidateRecord, error) {
	if !validUUID(candidate.ID) {
		return GenerationCandidateRecord{}, errors.New("generation candidate id is invalid")
	}
	if candidate.Ordinal < 0 || candidate.Ordinal >= maxGenerationStateCandidates {
		return GenerationCandidateRecord{}, errors.New("generation candidate ordinal is invalid")
	}
	if !validGenerationCandidateKind(candidate.Kind) {
		return GenerationCandidateRecord{}, errors.New("generation candidate kind is invalid")
	}
	snapshot, err := normalizeSkillRevisionSnapshot(SkillRevisionSnapshot{
		Name: candidate.Snapshot.Name, Description: candidate.Snapshot.Description,
		Type: candidate.Snapshot.Type, Tags: candidate.Snapshot.Tags,
		Content: candidate.Snapshot.Content, IsAIGenerated: candidate.Snapshot.IsAIGenerated,
		SourceSessionIDs: candidate.SourceSessionIDs,
	})
	if err != nil {
		return GenerationCandidateRecord{}, err
	}
	candidate.Snapshot = GenerationCandidateSnapshot{
		Name: snapshot.Name, Description: snapshot.Description,
		Type: snapshot.Type, Tags: snapshot.Tags, Content: snapshot.Content,
		IsAIGenerated: snapshot.IsAIGenerated,
	}
	candidate.SourceSessionIDs = snapshot.SourceSessionIDs
	candidate.Insights, err = CanonicalGenerationCandidateInsights(candidate.Insights)
	if err != nil {
		return GenerationCandidateRecord{}, err
	}
	expectedHash := canonicalGenerationCandidateBundleSHA256(candidate)
	if requireHash && (len(candidate.BundleSHA256) != sha256.Size*2 ||
		candidate.BundleSHA256 != strings.ToLower(candidate.BundleSHA256) ||
		candidate.BundleSHA256 != expectedHash) {
		return GenerationCandidateRecord{}, errors.New("generation candidate bundle hash is invalid")
	}
	candidate.BundleSHA256 = expectedHash
	return candidate, nil
}

func validGenerationCandidateKind(kind GenerationCandidateKind) bool {
	switch kind {
	case GenerationCandidateSession, GenerationCandidateContext, GenerationCandidateSynthesis:
		return true
	default:
		return false
	}
}

func validGenerationSessionStatus(status GenerationSessionStatus) bool {
	switch status {
	case GenerationSessionPending, GenerationSessionTranscriptFailed, GenerationSessionCandidateFailed,
		GenerationSessionCandidateReady, GenerationSessionEvaluationFailed, GenerationSessionEvaluated:
		return true
	default:
		return false
	}
}

func validateGenerationSessionArtifact(session GenerationSessionRecord) error {
	if !validSnapshotIdentity(session.SessionID, MaxRevisionIdentityCodePoints) ||
		session.Ordinal < 0 || session.Ordinal >= maxGenerationStateSessions ||
		!validGenerationSessionStatus(session.Status) {
		return errors.New("generation session artifact is invalid")
	}
	requiresCandidate := session.Status == GenerationSessionCandidateReady ||
		session.Status == GenerationSessionEvaluationFailed || session.Status == GenerationSessionEvaluated
	if requiresCandidate != (session.CandidateID != "") ||
		(session.CandidateID != "" && !validUUID(session.CandidateID)) {
		return errors.New("generation session candidate state is invalid")
	}
	requiresDiagnostic := session.Status == GenerationSessionTranscriptFailed ||
		session.Status == GenerationSessionCandidateFailed || session.Status == GenerationSessionEvaluationFailed
	if requiresDiagnostic != (session.DiagnosticCode != "") ||
		(session.DiagnosticCode != "" && (len(session.DiagnosticCode) > MaxGenerationDiagnosticCodeBytes ||
			!validLowerSnakeCode(session.DiagnosticCode))) {
		return errors.New("generation session diagnostic state is invalid")
	}
	return nil
}

func validateGenerationArtifactOwner(storedGenerationID, generationID string) error {
	if storedGenerationID != "" && storedGenerationID != generationID {
		return errors.New("generation artifact belongs to another generation")
	}
	return nil
}

func validateGenerationCandidatePlacement(
	generation SkillGenerationRecord,
	sessions []GenerationSessionRecord,
	candidates []GenerationCandidateRecord,
	candidate GenerationCandidateRecord,
) error {
	if candidate.GenerationID != "" && candidate.GenerationID != generation.ID {
		return errors.New("generation candidate belongs to another generation")
	}
	if len(candidates) >= maxGenerationStateCandidates {
		return errors.New("generation candidate limit reached")
	}
	switch candidate.Kind {
	case GenerationCandidateSession:
		if len(candidate.SourceSessionIDs) != 1 {
			return errors.New("session candidate must identify exactly one source")
		}
		matched := false
		for _, session := range sessions {
			if session.Ordinal == candidate.Ordinal && session.SessionID == candidate.SourceSessionIDs[0] {
				matched = true
				break
			}
		}
		if !matched {
			return errors.New("session candidate source and ordinal are invalid")
		}
	case GenerationCandidateContext:
		if len(generation.SelectedSessionIDs) != 0 || len(candidate.SourceSessionIDs) != 0 || candidate.Ordinal != 0 {
			return errors.New("context candidate source or ordinal is invalid")
		}
		for _, existing := range candidates {
			if existing.Kind == GenerationCandidateContext {
				return errors.New("generation already has a context candidate")
			}
		}
	case GenerationCandidateSynthesis:
		if len(generation.SelectedSessionIDs) == 0 || len(candidate.SourceSessionIDs) == 0 {
			return errors.New("synthesis candidate sources are invalid")
		}
		selected := make(map[string]struct{}, len(generation.SelectedSessionIDs))
		for _, sessionID := range generation.SelectedSessionIDs {
			selected[sessionID] = struct{}{}
		}
		for _, sessionID := range candidate.SourceSessionIDs {
			if _, exists := selected[sessionID]; !exists {
				return errors.New("synthesis candidate source is invalid")
			}
		}
		for _, existing := range candidates {
			if existing.Kind == GenerationCandidateSynthesis {
				return errors.New("generation already has a synthesis candidate")
			}
			if existing.Ordinal >= candidate.Ordinal {
				return errors.New("synthesis candidate ordinal is invalid")
			}
		}
	default:
		return errors.New("generation candidate kind is invalid")
	}
	for _, existing := range candidates {
		if existing.ID == candidate.ID || existing.Ordinal == candidate.Ordinal {
			return errors.New("store generation candidate: conflicting idempotent artifact")
		}
	}
	return nil
}

func sameGenerationCandidateArtifact(existing, candidate GenerationCandidateRecord) bool {
	return existing.ID == candidate.ID && existing.GenerationID == candidate.GenerationID &&
		existing.Ordinal == candidate.Ordinal && existing.Kind == candidate.Kind &&
		equalStrings(existing.SourceSessionIDs, candidate.SourceSessionIDs) &&
		equalGenerationCandidateSnapshots(existing.Snapshot, candidate.Snapshot) &&
		existing.BundleSHA256 == candidate.BundleSHA256 && bytes.Equal(existing.Insights, candidate.Insights)
}

func equalGenerationCandidateSnapshots(left, right GenerationCandidateSnapshot) bool {
	return left.Name == right.Name && left.Description == right.Description && left.Type == right.Type &&
		equalStrings(left.Tags, right.Tags) && left.Content == right.Content &&
		left.IsAIGenerated == right.IsAIGenerated
}

func generationCandidateRevisionSnapshot(candidate GenerationCandidateRecord) SkillRevisionSnapshot {
	return canonicalSkillRevisionSnapshot(SkillRevisionSnapshot{
		Name: candidate.Snapshot.Name, Description: candidate.Snapshot.Description,
		Type: candidate.Snapshot.Type, Tags: candidate.Snapshot.Tags,
		Content: candidate.Snapshot.Content, IsAIGenerated: candidate.Snapshot.IsAIGenerated,
		SourceSessionIDs: candidate.SourceSessionIDs,
	})
}

// GenerationCandidateRecord is one complete independently generated bundle.
type GenerationCandidateRecord struct {
	ID               string
	GenerationID     string
	Ordinal          int
	Kind             GenerationCandidateKind
	SourceSessionIDs []string
	Snapshot         GenerationCandidateSnapshot
	Insights         json.RawMessage
	BundleSHA256     string
	CreatedAt        time.Time
}

// CandidateEvaluationRecord is one idempotent bounded evaluator response.
type CandidateEvaluationRecord struct {
	ID                   string
	GenerationID         string
	CandidateID          string
	RequestSHA256        string
	Profile              string
	ProfileVersion       string
	EvaluatorVersion     string
	Score                *float64
	Decision             string
	CriticalFindingCount int
	WarningFindingCount  int
	CriterionResults     json.RawMessage
	Findings             json.RawMessage
	Strengths            json.RawMessage
	Panel                json.RawMessage
	CreatedAt            time.Time
}

const (
	maxCandidateEvaluationCriteria       = 100
	maxCandidateEvaluationFindings       = 50
	maxCandidateEvaluationStrengths      = 50
	maxCandidateEvaluationTextRunes      = 4000
	maxCandidateEvaluationPathRunes      = 64 << 10
	maxCandidateEvaluationStructuredJSON = 256 << 10
	maxCandidateEvaluationPanelJSON      = 64 << 10
	// MaxCandidateEvaluatorVersionCodePoints is the shared storage/wire limit
	// for the evaluator service identity returned with a judgment.
	MaxCandidateEvaluatorVersionCodePoints = 256
)

type candidateCriterionResultDetail struct {
	CriterionID string `json:"criterion_id"`
	Weight      int    `json:"weight"`
	Passed      bool   `json:"passed"`
	Rationale   string `json:"rationale"`
}

type candidateFindingDetail struct {
	RuleID   *string `json:"rule_id,omitempty"`
	Severity string  `json:"severity"`
	Message  string  `json:"message"`
	File     *string `json:"file,omitempty"`
	Line     *int    `json:"line,omitempty"`
}

// canonicalCandidateEvaluationDetails strictly closes and re-marshals all
// portable evaluator details at the storage boundary. Opaque panel data is
// discarded before either driver can persist it.
func canonicalCandidateEvaluationDetails(evaluation CandidateEvaluationRecord) (CandidateEvaluationRecord, error) {
	if !validUUID(evaluation.ID) {
		return CandidateEvaluationRecord{}, errors.New("candidate evaluation id is invalid")
	}
	for _, artifact := range []struct {
		name string
		raw  json.RawMessage
	}{
		{name: "criterion results", raw: evaluation.CriterionResults},
		{name: "findings", raw: evaluation.Findings},
		{name: "strengths", raw: evaluation.Strengths},
		{name: "panel", raw: evaluation.Panel},
	} {
		if len(artifact.raw) != 0 {
			if err := ValidateUniqueJSONObjectFields(artifact.raw); err != nil {
				return CandidateEvaluationRecord{}, fmt.Errorf("candidate evaluation %s are invalid", artifact.name)
			}
		}
	}
	if err := validateCandidateEvaluationPanel(evaluation.Panel); err != nil {
		return CandidateEvaluationRecord{}, err
	}
	if evaluation.EvaluatorVersion == "" || evaluation.EvaluatorVersion != strings.TrimSpace(evaluation.EvaluatorVersion) ||
		!validSnapshotIdentity(evaluation.EvaluatorVersion, MaxCandidateEvaluatorVersionCodePoints) {
		return CandidateEvaluationRecord{}, errors.New("candidate evaluator version is invalid or too long")
	}
	if evaluation.Score == nil || math.IsNaN(*evaluation.Score) || math.IsInf(*evaluation.Score, 0) ||
		*evaluation.Score < 0 || *evaluation.Score > 1 {
		return CandidateEvaluationRecord{}, errors.New("candidate evaluation score is invalid")
	}
	if evaluation.Decision != "pass" && evaluation.Decision != "revise" {
		return CandidateEvaluationRecord{}, errors.New("candidate evaluation decision is invalid")
	}
	criteria, err := canonicalCriterionResultDetails(evaluation.CriterionResults)
	if err != nil {
		return CandidateEvaluationRecord{}, err
	}
	findings, criticalFindings, warningFindings, err := canonicalFindingDetails(evaluation.Findings)
	if err != nil {
		return CandidateEvaluationRecord{}, err
	}
	strengths, err := canonicalStrengthDetails(evaluation.Strengths)
	if err != nil {
		return CandidateEvaluationRecord{}, err
	}
	evaluation.CriterionResults = criteria
	evaluation.Findings = findings
	evaluation.Strengths = strengths
	evaluation.CriticalFindingCount = criticalFindings
	evaluation.WarningFindingCount = warningFindings
	evaluation.Panel = json.RawMessage(`{}`)
	return evaluation, nil
}

func canonicalCriterionResultDetails(raw json.RawMessage) (json.RawMessage, error) {
	items, err := strictRawJSONArray(raw)
	if err != nil || len(items) > maxCandidateEvaluationCriteria || len(raw) > maxCandidateEvaluationStructuredJSON {
		return nil, errors.New("candidate evaluation criterion results are invalid")
	}
	results := make([]candidateCriterionResultDetail, len(items))
	for index, item := range items {
		var wire struct {
			CriterionID *string `json:"criterion_id"`
			Weight      *int    `json:"weight"`
			Passed      *bool   `json:"passed"`
			Rationale   *string `json:"rationale"`
		}
		if strictDecodeJSON(item, &wire) != nil || wire.CriterionID == nil || wire.Weight == nil ||
			wire.Passed == nil || wire.Rationale == nil || *wire.CriterionID == "" ||
			!validSnapshotIdentity(*wire.CriterionID, MaxRevisionIdentityCodePoints) ||
			*wire.Weight < 1 || *wire.Weight > 3 || strings.TrimSpace(*wire.Rationale) == "" ||
			!validSnapshotText(*wire.Rationale, maxCandidateEvaluationTextRunes) {
			return nil, errors.New("candidate evaluation criterion result is invalid")
		}
		results[index] = candidateCriterionResultDetail{
			CriterionID: *wire.CriterionID, Weight: *wire.Weight,
			Passed: *wire.Passed, Rationale: *wire.Rationale,
		}
	}
	return marshalBoundedEvaluationDetails(results)
}

func canonicalFindingDetails(raw json.RawMessage) (json.RawMessage, int, int, error) {
	items, err := strictRawJSONArray(raw)
	if err != nil || len(items) > maxCandidateEvaluationFindings || len(raw) > maxCandidateEvaluationStructuredJSON {
		return nil, 0, 0, errors.New("candidate evaluation findings are invalid")
	}
	findings := make([]candidateFindingDetail, len(items))
	critical := 0
	warning := 0
	for index, item := range items {
		var finding candidateFindingDetail
		if strictDecodeJSON(item, &finding) != nil || strings.TrimSpace(finding.Message) == "" ||
			!validSnapshotText(finding.Message, maxCandidateEvaluationTextRunes) {
			return nil, 0, 0, errors.New("candidate evaluation finding is invalid")
		}
		finding.Severity = strings.ToLower(finding.Severity)
		switch finding.Severity {
		case "critical":
			critical++
		case "warn", "warning":
			warning++
		case "info":
		default:
			return nil, 0, 0, errors.New("candidate evaluation finding severity is invalid")
		}
		if finding.RuleID != nil && !validSnapshotIdentity(*finding.RuleID, MaxRevisionIdentityCodePoints) {
			return nil, 0, 0, errors.New("candidate evaluation finding rule id is invalid")
		}
		if finding.File != nil && !validSnapshotText(*finding.File, maxCandidateEvaluationPathRunes) {
			return nil, 0, 0, errors.New("candidate evaluation finding file is invalid")
		}
		if finding.Line != nil && *finding.Line < 0 {
			return nil, 0, 0, errors.New("candidate evaluation finding line is invalid")
		}
		findings[index] = finding
	}
	encoded, err := marshalBoundedEvaluationDetails(findings)
	return encoded, critical, warning, err
}

func canonicalStrengthDetails(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		raw = json.RawMessage(`[]`)
	}
	var strengths []string
	if strictDecodeJSON(raw, &strengths) != nil || len(strengths) > maxCandidateEvaluationStrengths ||
		len(raw) > maxCandidateEvaluationStructuredJSON {
		return nil, errors.New("candidate evaluation strengths are invalid")
	}
	for _, strength := range strengths {
		if strings.TrimSpace(strength) == "" || !validSnapshotText(strength, maxCandidateEvaluationTextRunes) {
			return nil, errors.New("candidate evaluation strength is invalid")
		}
	}
	if strengths == nil {
		strengths = []string{}
	}
	return marshalBoundedEvaluationDetails(strengths)
}

func strictRawJSONArray(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return []json.RawMessage{}, nil
	}
	var items []json.RawMessage
	if err := strictDecodeJSON(raw, &items); err != nil {
		return nil, err
	}
	return items, nil
}

func validateCandidateEvaluationPanel(raw json.RawMessage) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if len(raw) > maxCandidateEvaluationPanelJSON {
		return errors.New("candidate evaluation panel is invalid or too large")
	}
	var panel map[string]json.RawMessage
	if err := json.Unmarshal(raw, &panel); err != nil || panel == nil {
		return errors.New("candidate evaluation panel is invalid or too large")
	}
	return nil
}

func strictDecodeJSON(raw []byte, output any) error {
	if err := ValidateUniqueJSONObjectFields(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

// ValidateUniqueJSONObjectFields rejects duplicate object member names at
// every nesting depth before typed decoding or canonical re-marshalling can
// erase them. It accepts any single valid JSON value.
func ValidateUniqueJSONObjectFields(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var validateValue func() error
	validateValue = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, isDelimiter := token.(json.Delim)
		if !isDelimiter {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				nameToken, tokenErr := decoder.Token()
				if tokenErr != nil {
					return tokenErr
				}
				name, ok := nameToken.(string)
				if !ok {
					return errors.New("JSON object field name is invalid")
				}
				if _, duplicate := seen[name]; duplicate {
					return fmt.Errorf("JSON object field %q is duplicated", name)
				}
				seen[name] = struct{}{}
				if valueErr := validateValue(); valueErr != nil {
					return valueErr
				}
			}
			closing, closeErr := decoder.Token()
			if closeErr != nil || closing != json.Delim('}') {
				return errors.New("JSON object is incomplete")
			}
		case '[':
			for decoder.More() {
				if valueErr := validateValue(); valueErr != nil {
					return valueErr
				}
			}
			closing, closeErr := decoder.Token()
			if closeErr != nil || closing != json.Delim(']') {
				return errors.New("JSON array is incomplete")
			}
		default:
			return errors.New("JSON delimiter is invalid")
		}
		return nil
	}
	if err := validateValue(); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func marshalBoundedEvaluationDetails(value any) (json.RawMessage, error) {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > maxCandidateEvaluationStructuredJSON {
		return nil, errors.New("candidate evaluation details are too large")
	}
	return encoded, nil
}

// GenerationDiagnosticRecord is a bounded user-safe failure or degradation.
type GenerationDiagnosticRecord struct {
	ID           string
	GenerationID string
	SessionID    string
	CandidateID  string
	Stage        string
	Code         string
	Message      string
	Retryable    bool
	CreatedAt    time.Time
}

const (
	// MaxGenerationFailureCodeBytes bounds a durable machine-readable failure
	// code before it can reach public generation history.
	MaxGenerationFailureCodeBytes = 128
	// MaxGenerationFailureMessageBytes bounds a durable curated failure message.
	// Raw provider, model, transcript, and storage errors are never valid input.
	MaxGenerationFailureMessageBytes = 1024
	// Durable diagnostics use the same finite wire bounds as generation history.
	MaxGenerationDiagnosticStageBytes     = 64
	MaxGenerationDiagnosticCodeBytes      = 128
	MaxGenerationDiagnosticMessageBytes   = 1024
	MaxGenerationDiagnosticSessionIDBytes = 256
	// MaxGenerationDiagnostics is the maximum retained logical diagnostic slots
	// for 100 sources, 101 candidates, and one synthesis marker.
	MaxGenerationDiagnostics = 303
)

// GenerationFailure is the bounded safe error persisted for a terminal or
// requeued attempt. Code and Message come from a finite caller-owned vocabulary;
// Cause values and raw external responses deliberately have no storage field.
type GenerationFailure struct {
	Code    string
	Message string
}

type generationDiagnosticSource int

const (
	diagnosticSourceGeneration generationDiagnosticSource = iota
	diagnosticSourceSession
	diagnosticSourceCandidate
	diagnosticSourceSynthesis
)

type generationDiagnosticRule struct {
	Stage     string
	Code      string
	Message   string
	Retryable bool
	Source    generationDiagnosticSource
}

// GenerationDiagnosticDefinition is the public, non-sensitive portion of one
// finite diagnostic catalog entry used to generate the OpenAPI enums.
type GenerationDiagnosticDefinition struct {
	Stage     string
	Code      string
	Message   string
	Retryable bool
}

var generationDiagnosticCatalog = []generationDiagnosticRule{
	{Stage: "candidate", Code: "session_limit_exceeded", Message: "The generation selected more sessions than this worker permits.", Source: diagnosticSourceGeneration},
	{Stage: "candidate", Code: "context_candidate_failed", Message: "A context-derived candidate could not be generated.", Source: diagnosticSourceGeneration},
	{Stage: "candidate", Code: "context_candidate_failed", Message: "A context-derived candidate could not be generated.", Retryable: true, Source: diagnosticSourceGeneration},
	{Stage: "transcript", Code: "transcript_unavailable", Message: "The selected session transcript could not be loaded.", Source: diagnosticSourceSession},
	{Stage: "transcript", Code: "transcript_unavailable", Message: "The selected session transcript could not be loaded.", Retryable: true, Source: diagnosticSourceSession},
	{Stage: "candidate", Code: "candidate_generation_failed", Message: "A candidate could not be generated from the selected session.", Source: diagnosticSourceSession},
	{Stage: "candidate", Code: "candidate_generation_failed", Message: "A candidate could not be generated from the selected session.", Retryable: true, Source: diagnosticSourceSession},
	{Stage: "evaluation", Code: "evaluator_rate_limited", Message: "Candidate evaluation is temporarily rate limited.", Retryable: true, Source: diagnosticSourceCandidate},
	{Stage: "evaluation", Code: "evaluator_unavailable", Message: "Candidate evaluation is temporarily unavailable.", Retryable: true, Source: diagnosticSourceCandidate},
	{Stage: "evaluation", Code: "evaluator_canceled", Message: "Candidate evaluation was canceled.", Source: diagnosticSourceCandidate},
	{Stage: "evaluation", Code: "evaluator_rejected", Message: "The evaluator rejected this candidate.", Source: diagnosticSourceCandidate},
	{Stage: "evaluation", Code: "evaluator_invalid_response", Message: "The evaluator returned an invalid candidate judgment.", Source: diagnosticSourceCandidate},
	{Stage: "evaluation", Code: "candidate_evaluation_failed", Message: "The candidate could not be evaluated.", Source: diagnosticSourceCandidate},
	{Stage: "evaluation", Code: "evaluation_unrankable", Message: "The candidate evaluation could not be ranked.", Source: diagnosticSourceCandidate},
	{Stage: "evaluation", Code: "evaluation_profile_mismatch", Message: "The candidate evaluation did not match the generation rubric.", Source: diagnosticSourceCandidate},
	{Stage: "synthesis", Code: "synthesis_generation_failed", Message: "The optional synthesis candidate could not be generated.", Source: diagnosticSourceSynthesis},
	{Stage: "synthesis", Code: "synthesis_evaluator_rate_limited", Message: "Candidate evaluation is temporarily rate limited.", Retryable: true, Source: diagnosticSourceSynthesis},
	{Stage: "synthesis", Code: "synthesis_evaluator_unavailable", Message: "Candidate evaluation is temporarily unavailable.", Retryable: true, Source: diagnosticSourceSynthesis},
	{Stage: "synthesis", Code: "synthesis_evaluator_canceled", Message: "Candidate evaluation was canceled.", Source: diagnosticSourceSynthesis},
	{Stage: "synthesis", Code: "synthesis_evaluator_rejected", Message: "The evaluator rejected this candidate.", Source: diagnosticSourceSynthesis},
	{Stage: "synthesis", Code: "synthesis_evaluator_invalid_response", Message: "The evaluator returned an invalid candidate judgment.", Source: diagnosticSourceSynthesis},
	{Stage: "synthesis", Code: "synthesis_candidate_evaluation_failed", Message: "The candidate could not be evaluated.", Source: diagnosticSourceSynthesis},
	{Stage: "synthesis", Code: "synthesis_evaluation_profile_mismatch", Message: "The candidate evaluation did not match the generation rubric.", Source: diagnosticSourceSynthesis},
}

// GenerationDiagnosticDefinitions returns a copy of the finite catalog for
// contract generation. Duplicate code entries differ only by retryability.
func GenerationDiagnosticDefinitions() []GenerationDiagnosticDefinition {
	definitions := make([]GenerationDiagnosticDefinition, len(generationDiagnosticCatalog))
	for index, rule := range generationDiagnosticCatalog {
		definitions[index] = GenerationDiagnosticDefinition{
			Stage: rule.Stage, Code: rule.Code, Message: rule.Message, Retryable: rule.Retryable,
		}
	}
	return definitions
}

func generationDiagnosticRuleFor(diagnostic GenerationDiagnosticRecord) (generationDiagnosticRule, bool) {
	for _, rule := range generationDiagnosticCatalog {
		if diagnostic.Stage == rule.Stage && diagnostic.Code == rule.Code &&
			diagnostic.Message == rule.Message && diagnostic.Retryable == rule.Retryable {
			return rule, true
		}
	}
	return generationDiagnosticRule{}, false
}

func validateGenerationDiagnostic(diagnostic GenerationDiagnosticRecord) error {
	if !validUUID(diagnostic.ID) {
		return errors.New("generation diagnostic id is invalid")
	}
	if diagnostic.CandidateID != "" && !validUUID(diagnostic.CandidateID) {
		return errors.New("generation diagnostic candidate id is invalid")
	}
	if !validSnapshotIdentity(diagnostic.SessionID, MaxGenerationDiagnosticSessionIDBytes) {
		return errors.New("generation diagnostic session id is invalid or too long")
	}
	rule, exists := generationDiagnosticRuleFor(diagnostic)
	if !exists {
		return errors.New("generation diagnostic is not in the supported catalog")
	}
	switch rule.Source {
	case diagnosticSourceGeneration:
		if diagnostic.SessionID != "" || diagnostic.CandidateID != "" {
			return errors.New("generation diagnostic source is invalid")
		}
	case diagnosticSourceSession:
		if diagnostic.SessionID == "" || diagnostic.CandidateID != "" {
			return errors.New("generation diagnostic source is invalid")
		}
	case diagnosticSourceCandidate:
		if diagnostic.CandidateID == "" {
			return errors.New("generation diagnostic source is invalid")
		}
	case diagnosticSourceSynthesis:
		if diagnostic.SessionID != "" || diagnostic.CandidateID == "" {
			return errors.New("generation diagnostic source is invalid")
		}
	default:
		return errors.New("generation diagnostic source is invalid")
	}
	return nil
}

func sameGenerationDiagnosticArtifact(existing GenerationDiagnosticRecord, generationID string, diagnostic GenerationDiagnosticRecord) bool {
	return existing.GenerationID == generationID && existing.ID == diagnostic.ID &&
		existing.SessionID == diagnostic.SessionID && existing.CandidateID == diagnostic.CandidateID &&
		existing.Stage == diagnostic.Stage && existing.Code == diagnostic.Code &&
		existing.Message == diagnostic.Message && existing.Retryable == diagnostic.Retryable
}

func generationDiagnosticSlot(diagnostic GenerationDiagnosticRecord) string {
	switch diagnostic.Stage {
	case "transcript", "candidate":
		return diagnostic.Stage + ":session:" + diagnostic.SessionID
	case "evaluation":
		return diagnostic.Stage + ":candidate:" + diagnostic.CandidateID
	case "synthesis":
		return diagnostic.Stage + ":generation"
	default:
		return ""
	}
}

func validLowerSnakeCode(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}

func validateGenerationFailure(failure GenerationFailure) error {
	if failure.Code == "" || failure.Code != strings.TrimSpace(failure.Code) ||
		len(failure.Code) > MaxGenerationFailureCodeBytes || !utf8.ValidString(failure.Code) {
		return errors.New("generation failure code is invalid or too long")
	}
	if !validLowerSnakeCode(failure.Code) {
		return errors.New("generation failure code is invalid or too long")
	}
	if failure.Message == "" || failure.Message != strings.TrimSpace(failure.Message) ||
		len(failure.Message) > MaxGenerationFailureMessageBytes || !utf8.ValidString(failure.Message) {
		return errors.New("generation failure message is invalid or too long")
	}
	for _, character := range failure.Message {
		if unicode.IsControl(character) {
			return errors.New("generation failure message is invalid or too long")
		}
	}
	return nil
}

// GenerationState is the durable processor resume view.
type GenerationState struct {
	Generation  SkillGenerationRecord
	Sessions    []GenerationSessionRecord
	Candidates  []GenerationCandidateRecord
	Evaluations []CandidateEvaluationRecord
	Diagnostics []GenerationDiagnosticRecord
}

const (
	// DefaultGenerationListLimit is the modern generation history page size.
	DefaultGenerationListLimit = 20
	// MaxGenerationListLimit is enforced by both storage drivers even when a
	// caller bypasses HTTP request validation.
	MaxGenerationListLimit = 100

	// A generation accepts at most 100 independent session sources.
	maxGenerationStateSessions = 100
	// Every source may retain one candidate, and bounded synthesis may append
	// one more candidate at the next ordinal.
	maxGenerationStateCandidates = 101
	// Every retained candidate, including synthesis, may have one persisted
	// ranking evaluation.
	maxGenerationStateEvaluations = 101
	// Retain the complete bounded logical diagnostic catalog surface.
	maxGenerationStateDiagnostics = MaxGenerationDiagnostics
)

// SkillGenerationSummaryRecord contains only fields needed to render one
// generation-history row. Artifact collections and immutable generation input
// are available only from exact reads.
type SkillGenerationSummaryRecord struct {
	ID               string
	SkillID          string
	BaseRevisionID   string
	Status           GenerationStatus
	ResultRevisionID string
	ErrorCode        string
	ErrorMessage     string
	AttemptCount     int
	CreatedAt        time.Time
	UpdatedAt        time.Time
	StartedAt        *time.Time
	CompletedAt      *time.Time
}

// SkillGenerationListOpts selects one bounded newest-first keyset page.
// CursorCreatedAt and CursorID must either both be set or both be absent.
type SkillGenerationListOpts struct {
	CallerSubject   string
	SkillID         string
	CursorCreatedAt *time.Time
	CursorID        string
	Limit           int
}

// SkillGenerationListPage returns summary rows and the final row key when a
// subsequent page exists. The API boundary turns that key into an opaque cursor.
type SkillGenerationListPage struct {
	Generations   []SkillGenerationSummaryRecord
	NextCreatedAt *time.Time
	NextID        string
}

// CreateGenerationInput contains every immutable value committed before a
// generation can be claimed. ID is allocated by the API boundary. SkillID is
// the stable target. BaseRevisionID is optional; when present CreateGeneration
// MUST resolve it with CreatorSubject, reject inaccessible revisions, and reject
// revisions whose SkillID differs from this SkillID. SelectedSessionIDs preserve
// caller order.
type CreateGenerationInput struct {
	ID             string
	SkillID        string
	BaseRevisionID string
	CreatorSubject string
	// Snapshot is the immutable runtime generation seed. Stable slug identity
	// and revision ancestry live on SkillID and BaseRevisionID.
	Snapshot                SkillRevisionSnapshot
	AuthorContext           string
	SelectedSessionIDs      []string
	EvaluatorProfile        string
	EvaluatorProfileVersion string
	EvaluationCriteria      json.RawMessage
	CreatedAt               time.Time
}

// ClaimGenerationInput configures one short leased queue claim.
type ClaimGenerationInput struct {
	WorkerID      string
	LeaseDuration time.Duration
}

// FailGenerationInput terminates one currently claimed executable generation.
// Storage validates the live claim against its clock, validates the curated
// failure bounds, and atomically persists Failure, marks the row failed, sets
// UpdatedAt and CompletedAt from one storage-clock sample, and clears all claim
// metadata.
type FailGenerationInput struct {
	GenerationID string
	ClaimToken   string
	Failure      GenerationFailure
}

// RequeueGenerationInput releases one currently claimed executable generation
// for a durable retry. Storage validates the live claim and curated failure,
// leaves the current executable Status unchanged, sets UpdatedAt and
// NextAttemptAt from one storage-clock sample, and clears all claim metadata.
// RetryAfter is a relative delay so a worker never supplies an absolute time.
type RequeueGenerationInput struct {
	GenerationID string
	ClaimToken   string
	RetryAfter   time.Duration
	Failure      GenerationFailure
}

// GenerationQueueStats is one storage-clock snapshot of rows claimable at that
// instant: executable status, NextAttemptAt <= storage now, and no lease whose
// expiry is after storage now. Depth counts exactly those rows. Lag is
// max(0, storage now - min(NextAttemptAt)) over those rows, or zero at depth
// zero. Active, unexpired claims are intentionally excluded and reported by the
// separate active-workers metric.
type GenerationQueueStats struct {
	Depth int64
	Lag   time.Duration
}

// AppendPrivateGenerationResultInput identifies the evaluated candidates used
// by one fenced result append. Storage derives the revision UUID, timestamps,
// automatic change note, immutable content, and source-session references from
// its own clock and the locked same-generation rows.
type AppendPrivateGenerationResultInput struct {
	GenerationID             string
	ClaimToken               string
	InitialWinnerCandidateID string
	ResultCandidateID        string
}

const generationResultChangeNote = "Generated skill revision"

var generationResultRevisionNamespace = uuid.MustParse("08c91982-ee90-4ad4-af5e-182112abb48a")

func generationResultRevisionID(generationID string) string {
	return uuid.NewSHA1(generationResultRevisionNamespace, []byte(generationID)).String()
}

// SkillVersionRecord is one immutable published snapshot of a skill's content.
// The skill's working/current content lives on SkillRecord.Content; versions
// are history only.
type SkillVersionRecord struct {
	SkillID       string
	VersionNumber int
	Semver        string
	Changelog     string
	Content       string
	// ExpectedContent is the caller's original publication precondition. It is
	// persisted as part of the request identity used to reconcile retries.
	ExpectedContent *string
	// CASContent overrides the value checked against the locked head. It differs
	// only when the proposed content was manually saved before publication.
	CASContent    *string
	AuthorSubject string
	PublishedAt   time.Time
}

// SkillListOpts controls a single keyset page of skills. Query, Author and
// NotAuthor are all optional filters; an empty string disables each.
type SkillListOpts struct {
	Query     string // name/description/tag search (empty = no filter)
	Author    string // only skills authored by this subject ("mine")
	NotAuthor string // exclude this subject ("team")
	// Sort selects the ordering and which cursor column applies:
	// "downloads" orders by download_count DESC (keyset on CursorDownloads);
	// anything else orders by updated_at DESC (keyset on CursorTs). Both
	// tiebreak on id DESC.
	Sort            string
	CursorTs        *time.Time // recent keyset: updated_at of the prior page's last row
	CursorDownloads *int64     // downloads keyset: download_count of that row
	CursorID        string     // keyset tiebreak: id of that last row
	Limit           int        // page size; zero falls back to DefaultListLimit
	// External carries the deployment-configured attachment-view filters
	// armed for this request; every filter must hold for a row to appear.
	External []ExternalAttachmentFilter
}

// SkillSortDownloads is the SkillListOpts.Sort value for most-downloaded order.
const SkillSortDownloads = "downloads"

// SkillCountOpts controls the per-tab totals query. It carries the same
// search text and armed external filters as the page the totals describe —
// counting a superset of a filtered page would report tabs for rows the
// caller can never see.
type SkillCountOpts struct {
	Query  string // name/description/tag search (empty = no filter)
	Author string // the caller's subject, splitting Total into Mine/team
	// External carries the deployment-configured attachment-view filters
	// armed for this request; the totals must honor every one of them.
	External []ExternalAttachmentFilter
}

// SkillCounts are the per-tab totals for a search: every matching skill, and
// how many the caller authored. "team" is derived as Total - Mine.
type SkillCounts struct {
	Total int64
	Mine  int64
}

// EffectiveSkillListOpts carries the predecessor list filters together with
// the trusted viewer subject needed to select creator-private continuations.
type EffectiveSkillListOpts struct {
	SkillListOpts
	CallerSubject string
}

// EffectiveSkillCountOpts carries the same filters and viewer identity used by
// the effective list so each stable skill contributes at most one count.
type EffectiveSkillCountOpts struct {
	SkillCountOpts
	CallerSubject string
}

// EffectiveSkillSessionListOpts selects one viewer-aware projection per skill
// with accessible revision provenance for SessionID.
type EffectiveSkillSessionListOpts struct {
	SessionID     string
	CallerSubject string
	// Limit bounds the number of projections materialized after session and
	// visibility selection. Zero falls back to DefaultListLimit.
	Limit int
}

// SkillReader exposes revision-backed skill reads without changing the
// predecessor Store signatures before the HTTP cutover.
type SkillReader interface {
	ResolveEffectiveRevision(ctx context.Context, opts EffectiveRevisionReadOpts) (*AccessibleRevisionRecord, error)
	GetEffectiveSkill(ctx context.Context, opts EffectiveSkillReadOpts) (*EffectiveSkillRecord, error)
	ListEffectiveSkills(ctx context.Context, opts EffectiveSkillListOpts) ([]EffectiveSkillRecord, error)
	CountEffectiveSkills(ctx context.Context, opts EffectiveSkillCountOpts) (SkillCounts, error)
	ListEffectiveSkillsBySession(ctx context.Context, opts EffectiveSkillSessionListOpts) ([]EffectiveSkillRecord, error)
}

// SkillIdentityStore resolves one stable identity for a normalized slug. It
// returns no revision data, including when another creator won the slug race.
type SkillIdentityStore interface {
	ResolveSkill(ctx context.Context, input ResolveSkillInput) (*SkillRecord, error)
}

// RevisionReader returns immutable content together with separately stored
// visibility only after applying the public-or-creator predicate in storage.
type RevisionReader interface {
	GetRevision(ctx context.Context, opts RevisionReadOpts) (*AccessibleRevisionRecord, error)
	ListRevisions(ctx context.Context, opts RevisionListOpts) ([]AccessibleRevisionRecord, error)
}

// RevisionWriter appends complete immutable snapshots. Implementations must
// validate base and source visibility before materializing their content.
type RevisionWriter interface {
	AppendRevision(ctx context.Context, input AppendRevisionInput) (*SkillRevisionRecord, error)
}

// RevisionStore is the shared immutable-revision contract implemented by both
// memory and Postgres stores. Mutable visibility/latest capabilities remain
// separate from this interface.
type RevisionStore interface {
	RevisionReader
	RevisionWriter
}

// RevisionMetadataStore owns the independent idempotent visibility and
// explicit-latest mutations. Implementations must lock invariant checks and
// writes together without changing immutable revision content.
type RevisionMetadataStore interface {
	SetRevisionVisibility(ctx context.Context, input SetRevisionVisibilityInput) (*RevisionVisibilityRecord, error)
	SetExplicitLatestRevision(ctx context.Context, input SetExplicitLatestRevisionInput) (*SkillLatestRecord, error)
	ClearExplicitLatestRevision(ctx context.Context, input ClearExplicitLatestRevisionInput) (*SkillLatestRecord, error)
}

// Store is the predecessor capability surface retained while handlers cut over
// to SkillIdentityStore and RevisionStore.
type Store interface {
	// Kind names the backing store ("memory" or "postgres") so responses and
	// logs can never misread a demo as durable.
	Kind() string
	UpsertSkill(ctx context.Context, rec SkillRecord) (*SkillRecord, error)
	// CreatePublishedSkill atomically inserts a new skill and its initial
	// immutable version. Callers must supply version number 1 for the same id.
	CreatePublishedSkill(ctx context.Context, rec SkillRecord, version SkillVersionRecord) (*SkillRecord, error)
	GetSkill(ctx context.Context, id string) (*SkillRecord, error)
	ListSkills(ctx context.Context, opts SkillListOpts) ([]SkillRecord, error)
	ListSkillsBySession(ctx context.Context, sessionID string) ([]SkillRecord, error)
	CountSkills(ctx context.Context, opts SkillCountOpts) (SkillCounts, error)
	NextSkillVersionNumber(ctx context.Context, skillID string) (int, error)
	// PublishSkillVersion appends the immutable snapshot and advances the
	// skill's head (version, content, updated_at) in one atomic step. The head
	// only moves when rec.VersionNumber is the highest published number, so of
	// two overlapping publishes the older one can never regress the head the
	// newer one already set. A duplicate (skill_id, version_number) returns
	// ErrSkillVersionConflict for the caller's recompute-and-retry loop.
	PublishSkillVersion(ctx context.Context, rec SkillVersionRecord) (*SkillVersionRecord, error)
	ListSkillVersions(ctx context.Context, skillID string) ([]SkillVersionRecord, error)
	IncrementSkillDownloads(ctx context.Context, id string) error
	DeleteSkill(ctx context.Context, id string) (bool, error)
	Close()
}

// GenerationReader owns storage-authorized history. Nonterminal, failed,
// canceled, and private-result generations are creator-only. A completed
// generation is also visible to another organization member only while its
// current result revision satisfies the public-or-creator predicate. The same
// rule applies before loading artifacts for list, nested, and direct-ID reads.
type GenerationReader interface {
	ListSkillGenerations(ctx context.Context, opts SkillGenerationListOpts) (*SkillGenerationListPage, error)
	GetSkillGeneration(ctx context.Context, callerSubject, skillID, generationID string) (*GenerationState, error)
	GetGenerationByID(ctx context.Context, callerSubject, generationID string) (*GenerationState, error)
}

// GenerationClaimer owns short queue transactions and lease fencing. Requeue
// and queue statistics use the storage clock so worker replicas cannot disagree
// because of process-clock skew.
type GenerationClaimer interface {
	ClaimGeneration(ctx context.Context, input ClaimGenerationInput) (*SkillGenerationRecord, error)
	RenewGenerationLease(ctx context.Context, generationID, claimToken string, leaseDuration time.Duration) (bool, error)
	FailGeneration(ctx context.Context, input FailGenerationInput) (*SkillGenerationRecord, error)
	RequeueGeneration(ctx context.Context, input RequeueGenerationInput) (*SkillGenerationRecord, error)
	GenerationQueueStats(ctx context.Context) (GenerationQueueStats, error)
}

// GenerationArtifactStore contains worker-only resumable state mutations. Every
// method is fenced by the current live claim.
type GenerationArtifactStore interface {
	GetClaimedGeneration(ctx context.Context, generationID, claimToken string) (*GenerationState, error)
	// UpdateGenerationStatus advances only between executable processing stages.
	// Terminal failure uses FailGeneration so failure details, terminal state,
	// timestamps, and claim release cannot commit independently.
	UpdateGenerationStatus(ctx context.Context, generationID, claimToken string, from, to GenerationStatus) error
	UpdateGenerationSession(ctx context.Context, generationID, claimToken string, session GenerationSessionRecord) error
	PutGenerationCandidate(ctx context.Context, generationID, claimToken string, candidate GenerationCandidateRecord) (*GenerationCandidateRecord, error)
	PutCandidateEvaluation(ctx context.Context, generationID, claimToken string, evaluation CandidateEvaluationRecord) (*CandidateEvaluationRecord, error)
	PutGenerationDiagnostic(ctx context.Context, generationID, claimToken string, diagnostic GenerationDiagnosticRecord) (*GenerationDiagnosticRecord, error)

	// AppendPrivateGenerationResult atomically verifies the live claim, current
	// same-skill creator access to the base, and matching-profile evaluations for
	// both candidate identities; appends exactly one creator-private revision
	// using GenerationID as the idempotency lineage; records the initial
	// deterministic winner and final result candidate; and completes the
	// generation. Exact completed retries return that immutable revision without
	// consulting mutable visibility/latest metadata.
	AppendPrivateGenerationResult(ctx context.Context, input AppendPrivateGenerationResultInput) (*SkillRevisionRecord, error)
}

// GenerationStore is the complete skill/revision-anchored generation surface.
// Creation never reserves a result sequence. Cancellation remains creator-only
// even when completed generation history later becomes organization-visible.
type GenerationStore interface {
	GenerationReader
	GenerationClaimer
	GenerationArtifactStore

	// CreateGeneration MUST verify that a non-empty BaseRevisionID is visible to
	// CreatorSubject and belongs to SkillID before committing any generation row.
	CreateGeneration(ctx context.Context, input CreateGenerationInput) (*SkillGenerationRecord, error)
	CancelSkillGeneration(ctx context.Context, callerSubject, skillID, generationID string) (*GenerationState, error)
}

// ErrExternalViewUnavailable means a deployment-configured external
// attachment view could not be read at query time — dropped, or its grant
// revoked, after the startup probe passed. Handlers translate it into the
// missing-relation error convention (503); serving unfiltered rows as if
// the filter had applied is the one forbidden degradation.
var ErrExternalViewUnavailable = errors.New("external attachment view unavailable")

// ExternalAttachmentFilter restricts a skills page to rows referenced by an
// external view of the canonical attachment shape (primitive_type,
// primitive_id, value). Values arrive already normalized (the API boundary
// applies the configured verbs) and are matched exactly, one independent
// probe per value, ANDed.
type ExternalAttachmentFilter struct {
	View      string   // schema-qualified relation, deployment-configured
	TypeValue string   // primitive_type discriminator for this surface
	Values    []string // one EXISTS probe per value, ANDed
}

// ExternalViewProber is implemented by stores that can check whether a
// configured external attachment view is readable. The server probes once at
// startup; a store without the capability never arms external filters.
type ExternalViewProber interface {
	ProbeExternalView(ctx context.Context, view string) error
}
