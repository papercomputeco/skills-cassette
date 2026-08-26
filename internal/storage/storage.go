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
	"context"
	"errors"
	"time"
)

// ErrSkillVersionConflict is returned by CreateSkillVersion when the
// (skill_id, version_number) pair already exists — i.e. a concurrent publish
// claimed the same number. Callers translate it into a retry rather than a 500.
var ErrSkillVersionConflict = errors.New("skill version number already exists")

// ErrSkillChanged means a conditional publish no longer matches the skill head.
var ErrSkillChanged = errors.New("skill content changed")

// DefaultListLimit bounds a skills page when the caller supplies no limit.
const DefaultListLimit = 24

// SkillRecord is the flat skills-table row surfaced by the skills API. It
// mirrors the console's Skill shape so a generated skill round-trips through
// persistence without a separate projection. Fields absent in the DB (parent_id
// NULL) are represented as empty strings so API callers never unwrap optionals.
type SkillRecord struct {
	// ID is the opaque, immutable identity (the route/URL key), mirroring
	// sessions. Slug is a cosmetic, non-unique display label and SKILL.md
	// filename derived from the name.
	ID                      string
	Slug                    string
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

// Store is the capability surface the skills API needs from persistence.
// Skills are keyed on an opaque id (the route key); slug is a cosmetic label.
type Store interface {
	// Kind names the backing store ("memory" or "postgres") so responses and
	// logs can never misread a demo as durable.
	Kind() string
	UpsertSkill(ctx context.Context, rec SkillRecord) (*SkillRecord, error)
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
