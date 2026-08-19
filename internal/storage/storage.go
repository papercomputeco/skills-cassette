// Package storage owns the skills cassette's durable identities, mutable drafts,
// and immutable published versions. One cassette installation serves one tenant.
package storage

import (
	"context"
	"errors"
	"time"
)

var (
	ErrSkillExists           = errors.New("skill already exists")
	ErrDraftExists           = errors.New("skill draft already exists")
	ErrDraftNotFound         = errors.New("skill draft not found")
	ErrDraftRevisionConflict = errors.New("skill draft revision conflict")
)

const DefaultListLimit = 24

// SkillRecord is a stable skill identity joined to its current immutable
// published version. VersionNumber == 0 means the identity has never published.
type SkillRecord struct {
	ID                   string
	ParentID             string
	AuthorSubject        string
	Visibility           string
	DownloadCount        int64
	CurrentVersionNumber int
	CreatedAt            time.Time
	UpdatedAt            time.Time

	Slug                    string
	Name                    string
	Description             string
	Type                    string
	Version                 string
	Tags                    []string
	Content                 string
	IsAIGenerated           bool
	GeneratedFromSessionIDs []string
}

// SkillDraftRecord is the one mutable working copy for a skill. Revision is
// incremented on every successful write and used for optimistic concurrency.
type SkillDraftRecord struct {
	SkillID                 string
	Revision                int
	Slug                    string
	Name                    string
	Description             string
	Type                    string
	Tags                    []string
	Content                 string
	IsAIGenerated           bool
	GeneratedFromSessionIDs []string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

// SkillVersionRecord is a complete immutable published snapshot.
type SkillVersionRecord struct {
	SkillID                 string
	VersionNumber           int
	Semver                  string
	Slug                    string
	Name                    string
	Description             string
	Type                    string
	Tags                    []string
	Content                 string
	IsAIGenerated           bool
	GeneratedFromSessionIDs []string
	Changelog               string
	AuthorSubject           string
	PublishedAt             time.Time
}

type SkillListOpts struct {
	Query     string
	Author    string
	NotAuthor string
	Sort      string

	CursorTs        *time.Time
	CursorDownloads *int64
	CursorID        string
	Limit           int
}

const SkillSortDownloads = "downloads"

type SkillCounts struct {
	Total int64
	Mine  int64
}

// Store keeps draft writes and publication transitions atomic in every driver.
type Store interface {
	Kind() string
	CreateDraft(ctx context.Context, identity SkillRecord, draft SkillDraftRecord) (*SkillDraftRecord, error)
	CreateDraftFromPublished(ctx context.Context, skillID string, now time.Time) (*SkillDraftRecord, error)
	GetDraft(ctx context.Context, skillID string) (*SkillDraftRecord, error)
	ListDrafts(ctx context.Context) ([]SkillDraftRecord, error)
	UpdateDraft(ctx context.Context, draft SkillDraftRecord, expectedRevision int) (*SkillDraftRecord, error)
	PublishDraft(ctx context.Context, skillID string, expectedRevision int, changelog, author string, publishedAt time.Time) (*SkillVersionRecord, error)

	GetSkillIdentity(ctx context.Context, id string) (*SkillRecord, error)
	GetSkill(ctx context.Context, id string) (*SkillRecord, error)
	ListSkills(ctx context.Context, opts SkillListOpts) ([]SkillRecord, error)
	ListSkillsBySession(ctx context.Context, sessionID string) ([]SkillRecord, error)
	CountSkills(ctx context.Context, query, author string) (SkillCounts, error)
	ListSkillVersions(ctx context.Context, skillID string) ([]SkillVersionRecord, error)
	IncrementSkillDownloads(ctx context.Context, id string) error
	DeleteSkill(ctx context.Context, id string) (bool, error)
	Close()
}
