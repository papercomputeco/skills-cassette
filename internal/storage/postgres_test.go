package storage_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/skills-cassette/internal/storage"
)

var _ = Describe("PostgresStore drafts", func() {
	var (
		ctx    context.Context
		dsn    string
		schema string
		admin  *pgxpool.Pool
	)

	BeforeEach(func() {
		dsn = os.Getenv("TEST_POSTGRES_DSN")
		if dsn == "" {
			Skip("TEST_POSTGRES_DSN is not configured")
		}
		ctx = context.Background()
		schema = "skills_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		var err error
		admin, err = pgxpool.New(ctx, dsn)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if admin != nil {
			_, _ = admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
			admin.Close()
		}
	})

	It("keeps a persisted draft unpublished across restart", func() {
		store, err := storage.OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		now := time.Now().UTC()
		id := uuid.NewString()
		_, err = store.CreateDraft(ctx,
			storage.SkillRecord{ID: id, AuthorSubject: "author", CreatedAt: now, UpdatedAt: now},
			storage.SkillDraftRecord{SkillID: id, Slug: "draft", Name: "Draft", Type: "workflow", Content: "# draft", CreatedAt: now, UpdatedAt: now},
		)
		Expect(err).NotTo(HaveOccurred())
		store.Close()

		store, err = storage.OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		defer store.Close()
		published, err := store.GetSkill(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(published).To(BeNil())
		draft, err := store.GetDraft(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(draft).NotTo(BeNil())
		Expect(draft.Content).To(Equal("# draft"))
	})

	It("conditionally updates and atomically publishes a complete draft", func() {
		store, err := storage.OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		defer store.Close()
		now := time.Now().UTC()
		id := uuid.NewString()
		draft, err := store.CreateDraft(ctx,
			storage.SkillRecord{ID: id, AuthorSubject: "author", CreatedAt: now, UpdatedAt: now},
			storage.SkillDraftRecord{SkillID: id, Slug: "generated", Name: "Generated", Description: "desc", Type: "workflow", Tags: []string{"one"}, Content: "# draft", IsAIGenerated: true, GeneratedFromSessionIDs: []string{"session-1"}, CreatedAt: now, UpdatedAt: now},
		)
		Expect(err).NotTo(HaveOccurred())
		draft.Content = "# revised"
		draft.Name = "Revised"
		draft.GeneratedFromSessionIDs = []string{"tampered"}
		updated, err := store.UpdateDraft(ctx, *draft, 1)
		Expect(err).NotTo(HaveOccurred())
		Expect(updated.Revision).To(Equal(2))
		Expect(updated.GeneratedFromSessionIDs).To(Equal([]string{"session-1"}))
		_, err = store.UpdateDraft(ctx, *updated, 1)
		Expect(err).To(MatchError(storage.ErrDraftRevisionConflict))

		version, err := store.PublishDraft(ctx, id, 2, "first", "publisher", now.Add(time.Minute))
		Expect(err).NotTo(HaveOccurred())
		Expect(version.Name).To(Equal("Revised"))
		Expect(version.Content).To(Equal("# revised"))
		Expect(version.GeneratedFromSessionIDs).To(Equal([]string{"session-1"}))
		remaining, err := store.GetDraft(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(remaining).To(BeNil())
		published, err := store.GetSkill(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(published.Name).To(Equal("Revised"))
		Expect(published.Content).To(Equal("# revised"))
	})

	It("migrates a legacy row without history as published", func() {
		createLegacySchema(ctx, admin, schema)
		id := uuid.NewString()
		now := time.Now().UTC()
		_, err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skills (
			id,slug,name,description,type,version,visibility,tags,content,is_ai_generated,
			generated_from_session_ids,author_subject,created_at,updated_at)
			VALUES ($1,'legacy','Legacy','desc','workflow','0.1.0','team','{}','# legacy',TRUE,
			ARRAY['session-1'],'author',$2,$2)`, schema), id, now)
		Expect(err).NotTo(HaveOccurred())
		orphanID := uuid.NewString()
		_, err = admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_versions
			(skill_id,version_number,semver,changelog,content,author_subject,published_at)
			VALUES ($1,1,'0.1.0','','# orphan','',$2)`, schema), orphanID, now)
		Expect(err).NotTo(HaveOccurred())

		store, err := storage.OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		defer store.Close()
		var orphanCount int
		Expect(admin.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s.skill_versions WHERE skill_id=$1`, schema), orphanID).Scan(&orphanCount)).To(Succeed())
		Expect(orphanCount).To(BeZero())
		published, err := store.GetSkill(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(published).NotTo(BeNil())
		Expect(published.Content).To(Equal("# legacy"))
		Expect(published.GeneratedFromSessionIDs).To(Equal([]string{"session-1"}))
		draft, err := store.GetDraft(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(draft).To(BeNil())
	})

	It("preserves prerelease status drafts without publishing them", func() {
		createLegacySchema(ctx, admin, schema)
		_, err := admin.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s.skills ADD COLUMN status TEXT NOT NULL DEFAULT 'draft'`, schema))
		Expect(err).NotTo(HaveOccurred())
		id := uuid.NewString()
		now := time.Now().UTC()
		_, err = admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skills (
			id,slug,name,description,type,version,visibility,tags,content,is_ai_generated,
			generated_from_session_ids,author_subject,created_at,updated_at,status)
			VALUES ($1,'draft','Draft','','workflow','0.1.0','private','{}','# draft',FALSE,
			'{}','author',$2,$2,'draft')`, schema), id, now)
		Expect(err).NotTo(HaveOccurred())

		store, err := storage.OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		defer store.Close()
		published, err := store.GetSkill(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(published).To(BeNil())
		draft, err := store.GetDraft(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(draft).NotTo(BeNil())
		Expect(draft.Content).To(Equal("# draft"))
	})

	It("preserves a divergent legacy working head as a draft", func() {
		createLegacySchema(ctx, admin, schema)
		id := uuid.NewString()
		now := time.Now().UTC()
		_, err := admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skills (
			id,slug,name,description,type,version,visibility,tags,content,is_ai_generated,
			generated_from_session_ids,author_subject,created_at,updated_at)
			VALUES ($1,'changed','Changed','new description','workflow','0.1.0','team',ARRAY['new'],'# working',TRUE,
			ARRAY['session-new'],'author',$2,$2)`, schema), id, now)
		Expect(err).NotTo(HaveOccurred())
		_, err = admin.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.skill_versions
			(skill_id,version_number,semver,changelog,content,author_subject,published_at)
			VALUES ($1,1,'0.1.0','first','# published','publisher',$2)`, schema), id, now.Add(-time.Hour))
		Expect(err).NotTo(HaveOccurred())

		store, err := storage.OpenPostgresStore(ctx, dsn, schema)
		Expect(err).NotTo(HaveOccurred())
		defer store.Close()
		published, err := store.GetSkill(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(published.Content).To(Equal("# published"))
		draft, err := store.GetDraft(ctx, id)
		Expect(err).NotTo(HaveOccurred())
		Expect(draft).NotTo(BeNil())
		Expect(draft.Content).To(Equal("# working"))
		Expect(draft.GeneratedFromSessionIDs).To(Equal([]string{"session-new"}))
	})
})

func createLegacySchema(ctx context.Context, pool *pgxpool.Pool, schema string) {
	_, err := pool.Exec(ctx, `CREATE SCHEMA `+schema)
	Expect(err).NotTo(HaveOccurred())
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.skills (
		id UUID PRIMARY KEY, slug TEXT NOT NULL, name TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '', type TEXT NOT NULL DEFAULT 'workflow',
		version TEXT NOT NULL DEFAULT '0.1.0', visibility TEXT NOT NULL DEFAULT 'private',
		tags TEXT[] NOT NULL DEFAULT '{}', content TEXT NOT NULL DEFAULT '',
		is_ai_generated BOOLEAN NOT NULL DEFAULT FALSE,
		generated_from_session_ids TEXT[] NOT NULL DEFAULT '{}', parent_id UUID,
		author_subject TEXT NOT NULL DEFAULT '', download_count BIGINT NOT NULL DEFAULT 0,
		created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL
	)`, schema))
	Expect(err).NotTo(HaveOccurred())
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.skill_versions (
		skill_id UUID NOT NULL, version_number INT NOT NULL, semver TEXT NOT NULL,
		changelog TEXT NOT NULL DEFAULT '', content TEXT NOT NULL DEFAULT '',
		author_subject TEXT NOT NULL DEFAULT '', published_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (skill_id, version_number)
	)`, schema))
	Expect(err).NotTo(HaveOccurred())
}
