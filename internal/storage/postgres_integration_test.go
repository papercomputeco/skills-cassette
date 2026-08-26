package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPostgresConditionalPublish(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	schema := "skills_cas_" + uuid.NewString()[:8]
	store, err := OpenPostgresStore(ctx, dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = store.pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", quoteIdentifier(schema)))
		store.Close()
	}()

	now := time.Now().UTC()
	id := uuid.NewString()
	_, err = store.UpsertSkill(ctx, SkillRecord{
		ID: id, Slug: "cas", Name: "CAS", Content: "# original",
		Type: "workflow", Version: "0.1.0", Visibility: "private",
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	expected := "# original"
	_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
		SkillID: id, VersionNumber: 1, Semver: "0.1.0", Content: "# revised",
		ExpectedContent: &expected, PublishedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	versions, err := store.ListSkillVersions(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 1 || versions[0].ExpectedContent == nil || *versions[0].ExpectedContent != expected {
		t.Fatalf("stored expected content = %#v, want %q", versions, expected)
	}

	_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
		SkillID: id, VersionNumber: 2, Semver: "0.1.1", Content: "# overwrite",
		ExpectedContent: &expected, PublishedAt: now,
	})
	if !errors.Is(err, ErrSkillChanged) {
		t.Fatalf("stale conditional publish error = %v, want %v", err, ErrSkillChanged)
	}

	_, err = store.PublishSkillVersion(ctx, SkillVersionRecord{
		SkillID: uuid.NewString(), VersionNumber: 1, Semver: "0.1.0", Content: "# orphan",
		PublishedAt: now,
	})
	if !errors.Is(err, ErrSkillChanged) {
		t.Fatalf("missing-skill publish error = %v, want %v", err, ErrSkillChanged)
	}
}

// TestPostgresExternalAttachmentFilter pins the storage half of the
// deployment-configured external-filter capability: the probe, the
// EXISTS-per-value rendering against a fixture view of the canonical
// attachment shape, and the typed missing-relation error once the view is
// gone. The fixture is created by the test; no external product is installed
// or referenced.
func TestPostgresExternalAttachmentFilter(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN / TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	suffix := uuid.NewString()[:8]
	schema := "skills_extf_" + suffix
	fixture := "attach_fixt_" + suffix
	store, err := OpenPostgresStore(ctx, dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = store.pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", quoteIdentifier(fixture)))
		_, _ = store.pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", quoteIdentifier(schema)))
		store.Close()
	}()

	// The probe must refuse a view that does not exist yet.
	view := fixture + ".attachments"
	if err := store.ProbeExternalView(ctx, view); err == nil {
		t.Fatal("probe of a missing view must fail")
	}

	for _, statement := range []string{
		fmt.Sprintf(`CREATE SCHEMA %s`, quoteIdentifier(fixture)),
		fmt.Sprintf(`CREATE TABLE %s.rows (
			primitive_type text NOT NULL,
			primitive_id   text NOT NULL,
			value          text NOT NULL
		)`, quoteIdentifier(fixture)),
		fmt.Sprintf(`CREATE VIEW %s.attachments AS
			SELECT primitive_type, primitive_id, value FROM %s.rows`,
			quoteIdentifier(fixture), quoteIdentifier(fixture)),
	} {
		if _, err := store.pool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ProbeExternalView(ctx, view); err != nil {
		t.Fatalf("probe of the fixture view failed: %v", err)
	}

	now := time.Now().UTC()
	matching, other := uuid.NewString(), uuid.NewString()
	for i, id := range []string{matching, other} {
		if _, err := store.UpsertSkill(ctx, SkillRecord{
			ID: id, Slug: fmt.Sprintf("s-%d", i), Name: fmt.Sprintf("S %d", i),
			Type: "workflow", Version: "0.1.0", Visibility: "private",
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range [][3]string{
		{"skill", matching, "alpha"},
		{"skill", matching, "beta"},
		{"skill", other, "beta"},
	} {
		if _, err := store.pool.Exec(ctx,
			fmt.Sprintf(`INSERT INTO %s.rows (primitive_type, primitive_id, value) VALUES ($1, $2, $3)`,
				quoteIdentifier(fixture)),
			row[0], row[1], row[2]); err != nil {
			t.Fatal(err)
		}
	}

	filter := []ExternalAttachmentFilter{{View: view, TypeValue: "skill", Values: []string{"alpha", "beta"}}}
	recs, err := store.ListSkills(ctx, SkillListOpts{External: filter})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].ID != matching {
		t.Fatalf("filtered list = %#v, want exactly the skill carrying every value", recs)
	}

	// Broken after the probe: the typed error, never a silently unfiltered page.
	if _, err := store.pool.Exec(ctx,
		fmt.Sprintf(`DROP VIEW %s.attachments`, quoteIdentifier(fixture))); err != nil {
		t.Fatal(err)
	}
	_, err = store.ListSkills(ctx, SkillListOpts{External: filter})
	if !errors.Is(err, ErrExternalViewUnavailable) {
		t.Fatalf("broken-view list error = %v, want %v", err, ErrExternalViewUnavailable)
	}

	// Without the filter the list still serves.
	if _, err := store.ListSkills(ctx, SkillListOpts{}); err != nil {
		t.Fatal(err)
	}
}
