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
