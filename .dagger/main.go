// skills-cassette CI/CD
//
// Package main provides reproducible builds and tests locally and in GitHub Actions.
package main

import (
	"context"
	"fmt"

	"dagger/skills-cassette/internal/dagger"
)

const (
	testPostgresImage = "public.ecr.aws/g4e5l3z3/papercomputeco/postgres:17.7-pgduckdb-1.1.1"
	testPostgresUser  = "cassette"
	testPostgresPass  = "cassette"
	testPostgresDB    = "cassette"
	testPostgresPort  = 5432
)

// SkillsCassette is the main module for the skills-cassette CI/CD pipeline.
type SkillsCassette struct {
	// Project source directory.
	//
	// +private
	Source *dagger.Directory
}

// New creates a skills-cassette CI/CD module instance.
func New(
	// Project source directory.
	//
	// +defaultPath="/"
	// +ignore=[".git", ".dagger", ".direnv", "build", "tmp"]
	source *dagger.Directory,
) *SkillsCassette {
	return &SkillsCassette{Source: source}
}

// goContainer returns the shared Go container used by tests, builds, and linting.
func (t *SkillsCassette) goContainer() *dagger.Container {
	return dag.Container().
		From("golang:1.26-bookworm").
		WithEnvVariable("CGO_ENABLED", "0").
		WithEnvVariable("PATH", "/go/bin:$PATH", dagger.ContainerWithEnvVariableOpts{Expand: true}).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod")).
		WithMountedCache("/root/.cache/go-build", dag.CacheVolume("go-build")).
		WithWorkdir("/src").
		WithDirectory("/src", t.Source)
}

// testPostgresService provides the real Postgres engine used by storage and
// HTTP integration tests. Dagger waits for the exposed service before running
// the test container.
func testPostgresService() *dagger.Service {
	return dag.Container().
		From(testPostgresImage).
		WithEnvVariable("POSTGRES_USER", testPostgresUser).
		WithEnvVariable("POSTGRES_PASSWORD", testPostgresPass).
		WithEnvVariable("POSTGRES_DB", testPostgresDB).
		WithExposedPort(testPostgresPort).
		AsService(dagger.ContainerAsServiceOpts{UseEntrypoint: true})
}

func testPostgresDSN() string {
	return fmt.Sprintf(
		"host=postgres user=%s password=%s dbname=%s port=%d sslmode=disable",
		testPostgresUser, testPostgresPass, testPostgresDB, testPostgresPort,
	)
}

// Test runs all skills-cassette tests with a bound Postgres service so the
// database-backed Ginkgo specs execute instead of skipping.
//
// +check
func (t *SkillsCassette) Test(ctx context.Context) (string, error) {
	return t.goContainer().
		WithServiceBinding("postgres", testPostgresService()).
		WithEnvVariable("TEST_POSTGRES_DSN", testPostgresDSN()).
		WithEnvVariable("TEST_DATABASE_URL", testPostgresDSN()).
		WithExec([]string{"go", "test", "-count=1", "-v", "./..."}).
		Stdout(ctx)
}
