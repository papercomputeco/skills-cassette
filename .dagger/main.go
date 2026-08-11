// skills-cassette CI/CD
//
// Package main provides reproducible builds and tests locally and in GitHub Actions.
package main

import (
	"context"

	"dagger/skills-cassette/internal/dagger"
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
		From("golang:1.25-bookworm").
		WithEnvVariable("CGO_ENABLED", "0").
		WithEnvVariable("PATH", "/go/bin:$PATH", dagger.ContainerWithEnvVariableOpts{Expand: true}).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod")).
		WithMountedCache("/root/.cache/go-build", dag.CacheVolume("go-build")).
		WithWorkdir("/src").
		WithDirectory("/src", t.Source)
}

// Test runs the skills-cassette unit tests.
//
// +check
func (t *SkillsCassette) Test(ctx context.Context) (string, error) {
	return t.goContainer().
		WithExec([]string{"go", "test", "-v", "./..."}).
		Stdout(ctx)
}
