package main

import (
	"context"
	"fmt"

	"dagger/skills-cassette/internal/dagger"
)

type buildTarget struct {
	goos   string
	goarch string
}

// Build compiles skills-cassette for all supported platforms.
func (t *SkillsCassette) Build(
	_ context.Context,

	// Linker flags for go build.
	// +optional
	// +default="-s -w"
	ldflags string,
) *dagger.Directory {
	targets := []buildTarget{
		{"linux", "amd64"},
		{"linux", "arm64"},
		{"darwin", "amd64"},
		{"darwin", "arm64"},
	}

	golang := t.goContainer()
	outputs := dag.Directory()

	for _, target := range targets {
		path := fmt.Sprintf("%s/%s/", target.goos, target.goarch)

		build := golang.
			WithEnvVariable("GOOS", target.goos).
			WithEnvVariable("GOARCH", target.goarch).
			WithExec([]string{
				"go", "build", "-ldflags", ldflags,
				"-o", path + "skills-cassette", "./cli/skills-cassette",
			})

		outputs = outputs.WithDirectory(path, build.Directory(path))
	}

	return outputs
}

// BuildRelease compiles development-identified release binaries and adds
// SHA256 checksums. Versioned publishing uses buildVersionedRelease below.
func (t *SkillsCassette) BuildRelease(ctx context.Context) *dagger.Directory {
	return t.buildVersionedRelease(ctx, "dev")
}

func releaseLDFlags(version string) string {
	return fmt.Sprintf(
		"-s -w -X github.com/papercomputeco/skills-cassette/cmd/skills-cassette.Version=%s",
		version,
	)
}

func (t *SkillsCassette) buildVersionedRelease(ctx context.Context, version string) *dagger.Directory {
	return t.checksum(t.Build(ctx, releaseLDFlags(version)))
}

// checksum generates a SHA256 sidecar for every artifact.
func (t *SkillsCassette) checksum(dir *dagger.Directory) *dagger.Directory {
	return dag.Container().
		From("alpine:latest").
		WithDirectory("/artifacts", dir).
		WithWorkdir("/artifacts").
		WithExec([]string{"sh", "-c", `
			find . -type f ! -name "*.sha256" | while read file; do
				dir="$(dirname "$file")"
				name="$(basename "$file")"
				(cd "$dir" && sha256sum "$name" > "$name.sha256")
			done
		`}).
		Directory("/artifacts")
}
