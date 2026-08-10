package main

import (
	"context"

	"dagger/skills-cassette/internal/dagger"
)

const imageName = "skills-cassette"

func (t *SkillsCassette) image(version string) *dagger.Dockerimage {
	if version == "" {
		version = "dev"
	}

	return dag.Dockerimage(dagger.DockerimageOpts{Source: t.Source}).
		WithBuildArg("LDFLAGS", releaseLDFlags(version))
}

// BuildImage builds the local-platform skills cassette image.
func (t *SkillsCassette) BuildImage(
	// Version embedded in the binary.
	// +optional
	// +default="dev"
	version string,
) *dagger.Container {
	return t.image(version).Build()
}

// BuildPushImage builds and publishes the multi-platform skills cassette image.
func (t *SkillsCassette) BuildPushImage(
	ctx context.Context,

	// Registry namespace, for example public.ecr.aws/example/papercomputeco.
	registry string,

	// Tags to publish, for example ["v1.0.0", "latest"].
	tags []string,

	// Version embedded in the binary.
	// +optional
	// +default="dev"
	version string,
) ([]string, error) {
	return t.image(version).Publish(ctx, registry+"/"+imageName, tags)
}
