package main

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"dagger/skills-cassette/internal/dagger"
)

const imageName = "skills-cassette"

// manifestVersionSymbol is the variable holding the identity this cassette
// publishes in its manifest and serves at /openapi. It lives in a non-main
// package on purpose: `-X` addresses a variable by import path, and a symbol
// in package main is addressed as "main.X" when linking a binary but by its
// real import path when compiled as the package under test — so a stamp there
// could only ever be verified through a different flag than the one that
// ships. Here one flag string works for both, which is what lets
// verifyStamp prove the exact stamp being published.
const manifestVersionSymbol = "github.com/papercomputeco/skills-cassette/internal/server.Version"

// cliVersionSymbol is what `skills-cassette --version` reports.
const cliVersionSymbol = "github.com/papercomputeco/skills-cassette/cmd/skills-cassette.Version"

// manifestVersionFlag stamps the manifest identity. It takes the bare version:
// the manifest composes its image reference as ":v" + Version, where the CLI
// version keeps the tag as released.
func manifestVersionFlag(bare string) string {
	return "-X " + manifestVersionSymbol + "=" + bare
}

// releaseTag matches a version that names a real release: the manifest
// advertises its own image as ":v<version>", so only this shape can be
// stamped without the manifest naming a reference that was never published.
var releaseTag = regexp.MustCompile(`^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)*$`)

// manifestStamp reports whether a publish is a release whose identity belongs
// in the manifest, and returns the bare version to stamp.
//
// The nightly workflow publishes with version "nightly", which is neither a
// version the manifest's schema accepts nor a tag that ":v"+it would resolve
// to — a nightly image advertising ":vnightly" would send an orchestrator to
// an image that does not exist. Nightlies keep the placeholder for the same
// reason a source tree does: they are not releases. The CLI version is still
// stamped, so `skills-cassette --version` reports "nightly" as it always has.
func manifestStamp(version string) (string, bool) {
	bare := strings.TrimPrefix(version, "v")
	if !releaseTag.MatchString(bare) {
		return "", false
	}
	return bare, true
}

// releaseLDFlags stamps the CLI identity always, and the manifest identity for
// releases, so the version a running cassette reports in its manifest and the
// version its binary reports on the command line cannot disagree.
func releaseLDFlags(version string) string {
	flags := fmt.Sprintf("-s -w -X %s=%s", cliVersionSymbol, version)
	if bare, ok := manifestStamp(version); ok {
		flags += " " + manifestVersionFlag(bare)
	}
	return flags
}

// verifyStamp proves the manifest identity reaches the binary before anything
// is published, using the flag byte-identical to the one the image build uses.
// `-X` silently does nothing to a constant or a renamed symbol, so a refactor
// could otherwise publish images reporting the placeholder forever — with the
// build, the tests, and the manifest digest all still green.
func (t *SkillsCassette) verifyStamp(ctx context.Context, bare string) error {
	_, err := t.goContainer().
		WithEnvVariable("CASSETTE_VERSION_WANT", bare).
		WithExec([]string{
			"go", "test", "-count=1", "-run", "TestReleaseIdentityIsStamped",
			"-ldflags", manifestVersionFlag(bare), "./internal/server",
		}).
		Sync(ctx)
	return err
}

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
	if bare, ok := manifestStamp(version); ok {
		// The manifest will advertise ":v<bare>", so that exact tag has to be
		// among the ones being published — otherwise the manifest names an
		// image reference that does not exist. This is what catches a release
		// tagged "0.3.0" rather than "v0.3.0": it would publish ":0.3.0"
		// while the manifest advertised ":v0.3.0".
		advertised := "v" + bare
		if !slices.Contains(tags, advertised) {
			return nil, fmt.Errorf(
				"manifest will advertise image tag %q, which is not among the tags being published (%v): release tags must be v-prefixed",
				advertised, tags)
		}
		if err := t.verifyStamp(ctx, bare); err != nil {
			return nil, err
		}
	}
	return t.image(version).Publish(ctx, registry+"/"+imageName, tags)
}
