package server

import (
	"os"
	"testing"
)

// TestReleaseIdentityIsStamped proves that `-ldflags -X` actually reaches
// Version, which is the one thing the stamping scheme cannot take on
// faith: -X writes to package-level variables only, and silently does nothing
// when its target is a constant, is renamed, or moves package. Any of those
// refactors would keep compiling, keep passing every other test, and ship
// images that report the placeholder version forever — the failure this
// scheme exists to prevent, reintroduced by the mechanism meant to fix it.
//
// The release runs this with the same flag it builds the binary with (see
// .dagger/main.go) and refuses to publish when it fails. An ordinary `go
// test ./...` skips it: without the flag there is no stamp to check.
//
// It is a plain test rather than a Ginkgo spec so the pipeline can select it
// by name with -run; the Ginkgo suite entry point does not match that filter.
func TestReleaseIdentityIsStamped(t *testing.T) {
	want := os.Getenv("CASSETTE_VERSION_WANT")
	if want == "" {
		t.Skip("unstamped build: no CASSETTE_VERSION_WANT to check against")
	}
	if Version != want {
		t.Fatalf("release identity did not reach the binary: Version = %q, want %q\n"+
			"the -X flag names internal/server.Version; check it is still a package-level var there",
			Version, want)
	}
	if Version == Placeholder {
		t.Fatalf("release identity is the placeholder %q; a release must stamp a real version", Placeholder)
	}
}
