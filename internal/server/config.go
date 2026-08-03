package server

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

// DefaultName is the cassette identity everything derives from: the local
// route prefix (/api/skills), the public route (/v1/cassettes/skills), and
// the Postgres schema ("skills").
const DefaultName = "skills"

// Config is everything the cassette process reads from its environment. The
// deployment supplies it; the cassette has no config file and no knowledge of
// which runtime started it. Keys mirror the manifest's config schema through
// the conventional CASSETTE_* environment names.
type Config struct {
	// Name is the installed cassette name (route prefix and schema).
	Name string
	// CoreURL is the Tapes core API origin the generator reads trace
	// transcripts from (GET /v1/traces?session_id= and GET /v1/traces/{id}).
	CoreURL string
	// LLM configures the provider used by POST generate.
	LLM skill.LLMCallerConfig
}

// ConfigFromEnv reads the manifest-declared configuration from the
// conventional CASSETTE_* environment variables.
func ConfigFromEnv() Config {
	return Config{
		Name:    envOrDefault("CASSETTE_NAME", DefaultName),
		CoreURL: strings.TrimSpace(os.Getenv("CASSETTE_CORE_URL")),
		LLM: skill.LLMCallerConfig{
			Provider: strings.TrimSpace(os.Getenv("CASSETTE_LLM_PROVIDER")),
			Model:    strings.TrimSpace(os.Getenv("CASSETTE_LLM_MODEL")),
			APIKey:   strings.TrimSpace(os.Getenv("CASSETTE_LLM_API_KEY")),
			BaseURL:  strings.TrimSpace(os.Getenv("CASSETTE_LLM_BASE_URL")),
		},
	}
}

// ValidateCoreURL checks the configured core target. An empty value is
// allowed — generation then answers 501 rather than the process refusing to
// start — but a configured value must be a full http(s) URL with a host and
// no userinfo, query, or fragment, and must use TLS unless it points at a
// loopback address (a plaintext core URL crossing a network would leak every
// transcript it reads).
func ValidateCoreURL(raw string) error {
	if raw == "" {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("core url %q: %w", raw, err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Hostname() == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("core url %q must be a full http(s) URL with a host and no userinfo, query, or fragment", raw)
	}
	if parsed.Scheme == "http" && !isLoopbackHost(parsed.Hostname()) {
		return fmt.Errorf("core url %q must use https for non-loopback targets", raw)
	}
	return nil
}

// isLoopbackHost reports whether host names the local machine.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
