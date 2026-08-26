package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"

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
	// Filters are the deployment-configured external attachment-view
	// filters (CASSETTE_FILTERS), validated at startup. Absent: the
	// capability is off with zero behavioral change.
	Filters []ExternalFilter
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
// loopback address or a Kubernetes cluster-local Service name (a plaintext
// core URL crossing a real network would leak every transcript it reads).
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
	if parsed.Scheme == "http" && !isPlaintextSafeHost(parsed.Hostname()) {
		return fmt.Errorf("core url %q must use https for non-loopback targets", raw)
	}
	return nil
}

// isPlaintextSafeHost reports whether host may be spoken to over plain http:
// the local machine, or a Kubernetes cluster-local Service DNS name. Service
// names (`<svc>.<ns>.svc` and `<svc>.<ns>.svc.cluster.local`) resolve only
// inside a cluster, where pod-to-pod traffic never crosses the cluster
// boundary and no in-cluster TLS exists to require — this is the URL shape a
// deployment orchestrator hands the cassette for its tenant-local core.
func isPlaintextSafeHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	lower := strings.ToLower(strings.TrimSuffix(host, "."))
	return strings.HasSuffix(lower, ".svc") || strings.HasSuffix(lower, ".svc.cluster.local")
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

// ExternalFilter is one deployment-configured attachment-view filter: a
// repeatable query param on the skills list wired to an external view of the
// canonical attachment shape (primitive_type, primitive_id, value). The
// manifest declares only this generic schema; every value here — the param
// name, the view it reads, the type it selects on — is deployment-supplied,
// so no external product's names are compiled into this repository.
type ExternalFilter struct {
	// Param is the query param the deployment claims on the skills list.
	Param string `json:"param"`
	// View is the schema-qualified relation of the canonical attachment
	// shape this filter reads. SELECT grants on it are deployment-owned.
	View string `json:"view"`
	// TypeValue is the primitive_type discriminator selecting this
	// surface's rows in the view.
	TypeValue string `json:"type_value"`
	// Normalize names the verbs applied, in order, to each supplied value
	// before it binds. Vocabulary: trim, nfc, casefold.
	Normalize []string `json:"normalize,omitempty"`
}

// reservedListParams are the skills list's own query params. A configured
// external filter may not claim one — the same reserved-param discipline the
// platform applies to cassette filter claims, derived from this surface's
// documented parameters (openapi.go's listSkills operation).
var reservedListParams = map[string]bool{
	"limit":      true,
	"cursor":     true,
	"q":          true,
	"scope":      true,
	"sort":       true,
	"session_id": true,
}

// lowerSnakeToken is the grammar shared by filter params, view segments, and
// type values: lowercase snake tokens of at most 63 bytes, matching the
// platform's identifier admission rules.
var lowerSnakeToken = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// normalizeVerbs is the supported normalization vocabulary.
var normalizeVerbs = map[string]bool{"trim": true, "nfc": true, "casefold": true}

// ExternalFiltersFromEnv parses and validates the deployment-supplied
// external-filter configuration from CASSETTE_FILTERS. Invalid configuration
// refuses startup — a deployment that asked for a filter must get it or know
// why not; an absent value simply turns the capability off.
func ExternalFiltersFromEnv() ([]ExternalFilter, error) {
	return ParseExternalFilters(os.Getenv("CASSETTE_FILTERS"))
}

// ParseExternalFilters strictly decodes and validates an external-filter
// configuration document: a JSON list of {param, view, type_value,
// normalize} entries. Empty input means the capability is off.
func ParseExternalFilters(raw string) ([]ExternalFilter, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var filters []ExternalFilter
	if err := decoder.Decode(&filters); err != nil {
		return nil, fmt.Errorf("external filters: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("external filters: trailing JSON value")
	}

	seen := make(map[string]int, len(filters))
	for index, filter := range filters {
		if err := validateExternalFilter(filter); err != nil {
			return nil, fmt.Errorf("external filters[%d]: %w", index, err)
		}
		if previous, exists := seen[filter.Param]; exists {
			return nil, fmt.Errorf("external filters[%d]: param %q duplicates filters[%d]", index, filter.Param, previous)
		}
		seen[filter.Param] = index
	}
	return filters, nil
}

// validateExternalFilter enforces the configuration grammar. Configured
// strings later reach an identifier position (the view) and a query-param
// namespace (the param), so both are held to strict token grammars — the
// quoting in storage is belt and braces, not the defense.
func validateExternalFilter(filter ExternalFilter) error {
	if !lowerSnakeToken.MatchString(filter.Param) {
		return fmt.Errorf("param %q must be a lowercase snake token of at most 63 bytes", filter.Param)
	}
	if reservedListParams[filter.Param] {
		return fmt.Errorf("param %q is owned by the skills list itself", filter.Param)
	}
	segments := strings.Split(filter.View, ".")
	if len(segments) != 2 {
		return fmt.Errorf("view %q must be a schema-qualified relation (schema.view)", filter.View)
	}
	for _, segment := range segments {
		if !lowerSnakeToken.MatchString(segment) {
			return fmt.Errorf("view %q segments must be lowercase snake tokens of at most 63 bytes", filter.View)
		}
	}
	if !lowerSnakeToken.MatchString(filter.TypeValue) {
		return fmt.Errorf("type_value %q must be a lowercase snake token of at most 63 bytes", filter.TypeValue)
	}
	for _, verb := range filter.Normalize {
		if !normalizeVerbs[verb] {
			return fmt.Errorf("unknown normalize verb %q (supported: trim, nfc, casefold)", verb)
		}
	}
	return nil
}

// NormalizeFilterValue applies the configured normalize verbs to one filter
// value, in the configured order. Normalization happens exactly once, here
// at the API boundary — never per-row in SQL, where it would defeat index
// probes on the external view.
func NormalizeFilterValue(value string, verbs []string) string {
	for _, verb := range verbs {
		switch verb {
		case "trim":
			value = strings.TrimSpace(value)
		case "nfc":
			value = norm.NFC.String(value)
		case "casefold":
			value = cases.Fold().String(value)
		}
	}
	return value
}
