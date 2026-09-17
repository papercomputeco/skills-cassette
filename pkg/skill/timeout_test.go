package skill

import (
	"testing"
	"time"
)

func TestCallTimeoutFrom(t *testing.T) {
	cases := map[string]time.Duration{
		"":         llmCallTimeout,
		"5s":       llmCallTimeout, // shorter never applies
		"30s":      llmCallTimeout, // equal is not an extension
		"nonsense": llmCallTimeout,
		"-1m":      llmCallTimeout,
		"5m":       5 * time.Minute,
		" 2h ":     2 * time.Hour,
	}
	for raw, want := range cases {
		if got := callTimeoutFrom(raw); got != want {
			t.Errorf("callTimeoutFrom(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestCallTimeoutReadsEnv(t *testing.T) {
	t.Setenv("CASSETTE_LLM_TIMEOUT", "10m")
	if got := callTimeout(); got != 10*time.Minute {
		t.Fatalf("callTimeout() = %v, want 10m", got)
	}
	t.Setenv("CASSETTE_LLM_TIMEOUT", "")
	if got := callTimeout(); got != llmCallTimeout {
		t.Fatalf("callTimeout() unset = %v, want %v", got, llmCallTimeout)
	}
}
