// Package skillscassettecmder provides the skills-cassette command surface.
package skillscassettecmder

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/papercomputeco/skills-cassette/internal/server"
	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

// Version is overridden by release builds when desired.
var Version = "dev"

// NewSkillsCassetteCmd creates the root command.
func NewSkillsCassetteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "skills-cassette",
		Short:         "Generate reusable skills from Tapes sessions",
		Version:       Version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(newServeCmd())
	return cmd
}

func newServeCmd() *cobra.Command {
	address := "127.0.0.1:8080"
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the skills cassette server",
		Long: `Run the skills cassette: the /ping and /openapi anchors Tapes core probes,
and the skills API under /api/<name> that core republishes at
/v1/cassettes/<name>.

Configuration arrives through the environment supplied by the deployment:

  CASSETTE_NAME          installed cassette name (default "skills")
  CASSETTE_CORE_URL      Tapes core API origin for reading trace transcripts;
                         https is required except for loopback and
                         cluster-local Service targets
  CASSETTE_LLM_PROVIDER  openai (default), anthropic, or ollama
  CASSETTE_LLM_MODEL     model override
  CASSETTE_LLM_API_KEY   provider API key (falls back to OPENAI_API_KEY /
                         ANTHROPIC_API_KEY)
  CASSETTE_LLM_BASE_URL  provider base URL override
  CASSETTE_FILTERS       external attachment-view filters: a JSON list of
                         {param, view, type_value, normalize} entries; absent
                         turns the capability off
  TAPES_DATABASE_URL     Postgres DSN; without one skills are stored in a
                         non-durable in-memory store`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServe(cmd.Context(), address)
		},
	}
	cmd.Flags().StringVar(&address, "listen", address, "listen address")
	return cmd
}

func runServe(ctx context.Context, address string) error {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg := server.ConfigFromEnv()

	if err := server.ValidateCoreURL(cfg.CoreURL); err != nil {
		return err
	}
	filters, err := server.ExternalFiltersFromEnv()
	if err != nil {
		return err
	}
	cfg.Filters = filters
	var querier skill.Querier
	if cfg.CoreURL != "" {
		querier = skill.NewAPIClient(cfg.CoreURL)
	} else {
		logger.Warn("no CASSETTE_CORE_URL configured; skill generation is disabled")
	}

	store, err := openStore(ctx, strings.TrimSpace(os.Getenv("TAPES_DATABASE_URL")), cfg.Name)
	if err != nil {
		return fmt.Errorf("open skills store: %w", err)
	}
	defer store.Close()

	logger.Info("skills cassette listening",
		"listen", address, "name", cfg.Name, "store", store.Kind())
	return server.New(cfg, store, querier, logger).ListenAndServe(ctx, address)
}

// openStore picks the cassette's persistence: Postgres when the deployment
// supplies a DSN, otherwise a non-durable in-memory store so the cassette is
// runnable with nothing configured.
func openStore(ctx context.Context, dsn, schema string) (storage.Store, error) {
	if dsn == "" {
		return storage.NewMemoryStore(), nil
	}
	return storage.OpenPostgresStore(ctx, dsn, schema)
}
