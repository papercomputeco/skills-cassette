// Package server provides the skills cassette's HTTP service: the /ping and
// /openapi anchors core probes and fetches, and the skills API under the
// declared local prefix (/api/<name>) that core republishes at
// /v1/cassettes/<name>.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const shutdownTimeout = 5 * time.Second

// externalFilterProbeTimeout bounds the one-time startup probe of a single
// configured external attachment view. The budget is per view: one slow or
// hanging probe exhausts only its own deadline, never the deadline of the
// views probed after it. It is a var only so tests can shrink it.
var externalFilterProbeTimeout = 5 * time.Second

// Server is the whole cassette: an identity, a store for skills, a querier
// for reading trace transcripts off the core, and an LLM configuration for
// the generator.
type Server struct {
	name    string
	store   storage.Store
	querier skill.Querier
	llm     skill.LLMCallerConfig
	logger  *slog.Logger
	openapi []byte
	// filters are the external attachment-view filters that survived the
	// startup availability probe. Only these params are ever parsed.
	filters []ExternalFilter
}

// New builds the cassette server. querier may be nil when no core URL is
// configured — generation then answers 501 while the rest of the API serves.
func New(cfg Config, store storage.Store, querier skill.Querier, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	name := cfg.Name
	if name == "" {
		name = DefaultName
	}
	return &Server{
		name:    name,
		store:   store,
		querier: querier,
		llm:     cfg.LLM,
		logger:  logger,
		openapi: openAPIDocument(name),
		filters: armExternalFilters(cfg.Filters, store, logger),
	}
}

// armExternalFilters probes each configured external attachment view once
// and returns the filters whose views are readable. Absent is cheap: an
// unreadable view (or a store that reads no external views at all) leaves
// that filter unarmed, its param ignored with zero behavioral change. The
// distinct case — a view that breaks after arming — stays loud at request
// time (ErrExternalViewUnavailable, 503); there is no per-request re-probe
// and no fallback.
func armExternalFilters(filters []ExternalFilter, store storage.Store, logger *slog.Logger) []ExternalFilter {
	if len(filters) == 0 {
		return nil
	}
	prober, ok := store.(storage.ExternalViewProber)
	if !ok {
		logger.Warn("external filters configured but the store reads no external views; the capability is off",
			"store", store.Kind())
		return nil
	}
	armed := make([]ExternalFilter, 0, len(filters))
	for _, filter := range filters {
		if err := probeExternalView(prober, filter.View); err != nil {
			logger.Warn("external filter view is not readable; its param will be ignored",
				"param", filter.Param, "view", filter.View, "error", err)
			continue
		}
		armed = append(armed, filter)
	}
	return armed
}

// probeExternalView checks one view under its own fresh deadline. A shared
// deadline would let one slow probe hand every later view an expired context,
// silently disarming filters whose views are perfectly readable.
func probeExternalView(prober storage.ExternalViewProber, view string) error {
	ctx, cancel := context.WithTimeout(context.Background(), externalFilterProbeTimeout)
	defer cancel()
	return prober.ProbeExternalView(ctx, view)
}

// Handler mounts the anchors and the skills API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Anchors. These live at the root of the listener because they describe
	// the process, not the API — core probes and fetches them directly and
	// never proxies them.
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("pong\n"))
	})
	mux.HandleFunc("GET /openapi", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.openapi)
	})

	// The API itself, under the prefix clients call through tapes:
	// /api/<name>/... republishes as /v1/cassettes/<name>/...
	prefix := "/api/" + s.name
	mux.HandleFunc("GET "+prefix, s.handleListSkills)
	mux.HandleFunc("POST "+prefix, s.handleCreateSkill)
	mux.HandleFunc("POST "+prefix+"/generate", s.handleGenerateSkill)
	mux.HandleFunc("GET "+prefix+"/{id}", s.handleGetSkill)
	mux.HandleFunc("PUT "+prefix+"/{id}", s.handleUpdateSkill)
	mux.HandleFunc("DELETE "+prefix+"/{id}", s.handleDeleteSkill)
	mux.HandleFunc("GET "+prefix+"/{id}/skill.md", s.handleSkillMarkdown)
	mux.HandleFunc("GET "+prefix+"/{id}/versions", s.handleListSkillVersions)
	mux.HandleFunc("POST "+prefix+"/{id}/versions", s.handlePublishSkill)
	mux.HandleFunc("POST "+prefix+"/{id}/duplicate", s.handleDuplicateSkill)

	return mux
}

// Serve runs the cassette server on listener until ctx is canceled.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	httpServer := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()

	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		// The parent context is already canceled; detach from its cancellation
		// but keep its values so shutdown gets its own bounded deadline.
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return err
		}
		err := <-done
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// ListenAndServe listens on address and serves until ctx is canceled.
func (s *Server) ListenAndServe(ctx context.Context, address string) error {
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", address)
	if err != nil {
		return err
	}
	return s.Serve(ctx, listener)
}
