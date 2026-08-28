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
	"sync"
	"time"

	"github.com/papercomputeco/skills-cassette/internal/storage"
	"github.com/papercomputeco/skills-cassette/pkg/skill"
)

const shutdownTimeout = 5 * time.Second

// externalFilterProbeTimeout bounds one probe of a single configured
// external attachment view — at startup and on every background re-probe.
// The budget is per view: one slow or hanging probe exhausts only its own
// deadline, never the deadline of the views probed after it. It is a var
// only so tests can shrink it.
var externalFilterProbeTimeout = 5 * time.Second

// externalFilterReprobeInterval is the cadence at which filters left
// unarmed by the startup probe are re-probed in the background while the
// server runs. It is a var only so tests can shrink it.
var externalFilterReprobeInterval = 30 * time.Second

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
	// mu guards filters and pending: request handlers read the armed set
	// on every list, while the background re-probe loop arms filters after
	// startup. Writers swap in fresh slices, never mutate published ones,
	// so a slice returned by armedFilters stays safe to iterate unlocked.
	mu sync.RWMutex
	// filters are the external attachment-view filters whose views probed
	// readable — at startup, or later by the background re-probe loop.
	// Only these params are ever parsed.
	filters []ExternalFilter
	// pending are the configured filters whose views were not readable at
	// their last probe. The re-probe loop retries them until they arm.
	pending []ExternalFilter
	// prober re-probes pending filters. It is nil when the store reads no
	// external views; nothing is ever pending then.
	prober storage.ExternalViewProber
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
	armed, pending, prober := armExternalFilters(cfg.Filters, store, logger)
	return &Server{
		name:    name,
		store:   store,
		querier: querier,
		llm:     cfg.LLM,
		logger:  logger,
		openapi: openAPIDocument(name),
		filters: armed,
		pending: pending,
		prober:  prober,
	}
}

// armExternalFilters probes each configured external attachment view once
// and splits the filters into armed (view readable now) and pending (not
// yet). Absent is cheap: an unreadable view (or a store that reads no
// external views at all) leaves that filter unarmed, its param ignored with
// zero behavioral change — but not forever. While the server runs, pending
// filters are re-probed in the background (reprobeExternalFilters) and arm
// without a restart once their views become readable, covering the
// deployment whose SELECT grant converges moments after boot. The distinct
// case — a view that breaks after arming — stays loud at request time
// (ErrExternalViewUnavailable, 503); per-request behavior is unchanged:
// there is no per-request re-probe and no fallback.
func armExternalFilters(filters []ExternalFilter, store storage.Store, logger *slog.Logger) (armed, pending []ExternalFilter, prober storage.ExternalViewProber) {
	if len(filters) == 0 {
		return nil, nil, nil
	}
	prober, ok := store.(storage.ExternalViewProber)
	if !ok {
		logger.Warn("external filters configured but the store reads no external views; the capability is off",
			"store", store.Kind())
		return nil, nil, nil
	}
	for _, filter := range filters {
		if err := probeExternalView(context.Background(), prober, filter.View); err != nil {
			logger.Warn("external filter view is not readable; its param is ignored until a re-probe arms it",
				"param", filter.Param, "view", filter.View, "error", err)
			pending = append(pending, filter)
			continue
		}
		armed = append(armed, filter)
	}
	return armed, pending, prober
}

// probeExternalView checks one view under its own fresh deadline, derived
// from parent so a canceled server also cancels an in-flight probe. A shared
// deadline would let one slow probe hand every later view an expired context,
// silently disarming filters whose views are perfectly readable.
func probeExternalView(parent context.Context, prober storage.ExternalViewProber, view string) error {
	ctx, cancel := context.WithTimeout(parent, externalFilterProbeTimeout)
	defer cancel()
	return prober.ProbeExternalView(ctx, view)
}

// armedFilters returns the external filters armed right now. The returned
// slice is never mutated after publication (armFilter swaps in a fresh one),
// so callers may iterate it without holding the lock.
func (s *Server) armedFilters() []ExternalFilter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.filters
}

// reprobeExternalFilters retries every pending filter on the re-probe
// interval until each arms, then stops. Armed filters are never re-probed —
// arming is one-way, and a view that breaks after arming stays a loud 503.
// The loop runs for the lifetime of Serve and exits when its context is
// canceled or nothing remains unarmed.
func (s *Server) reprobeExternalFilters(ctx context.Context) {
	if s.prober == nil {
		return
	}
	ticker := time.NewTicker(externalFilterReprobeInterval)
	defer ticker.Stop()
	for {
		s.mu.RLock()
		remaining := len(s.pending)
		s.mu.RUnlock()
		if remaining == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reprobePendingFilters(ctx)
		}
	}
}

// reprobePendingFilters probes each pending filter once — each attempt under
// its own fresh deadline, the same per-view budget as startup — and arms the
// ones whose views have become readable. Failures stay quiet here: startup
// already warned once per filter, and re-warning on every interval would
// only be noise.
func (s *Server) reprobePendingFilters(ctx context.Context) {
	s.mu.RLock()
	pending := s.pending
	s.mu.RUnlock()
	for _, filter := range pending {
		if ctx.Err() != nil {
			return
		}
		if err := probeExternalView(ctx, s.prober, filter.View); err != nil {
			continue
		}
		s.armFilter(filter)
	}
}

// armFilter publishes filter into the armed set and drops it from pending.
// Both slices are replaced rather than mutated in place: a request iterating
// a previously returned armed slice keeps a consistent snapshot.
func (s *Server) armFilter(filter ExternalFilter) {
	s.mu.Lock()
	armed := make([]ExternalFilter, 0, len(s.filters)+1)
	armed = append(armed, s.filters...)
	armed = append(armed, filter)
	s.filters = armed
	pending := make([]ExternalFilter, 0, len(s.pending))
	for _, p := range s.pending {
		if p.Param != filter.Param {
			pending = append(pending, p)
		}
	}
	s.pending = pending
	s.mu.Unlock()
	s.logger.Info("external filter view became readable; its param is now armed",
		"param", filter.Param, "view", filter.View)
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
	// The background re-probe loop shares the server's lifecycle: it starts
	// with serving and is canceled — and fully drained — before Serve
	// returns, so no probe outlives the server.
	reprobeCtx, stopReprobe := context.WithCancel(ctx)
	reprobeDone := make(chan struct{})
	go func() {
		defer close(reprobeDone)
		s.reprobeExternalFilters(reprobeCtx)
	}()
	defer func() {
		stopReprobe()
		<-reprobeDone
	}()

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
