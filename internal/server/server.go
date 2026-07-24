// Package server provides the skills-cassette health server.
package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

const shutdownTimeout = 5 * time.Second

// New returns the milestone health handler. Product skill routes intentionally
// remain absent until cassette routing and schema ownership are approved.
func New() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("pong\n"))
	})
	return mux
}

// Serve runs the health server on listener until ctx is canceled.
func Serve(ctx context.Context, listener net.Listener) error {
	httpServer := &http.Server{Handler: New(), ReadHeaderTimeout: 5 * time.Second}
	done := make(chan error, 1)
	go func() { done <- httpServer.Serve(listener) }()

	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
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
func ListenAndServe(ctx context.Context, address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	return Serve(ctx, listener)
}
