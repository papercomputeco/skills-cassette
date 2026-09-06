package server

import "net/http"

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := s.generationMetrics.WritePrometheus(w); err != nil {
		s.logger.Error("write generation metrics", "error", err)
	}
}
