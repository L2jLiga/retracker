// Package health provides liveness, readiness, and healthcheck HTTP endpoints.
package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Server exposes /healthz, /livez, and /readyz endpoints.
type Server struct {
	addr  string
	ready atomic.Bool
	log   *slog.Logger
}

func New(addr string, log *slog.Logger) *Server {
	return &Server{addr: addr, log: log}
}

// SetReady marks the service as ready (call after all components are started).
func (s *Server) SetReady(v bool) {
	s.ready.Store(v)
}

func (s *Server) Start(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/livez", s.handleLiveness)
	mux.HandleFunc("/readyz", s.handleReadiness)

	srv := &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	go func() {
		s.log.Info("Health server listening", "addr", s.addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Warn("Health server error", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleLiveness(w http.ResponseWriter, r *http.Request) {
	// Always alive as long as the process is running
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (s *Server) handleReadiness(w http.ResponseWriter, r *http.Request) {
	if s.ready.Load() {
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	} else {
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
	}
}

func (s *Server) writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
