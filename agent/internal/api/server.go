package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/executor"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/multipass"
)

// Server wraps the HTTP server for the provisioning agent.
type Server struct {
	httpServer *http.Server
}

// NewServer creates a server with all routes registered and auth middleware applied.
func NewServer(addr, token string, exec *executor.TaskExecutor, mp *multipass.Client) *Server {
	mux := http.NewServeMux()

	// Health routes (no auth).
	RegisterRoutes(mux)

	// Task and VM routes (auth required — handled by middleware).
	NewTaskHandlers(exec).Register(mux)
	NewVMHandlers(mp).Register(mux)

	handler := AuthMiddleware(token)(mux)

	return &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
		},
	}
}

// ListenAndServe starts the HTTP server (blocking).
func (s *Server) ListenAndServe() error {
	slog.Info("agent listening", "addr", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
