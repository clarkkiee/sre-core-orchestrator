package api

import "net/http"

// RegisterRoutes sets up all HTTP routes on the given mux.
// Task and VM handlers are added once the executor is wired in.
func RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("GET /readyz", handleReadyz)
}
