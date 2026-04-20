package api

import (
	"net/http"
	"time"
)

// Version is set at build time via ldflags.
var Version = "dev"

var startTime = time.Now()

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"version":        Version,
		"uptime_seconds": int(time.Since(startTime).Seconds()),
	})
}

func handleReadyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ready": true,
	})
}
