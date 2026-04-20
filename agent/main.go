package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/api"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/config"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/executor"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/k3s"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/multipass"
	"github.com/clarkkiee/sre-core-orchestrator/agent/internal/store"
)

// Version is set at build time via -ldflags.
var Version = "dev"

func main() {
	cfg, err := config.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	initLogger(cfg.LogLevel)

	// Ensure data directories exist.
	for _, dir := range []string{cfg.DataDir, cfg.DataDir + "/tasks", cfg.DataDir + "/cloud-init"} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			slog.Error("failed to create directory", "path", dir, "error", err)
			os.Exit(1)
		}
	}

	// Set build version for the health endpoint.
	api.Version = Version

	// Initialize dependencies.
	taskStore, err := store.NewTaskStore(cfg.DataDir)
	if err != nil {
		slog.Error("failed to initialize task store", "error", err)
		os.Exit(1)
	}

	mpClient := multipass.NewClient(cfg.MultipassBinary)
	k3sBoot := k3s.NewBootstrapper(mpClient)
	exec := executor.NewTaskExecutor(taskStore, mpClient, k3sBoot, cfg.DataDir)

	srv := api.NewServer(cfg.ListenAddr, cfg.Token, exec, mpClient)

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := srv.ListenAndServe(); err != nil && err.Error() != "http: Server closed" {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	slog.Info("orchestrator-agent started",
		"version", Version,
		"data_dir", cfg.DataDir,
		"multipass_binary", cfg.MultipassBinary,
	)

	<-ctx.Done()
	slog.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}
}

func initLogger(level string) {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l})))
}
