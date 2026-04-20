package config

import (
	"flag"
	"fmt"
	"os"
)

// Config holds the agent runtime configuration parsed from CLI flags.
type Config struct {
	ListenAddr      string
	Token           string
	DataDir         string
	MultipassBinary string
	LogLevel        string
}

// Parse reads CLI flags and returns a validated Config.
func Parse() (*Config, error) {
	cfg := &Config{}

	flag.StringVar(&cfg.ListenAddr, "listen", envOrDefault("AGENT_LISTEN", ":9090"), "address to listen on (host:port)")
	flag.StringVar(&cfg.Token, "token", envOrDefault("AGENT_TOKEN", ""), "bearer token for API authentication")
	flag.StringVar(&cfg.DataDir, "data-dir", envOrDefault("AGENT_DATA_DIR", defaultDataDir()), "directory for task state and temp files")
	flag.StringVar(&cfg.MultipassBinary, "multipass-binary", envOrDefault("MULTIPASS_BINARY", "/snap/bin/multipass"), "path to multipass binary")
	flag.StringVar(&cfg.LogLevel, "log-level", envOrDefault("AGENT_LOG_LEVEL", "info"), "log level (debug, info, warn, error)")
	flag.Parse()

	if cfg.Token == "" {
		return nil, fmt.Errorf("--token is required")
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/orchestrator-agent"
	}
	return home + "/.orchestrator-agent"
}
