// Package config loads runtime configuration for jobqueued from the environment.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config is the complete runtime configuration of the service.
type Config struct {
	HTTPAddr string
	// DatabaseURL points at the PostgreSQL database that holds the jobs,
	// e.g. postgres://jobs:secret@localhost:5432/jobs?sslmode=disable.
	DatabaseURL string
	// DBMaxConns caps open database connections. Workers, the API and the
	// reaper share the pool, so it bounds the load the service can put on
	// PostgreSQL.
	DBMaxConns      int
	ShutdownTimeout time.Duration
	// ShutdownDrain is how long to keep serving with /readyz failing before
	// closing the listener. 0 locally; a few seconds behind a load balancer.
	ShutdownDrain time.Duration
	LogLevel      string
	// ReapInterval is how often expired leases are swept. Jobs whose
	// worker crashed wait at most this long after their lease to be
	// retried.
	ReapInterval time.Duration
}

// Load reads configuration from environment variables, applying defaults
// for optional values. It reports every invalid value at once so that a
// misconfigured deployment fails fast with a complete error message.
func Load() (Config, error) {
	return load(os.Getenv)
}

func load(getenv func(string) string) (Config, error) {
	cfg := Config{
		HTTPAddr:        valueOr(getenv("JQ_HTTP_ADDR"), ":8080"),
		DatabaseURL:     getenv("JQ_DATABASE_URL"),
		DBMaxConns:      20,
		ShutdownTimeout: 15 * time.Second,
		LogLevel:        valueOr(getenv("JQ_LOG_LEVEL"), "info"),
		ReapInterval:    5 * time.Second,
	}

	var errs []error

	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("JQ_DATABASE_URL: required"))
	} else if u, err := url.Parse(cfg.DatabaseURL); err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host == "" {
		// The URL holds a password, so it is never echoed back.
		errs = append(errs, errors.New("JQ_DATABASE_URL: must be a postgres:// or postgresql:// URL with a host"))
	}

	if raw := getenv("JQ_DB_MAX_CONNS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			errs = append(errs, errors.New("JQ_DB_MAX_CONNS: must be a positive integer"))
		} else {
			cfg.DBMaxConns = n
		}
	}

	if raw := getenv("JQ_SHUTDOWN_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("JQ_SHUTDOWN_TIMEOUT: %w", err))
		case d <= 0:
			errs = append(errs, errors.New("JQ_SHUTDOWN_TIMEOUT: must be positive"))
		default:
			cfg.ShutdownTimeout = d
		}
	}

	if raw := getenv("JQ_SHUTDOWN_DRAIN"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			errs = append(errs, errors.New("JQ_SHUTDOWN_DRAIN: must be a non-negative duration"))
		} else {
			cfg.ShutdownDrain = d
		}
	}

	if raw := getenv("JQ_REAP_INTERVAL"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 100*time.Millisecond {
			errs = append(errs, errors.New("JQ_REAP_INTERVAL: must be a duration of at least 100ms"))
		} else {
			cfg.ReapInterval = d
		}
	}

	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("JQ_LOG_LEVEL: unsupported level %s", strconv.Quote(cfg.LogLevel)))
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
