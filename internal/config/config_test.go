package config

import (
	"strings"
	"testing"
	"time"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(envFrom(map[string]string{"JQ_DATABASE_URL": "postgres://jobs:jobs@localhost:5432/jobs"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		HTTPAddr:        ":8080",
		DatabaseURL:     "postgres://jobs:jobs@localhost:5432/jobs",
		DBMaxConns:      20,
		ShutdownTimeout: 15 * time.Second,
		LogLevel:        "info",
	}
	if cfg != want {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(envFrom(map[string]string{
		"JQ_HTTP_ADDR":        "127.0.0.1:9090",
		"JQ_DATABASE_URL":     "postgresql://jobs:pw@db.internal:6432/jobs?sslmode=require",
		"JQ_DB_MAX_CONNS":     "50",
		"JQ_SHUTDOWN_TIMEOUT": "30s",
		"JQ_SHUTDOWN_DRAIN":   "5s",
		"JQ_LOG_LEVEL":        "debug",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" || cfg.ShutdownTimeout != 30*time.Second ||
		cfg.ShutdownDrain != 5*time.Second || cfg.LogLevel != "debug" || cfg.DBMaxConns != 50 {
		t.Fatalf("unexpected config %+v", cfg)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []string // every substring must appear in the error
	}{
		{
			name: "missing database url",
			env:  map[string]string{},
			want: []string{"JQ_DATABASE_URL: required"},
		},
		{
			name: "wrong scheme",
			env:  map[string]string{"JQ_DATABASE_URL": "mysql://localhost:3306/jobs"},
			want: []string{"JQ_DATABASE_URL: must be"},
		},
		{
			name: "no host",
			env:  map[string]string{"JQ_DATABASE_URL": "postgres://"},
			want: []string{"JQ_DATABASE_URL: must be"},
		},
		{
			name: "bad durations",
			env: map[string]string{
				"JQ_DATABASE_URL":     "postgres://localhost/jobs",
				"JQ_SHUTDOWN_TIMEOUT": "0s",
				"JQ_SHUTDOWN_DRAIN":   "-1s",
			},
			want: []string{"JQ_SHUTDOWN_TIMEOUT: must be positive", "JQ_SHUTDOWN_DRAIN"},
		},
		{
			name: "unparsable timeout",
			env:  map[string]string{"JQ_DATABASE_URL": "postgres://localhost/jobs", "JQ_SHUTDOWN_TIMEOUT": "soon"},
			want: []string{"JQ_SHUTDOWN_TIMEOUT"},
		},
		{
			name: "every error is reported at once",
			env:  map[string]string{"JQ_LOG_LEVEL": "loud"},
			want: []string{"JQ_DATABASE_URL: required", `JQ_LOG_LEVEL: unsupported level "loud"`},
		},
		{
			name: "pool size",
			env:  map[string]string{"JQ_DATABASE_URL": "postgres://localhost/jobs", "JQ_DB_MAX_CONNS": "0"},
			want: []string{"JQ_DB_MAX_CONNS: must be a positive integer"},
		},
		{
			name: "password is never echoed",
			env:  map[string]string{"JQ_DATABASE_URL": "http://jobs:hunter2@localhost/jobs"},
			want: []string{"JQ_DATABASE_URL: must be"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(envFrom(tt.env))
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not contain %q", err, w)
				}
			}
			if strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error %q leaks the database password", err)
			}
		})
	}
}
