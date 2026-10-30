// Package testdb gives tests an isolated PostgreSQL schema.
//
// Tests that need a database call New. If JQ_TEST_DATABASE_URL is not set
// the test is skipped, so `go test ./...` stays green on a laptop with no
// database; CI sets it from a postgres service container.
//
// Every call creates a fresh schema with a random name and points the
// returned handle's search_path at it, so tests can run in parallel and
// never see each other's rows. The schema is dropped when the test ends.
package testdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"
)

// EnvURL names the environment variable holding the test database URL.
const EnvURL = "JQ_TEST_DATABASE_URL"

// MigrateFunc prepares a schema. Pass postgres.Migrate; taking it as a
// parameter keeps this package free of an import on postgres, so the
// postgres package's own tests can use it too.
type MigrateFunc func(context.Context, *sql.DB) error

// New returns a handle scoped to a fresh schema after running migrate on it.
func New(t testing.TB, migrate MigrateFunc) *sql.DB {
	t.Helper()
	db := NewEmpty(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := migrate(ctx, db); err != nil {
		t.Fatalf("testdb: migrate: %v", err)
	}
	return db
}

// NewEmpty returns a handle scoped to a fresh, empty schema.
func NewEmpty(t testing.TB) *sql.DB {
	t.Helper()
	db, _ := NewEmptyURL(t)
	return db
}

// NewURL is New, also returning the schema-scoped URL of the handle, for
// code that opens its own connections such as a LISTEN subscriber.
func NewURL(t testing.TB, migrate MigrateFunc) (*sql.DB, string) {
	t.Helper()
	db, url := NewEmptyURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := migrate(ctx, db); err != nil {
		t.Fatalf("testdb: migrate: %v", err)
	}
	return db, url
}

// NewEmptyURL is NewEmpty, also returning the schema-scoped URL.
func NewEmptyURL(t testing.TB) (*sql.DB, string) {
	t.Helper()
	base := os.Getenv(EnvURL)
	if base == "" {
		t.Skip(EnvURL + " not set; skipping PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin := open(ctx, t, base)
	t.Cleanup(func() { _ = admin.Close() })

	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		t.Fatalf("testdb: %v", err)
	}
	schema := "test_" + hex.EncodeToString(buf)
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema)); err != nil {
		t.Fatalf("testdb: create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE")
	})

	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("testdb: parse %s: %v", EnvURL, err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()

	db := open(ctx, t, u.String())
	t.Cleanup(func() { _ = db.Close() })
	return db, u.String()
}

func open(ctx context.Context, t testing.TB, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("testdb: open: %v", err)
	}
	// Concurrency tests start more goroutines than PostgreSQL accepts
	// connections, and must queue on the pool.
	db.SetMaxOpenConns(20)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("testdb: ping: %v", err)
	}
	return db
}
