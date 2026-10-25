package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOpenFailsFastWhenDatabaseIsDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Port 1 is reserved and closed on any sane test machine.
	start := time.Now()
	_, err := Open(ctx, "postgres://jobs:secret@127.0.0.1:1/jobs?sslmode=disable&connect_timeout=2", 4)
	if err == nil || !strings.Contains(err.Error(), "ping postgres") {
		t.Fatalf("error = %v, want ping error", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("error %q leaks the password", err)
	}
	if time.Since(start) > 4*time.Second {
		t.Fatalf("Open took %s, want a fast failure", time.Since(start))
	}
}

func TestOpenRejectsBadURL(t *testing.T) {
	_, err := Open(context.Background(), "postgres://localhost:5432/jobs?sslmode=bogus", 4)
	if err == nil {
		t.Fatal("expected an error for an invalid sslmode")
	}
}

func TestOpenAndPing(t *testing.T) {
	url := os.Getenv("JQ_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("JQ_TEST_DATABASE_URL not set; skipping PostgreSQL tests")
	}
	ctx := context.Background()
	db, err := Open(ctx, url, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.Stats().MaxOpenConnections; got != 4 {
		t.Fatalf("max open connections = %d, want 4", got)
	}
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("SELECT 1 = %d, %v", one, err)
	}
}
