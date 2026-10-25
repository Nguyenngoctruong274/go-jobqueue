// Package postgres keeps the job queue in PostgreSQL. This file owns the
// connection; the schema and the queue operations build on it.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // registers the "postgres" driver
)

// Open connects to PostgreSQL with a pool of at most maxConns connections
// and verifies the connection. The caller closes the returned handle.
func Open(ctx context.Context, url string, maxConns int) (*sql.DB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	// Recycle connections now and then, so a failover or a pooler restart
	// does not leave the service holding connections to the old server.
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return db, nil
}
