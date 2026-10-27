package postgres

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationLockID is an arbitrary constant for pg_advisory_lock so that
// several instances starting at once apply migrations exactly once.
const migrationLockID = 4_210_771_903

type migration struct {
	version int
	name    string
	sql     string
}

// Migrate applies every embedded migration that has not been applied yet.
// Each migration runs in its own transaction together with the row that
// records it, so a failed migration leaves no partial state behind.
func Migrate(ctx context.Context, db *sql.DB) error {
	return migrate(ctx, db, migrationFS)
}

func migrate(ctx context.Context, db *sql.DB, fsys fs.FS) error {
	migrations, err := loadMigrations(fsys)
	if err != nil {
		return err
	}

	// Advisory locks are per session, so pin one connection for the whole run.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("migrate: acquire connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("migrate: lock: %w", err)
	}
	defer func() {
		// Use a fresh context: the caller's may already be cancelled.
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID)
	}()

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    INT         PRIMARY KEY,
		name       TEXT        NOT NULL,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("migrate: create schema_migrations: %w", err)
	}

	applied := make(map[int]bool)
	rows, err := conn.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("migrate: read applied versions: %w", err)
	}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			_ = rows.Close()
			return fmt.Errorf("migrate: scan version: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("migrate: read applied versions: %w", err)
	}

	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		if err := applyMigration(ctx, conn, m); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, conn *sql.Conn, m migration) (err error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrate %s: begin: %w", m.name, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(ctx, m.sql); err != nil {
		return fmt.Errorf("migrate %s: %w", m.name, err)
	}
	if _, err = tx.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`, m.version, m.name); err != nil {
		return fmt.Errorf("migrate %s: record version: %w", m.name, err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("migrate %s: commit: %w", m.name, err)
	}
	return nil
}

// loadMigrations reads files named NNNN_description.sql, sorted by version.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	names, err := fs.Glob(fsys, "migrations/*.sql")
	if err != nil {
		return nil, fmt.Errorf("migrate: list files: %w", err)
	}

	seen := make(map[int]string, len(names))
	out := make([]migration, 0, len(names))
	for _, path := range names {
		base := strings.TrimPrefix(path, "migrations/")
		prefix, _, ok := strings.Cut(base, "_")
		if !ok {
			return nil, fmt.Errorf("migrate: %s: name must look like 0001_description.sql", base)
		}
		version, err := strconv.Atoi(prefix)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("migrate: %s: invalid version %q", base, prefix)
		}
		if other, dup := seen[version]; dup {
			return nil, fmt.Errorf("migrate: %s and %s share version %d", other, base, version)
		}
		seen[version] = base

		body, err := fs.ReadFile(fsys, path)
		if err != nil {
			return nil, fmt.Errorf("migrate: read %s: %w", base, err)
		}
		out = append(out, migration{version: version, name: base, sql: string(body)})
	}
	if len(out) == 0 {
		return nil, errors.New("migrate: no migrations found")
	}

	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}
