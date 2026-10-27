package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// Store keeps jobs in the jobs table.
type Store struct {
	db *sql.DB
}

// NewStore returns a Store using db. The caller owns db and closes it.
func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

const jobColumns = `id, type, payload, state, priority, attempt, max_attempts,
	run_at, lease_until, last_error, idempotency_key, created_at`

// keyRaceRetries bounds how often Enqueue goes back and forth between
// inserting and reading a keyed job. Each extra round needs the job that
// holds the key to be deleted in between, so 3 is plenty.
const keyRaceRetries = 3

// Enqueue stores a new pending job and reports whether it was created.
//
// With an idempotency key, enqueueing a job whose type and key are already
// taken returns the existing job and created=false, whatever its state: a
// producer that retries after a timeout gets the job it already made. If
// that job was enqueued with a different payload, priority or attempt
// limit, the key was reused for other work and Enqueue returns
// job.ErrIdempotencyConflict.
func (s *Store) Enqueue(ctx context.Context, spec job.Spec) (j job.Job, created bool, err error) {
	spec, err = spec.Normalize()
	if err != nil {
		return job.Job{}, false, fmt.Errorf("enqueue: %w", err)
	}
	var runAt, key any
	if !spec.RunAt.IsZero() {
		runAt = spec.RunAt
	}
	if spec.IdempotencyKey != "" {
		key = spec.IdempotencyKey
	}
	// A string, not []byte: lib/pq would send []byte as bytea.
	payload := string(spec.Payload)

	for range keyRaceRetries {
		j, err = scanJob(s.db.QueryRowContext(ctx, `
			INSERT INTO jobs (type, payload, priority, max_attempts, run_at, idempotency_key)
			VALUES ($1, $2, $3, $4, COALESCE($5, now()), $6)
			ON CONFLICT (type, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING `+jobColumns,
			spec.Type, payload, spec.Priority, spec.MaxAttempts, runAt, key))
		if err == nil {
			return j, true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return job.Job{}, false, fmt.Errorf("enqueue %s: %w", spec.Type, mapPayloadError(err))
		}

		// The key is taken. ON CONFLICT waited for the holder to commit, but
		// this statement's snapshot predates it, so read the holder in a
		// new statement, which sees it.
		var same bool
		j, err = scanJob(s.db.QueryRowContext(ctx, `
			SELECT `+jobColumns+`,
			       payload = $3::jsonb AND priority = $4 AND max_attempts = $5
			FROM jobs WHERE type = $1 AND idempotency_key = $2`,
			spec.Type, spec.IdempotencyKey, payload, spec.Priority, spec.MaxAttempts), &same)
		if errors.Is(err, sql.ErrNoRows) {
			continue // deleted in between: insert again
		}
		if err != nil {
			return job.Job{}, false, fmt.Errorf("enqueue %s: read job holding the key: %w", spec.Type, mapPayloadError(err))
		}
		if !same {
			return job.Job{}, false, fmt.Errorf("enqueue %s: key %q held by job %d: %w",
				spec.Type, spec.IdempotencyKey, j.ID, job.ErrIdempotencyConflict)
		}
		return j, false, nil
	}
	return job.Job{}, false, fmt.Errorf("enqueue %s: key %q kept changing hands", spec.Type, spec.IdempotencyKey)
}

// Get returns the job with the given id.
func (s *Store) Get(ctx context.Context, id int64) (job.Job, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return job.Job{}, fmt.Errorf("get job %d: %w", id, job.ErrNotFound)
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("get job %d: %w", id, err)
	}
	return j, nil
}

// scanJob reads jobColumns, then any extra destinations, from row.
func scanJob(row *sql.Row, extra ...any) (job.Job, error) {
	var (
		j          job.Job
		state      string
		leaseUntil sql.NullTime
		lastError  sql.NullString
		key        sql.NullString
	)
	dest := append([]any{
		&j.ID, &j.Type, &j.Payload, &state, &j.Priority, &j.Attempt, &j.MaxAttempts,
		&j.RunAt, &leaseUntil, &lastError, &key, &j.CreatedAt,
	}, extra...)
	if err := row.Scan(dest...); err != nil {
		return job.Job{}, err
	}
	st, err := job.ParseState(state)
	if err != nil {
		return job.Job{}, fmt.Errorf("job %d: %w", j.ID, err)
	}
	j.State = st
	j.LeaseUntil = leaseUntil.Time
	j.LastError = lastError.String
	j.IdempotencyKey = key.String
	return j, nil
}

// mapPayloadError turns PostgreSQL refusing a payload that encoding/json
// accepted, such as one containing \u0000 or a lone surrogate, into
// job.ErrInvalidSpec.
func mapPayloadError(err error) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		switch pqErr.Code {
		case "22P02", "22P05": // invalid_text_representation, untranslatable_character
			return fmt.Errorf("%w: payload: %s", job.ErrInvalidSpec, pqErr.Message)
		}
	}
	return err
}
