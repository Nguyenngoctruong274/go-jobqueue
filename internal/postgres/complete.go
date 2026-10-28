package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// Option configures a Store.
type Option func(*Store)

// WithBackoff sets the retry policy Fail and Reap use. The default is
// job.DefaultBackoff.
func WithBackoff(b job.Backoff) Option {
	return func(s *Store) { s.backoff = b }
}

// WithRand sets the jitter source, for tests that need exact delays.
func WithRand(r job.Rand) Option {
	return func(s *Store) { s.rnd = r }
}

// Succeed completes the attempt a worker holds. The attempt number is the
// fencing token Claim returned; a stale one is refused with
// job.ErrLeaseLost and the job is left as it is.
func (s *Store) Succeed(ctx context.Context, id int64, attempt int) error {
	return s.transition(ctx, "succeed", id, func(j *job.Job, _ time.Time) error {
		return j.Succeed(attempt)
	})
}

// Fail records a failed attempt. Unless cause is marked job.Permanent or
// the attempts are used up, the job becomes pending again at now plus the
// backoff delay; otherwise it is dead. It returns the job as stored.
func (s *Store) Fail(ctx context.Context, id int64, attempt int, cause error) (job.Job, error) {
	var out job.Job
	err := s.transition(ctx, "fail", id, func(j *job.Job, now time.Time) error {
		if err := j.Fail(attempt, now, cause, s.backoff, s.rnd); err != nil {
			return err
		}
		out = *j
		return nil
	})
	return out, err
}

// Heartbeat extends the lease of the attempt a worker holds to now+lease.
func (s *Store) Heartbeat(ctx context.Context, id int64, attempt int, lease time.Duration) error {
	return s.transition(ctx, "heartbeat", id, func(j *job.Job, now time.Time) error {
		return j.Heartbeat(attempt, now, lease)
	})
}

// Cancel stops a pending job from ever running.
func (s *Store) Cancel(ctx context.Context, id int64) error {
	return s.transition(ctx, "cancel", id, func(j *job.Job, _ time.Time) error {
		return j.Cancel()
	})
}

// Requeue gives a dead job a fresh set of attempts, due now.
func (s *Store) Requeue(ctx context.Context, id int64) error {
	return s.transition(ctx, "requeue", id, func(j *job.Job, now time.Time) error {
		return j.Requeue(now)
	})
}

// Reap ends up to limit running attempts whose lease has run out, as if
// their worker had reported a failure: the job retries after the backoff
// delay or dies when its attempts are spent. It returns the ids it
// expired. Several reapers may run at once; rows one is handling are
// skipped by the others.
//
// Each job is expired in its own transaction so that one reaper holding
// a long batch does not block the workers of the other jobs.
func (s *Store) Reap(ctx context.Context, limit int) ([]int64, error) {
	if limit < 1 || limit > MaxClaimBatch {
		return nil, fmt.Errorf("reap: limit must be between 1 and %d, got %d", MaxClaimBatch, limit)
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM jobs
		WHERE state = 'running' AND lease_until <= now()
		ORDER BY lease_until
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("reap: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("reap: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reap: %w", err)
	}

	expired := make([]int64, 0, len(ids))
	for _, id := range ids {
		err := s.transition(ctx, "expire", id, func(j *job.Job, now time.Time) error {
			return j.Expire(now, s.backoff, s.rnd)
		})
		switch {
		case errors.Is(err, job.ErrInvalidTransition), errors.Is(err, job.ErrLeaseActive):
			// Between the scan and the lock the worker finished, or sent a
			// heartbeat. Its report stands.
			continue
		case err != nil:
			return expired, err
		}
		expired = append(expired, id)
	}
	return expired, nil
}

// transition locks the job, applies fn to it at the database's clock and
// writes the result back, all in one transaction. The lock makes the
// read-check-write atomic against workers, reapers and operators acting
// on the same job; fn runs the domain rules, so the store cannot drift
// from internal/job.
func (s *Store) transition(ctx context.Context, op string, id int64, fn func(j *job.Job, now time.Time) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%s job %d: begin: %w", op, id, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var now time.Time
	j, err := scanJob(tx.QueryRowContext(ctx,
		`SELECT `+jobColumns+`, now() FROM jobs WHERE id = $1 FOR UPDATE`, id), &now)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s job %d: %w", op, id, job.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("%s job %d: %w", op, id, err)
	}

	if err = fn(&j, now); err != nil {
		return err // the domain error already names the job and operation
	}

	var leaseUntil, lastError any
	if !j.LeaseUntil.IsZero() {
		leaseUntil = j.LeaseUntil
	}
	if j.LastError != "" {
		lastError = j.LastError
	}
	if _, err = tx.ExecContext(ctx, `
		UPDATE jobs SET
			state = $2, attempt = $3, run_at = $4, lease_until = $5, last_error = $6, updated_at = now()
		WHERE id = $1`,
		j.ID, string(j.State), j.Attempt, j.RunAt, leaseUntil, lastError); err != nil {
		return fmt.Errorf("%s job %d: update: %w", op, id, err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("%s job %d: commit: %w", op, id, err)
	}
	return nil
}
