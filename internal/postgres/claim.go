package postgres

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/lib/pq"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// MaxClaimBatch caps how many jobs one Claim may take, so a single worker
// cannot hoard the queue or hold row locks across a huge update.
const MaxClaimBatch = 1000

// ClaimRequest says which jobs a worker wants and for how long.
type ClaimRequest struct {
	// Types limits claiming to jobs the worker has handlers for; empty
	// means any type.
	Types []string
	// Limit is the most jobs to claim, 1 to MaxClaimBatch.
	Limit int
	// Lease is how long each claimed job belongs to the worker before the
	// reaper may hand it to another. At least a microsecond, the
	// database's resolution.
	Lease time.Duration
}

// Claim starts the next attempt of up to req.Limit due pending jobs, in
// claim order: highest priority first, then earliest run_at, then id. It
// returns them in that order, running under a lease that ends at the
// database's now() plus req.Lease. No due job is not an error: Claim
// returns an empty slice.
//
// FOR UPDATE SKIP LOCKED lets many workers claim at once without waiting
// on each other: a row another transaction is claiming is skipped, not
// waited for, and once that transaction commits the row is running and
// no longer matches. Each attempt therefore goes to exactly one worker.
func (s *Store) Claim(ctx context.Context, req ClaimRequest) ([]job.Job, error) {
	if req.Limit < 1 || req.Limit > MaxClaimBatch {
		return nil, fmt.Errorf("claim: limit must be between 1 and %d, got %d", MaxClaimBatch, req.Limit)
	}
	lease := req.Lease.Microseconds()
	if lease < 1 {
		return nil, fmt.Errorf("claim: lease must be at least 1µs, got %v", req.Lease)
	}
	var types any
	if len(req.Types) > 0 {
		types = pq.Array(req.Types)
	}

	// The CTE walks the jobs_claim index in claim order and locks the
	// first rows nobody else holds; the UPDATE then applies job.Claim's
	// transition to exactly those rows.
	rows, err := s.db.QueryContext(ctx, `
		WITH due AS (
			SELECT id FROM jobs
			WHERE state = 'pending' AND run_at <= now()
			  AND ($3::text[] IS NULL OR type = ANY($3))
			ORDER BY priority DESC, run_at, id
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs SET
			state       = 'running',
			attempt     = attempt + 1,
			lease_until = now() + $2 * interval '1 microsecond',
			updated_at  = now()
		WHERE id IN (SELECT id FROM due)
		RETURNING `+jobColumns,
		req.Limit, lease, types)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()

	claimed := make([]job.Job, 0, req.Limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("claim: %w", err)
		}
		claimed = append(claimed, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	// RETURNING has no defined order; restore the claim order.
	slices.SortFunc(claimed, claimOrder)
	return claimed, nil
}

// claimOrder compares two jobs in the order Claim takes them.
func claimOrder(a, b job.Job) int {
	switch {
	case a.Priority != b.Priority:
		return cmp.Compare(b.Priority, a.Priority)
	case !a.RunAt.Equal(b.RunAt):
		return a.RunAt.Compare(b.RunAt)
	default:
		return cmp.Compare(a.ID, b.ID)
	}
}
