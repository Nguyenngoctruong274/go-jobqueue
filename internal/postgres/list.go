package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// MaxListLimit bounds ListRequest.Limit.
const MaxListLimit = 500

// ErrInvalidListRequest means a ListRequest failed validation.
var ErrInvalidListRequest = errors.New("invalid list request")

// ListRequest selects jobs for List. Zero State and Type match any.
type ListRequest struct {
	State job.State
	Type  string
	// Limit is the page size, 1 to MaxListLimit.
	Limit int
	// Before, if positive, starts the page after the job with that id: pass
	// the last id of the previous page.
	Before int64
}

// listColumns are jobColumns without the payload, which can be 1 MiB a
// job: a page of jobs is for finding them, Get is for reading one.
const listColumns = `id, type, NULL::jsonb, state, priority, attempt, max_attempts,
	run_at, lease_until, last_error, idempotency_key, created_at`

// listQuery takes state, type, before and limit. lib/pq plans an unnamed
// statement with its parameters bound, so the filters left empty fold
// away and a state filter pages through jobs_state_id.
const listQuery = `
	SELECT ` + listColumns + ` FROM jobs
	WHERE ($1::text = '' OR state = $1::text)
	  AND ($2::text = '' OR type = $2::text)
	  AND ($3::bigint = 0 OR id < $3::bigint)
	ORDER BY id DESC
	LIMIT $4`

// List returns up to req.Limit jobs matching req, newest first, without
// their payloads. Pages are keyed on id, not offsets, so a page costs the
// same however deep it is and jobs enqueued meanwhile never shift it.
func (s *Store) List(ctx context.Context, req ListRequest) ([]job.Job, error) {
	var errs []error
	if req.State != "" {
		if _, err := job.ParseState(string(req.State)); err != nil {
			errs = append(errs, err)
		}
	}
	if req.Type != "" {
		if err := job.ValidateType(req.Type); err != nil {
			errs = append(errs, err)
		}
	}
	if req.Limit < 1 || req.Limit > MaxListLimit {
		errs = append(errs, fmt.Errorf("limit: must be between 1 and %d", MaxListLimit))
	}
	if req.Before < 0 {
		errs = append(errs, errors.New("before: must not be negative"))
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("list jobs: %w: %w", ErrInvalidListRequest, errors.Join(errs...))
	}

	rows, err := s.db.QueryContext(ctx, listQuery, string(req.State), req.Type, req.Before, req.Limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	jobs := make([]job.Job, 0, req.Limit)
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("list jobs: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return jobs, nil
}
