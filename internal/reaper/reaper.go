// Package reaper returns jobs whose worker went silent to the queue. A
// worker that crashes, is killed or loses its network stops renewing its
// lease; once the lease runs out the reaper ends the attempt as a failure,
// so the job retries or dies by the same rules as any other failure.
package reaper

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Store is what the reaper needs from the job store.
type Store interface {
	// Reap expires up to limit run-out leases and returns the ids it expired.
	Reap(ctx context.Context, limit int) ([]int64, error)
}

// Options tunes a Reaper. Zero values take the defaults.
type Options struct {
	// Interval is the pause between sweeps when the last one found
	// nothing. Default 5s. A sweep that filled its batch is followed by
	// another at once, so a backlog clears quickly.
	Interval time.Duration
	// BatchSize is the most leases one sweep expires. Default 100.
	BatchSize int
}

// Reaper sweeps expired leases on a timer.
type Reaper struct {
	store  Store
	logger *slog.Logger
	opts   Options
}

// New returns a Reaper over store.
func New(store Store, logger *slog.Logger, opts Options) *Reaper {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Reaper{store: store, logger: logger, opts: opts}
}

// Run sweeps until ctx ends. Errors are logged and retried on the next
// sweep: a database blip must not stop the reaper for good, because then
// crashed workers' jobs would stay running forever. It returns nil when
// ctx ends.
func (r *Reaper) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		n, err := r.Sweep(ctx)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			r.logger.ErrorContext(ctx, "reap failed", slog.Any("error", err))
			timer.Reset(r.opts.Interval)
		case n == r.opts.BatchSize:
			timer.Reset(0) // more may be waiting
		default:
			timer.Reset(r.opts.Interval)
		}
	}
}

// Sweep expires one batch and returns how many leases it ended.
func (r *Reaper) Sweep(ctx context.Context) (int, error) {
	ids, err := r.store.Reap(ctx, r.opts.BatchSize)
	if len(ids) > 0 {
		r.logger.WarnContext(ctx, "expired leases of silent workers",
			slog.Int("jobs", len(ids)), slog.Any("ids", ids))
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return len(ids), err
	}
	return len(ids), nil
}
