// Package worker runs job handlers in a bounded pool. The pool claims due
// jobs of the types it has handlers for, runs each under a lease it keeps
// renewing with heartbeats, turns handler panics into failures, and
// reports every outcome to the store.
//
// On shutdown it stops claiming at once, gives running handlers a grace
// period to finish, then cancels them and hands their attempts back, so a
// deploy neither loses jobs nor leaves them waiting for the reaper.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// Handler runs one attempt of a job. Returning nil completes the job; an
// error fails the attempt, and the job retries after a backoff unless the
// error is marked job.Permanent or the attempts are used up.
//
// ctx is cancelled when the pool loses the job's lease (another worker may
// already be running it) or gives up waiting for the handler at shutdown.
// Handlers must return promptly once it is done. Jobs run at least once,
// so handlers must be idempotent.
type Handler interface {
	Handle(ctx context.Context, j job.Job) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, j job.Job) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, j job.Job) error { return f(ctx, j) }

// Store is what the pool needs from the job store. Every method that
// takes an attempt uses it as the fencing token Claim handed out.
type Store interface {
	Claim(ctx context.Context, types []string, limit int, lease time.Duration) ([]job.Job, error)
	Heartbeat(ctx context.Context, id int64, attempt int, lease time.Duration) error
	Succeed(ctx context.Context, id int64, attempt int) error
	Fail(ctx context.Context, id int64, attempt int, cause error) (job.Job, error)
	Release(ctx context.Context, id int64, attempt int) error
}

// MaxConcurrency bounds Options.Concurrency; it matches the largest batch
// the store claims at once.
const MaxConcurrency = 1000

// reportTimeout bounds each claim and each report of an outcome. Both run
// detached from the pool's context, so that shutdown cannot abort a claim
// that already took rows, or drop the result of a finished handler.
const reportTimeout = 10 * time.Second

// Options tunes a Pool. Zero values take the defaults.
type Options struct {
	// Concurrency is the most handlers running at once. Default 10.
	Concurrency int
	// Lease is how long a claimed job belongs to this pool without a
	// heartbeat. Default 30s.
	Lease time.Duration
	// Heartbeat is how often a running job's lease is renewed; it must be
	// shorter than Lease. Default Lease/3, which survives two missed beats.
	Heartbeat time.Duration
	// PollInterval is the pause before claiming again when no job was due.
	// Default 1s.
	PollInterval time.Duration
	// DrainTimeout is how long shutdown waits for running handlers before
	// cancelling them. Default 10s.
	DrainTimeout time.Duration
}

// Pool claims jobs and runs their handlers.
type Pool struct {
	store    Store
	handlers map[string]Handler
	types    []string
	logger   *slog.Logger
	opts     Options
}

// New returns a Pool that runs handlers, keyed by job type.
func New(store Store, handlers map[string]Handler, logger *slog.Logger, opts Options) (*Pool, error) {
	if opts.Concurrency == 0 {
		opts.Concurrency = 10
	}
	if opts.Lease == 0 {
		opts.Lease = 30 * time.Second
	}
	if opts.Heartbeat == 0 {
		opts.Heartbeat = opts.Lease / 3
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = time.Second
	}
	if opts.DrainTimeout == 0 {
		opts.DrainTimeout = 10 * time.Second
	}

	var errs []error
	if store == nil {
		errs = append(errs, errors.New("store is required"))
	}
	if len(handlers) == 0 {
		errs = append(errs, errors.New("at least one handler is required"))
	}
	types := make([]string, 0, len(handlers))
	for t, h := range handlers {
		if err := job.ValidateType(t); err != nil {
			errs = append(errs, fmt.Errorf("handler %q: %w", t, err))
		}
		if h == nil {
			errs = append(errs, fmt.Errorf("handler %q is nil", t))
		}
		types = append(types, t)
	}
	slices.Sort(types)
	if opts.Concurrency < 1 || opts.Concurrency > MaxConcurrency {
		errs = append(errs, fmt.Errorf("concurrency must be between 1 and %d", MaxConcurrency))
	}
	if opts.Lease < time.Millisecond {
		errs = append(errs, errors.New("lease must be at least 1ms"))
	}
	if opts.Heartbeat <= 0 || opts.Heartbeat >= opts.Lease {
		errs = append(errs, errors.New("heartbeat must be positive and shorter than the lease"))
	}
	if opts.PollInterval < 0 || opts.DrainTimeout < 0 {
		errs = append(errs, errors.New("poll interval and drain timeout must not be negative"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("worker pool: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}
	return &Pool{store: store, handlers: handlers, types: types, logger: logger, opts: opts}, nil
}

// Run claims and runs jobs until ctx ends, then drains: it claims nothing
// more, waits up to DrainTimeout for running handlers, cancels those still
// running and waits for them to return. It returns nil once every claimed
// job has been reported.
func (p *Pool) Run(ctx context.Context) error {
	// Handlers outlive ctx by the drain period, under their own context.
	handlerCtx, cancelHandlers := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHandlers()

	var running sync.WaitGroup
	slots := make(chan struct{}, p.opts.Concurrency)
	p.logger.InfoContext(ctx, "worker pool started",
		slog.Any("types", p.types), slog.Int("concurrency", p.opts.Concurrency), slog.Duration("lease", p.opts.Lease))

	p.claimLoop(ctx, handlerCtx, slots, &running)

	done := make(chan struct{})
	go func() {
		running.Wait()
		close(done)
	}()
	p.logger.Info("worker pool draining", slog.Int("running", len(slots)))
	timer := time.NewTimer(p.opts.DrainTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		p.logger.Warn("drain timed out; cancelling running handlers", slog.Int("running", len(slots)))
		cancelHandlers()
		<-done
	}
	p.logger.Info("worker pool stopped")
	return nil
}

// claimLoop holds one slot per running handler and claims only as many
// jobs as there are free slots, so concurrency never exceeds the bound
// and no claimed job waits for a slot while its lease runs down.
func (p *Pool) claimLoop(ctx, handlerCtx context.Context, slots chan struct{}, running *sync.WaitGroup) {
	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		free := 1
	fill:
		for free < cap(slots) {
			select {
			case slots <- struct{}{}:
				free++
			default:
				break fill
			}
		}
		if ctx.Err() != nil {
			for range free {
				<-slots
			}
			return
		}

		jobs, err := p.claim(ctx, free)
		for range free - len(jobs) {
			<-slots
		}
		for _, j := range jobs {
			running.Add(1)
			go func() {
				defer running.Done()
				defer func() { <-slots }()
				p.process(handlerCtx, j)
			}()
		}
		if err != nil {
			p.logger.ErrorContext(ctx, "claim failed", slog.Any("error", err))
		}
		if len(jobs) < free {
			// The queue has no more due jobs for now.
			select {
			case <-ctx.Done():
				return
			case <-time.After(p.opts.PollInterval):
			}
		}
	}
}

func (p *Pool) claim(ctx context.Context, limit int) ([]job.Job, error) {
	// Detached: if shutdown cancelled the query after PostgreSQL had
	// committed the claim, the jobs would sit running until the reaper.
	claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reportTimeout)
	defer cancel()
	return p.store.Claim(claimCtx, p.types, limit, p.opts.Lease)
}

// process runs one claimed job to the end and reports the outcome.
func (p *Pool) process(handlerCtx context.Context, j job.Job) {
	log := p.logger.With(slog.Int64("job_id", j.ID), slog.String("type", j.Type), slog.Int("attempt", j.Attempt))
	ctx, cancel := context.WithCancel(handlerCtx)
	defer cancel()

	var lost atomic.Bool
	beating := make(chan struct{})
	go func() {
		defer close(beating)
		p.heartbeat(ctx, j, &lost, cancel, log)
	}()

	start := time.Now()
	err := p.call(ctx, j, log)
	cancel()
	<-beating

	reportCtx, cancelReport := context.WithTimeout(context.WithoutCancel(handlerCtx), reportTimeout)
	defer cancelReport()
	var reportErr error
	switch {
	case lost.Load():
		// Another worker may own the job now; whatever this attempt did,
		// its report would be refused.
		log.Warn("lease lost; outcome dropped", slog.Any("error", err))
		return
	case err == nil:
		reportErr = p.store.Succeed(reportCtx, j.ID, j.Attempt)
		if reportErr == nil {
			log.Info("job succeeded", slog.Duration("took", time.Since(start)))
		}
	case handlerCtx.Err() != nil:
		reportErr = p.store.Release(reportCtx, j.ID, j.Attempt)
		if reportErr == nil {
			log.Warn("job interrupted by shutdown; released", slog.Any("error", err))
		}
	default:
		var after job.Job
		after, reportErr = p.store.Fail(reportCtx, j.ID, j.Attempt, err)
		if reportErr == nil {
			log.Warn("job failed", slog.Any("error", err), slog.String("state", string(after.State)),
				slog.Time("run_at", after.RunAt), slog.Duration("took", time.Since(start)))
		}
	}
	switch {
	case errors.Is(reportErr, job.ErrLeaseLost):
		log.Warn("lease lost before the outcome was reported", slog.Any("error", reportErr))
	case reportErr != nil:
		// The job stays running; the reaper retries it once the lease runs out.
		log.Error("reporting the outcome failed", slog.Any("error", reportErr))
	}
}

// call runs the handler, turning a panic into an error so that one bad
// job fails instead of taking the process down.
func (p *Pool) call(ctx context.Context, j job.Job, log *slog.Logger) (err error) {
	h, ok := p.handlers[j.Type]
	if !ok {
		return job.Permanent(fmt.Errorf("no handler for job type %q", j.Type))
	}
	defer func() {
		if r := recover(); r != nil {
			log.Error("handler panicked", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h.Handle(ctx, j)
}

// heartbeat renews the lease until ctx ends. If the store says the lease
// is gone, it marks the attempt lost and cancels the handler. Other errors
// are retried at the next beat: the lease outlives a couple of misses.
func (p *Pool) heartbeat(ctx context.Context, j job.Job, lost *atomic.Bool, cancel context.CancelFunc, log *slog.Logger) {
	ticker := time.NewTicker(p.opts.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		beatCtx, cancelBeat := context.WithTimeout(ctx, p.opts.Heartbeat)
		err := p.store.Heartbeat(beatCtx, j.ID, j.Attempt, p.opts.Lease)
		cancelBeat()
		switch {
		case err == nil:
		case errors.Is(err, job.ErrLeaseLost), errors.Is(err, job.ErrNotFound):
			lost.Store(true)
			log.Warn("lease lost; cancelling the handler", slog.Any("error", err))
			cancel()
			return
		case ctx.Err() != nil:
			return
		default:
			log.Warn("heartbeat failed; retrying at the next beat", slog.Any("error", err))
		}
	}
}
