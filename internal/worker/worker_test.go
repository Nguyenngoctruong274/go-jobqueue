package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeStore hands out queued jobs and records every report.
type fakeStore struct {
	mu        sync.Mutex
	queue     []job.Job
	nextID    int64
	claims    []claimCall
	succeeded []int64
	failed    map[int64]error
	released  []int64
	beats     []time.Duration
	// Optional hooks; nil means the default behaviour.
	claimErr  func(call int) error
	beatErr   func(id int64) error
	running   func() int // reports how many handlers run, at claim time
	reportedC chan int64 // receives the id of every reported job
}

type claimCall struct {
	types   []string
	limit   int
	running int
}

func newFakeStore(jobs ...job.Job) *fakeStore {
	s := &fakeStore{failed: make(map[int64]error), reportedC: make(chan int64, 1000)}
	for _, j := range jobs {
		s.add(j)
	}
	return s
}

func (s *fakeStore) add(j job.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	j.ID = s.nextID
	j.State = job.StatePending
	if j.MaxAttempts == 0 {
		j.MaxAttempts = 3
	}
	s.queue = append(s.queue, j)
}

func (s *fakeStore) Claim(_ context.Context, types []string, limit int, lease time.Duration) ([]job.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	call := claimCall{types: types, limit: limit}
	if s.running != nil {
		call.running = s.running()
	}
	s.claims = append(s.claims, call)
	if s.claimErr != nil {
		if err := s.claimErr(len(s.claims)); err != nil {
			return nil, err
		}
	}
	var out []job.Job
	rest := s.queue[:0]
	for _, j := range s.queue {
		if len(out) < limit && slices.Contains(types, j.Type) {
			j.Attempt++
			j.State = job.StateRunning
			j.LeaseUntil = time.Now().Add(lease)
			out = append(out, j)
			continue
		}
		rest = append(rest, j)
	}
	s.queue = rest
	return out, nil
}

func (s *fakeStore) Heartbeat(_ context.Context, id int64, _ int, lease time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beats = append(s.beats, lease)
	if s.beatErr != nil {
		return s.beatErr(id)
	}
	return nil
}

func (s *fakeStore) Succeed(_ context.Context, id int64, _ int) error {
	s.mu.Lock()
	s.succeeded = append(s.succeeded, id)
	s.mu.Unlock()
	s.reportedC <- id
	return nil
}

func (s *fakeStore) Fail(_ context.Context, id int64, attempt int, cause error) (job.Job, error) {
	s.mu.Lock()
	s.failed[id] = cause
	s.mu.Unlock()
	s.reportedC <- id
	return job.Job{ID: id, Attempt: attempt, State: job.StatePending}, nil
}

func (s *fakeStore) Release(_ context.Context, id int64, _ int) error {
	s.mu.Lock()
	s.released = append(s.released, id)
	s.mu.Unlock()
	s.reportedC <- id
	return nil
}

func (s *fakeStore) snapshot() (succeeded []int64, failed map[int64]error, released []int64, claims []claimCall, beats int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	failed = make(map[int64]error, len(s.failed))
	for k, v := range s.failed {
		failed[k] = v
	}
	return slices.Clone(s.succeeded), failed, slices.Clone(s.released), slices.Clone(s.claims), len(s.beats)
}

// waitReports blocks until n jobs have been reported.
func (s *fakeStore) waitReports(t *testing.T, n int) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for range n {
		select {
		case <-s.reportedC:
		case <-timeout:
			t.Fatalf("timed out waiting for %d reports", n)
		}
	}
}

func ok(context.Context, job.Job) error { return nil }

// startPool runs a pool in the background; stop cancels it and waits for
// Run to return.
func startPool(t *testing.T, store Store, handlers map[string]Handler, opts Options) (stop func()) {
	t.Helper()
	if opts.PollInterval == 0 {
		opts.PollInterval = 5 * time.Millisecond
	}
	p, err := New(store, handlers, quiet, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after cancel")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func TestNewValidates(t *testing.T) {
	store := newFakeStore()
	h := map[string]Handler{"t": HandlerFunc(ok)}
	tests := []struct {
		name     string
		store    Store
		handlers map[string]Handler
		opts     Options
		want     string
	}{
		{"no store", nil, h, Options{}, "store is required"},
		{"no handlers", store, nil, Options{}, "at least one handler"},
		{"bad type", store, map[string]Handler{"Bad Type": HandlerFunc(ok)}, Options{}, `handler "Bad Type"`},
		{"nil handler", store, map[string]Handler{"t": nil}, Options{}, "is nil"},
		{"too much concurrency", store, h, Options{Concurrency: MaxConcurrency + 1}, "concurrency"},
		{"negative concurrency", store, h, Options{Concurrency: -1}, "concurrency"},
		{"heartbeat not shorter than lease", store, h, Options{Lease: time.Second, Heartbeat: time.Second}, "heartbeat"},
		{"tiny lease", store, h, Options{Lease: time.Microsecond, Heartbeat: time.Nanosecond}, "lease must be"},
		{"negative drain", store, h, Options{DrainTimeout: -time.Second}, "must not be negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.store, tt.handlers, nil, tt.opts)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want one containing %q", err, tt.want)
			}
		})
	}
	p, err := New(store, h, nil, Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := Options{Concurrency: 10, Lease: 30 * time.Second, Heartbeat: 10 * time.Second, PollInterval: time.Second, DrainTimeout: 10 * time.Second}
	if p.opts != want {
		t.Fatalf("defaults %+v, want %+v", p.opts, want)
	}
}

func TestRunsHandlersByType(t *testing.T) {
	store := newFakeStore(job.Job{Type: "email.send"}, job.Job{Type: "sms.send"}, job.Job{Type: "email.send"}, job.Job{Type: "fax.send"})
	var emails, sms atomic.Int32
	stop := startPool(t, store, map[string]Handler{
		"sms.send":   HandlerFunc(func(context.Context, job.Job) error { sms.Add(1); return nil }),
		"email.send": HandlerFunc(func(context.Context, job.Job) error { emails.Add(1); return nil }),
	}, Options{})
	store.waitReports(t, 3)
	stop()

	succeeded, failed, _, claims, _ := store.snapshot()
	if emails.Load() != 2 || sms.Load() != 1 || len(succeeded) != 3 || len(failed) != 0 {
		t.Fatalf("emails %d, sms %d, succeeded %v, failed %v", emails.Load(), sms.Load(), succeeded, failed)
	}
	for _, c := range claims {
		if !slices.Equal(c.types, []string{"email.send", "sms.send"}) {
			t.Fatalf("claimed types %v, want the sorted handler types", c.types)
		}
	}
	if slices.Contains(succeeded, 4) {
		t.Fatal("ran a job with no handler")
	}
}

func TestHandlerErrorFailsTheAttempt(t *testing.T) {
	boom := errors.New("smtp 451")
	store := newFakeStore(job.Job{Type: "retry"}, job.Job{Type: "perm"})
	startPool(t, store, map[string]Handler{
		"retry": HandlerFunc(func(context.Context, job.Job) error { return boom }),
		"perm":  HandlerFunc(func(context.Context, job.Job) error { return job.Permanent(boom) }),
	}, Options{})
	store.waitReports(t, 2)

	_, failed, _, _, _ := store.snapshot()
	if !errors.Is(failed[1], boom) || job.IsPermanent(failed[1]) {
		t.Fatalf("retryable failure reported as %v", failed[1])
	}
	if !errors.Is(failed[2], boom) || !job.IsPermanent(failed[2]) {
		t.Fatalf("permanent failure reported as %v, want it still marked permanent", failed[2])
	}
}

func TestPanicBecomesAFailure(t *testing.T) {
	store := newFakeStore(job.Job{Type: "t", Payload: []byte(`{"panic":true}`)}, job.Job{Type: "t"})
	startPool(t, store, map[string]Handler{
		"t": HandlerFunc(func(_ context.Context, j job.Job) error {
			if strings.Contains(string(j.Payload), "panic") {
				panic("unexpected payload shape")
			}
			return nil
		}),
	}, Options{Concurrency: 1})
	store.waitReports(t, 2)

	succeeded, failed, _, _, _ := store.snapshot()
	if err := failed[1]; err == nil || !strings.Contains(err.Error(), "handler panicked") || job.IsPermanent(err) {
		t.Fatalf("panicking job reported as %v, want a retryable failure", err)
	}
	if !slices.Equal(succeeded, []int64{2}) {
		t.Fatalf("succeeded %v: the pool must keep going after a panic", succeeded)
	}
}

func TestConcurrencyIsBounded(t *testing.T) {
	const limit, jobs = 3, 12
	store := newFakeStore()
	for range jobs {
		store.add(job.Job{Type: "t"})
	}
	var (
		running, peak atomic.Int32
		gate          = make(chan struct{})
	)
	store.running = func() int { return int(running.Load()) }
	startPool(t, store, map[string]Handler{
		"t": HandlerFunc(func(context.Context, job.Job) error {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			<-gate
			running.Add(-1)
			return nil
		}),
	}, Options{Concurrency: limit})

	// Give the pool time to try to overfill.
	time.Sleep(50 * time.Millisecond)
	if got := running.Load(); got != limit {
		t.Fatalf("%d handlers running, want %d", got, limit)
	}
	close(gate)
	store.waitReports(t, jobs)

	if peak.Load() != limit {
		t.Fatalf("peak concurrency %d, want %d", peak.Load(), limit)
	}
	_, _, _, claims, _ := store.snapshot()
	for _, c := range claims {
		if c.limit < 1 || c.limit+c.running > limit {
			t.Fatalf("claimed %d jobs with %d running: over the bound of %d", c.limit, c.running, limit)
		}
	}
}

func TestHeartbeatsRenewTheLease(t *testing.T) {
	store := newFakeStore(job.Job{Type: "slow"})
	startPool(t, store, map[string]Handler{
		"slow": HandlerFunc(func(context.Context, job.Job) error { time.Sleep(250 * time.Millisecond); return nil }),
	}, Options{Lease: 200 * time.Millisecond, Heartbeat: 40 * time.Millisecond})
	store.waitReports(t, 1)

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.beats) < 4 {
		t.Fatalf("%d heartbeats in 250ms at a 40ms period, want at least 4", len(store.beats))
	}
	for _, lease := range store.beats {
		if lease != 200*time.Millisecond {
			t.Fatalf("heartbeat renewed for %v, want the 200ms lease", lease)
		}
	}
}

func TestLostLeaseCancelsTheHandlerAndDropsTheOutcome(t *testing.T) {
	store := newFakeStore(job.Job{Type: "t"})
	store.beatErr = func(int64) error { return job.ErrLeaseLost }
	cancelled := make(chan struct{})
	stop := startPool(t, store, map[string]Handler{
		"t": HandlerFunc(func(ctx context.Context, _ job.Job) error {
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		}),
	}, Options{Lease: 100 * time.Millisecond, Heartbeat: 10 * time.Millisecond})

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not cancelled after the lease was lost")
	}
	stop()
	succeeded, failed, released, _, _ := store.snapshot()
	if len(succeeded)+len(failed)+len(released) != 0 {
		t.Fatalf("reported succeeded %v, failed %v, released %v; a lost attempt must report nothing", succeeded, failed, released)
	}
}

func TestTransientHeartbeatErrorsAreRetried(t *testing.T) {
	store := newFakeStore(job.Job{Type: "t"})
	var beats atomic.Int32
	store.beatErr = func(int64) error {
		if beats.Add(1) <= 2 {
			return errors.New("connection reset")
		}
		return nil
	}
	startPool(t, store, map[string]Handler{
		"t": HandlerFunc(func(context.Context, job.Job) error { time.Sleep(100 * time.Millisecond); return nil }),
	}, Options{Lease: 100 * time.Millisecond, Heartbeat: 10 * time.Millisecond})
	store.waitReports(t, 1)

	succeeded, _, _, _, _ := store.snapshot()
	if !slices.Equal(succeeded, []int64{1}) {
		t.Fatalf("succeeded %v; failed heartbeats must not abandon the job", succeeded)
	}
}

func TestShutdownWaitsForRunningHandlers(t *testing.T) {
	store := newFakeStore(job.Job{Type: "t"})
	started, finish := make(chan struct{}), make(chan struct{})
	p, err := New(store, map[string]Handler{
		"t": HandlerFunc(func(ctx context.Context, _ job.Job) error {
			close(started)
			select {
			case <-finish:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
	}, quiet, Options{PollInterval: 5 * time.Millisecond, DrainTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	<-started
	cancel()
	_, _, _, claimsAtStop, _ := store.snapshot()
	store.add(job.Job{Type: "t"}) // arrives during the drain
	select {
	case <-done:
		t.Fatal("Run returned while a handler was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	succeeded, _, released, claims, _ := store.snapshot()
	if !slices.Equal(succeeded, []int64{1}) || len(released) != 0 {
		t.Fatalf("succeeded %v, released %v; the running job should finish normally", succeeded, released)
	}
	if len(claims) != len(claimsAtStop) {
		t.Fatalf("%d claims after shutdown started", len(claims)-len(claimsAtStop))
	}
}

func TestShutdownReleasesHandlersThatOutliveTheDrain(t *testing.T) {
	store := newFakeStore(job.Job{Type: "t"})
	started := make(chan struct{})
	stop := startPool(t, store, map[string]Handler{
		"t": HandlerFunc(func(ctx context.Context, _ job.Job) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}),
	}, Options{DrainTimeout: 50 * time.Millisecond})

	<-started
	begin := time.Now()
	stop()
	if took := time.Since(begin); took < 50*time.Millisecond {
		t.Fatalf("Run returned after %v, before the drain timeout", took)
	}
	succeeded, failed, released, _, _ := store.snapshot()
	if !slices.Equal(released, []int64{1}) || len(succeeded)+len(failed) != 0 {
		t.Fatalf("released %v, succeeded %v, failed %v; want the job released", released, succeeded, failed)
	}
}

func TestClaimErrorsAreRetried(t *testing.T) {
	store := newFakeStore(job.Job{Type: "t"})
	store.claimErr = func(call int) error {
		if call <= 2 {
			return errors.New("too many connections")
		}
		return nil
	}
	startPool(t, store, map[string]Handler{"t": HandlerFunc(ok)}, Options{})
	store.waitReports(t, 1)
	succeeded, _, _, _, _ := store.snapshot()
	if !slices.Equal(succeeded, []int64{1}) {
		t.Fatalf("succeeded %v after claim errors", succeeded)
	}
}

func TestIdlePoolPollsInsteadOfSpinning(t *testing.T) {
	store := newFakeStore()
	stop := startPool(t, store, map[string]Handler{"t": HandlerFunc(ok)}, Options{PollInterval: 30 * time.Millisecond})
	time.Sleep(200 * time.Millisecond)
	stop()
	_, _, _, claims, _ := store.snapshot()
	// About 200/30 ≈ 7 polls; a busy loop would make thousands.
	if len(claims) < 3 || len(claims) > 12 {
		t.Fatalf("%d claims in 200ms at a 30ms poll interval", len(claims))
	}
}
