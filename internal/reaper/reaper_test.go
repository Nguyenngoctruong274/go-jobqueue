package reaper

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// scriptedStore returns one prepared answer per Reap call and records
// the limit of each call.
type scriptedStore struct {
	mu      sync.Mutex
	answers []answer
	limits  []int
	calls   chan struct{}
}

type answer struct {
	ids []int64
	err error
}

func (s *scriptedStore) Reap(_ context.Context, limit int) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits = append(s.limits, limit)
	var a answer
	if len(s.answers) > 0 {
		a, s.answers = s.answers[0], s.answers[1:]
	}
	if s.calls != nil {
		s.calls <- struct{}{}
	}
	return a.ids, a.err
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRunDrainsBacklogThenIdles(t *testing.T) {
	full := make([]int64, 3)
	store := &scriptedStore{
		// Two full batches, one partial, then an error, then nothing.
		answers: []answer{{ids: full}, {ids: full}, {ids: full[:1]}, {err: errors.New("db down")}},
		calls:   make(chan struct{}, 100),
	}
	r := New(store, quiet, Options{Interval: 30 * time.Millisecond, BatchSize: 3})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// The three backlog sweeps happen back to back, well within one interval.
	start := time.Now()
	for i := 0; i < 3; i++ {
		<-store.calls
	}
	if time.Since(start) > 25*time.Millisecond {
		t.Fatalf("backlog took %s; full batches should be followed at once", time.Since(start))
	}
	// The partial batch, the error and the empty sweep each wait an interval.
	for i := 0; i < 2; i++ {
		before := time.Now()
		<-store.calls
		if since := time.Since(before); since < 20*time.Millisecond {
			t.Fatalf("sweep %d came after %s, want about an interval", 4+i, since)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, l := range store.limits {
		if l != 3 {
			t.Fatalf("Reap called with limit %d, want 3", l)
		}
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	store := &scriptedStore{}
	r := New(store, nil, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDefaults(t *testing.T) {
	r := New(&scriptedStore{}, nil, Options{})
	if r.opts.Interval != 5*time.Second || r.opts.BatchSize != 100 || r.logger == nil {
		t.Fatalf("defaults: %+v", r.opts)
	}
}

func TestSweepReportsPartialProgress(t *testing.T) {
	store := &scriptedStore{answers: []answer{{ids: []int64{1, 2}, err: errors.New("then the connection dropped")}}}
	n, err := New(store, quiet, Options{}).Sweep(context.Background())
	if n != 2 || err == nil {
		t.Fatalf("Sweep = %d, %v", n, err)
	}
}
