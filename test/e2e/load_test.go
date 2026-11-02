package e2e

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// TestLoad measures the whole pipeline: producers enqueue through the
// store while a pool of 16 drains, and reports throughput and the pickup
// delay (claim time minus run_at) percentiles. It only runs when
// JQ_LOAD_JOBS is set, e.g. JQ_LOAD_JOBS=5000, because it takes seconds
// and its numbers depend on the machine.
func TestLoad(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("JQ_LOAD_JOBS"))
	if n <= 0 {
		t.Skip("JQ_LOAD_JOBS not set; skipping the load test")
	}
	s := newStack(t)
	ctx := context.Background()

	var (
		mu     sync.Mutex
		pickup []time.Duration
		done   = make(chan struct{})
		seen   int
	)
	s.mu.Lock()
	s.onLoad = func(_ context.Context, j job.Job) error {
		d := max(0, time.Since(j.RunAt))
		mu.Lock()
		pickup = append(pickup, d)
		seen++
		if seen == n {
			close(done)
		}
		mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	start := time.Now()
	var wg sync.WaitGroup
	const producers = 8
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := p; i < n; i += producers {
				if _, _, err := s.store.Enqueue(ctx, job.Spec{Type: "load", Payload: []byte(fmt.Sprintf(`{"i":%d}`, i))}); err != nil {
					t.Error(err)
					return
				}
			}
		}(p)
	}
	wg.Wait()
	enqueued := time.Since(start)
	select {
	case <-done:
	case <-time.After(2 * time.Minute):
		t.Fatalf("only %d of %d jobs ran within 2 minutes", seen, n)
	}
	total := time.Since(start)

	sort.Slice(pickup, func(i, j int) bool { return pickup[i] < pickup[j] })
	pct := func(p float64) time.Duration { return pickup[min(len(pickup)-1, int(float64(len(pickup))*p))] }
	t.Logf("%d jobs: enqueued in %v (%.0f/s by %d producers), all done in %v (%.0f jobs/s end to end)",
		n, enqueued.Round(time.Millisecond), float64(n)/enqueued.Seconds(), producers,
		total.Round(time.Millisecond), float64(n)/total.Seconds())
	t.Logf("pickup delay: p50 %v, p90 %v, p99 %v, max %v",
		pct(0.50).Round(time.Millisecond), pct(0.90).Round(time.Millisecond),
		pct(0.99).Round(time.Millisecond), slices.Max(pickup).Round(time.Millisecond))
}
