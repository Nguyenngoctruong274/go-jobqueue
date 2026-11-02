package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/metrics"
)

func TestQueueMetrics(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()
	reg := metrics.NewRegistry()
	RegisterQueueMetrics(reg, s.db, quiet, 0) // no cache: every scrape reads
	RegisterPoolMetrics(reg, s.db)

	scrape := func() string {
		var b strings.Builder
		reg.Write(&b)
		return b.String()
	}
	for _, want := range []string{
		`jobqueue_jobs{state="pending"} 0`, `jobqueue_jobs{state="dead"} 0`,
		"jobqueue_due_jobs 0\n", "jobqueue_oldest_due_seconds 0\n", "db_pool_max_open_connections 20\n",
	} {
		if out := scrape(); !strings.Contains(out, want) {
			t.Errorf("empty queue: missing %q in:\n%s", want, out)
		}
	}

	// One running, one dead (claimed first so the claims take them), then
	// two due (one for 10 minutes) and one delayed.
	claimOne(t, s, job.Spec{Type: "t"}, time.Minute)
	dead := claimOne(t, s, job.Spec{Type: "t", MaxAttempts: 1}, time.Minute)
	if _, err := s.Fail(ctx, dead.ID, 1, errors.New("x")); err != nil {
		t.Fatal(err)
	}
	enqueue(t, s, job.Spec{Type: "t", RunAt: time.Now().Add(-10 * time.Minute)})
	enqueue(t, s, job.Spec{Type: "t"})
	enqueue(t, s, job.Spec{Type: "t", RunAt: time.Now().Add(time.Hour)})

	out := scrape()
	for _, want := range []string{
		`jobqueue_jobs{state="pending"} 3`, `jobqueue_jobs{state="running"} 1`, `jobqueue_jobs{state="dead"} 1`,
		"jobqueue_due_jobs 2\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	var oldest float64
	for _, line := range strings.Split(out, "\n") {
		if v, ok := strings.CutPrefix(line, "jobqueue_oldest_due_seconds "); ok {
			if _, err := scan(v, &oldest); err != nil {
				t.Fatal(err)
			}
		}
	}
	if oldest < 599 || oldest > 601 {
		t.Fatalf("jobqueue_oldest_due_seconds = %v, want about 600", oldest)
	}
}

func TestQueueMetricsCacheAndFailure(t *testing.T) {
	s := newExactStore(t)
	reg := metrics.NewRegistry()
	RegisterQueueMetrics(reg, s.db, quiet, time.Hour)
	scrape := func() string {
		var b strings.Builder
		reg.Write(&b)
		return b.String()
	}
	if !strings.Contains(scrape(), `jobqueue_jobs{state="pending"} 0`) {
		t.Fatal("first scrape")
	}
	enqueue(t, s, job.Spec{Type: "t"})
	// Cached: the new job is not visible yet.
	if !strings.Contains(scrape(), `jobqueue_jobs{state="pending"} 0`) {
		t.Fatal("cache did not hold")
	}

	// A broken read keeps the last snapshot instead of reporting zeros.
	reg2 := metrics.NewRegistry()
	RegisterQueueMetrics(reg2, s.db, quiet, 0)
	var b strings.Builder
	reg2.Write(&b)
	if !strings.Contains(b.String(), `jobqueue_jobs{state="pending"} 1`) {
		t.Fatalf("fresh registry: %s", b.String())
	}
	s.mustExec(t, `ALTER TABLE jobs RENAME TO jobs_gone`)
	b.Reset()
	reg2.Write(&b)
	if !strings.Contains(b.String(), `jobqueue_jobs{state="pending"} 1`) {
		t.Fatalf("after a failed read, want the last snapshot: %s", b.String())
	}
}

func scan(s string, v *float64) (int, error) { return fmt.Sscan(s, v) }
