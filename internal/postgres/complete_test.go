package postgres

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/testdb"
)

// exact is a backoff with no jitter, so retry times can be checked to the
// microsecond: 1s, 2s, 4s, ... capped at 1m.
var exact = job.Backoff{Base: time.Second, Max: time.Minute}

func newExactStore(t *testing.T) *Store {
	t.Helper()
	return NewStore(testdb.New(t, Migrate), WithBackoff(exact))
}

// claimOne enqueues a job and claims it, returning the running job.
func claimOne(t *testing.T, s *Store, spec job.Spec, lease time.Duration) job.Job {
	t.Helper()
	enqueue(t, s, spec)
	got := claimN(t, s, ClaimRequest{Limit: 1, Lease: lease})
	if len(got) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(got))
	}
	return got[0]
}

func get(t *testing.T, s *Store, id int64) job.Job {
	t.Helper()
	j, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// updatedAt reads the database clock at the job's last change.
func updatedAt(t *testing.T, s *Store, id int64) time.Time {
	t.Helper()
	var ts time.Time
	if err := s.db.QueryRowContext(context.Background(), `SELECT updated_at FROM jobs WHERE id = $1`, id).Scan(&ts); err != nil {
		t.Fatal(err)
	}
	return ts
}

func TestSucceed(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()
	running := claimOne(t, s, job.Spec{Type: "t"}, time.Minute)

	if err := s.Succeed(ctx, running.ID, running.Attempt); err != nil {
		t.Fatal(err)
	}
	done := get(t, s, running.ID)
	if done.State != job.StateSucceeded || !done.LeaseUntil.IsZero() || done.Attempt != 1 {
		t.Fatalf("after Succeed: %+v", done)
	}
	// Finished jobs are not claimable and cannot be reported on again.
	if more := claimN(t, s, ClaimRequest{Limit: 10, Lease: time.Minute}); len(more) != 0 {
		t.Fatalf("claimed a succeeded job: %v", ids(more))
	}
	if err := s.Succeed(ctx, running.ID, running.Attempt); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("second Succeed: %v, want ErrLeaseLost", err)
	}
}

func TestFailRetriesWithBackoffThenDies(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()
	first := claimOne(t, s, job.Spec{Type: "t", MaxAttempts: 3}, time.Minute)

	// Attempt 1 fails: pending again, due 1s after the failure.
	j, err := s.Fail(ctx, first.ID, 1, errors.New("boom"))
	if err != nil {
		t.Fatal(err)
	}
	failedAt := updatedAt(t, s, first.ID)
	if j.State != job.StatePending || j.LastError != "boom" || !j.RunAt.Equal(failedAt.Add(time.Second)) || !j.LeaseUntil.IsZero() {
		t.Fatalf("after first failure: %+v (failed at %s)", j, failedAt)
	}
	if stored := get(t, s, first.ID); stored.State != j.State || !stored.RunAt.Equal(j.RunAt) || stored.LastError != j.LastError {
		t.Fatalf("Fail returned %+v but stored %+v", j, stored)
	}
	// Not due for another second.
	if got := claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute}); len(got) != 0 {
		t.Fatalf("claimed a job in backoff: %v", ids(got))
	}

	// Make it due, claim attempt 2, fail: 2s backoff.
	s.mustExec(t, `UPDATE jobs SET run_at = now() WHERE id = $1`, first.ID)
	second := claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute})
	if len(second) != 1 || second[0].Attempt != 2 {
		t.Fatalf("second claim: %+v", second)
	}
	j, err = s.Fail(ctx, first.ID, 2, errors.New("boom again"))
	if err != nil {
		t.Fatal(err)
	}
	if !j.RunAt.Equal(updatedAt(t, s, first.ID).Add(2 * time.Second)) {
		t.Fatalf("second backoff: run_at %s, updated %s", j.RunAt, updatedAt(t, s, first.ID))
	}

	// Attempt 3 is the last: a failure kills the job.
	s.mustExec(t, `UPDATE jobs SET run_at = now() WHERE id = $1`, first.ID)
	claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute})
	j, err = s.Fail(ctx, first.ID, 3, errors.New("final"))
	if err != nil {
		t.Fatal(err)
	}
	if j.State != job.StateDead || j.LastError != "final" || j.Attempt != 3 {
		t.Fatalf("after last failure: %+v", j)
	}
	if got := claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute}); len(got) != 0 {
		t.Fatalf("claimed a dead job: %v", ids(got))
	}
}

func TestFailPermanentDiesAtOnce(t *testing.T) {
	s := newExactStore(t)
	running := claimOne(t, s, job.Spec{Type: "t", MaxAttempts: 5}, time.Minute)
	j, err := s.Fail(context.Background(), running.ID, 1, job.Permanent(errors.New("payload will never parse")))
	if err != nil {
		t.Fatal(err)
	}
	if j.State != job.StateDead || j.Attempt != 1 || j.LastError != "payload will never parse" {
		t.Fatalf("permanent failure: %+v", j)
	}
}

func TestFailTruncatesAndSanitizesTheError(t *testing.T) {
	s := newExactStore(t)
	running := claimOne(t, s, job.Spec{Type: "t"}, time.Minute)
	huge := strings.Repeat("é", job.MaxErrorBytes) + "\x00tail"
	j, err := s.Fail(context.Background(), running.ID, 1, errors.New(huge))
	if err != nil {
		t.Fatal(err)
	}
	if len(j.LastError) > job.MaxErrorBytes || strings.ContainsRune(j.LastError, 0) || !strings.HasPrefix(j.LastError, "éé") {
		t.Fatalf("stored error of %d bytes: %q...", len(j.LastError), j.LastError[:20])
	}
}

func TestStaleAttemptIsFenced(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()
	// Worker A claims with a lease that expires immediately.
	a := claimOne(t, s, job.Spec{Type: "t", MaxAttempts: 5}, time.Microsecond)
	time.Sleep(2 * time.Millisecond)

	// The reaper hands the job on; worker B claims attempt 2.
	expired, err := s.Reap(ctx, 10)
	if err != nil || !slices.Equal(expired, []int64{a.ID}) {
		t.Fatalf("Reap: %v, %v", expired, err)
	}
	after := get(t, s, a.ID)
	if after.State != job.StatePending || after.LastError == "" {
		t.Fatalf("after reap: %+v", after)
	}
	s.mustExec(t, `UPDATE jobs SET run_at = now() WHERE id = $1`, a.ID)
	b := claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute})
	if len(b) != 1 || b[0].Attempt != 2 {
		t.Fatalf("worker B claim: %+v", b)
	}

	// Worker A wakes up and reports on attempt 1: every report is refused
	// and B's attempt is untouched.
	if err := s.Succeed(ctx, a.ID, a.Attempt); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("stale Succeed: %v", err)
	}
	if _, err := s.Fail(ctx, a.ID, a.Attempt, errors.New("late")); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("stale Fail: %v", err)
	}
	if err := s.Heartbeat(ctx, a.ID, a.Attempt, time.Hour); !errors.Is(err, job.ErrLeaseLost) {
		t.Fatalf("stale Heartbeat: %v", err)
	}
	cur := get(t, s, a.ID)
	if cur.State != job.StateRunning || cur.Attempt != 2 || !cur.LeaseUntil.Equal(b[0].LeaseUntil) || cur.LastError == "late" {
		t.Fatalf("B's attempt was touched by A: %+v", cur)
	}

	// B's own report goes through.
	if err := s.Succeed(ctx, a.ID, 2); err != nil {
		t.Fatal(err)
	}
}

func TestHeartbeatExtendsTheLease(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()
	running := claimOne(t, s, job.Spec{Type: "t"}, time.Second)

	if err := s.Heartbeat(ctx, running.ID, running.Attempt, time.Hour); err != nil {
		t.Fatal(err)
	}
	j := get(t, s, running.ID)
	if want := updatedAt(t, s, running.ID).Add(time.Hour); !j.LeaseUntil.Equal(want) || j.State != job.StateRunning {
		t.Fatalf("lease_until %s, want %s; state %s", j.LeaseUntil, want, j.State)
	}
	if err := s.Heartbeat(ctx, running.ID, running.Attempt, 0); err == nil {
		t.Fatal("zero lease accepted")
	}
}

func TestReapExpiresOnlyRunOutLeases(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()
	// Two jobs: one with a lease long gone, one still alive.
	stale := claimOne(t, s, job.Spec{Type: "t", MaxAttempts: 2}, time.Microsecond)
	live := claimOne(t, s, job.Spec{Type: "t"}, time.Hour)
	time.Sleep(2 * time.Millisecond)

	expired, err := s.Reap(ctx, 10)
	if err != nil || !slices.Equal(expired, []int64{stale.ID}) {
		t.Fatalf("Reap: %v, %v; want [%d]", expired, err, stale.ID)
	}
	j := get(t, s, stale.ID)
	if j.State != job.StatePending || j.Attempt != 1 || !strings.Contains(j.LastError, "lease expired") ||
		!j.RunAt.Equal(updatedAt(t, s, stale.ID).Add(time.Second)) {
		t.Fatalf("reaped job: %+v", j)
	}
	if l := get(t, s, live.ID); l.State != job.StateRunning || !l.LeaseUntil.Equal(live.LeaseUntil) {
		t.Fatalf("live job was touched: %+v", l)
	}
	if again, err := s.Reap(ctx, 10); err != nil || len(again) != 0 {
		t.Fatalf("second Reap: %v, %v", again, err)
	}

	// A reaped job whose attempts are spent dies.
	s.mustExec(t, `UPDATE jobs SET run_at = now() WHERE id = $1`, stale.ID)
	claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Microsecond})
	time.Sleep(2 * time.Millisecond)
	if expired, err := s.Reap(ctx, 10); err != nil || !slices.Equal(expired, []int64{stale.ID}) {
		t.Fatalf("Reap of last attempt: %v, %v", expired, err)
	}
	if j := get(t, s, stale.ID); j.State != job.StateDead || j.Attempt != 2 {
		t.Fatalf("after last lease expired: %+v", j)
	}
}

func TestReapRespectsLimitAndValidatesIt(t *testing.T) {
	s := newExactStore(t)
	for i := 0; i < 5; i++ {
		enqueue(t, s, job.Spec{Type: "t"})
	}
	claimN(t, s, ClaimRequest{Limit: 5, Lease: time.Microsecond})
	time.Sleep(2 * time.Millisecond)

	if _, err := s.Reap(context.Background(), 0); err == nil {
		t.Fatal("limit 0 accepted")
	}
	first, err := s.Reap(context.Background(), 2)
	if err != nil || len(first) != 2 {
		t.Fatalf("Reap(2): %v, %v", first, err)
	}
	rest, err := s.Reap(context.Background(), 10)
	if err != nil || len(rest) != 3 {
		t.Fatalf("Reap(10): %v, %v", rest, err)
	}
}

// TestReapersRaceWorkers runs reapers against workers that finish just as
// their lease runs out. Whatever the interleaving, each attempt ends
// exactly once: a worker's Succeed and the reaper's Expire never both
// apply to the same attempt.
func TestReapersRaceWorkers(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()
	const n = 40
	for i := 0; i < n; i++ {
		enqueue(t, s, job.Spec{Type: "t", MaxAttempts: 10})
	}
	claimed := claimN(t, s, ClaimRequest{Limit: n, Lease: 3 * time.Millisecond})
	if len(claimed) != n {
		t.Fatalf("claimed %d, want %d", len(claimed), n)
	}

	var (
		wg        sync.WaitGroup
		succeeded atomic.Int64
		lost      atomic.Int64
		reaped    atomic.Int64
	)
	for _, j := range claimed {
		wg.Add(1)
		go func(j job.Job) {
			defer wg.Done()
			// Odd jobs report at once; even ones outlive their 3ms lease and
			// should find it taken by the reaper, unless they slip in first.
			if j.ID%2 == 0 {
				time.Sleep(60 * time.Millisecond)
			}
			switch err := s.Succeed(ctx, j.ID, j.Attempt); {
			case err == nil:
				succeeded.Add(1)
			case errors.Is(err, job.ErrLeaseLost):
				lost.Add(1)
			default:
				t.Error(err)
			}
		}(j)
	}
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			deadline := time.Now().Add(200 * time.Millisecond)
			for time.Now().Before(deadline) {
				expired, err := s.Reap(ctx, 10)
				if err != nil {
					t.Error(err)
					return
				}
				reaped.Add(int64(len(expired)))
				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()

	if succeeded.Load()+lost.Load() != n {
		t.Fatalf("succeeded %d + lost %d != %d", succeeded.Load(), lost.Load(), n)
	}
	// Every job that a worker lost was reaped, and no succeeded job was.
	if reaped.Load() != lost.Load() {
		t.Fatalf("reaped %d attempts but workers lost %d", reaped.Load(), lost.Load())
	}
	var done, pending, running int
	if err := s.db.QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE state = 'succeeded'),
		       count(*) FILTER (WHERE state = 'pending'),
		       count(*) FILTER (WHERE state = 'running') FROM jobs`).Scan(&done, &pending, &running); err != nil {
		t.Fatal(err)
	}
	if int64(done) != succeeded.Load() || int64(pending) != lost.Load() || running != 0 {
		t.Fatalf("states: succeeded %d pending %d running %d; workers: %d/%d", done, pending, running, succeeded.Load(), lost.Load())
	}
	t.Logf("%d finished in time, %d lost their lease and were reaped", succeeded.Load(), lost.Load())
}

func TestCancelAndRequeue(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()

	pending, _ := enqueue(t, s, job.Spec{Type: "t"})
	if err := s.Cancel(ctx, pending.ID); err != nil {
		t.Fatal(err)
	}
	if j := get(t, s, pending.ID); j.State != job.StateCancelled {
		t.Fatalf("after Cancel: %+v", j)
	}
	if err := s.Cancel(ctx, pending.ID); !errors.Is(err, job.ErrInvalidTransition) {
		t.Fatalf("cancel twice: %v", err)
	}

	running := claimOne(t, s, job.Spec{Type: "t", MaxAttempts: 1}, time.Minute)
	if err := s.Cancel(ctx, running.ID); !errors.Is(err, job.ErrInvalidTransition) {
		t.Fatalf("cancel running: %v", err)
	}
	if err := s.Requeue(ctx, running.ID); !errors.Is(err, job.ErrInvalidTransition) {
		t.Fatalf("requeue running: %v", err)
	}
	if _, err := s.Fail(ctx, running.ID, 1, errors.New("dead")); err != nil {
		t.Fatal(err)
	}

	if err := s.Requeue(ctx, running.ID); err != nil {
		t.Fatal(err)
	}
	j := get(t, s, running.ID)
	if j.State != job.StatePending || j.Attempt != 0 || j.LastError != "dead" || !j.RunAt.Equal(updatedAt(t, s, j.ID)) {
		t.Fatalf("after Requeue: %+v", j)
	}
	if got := claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute}); len(got) != 1 || got[0].Attempt != 1 {
		t.Fatalf("requeued job not claimable: %+v", got)
	}
}

func TestTransitionsOnMissingJob(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()
	for name, err := range map[string]error{
		"succeed":   s.Succeed(ctx, 404, 1),
		"heartbeat": s.Heartbeat(ctx, 404, 1, time.Minute),
		"cancel":    s.Cancel(ctx, 404),
		"requeue":   s.Requeue(ctx, 404),
	} {
		if !errors.Is(err, job.ErrNotFound) {
			t.Errorf("%s: %v, want ErrNotFound", name, err)
		}
	}
	if _, err := s.Fail(ctx, 404, 1, errors.New("x")); !errors.Is(err, job.ErrNotFound) {
		t.Errorf("fail: %v", err)
	}
}

func (s *Store) mustExec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}
