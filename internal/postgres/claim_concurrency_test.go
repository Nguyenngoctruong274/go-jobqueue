package postgres

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// claimLog records which worker claimed which job, and fails on a repeat.
type claimLog struct {
	mu sync.Mutex
	by map[int64]int
}

func (l *claimLog) add(t *testing.T, worker int, jobs []job.Job) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, j := range jobs {
		if prev, dup := l.by[j.ID]; dup {
			t.Errorf("job %d claimed by worker %d and again by worker %d", j.ID, prev, worker)
		}
		l.by[j.ID] = worker
		if j.Attempt != 1 || j.State != job.StateRunning {
			t.Errorf("job %d claimed at attempt %d in state %s, want attempt 1, running", j.ID, j.Attempt, j.State)
		}
	}
}

// TestClaimEachJobOnceUnderContention has many workers drain a full queue
// at once with random batch sizes. Every job must be claimed exactly once.
func TestClaimEachJobOnceUnderContention(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const jobs, workers = 2000, 16
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (type, max_attempts, priority)
		SELECT 'bulk', 3, i % 7 - 3 FROM generate_series(1, $1) AS i`, jobs); err != nil {
		t.Fatal(err)
	}

	log := &claimLog{by: make(map[int64]int)}
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		calls atomic.Int64
	)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rnd := rand.New(rand.NewPCG(uint64(w), 1))
			<-start
			for {
				calls.Add(1)
				got, err := s.Claim(ctx, ClaimRequest{Limit: 1 + rnd.IntN(25), Lease: time.Minute})
				if err != nil {
					t.Errorf("worker %d: %v", w, err)
					return
				}
				if len(got) == 0 {
					// Rows still locked by other workers' claims are theirs.
					return
				}
				log.add(t, w, got)
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(log.by) != jobs {
		t.Fatalf("claimed %d distinct jobs, want %d", len(log.by), jobs)
	}
	perWorker := make(map[int]int)
	for _, w := range log.by {
		perWorker[w]++
	}
	t.Logf("%d claim calls; jobs per worker: %v", calls.Load(), perWorker)
	assertAllRunningOnce(t, s, jobs)
}

// TestClaimWhileProducersEnqueue interleaves producers and workers: no job
// enqueued during the run may be lost or handed out twice.
func TestClaimWhileProducersEnqueue(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const producers, perProducer, workers = 4, 150, 8

	var (
		produced sync.WaitGroup
		done     atomic.Bool
		enqueued sync.Map
	)
	for p := range producers {
		produced.Add(1)
		go func() {
			defer produced.Done()
			for i := range perProducer {
				j, _, err := s.Enqueue(ctx, job.Spec{
					Type: "live", Priority: i % 3,
					Payload: []byte(fmt.Sprintf(`{"producer":%d,"i":%d}`, p, i)),
				})
				if err != nil {
					t.Errorf("producer %d: %v", p, err)
					return
				}
				enqueued.Store(j.ID, true)
			}
		}()
	}
	go func() {
		produced.Wait()
		done.Store(true)
	}()

	log := &claimLog{by: make(map[int64]int)}
	var consumed sync.WaitGroup
	for w := range workers {
		consumed.Add(1)
		go func() {
			defer consumed.Done()
			for {
				// Read the flag before claiming: if producers had finished,
				// every job is committed and visible to this claim.
				finished := done.Load()
				got, err := s.Claim(ctx, ClaimRequest{Limit: 5, Lease: time.Minute})
				if err != nil {
					t.Errorf("worker %d: %v", w, err)
					return
				}
				log.add(t, w, got)
				if len(got) == 0 {
					if finished {
						return
					}
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}
	consumed.Wait()

	enqueued.Range(func(id, _ any) bool {
		if _, ok := log.by[id.(int64)]; !ok {
			t.Errorf("job %d was enqueued but never claimed", id)
		}
		return true
	})
	assertAllRunningOnce(t, s, producers*perProducer)
}

// TestClaimSkipsLockedRowsInsteadOfWaiting holds a lock on the best job in
// another transaction. Claim must hand out the next job at once rather
// than queue behind the lock, and the locked job stays available.
func TestClaimSkipsLockedRowsInsteadOfWaiting(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	top, _ := enqueue(t, s, job.Spec{Type: "t", Priority: 10})
	next, _ := enqueue(t, s, job.Spec{Type: "t"})

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SELECT 1 FROM jobs WHERE id = $1 FOR UPDATE`, top.ID); err != nil {
		t.Fatal(err)
	}

	// Without SKIP LOCKED this would block until the deadline.
	shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, err := s.Claim(shortCtx, ClaimRequest{Limit: 2, Lease: time.Minute})
	if err != nil {
		t.Fatalf("Claim blocked or failed behind a locked row: %v", err)
	}
	if want := []int64{next.ID}; !equalIDs(ids(got), want) {
		t.Fatalf("claimed %v, want %v", ids(got), want)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	got = claimN(t, s, ClaimRequest{Limit: 2, Lease: time.Minute})
	if want := []int64{top.ID}; !equalIDs(ids(got), want) {
		t.Fatalf("after the lock was released claimed %v, want %v", ids(got), want)
	}
}

func assertAllRunningOnce(t *testing.T, s *Store, want int) {
	t.Helper()
	var total, running, firstAttempt int
	if err := s.db.QueryRowContext(context.Background(), `
		SELECT count(*), count(*) FILTER (WHERE state = 'running'), count(*) FILTER (WHERE attempt = 1)
		FROM jobs`).Scan(&total, &running, &firstAttempt); err != nil {
		t.Fatal(err)
	}
	if total != want || running != want || firstAttempt != want {
		t.Fatalf("jobs: %d total, %d running, %d at attempt 1; want all %d", total, running, firstAttempt, want)
	}
}
