package worker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/postgres"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/testdb"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/worker"
)

// pgStore adapts postgres.Store's ClaimRequest to the pool's Claim.
type pgStore struct{ *postgres.Store }

func (s pgStore) Claim(ctx context.Context, types []string, limit int, lease time.Duration) ([]job.Job, error) {
	return s.Store.Claim(ctx, postgres.ClaimRequest{Types: types, Limit: limit, Lease: lease})
}

// TestPoolOnPostgreSQL runs two pools against one database with a mix of
// jobs that succeed, fail once then succeed, fail for good, and panic.
// Every job must end in the state its handler implies, with the attempts
// it implies, and no job may run twice at the same attempt.
func TestPoolOnPostgreSQL(t *testing.T) {
	db := testdb.New(t, postgres.Migrate)
	store := postgres.NewStore(db, postgres.WithBackoff(job.Backoff{Base: 10 * time.Millisecond, Max: 10 * time.Millisecond}))
	ctx := context.Background()

	const perType = 40
	kinds := []string{"ok", "flaky", "bad", "panics"}
	want := map[int64]struct {
		state   job.State
		attempt int
	}{}
	for i := range perType {
		for _, k := range kinds {
			j, _, err := store.Enqueue(ctx, job.Spec{Type: k, MaxAttempts: 3, Payload: []byte(fmt.Sprintf(`{"i":%d}`, i))})
			if err != nil {
				t.Fatal(err)
			}
			w := want[j.ID]
			switch k {
			case "ok":
				w.state, w.attempt = job.StateSucceeded, 1
			case "flaky":
				w.state, w.attempt = job.StateSucceeded, 2
			case "bad":
				w.state, w.attempt = job.StateDead, 1
			case "panics":
				w.state, w.attempt = job.StateDead, 3
			}
			want[j.ID] = w
		}
	}

	var (
		mu   sync.Mutex
		runs = map[[2]int64]int{} // (id, attempt) -> times run
	)
	record := func(j job.Job) {
		mu.Lock()
		defer mu.Unlock()
		runs[[2]int64{j.ID, int64(j.Attempt)}]++
	}
	handlers := map[string]worker.Handler{
		"ok": worker.HandlerFunc(func(_ context.Context, j job.Job) error { record(j); return nil }),
		"flaky": worker.HandlerFunc(func(_ context.Context, j job.Job) error {
			record(j)
			if j.Attempt == 1 {
				return errors.New("upstream 503")
			}
			return nil
		}),
		"bad": worker.HandlerFunc(func(_ context.Context, j job.Job) error {
			record(j)
			return job.Permanent(errors.New("payload rejected"))
		}),
		"panics": worker.HandlerFunc(func(_ context.Context, j job.Job) error {
			record(j)
			panic("handler bug")
		}),
	}

	runCtx, stop := context.WithCancel(ctx)
	var pools sync.WaitGroup
	for range 2 {
		p, err := worker.New(pgStore{store}, handlers, slog.New(slog.NewTextHandler(io.Discard, nil)), worker.Options{
			Concurrency: 8, Lease: 5 * time.Second, PollInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		pools.Add(1)
		go func() {
			defer pools.Done()
			_ = p.Run(runCtx)
		}()
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		var open int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM jobs WHERE state IN ('pending', 'running')`).Scan(&open); err != nil {
			t.Fatal(err)
		}
		if open == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d jobs still open after 30s", open)
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	pools.Wait()

	for id, w := range want {
		j, err := store.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if j.State != w.state || j.Attempt != w.attempt {
			t.Errorf("job %d (%s): %s at attempt %d, want %s at attempt %d (last error %q)",
				id, j.Type, j.State, j.Attempt, w.state, w.attempt, j.LastError)
		}
	}
	for key, n := range runs {
		if n != 1 {
			t.Errorf("job %d attempt %d ran %d times", key[0], key[1], n)
		}
	}
}
