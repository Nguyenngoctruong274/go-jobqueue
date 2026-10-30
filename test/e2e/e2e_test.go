// Package e2e runs the whole service in one process against a real
// PostgreSQL: the HTTP API, the worker pool, the reaper and the LISTEN
// subscriber, exactly as jobqueued wires them, and drives it the way a
// producer and an operator would. It needs JQ_TEST_DATABASE_URL and skips
// otherwise.
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/postgres"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/reaper"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/testdb"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/worker"
)

// pollInterval is deliberately long: a job that runs sooner than this
// after being enqueued proves the LISTEN/NOTIFY wake-up works.
const pollInterval = 5 * time.Second

type poolStore struct{ *postgres.Store }

func (s poolStore) Claim(ctx context.Context, types []string, limit int, lease time.Duration) ([]job.Job, error) {
	return s.Store.Claim(ctx, postgres.ClaimRequest{Types: types, Limit: limit, Lease: lease})
}

type apiStore struct{ *postgres.Store }

func (s apiStore) List(ctx context.Context, f httpapi.ListFilter) ([]job.Job, error) {
	return s.Store.List(ctx, postgres.ListRequest{State: f.State, Type: f.Type, Limit: f.Limit, Before: f.Before})
}

// stack is one running copy of the service.
type stack struct {
	t     *testing.T
	db    *sql.DB
	store *postgres.Store
	api   *httptest.Server

	mu   sync.Mutex
	runs map[string][]int // payload id -> attempts seen by handlers
	fail map[string]int   // payload id -> attempts that must fail
}

func newStack(t *testing.T) *stack {
	t.Helper()
	db, url := testdb.NewURL(t, postgres.Migrate)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &stack{t: t, db: db, runs: map[string][]int{}, fail: map[string]int{}}
	// A short, exact backoff keeps retries fast and their timing checkable.
	s.store = postgres.NewStore(db, postgres.WithBackoff(job.Backoff{Base: 50 * time.Millisecond, Max: 200 * time.Millisecond}))

	ctx, cancel := context.WithCancel(context.Background())
	var bg sync.WaitGroup

	l, err := postgres.Listen(ctx, url, logger)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := worker.New(poolStore{s.store}, map[string]worker.Handler{
		"echo":  worker.HandlerFunc(s.echo),
		"flaky": worker.HandlerFunc(s.flaky),
	}, logger, worker.Options{Concurrency: 8, Lease: 2 * time.Second, PollInterval: pollInterval, DrainTimeout: 2 * time.Second, Wake: l.C()})
	if err != nil {
		t.Fatal(err)
	}
	bg.Add(2)
	go func() { defer bg.Done(); _ = pool.Run(ctx) }()
	go func() {
		defer bg.Done()
		_ = reaper.New(s.store, logger, reaper.Options{Interval: 200 * time.Millisecond}).Run(ctx)
	}()

	s.api = httptest.NewServer(httpapi.NewHandler(logger, httpapi.Options{Ready: db.PingContext, Jobs: apiStore{s.store}}))
	t.Cleanup(func() {
		s.api.Close()
		cancel()
		bg.Wait()
		_ = l.Close()
	})
	return s
}

type payload struct {
	ID string `json:"id"`
}

func (s *stack) record(j job.Job) (string, int) {
	var p payload
	_ = json.Unmarshal(j.Payload, &p)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[p.ID] = append(s.runs[p.ID], j.Attempt)
	return p.ID, s.fail[p.ID]
}

func (s *stack) echo(_ context.Context, j job.Job) error {
	s.record(j)
	return nil
}

// flaky fails the first fail[id] attempts of a job, then succeeds. An id
// starting with "permanent" fails for good at once.
func (s *stack) flaky(_ context.Context, j job.Job) error {
	id, failures := s.record(j)
	if len(id) >= 9 && id[:9] == "permanent" {
		return job.Permanent(errors.New("bad payload, do not retry"))
	}
	if j.Attempt <= failures {
		return fmt.Errorf("attempt %d of %s failed on purpose", j.Attempt, id)
	}
	return nil
}

func (s *stack) attempts(id string) []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.runs[id]...)
}

type jobJSON struct {
	ID        int64     `json:"id"`
	State     job.State `json:"state"`
	Attempt   int       `json:"attempt"`
	LastError string    `json:"last_error"`
}

func (s *stack) call(method, path string, body any) (int, []byte) {
	s.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, s.api.URL+path, &buf)
	if err != nil {
		s.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.api.Client().Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func (s *stack) enqueue(jobType, id string, extra map[string]any) jobJSON {
	s.t.Helper()
	body := map[string]any{"type": jobType, "payload": payload{ID: id}}
	for k, v := range extra {
		body[k] = v
	}
	status, raw := s.call("POST", "/v1/jobs", body)
	if status != http.StatusCreated && status != http.StatusOK {
		s.t.Fatalf("POST /v1/jobs: %d %s", status, raw)
	}
	var j jobJSON
	if err := json.Unmarshal(raw, &j); err != nil {
		s.t.Fatal(err)
	}
	return j
}

func (s *stack) get(id int64) jobJSON {
	s.t.Helper()
	status, raw := s.call("GET", fmt.Sprintf("/v1/jobs/%d", id), nil)
	if status != http.StatusOK {
		s.t.Fatalf("GET /v1/jobs/%d: %d %s", id, status, raw)
	}
	var j jobJSON
	if err := json.Unmarshal(raw, &j); err != nil {
		s.t.Fatal(err)
	}
	return j
}

// waitState polls the API until the job reaches state, or fails the test.
func (s *stack) waitState(id int64, state job.State, timeout time.Duration) jobJSON {
	s.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		j := s.get(id)
		if j.State == state {
			return j
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("job %d is %s after %v, want %s", id, j.State, timeout, state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestEnqueueRunsWithinMillisecondsThanksToNotify(t *testing.T) {
	s := newStack(t)
	start := time.Now()
	j := s.enqueue("echo", "hello", nil)
	s.waitState(j.ID, job.StateSucceeded, pollInterval)
	took := time.Since(start)
	if took > pollInterval/5 {
		t.Fatalf("job finished %v after enqueue; the poll interval is %v, so the wake-up did not work", took, pollInterval)
	}
	if got := s.attempts("hello"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("handler attempts = %v, want [1]", got)
	}
	t.Logf("enqueue → done in %v", took)
}

func TestRetriesThenDeadLetterThenRequeue(t *testing.T) {
	s := newStack(t)
	s.mu.Lock()
	s.fail["r1"] = 1 // fails once, then succeeds
	s.fail["d1"] = 5 // more failures than attempts: dies
	s.mu.Unlock()

	retry := s.enqueue("flaky", "r1", map[string]any{"max_attempts": 3})
	dead := s.enqueue("flaky", "d1", map[string]any{"max_attempts": 2})
	perm := s.enqueue("flaky", "permanent-1", map[string]any{"max_attempts": 5})

	r := s.waitState(retry.ID, job.StateSucceeded, 3*time.Second)
	if r.Attempt != 2 || len(s.attempts("r1")) != 2 {
		t.Fatalf("retried job: %+v, attempts %v", r, s.attempts("r1"))
	}
	d := s.waitState(dead.ID, job.StateDead, 3*time.Second)
	if d.Attempt != 2 || d.LastError == "" {
		t.Fatalf("dead job: %+v", d)
	}
	p := s.waitState(perm.ID, job.StateDead, 3*time.Second)
	if p.Attempt != 1 || p.LastError != "bad payload, do not retry" {
		t.Fatalf("permanently failed job: %+v", p)
	}

	// The operator sees both in the dead-letter view and requeues one.
	status, raw := s.call("GET", "/v1/jobs?state=dead", nil)
	var list struct{ Jobs []jobJSON }
	if err := json.Unmarshal(raw, &list); status != 200 || err != nil || len(list.Jobs) != 2 {
		t.Fatalf("dead list: %d %s", status, raw)
	}
	s.mu.Lock()
	s.fail["d1"] = 0 // the downstream problem is fixed
	s.mu.Unlock()
	if status, raw := s.call("POST", fmt.Sprintf("/v1/jobs/%d/requeue", dead.ID), nil); status != 200 {
		t.Fatalf("requeue: %d %s", status, raw)
	}
	if got := s.waitState(dead.ID, job.StateSucceeded, 3*time.Second); got.Attempt != 1 {
		t.Fatalf("requeued job: %+v", got)
	}
	if got := s.attempts("d1"); len(got) != 3 {
		t.Fatalf("d1 attempts = %v, want 2 failures then 1 success", got)
	}
}

// TestCrashedWorkerIsReapedAndTheJobRunsAgain plants a job that looks
// like a crashed worker left it: running, lease long expired. The reaper
// must hand it back and this stack's pool must run it.
func TestCrashedWorkerIsReapedAndTheJobRunsAgain(t *testing.T) {
	s := newStack(t)
	j := s.enqueue("echo", "orphan", map[string]any{"run_at": time.Now().Add(time.Hour).Format(time.RFC3339)})
	// Not due, so the pool has not touched it. Make it a crashed attempt.
	mustExec(t, s, `UPDATE jobs SET state = 'running', attempt = 1, lease_until = now() - interval '1 second', run_at = now() WHERE id = $1`, j.ID)

	got := s.waitState(j.ID, job.StateSucceeded, 3*time.Second)
	if got.Attempt != 2 || got.LastError == "" {
		t.Fatalf("after reap: %+v (last_error should record the expired lease)", got)
	}
	if a := s.attempts("orphan"); len(a) != 1 || a[0] != 2 {
		t.Fatalf("handler saw attempts %v, want [2]", a)
	}
}

func TestManyProducersEachJobRunsOnce(t *testing.T) {
	s := newStack(t)
	const producers, perProducer = 8, 25
	var (
		wg      sync.WaitGroup
		created atomic.Int64
	)
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				id := fmt.Sprintf("p%d-%d", p, i)
				// Every job is enqueued twice with the same key, as a
				// producer retrying after a timeout would.
				for k := 0; k < 2; k++ {
					status, raw := s.call("POST", "/v1/jobs", map[string]any{
						"type": "echo", "payload": payload{ID: id}, "idempotency_key": id,
					})
					switch status {
					case http.StatusCreated:
						created.Add(1)
					case http.StatusOK:
					default:
						t.Errorf("%s: %d %s", id, status, raw)
					}
				}
			}
		}(p)
	}
	wg.Wait()
	if created.Load() != producers*perProducer {
		t.Fatalf("%d jobs created, want %d", created.Load(), producers*perProducer)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		var n int
		if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM jobs WHERE state = 'succeeded'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == producers*perProducer {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d jobs succeeded after 20s", n, producers*perProducer)
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.runs) != producers*perProducer {
		t.Fatalf("%d distinct jobs ran, want %d", len(s.runs), producers*perProducer)
	}
	for id, attempts := range s.runs {
		if len(attempts) != 1 {
			t.Fatalf("job %s ran %d times: %v", id, len(attempts), attempts)
		}
	}
}

func mustExec(t *testing.T, s *stack, q string, args ...any) {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}
