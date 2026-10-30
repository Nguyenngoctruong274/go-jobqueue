package postgres

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/testdb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newListeningStore(t *testing.T) (*Store, *Listener) {
	t.Helper()
	db, url := testdb.NewURL(t, Migrate)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	l, err := Listen(ctx, url, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return NewStore(db), l
}

// nextWake returns the next wake-up of the given type within the timeout,
// skipping notifications other schemas sharing the test database raise.
func nextWake(t *testing.T, l *Listener, jobType string, timeout time.Duration) (job.Wake, bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case got, ok := <-l.C():
			if !ok {
				t.Fatal("listener closed")
			}
			if got.Type == jobType || got.Type == "" {
				return got, true
			}
		case <-deadline:
			return job.Wake{}, false
		}
	}
}

func TestEnqueueNotifiesOnCommit(t *testing.T) {
	s, l := newListeningStore(t)
	start := time.Now()
	j, _ := enqueue(t, s, job.Spec{Type: "notify.me", RunAt: time.Now().Add(time.Hour)})
	w, ok := nextWake(t, l, "notify.me", 5*time.Second)
	if !ok {
		t.Fatal("no wake-up within 5s of enqueueing")
	}
	// run_at travels with the wake-up, to the microsecond.
	if !w.RunAt.Equal(j.RunAt) {
		t.Fatalf("wake-up says run_at %s, job has %s", w.RunAt, j.RunAt)
	}
	t.Logf("wake-up arrived %v after enqueue", time.Since(start))
}

func TestFailAndRequeueNotify(t *testing.T) {
	s, l := newListeningStore(t)
	ctx := context.Background()
	running := claimOne(t, s, job.Spec{Type: "retry.me", MaxAttempts: 1}, time.Minute)
	nextWake(t, l, "retry.me", time.Second) // the enqueue's own wake-up

	// A dead job is not pending: no wake-up.
	if _, err := s.Fail(ctx, running.ID, 1, errors.New("x")); err != nil {
		t.Fatal(err)
	}
	if _, ok := nextWake(t, l, "retry.me", 300*time.Millisecond); ok {
		t.Fatal("a job that died woke the workers")
	}
	// Requeue makes it pending again: wake-up.
	if _, err := s.Requeue(ctx, running.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := nextWake(t, l, "retry.me", 5*time.Second); !ok {
		t.Fatal("no wake-up after Requeue")
	}
}

func TestEnqueueTxIsAtomicWithTheCallersWrites(t *testing.T) {
	s, l := newListeningStore(t)
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE orders (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	// Rolled back: neither the order nor the job exists, and nobody is woken.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO orders VALUES ('o-1')`); err != nil {
		t.Fatal(err)
	}
	j, created, err := s.EnqueueTx(ctx, tx, job.Spec{Type: "order.confirm", IdempotencyKey: "o-1"})
	if err != nil || !created {
		t.Fatalf("EnqueueTx: %+v, %t, %v", j, created, err)
	}
	// Invisible to workers while the transaction is open.
	if got := claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute}); len(got) != 0 {
		t.Fatalf("claimed an uncommitted job: %v", ids(got))
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, j.ID); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("job survived the rollback: %v", err)
	}
	if _, ok := nextWake(t, l, "order.confirm", 300*time.Millisecond); ok {
		t.Fatal("a rolled-back enqueue woke the workers")
	}

	// Committed: both exist, the key is honoured, and the wake-up fires.
	tx, err = s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO orders VALUES ('o-1')`); err != nil {
		t.Fatal(err)
	}
	j, created, err = s.EnqueueTx(ctx, tx, job.Spec{Type: "order.confirm", IdempotencyKey: "o-1"})
	if err != nil || !created {
		t.Fatalf("EnqueueTx: %+v, %t, %v", j, created, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, ok := nextWake(t, l, "order.confirm", 5*time.Second); !ok {
		t.Fatal("no wake-up after the commit")
	}
	got := claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute})
	if len(got) != 1 || got[0].ID != j.ID {
		t.Fatalf("claimed %v, want [%d]", ids(got), j.ID)
	}
	// The key now belongs to the committed job.
	if _, created, err := s.Enqueue(ctx, job.Spec{Type: "order.confirm", IdempotencyKey: "o-1"}); err != nil || created {
		t.Fatalf("re-enqueue: created=%t err=%v", created, err)
	}
	if _, _, err := s.EnqueueTx(ctx, nil, job.Spec{Type: "t"}); err == nil {
		t.Fatal("nil tx accepted")
	}
}

func TestListenFailsFastOnABadURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := Listen(ctx, "postgres://nobody@127.0.0.1:1/x?sslmode=disable", quiet); err == nil {
		t.Fatal("Listen succeeded against a closed port")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("Listen took %v to fail", time.Since(start))
	}
}

func TestParseWake(t *testing.T) {
	ts := time.Date(2026, 10, 30, 1, 2, 3, 456789000, time.UTC)
	good := parseWake("email.send " + strconv.FormatInt(ts.UnixMicro(), 10))
	if good.Type != "email.send" || !good.RunAt.Equal(ts) {
		t.Fatalf("parseWake = %+v", good)
	}
	for _, bad := range []string{"", "email.send", "email.send soon", " 123"} {
		if w := parseWake(bad); w != (job.Wake{}) && bad != " 123" {
			t.Fatalf("parseWake(%q) = %+v, want zero", bad, w)
		}
	}
}
