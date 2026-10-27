package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/testdb"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	return NewStore(testdb.New(t, Migrate))
}

func enqueue(t *testing.T, s *Store, spec job.Spec) (job.Job, bool) {
	t.Helper()
	j, created, err := s.Enqueue(context.Background(), spec)
	if err != nil {
		t.Fatalf("Enqueue(%+v): %v", spec, err)
	}
	return j, created
}

func TestEnqueueDefaults(t *testing.T) {
	s := newStore(t)
	before := time.Now().Add(-time.Second)
	j, created := enqueue(t, s, job.Spec{Type: "email.send"})
	if !created || j.ID == 0 {
		t.Fatalf("created %v, id %d", created, j.ID)
	}
	if j.State != job.StatePending || j.Attempt != 0 || j.MaxAttempts != job.DefaultMaxAttempts ||
		j.Priority != 0 || string(j.Payload) != "{}" || j.IdempotencyKey != "" || !j.LeaseUntil.IsZero() {
		t.Fatalf("got %+v", j)
	}
	// Without RunAt the job is due at once, by the database's clock.
	if j.RunAt.Before(before) || j.RunAt.After(time.Now().Add(time.Second)) || !j.RunAt.Equal(j.CreatedAt) {
		t.Fatalf("run_at %v, created_at %v, want both now", j.RunAt, j.CreatedAt)
	}
}

func TestEnqueueStoresEveryField(t *testing.T) {
	s := newStore(t)
	// PostgreSQL keeps microseconds.
	runAt := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	j, _ := enqueue(t, s, job.Spec{
		Type: "report.build", Payload: []byte(`{"month":"2026-10","ids":[1,2]}`),
		Priority: 50, MaxAttempts: 9, RunAt: runAt, IdempotencyKey: "report/2026-10",
	})
	got, err := s.Get(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	// jsonb normalizes whitespace and key order.
	if string(got.Payload) != `{"ids": [1, 2], "month": "2026-10"}` {
		t.Fatalf("payload = %s", got.Payload)
	}
	if got.Type != "report.build" || got.Priority != 50 || got.MaxAttempts != 9 ||
		!got.RunAt.Equal(runAt) || got.IdempotencyKey != "report/2026-10" || got.State != job.StatePending {
		t.Fatalf("got %+v", got)
	}
}

func TestEnqueueWithoutKeyNeverDeduplicates(t *testing.T) {
	s := newStore(t)
	a, _ := enqueue(t, s, job.Spec{Type: "t"})
	b, created := enqueue(t, s, job.Spec{Type: "t"})
	if !created || a.ID == b.ID {
		t.Fatalf("second enqueue: created %v, ids %d and %d", created, a.ID, b.ID)
	}
}

func TestEnqueueIdempotency(t *testing.T) {
	s := newStore(t)
	spec := job.Spec{Type: "email.send", Payload: []byte(`{"to":"a@example.com","n":1}`), Priority: 5, IdempotencyKey: "signup/42"}
	first, created := enqueue(t, s, spec)
	if !created {
		t.Fatal("first enqueue not created")
	}

	t.Run("same spec returns the first job", func(t *testing.T) {
		again := spec
		// Same JSON, different bytes; and a different delay, which a
		// retrying producer computes afresh every time.
		again.Payload = []byte(`{ "n": 1, "to": "a@example.com" }`)
		again.RunAt = time.Now().Add(time.Hour)
		got, created := enqueue(t, s, again)
		if created || got.ID != first.ID || !got.RunAt.Equal(first.RunAt) {
			t.Fatalf("created %v, got %+v, want job %d unchanged", created, got, first.ID)
		}
	})
	t.Run("same key under another type is another job", func(t *testing.T) {
		other := spec
		other.Type = "sms.send"
		got, created := enqueue(t, s, other)
		if !created || got.ID == first.ID {
			t.Fatalf("created %v, id %d", created, got.ID)
		}
	})
	for name, modify := range map[string]func(*job.Spec){
		"different payload":  func(s *job.Spec) { s.Payload = []byte(`{"to":"b@example.com","n":1}`) },
		"different priority": func(s *job.Spec) { s.Priority = 6 },
		"different attempts": func(s *job.Spec) { s.MaxAttempts = 2 },
	} {
		t.Run(name+" is a conflict", func(t *testing.T) {
			other := spec
			modify(&other)
			_, _, err := s.Enqueue(context.Background(), other)
			if !errors.Is(err, job.ErrIdempotencyConflict) {
				t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
			}
		})
	}
	t.Run("the key stays taken after the job finishes", func(t *testing.T) {
		if _, err := s.db.ExecContext(context.Background(),
			`UPDATE jobs SET state = 'succeeded' WHERE id = $1`, first.ID); err != nil {
			t.Fatal(err)
		}
		got, created := enqueue(t, s, spec)
		if created || got.ID != first.ID || got.State != job.StateSucceeded {
			t.Fatalf("created %v, got %+v", created, got)
		}
	})
}

// TestEnqueueIdempotencyUnderConcurrency races many producers on one key.
// Exactly one creates the job; every other one gets that same job back,
// including those whose INSERT ran before the winner committed.
func TestEnqueueIdempotencyUnderConcurrency(t *testing.T) {
	s := newStore(t)
	const producers = 40
	spec := job.Spec{Type: "invoice.issue", Payload: []byte(`{"order":7}`), IdempotencyKey: "order-7"}

	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		ids     = make([]int64, producers)
		created = make([]bool, producers)
		errs    = make([]error, producers)
	)
	for i := range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var j job.Job
			j, created[i], errs[i] = s.Enqueue(context.Background(), spec)
			ids[i] = j.ID
		}()
	}
	close(start)
	wg.Wait()

	winners := 0
	for i := range producers {
		if errs[i] != nil {
			t.Fatalf("producer %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("producer %d got job %d, producer 0 got %d", i, ids[i], ids[0])
		}
		if created[i] {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("%d producers created the job, want 1", winners)
	}
	var n int
	if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM jobs`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("jobs table has %d rows (err %v), want 1", n, err)
	}
}

func TestEnqueueRejectsPayloadsPostgreSQLCannotStore(t *testing.T) {
	s := newStore(t)
	for name, payload := range map[string]string{
		"escaped NUL":    `{"a":"\u0000"}`,
		"lone surrogate": `{"a":"\ud800"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := s.Enqueue(context.Background(), job.Spec{Type: "t", Payload: []byte(payload)})
			if !errors.Is(err, job.ErrInvalidSpec) {
				t.Fatalf("error = %v, want ErrInvalidSpec", err)
			}
		})
	}
}

func TestEnqueueRejectsInvalidSpecWithoutTouchingTheDatabase(t *testing.T) {
	// A closed handle: any query would fail with a different error.
	db := testdb.New(t, Migrate)
	_ = db.Close()
	_, _, err := NewStore(db).Enqueue(context.Background(), job.Spec{Type: "Bad Type"})
	if !errors.Is(err, job.ErrInvalidSpec) {
		t.Fatalf("error = %v, want ErrInvalidSpec", err)
	}
}

func TestGetNotFound(t *testing.T) {
	s := newStore(t)
	if _, err := s.Get(context.Background(), 404); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
}

// TestClaimOrderMatchesIndex checks the order the claim index encodes:
// priority first, then run_at, then id, and only due pending jobs.
func TestClaimOrderMatchesIndex(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	low, _ := enqueue(t, s, job.Spec{Type: "t", Priority: -10, RunAt: now.Add(-time.Hour)})
	lateHigh, _ := enqueue(t, s, job.Spec{Type: "t", Priority: 10, RunAt: now.Add(-time.Minute)})
	earlyHigh, _ := enqueue(t, s, job.Spec{Type: "t", Priority: 10, RunAt: now.Add(-time.Hour)})
	mid, _ := enqueue(t, s, job.Spec{Type: "t"})
	enqueue(t, s, job.Spec{Type: "t", Priority: 100, RunAt: now.Add(time.Hour)}) // not due

	rows, err := s.db.QueryContext(context.Background(), `
		SELECT id FROM jobs WHERE state = 'pending' AND run_at <= now()
		ORDER BY priority DESC, run_at, id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		got = append(got, id)
	}
	want := []int64{earlyHigh.ID, lateHigh.ID, mid.ID, low.ID}
	if len(got) != len(want) {
		t.Fatalf("due jobs %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("due jobs %v, want %v", got, want)
		}
	}
}
