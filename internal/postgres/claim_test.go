package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

func claimN(t *testing.T, s *Store, req ClaimRequest) []job.Job {
	t.Helper()
	jobs, err := s.Claim(context.Background(), req)
	if err != nil {
		t.Fatalf("Claim(%+v): %v", req, err)
	}
	return jobs
}

func ids(jobs []job.Job) []int64 {
	out := make([]int64, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}

func equalIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestClaimValidatesRequest(t *testing.T) {
	// No database: the request is refused before any query.
	s := NewStore(nil)
	tests := []struct {
		name string
		req  ClaimRequest
		want string
	}{
		{"zero limit", ClaimRequest{Lease: time.Second}, "limit"},
		{"limit too high", ClaimRequest{Limit: MaxClaimBatch + 1, Lease: time.Second}, "limit"},
		{"zero lease", ClaimRequest{Limit: 1}, "lease"},
		{"sub-microsecond lease", ClaimRequest{Limit: 1, Lease: 999 * time.Nanosecond}, "lease"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.Claim(context.Background(), tt.req)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want one about %s", err, tt.want)
			}
		})
	}
}

func TestClaimEmptyQueue(t *testing.T) {
	s := newStore(t)
	got := claimN(t, s, ClaimRequest{Limit: 10, Lease: time.Minute})
	if got == nil || len(got) != 0 {
		t.Fatalf("got %v, want an empty, non-nil slice", got)
	}
}

func TestClaimTakesDueJobsInOrder(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	low, _ := enqueue(t, s, job.Spec{Type: "t", Priority: -10, RunAt: now.Add(-time.Hour)})
	lateHigh, _ := enqueue(t, s, job.Spec{Type: "t", Priority: 10, RunAt: now.Add(-time.Minute)})
	earlyHigh, _ := enqueue(t, s, job.Spec{Type: "t", Priority: 10, RunAt: now.Add(-time.Hour)})
	mid1, _ := enqueue(t, s, job.Spec{Type: "t", RunAt: now.Add(-time.Hour)})
	mid2, _ := enqueue(t, s, job.Spec{Type: "t", RunAt: now.Add(-time.Hour)})
	future, _ := enqueue(t, s, job.Spec{Type: "t", Priority: 100, RunAt: now.Add(time.Hour)})

	first := claimN(t, s, ClaimRequest{Limit: 3, Lease: time.Minute})
	if want := []int64{earlyHigh.ID, lateHigh.ID, mid1.ID}; !equalIDs(ids(first), want) {
		t.Fatalf("first batch %v, want %v", ids(first), want)
	}
	rest := claimN(t, s, ClaimRequest{Limit: 10, Lease: time.Minute})
	if want := []int64{mid2.ID, low.ID}; !equalIDs(ids(rest), want) {
		t.Fatalf("second batch %v, want %v (job %d is not due)", ids(rest), want, future.ID)
	}
	if more := claimN(t, s, ClaimRequest{Limit: 10, Lease: time.Minute}); len(more) != 0 {
		t.Fatalf("claimed %v after the queue was drained", ids(more))
	}
}

// TestClaimMatchesDomainTransition checks that the SQL makes the same
// change job.Claim does, and that the lease runs from the database clock.
func TestClaimMatchesDomainTransition(t *testing.T) {
	s := newStore(t)
	before, _ := enqueue(t, s, job.Spec{Type: "email.send", Payload: []byte(`{"to":"a@example.com"}`), MaxAttempts: 3})
	const lease = 90 * time.Second

	got := claimN(t, s, ClaimRequest{Limit: 1, Lease: lease})
	if len(got) != 1 {
		t.Fatalf("claimed %d jobs, want 1", len(got))
	}
	claimed := got[0]

	want := before
	attempt, err := want.Claim(claimed.LeaseUntil.Add(-lease), lease)
	if err != nil {
		t.Fatal(err)
	}
	if attempt != 1 || claimed.Attempt != attempt || claimed.State != want.State ||
		!claimed.LeaseUntil.Equal(want.LeaseUntil) || claimed.ID != want.ID ||
		string(claimed.Payload) != string(want.Payload) || !claimed.RunAt.Equal(want.RunAt) {
		t.Fatalf("store claimed %+v, domain would give %+v", claimed, want)
	}

	var exact bool
	if err := s.db.QueryRowContext(context.Background(),
		`SELECT lease_until = updated_at + interval '90 seconds' FROM jobs WHERE id = $1`, claimed.ID,
	).Scan(&exact); err != nil || !exact {
		t.Fatalf("lease does not end exactly 90s after the claim (err %v)", err)
	}
}

func TestClaimSkipsJobsThatAreNotPending(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	running, _ := enqueue(t, s, job.Spec{Type: "t", Priority: 9})
	claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Hour})
	for _, state := range []string{"succeeded", "dead", "cancelled"} {
		j, _ := enqueue(t, s, job.Spec{Type: "t", Priority: 9})
		if _, err := s.db.ExecContext(ctx, `UPDATE jobs SET state = $1 WHERE id = $2`, state, j.ID); err != nil {
			t.Fatal(err)
		}
	}
	pending, _ := enqueue(t, s, job.Spec{Type: "t"})

	got := claimN(t, s, ClaimRequest{Limit: 10, Lease: time.Minute})
	if want := []int64{pending.ID}; !equalIDs(ids(got), want) {
		t.Fatalf("claimed %v, want only %v (running job %d must stay with its worker)", ids(got), want, running.ID)
	}
}

func TestClaimFiltersByType(t *testing.T) {
	s := newStore(t)
	email, _ := enqueue(t, s, job.Spec{Type: "email.send"})
	sms, _ := enqueue(t, s, job.Spec{Type: "sms.send"})
	report, _ := enqueue(t, s, job.Spec{Type: "report.build"})

	got := claimN(t, s, ClaimRequest{Types: []string{"sms.send", "email.send", "unknown"}, Limit: 10, Lease: time.Minute})
	if want := []int64{email.ID, sms.ID}; !equalIDs(ids(got), want) {
		t.Fatalf("claimed %v, want %v", ids(got), want)
	}
	got = claimN(t, s, ClaimRequest{Limit: 10, Lease: time.Minute})
	if want := []int64{report.ID}; !equalIDs(ids(got), want) {
		t.Fatalf("claim without types got %v, want %v", ids(got), want)
	}
}

func TestClaimRespectsContext(t *testing.T) {
	s := newStore(t)
	enqueue(t, s, job.Spec{Type: "t"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Claim(ctx, ClaimRequest{Limit: 1, Lease: time.Minute}); err == nil {
		t.Fatal("Claim with a cancelled context succeeded")
	}
	// Nothing was claimed by the cancelled call.
	if got := claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute}); len(got) != 1 || got[0].Attempt != 1 {
		t.Fatalf("got %+v, want the job at its first attempt", got)
	}
}

func TestClaimOrder(t *testing.T) {
	t0 := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		a, b job.Job
		want int
	}{
		{"higher priority first", job.Job{ID: 2, Priority: 5, RunAt: t0.Add(time.Hour)}, job.Job{ID: 1, Priority: 4, RunAt: t0}, -1},
		{"then earlier run_at", job.Job{ID: 2, RunAt: t0}, job.Job{ID: 1, RunAt: t0.Add(time.Second)}, -1},
		{"then lower id", job.Job{ID: 1, RunAt: t0}, job.Job{ID: 2, RunAt: t0}, -1},
		{"ids far apart", job.Job{ID: 1 << 62, RunAt: t0}, job.Job{ID: -1 << 62, RunAt: t0}, 1},
		{"same job", job.Job{ID: 3, RunAt: t0}, job.Job{ID: 3, RunAt: t0}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := claimOrder(tt.a, tt.b); got != tt.want {
				t.Fatalf("claimOrder = %d, want %d", got, tt.want)
			}
			if got := claimOrder(tt.b, tt.a); got != -tt.want {
				t.Fatalf("reversed claimOrder = %d, want %d", got, -tt.want)
			}
		})
	}
}
