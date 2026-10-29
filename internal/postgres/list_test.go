package postgres

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// sameJob reports whether a and b hold the same values, comparing times
// as instants.
func sameJob(a, b job.Job) bool {
	return a.ID == b.ID && a.Type == b.Type && bytes.Equal(a.Payload, b.Payload) && a.State == b.State &&
		a.Priority == b.Priority && a.Attempt == b.Attempt && a.MaxAttempts == b.MaxAttempts &&
		a.RunAt.Equal(b.RunAt) && a.LeaseUntil.Equal(b.LeaseUntil) && a.LastError == b.LastError &&
		a.IdempotencyKey == b.IdempotencyKey && a.CreatedAt.Equal(b.CreatedAt)
}

func TestList(t *testing.T) {
	s := newExactStore(t)
	ctx := context.Background()

	// Jobs 1-6 alternate between types a and b; 2 and 5 die, 3 is cancelled.
	var all []int64
	for i := range 6 {
		typ := "a"
		if i%2 == 1 {
			typ = "b"
		}
		j, _ := enqueue(t, s, job.Spec{Type: typ, Payload: []byte(`{"big":"payload"}`), MaxAttempts: 1})
		all = append(all, j.ID)
	}
	for _, id := range []int64{all[1], all[4]} {
		running := claimByID(t, s, id)
		if _, err := s.Fail(ctx, id, running.Attempt, errors.New("boom")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Cancel(ctx, all[2]); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		req  ListRequest
		want []int64
	}{
		{"all, newest first", ListRequest{Limit: 10}, []int64{all[5], all[4], all[3], all[2], all[1], all[0]}},
		{"dead", ListRequest{State: job.StateDead, Limit: 10}, []int64{all[4], all[1]}},
		{"dead of type b", ListRequest{State: job.StateDead, Type: "b", Limit: 10}, []int64{all[1]}},
		{"type a", ListRequest{Type: "a", Limit: 10}, []int64{all[4], all[2], all[0]}},
		{"pending", ListRequest{State: job.StatePending, Limit: 10}, []int64{all[5], all[3], all[0]}},
		{"limit", ListRequest{Limit: 2}, []int64{all[5], all[4]}},
		{"before", ListRequest{Limit: 2, Before: all[4]}, []int64{all[3], all[2]}},
		{"before the first", ListRequest{Limit: 2, Before: all[0]}, []int64{}},
		{"no match", ListRequest{Type: "c", Limit: 10}, []int64{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.List(ctx, tt.req)
			if err != nil {
				t.Fatal(err)
			}
			if g := ids(got); !equalIDs(g, tt.want) {
				t.Fatalf("ids = %v, want %v", g, tt.want)
			}
			for _, j := range got {
				stored := get(t, s, j.ID)
				if j.Payload != nil {
					t.Errorf("job %d: payload %q, want none in a listing", j.ID, j.Payload)
				}
				stored.Payload = nil
				if !sameJob(j, stored) {
					t.Errorf("listed %+v, stored %+v", j, stored)
				}
			}
		})
	}
}

// claimByID claims the given job, failing the test if another is claimed.
func claimByID(t *testing.T, s *Store, id int64) job.Job {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(),
		`UPDATE jobs SET priority = 100 WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	got := claimN(t, s, ClaimRequest{Limit: 1, Lease: time.Minute})
	if len(got) != 1 || got[0].ID != id {
		t.Fatalf("claimed %v, want job %d", ids(got), id)
	}
	return got[0]
}

// TestListPagination walks every page and checks that the pages join up
// with no job missing or repeated, even with jobs enqueued mid-walk.
func TestListPagination(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const n = 23
	for range n {
		enqueue(t, s, job.Spec{Type: "t"})
	}

	var seen []int64
	req := ListRequest{State: job.StatePending, Limit: 5}
	for page := 0; ; page++ {
		got, err := s.List(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if page == 1 {
			enqueue(t, s, job.Spec{Type: "t"}) // newer than every cursor: never listed
		}
		seen = append(seen, ids(got)...)
		if len(got) < req.Limit {
			break
		}
		req.Before = got[len(got)-1].ID
	}
	if len(seen) != n {
		t.Fatalf("walked %d jobs, want %d: %v", len(seen), n, seen)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] >= seen[i-1] {
			t.Fatalf("ids not strictly decreasing at %d: %v", i, seen)
		}
	}
}

func TestListRejectsInvalidRequests(t *testing.T) {
	s := newStore(t)
	tests := []struct {
		name string
		req  ListRequest
		want string
	}{
		{"zero limit", ListRequest{}, "limit"},
		{"limit too big", ListRequest{Limit: MaxListLimit + 1}, "limit"},
		{"unknown state", ListRequest{State: "lost", Limit: 1}, "state"},
		{"bad type", ListRequest{Type: "Bad Type", Limit: 1}, "type"},
		{"negative cursor", ListRequest{Limit: 1, Before: -1}, "before"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.List(context.Background(), tt.req)
			if !errors.Is(err, ErrInvalidListRequest) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want ErrInvalidListRequest naming %q", err, tt.want)
			}
		})
	}
}

// TestListPlanUsesStateIndex checks the claim behind listQuery's optional
// filters: with the parameters bound, a state filter becomes an index
// condition on jobs_state_id instead of a filter over the whole table.
func TestListPlanUsesStateIndex(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	// The table is tiny; make the planner show what it would do on a big one.
	if _, err := tx.ExecContext(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(ctx, `EXPLAIN `+listQuery, "dead", "", int64(1)<<40, 50)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if p := plan.String(); !strings.Contains(p, "jobs_state_id") || !strings.Contains(p, "Index Cond") {
		t.Fatalf("plan does not page through jobs_state_id:\n%s", p)
	}
}
