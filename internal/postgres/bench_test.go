package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/testdb"
)

// Benchmarks run against the database in JQ_TEST_DATABASE_URL and are
// skipped without it:
//
//	JQ_TEST_DATABASE_URL=... go test -run '^$' -bench . -benchmem ./internal/postgres

func benchStore(b *testing.B) *Store {
	b.Helper()
	return NewStore(testdb.New(b, Migrate))
}

// seed inserts n pending, due jobs of type t in one statement.
func seed(b *testing.B, s *Store, n int, t string) {
	b.Helper()
	if _, err := s.db.ExecContext(context.Background(), `
		INSERT INTO jobs (type, payload, max_attempts, run_at)
		SELECT $1, jsonb_build_object('i', g), 3, now() - interval '1 second'
		FROM generate_series(1, $2) AS g`, t, n); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkEnqueue(b *testing.B) {
	s := benchStore(b)
	ctx := context.Background()
	payload := []byte(`{"to":"a@example.com","template":"welcome"}`)

	b.Run("plain", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := s.Enqueue(ctx, job.Spec{Type: "email.send", Payload: payload}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("idempotency_key", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, err := s.Enqueue(ctx, job.Spec{Type: "email.send", Payload: payload, IdempotencyKey: fmt.Sprintf("k-%d", i)}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("duplicate_key", func(b *testing.B) {
		if _, _, err := s.Enqueue(ctx, job.Spec{Type: "email.send", Payload: payload, IdempotencyKey: "dup"}); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, created, err := s.Enqueue(ctx, job.Spec{Type: "email.send", Payload: payload, IdempotencyKey: "dup"}); err != nil || created {
				b.Fatalf("created=%t err=%v", created, err)
			}
		}
	})
}

// BenchmarkClaim measures one claim of a batch against a deep queue: the
// cost that bounds how fast the whole system can hand out work.
func BenchmarkClaim(b *testing.B) {
	ctx := context.Background()
	for _, batch := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("batch=%d/pending=50k", batch), func(b *testing.B) {
			s := benchStore(b)
			seed(b, s, 50_000, "t")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				jobs, err := s.Claim(ctx, ClaimRequest{Limit: batch, Lease: time.Minute})
				if err != nil {
					b.Fatal(err)
				}
				if len(jobs) < batch {
					b.StopTimer()
					seed(b, s, 50_000, "t") // refill rather than measure an empty queue
					b.StartTimer()
				}
			}
			b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "jobs/s")
		})
	}
}

// BenchmarkClaimContended has several workers claim from the same queue
// at once, which is how a deployment with many replicas behaves.
func BenchmarkClaimContended(b *testing.B) {
	s := benchStore(b)
	seed(b, s, 200_000, "t")
	ctx := context.Background()
	b.ReportAllocs()
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			jobs, err := s.Claim(ctx, ClaimRequest{Limit: 10, Lease: time.Minute})
			if err != nil {
				b.Error(err)
				return
			}
			if len(jobs) == 0 {
				b.Error("queue ran dry; seed more jobs")
				return
			}
		}
	})
	b.ReportMetric(float64(b.N*10)/b.Elapsed().Seconds(), "jobs/s")
}

// BenchmarkAttempt measures the full life of one attempt as a worker sees
// it: claim, succeed. Fail adds the backoff computation and an index
// update for the new run_at.
func BenchmarkAttempt(b *testing.B) {
	ctx := context.Background()
	b.Run("claim+succeed", func(b *testing.B) {
		s := benchStore(b)
		seed(b, s, b.N+1, "t")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			jobs, err := s.Claim(ctx, ClaimRequest{Limit: 1, Lease: time.Minute})
			if err != nil || len(jobs) != 1 {
				b.Fatalf("claim: %v %v", jobs, err)
			}
			if err := s.Succeed(ctx, jobs[0].ID, jobs[0].Attempt); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "jobs/s")
	})
	b.Run("claim+fail", func(b *testing.B) {
		s := benchStore(b)
		seed(b, s, b.N+1, "t")
		cause := fmt.Errorf("downstream timeout")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			jobs, err := s.Claim(ctx, ClaimRequest{Limit: 1, Lease: time.Minute})
			if err != nil || len(jobs) != 1 {
				b.Fatalf("claim: %v %v", jobs, err)
			}
			if _, err := s.Fail(ctx, jobs[0].ID, jobs[0].Attempt, cause); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("heartbeat", func(b *testing.B) {
		s := benchStore(b)
		seed(b, s, 1, "t")
		jobs, err := s.Claim(ctx, ClaimRequest{Limit: 1, Lease: time.Minute})
		if err != nil || len(jobs) != 1 {
			b.Fatalf("claim: %v %v", jobs, err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := s.Heartbeat(ctx, jobs[0].ID, jobs[0].Attempt, time.Minute); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkQueueMetrics is the cost of one /metrics scrape with an
// uncached snapshot on a large table.
func BenchmarkQueueMetrics(b *testing.B) {
	s := benchStore(b)
	seed(b, s, 100_000, "t")
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := readQueueStats(ctx, s.db); err != nil {
			b.Fatal(err)
		}
	}
}
