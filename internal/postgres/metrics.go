package postgres

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/metrics"
)

// queueStats is one snapshot of the queue, read at scrape time.
type queueStats struct {
	byState map[job.State]float64
	// oldestDue is how long the longest-waiting due job has been due, 0
	// when nothing is due. It rises when workers cannot keep up.
	oldestDue float64
	// dueNow counts pending jobs whose run_at has passed: the backlog
	// workers could take right now, as opposed to delayed jobs.
	dueNow float64
}

// RegisterQueueMetrics exposes the state of the queue as gauges computed
// when /metrics is scraped: jobs per state, the backlog due right now, and
// how long the oldest due job has waited. The snapshot is cached for
// cacheFor so that several scrapers, or a tight scrape interval, cost one
// query; a failed read keeps the last snapshot and logs the error.
func RegisterQueueMetrics(reg *metrics.Registry, db *sql.DB, logger *slog.Logger, cacheFor time.Duration) {
	if logger == nil {
		logger = slog.Default()
	}
	var (
		mu      sync.Mutex
		last    queueStats
		readAt  time.Time
		timeout = 2 * time.Second
	)
	snapshot := func() queueStats {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(readAt) < cacheFor && readAt.After(time.Time{}) {
			return last
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		s, err := readQueueStats(ctx, db)
		if err != nil {
			logger.ErrorContext(ctx, "queue metrics", slog.Any("error", err))
			return last
		}
		last, readAt = s, time.Now()
		return last
	}

	states := []job.State{job.StatePending, job.StateRunning, job.StateSucceeded, job.StateDead, job.StateCancelled}
	reg.NewGaugeVecFunc("jobqueue_jobs", "Jobs in the table by state.", []string{"state"},
		func(set func(float64, ...string)) {
			s := snapshot()
			for _, st := range states {
				set(s.byState[st], string(st))
			}
		})
	reg.NewGaugeFunc("jobqueue_due_jobs",
		"Pending jobs whose run_at has passed: the backlog workers could claim now.",
		func() float64 { return snapshot().dueNow })
	reg.NewGaugeFunc("jobqueue_oldest_due_seconds",
		"How long the longest-waiting due job has been due; 0 when nothing is due.",
		func() float64 { return snapshot().oldestDue })
}

func readQueueStats(ctx context.Context, db *sql.DB) (queueStats, error) {
	s := queueStats{byState: map[job.State]float64{}}
	rows, err := db.QueryContext(ctx, `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return s, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			st string
			n  float64
		)
		if err := rows.Scan(&st, &n); err != nil {
			return s, err
		}
		s.byState[job.State(st)] = n
	}
	if err := rows.Err(); err != nil {
		return s, err
	}
	// Both come from the claim index: pending rows, oldest run_at first.
	err = db.QueryRowContext(ctx, `
		SELECT count(*), COALESCE(extract(epoch FROM now() - min(run_at)), 0)
		FROM jobs WHERE state = 'pending' AND run_at <= now()`).Scan(&s.dueNow, &s.oldestDue)
	return s, err
}

// RegisterPoolMetrics exposes the connection pool: a saturated pool shows
// up as waits before it shows up as latency.
func RegisterPoolMetrics(reg *metrics.Registry, db *sql.DB) {
	reg.NewGaugeFunc("db_pool_open_connections", "Open connections, in use or idle.",
		func() float64 { return float64(db.Stats().OpenConnections) })
	reg.NewGaugeFunc("db_pool_in_use_connections", "Connections currently handed out.",
		func() float64 { return float64(db.Stats().InUse) })
	reg.NewGaugeFunc("db_pool_max_open_connections", "Configured connection limit.",
		func() float64 { return float64(db.Stats().MaxOpenConnections) })
	reg.NewCounterFunc("db_pool_wait_total", "Times a caller had to wait for a connection.",
		func() float64 { return float64(db.Stats().WaitCount) })
	reg.NewCounterFunc("db_pool_wait_seconds_total", "Total time callers waited for a connection.",
		func() float64 { return db.Stats().WaitDuration.Seconds() })
}
