package worker

import (
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/metrics"
)

// Metrics are the pool's Prometheus series. One Metrics may be shared by
// several pools in a process; the series are then summed.
type Metrics struct {
	claims   *metrics.CounterVec
	attempts *metrics.CounterVec
	panics   *metrics.CounterVec
	running  *metrics.GaugeVec
	pickup   *metrics.HistogramVec
	duration *metrics.HistogramVec
}

// pickupBuckets span the wake-up path (milliseconds) to a slow poll or a
// backlog (minutes).
var pickupBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300}

// durationBuckets span quick handlers to ones that run for minutes.
var durationBuckets = []float64{.001, .005, .01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900}

// NewMetrics registers the pool's series on reg.
func NewMetrics(reg *metrics.Registry) *Metrics {
	return &Metrics{
		claims: reg.NewCounterVec("jobqueue_claims_total",
			"Claim calls by result: jobs (took at least one), empty (none due), error.", "result"),
		attempts: reg.NewCounterVec("jobqueue_attempts_total",
			"Finished attempts by job type and result: succeeded, retried, dead, released, lost, report_failed.",
			"type", "result"),
		panics: reg.NewCounterVec("jobqueue_handler_panics_total",
			"Handler panics by job type; each is also a failed attempt.", "type"),
		running: reg.NewGaugeVec("jobqueue_running_jobs",
			"Handlers running right now, by job type.", "type"),
		pickup: reg.NewHistogramVec("jobqueue_pickup_delay_seconds",
			"How long a job was due before a worker claimed it, by type. The queue's latency.",
			pickupBuckets, "type"),
		duration: reg.NewHistogramVec("jobqueue_attempt_duration_seconds",
			"Handler run time per attempt, by job type.", durationBuckets, "type"),
	}
}

func claimResult(err error, n int) string {
	switch {
	case err != nil:
		return "error"
	case n == 0:
		return "empty"
	default:
		return "jobs"
	}
}
