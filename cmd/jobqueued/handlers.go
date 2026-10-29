package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/worker"
)

// maxDemoSleep keeps a demo.sleep job from holding a worker slot for long.
const maxDemoSleep = 10 * time.Minute

// demoHandlers are the handlers jobqueued runs out of the box. A real
// deployment links its own handlers against internal/worker; these exist
// to try the queue end to end and to load-test it.
//
//   - demo.sleep {"ms": 250}: waits, then succeeds. Honours cancellation.
//   - demo.fail {"permanent": false}: always fails, so the job walks its
//     retries into the dead-letter queue; permanent skips the retries.
func demoHandlers() map[string]worker.Handler {
	return map[string]worker.Handler{
		"demo.sleep": worker.HandlerFunc(demoSleep),
		"demo.fail":  worker.HandlerFunc(demoFail),
	}
}

func demoSleep(ctx context.Context, j job.Job) error {
	var in struct {
		MS int64 `json:"ms"`
	}
	if err := json.Unmarshal(j.Payload, &in); err != nil {
		return job.Permanent(fmt.Errorf("demo.sleep: decode payload: %w", err))
	}
	d := time.Duration(in.MS) * time.Millisecond
	if in.MS < 0 || d > maxDemoSleep {
		return job.Permanent(fmt.Errorf("demo.sleep: ms must be between 0 and %d", maxDemoSleep.Milliseconds()))
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func demoFail(_ context.Context, j job.Job) error {
	var in struct {
		Permanent bool `json:"permanent"`
	}
	if err := json.Unmarshal(j.Payload, &in); err != nil {
		return job.Permanent(fmt.Errorf("demo.fail: decode payload: %w", err))
	}
	err := fmt.Errorf("demo.fail: failing attempt %d of %d on purpose", j.Attempt, j.MaxAttempts)
	if in.Permanent {
		return job.Permanent(err)
	}
	return err
}
