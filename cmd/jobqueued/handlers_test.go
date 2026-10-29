package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

func TestDemoHandlersHaveValidTypes(t *testing.T) {
	for typ := range demoHandlers() {
		if err := job.ValidateType(typ); err != nil {
			t.Errorf("%q: %v", typ, err)
		}
	}
}

func TestDemoSleep(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		permanent bool // want a permanent error; otherwise nil
	}{
		{"sleeps then succeeds", `{"ms": 5}`, false},
		{"zero", `{}`, false},
		{"not JSON", `nope`, true},
		{"negative", `{"ms": -1}`, true},
		{"too long", `{"ms": 600001}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := demoSleep(context.Background(), job.Job{Payload: []byte(tt.payload)})
			if tt.permanent != job.IsPermanent(err) || (!tt.permanent && err != nil) {
				t.Fatalf("error = %v, want permanent %v", err, tt.permanent)
			}
		})
	}
}

func TestDemoSleepStopsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := demoSleep(ctx, job.Job{Payload: []byte(`{"ms": 60000}`)})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
		t.Fatalf("error = %v after %v, want a prompt context error", err, time.Since(start))
	}
}

func TestDemoFail(t *testing.T) {
	j := job.Job{Attempt: 2, MaxAttempts: 5, Payload: []byte(`{}`)}
	if err := demoFail(context.Background(), j); err == nil || job.IsPermanent(err) {
		t.Fatalf("error = %v, want a retryable failure", err)
	}
	j.Payload = []byte(`{"permanent": true}`)
	if err := demoFail(context.Background(), j); !job.IsPermanent(err) {
		t.Fatalf("error = %v, want a permanent failure", err)
	}
	j.Payload = []byte(`[`)
	if err := demoFail(context.Background(), j); !job.IsPermanent(err) {
		t.Fatalf("bad payload: error = %v, want a permanent failure", err)
	}
}
