package job

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

var t0 = time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)

const lease = 30 * time.Second

// noJitter makes retry times exact: 1s, 2s, 4s, ... capped at 1m.
var noJitter = Backoff{Base: time.Second, Max: time.Minute}

func pending(maxAttempts int) *Job {
	return &Job{ID: 7, Type: "email.send", State: StatePending, MaxAttempts: maxAttempts, RunAt: t0}
}

func claim(t *testing.T, j *Job, now time.Time) int {
	t.Helper()
	attempt, err := j.Claim(now, lease)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return attempt
}

func TestParseState(t *testing.T) {
	for _, s := range []State{StatePending, StateRunning, StateSucceeded, StateDead, StateCancelled} {
		got, err := ParseState(string(s))
		if err != nil || got != s {
			t.Errorf("ParseState(%q) = %q, %v", s, got, err)
		}
	}
	for _, s := range []string{"", "PENDING", "failed", "queued"} {
		if _, err := ParseState(s); err == nil {
			t.Errorf("ParseState(%q) succeeded, want an error", s)
		}
	}
}

// TestTransitionTable tries every operation from every state and checks
// which are allowed, the resulting state and the error otherwise.
func TestTransitionTable(t *testing.T) {
	ops := []struct {
		name string
		run  func(j *Job) error
	}{
		{"claim", func(j *Job) error { _, err := j.Claim(t0, lease); return err }},
		{"heartbeat", func(j *Job) error { return j.Heartbeat(1, t0, lease) }},
		{"succeed", func(j *Job) error { return j.Succeed(1) }},
		{"fail", func(j *Job) error { return j.Fail(1, t0, errors.New("boom"), noJitter, nil) }},
		{"expire", func(j *Job) error { return j.Expire(t0.Add(lease), noJitter, nil) }},
		{"cancel", func(j *Job) error { return j.Cancel() }},
		{"requeue", func(j *Job) error { return j.Requeue(t0) }},
	}
	// want[state][op] is the state after the op, or the error it returns.
	type outcome struct {
		state State
		err   error
	}
	want := map[State]map[string]outcome{
		StatePending: {
			"claim": {state: StateRunning}, "heartbeat": {err: ErrLeaseLost}, "succeed": {err: ErrLeaseLost},
			"fail": {err: ErrLeaseLost}, "expire": {err: ErrInvalidTransition}, "cancel": {state: StateCancelled},
			"requeue": {err: ErrInvalidTransition},
		},
		StateRunning: {
			"claim": {err: ErrInvalidTransition}, "heartbeat": {state: StateRunning}, "succeed": {state: StateSucceeded},
			"fail": {state: StatePending}, "expire": {state: StatePending}, "cancel": {err: ErrInvalidTransition},
			"requeue": {err: ErrInvalidTransition},
		},
		StateSucceeded: {
			"claim": {err: ErrInvalidTransition}, "heartbeat": {err: ErrLeaseLost}, "succeed": {err: ErrLeaseLost},
			"fail": {err: ErrLeaseLost}, "expire": {err: ErrInvalidTransition}, "cancel": {err: ErrInvalidTransition},
			"requeue": {err: ErrInvalidTransition},
		},
		StateDead: {
			"claim": {err: ErrInvalidTransition}, "heartbeat": {err: ErrLeaseLost}, "succeed": {err: ErrLeaseLost},
			"fail": {err: ErrLeaseLost}, "expire": {err: ErrInvalidTransition}, "cancel": {err: ErrInvalidTransition},
			"requeue": {state: StatePending},
		},
		StateCancelled: {
			"claim": {err: ErrInvalidTransition}, "heartbeat": {err: ErrLeaseLost}, "succeed": {err: ErrLeaseLost},
			"fail": {err: ErrLeaseLost}, "expire": {err: ErrInvalidTransition}, "cancel": {err: ErrInvalidTransition},
			"requeue": {err: ErrInvalidTransition},
		},
	}
	for from, byOp := range want {
		for _, op := range ops {
			w := byOp[op.name]
			t.Run(fmt.Sprintf("%s/%s", from, op.name), func(t *testing.T) {
				// Attempt 1 of 3 so that the running job's token is 1 and a
				// failure still has attempts left.
				j := &Job{ID: 1, State: from, Attempt: 1, MaxAttempts: 3, RunAt: t0, LeaseUntil: t0.Add(lease)}
				if from == StatePending {
					j.Attempt = 0
					j.LeaseUntil = time.Time{}
				}
				before := *j
				err := op.run(j)
				if w.err != nil {
					if !errors.Is(err, w.err) {
						t.Fatalf("error = %v, want %v", err, w.err)
					}
					if !reflect.DeepEqual(*j, before) {
						t.Fatalf("failed %s changed the job: %+v -> %+v", op.name, before, *j)
					}
					return
				}
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
				if j.State != w.state {
					t.Fatalf("state = %s, want %s", j.State, w.state)
				}
			})
		}
	}
}

func TestClaim(t *testing.T) {
	t.Run("starts an attempt under a lease", func(t *testing.T) {
		j := pending(3)
		if got := claim(t, j, t0.Add(time.Second)); got != 1 {
			t.Fatalf("attempt = %d, want 1", got)
		}
		if j.State != StateRunning || j.LeaseUntil != t0.Add(time.Second+lease) {
			t.Fatalf("got %+v", j)
		}
	})
	t.Run("not before RunAt", func(t *testing.T) {
		j := pending(3)
		if _, err := j.Claim(t0.Add(-time.Nanosecond), lease); !errors.Is(err, ErrNotDue) {
			t.Fatalf("error = %v, want ErrNotDue", err)
		}
		if j.State != StatePending || j.Attempt != 0 {
			t.Fatalf("job changed: %+v", j)
		}
	})
	t.Run("lease must be positive", func(t *testing.T) {
		j := pending(3)
		if _, err := j.Claim(t0, 0); err == nil || j.State != StatePending {
			t.Fatalf("Claim with zero lease: err %v, job %+v", err, j)
		}
	})
	t.Run("no attempts left", func(t *testing.T) {
		j := pending(2)
		j.Attempt = 2
		if _, err := j.Claim(t0, lease); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("error = %v, want ErrInvalidTransition", err)
		}
	})
}

func TestHeartbeatExtendsLease(t *testing.T) {
	j := pending(3)
	a := claim(t, j, t0)
	if err := j.Heartbeat(a, t0.Add(20*time.Second), lease); err != nil {
		t.Fatal(err)
	}
	if want := t0.Add(50 * time.Second); !j.LeaseUntil.Equal(want) {
		t.Fatalf("LeaseUntil = %v, want %v", j.LeaseUntil, want)
	}
	if err := j.Heartbeat(a, t0, 0); err == nil {
		t.Fatal("Heartbeat with zero lease succeeded")
	}
}

func TestRetriesBackOffThenDie(t *testing.T) {
	j := pending(4)
	now := t0
	// The delay doubles after each failed attempt: 1s, 2s, 4s.
	for i, wantDelay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
		a := claim(t, j, now)
		if a != i+1 {
			t.Fatalf("attempt = %d, want %d", a, i+1)
		}
		if err := j.Fail(a, now, fmt.Errorf("smtp: try %d", a), noJitter, nil); err != nil {
			t.Fatal(err)
		}
		if j.State != StatePending || !j.RunAt.Equal(now.Add(wantDelay)) || !j.LeaseUntil.IsZero() {
			t.Fatalf("after attempt %d: %+v, want pending until +%v", a, j, wantDelay)
		}
		if j.LastError != fmt.Sprintf("smtp: try %d", a) {
			t.Fatalf("LastError = %q", j.LastError)
		}
		if _, err := j.Claim(j.RunAt.Add(-time.Nanosecond), lease); !errors.Is(err, ErrNotDue) {
			t.Fatalf("claimed before the backoff ended: %v", err)
		}
		now = j.RunAt
	}
	a := claim(t, j, now)
	if err := j.Fail(a, now, errors.New("smtp: last try"), noJitter, nil); err != nil {
		t.Fatal(err)
	}
	if j.State != StateDead || j.Attempt != 4 || j.LastError != "smtp: last try" {
		t.Fatalf("after the last attempt: %+v, want dead", j)
	}
}

func TestPermanentFailureDiesAtOnce(t *testing.T) {
	j := pending(5)
	a := claim(t, j, t0)
	cause := fmt.Errorf("decode payload: %w", Permanent(errors.New("unexpected EOF")))
	if err := j.Fail(a, t0, cause, noJitter, nil); err != nil {
		t.Fatal(err)
	}
	if j.State != StateDead || j.LastError != "decode payload: unexpected EOF" {
		t.Fatalf("got %+v, want dead with the cause", j)
	}
}

func TestFailWithNilCauseStillRecordsSomething(t *testing.T) {
	j := pending(2)
	a := claim(t, j, t0)
	if err := j.Fail(a, t0, nil, noJitter, nil); err != nil {
		t.Fatal(err)
	}
	if j.State != StatePending || j.LastError == "" {
		t.Fatalf("got %+v", j)
	}
}

func TestExpire(t *testing.T) {
	t.Run("not while the lease is active", func(t *testing.T) {
		j := pending(3)
		claim(t, j, t0)
		if err := j.Expire(t0.Add(lease-time.Nanosecond), noJitter, nil); !errors.Is(err, ErrLeaseActive) {
			t.Fatalf("error = %v, want ErrLeaseActive", err)
		}
		if j.State != StateRunning {
			t.Fatalf("state = %s, want running", j.State)
		}
	})
	t.Run("counts as a failed attempt", func(t *testing.T) {
		j := pending(3)
		claim(t, j, t0)
		end := t0.Add(lease)
		if err := j.Expire(end, noJitter, nil); err != nil {
			t.Fatal(err)
		}
		if j.State != StatePending || !j.RunAt.Equal(end.Add(time.Second)) || !strings.Contains(j.LastError, "lease expired") {
			t.Fatalf("got %+v", j)
		}
	})
	t.Run("on the last attempt the job dies", func(t *testing.T) {
		j := pending(1)
		claim(t, j, t0)
		if err := j.Expire(t0.Add(time.Hour), noJitter, nil); err != nil {
			t.Fatal(err)
		}
		if j.State != StateDead {
			t.Fatalf("state = %s, want dead", j.State)
		}
	})
}

// TestStaleWorkerIsFenced is the crash-and-recover story: worker A stalls,
// its lease expires, worker B claims the job, and A wakes up and reports.
// A's attempt token is stale, so it can neither complete nor fail B's run.
func TestStaleWorkerIsFenced(t *testing.T) {
	j := pending(3)
	workerA := claim(t, j, t0)
	if err := j.Expire(t0.Add(lease), noJitter, nil); err != nil {
		t.Fatal(err)
	}
	workerB := claim(t, j, j.RunAt)

	for name, report := range map[string]func() error{
		"heartbeat": func() error { return j.Heartbeat(workerA, t0.Add(time.Minute), lease) },
		"succeed":   func() error { return j.Succeed(workerA) },
		"fail":      func() error { return j.Fail(workerA, t0.Add(time.Minute), errors.New("late"), noJitter, nil) },
	} {
		if err := report(); !errors.Is(err, ErrLeaseLost) {
			t.Errorf("stale %s: error = %v, want ErrLeaseLost", name, err)
		}
	}
	if j.State != StateRunning || j.Attempt != workerB {
		t.Fatalf("stale reports changed the job: %+v", j)
	}
	if err := j.Succeed(workerB); err != nil {
		t.Fatalf("current worker: %v", err)
	}
}

func TestSucceedAfterLeaseRanOutButBeforeReaper(t *testing.T) {
	j := pending(3)
	a := claim(t, j, t0)
	// The lease ran out an hour ago, but nobody expired the attempt.
	if err := j.Succeed(a); err != nil {
		t.Fatal(err)
	}
	if j.State != StateSucceeded || !j.LeaseUntil.IsZero() {
		t.Fatalf("got %+v", j)
	}
}

func TestRequeueGivesFreshAttempts(t *testing.T) {
	j := pending(1)
	a := claim(t, j, t0)
	if err := j.Fail(a, t0, errors.New("downstream 503"), noJitter, nil); err != nil {
		t.Fatal(err)
	}
	later := t0.Add(24 * time.Hour)
	if err := j.Requeue(later); err != nil {
		t.Fatal(err)
	}
	if j.State != StatePending || j.Attempt != 0 || !j.RunAt.Equal(later) || j.LastError != "downstream 503" {
		t.Fatalf("got %+v", j)
	}
	if got := claim(t, j, later); got != 1 {
		t.Fatalf("attempt after requeue = %d, want 1", got)
	}
}

func TestFinal(t *testing.T) {
	for s, want := range map[State]bool{
		StatePending: false, StateRunning: false, StateDead: false,
		StateSucceeded: true, StateCancelled: true,
	} {
		if got := s.Final(); got != want {
			t.Errorf("%s.Final() = %v, want %v", s, got, want)
		}
	}
}

func TestPermanent(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) != nil")
	}
	base := errors.New("bad payload")
	wrapped := fmt.Errorf("handler: %w", Permanent(base))
	if !IsPermanent(wrapped) || !errors.Is(wrapped, base) {
		t.Fatalf("IsPermanent %v, Is(base) %v; want both true", IsPermanent(wrapped), errors.Is(wrapped, base))
	}
	if IsPermanent(base) || IsPermanent(nil) {
		t.Fatal("unmarked errors reported as permanent")
	}
}

func TestErrorText(t *testing.T) {
	long := strings.Repeat("é", MaxErrorBytes) // 2 bytes per rune
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "timeout", want: "timeout"},
		{name: "invalid utf8", in: "bad \xff byte", want: "bad � byte"},
		{name: "nul byte", in: "a\x00b", want: "a�b"},
		{name: "exact limit", in: strings.Repeat("x", MaxErrorBytes), want: strings.Repeat("x", MaxErrorBytes)},
		{name: "cut on a rune boundary", in: "x" + long, want: "x" + long[:MaxErrorBytes-2]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ErrorText(errors.New(tt.in)); got != tt.want {
				t.Fatalf("ErrorText = %q (%d bytes), want %q (%d bytes)", got, len(got), tt.want, len(tt.want))
			}
		})
	}
}

func FuzzErrorText(f *testing.F) {
	f.Add("timeout")
	f.Add("a\x00b\xff")
	f.Add(strings.Repeat("日本", 1000))
	f.Fuzz(func(t *testing.T, msg string) {
		got := ErrorText(errors.New(msg))
		if len(got) > MaxErrorBytes || !utf8.ValidString(got) || strings.ContainsRune(got, 0) {
			t.Fatalf("ErrorText(%q) = %q: too long, invalid UTF-8 or has NUL", msg, got)
		}
		clean := strings.ReplaceAll(strings.ToValidUTF8(msg, "�"), "\x00", "�")
		if !strings.HasPrefix(clean, got) {
			t.Fatalf("ErrorText(%q) = %q, not a prefix of the cleaned message", msg, got)
		}
	})
}

// FuzzLifecycle drives a job through arbitrary sequences of operations,
// including stale and early ones, and checks the invariants after each.
func FuzzLifecycle(f *testing.F) {
	f.Add([]byte{0, 3, 0, 3, 0, 3}, uint8(3))
	f.Add([]byte{0, 4, 0, 2, 6, 0, 2}, uint8(1))
	f.Add([]byte{5, 6, 0, 1, 2}, uint8(2))
	f.Fuzz(func(t *testing.T, ops []byte, maxAttempts uint8) {
		j := &Job{ID: 1, State: StatePending, MaxAttempts: int(maxAttempts%10) + 1, RunAt: t0}
		now := t0
		lastToken := 0
		for _, op := range ops {
			now = now.Add(time.Duration(op>>3) * time.Second)
			// Odd high bits make the worker report with a stale token.
			token := lastToken
			if op&0x80 != 0 {
				token--
			}
			before := *j
			var err error
			switch op % 7 {
			case 0:
				var a int
				if a, err = j.Claim(now, lease); err == nil {
					lastToken = a
				}
			case 1:
				err = j.Heartbeat(token, now, lease)
			case 2:
				err = j.Succeed(token)
			case 3:
				err = j.Fail(token, now, errors.New("boom"), DefaultBackoff(), fixedRand(op))
			case 4:
				err = j.Expire(now, DefaultBackoff(), fixedRand(op))
			case 5:
				err = j.Cancel()
			case 6:
				err = j.Requeue(now)
			}
			if err != nil && !reflect.DeepEqual(*j, before) {
				t.Fatalf("op %d failed with %v but changed the job: %+v -> %+v", op, err, before, *j)
			}
			if before.State.Final() && !reflect.DeepEqual(*j, before) {
				t.Fatalf("op %d left the final state %s: %+v", op, before.State, *j)
			}
			checkInvariants(t, j)
		}
	})
}

func checkInvariants(t *testing.T, j *Job) {
	t.Helper()
	if _, err := ParseState(string(j.State)); err != nil {
		t.Fatalf("invalid state: %+v", j)
	}
	if j.Attempt < 0 || j.Attempt > j.MaxAttempts {
		t.Fatalf("attempt %d outside [0, %d]: %+v", j.Attempt, j.MaxAttempts, j)
	}
	if (j.State == StateRunning) == j.LeaseUntil.IsZero() {
		t.Fatalf("a lease must be set exactly while running: %+v", j)
	}
	if j.State == StatePending && j.Attempt >= j.MaxAttempts {
		t.Fatalf("pending with no attempts left: %+v", j)
	}
	if j.State == StateRunning && j.Attempt == 0 {
		t.Fatalf("running without an attempt: %+v", j)
	}
}
