package job

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// State is where a job is in its lifecycle.
type State string

// The states of a job. Only pending jobs can be claimed; succeeded and
// cancelled are final; dead jobs stay for inspection until requeued.
const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

// ParseState converts a stored state back to a State.
func ParseState(s string) (State, error) {
	switch st := State(s); st {
	case StatePending, StateRunning, StateSucceeded, StateDead, StateCancelled:
		return st, nil
	}
	return "", fmt.Errorf("unknown job state %q", s)
}

// Final reports whether no transition leaves the state. Dead is not
// final: an operator can requeue a dead job.
func (s State) Final() bool { return s == StateSucceeded || s == StateCancelled }

// Errors returned by the transitions. They are wrapped with the job and
// the attempted transition; match them with errors.Is.
var (
	// ErrInvalidTransition means the job's state does not allow the
	// transition, such as cancelling a running job.
	ErrInvalidTransition = errors.New("invalid job transition")
	// ErrNotDue means a pending job was claimed before its RunAt.
	ErrNotDue = errors.New("job not due yet")
	// ErrLeaseLost means a worker reported on an attempt it no longer
	// owns: the job is not running, or runs under a newer attempt because
	// the lease expired and another worker claimed it.
	ErrLeaseLost = errors.New("job lease lost")
	// ErrLeaseActive means the reaper tried to expire a lease that has
	// not run out yet.
	ErrLeaseActive = errors.New("job lease still active")
)

// MaxErrorBytes bounds LastError, so that a handler returning a huge
// error cannot bloat the jobs table.
const MaxErrorBytes = 2048

// errLeaseExpired is recorded as the failure of an attempt whose worker
// stopped renewing its lease, typically because it crashed.
var errLeaseExpired = errors.New("lease expired before the attempt finished")

// errReleased is recorded for an attempt its worker gave back unfinished.
var errReleased = errors.New("released by its worker before the attempt finished")

// Job is one unit of work and the bookkeeping of its attempts.
//
// The attempt number doubles as a fencing token: Claim returns it, and a
// worker's Heartbeat, Succeed and Fail must present it. After a lease
// expires and the job is claimed again, the old worker's token is stale
// and its report is rejected with ErrLeaseLost instead of overwriting the
// new attempt.
type Job struct {
	ID   int64
	Type string
	// Payload is the handler's input, a JSON document.
	Payload []byte
	State   State
	// Priority orders due jobs: higher runs first.
	Priority int
	// Attempt counts started attempts; it is the token of the current one
	// while the job runs.
	Attempt     int
	MaxAttempts int
	// RunAt is the earliest time a pending job may be claimed.
	RunAt time.Time
	// LeaseUntil is when a running job's lease ends; zero otherwise.
	LeaseUntil time.Time
	// LastError is the failure of the latest failed attempt, if any.
	LastError string
	// IdempotencyKey, if set, makes enqueueing the same job again return
	// this one instead of a duplicate.
	IdempotencyKey string
	CreatedAt      time.Time
}

// Claim starts the next attempt of a due pending job under a lease that
// ends at now+lease. It returns the attempt's fencing token.
func (j *Job) Claim(now time.Time, lease time.Duration) (int, error) {
	if lease <= 0 {
		return 0, fmt.Errorf("job %d: claim: lease must be positive, got %v", j.ID, lease)
	}
	if j.State != StatePending {
		return 0, j.transitionErr("claim", ErrInvalidTransition)
	}
	if now.Before(j.RunAt) {
		return 0, fmt.Errorf("job %d: claim: due at %s: %w", j.ID, j.RunAt.Format(time.RFC3339Nano), ErrNotDue)
	}
	if j.Attempt >= j.MaxAttempts {
		// Fail and Expire never leave a job pending with no attempts
		// left, so this is a corrupted job, not a normal outcome.
		return 0, fmt.Errorf("job %d: claim: %d of %d attempts used: %w", j.ID, j.Attempt, j.MaxAttempts, ErrInvalidTransition)
	}
	j.Attempt++
	j.State = StateRunning
	j.LeaseUntil = now.Add(lease)
	return j.Attempt, nil
}

// Heartbeat extends the lease of a running attempt to now+lease, for
// handlers that run longer than one lease.
func (j *Job) Heartbeat(attempt int, now time.Time, lease time.Duration) error {
	if lease <= 0 {
		return fmt.Errorf("job %d: heartbeat: lease must be positive, got %v", j.ID, lease)
	}
	if err := j.checkOwner("heartbeat", attempt); err != nil {
		return err
	}
	j.LeaseUntil = now.Add(lease)
	return nil
}

// Succeed completes a running attempt.
//
// A lease that ran out is not checked here: until the reaper moves the job
// on, the attempt still owns it, and keeping a finished result beats
// running the job again.
func (j *Job) Succeed(attempt int) error {
	if err := j.checkOwner("succeed", attempt); err != nil {
		return err
	}
	j.State = StateSucceeded
	j.LeaseUntil = time.Time{}
	return nil
}

// Fail records a failed attempt. The job retries after the backoff delay
// if it has attempts left and cause is not Permanent; otherwise it dies.
func (j *Job) Fail(attempt int, now time.Time, cause error, b Backoff, rnd Rand) error {
	if err := j.checkOwner("fail", attempt); err != nil {
		return err
	}
	if cause == nil {
		cause = errors.New("failed without an error")
	}
	j.finishAttempt(now, cause, IsPermanent(cause), b, rnd)
	return nil
}

// Expire ends a running attempt whose lease ran out, as the reaper does
// for workers that crashed or stalled. It counts as a failed attempt.
func (j *Job) Expire(now time.Time, b Backoff, rnd Rand) error {
	if j.State != StateRunning {
		return j.transitionErr("expire", ErrInvalidTransition)
	}
	if now.Before(j.LeaseUntil) {
		return fmt.Errorf("job %d: expire: lease until %s: %w", j.ID, j.LeaseUntil.Format(time.RFC3339Nano), ErrLeaseActive)
	}
	j.finishAttempt(now, errLeaseExpired, false, b, rnd)
	return nil
}

// Release hands a running attempt back without waiting for its lease to
// run out, as a worker does when it shuts down mid-job. The attempt still
// counts, as it would if the worker had crashed, but the job is due again
// at once instead of after a backoff: nothing suggests the job itself is
// at fault. If that was its last attempt, the job dies.
func (j *Job) Release(attempt int, now time.Time) error {
	if err := j.checkOwner("release", attempt); err != nil {
		return err
	}
	j.LastError = ErrorText(errReleased)
	j.LeaseUntil = time.Time{}
	if j.Attempt >= j.MaxAttempts {
		j.State = StateDead
		return nil
	}
	j.State = StatePending
	j.RunAt = now
	return nil
}

// Cancel stops a pending job from running. A running job cannot be
// cancelled: its handler is already in flight.
func (j *Job) Cancel() error {
	if j.State != StatePending {
		return j.transitionErr("cancel", ErrInvalidTransition)
	}
	j.State = StateCancelled
	return nil
}

// Requeue gives a dead job a fresh set of attempts, due now. LastError is
// kept so that the cause of the death stays visible until the next one.
func (j *Job) Requeue(now time.Time) error {
	if j.State != StateDead {
		return j.transitionErr("requeue", ErrInvalidTransition)
	}
	j.State = StatePending
	j.Attempt = 0
	j.RunAt = now
	return nil
}

func (j *Job) finishAttempt(now time.Time, cause error, permanent bool, b Backoff, rnd Rand) {
	j.LastError = ErrorText(cause)
	j.LeaseUntil = time.Time{}
	if permanent || j.Attempt >= j.MaxAttempts {
		j.State = StateDead
		return
	}
	j.State = StatePending
	j.RunAt = now.Add(b.Delay(j.Attempt, rnd))
}

func (j *Job) checkOwner(op string, attempt int) error {
	if j.State != StateRunning || j.Attempt != attempt {
		return fmt.Errorf("job %d: %s: attempt %d, job is %s at attempt %d: %w", j.ID, op, attempt, j.State, j.Attempt, ErrLeaseLost)
	}
	return nil
}

func (j *Job) transitionErr(op string, err error) error {
	return fmt.Errorf("job %d: cannot %s a %s job: %w", j.ID, op, j.State, err)
}

// ErrorText turns an error into text fit for storage: valid UTF-8 with no
// NUL bytes (PostgreSQL text rejects both) and at most MaxErrorBytes long,
// cut on a rune boundary.
func ErrorText(err error) string {
	s := strings.ToValidUTF8(err.Error(), "�")
	s = strings.ReplaceAll(s, "\x00", "�")
	if len(s) <= MaxErrorBytes {
		return s
	}
	cut := MaxErrorBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// Permanent marks a handler error as not worth retrying, such as a
// payload that will never parse. The job dies at once instead of using up
// its attempts. Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, was marked by
// Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }
