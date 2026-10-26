// Package job defines the lifecycle of a background job: the states it
// moves through, the transitions between them, and the retry policy that
// decides when a failed attempt runs again. It does no I/O. The PostgreSQL
// store applies the same rules, and this package is their reference.
package job

import (
	"errors"
	"math"
	"math/rand/v2"
	"time"
)

// Rand is the source of jitter. *rand.Rand from math/rand/v2 satisfies it;
// tests pass a fixed source to get exact delays.
type Rand interface {
	Int64N(n int64) int64
}

// Backoff computes the delay before a failed job runs again. The ceiling
// doubles with every failed attempt, from Base up to Max, and a random
// share of it (JitterPercent) is taken off so that jobs failing together,
// for example during a downstream outage, do not all retry at once.
type Backoff struct {
	// Base is the ceiling after the first failed attempt.
	Base time.Duration
	// Max caps the ceiling however many attempts have failed.
	Max time.Duration
	// JitterPercent is how much of the ceiling may be randomized away, from
	// 0 (no jitter, exact exponential delays) to 100 ("full jitter": any
	// delay between 0 and the ceiling).
	JitterPercent int
}

// DefaultBackoff starts at 1s and caps at 1h. Half of each delay is
// random: retries spread out, yet never come back sooner than half the
// ceiling, so a failing dependency still gets breathing room.
func DefaultBackoff() Backoff {
	return Backoff{Base: time.Second, Max: time.Hour, JitterPercent: 50}
}

// Validate reports every invalid field at once.
func (b Backoff) Validate() error {
	var errs []error
	if b.Base <= 0 {
		errs = append(errs, errors.New("backoff base must be positive"))
	}
	if b.Max < b.Base {
		errs = append(errs, errors.New("backoff max must be at least the base"))
	}
	if b.JitterPercent < 0 || b.JitterPercent > 100 {
		errs = append(errs, errors.New("backoff jitter must be between 0 and 100 percent"))
	}
	return errors.Join(errs...)
}

// Ceiling is the delay without jitter after the given number of failed
// attempts: Base * 2^(attempt-1), capped at Max. Attempts below 1 count
// as 1. It never overflows, however large attempt is.
func (b Backoff) Ceiling(attempt int) time.Duration {
	d := b.Base
	for i := 1; i < attempt && d < b.Max; i++ {
		if d > b.Max/2 {
			return b.Max
		}
		d *= 2
	}
	return min(d, b.Max)
}

// Delay is the jittered delay after the given number of failed attempts,
// in [Ceiling*(100-JitterPercent)/100, Ceiling]. A nil rnd uses the
// shared math/rand/v2 source.
func (b Backoff) Delay(attempt int, rnd Rand) time.Duration {
	c := int64(b.Ceiling(attempt))
	// c/100*p + c%100*p/100 is c*p/100 without overflowing int64.
	span := c/100*int64(b.JitterPercent) + c%100*int64(b.JitterPercent)/100
	if span <= 0 {
		return time.Duration(c)
	}
	// span+1 below must not overflow; shortening a ~292-year span by 1ns
	// changes nothing.
	span = min(span, math.MaxInt64-1)
	if rnd == nil {
		rnd = sharedRand{}
	}
	return time.Duration(c - rnd.Int64N(span+1))
}

type sharedRand struct{}

// Int64N uses math/rand: jitter only spreads load and needs no
// unpredictability.
func (sharedRand) Int64N(n int64) int64 { return rand.Int64N(n) } //nolint:gosec // not security sensitive
