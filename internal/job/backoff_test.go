package job

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

// fixedRand returns the same draw, clamped to [0, n), for every call.
type fixedRand int64

func (f fixedRand) Int64N(n int64) int64 { return min(int64(f), n-1) }

// maxRand always draws the largest value, giving the shortest delay.
type maxRand struct{}

func (maxRand) Int64N(n int64) int64 { return n - 1 }

func TestBackoffValidate(t *testing.T) {
	tests := []struct {
		name string
		b    Backoff
		want []string // every substring must appear in the error; none means valid
	}{
		{name: "default", b: DefaultBackoff()},
		{name: "base equals max", b: Backoff{Base: time.Second, Max: time.Second}},
		{name: "full jitter", b: Backoff{Base: time.Second, Max: time.Minute, JitterPercent: 100}},
		{name: "zero base", b: Backoff{Max: time.Second}, want: []string{"base must be positive"}},
		{name: "max below base", b: Backoff{Base: time.Minute, Max: time.Second}, want: []string{"max must be at least the base"}},
		{name: "negative jitter", b: Backoff{Base: time.Second, Max: time.Second, JitterPercent: -1}, want: []string{"jitter"}},
		{
			name: "everything wrong",
			b:    Backoff{Base: -time.Second, Max: -2 * time.Second, JitterPercent: 101},
			want: []string{"base must be positive", "max must be at least", "jitter"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.b.Validate()
			if len(tt.want) == 0 {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate() = nil, want an error")
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not mention %q", err, w)
				}
			}
		})
	}
}

func TestBackoffCeiling(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Minute}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: -3, want: time.Second},
		{attempt: 0, want: time.Second},
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 6, want: 32 * time.Second},
		{attempt: 7, want: time.Minute}, // 64s is capped
		{attempt: 1 << 30, want: time.Minute},
	}
	for _, tt := range tests {
		if got := b.Ceiling(tt.attempt); got != tt.want {
			t.Errorf("Ceiling(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestBackoffCeilingDoesNotOverflow(t *testing.T) {
	b := Backoff{Base: time.Duration(1<<62 + 1), Max: time.Duration(1<<63 - 1)}
	if got := b.Ceiling(2); got != b.Max {
		t.Fatalf("Ceiling(2) = %v, want the cap %v", got, b.Max)
	}
}

func TestBackoffDelay(t *testing.T) {
	tests := []struct {
		name    string
		b       Backoff
		attempt int
		rnd     Rand
		want    time.Duration
	}{
		{name: "no jitter", b: Backoff{Base: time.Second, Max: time.Hour}, attempt: 3, rnd: maxRand{}, want: 4 * time.Second},
		{name: "zero draw is the ceiling", b: DefaultBackoff(), attempt: 3, rnd: fixedRand(0), want: 4 * time.Second},
		{name: "largest draw halves it", b: DefaultBackoff(), attempt: 3, rnd: maxRand{}, want: 2 * time.Second},
		{name: "exact draw", b: DefaultBackoff(), attempt: 1, rnd: fixedRand(250 * time.Millisecond), want: 750 * time.Millisecond},
		{name: "full jitter can reach zero", b: Backoff{Base: time.Second, Max: time.Hour, JitterPercent: 100}, attempt: 1, rnd: maxRand{}, want: 0},
		{name: "capped", b: DefaultBackoff(), attempt: 40, rnd: fixedRand(0), want: time.Hour},
		{name: "span rounds down to zero", b: Backoff{Base: 1, Max: 1, JitterPercent: 50}, attempt: 1, rnd: maxRand{}, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.b.Delay(tt.attempt, tt.rnd); got != tt.want {
				t.Fatalf("Delay(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestBackoffDelaySpreadsRetries(t *testing.T) {
	b := DefaultBackoff()
	rnd := rand.New(rand.NewPCG(1, 2))
	seen := make(map[time.Duration]bool)
	for range 100 {
		seen[b.Delay(5, rnd)] = true
	}
	// 100 draws from a 8s-wide window should almost never collide; a
	// handful of distinct values would mean the jitter is not applied.
	if len(seen) < 90 {
		t.Fatalf("100 delays took only %d distinct values", len(seen))
	}
}

func TestBackoffDelayNilRandUsesSharedSource(t *testing.T) {
	b := DefaultBackoff()
	for range 50 {
		d := b.Delay(2, nil)
		if d < time.Second || d > 2*time.Second {
			t.Fatalf("Delay(2, nil) = %v, want within [1s, 2s]", d)
		}
	}
}

func FuzzBackoffDelay(f *testing.F) {
	f.Add(int64(time.Second), int64(time.Hour), 50, 3, uint64(1))
	f.Add(int64(1), int64(1), 100, 0, uint64(7))
	f.Add(int64(1<<62), int64(1<<63-1), 100, 1<<20, uint64(42))
	f.Fuzz(func(t *testing.T, base, maxDelay int64, jitter, attempt int, seed uint64) {
		b := Backoff{Base: time.Duration(base), Max: time.Duration(maxDelay), JitterPercent: jitter}
		if b.Validate() != nil {
			t.Skip()
		}
		c := b.Ceiling(attempt)
		if c < b.Base || c > b.Max {
			t.Fatalf("Ceiling(%d) = %v, outside [%v, %v]", attempt, c, b.Base, b.Max)
		}
		if next := b.Ceiling(attempt + 1); attempt < 1<<30 && next < c {
			t.Fatalf("Ceiling decreased from %v to %v at attempt %d", c, next, attempt)
		}
		d := b.Delay(attempt, rand.New(rand.NewPCG(seed, seed)))
		floor := c - (c/100*time.Duration(jitter) + c%100*time.Duration(jitter)/100)
		if d < floor || d > c {
			t.Fatalf("Delay(%d) = %v, outside [%v, %v]", attempt, d, floor, c)
		}
	})
}
