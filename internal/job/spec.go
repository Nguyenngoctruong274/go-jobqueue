package job

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Limits on what a producer may enqueue. The schema enforces the same
// bounds; checking them here first turns a constraint violation into an
// error that names the field.
const (
	MinPriority        = -100
	MaxPriority        = 100
	DefaultMaxAttempts = 5
	MaxMaxAttempts     = 100
	MaxPayloadBytes    = 1 << 20
	MaxKeyBytes        = 200
)

// Errors of the job store, wrapped with context; match them with errors.Is.
var (
	// ErrInvalidSpec means a Spec failed validation.
	ErrInvalidSpec = errors.New("invalid job")
	// ErrNotFound means no job has the requested id.
	ErrNotFound = errors.New("job not found")
	// ErrIdempotencyConflict means a job with the same type and key exists
	// but was enqueued with a different payload, priority or attempt limit:
	// the key was reused for different work.
	ErrIdempotencyConflict = errors.New("idempotency key reused with different parameters")
)

var typePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// Spec is what a producer asks to run.
type Spec struct {
	// Type selects the handler, e.g. "email.send": lowercase letters,
	// digits, '.', '_' and '-', at most 128 bytes.
	Type string
	// Payload is the handler's input as JSON; empty means {}. PostgreSQL
	// stores it as jsonb, so whitespace and key order are not preserved.
	Payload []byte
	// Priority in [MinPriority, MaxPriority]; higher runs first.
	Priority int
	// MaxAttempts in [1, MaxMaxAttempts]; 0 means DefaultMaxAttempts.
	MaxAttempts int
	// RunAt delays the job until then; zero means now.
	RunAt time.Time
	// IdempotencyKey, if set, deduplicates enqueues of the same Type: the
	// second and later return the first job instead of creating another.
	IdempotencyKey string
}

// Normalize validates the spec and fills in defaults. It reports every
// invalid field at once, each wrapped in ErrInvalidSpec.
func (s Spec) Normalize() (Spec, error) {
	var errs []error
	if !typePattern.MatchString(s.Type) {
		errs = append(errs, errors.New("type: must be 1-128 of [a-z0-9._-], starting with a letter or digit"))
	}
	if len(s.Payload) == 0 {
		s.Payload = []byte("{}")
	}
	switch {
	case len(s.Payload) > MaxPayloadBytes:
		errs = append(errs, fmt.Errorf("payload: larger than %d bytes", MaxPayloadBytes))
	case !json.Valid(s.Payload):
		// jsonb is stricter still (no \u0000, no lone surrogates); the
		// store reports those as ErrInvalidSpec too.
		errs = append(errs, errors.New("payload: not valid JSON"))
	}
	if s.Priority < MinPriority || s.Priority > MaxPriority {
		errs = append(errs, fmt.Errorf("priority: must be between %d and %d", MinPriority, MaxPriority))
	}
	if s.MaxAttempts == 0 {
		s.MaxAttempts = DefaultMaxAttempts
	}
	if s.MaxAttempts < 1 || s.MaxAttempts > MaxMaxAttempts {
		errs = append(errs, fmt.Errorf("max attempts: must be between 1 and %d", MaxMaxAttempts))
	}
	if k := s.IdempotencyKey; k != "" && (len(k) > MaxKeyBytes || !utf8.ValidString(k) || strings.ContainsRune(k, 0)) {
		errs = append(errs, fmt.Errorf("idempotency key: must be valid UTF-8 without NUL, at most %d bytes", MaxKeyBytes))
	}
	if len(errs) > 0 {
		return Spec{}, fmt.Errorf("%w: %w", ErrInvalidSpec, errors.Join(errs...))
	}
	return s, nil
}
