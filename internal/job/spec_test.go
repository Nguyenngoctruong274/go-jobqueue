package job

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSpecNormalizeDefaults(t *testing.T) {
	got, err := Spec{Type: "email.send"}.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != "{}" || got.MaxAttempts != DefaultMaxAttempts || got.Priority != 0 || !got.RunAt.IsZero() {
		t.Fatalf("got %+v", got)
	}
}

func TestSpecNormalizeKeepsValidFields(t *testing.T) {
	in := Spec{
		Type: "report.build-v2", Payload: []byte(`{"id": 42}`), Priority: -100,
		MaxAttempts: 100, RunAt: t0, IdempotencyKey: "order-42/confirmation",
	}
	got, err := in.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != in.Type || string(got.Payload) != string(in.Payload) || got.Priority != in.Priority ||
		got.MaxAttempts != in.MaxAttempts || !got.RunAt.Equal(in.RunAt) || got.IdempotencyKey != in.IdempotencyKey {
		t.Fatalf("Normalize changed a valid spec: %+v -> %+v", in, got)
	}
}

func TestSpecNormalizeRejects(t *testing.T) {
	valid := Spec{Type: "t"}
	tests := []struct {
		name   string
		modify func(*Spec)
		want   []string
	}{
		{"empty type", func(s *Spec) { s.Type = "" }, []string{"type:"}},
		{"uppercase type", func(s *Spec) { s.Type = "Email" }, []string{"type:"}},
		{"type starting with a dot", func(s *Spec) { s.Type = ".x" }, []string{"type:"}},
		{"type too long", func(s *Spec) { s.Type = strings.Repeat("a", 129) }, []string{"type:"}},
		{"payload not JSON", func(s *Spec) { s.Payload = []byte(`{"a":`) }, []string{"payload: not valid JSON"}},
		{"payload too large", func(s *Spec) { s.Payload = []byte(`"` + strings.Repeat("x", MaxPayloadBytes) + `"`) }, []string{"payload: larger"}},
		{"priority too high", func(s *Spec) { s.Priority = 101 }, []string{"priority:"}},
		{"priority too low", func(s *Spec) { s.Priority = -101 }, []string{"priority:"}},
		{"negative attempts", func(s *Spec) { s.MaxAttempts = -1 }, []string{"max attempts:"}},
		{"too many attempts", func(s *Spec) { s.MaxAttempts = 101 }, []string{"max attempts:"}},
		{"key too long", func(s *Spec) { s.IdempotencyKey = strings.Repeat("k", 201) }, []string{"idempotency key:"}},
		{"key not UTF-8", func(s *Spec) { s.IdempotencyKey = "\xff" }, []string{"idempotency key:"}},
		{"key with NUL", func(s *Spec) { s.IdempotencyKey = "a\x00" }, []string{"idempotency key:"}},
		{
			name:   "everything at once",
			modify: func(s *Spec) { *s = Spec{Type: "X", Payload: []byte("nope"), Priority: 500, MaxAttempts: -2} },
			want:   []string{"type:", "payload:", "priority:", "max attempts:"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := valid
			tt.modify(&s)
			_, err := s.Normalize()
			if !errors.Is(err, ErrInvalidSpec) {
				t.Fatalf("error = %v, want ErrInvalidSpec", err)
			}
			for _, w := range tt.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not mention %q", err, w)
				}
			}
		})
	}
}

func FuzzSpecNormalize(f *testing.F) {
	f.Add("email.send", []byte(`{"to":"a@b.c"}`), 0, 0, "k1")
	f.Add("X", []byte(`[`), 200, -1, "\xff")
	f.Fuzz(func(t *testing.T, typ string, payload []byte, priority, attempts int, key string) {
		got, err := Spec{Type: typ, Payload: payload, Priority: priority, MaxAttempts: attempts, IdempotencyKey: key, RunAt: time.Time{}}.Normalize()
		if err != nil {
			if !errors.Is(err, ErrInvalidSpec) {
				t.Fatalf("error %v is not ErrInvalidSpec", err)
			}
			return
		}
		// Whatever passes must fit the schema's constraints.
		if !typePattern.MatchString(got.Type) || len(got.Payload) == 0 || len(got.Payload) > MaxPayloadBytes ||
			got.Priority < MinPriority || got.Priority > MaxPriority ||
			got.MaxAttempts < 1 || got.MaxAttempts > MaxMaxAttempts || len(got.IdempotencyKey) > MaxKeyBytes {
			t.Fatalf("Normalize accepted %+v", got)
		}
	})
}
