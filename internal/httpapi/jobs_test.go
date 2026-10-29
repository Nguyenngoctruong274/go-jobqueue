package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// fakeJobs records what the handlers asked for and answers with canned
// results.
type fakeJobs struct {
	spec    job.Spec
	filter  ListFilter
	id      int64
	j       job.Job
	jobs    []job.Job
	created bool
	err     error
}

func (f *fakeJobs) Enqueue(_ context.Context, spec job.Spec) (job.Job, bool, error) {
	f.spec = spec
	return f.j, f.created, f.err
}

func (f *fakeJobs) Get(_ context.Context, id int64) (job.Job, error) {
	f.id = id
	return f.j, f.err
}

func (f *fakeJobs) List(_ context.Context, lf ListFilter) ([]job.Job, error) {
	f.filter = lf
	return f.jobs, f.err
}

func (f *fakeJobs) Cancel(_ context.Context, id int64) (job.Job, error) {
	f.id = id
	return f.j, f.err
}

func (f *fakeJobs) Requeue(_ context.Context, id int64) (job.Job, error) {
	f.id = id
	return f.j, f.err
}

var t0 = time.Date(2026, 10, 29, 9, 0, 0, 0, time.FixedZone("ICT", 7*3600))

func sample(id int64) job.Job {
	return job.Job{
		ID: id, Type: "email.send", State: job.StatePending, Payload: []byte(`{"to":"a@example.com"}`),
		Priority: 5, MaxAttempts: 3, RunAt: t0, CreatedAt: t0,
	}
}

func serve(t *testing.T, jobs Jobs, method, target, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	NewHandler(discardLogger(), Options{Jobs: jobs}).ServeHTTP(rec, req)
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body %q: %v", rec.Body.String(), err)
	}
	return body.Error.Code
}

func TestEnqueue(t *testing.T) {
	f := &fakeJobs{j: sample(7), created: true}
	rec := serve(t, f, http.MethodPost, "/v1/jobs", "application/json; charset=utf-8", `{
		"type": "email.send", "payload": {"to": "a@example.com"}, "priority": 5,
		"max_attempts": 3, "run_at": "2026-10-29T09:00:00+07:00", "idempotency_key": "order-42"}`)
	if rec.Code != http.StatusCreated || rec.Header().Get("Location") != "/v1/jobs/7" {
		t.Fatalf("status %d, Location %q: %s", rec.Code, rec.Header().Get("Location"), rec.Body)
	}
	want := job.Spec{Type: "email.send", Payload: []byte(`{"to": "a@example.com"}`), Priority: 5,
		MaxAttempts: 3, RunAt: t0, IdempotencyKey: "order-42"}
	if f.spec.Type != want.Type || !bytes.Equal(f.spec.Payload, want.Payload) || f.spec.Priority != want.Priority ||
		f.spec.MaxAttempts != want.MaxAttempts || !f.spec.RunAt.Equal(want.RunAt) || f.spec.IdempotencyKey != want.IdempotencyKey {
		t.Fatalf("spec = %+v, want %+v", f.spec, want)
	}

	var got jobJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != 7 || got.State != job.StatePending || string(got.Payload) != `{"to":"a@example.com"}` ||
		got.RunAt.Location() != time.UTC || !got.RunAt.Equal(t0) || got.LeaseUntil != nil {
		t.Fatalf("response = %+v", got)
	}
	if strings.Contains(rec.Body.String(), "lease_until") || strings.Contains(rec.Body.String(), "last_error") {
		t.Fatalf("empty fields not omitted: %s", rec.Body)
	}
}

func TestEnqueueReturnsTheExistingJob(t *testing.T) {
	f := &fakeJobs{j: sample(7), created: false}
	rec := serve(t, f, http.MethodPost, "/v1/jobs", "application/json", `{"type":"email.send","idempotency_key":"k"}`)
	if rec.Code != http.StatusOK || rec.Header().Get("Location") != "" {
		t.Fatalf("status %d, Location %q", rec.Code, rec.Header().Get("Location"))
	}
}

func TestEnqueueNullPayloadMeansEmpty(t *testing.T) {
	for _, body := range []string{`{"type":"t"}`, `{"type":"t","payload":null}`} {
		f := &fakeJobs{j: sample(1), created: true}
		serve(t, f, http.MethodPost, "/v1/jobs", "application/json", body)
		if f.spec.Payload != nil {
			t.Errorf("%s: payload %q, want none", body, f.spec.Payload)
		}
	}
}

func TestEnqueueRejectsBadRequests(t *testing.T) {
	big := `{"type":"t","payload":"` + strings.Repeat("x", maxEnqueueBody) + `"}`
	tests := []struct {
		name        string
		contentType string
		body        string
		status      int
		code        string
	}{
		{"no content type", "", `{"type":"t"}`, 415, "unsupported_media_type"},
		{"form", "application/x-www-form-urlencoded", `type=t`, 415, "unsupported_media_type"},
		{"not JSON", "application/json", `{`, 400, "bad_request"},
		{"unknown field", "application/json", `{"type":"t","max_attempt":3}`, 400, "bad_request"},
		{"two objects", "application/json", `{"type":"t"}{"type":"u"}`, 400, "bad_request"},
		{"wrong type", "application/json", `{"type":"t","priority":"high"}`, 400, "bad_request"},
		{"bad time", "application/json", `{"type":"t","run_at":"tomorrow"}`, 400, "bad_request"},
		{"too large", "application/json", big, 413, "too_large"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeJobs{}
			rec := serve(t, f, http.MethodPost, "/v1/jobs", tt.contentType, tt.body)
			if rec.Code != tt.status || errorCode(t, rec) != tt.code {
				t.Fatalf("status %d %s, want %d %s: %s", rec.Code, errorCode(t, rec), tt.status, tt.code, rec.Body)
			}
			if f.spec.Type != "" {
				t.Fatal("store reached despite a bad request")
			}
		})
	}
}

// TestStoreErrors checks how each store error is answered, and that
// unexpected errors are not shown to the caller.
func TestStoreErrors(t *testing.T) {
	internal := errors.New("dial tcp 10.0.0.5:5432: connection refused")
	tests := []struct {
		name   string
		method string
		path   string
		err    error
		status int
		code   string
	}{
		{"invalid spec", "POST", "/v1/jobs", fmt.Errorf("enqueue: %w: %w", job.ErrInvalidSpec,
			errors.Join(errors.New("type: bad"), errors.New("priority: bad"))), 422, "invalid_job"},
		{"key conflict", "POST", "/v1/jobs", fmt.Errorf("enqueue t: %w", job.ErrIdempotencyConflict), 409, "idempotency_conflict"},
		{"enqueue fails", "POST", "/v1/jobs", internal, 500, "internal"},
		{"get missing", "GET", "/v1/jobs/9", fmt.Errorf("get job 9: %w", job.ErrNotFound), 404, "not_found"},
		{"get fails", "GET", "/v1/jobs/9", internal, 500, "internal"},
		{"list fails", "GET", "/v1/jobs", internal, 500, "internal"},
		{"cancel running", "POST", "/v1/jobs/9/cancel", fmt.Errorf("job 9: %w", job.ErrInvalidTransition), 409, "invalid_state"},
		{"cancel missing", "POST", "/v1/jobs/9/cancel", fmt.Errorf("cancel job 9: %w", job.ErrNotFound), 404, "not_found"},
		{"requeue pending", "POST", "/v1/jobs/9/requeue", fmt.Errorf("job 9: %w", job.ErrInvalidTransition), 409, "invalid_state"},
		{"requeue fails", "POST", "/v1/jobs/9/requeue", internal, 500, "internal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := serve(t, &fakeJobs{err: tt.err}, tt.method, tt.path, "application/json", `{"type":"t"}`)
			if rec.Code != tt.status || errorCode(t, rec) != tt.code {
				t.Fatalf("status %d %s, want %d %s", rec.Code, errorCode(t, rec), tt.status, tt.code)
			}
			if strings.Contains(rec.Body.String(), "10.0.0.5") {
				t.Fatalf("internal error leaked: %s", rec.Body)
			}
			if strings.Contains(rec.Body.String(), `\n`) {
				t.Fatalf("message spans lines: %s", rec.Body)
			}
		})
	}
}

func TestGetCancelRequeue(t *testing.T) {
	for _, tt := range []struct{ method, path string }{
		{"GET", "/v1/jobs/42"},
		{"POST", "/v1/jobs/42/cancel"},
		{"POST", "/v1/jobs/42/requeue"},
	} {
		f := &fakeJobs{j: sample(42)}
		rec := serve(t, f, tt.method, tt.path, "", "")
		if rec.Code != http.StatusOK || f.id != 42 {
			t.Errorf("%s %s: status %d, store got id %d", tt.method, tt.path, rec.Code, f.id)
		}
	}
	for _, path := range []string{"/v1/jobs/0", "/v1/jobs/-1", "/v1/jobs/abc", "/v1/jobs/99999999999999999999"} {
		f := &fakeJobs{j: sample(1)}
		if rec := serve(t, f, "GET", path, "", ""); rec.Code != http.StatusBadRequest || f.id != 0 {
			t.Errorf("GET %s: status %d, store got id %d", path, rec.Code, f.id)
		}
	}
	if rec := serve(t, &fakeJobs{}, "DELETE", "/v1/jobs/1", "", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("DELETE: status %d, want 405", rec.Code)
	}
}

func TestList(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  ListFilter
	}{
		{"defaults", "", ListFilter{Limit: DefaultListLimit}},
		{"dead letters", "?state=dead", ListFilter{State: job.StateDead, Limit: DefaultListLimit}},
		{"everything", "?state=pending&type=email.send&limit=2&cursor=17",
			ListFilter{State: job.StatePending, Type: "email.send", Limit: 2, Before: 17}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeJobs{}
			rec := serve(t, f, "GET", "/v1/jobs"+tt.query, "", "")
			if rec.Code != http.StatusOK || f.filter != tt.want {
				t.Fatalf("status %d, filter %+v, want %+v", rec.Code, f.filter, tt.want)
			}
			if rec.Body.String() != "{\"jobs\":[]}\n" {
				t.Fatalf("empty page = %s", rec.Body)
			}
		})
	}
}

func TestListCursor(t *testing.T) {
	listed := []job.Job{sample(9), sample(8)}
	listed[0].Payload, listed[1].Payload = nil, nil

	var page listResponse
	rec := serve(t, &fakeJobs{jobs: listed}, "GET", "/v1/jobs?limit=2", "", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 2 || page.NextCursor != "8" || strings.Contains(rec.Body.String(), "payload") {
		t.Fatalf("full page: %s", rec.Body)
	}

	page = listResponse{}
	rec = serve(t, &fakeJobs{jobs: listed}, "GET", "/v1/jobs?limit=3", "", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != 2 || page.NextCursor != "" {
		t.Fatalf("short page has a cursor: %s", rec.Body)
	}
}

func TestListRejectsBadQueries(t *testing.T) {
	for _, q := range []string{
		"?state=lost", "?type=Email", "?limit=0", "?limit=501", "?limit=ten", "?cursor=0", "?cursor=abc",
	} {
		f := &fakeJobs{}
		rec := serve(t, f, "GET", "/v1/jobs"+q, "", "")
		if rec.Code != http.StatusBadRequest || f.filter.Limit != 0 {
			t.Errorf("%s: status %d, store reached %v", q, rec.Code, f.filter.Limit != 0)
		}
	}
}

func TestJobRoutesNeedAStore(t *testing.T) {
	rec := httptest.NewRecorder()
	NewHandler(discardLogger(), Options{}).ServeHTTP(rec, httptest.NewRequest("GET", "/v1/jobs", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404 without a store", rec.Code)
	}
}

func TestLeaseUntilIsShownWhileRunning(t *testing.T) {
	j := sample(3)
	j.State, j.Attempt, j.LeaseUntil = job.StateRunning, 1, t0.Add(time.Minute)
	rec := serve(t, &fakeJobs{j: j}, "GET", "/v1/jobs/3", "", "")
	var got jobJSON
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.LeaseUntil == nil || !got.LeaseUntil.Equal(j.LeaseUntil) || got.LeaseUntil.Location() != time.UTC {
		t.Fatalf("lease_until = %v, want %v in UTC", got.LeaseUntil, j.LeaseUntil)
	}
}
