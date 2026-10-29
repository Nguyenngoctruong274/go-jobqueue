package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/postgres"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/testdb"
)

// pgJobs adapts postgres.Store the way jobqueued does.
type pgJobs struct{ *postgres.Store }

func (s pgJobs) List(ctx context.Context, f httpapi.ListFilter) ([]job.Job, error) {
	return s.Store.List(ctx, postgres.ListRequest{State: f.State, Type: f.Type, Limit: f.Limit, Before: f.Before})
}

// The API's page limit must not exceed what the store accepts, or a
// valid request would fail as an internal error.
func TestListLimitsAgree(t *testing.T) {
	if httpapi.MaxListLimit > postgres.MaxListLimit {
		t.Fatalf("API allows pages of %d, the store %d", httpapi.MaxListLimit, postgres.MaxListLimit)
	}
}

type wireJob struct {
	ID          int64           `json:"id"`
	Type        string          `json:"type"`
	State       string          `json:"state"`
	Payload     json.RawMessage `json:"payload"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"`
	LastError   string          `json:"last_error"`
}

type client struct {
	t   *testing.T
	url string
}

func (c client) do(method, path, body string, wantStatus int, out any) {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, c.url+path, strings.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatal(err)
	}
	if resp.StatusCode != wantStatus {
		c.t.Fatalf("%s %s: status %d, want %d: %s", method, path, resp.StatusCode, wantStatus, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			c.t.Fatalf("%s %s: %v: %s", method, path, err, raw)
		}
	}
}

// TestJobsAPIOnPostgreSQL drives a job through the API against the real
// store: enqueue, a retried enqueue, inspection, death, the dead-letter
// listing, requeue and cancel.
func TestJobsAPIOnPostgreSQL(t *testing.T) {
	store := postgres.NewStore(testdb.New(t, postgres.Migrate))
	srv := httptest.NewServer(httpapi.NewHandler(slog.New(slog.NewTextHandler(io.Discard, nil)),
		httpapi.Options{Jobs: pgJobs{store}}))
	defer srv.Close()
	c := client{t, srv.URL}
	ctx := context.Background()

	const body = `{"type":"email.send","payload":{"to":"a@example.com"},"max_attempts":1,"idempotency_key":"order-42"}`
	var created, again wireJob
	c.do("POST", "/v1/jobs", body, http.StatusCreated, &created)
	c.do("POST", "/v1/jobs", body, http.StatusOK, &again)
	if again.ID != created.ID || created.State != "pending" || string(created.Payload) != `{"to":"a@example.com"}` {
		t.Fatalf("created %+v, retried %+v", created, again)
	}
	c.do("POST", "/v1/jobs", strings.Replace(body, "a@example.com", "b@example.com", 1), http.StatusConflict, nil)
	c.do("POST", "/v1/jobs", `{"type":"email.send","payload":{"bad":"\u0000"}}`, http.StatusUnprocessableEntity, nil)

	var other wireJob
	c.do("POST", "/v1/jobs", `{"type":"report.build","run_at":"2099-01-01T00:00:00Z"}`, http.StatusCreated, &other)
	if !other.RunAt.Equal(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("run_at = %v", other.RunAt)
	}
	id := strconv.FormatInt(created.ID, 10)

	// A worker claims the email job and it fails for good.
	claimed, err := store.Claim(ctx, postgres.ClaimRequest{Limit: 1, Lease: time.Minute})
	if err != nil || len(claimed) != 1 || claimed[0].ID != created.ID {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	c.do("POST", "/v1/jobs/"+id+"/cancel", "", http.StatusConflict, nil) // running
	if _, err := store.Fail(ctx, created.ID, 1, errors.New("smtp: mailbox unavailable")); err != nil {
		t.Fatal(err)
	}

	var dead struct {
		Jobs       []wireJob `json:"jobs"`
		NextCursor string    `json:"next_cursor"`
	}
	c.do("GET", "/v1/jobs?state=dead", "", http.StatusOK, &dead)
	if len(dead.Jobs) != 1 || dead.Jobs[0].ID != created.ID || dead.Jobs[0].LastError != "smtp: mailbox unavailable" ||
		dead.Jobs[0].Payload != nil || dead.NextCursor != "" {
		t.Fatalf("dead letters: %+v", dead)
	}

	var requeued, got wireJob
	c.do("POST", "/v1/jobs/"+id+"/requeue", "", http.StatusOK, &requeued)
	c.do("GET", "/v1/jobs/"+id, "", http.StatusOK, &got)
	if requeued.State != "pending" || requeued.Attempt != 0 || got.State != "pending" || got.LastError == "" {
		t.Fatalf("requeued %+v, then got %+v", requeued, got)
	}
	c.do("POST", "/v1/jobs/"+id+"/requeue", "", http.StatusConflict, nil) // not dead any more

	var cancelled wireJob
	c.do("POST", "/v1/jobs/"+id+"/cancel", "", http.StatusOK, &cancelled)
	if cancelled.State != "cancelled" {
		t.Fatalf("cancel: %+v", cancelled)
	}

	// Two jobs, one per page.
	var page1, page2 struct {
		Jobs       []wireJob `json:"jobs"`
		NextCursor string    `json:"next_cursor"`
	}
	c.do("GET", "/v1/jobs?limit=1", "", http.StatusOK, &page1)
	c.do("GET", "/v1/jobs?limit=1&cursor="+page1.NextCursor, "", http.StatusOK, &page2)
	if len(page1.Jobs) != 1 || page1.Jobs[0].ID != other.ID || len(page2.Jobs) != 1 || page2.Jobs[0].ID != created.ID {
		t.Fatalf("pages: %+v then %+v", page1, page2)
	}
	c.do("GET", "/v1/jobs/999999", "", http.StatusNotFound, nil)
}
