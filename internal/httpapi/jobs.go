package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
)

// Jobs is what the job endpoints need from the job store.
type Jobs interface {
	Enqueue(ctx context.Context, spec job.Spec) (j job.Job, created bool, err error)
	Get(ctx context.Context, id int64) (job.Job, error)
	List(ctx context.Context, f ListFilter) ([]job.Job, error)
	Cancel(ctx context.Context, id int64) (job.Job, error)
	Requeue(ctx context.Context, id int64) (job.Job, error)
}

// ListFilter selects a page of jobs, newest first. Zero State and Type
// match any; Before, if positive, starts after the job with that id.
type ListFilter struct {
	State  job.State
	Type   string
	Limit  int
	Before int64
}

// Page sizes of GET /v1/jobs.
const (
	DefaultListLimit = 50
	MaxListLimit     = 500
)

// maxEnqueueBody leaves room for the other fields around a payload of the
// largest size the store accepts.
const maxEnqueueBody = job.MaxPayloadBytes + 64<<10

func (a *api) routeJobs(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/jobs", a.enqueue)
	mux.HandleFunc("GET /v1/jobs", a.list)
	mux.HandleFunc("GET /v1/jobs/{id}", a.get)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", a.cancel)
	mux.HandleFunc("POST /v1/jobs/{id}/requeue", a.requeue)
}

// jobJSON is the wire form of a job. Times are UTC, RFC 3339.
type jobJSON struct {
	ID             int64           `json:"id"`
	Type           string          `json:"type"`
	State          job.State       `json:"state"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       int             `json:"priority"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	LeaseUntil     *time.Time      `json:"lease_until,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

func toJSON(j job.Job) jobJSON {
	out := jobJSON{
		ID: j.ID, Type: j.Type, State: j.State, Payload: j.Payload, Priority: j.Priority,
		Attempt: j.Attempt, MaxAttempts: j.MaxAttempts, RunAt: j.RunAt.UTC(),
		LastError: j.LastError, IdempotencyKey: j.IdempotencyKey, CreatedAt: j.CreatedAt.UTC(),
	}
	if !j.LeaseUntil.IsZero() {
		lease := j.LeaseUntil.UTC()
		out.LeaseUntil = &lease
	}
	return out
}

type enqueueRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          *time.Time      `json:"run_at"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// enqueue answers 201 with the new job, or 200 with the existing one when
// the idempotency key was already used for the same work.
func (a *api) enqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := decodeJSON(w, r, &req); err != nil {
		a.fail(w, r, err)
		return
	}
	spec := job.Spec{
		Type: req.Type, Payload: req.Payload, Priority: req.Priority,
		MaxAttempts: req.MaxAttempts, IdempotencyKey: req.IdempotencyKey,
	}
	if string(req.Payload) == "null" {
		spec.Payload = nil // same as leaving it out: {}
	}
	if req.RunAt != nil {
		spec.RunAt = *req.RunAt
	}
	j, created, err := a.jobs.Enqueue(r.Context(), spec)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
		w.Header().Set("Location", "/v1/jobs/"+strconv.FormatInt(j.ID, 10))
	}
	writeJSON(w, status, toJSON(j))
}

func (a *api) get(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	j, err := a.jobs.Get(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toJSON(j))
}

type listResponse struct {
	Jobs []jobJSON `json:"jobs"`
	// NextCursor fetches the next page; it is absent on a short page,
	// which is the last.
	NextCursor string `json:"next_cursor,omitempty"`
}

// list pages through jobs newest first, without payloads:
// GET /v1/jobs?state=dead&type=email.send&limit=50&cursor=...
func (a *api) list(w http.ResponseWriter, r *http.Request) {
	f, err := parseListFilter(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	jobs, err := a.jobs.List(r.Context(), f)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	resp := listResponse{Jobs: make([]jobJSON, len(jobs))}
	for i, j := range jobs {
		resp.Jobs[i] = toJSON(j)
	}
	if len(jobs) == f.Limit {
		resp.NextCursor = strconv.FormatInt(jobs[len(jobs)-1].ID, 10)
	}
	writeJSON(w, http.StatusOK, resp)
}

func parseListFilter(r *http.Request) (ListFilter, error) {
	q := r.URL.Query()
	f := ListFilter{Limit: DefaultListLimit, Type: q.Get("type")}
	var problems []string
	if s := q.Get("state"); s != "" {
		st, err := job.ParseState(s)
		if err != nil {
			problems = append(problems, "state: must be one of pending, running, succeeded, dead, cancelled")
		}
		f.State = st
	}
	if f.Type != "" {
		if err := job.ValidateType(f.Type); err != nil {
			problems = append(problems, err.Error())
		}
	}
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > MaxListLimit {
			problems = append(problems, fmt.Sprintf("limit: must be between 1 and %d", MaxListLimit))
		}
		f.Limit = n
	}
	// The cursor is the last id of the previous page. Clients should pass
	// next_cursor back as it is, not build one.
	if s := q.Get("cursor"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 1 {
			problems = append(problems, "cursor: not a cursor this API returned")
		}
		f.Before = n
	}
	if len(problems) > 0 {
		return ListFilter{}, badRequest(strings.Join(problems, "; "))
	}
	return f, nil
}

func (a *api) cancel(w http.ResponseWriter, r *http.Request) {
	a.transition(w, r, a.jobs.Cancel)
}

func (a *api) requeue(w http.ResponseWriter, r *http.Request) {
	a.transition(w, r, a.jobs.Requeue)
}

// transition runs an operator action on the job named in the path and
// answers with the job as stored afterwards.
func (a *api) transition(w http.ResponseWriter, r *http.Request, do func(context.Context, int64) (job.Job, error)) {
	id, err := pathID(r)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	j, err := do(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toJSON(j))
}

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		return 0, badRequest("id: must be a positive integer")
	}
	return id, nil
}

// decodeJSON reads one JSON object into v. Unknown fields are refused, so
// a misspelt "max_attempt" fails instead of silently taking the default.
// Requiring application/json also keeps plain HTML forms from posting.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		return &apiError{http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json"}
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEnqueueBody))
	dec.DisallowUnknownFields()
	err = dec.Decode(v)
	if err == nil {
		if dec.Decode(&struct{}{}) != io.EOF {
			return badRequest("body: must hold a single JSON object")
		}
		return nil
	}
	if maxErr := (*http.MaxBytesError)(nil); errors.As(err, &maxErr) {
		return &apiError{http.StatusRequestEntityTooLarge, "too_large", fmt.Sprintf("body: larger than %d bytes", maxErr.Limit)}
	}
	return badRequest("body: " + err.Error())
}

// apiError is an error with the response it maps to.
type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.message }

func badRequest(msg string) error { return &apiError{http.StatusBadRequest, "bad_request", msg} }

// oneLine puts the parts of a joined error on one line.
func oneLine(err error) string { return strings.ReplaceAll(err.Error(), "\n", "; ") }

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// fail answers with the error's status. The store's domain errors carry
// messages meant for the caller; anything else is logged and hidden.
func (a *api) fail(w http.ResponseWriter, r *http.Request, err error) {
	e := &apiError{http.StatusInternalServerError, "internal", "internal error"}
	var known *apiError
	switch {
	case errors.As(err, &known):
		e = known
	case errors.Is(err, job.ErrInvalidSpec):
		e = &apiError{http.StatusUnprocessableEntity, "invalid_job", oneLine(err)}
	case errors.Is(err, job.ErrIdempotencyConflict):
		e = &apiError{http.StatusConflict, "idempotency_conflict", oneLine(err)}
	case errors.Is(err, job.ErrNotFound):
		e = &apiError{http.StatusNotFound, "not_found", "job not found"}
	case errors.Is(err, job.ErrInvalidTransition):
		e = &apiError{http.StatusConflict, "invalid_state", oneLine(err)}
	case r.Context().Err() != nil:
		// The client went away; nobody reads the answer.
		a.logger.InfoContext(r.Context(), "request abandoned", slog.String("request_id", RequestID(r.Context())),
			slog.Any("error", err))
	default:
		a.logger.ErrorContext(r.Context(), "request failed", slog.String("request_id", RequestID(r.Context())),
			slog.String("method", r.Method), slog.String("path", r.URL.Path), slog.Any("error", err))
	}
	var body errorBody
	body.Error.Code, body.Error.Message = e.code, e.message
	writeJSON(w, e.status, body)
}
