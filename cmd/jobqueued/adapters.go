package main

import (
	"context"
	"time"

	"github.com/Nguyenngoctruong274/go-jobqueue/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/job"
	"github.com/Nguyenngoctruong274/go-jobqueue/internal/postgres"
)

// poolStore adapts postgres.Store to the worker pool's Store.
type poolStore struct{ *postgres.Store }

func (s poolStore) Claim(ctx context.Context, types []string, limit int, lease time.Duration) ([]job.Job, error) {
	return s.Store.Claim(ctx, postgres.ClaimRequest{Types: types, Limit: limit, Lease: lease})
}

// apiStore adapts postgres.Store to the HTTP API's Jobs.
type apiStore struct{ *postgres.Store }

func (s apiStore) List(ctx context.Context, f httpapi.ListFilter) ([]job.Job, error) {
	return s.Store.List(ctx, postgres.ListRequest{State: f.State, Type: f.Type, Limit: f.Limit, Before: f.Before})
}
