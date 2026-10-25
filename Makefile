.DEFAULT_GOAL := help
BIN := bin/jobqueued
export JQ_DATABASE_URL ?= postgres://jobs:jobs@localhost:5432/jobs?sslmode=disable

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the jobqueued binary
	go build -trimpath -o $(BIN) ./cmd/jobqueued

.PHONY: run
run: ## Run jobqueued locally (needs `make up`)
	go run ./cmd/jobqueued

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race -count=1 ./...

.PHONY: test-db
test-db: ## Run all tests, including PostgreSQL ones (needs `make up`)
	JQ_TEST_DATABASE_URL=postgres://jobs:jobs@localhost:5432/jobs_test?sslmode=disable go test -race -count=1 ./...

.PHONY: cover
cover: ## Run tests and print total coverage
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run

.PHONY: fmt
fmt: ## Format code
	gofmt -s -w .

.PHONY: up
up: ## Start local dependencies (PostgreSQL)
	docker compose up -d

.PHONY: stack
stack: ## Run jobqueued and PostgreSQL in Docker
	docker compose --profile stack up --build

.PHONY: down
down: ## Stop everything
	docker compose --profile stack down
