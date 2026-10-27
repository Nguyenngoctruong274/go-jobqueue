-- The jobs table is the queue. The CHECK constraints restate the
-- invariants of internal/job, so a bug in a query cannot store a job the
-- state machine would never produce.
CREATE TABLE jobs (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type            TEXT        NOT NULL CHECK (type ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    payload         JSONB       NOT NULL DEFAULT '{}',
    state           TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'running', 'succeeded', 'dead', 'cancelled')),
    -- Higher runs first; within a priority, the earliest run_at, then id.
    priority        SMALLINT    NOT NULL DEFAULT 0 CHECK (priority BETWEEN -100 AND 100),
    attempt         INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until     TIMESTAMPTZ,
    last_error      TEXT,
    idempotency_key TEXT        CHECK (octet_length(idempotency_key) BETWEEN 1 AND 200),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT jobs_attempt_range CHECK (attempt BETWEEN 0 AND max_attempts),
    CONSTRAINT jobs_lease_iff_running CHECK ((state = 'running') = (lease_until IS NOT NULL)),
    CONSTRAINT jobs_pending_has_attempts CHECK (state <> 'pending' OR attempt < max_attempts),
    CONSTRAINT jobs_running_has_attempt CHECK (state <> 'running' OR attempt > 0)
);

-- At most one job per (type, key). The key stays taken for as long as the
-- row exists, finished or not, so a producer retrying an enqueue after a
-- timeout never runs the job twice.
CREATE UNIQUE INDEX jobs_idempotency ON jobs (type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- The claim query reads this index in order and stops at the first due row
-- it can lock. Only pending jobs are in it, so finished jobs piling up do
-- not slow claiming down.
CREATE INDEX jobs_claim ON jobs (priority DESC, run_at, id)
    WHERE state = 'pending';
