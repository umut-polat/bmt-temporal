CREATE SCHEMA IF NOT EXISTS bmt;

CREATE TABLE IF NOT EXISTS bmt.batch_run (
    id            UUID PRIMARY KEY,
    workflow_id   TEXT NOT NULL UNIQUE,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL CHECK (status IN ('running', 'passed', 'failed')),
    config_json   JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS bmt.machine_run (
    id            UUID PRIMARY KEY,
    batch_id      UUID NOT NULL REFERENCES bmt.batch_run(id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    host          INET NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('running', 'passed', 'failed')),
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    UNIQUE (batch_id, name)
);

CREATE TABLE IF NOT EXISTS bmt.phase_result (
    id            UUID PRIMARY KEY,
    batch_id      UUID NOT NULL REFERENCES bmt.batch_run(id) ON DELETE CASCADE,
    machine_id    UUID REFERENCES bmt.machine_run(id) ON DELETE CASCADE,
    phase         TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('passed', 'failed', 'skipped')),
    details       JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS phase_result_batch_phase_idx
    ON bmt.phase_result (batch_id, phase);
CREATE INDEX IF NOT EXISTS phase_result_machine_idx
    ON bmt.phase_result (machine_id);

CREATE TABLE IF NOT EXISTS bmt.metric (
    id            BIGSERIAL PRIMARY KEY,
    phase_id      UUID NOT NULL REFERENCES bmt.phase_result(id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    value         DOUBLE PRECISION NOT NULL,
    unit          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS metric_phase_idx ON bmt.metric (phase_id);
