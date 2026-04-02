-- PostgreSQL equivalent of the inflight_taskactivations table
CREATE TABLE IF NOT EXISTS inflight_taskactivations (
    id TEXT NOT NULL PRIMARY KEY,
    activation BYTEA NOT NULL,
    partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    added_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    processing_attempts INTEGER NOT NULL,
    expires_at TIMESTAMPTZ,
    delay_until TIMESTAMPTZ,
    processing_deadline_duration INTEGER NOT NULL,
    processing_deadline TIMESTAMPTZ,
    status TEXT NOT NULL,
    at_most_once BOOLEAN NOT NULL DEFAULT FALSE,
    application TEXT NOT NULL,
    namespace TEXT NOT NULL,
    taskname TEXT NOT NULL,
    on_attempts_exceeded INTEGER NOT NULL DEFAULT 1,
    bucket SMALLINT NOT NULL DEFAULT 0,
    sent BOOLEAN NOT NULL DEFAULT FALSE
);

-- Supports pending claim queries (status, filters, ordering) including sent
CREATE INDEX IF NOT EXISTS idx_inflight_taskactivations_claim
ON inflight_taskactivations (status, bucket, sent);
