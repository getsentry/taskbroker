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
    on_attempts_exceeded INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX idx_activation_partition ON inflight_taskactivations (partition);
