CREATE TABLE IF NOT EXISTS inflight_taskactivations (
    id TEXT NOT NULL PRIMARY KEY,
    activation BLOB NOT NULL,
    partition INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    added_at INTEGER NOT NULL,
    processing_attempts INTEGER NOT NULL,
    expires_at INTEGER,
    processing_deadline_duration INTEGER NOT NULL,
    processing_deadline INTEGER,
    status TEXT NOT NULL,
    at_most_once INTEGER NOT NULL DEFAULT 0,
    namespace TEXT
) STRICT;

CREATE INDEX idx_pending_activation
ON inflight_taskactivations (status, added_at, namespace, id);

CREATE INDEX idx_processing_deadline
ON inflight_taskactivations (status, processing_deadline);

CREATE INDEX idx_processing_attempts
ON inflight_taskactivations (status, processing_attempts);

CREATE INDEX idx_expires_at
ON inflight_taskactivations (status, expires_at);