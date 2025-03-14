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
