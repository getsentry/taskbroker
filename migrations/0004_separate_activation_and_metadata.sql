CREATE TABLE IF NOT EXISTS activation_blobs (
    id TEXT NOT NULL PRIMARY KEY,
    activation BLOB NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS activation_metadata (
    id TEXT NOT NULL PRIMARY KEY, -- Can be joined with activation_blobs.id
    namespace TEXT,
    taskname TEXT NOT NULL DEFAULT "",
    status TEXT NOT NULL,
    received_at INTEGER NOT NULL DEFAULT 0,
    added_at INTEGER NOT NULL,
    processing_attempts INTEGER NOT NULL,
    processing_deadline_duration INTEGER NOT NULL,
    processing_deadline INTEGER,
    at_most_once INTEGER NOT NULL DEFAULT 0,
    on_attempts_exceeded INTEGER NOT NULL DEFAULT 1, -- 1 is Discard
    expires_at INTEGER,
    delay_until INTEGER
) STRICT;

CREATE INDEX idx_metadata_status
ON activation_metadata (status, added_at, namespace, id);
