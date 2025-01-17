CREATE TABLE IF NOT EXISTS inflight_taskactivations (
    id UUID NOT NULL PRIMARY KEY,
    activation BLOB NOT NULL,
    partition INTEGER NOT NULL,
    offset BIGINTEGER NOT NULL,
    added_at INTEGER NOT NULL,
    remove_at INTEGER NOT NULL,
    processing_deadline_duration INTEGER NOT NULL,
    processing_deadline INTEGER,
    status INTEGER NOT NULL,
    at_most_once BOOLEAN NOT NULL DEFAULT FALSE,
    namespace TEXT
);
