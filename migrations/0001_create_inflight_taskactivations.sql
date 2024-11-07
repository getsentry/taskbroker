CREATE TABLE IF NOT EXISTS inflight_taskactivations (
    id UUID NOT NULL PRIMARY KEY,
    activation BLOB NOT NULL,
    offset BIGINTEGER NOT NULL,
    added_at DATETIME NOT NULL,
    deadletter_at DATETIME,
    processing_deadline_duration INTEGER NOT NULL,
    processing_deadline DATETIME,
    status INTEGER NOT NULL
);