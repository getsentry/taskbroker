ALTER TABLE inflight_taskactivations ADD COLUMN topic TEXT NOT NULL DEFAULT '';

-- Contention filter for the upkeep queries, which filter by status +
-- (topic,partition) and have no ORDER BY, so they bitmap-scan this index.
-- Replaces idx_activation_partition (partition), which a followup drops.
CREATE INDEX idx_topic_partition
    ON inflight_taskactivations (topic, partition);

-- Age-based drain branch (added_at < threshold).
-- SQLite has an equivalent in its (status, added_at, ...) index, but we want
-- to keep this index separate from status as added_at is immutable.
CREATE INDEX idx_added_at
    ON inflight_taskactivations (added_at);
