-- Multi-topic + age-based contention management (STREAM-1205).
--
-- A `topic` column distinguishes activations across topics so that partition
-- indices, which overlap between topics, no longer collide. Contention queries
-- filter by (topic, partition) instead of partition alone.
ALTER TABLE inflight_taskactivations ADD COLUMN topic TEXT NOT NULL DEFAULT '';

-- Serves the contention branch of the claim/upkeep queries:
-- (topic, partition) IN (...) AND bucket BETWEEN ...
CREATE INDEX idx_topic_partition_bucket
    ON inflight_taskactivations (topic, partition, bucket);

-- Serves the age-based drain branch (added_at < threshold) that lets any broker
-- claim/maintain orphaned rows regardless of partition ownership. Non-partial so
-- it covers the drain branch for every status, not just Pending.
CREATE INDEX idx_added_at
    ON inflight_taskactivations (added_at);
