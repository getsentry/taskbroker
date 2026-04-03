ALTER TABLE inflight_taskactivations ADD COLUMN sent BOOLEAN NOT NULL DEFAULT FALSE;

DROP INDEX IF EXISTS idx_activation_claim;
CREATE INDEX idx_activation_claim ON inflight_taskactivations (status, bucket, sent);
