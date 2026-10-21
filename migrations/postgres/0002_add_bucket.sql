ALTER TABLE inflight_taskactivations ADD COLUMN bucket SMALLINT NOT NULL DEFAULT 0;

CREATE INDEX idx_activation_claim ON inflight_taskactivations (status, bucket);
