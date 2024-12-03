ALTER TABLE inflight_taskactivations ADD COLUMN at_most_once BOOLEAN NOT NULL DEFAULT FALSE;
