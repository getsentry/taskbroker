ALTER TABLE inflight_taskactivations ADD on_attempts_exceeded INTEGER NOT NULL;
ALTER TABLE inflight_taskactivations ADD received_at INTEGER NOT NULL;
ALTER TABLE inflight_taskactivations ADD taskname TEXT NOT NULL;
