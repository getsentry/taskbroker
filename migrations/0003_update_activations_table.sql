ALTER TABLE inflight_taskactivations ADD on_attempts_exceeded INTEGER NOT NULL DEFAULT 1; -- 1 is Discard
ALTER TABLE inflight_taskactivations ADD received_at INTEGER NOT NULL DEFAULT 0;
ALTER TABLE inflight_taskactivations ADD taskname TEXT NOT NULL DEFAULT "";
