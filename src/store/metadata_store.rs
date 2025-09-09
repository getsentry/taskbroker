use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap},
};

use chrono::{DateTime, Duration, Utc};
use sentry::capture_message;
use sentry_protos::taskbroker::v1::OnAttemptsExceeded;
use sqlx::{QueryBuilder, Sqlite, Transaction, sqlite::SqliteQueryResult};

use crate::store::inflight_activation::InflightActivationStatus;
use crate::store::records::{ActivationMetadata, TimestampEntry};

/// Metadata result about failed tasks.
pub struct FailedState {
    /// Ids of tasks that were discarded.
    pub to_discard: Vec<String>,
    /// Ids of tasks that were sent to deadletter.
    pub to_deadletter: Vec<String>,
}

/// The in-memory metadata store for activations.
/// This store is kept in memory, and is used to track runtime
/// state of activations.
///
/// The upkeep loop is responsible for keeping this store synced to
/// sqlite. As records are moved through the state machine, they are
/// added to a 'dirty' set. When the [`Self::flush()`] is called, all
/// records in the dirty list are flushed to sqlite.
///
/// This structure is typically wrapped in a Mutex or RwLock
/// to enable safe concurrent access.
#[derive(Clone, Debug)]
pub struct MetadataStore {
    /// The key/value of records.
    records: BTreeMap<String, ActivationMetadata>,

    /// Heap of records that are delayed ordered by due deadline. This is a min-heap.
    delayed: BinaryHeap<Reverse<TimestampEntry>>,

    /// Heap of records that are pending ordered by added datetime. This is a min-heap.
    pending: BinaryHeap<Reverse<TimestampEntry>>,

    /// Heap of records that are processding ordered by completion deadline. This is a min-heap.
    processing: BinaryHeap<Reverse<TimestampEntry>>,

    /// Retry status
    retry: BTreeSet<String>,

    /// Complete status
    complete: BTreeSet<String>,

    /// Failed status
    failed: BTreeSet<String>,

    /// Ids of records that have been mutated, and need to be flushed to storage
    dirty_ids: BTreeSet<String>,
}

impl Default for MetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataStore {
    /// Create a new metadata store
    pub fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            delayed: BinaryHeap::new(),
            pending: BinaryHeap::new(),
            processing: BinaryHeap::new(),
            retry: BTreeSet::new(),
            complete: BTreeSet::new(),
            failed: BTreeSet::new(),
            dirty_ids: BTreeSet::new(),
        }
    }

    /// Insert or update an activation metadata record
    pub fn upsert(&mut self, metadata: ActivationMetadata) -> anyhow::Result<()> {
        // File by status
        match metadata.status {
            InflightActivationStatus::Pending => {
                self.pending.push(Reverse(metadata.pending_entry()))
            }
            InflightActivationStatus::Processing => {
                self.processing.push(Reverse(metadata.processing_entry()))
            }
            InflightActivationStatus::Delay => self.delayed.push(Reverse(metadata.delayed_entry())),
            InflightActivationStatus::Retry => {
                self.retry.insert(metadata.id.clone());
            }
            InflightActivationStatus::Failure => {
                self.failed.insert(metadata.id.clone());
            }
            InflightActivationStatus::Complete => {
                self.complete.insert(metadata.id.clone());
            }
            InflightActivationStatus::Unspecified => {
                capture_message(
                    "Upsert called with unspecified status",
                    sentry::Level::Error,
                );
            }
        }
        self.dirty_ids.insert(metadata.id.clone());
        self.records.insert(metadata.id.clone(), metadata);

        Ok(())
    }

    pub fn upsert_batch(&mut self, records: Vec<ActivationMetadata>) -> anyhow::Result<()> {
        for record in records {
            self.upsert(record)?;
        }
        Ok(())
    }

    /// Add all dirty records to the provided sqlite connection.
    /// The connection can be part of a transaction guard.
    pub async fn commit(
        &mut self,
        mut atomic: Transaction<'static, Sqlite>,
    ) -> Result<SqliteQueryResult, sqlx::Error> {
        if self.dirty_ids.is_empty() {
            return Ok(SqliteQueryResult::default());
        }
        let mut query_builder = QueryBuilder::<Sqlite>::new(
            "INSERT INTO activation_metadata (
                id, 
                namespace,
                taskname,
                status,
                received_at,
                added_at,
                processing_attempts,
                processing_deadline_duration,
                processing_deadline,
                at_most_once,
                on_attempts_exceeded,
                expires_at,
                delay_until
            ) ",
        );
        let query_builder = query_builder
            .push_values(self.dirty_ids.iter(), |mut b, row| {
                let row = self
                    .records
                    .get(row)
                    .expect("Dirty id not found in records");
                b.push_bind(row.id.clone());
                b.push_bind(row.namespace.clone());
                b.push_bind(row.taskname.clone());
                b.push_bind(row.status);
                b.push_bind(row.received_at.timestamp());
                b.push_bind(row.added_at.timestamp());
                b.push_bind(row.processing_attempts);
                b.push_bind(row.processing_deadline_duration);
                if let Some(deadline) = row.processing_deadline {
                    b.push_bind(deadline.timestamp());
                } else {
                    // Add a literal null
                    b.push("null");
                }
                b.push_bind(row.at_most_once);
                b.push_bind(row.on_attempts_exceeded as i32);

                b.push_bind(row.expires_at.map(|t| Some(t.timestamp())));
                b.push_bind(row.delay_until.map(|t| Some(t.timestamp())));
            })
            .push(
                " ON CONFLICT(id) DO UPDATE SET
                namespace=excluded.namespace,
                taskname=excluded.taskname,
                status=excluded.status,
                received_at=excluded.received_at,
                added_at=excluded.added_at,
                processing_attempts=excluded.processing_attempts,
                processing_deadline_duration=excluded.processing_deadline_duration,
                processing_deadline=excluded.processing_deadline,
                at_most_once=excluded.at_most_once,
                on_attempts_exceeded=excluded.on_attempts_exceeded,
                expires_at=excluded.expires_at,
                delay_until=excluded.delay_until",
            );
        let query = query_builder.build();
        metrics::counter!("metadata_store.commit.records").increment(self.dirty_ids.len() as u64);

        let res = query.execute(&mut *atomic).await;
        atomic.commit().await.unwrap();
        self.clear_dirty();

        res
    }

    /// Advance the state of a record and move it between the status containers.
    fn advance_state(&mut self, record: &ActivationMetadata, new_status: InflightActivationStatus) {
        // Remove from existing status containers
        match record.status {
            InflightActivationStatus::Pending => {
                self.pending.retain(|entry| entry.0.id != record.id);
            }
            InflightActivationStatus::Processing => {
                self.processing.retain(|entry| entry.0.id != record.id);
            }
            InflightActivationStatus::Delay => {
                self.delayed.retain(|entry| entry.0.id != record.id);
            }
            InflightActivationStatus::Retry => {
                self.retry.remove(&record.id);
            }
            InflightActivationStatus::Failure => {
                self.failed.remove(&record.id);
            }
            InflightActivationStatus::Complete => {
                self.complete.remove(&record.id);
            }
            InflightActivationStatus::Unspecified => {}
        }

        // Add to new status containers
        match new_status {
            InflightActivationStatus::Pending => {
                self.pending.push(Reverse(record.pending_entry()));
            }
            InflightActivationStatus::Processing => {
                self.processing.push(Reverse(record.processing_entry()));
            }
            InflightActivationStatus::Delay => {
                self.delayed.push(Reverse(record.delayed_entry()));
            }
            InflightActivationStatus::Retry => {
                self.retry.insert(record.id.clone());
            }
            InflightActivationStatus::Failure => {
                self.failed.insert(record.id.clone());
            }
            InflightActivationStatus::Complete => {
                self.complete.insert(record.id.clone());
            }
            InflightActivationStatus::Unspecified => {}
        }
    }

    /// Mark a set of records by id as completed.
    pub fn mark_completed(&mut self, ids: Vec<String>) -> u64 {
        let mut updated = 0;
        for id in ids.iter() {
            if let Some(record) = self.records.get(id) {
                let mut record = record.clone();
                self.advance_state(&record, InflightActivationStatus::Complete);
                record.processing_deadline = None;
                record.status = InflightActivationStatus::Complete;

                self.complete.insert(id.clone());
                self.dirty_ids.insert(id.clone());
                self.records.insert(id.clone(), record);
                updated += 1;
            } else {
                println!("Warning: stale heap data encountered for id: {id}");
                self.add_to_failed(id.clone());
            }
        }

        updated
    }

    /// Remove all completed records from the in-memory store.
    /// Get a vec of ids that were removed.
    pub fn remove_completed(&mut self) -> Vec<String> {
        let mut removed = vec![];
        for id in self.complete.iter() {
            self.dirty_ids.remove(id);
            self.records.remove(id);

            removed.push(id.clone());
        }
        self.complete.clear();

        removed
    }

    /// Reset all in-memory state.
    pub fn clear(&mut self) {
        self.records.clear();
        self.delayed.clear();
        self.pending.clear();
        self.processing.clear();
        self.retry.clear();
        self.complete.clear();
        self.failed.clear();
        self.dirty_ids.clear();
    }

    /// Clear the dirty set after a successful commit.
    pub fn clear_dirty(&mut self) {
        self.dirty_ids.clear();
    }

    /// Remove a record by id from the the store and remove it from the dirty set.
    pub fn delete(&mut self, id: &str) {
        if let Some(record) = self.records.get(id) {
            match record.status {
                InflightActivationStatus::Pending => {
                    self.pending.retain(|entry| entry.0.id != id);
                }
                InflightActivationStatus::Processing => {
                    self.processing.retain(|entry| entry.0.id != id);
                }
                InflightActivationStatus::Delay => {
                    self.delayed.retain(|entry| entry.0.id != id);
                }
                InflightActivationStatus::Retry => {
                    self.retry.remove(id);
                }
                InflightActivationStatus::Failure => {
                    self.failed.remove(id);
                }
                InflightActivationStatus::Complete => {
                    self.complete.remove(id);
                }
                InflightActivationStatus::Unspecified => {}
            }
            self.records.remove(id);
            self.dirty_ids.remove(id);
        }
    }

    /// Add to failed set
    /// Record cannot be found, we have encountered stale heap data.
    /// This shouldn't happen.
    fn add_to_failed(&mut self, id: String) {
        let metadata = self.records.get(&id);
        if let Some(metadata) = metadata {
            let mut metadata = metadata.clone();
            metadata.status = InflightActivationStatus::Failure;
            self.records.insert(id.clone(), metadata);
        }
        self.dirty_ids.insert(id.clone());
        self.failed.insert(id);
    }

    /// Get the next pending activation, optionally by namespace.
    /// When looking for pending activations, state transitions are
    /// performed for activations past their expires_at, or that have
    /// exceeded their processing attempts.
    pub fn get_pending_activation(
        &mut self,
        namespace: Option<&str>,
        now: DateTime<Utc>,
        processing_grace_period: u64,
        max_processing_attempts: i32,
    ) -> Result<Option<ActivationMetadata>, anyhow::Error> {
        let mut namespace_skipped: Vec<TimestampEntry> = vec![];
        let mut metadata = loop {
            let next_entry = self.pending.pop();
            let Some(Reverse(entry)) = next_entry else {
                return Ok(None);
            };
            let metadata = self.records.get(&entry.id).unwrap();

            // Check if the entry expired in the pending state.
            // This relocates some logic from upkeep to run during fetch, which isn't great, but
            // also amortizes the work :shrug:
            if let Some(expires_at) = metadata.expires_at
                && expires_at < now
            {
                metrics::counter!("get_pending_activation.expired").increment(1);
                self.add_to_failed(entry.id.clone());
                continue;
            }
            // Check if the entry has exceeded processing attempts.
            if metadata.processing_attempts >= max_processing_attempts {
                metrics::counter!("get_pending_activation.attempts_exceeded").increment(1);
                self.add_to_failed(entry.id.clone());
                continue;
            }
            // TODO: Check if the entry is killed

            // Filter by namespace if provided.
            if let Some(namespace) = namespace {
                if metadata.namespace != namespace {
                    namespace_skipped.push(entry);
                    continue;
                }
            }

            // Found a valid entry.
            break metadata.clone();
        };
        // Readd any skipped entries
        namespace_skipped.iter().for_each(|entry| {
            self.pending.push(Reverse(entry.clone()));
        });

        // Update the processing deadline, and move to processing state, then update the metadata.
        // Ordering here is important.
        metadata.processing_deadline = Some(
            now + Duration::seconds(
                (metadata.processing_deadline_duration as u64 + processing_grace_period) as i64,
            ),
        );
        self.advance_state(&metadata, InflightActivationStatus::Processing);
        metadata.status = InflightActivationStatus::Processing;

        self.dirty_ids.insert(metadata.id.clone());
        self.records.insert(metadata.id.clone(), metadata.clone());

        Ok(Some(metadata.clone()))
    }

    pub fn handle_processing_deadline(&mut self) -> Result<u64, anyhow::Error> {
        let now = Utc::now();

        let mut past_deadline: Vec<TimestampEntry> = vec![];
        while let Some(Reverse(entry)) = self.processing.pop() {
            // Found a entry in the future stop and process what we found.
            if let Some(timestamp) = entry.timestamp
                && timestamp > now
            {
                self.processing.push(Reverse(entry));
                break;
            }
            past_deadline.push(entry);
        }

        for entry in past_deadline.iter() {
            let id = entry.id.clone();
            let record = if let Some(record) = self.records.get(&id) {
                record
            } else {
                println!("Warning: stale heap data encountered for id: {id}");
                self.add_to_failed(id.clone());
                continue;
            };
            let new_status = if record.at_most_once {
                InflightActivationStatus::Failure
            } else {
                InflightActivationStatus::Pending
            };

            let mut record = record.clone();
            // Reset the deadline and attempts
            record.status = new_status;
            record.processing_deadline = None;
            record.processing_attempts += 1;

            self.dirty_ids.insert(record.id.clone());
            self.records.insert(record.id.clone(), record.clone());
            match record.status {
                InflightActivationStatus::Pending => {
                    self.pending.push(Reverse(record.pending_entry()));
                }
                InflightActivationStatus::Failure => {
                    self.failed.insert(record.id.clone());
                }
                _ => {
                    panic!(
                        "Unexpected status in handle_processing_deadline: {:?}",
                        record.status
                    );
                }
            }
        }

        Ok(past_deadline.len() as u64)
    }

    // Get a structure of ids that have been discarded and vec of ids
    // that need to be sent to deadletter. Deadletter activations will remain in
    // failed until removed after messages are produced.
    pub fn handle_failed_tasks(&mut self) -> FailedState {
        let mut state = FailedState {
            to_discard: vec![],
            to_deadletter: vec![],
        };
        for id in self.failed.iter() {
            let metadata = if let Some(metadata) = self.records.get(id) {
                metadata
            } else {
                continue;
            };
            let on_attempts_exceeded = metadata.on_attempts_exceeded;

            if on_attempts_exceeded == OnAttemptsExceeded::Discard
                || on_attempts_exceeded == OnAttemptsExceeded::Unspecified
            {
                state.to_discard.push(id.clone())
            } else if on_attempts_exceeded == OnAttemptsExceeded::Deadletter {
                state.to_deadletter.push(id.clone())
            }
        }
        for id in state.to_discard.iter() {
            if let Some(metadata) = self.records.get(id) {
                let mut updated = metadata.clone();
                self.advance_state(&updated, InflightActivationStatus::Complete);
                updated.status = InflightActivationStatus::Complete;

                self.dirty_ids.insert(id.clone());
                self.records.insert(id.clone(), updated);
            };
        }

        state
    }

    /// Move any delayed records that are due to pending.
    pub fn handle_delay_until(&mut self) -> Result<u64, anyhow::Error> {
        let now = Utc::now();

        let mut ready: Vec<TimestampEntry> = vec![];
        while let Some(Reverse(entry)) = self.delayed.pop() {
            // Found a entry in the future.
            if let Some(timestamp) = entry.timestamp
                && timestamp > now
            {
                self.delayed.push(Reverse(entry));
                break;
            }
            ready.push(entry);
        }

        for entry in ready.iter() {
            let id = entry.id.clone();
            let record = if let Some(record) = self.records.get(&id) {
                record
            } else {
                println!("Warning: stale heap data encountered for id: {id}");
                self.add_to_failed(id.clone());
                continue;
            };
            let mut record = record.clone();
            self.advance_state(&record, InflightActivationStatus::Pending);
            record.status = InflightActivationStatus::Pending;

            self.dirty_ids.insert(record.id.clone());
            self.records.insert(record.id.clone(), record.clone());
        }

        Ok(ready.len() as u64)
    }

    /// Get the list of ids in currently in the retry state.
    pub fn get_retry_entry_map(&self) -> HashMap<String, ActivationMetadata> {
        let mut map = HashMap::new();
        for item in self.retry.iter() {
            if let Some(record) = self.records.get(item) {
                map.insert(item.clone(), record.clone());
            }
        }
        map
    }

    /// Get max lag of pending activations. This is o(n) over the pending set.
    pub fn pending_activation_max_lag(&mut self, now: &DateTime<Utc>) -> f64 {
        let mut found: Option<ActivationMetadata> = None;
        for Reverse(entry) in self.pending.iter() {
            let metadata = if let Some(metadata) = self.records.get(&entry.id) {
                metadata
            } else {
                continue;
            };
            if metadata.processing_attempts > 0 {
                continue;
            }
            if found.is_none() {
                found = Some(metadata.clone());
            } else if let Some(existing) = &found
                && metadata.received_at < existing.received_at
            {
                found = Some(metadata.clone());
            }
        }
        if let Some(oldest) = &found {
            // latency is: now - received_at - (delay_until - received_at)?
            // We remove the delay to better reflect the time
            // activations spend in pending state.
            let millis = now
                .signed_duration_since(oldest.received_at)
                .num_milliseconds()
                - oldest.delay_until.map_or(0, |delay_time| {
                    delay_time
                        .signed_duration_since(oldest.received_at)
                        .num_milliseconds()
                });
            millis as f64 / 1000.0
        } else {
            0.0
        }
    }

    /// Get total count of records in the store.
    pub fn count_all(&self) -> usize {
        self.records.len()
    }

    /// Get a count of records by a specific status.
    ///
    /// This will both do a linear scan of all records, and also use
    /// status specific containers and perform a sanity check. This should be configurable
    /// logic as the linear scan will be expensive in production.
    pub fn count_by_status(&self, status: InflightActivationStatus) -> usize {
        let status_count = match status {
            InflightActivationStatus::Pending => self.pending.len(),
            InflightActivationStatus::Processing => self.processing.len(),
            InflightActivationStatus::Delay => self.delayed.len(),
            InflightActivationStatus::Complete => self.complete.len(),
            InflightActivationStatus::Retry => self.retry.len(),
            InflightActivationStatus::Failure => self.failed.len(),
            _ => 0,
        };
        // TODO remove this sanity check, or have it record sentry errors instead.
        if true {
            let count = self.records.iter().fold(0, |acc, (_id, record)| {
                let increment = if record.status == status { 1 } else { 0 };
                acc + increment
            });
            if status_count != count {
                panic!(
                    "count_by_status() - counts unequal status={status:?} records={count} status_count={status_count}"
                );
            }
        }

        status_count
    }

    /// Update the status of a record by id, no domain rules are applied.
    pub fn set_status(&mut self, id: &str, status: InflightActivationStatus) -> anyhow::Result<()> {
        if let Some(record) = self.records.get(id) {
            let mut record = record.clone();
            self.advance_state(&record, status);
            record.status = status;

            self.dirty_ids.insert(id.to_string());
            self.records.insert(id.to_string(), record);

            Ok(())
        } else {
            Err(anyhow::anyhow!("Record not found"))
        }
    }

    /// Update the processing deadline for a record by id.
    pub fn set_processing_deadline(
        &mut self,
        id: &str,
        deadline: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        if let Some(record) = self.records.get_mut(id) {
            record.processing_deadline = deadline;

            // Remove the existing entry from the processing heap.
            self.processing.retain(|entry| entry.0.id != id);
            self.processing.push(Reverse(record.processing_entry()));

            self.dirty_ids.insert(id.to_string());
            Ok(())
        } else {
            Err(anyhow::anyhow!("Record not found"))
        }
    }

    /// Get activation metadata by id
    /// Returns a clone of the current state.
    pub fn get_by_id(&self, id: &str) -> Option<ActivationMetadata> {
        if let Some(record) = self.records.get(id) {
            return Some(record.clone());
        }
        None
    }
}
