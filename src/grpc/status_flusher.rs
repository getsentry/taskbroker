//! Flushes batched status updates from the channel to the store.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::config::Config;
use crate::store::inflight_activation::{InflightActivationStatus, InflightActivationStore};

type StatusUpdate = (String, InflightActivationStatus);

/// Run the status flusher task. Receives (id, status) from the channel and
/// flushes to the store in batches, either when the batch is full or when
/// the max flush interval has elapsed.
pub async fn run_status_flusher(
    mut rx: mpsc::Receiver<StatusUpdate>,
    store: Arc<dyn InflightActivationStore>,
    config: Arc<Config>,
) {
    let batch_size = config.push_status_batch_size.max(1);
    let flush_interval = Duration::from_millis(config.push_status_flush_interval_ms);
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut buffer: Vec<StatusUpdate> = Vec::with_capacity(batch_size);

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some((id, status)) => {
                        buffer.push((id, status));
                        while let Ok(update) = rx.try_recv() {
                            buffer.push(update);
                        }
                        if buffer.len() >= batch_size {
                            flush_buffer(&store, &mut buffer).await;
                        }
                    }
                    None => {
                        // Channel closed (shutdown); flush remaining and exit.
                        if !buffer.is_empty() {
                            flush_buffer(&store, &mut buffer).await;
                        }
                        debug!("Status flusher shutting down");
                        break;
                    }
                }
            }
            _ = interval.tick() => {
                if !buffer.is_empty() {
                    flush_buffer(&store, &mut buffer).await;
                }
            }
        }
    }
}

async fn flush_buffer(store: &Arc<dyn InflightActivationStore>, buffer: &mut Vec<StatusUpdate>) {
    if buffer.is_empty() {
        return;
    }

    let updates = std::mem::take(buffer);
    let mut by_status: HashMap<InflightActivationStatus, Vec<String>> = HashMap::new();
    for (id, status) in updates {
        by_status.entry(status).or_default().push(id);
    }

    for (status, ids) in by_status {
        if let Err(e) = store.set_status_batch(&ids, status).await {
            error!(
                ?status,
                count = ids.len(),
                "Failed to flush status batch: {:?}",
                e
            );
            // Re-push failed updates so they can be retried on next flush.
            for id in ids {
                buffer.push((id, status));
            }
        } else {
            debug!(?status, count = ids.len(), "Flushed status batch");
        }
    }
}
