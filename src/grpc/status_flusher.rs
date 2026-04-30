use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::mpsc::Receiver;
use tracing::{debug, error};

use crate::config::Config;
use crate::store::activation::InflightActivationStatus;
use crate::store::traits::InflightActivationStore;

pub type StatusUpdate = (String, InflightActivationStatus);

/// Run the status flusher task. Receives (id, status) from the channel and
/// flushes to the store in batches, either when the batch is full or when
/// the max flush interval has elapsed.
pub async fn run_status_flusher(
    mut rx: Receiver<StatusUpdate>,
    store: Arc<dyn InflightActivationStore>,
    config: Arc<Config>,
) -> Result<()> {
    let batch_size = config.status_flush_batch_size.max(1);
    let flush_interval = Duration::from_millis(config.status_flush_interval_ms);
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
                            metrics::histogram!("status_flush.batch_size").record(buffer.len() as f64);
                            flush_buffer(&store, &mut buffer).await;
                        }
                    }

                    None => {
                        // Channel closed (shutdown), flush remaining and exit.
                        if !buffer.is_empty() {
                            flush_buffer(&store, &mut buffer).await;
                        }

                        debug!("Status flusher shutting down...");
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

    Ok(())
}

async fn flush_buffer(store: &Arc<dyn InflightActivationStore>, buffer: &mut Vec<StatusUpdate>) {
    if buffer.is_empty() {
        return;
    }

    let start = Instant::now();

    let updates = std::mem::take(buffer);
    let mut by_status: HashMap<InflightActivationStatus, Vec<String>> = HashMap::new();

    for (id, status) in updates {
        by_status.entry(status).or_default().push(id);
    }

    for (status, ids) in by_status {
        let count = ids.len() as u64;

        let result = if status == InflightActivationStatus::Complete {
            let start = Instant::now();
            let result = store.delete_activation_batch(&ids).await;

            metrics::histogram!("status_flush.delete_activation_batch.duration")
                .record(start.elapsed());

            if result.is_err() {
                metrics::counter!("status_flush.delete_activation_batch.error").increment(1);
            }

            result
        } else {
            let start = Instant::now();
            let result = store
                .set_status_batch(&ids, status)
                .await
                .map(|()| ids.len() as u64);

            metrics::histogram!("status_flush.set_status_batch.duration").record(start.elapsed());

            if result.is_err() {
                metrics::counter!("status_flush.set_status_batch.error").increment(1);
            }

            result
        };

        if let Err(e) = result {
            error!(
                ?status,
                ?count,
                error = ?e,
                "Failed to flush status batch"
            );

            // Push failed updates back into the buffer so they can be retried on next flush
            for id in ids {
                buffer.push((id, status));
            }
        } else {
            metrics::counter!("status_flush.activations", "status" => status.to_string())
                .increment(count);

            debug!(?status, ?count, "Flushed status batch");
        }
    }

    metrics::histogram!("status_flush.flush.duration").record(start.elapsed());
}
