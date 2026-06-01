use std::cmp::max;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use chrono::Utc;
use elegant_departure::get_shutdown_guard;
use flume::Receiver;
use tracing::{debug, error};

use crate::store::activation::{Activation, ActivationStatus};
use crate::store::traits::ActivationStore;
use crate::timed;
use crate::worker::WorkerMap;

/// Alias for ergonomics.
pub type Submission = (Activation, Instant);

/// Abstraction for a single push thread.
pub struct PushThread {
    /// Maps every application to its worker service.
    pub(super) workers: WorkerMap,

    /// Channel containing claimed activations to be pushed.
    pub(super) receiver: Receiver<Submission>,

    /// Entity that marks tasks as processing.
    pub(super) store: Arc<dyn ActivationStore>,
}

impl PushThread {
    pub async fn start(&mut self) -> Result<()> {
        let guard = get_shutdown_guard().shutdown_on_drop();

        loop {
            let (activation, time) = match self.receiver.recv_async().await {
                // Received activation from fetch thread
                Ok(a) => a,

                // We only exit when the push queue is closed, which happens when the fetch pool has shut down
                Err(_) => break,
            };

            metrics::histogram!("push.queue.latency").record(time.elapsed());

            // Push the task and mark it as processing
            self.push_task(activation).await;
        }

        drop(guard);
        Ok(())
    }

    async fn push_task(&mut self, activation: Activation) {
        // Store the ID for later since `push_task` claims ownership over `activation`
        let id = activation.id.clone();

        // First, determine the correct worker service
        let Some(worker) = self.workers.get_mut(&activation.application) else {
            metrics::counter!("push.missing_worker_mapping", "application" => activation.application.clone()).increment(1);

            error!(
                task_id = %id,
                application = activation.application,
                "Application has no worker mapping"
            );

            return;
        };

        // Then, push the task to that service
        let result = timed!(
            worker.push_task(activation.clone()),
            "worker.push_task.duration"
        );

        if let Err(e) = result {
            metrics::counter!("worker.push_task", "result" => "error").increment(1);

            error!(
                task_id = %id,
                error = ?e,
                "Failed to send activation to worker: {e}"
            );

            // Revert claimed task back to pending
            if let Err(e) = self
                .store
                .set_status(&id, ActivationStatus::Pending, None, None)
                .await
            {
                metrics::counter!("push.undo_claim", "result" => "error").increment(1);

                error!(
                    task_id = %id,
                    error = ?e,
                    "Failed to undo claim on failed send"
                );
            }

            metrics::counter!("push.undo_claim", "result" => "ok").increment(1);

            return;
        }

        // If we have reached this line, the task was pushed successfully
        metrics::counter!("worker.push_task", "result" => "ok").increment(1);
        debug!(task_id = %id, "Activation sent to worker");

        if activation.processing_attempts < 1 {
            let latency = max(0, activation.received_latency(Utc::now()));

            metrics::histogram!(
                "push.received_to_push.latency",
                "namespace" => activation.namespace,
                "taskname" => activation.taskname,
            )
            .record(latency as f64);
        } else {
            debug!(task_id = %id, namespace = activation.namespace, taskname = activation.taskname, "Activation already processed, skipping received → push latency recording");
        }

        // Finally, mark the activation as processing
        let result = timed!(
            self.store.mark_activation_processing(&id),
            "push.mark_activation_processing.duration"
        );

        if let Err(e) = result {
            metrics::counter!("push.mark_activation_processing", "result" => "error").increment(1);

            error!(
                task_id = %id,
                error = ?e,
                "Failed to mark activation as sent after push"
            );
        }
    }
}
