use std::sync::Arc;
use std::time::{Duration, Instant};

use sentry_protos::taskbroker::v1::worker_service_client::WorkerServiceClient;
use sentry_protos::taskbroker::v1::{PushTaskRequest, TaskActivation};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use prost::Message;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::sleep;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::store::inflight_activation::{InflightActivation, InflightActivationStore};

/// This data structure fetches pending activations from the store and pushes them to the worker service. Each dispatcher has...
/// - One "fetch" loop that gets a pending activation from the store, sends it to a push channel, and repeats
/// - One or more "push" loops, each of which receives an activation from a channel, pushes that activation to a worker, and repeats
pub struct TaskDispatcher {
    /// Sender for every push loop.
    senders: Vec<Sender<InflightActivation>>,

    /// Receiver for every push loop.
    receivers: Vec<Receiver<InflightActivation>>,

    /// For every pending activation, increment and send to the channel with this index.
    next_sender_idx: usize,

    /// Broker configuration.
    config: Arc<Config>,

    /// Broker inflight activation store.
    store: Arc<dyn InflightActivationStore>,
}

impl TaskDispatcher {
    /// Create a new task dispatcher.
    pub fn new(config: Arc<Config>, store: Arc<dyn InflightActivationStore>) -> Self {
        let n = config.pushers;

        let mut senders = Vec::with_capacity(n);
        let mut receivers = Vec::with_capacity(n);
        let next_sender_idx = 0;

        for _ in 0..n {
            let (tx, rx) = mpsc::channel(config.push_queue_size);
            senders.push(tx);
            receivers.push(rx);
        }

        Self {
            senders,
            receivers,
            next_sender_idx,
            config,
            store,
        }
    }

    /// Initialize push loops and dispatcher loop.
    pub async fn start(mut self) -> Result<()> {
        let n = self.senders.len();
        info!("Starting {n} push loops...");

        let endpoint = self.config.worker_endpoint.clone();
        let receivers = std::mem::take(&mut self.receivers);

        // Initialize each push loop
        for mut rx in receivers.into_iter() {
            let endpoint = endpoint.clone();

            tokio::spawn(async move {
                let mut worker = match WorkerServiceClient::connect(endpoint).await {
                    Ok(w) => w,

                    Err(e) => {
                        error!("Failed to connect to worker - {:?}", e);
                        return;
                    }
                };

                while let Some(activation) = rx.recv().await {
                    // Receive activation from the channel
                    let id = activation.id.clone();

                    // Try to push activation to the worker service
                    if let Err(e) = push_task(&mut worker, activation).await {
                        error!("Pushing activation {id} resulted in error - {:?}", e);
                    } else {
                        debug!("Activation {id} was sent to worker!");
                    }
                }
            });
        }

        info!("Starting fetch loop...");
        let guard = get_shutdown_guard().shutdown_on_drop();

        // Initialize the fetch loop
        loop {
            tokio::select! {
                _ = guard.wait() => {
                    info!("Fetch loop received shutdown signal");
                    break;
                }

                _ = async {
                    debug!("About to fetch next activation...");
                    self.fetch_activation().await;
                } => {}
            }
        }

        info!("Activation dispatcher shutting down...");
        Ok(())
    }

    /// Grab the next pending activation from the store, mark it as processing, and send to push channel.
    async fn fetch_activation(&mut self) {
        let start = Instant::now();
        metrics::counter!("pusher.fetch_activation.runs").increment(1);

        debug!("Fetching next pending activation...");

        match self.store.get_pending_activation(None, None).await {
            Ok(Some(activation)) => {
                let id = activation.id.clone();

                let idx = self.next_sender_idx % self.senders.len();
                self.next_sender_idx = self.next_sender_idx.wrapping_add(1);

                if let Err(e) = self.senders[idx].send(activation).await {
                    error!("Failed to send activation {id} to worker - {:?}", e);
                }

                metrics::histogram!("pusher.fetch_activation.duration").record(start.elapsed());
            }

            Ok(_) => {
                debug!("No pending activations, sleeping briefly...");
                sleep(milliseconds(100)).await;

                metrics::histogram!("pusher.fetch_activation.duration").record(start.elapsed());
            }

            Err(e) => {
                error!("Failed to fetch pending activations - {:?}", e);
                sleep(milliseconds(100)).await;

                metrics::histogram!("pusher.fetch_activation.duration").record(start.elapsed());
            }
        }
    }
}

/// Decode task activation and push it to a worker.
async fn push_task(
    worker: &mut WorkerServiceClient<Channel>,
    activation: InflightActivation,
) -> Result<()> {
    let start = Instant::now();
    let id = activation.id.clone();

    // Try to decode activation (if it fails, we will see the error where `push_task` is called)
    let task = TaskActivation::decode(&activation.activation as &[u8])?;

    let request = PushTaskRequest { task: Some(task) };

    let result = match worker.push_task(request).await {
        Ok(_) => {
            debug!("Successfully sent activation {id} to worker service!");
            Ok(())
        }

        Err(e) => {
            error!("Could not push activation {id} to worker service - {:?}", e);
            Err(e.into())
        }
    };

    metrics::histogram!("pusher.push_task.duration").record(start.elapsed());
    result
}

#[inline]
fn milliseconds(i: u64) -> Duration {
    Duration::from_millis(i)
}
