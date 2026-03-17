use std::sync::Arc;
use std::time::{Duration, Instant};

use sentry_protos::taskbroker::v1::worker_service_client::WorkerServiceClient;
use sentry_protos::taskbroker::v1::{PushTaskRequest, TaskActivation};

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use prost::Message;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
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

    /// Number of senders (and receivers) for testing purposes.
    #[cfg(test)]
    pub fn pusher_count(&self) -> usize {
        self.senders.len()
    }

    /// Take the receivers so a test can drain them.
    #[cfg(test)]
    pub fn take_receivers(&mut self) -> Vec<Receiver<InflightActivation>> {
        std::mem::take(&mut self.receivers)
    }

    /// Initialize push loops and dispatcher loop.
    pub async fn start(mut self) -> Result<()> {
        let n = self.senders.len();
        info!("Starting {n} push loops...");

        let endpoint = self.config.worker_endpoint.clone();
        let receivers = std::mem::take(&mut self.receivers);

        // Collect pusher handles so we can wait on them if shutdown is initiated
        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(receivers.len());

        // Initialize each push loop
        for mut rx in receivers.into_iter() {
            let endpoint = endpoint.clone();

            let handle = tokio::spawn(async move {
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

            handles.push(handle);
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

        // Close channels and drain any tasks still in the pushing pipeline
        drop(std::mem::take(&mut self.senders));
        for handle in handles {
            let _ = handle.await;
        }

        info!("Activation dispatcher shut down.");
        Ok(())
    }

    /// Grab the next pending activation from the store, mark it as processing, and send to push channel.
    pub async fn fetch_activation(&mut self) {
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::config::Config;
    use crate::store::inflight_activation::{
        FailedTasksForwarder, InflightActivation, InflightActivationStatus,
        InflightActivationStore, QueryResult,
    };
    use crate::test_utils::{create_test_store, make_activations};

    use anyhow::Error;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};

    use super::TaskDispatcher;

    /// Mock store that returns activations from a queue for `get_pending_activation`.
    struct MockStore {
        activations: Mutex<Vec<InflightActivation>>,
    }

    impl MockStore {
        fn new(activations: Vec<InflightActivation>) -> Arc<Self> {
            Arc::new(Self {
                activations: Mutex::new(activations),
            })
        }
    }

    #[async_trait]
    impl InflightActivationStore for MockStore {
        async fn vacuum_db(&self) -> Result<(), Error> {
            unimplemented!()
        }

        async fn full_vacuum_db(&self) -> Result<(), Error> {
            unimplemented!()
        }

        async fn db_size(&self) -> Result<u64, Error> {
            unimplemented!()
        }

        async fn get_by_id(&self, _id: &str) -> Result<Option<InflightActivation>, Error> {
            unimplemented!()
        }

        async fn store(&self, _batch: Vec<InflightActivation>) -> Result<QueryResult, Error> {
            unimplemented!()
        }

        async fn get_pending_activations_from_namespaces(
            &self,
            _application: Option<&str>,
            _namespaces: Option<&[String]>,
            limit: Option<i32>,
        ) -> Result<Vec<InflightActivation>, Error> {
            let limit = limit.unwrap_or(1) as usize;
            let mut list = self.activations.lock().unwrap();
            let n = limit.min(list.len());

            if n == 0 {
                return Ok(vec![]);
            }

            Ok(list.drain(..n).collect())
        }

        async fn pending_activation_max_lag(&self, _now: &DateTime<Utc>) -> f64 {
            unimplemented!()
        }

        async fn count_by_status(&self, _status: InflightActivationStatus) -> Result<usize, Error> {
            Ok(self.activations.lock().unwrap().len())
        }

        async fn count(&self) -> Result<usize, Error> {
            Ok(self.activations.lock().unwrap().len())
        }

        async fn set_status(
            &self,
            _id: &str,
            _status: InflightActivationStatus,
        ) -> Result<Option<InflightActivation>, Error> {
            unimplemented!()
        }

        async fn set_processing_deadline(
            &self,
            _id: &str,
            _deadline: Option<DateTime<Utc>>,
        ) -> Result<(), Error> {
            unimplemented!()
        }

        async fn delete_activation(&self, _id: &str) -> Result<(), Error> {
            unimplemented!()
        }

        async fn get_retry_activations(&self) -> Result<Vec<InflightActivation>, Error> {
            unimplemented!()
        }

        async fn clear(&self) -> Result<(), Error> {
            unimplemented!()
        }

        async fn handle_processing_deadline(&self) -> Result<u64, Error> {
            unimplemented!()
        }

        async fn handle_processing_attempts(&self) -> Result<u64, Error> {
            unimplemented!()
        }

        async fn handle_expires_at(&self) -> Result<u64, Error> {
            unimplemented!()
        }

        async fn handle_delay_until(&self) -> Result<u64, Error> {
            unimplemented!()
        }

        async fn handle_failed_tasks(&self) -> Result<FailedTasksForwarder, Error> {
            unimplemented!()
        }

        async fn mark_completed(&self, _ids: Vec<String>) -> Result<u64, Error> {
            unimplemented!()
        }

        async fn remove_completed(&self) -> Result<u64, Error> {
            unimplemented!()
        }

        async fn remove_killswitched(
            &self,
            _killswitched_tasks: Vec<String>,
        ) -> Result<u64, Error> {
            unimplemented!()
        }
    }

    /// Asserts that a dispatcher built with X pushers has exactly X senders (and thus X receivers).
    #[test]
    fn pushers_x_creates_x_senders_and_receivers() {
        // Use an empty mock store because we only care about construction, not fetching
        let store: Arc<dyn InflightActivationStore> = MockStore::new(vec![]);

        let config = Arc::new(Config {
            pushers: 5,
            push_queue_size: 10,
            ..Config::default()
        });

        let dispatcher = TaskDispatcher::new(config, store);

        // One sender (and one receiver) per pusher
        assert_eq!(dispatcher.pusher_count(), 5);
    }

    /// Asserts that the fetch loop distributes activations round-robin across channels (0, 1, 2, 0, 1, 2, ...)
    #[tokio::test]
    async fn round_robin_sends_to_channels_0_1_2_0_1_2() {
        // Six activations (id_0 .. id_5) so we get two full cycles across three channels
        let activations = make_activations(6);
        let store = MockStore::new(activations);

        let config = Arc::new(Config {
            pushers: 3,
            push_queue_size: 10,
            ..Config::default()
        });

        let mut dispatcher = TaskDispatcher::new(config, store);

        // Take receivers so we can drain them - dispatcher keeps senders and will push to them
        let mut receivers = dispatcher.take_receivers();
        assert_eq!(receivers.len(), 3);

        // Run the fetch loop six times - each run takes one activation from the mock and sends to next channel
        for _ in 0..6 {
            dispatcher.fetch_activation().await;
        }

        // Receive in the same order the dispatcher sends - channel 0, then 1, then 2, then 0, 1, 2
        let mut received_by_channel: Vec<Vec<String>> = vec![vec![], vec![], vec![]];
        for i in 0..6 {
            let idx = i % 3;
            let activation = receivers[idx].recv().await.expect("activation");
            received_by_channel[idx].push(activation.id.clone());
        }

        // Make sure round-robin works as intended...
        // - Activations 1 and 4 go to channel 0
        // - Activations 2 and 5 go to channel 1
        // - Activations 3 and 6 go to channel 2
        assert_eq!(received_by_channel[0], &["id_0", "id_3"]);
        assert_eq!(received_by_channel[1], &["id_1", "id_4"]);
        assert_eq!(received_by_channel[2], &["id_2", "id_5"]);
    }

    /// Asserts that after N fetch steps the store has zero pending activations (each fetch marks one as processing).
    #[tokio::test]
    async fn fetch_loop_drains_store() {
        let activations = make_activations(3);
        let store = create_test_store("sqlite").await;

        // Add activations to test store
        store.store(activations).await.unwrap();
        assert_eq!(store.count_pending_activations().await.unwrap(), 3);

        let config = Arc::new(Config {
            pushers: 2,
            push_queue_size: 10,
            ..Config::default()
        });

        let mut dispatcher = TaskDispatcher::new(config, store.clone());
        let mut receivers = dispatcher.take_receivers();

        // Run fetch three times - each call gets one pending activation and moves it to processing
        for _ in 0..3 {
            dispatcher.fetch_activation().await;
        }

        // Drain all activations from the channels so we've fully consumed what was fetched
        let mut received = 0;

        for mut rx in receivers.drain(..) {
            while rx.try_recv().is_ok() {
                received += 1;
            }
        }

        // Have all activations been received?
        assert_eq!(received, 3);

        // Real store marks as processing on `get_pending_activation` - so no pending left
        assert_eq!(store.count_pending_activations().await.unwrap(), 0);
    }
}
