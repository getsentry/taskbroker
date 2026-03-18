use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use elegant_departure::get_shutdown_guard;
use flume::{Receiver, Sender};
use prost::Message;
use sentry_protos::taskbroker::v1::worker_service_client::WorkerServiceClient;
use sentry_protos::taskbroker::v1::{PushTaskRequest, TaskActivation};
use tonic::async_trait;
use tonic::transport::Channel;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::store::inflight_activation::InflightActivation;

/// Thin interface for the worker client. It mostly serves to enable proper unit testing, but it also decouples the actual client implementation from our pushing logic.
#[async_trait]
trait WorkerClient {
    /// Send a single `PushTaskRequest` to the worker service.
    async fn send(&mut self, request: PushTaskRequest) -> Result<()>;
}

#[async_trait]
impl WorkerClient for WorkerServiceClient<Channel> {
    async fn send(&mut self, request: PushTaskRequest) -> Result<()> {
        self.push_task(request).await?;
        Ok(())
    }
}

/// Wrapper around `config.push_threads` asynchronous tasks, each of which receives an activation from the channel, sends it to the worker service, and repeats.
pub struct PushPool {
    /// The sending end of a channel that accepts task activations.
    sender: Sender<InflightActivation>,

    /// The receiving end of a channel that accepts task activations.
    receiver: Receiver<InflightActivation>,

    /// Taskbroker configuration.
    config: Arc<Config>,
}

impl PushPool {
    /// Initialize a new push pool.
    pub fn new(config: Arc<Config>) -> Self {
        let (sender, receiver) = flume::bounded(config.push_queue_size);

        Self {
            sender,
            receiver,
            config,
        }
    }

    /// Spawn `config.push_threads` asynchronous tasks, each of which repeatedly moves pending activations from the channel to the worker service until the shutdown signal is received.
    pub async fn start(&self) -> Result<()> {
        let mut handles = vec![];

        for _ in 0..self.config.push_threads {
            let endpoint = self.config.worker_endpoint.clone();

            let callback_url = format!(
                "{}:{}",
                self.config.callback_addr, self.config.callback_port
            );

            let receiver = self.receiver.clone();
            let guard = get_shutdown_guard().shutdown_on_drop();

            let handle = tokio::spawn(async move {
                let mut worker = match WorkerServiceClient::connect(endpoint).await {
                    Ok(w) => w,
                    Err(e) => {
                        error!("Failed to connect to worker - {:?}", e);
                        return;
                    }
                };

                loop {
                    tokio::select! {
                        _ = guard.wait() => {
                            info!("Push worker received shutdown signal");
                            break;
                        }

                        message = receiver.recv_async() => {
                            let activation = match message {
                                // Received activation from fetch thread
                                Ok(a) => a,

                                // Channel closed
                                Err(_) => break
                            };

                            let id = activation.id.clone();

                            match push_task(&mut worker, activation, callback_url.clone()).await {
                                Ok(_) => debug!("Activation {id} was sent to worker!"),
                                Err(e) => error!("Pushing activation {id} resulted in error - {:?}", e)
                            };
                        }
                    }
                }

                // Drain channel before exiting
                for activation in receiver.drain() {
                    let id = activation.id.clone();

                    match push_task(&mut worker, activation, callback_url.clone()).await {
                        Ok(_) => debug!("Activation {id} was sent to worker!"),
                        Err(e) => error!("Pushing activation {id} resulted in error - {:?}", e),
                    };
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            if let Err(e) = handle.await {
                return Err(e.into());
            }
        }

        Ok(())
    }

    /// Send an activation to the internal asynchronous MPMC channel used by all running push threads.
    pub async fn submit(&self, activation: InflightActivation) -> Result<()> {
        Ok(self.sender.send_async(activation).await?)
    }
}

/// Decode task activation and push it to a worker.
async fn push_task<W: WorkerClient + Send>(
    worker: &mut W,
    activation: InflightActivation,
    callback_url: String,
) -> Result<()> {
    let start = Instant::now();
    let id = activation.id.clone();

    // Try to decode activation (if it fails, we will see the error where `push_task` is called)
    let task = TaskActivation::decode(&activation.activation as &[u8])?;

    let request = PushTaskRequest {
        task: Some(task),
        callback_url,
    };

    let result = match worker.send(request).await {
        Ok(_) => {
            debug!("Successfully sent activation {id} to worker service!");
            Ok(())
        }

        Err(e) => {
            error!("Could not push activation {id} to worker service - {:?}", e);
            Err(e.into())
        }
    };

    metrics::histogram!("push.push_task.duration").record(start.elapsed());
    result
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::test_utils::make_activations;

    /// Fake worker client for unit testing.
    struct MockWorkerClient {
        /// Capture all received requests so we can assert things about them.
        captured_requests: Vec<PushTaskRequest>,

        /// Should requests to the worker client fail?
        should_fail: bool,
    }

    impl MockWorkerClient {
        fn new(should_fail: bool) -> Self {
            let captured_requests = vec![];

            Self {
                captured_requests,
                should_fail,
            }
        }
    }

    #[async_trait]
    impl WorkerClient for MockWorkerClient {
        async fn send(&mut self, request: PushTaskRequest) -> Result<()> {
            self.captured_requests.push(request);

            if self.should_fail {
                return Err(anyhow!("mock send failure"));
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn push_task_returns_ok_on_client_success() {
        let activation = make_activations(1).remove(0);
        let mut worker = MockWorkerClient::new(false);
        let callback_url = "taskbroker:50051".to_string();

        let result = push_task(&mut worker, activation.clone(), callback_url.clone()).await;
        assert!(result.is_ok(), "push_task should succeed");
        assert_eq!(worker.captured_requests.len(), 1);

        let request = &worker.captured_requests[0];
        assert_eq!(request.callback_url, callback_url);
        assert_eq!(
            request.task.as_ref().map(|task| task.id.as_str()),
            Some(activation.id.as_str())
        );
    }

    #[tokio::test]
    async fn push_task_returns_err_on_invalid_payload() {
        let mut activation = make_activations(1).remove(0);
        activation.activation = vec![1, 2, 3, 4];

        let mut worker = MockWorkerClient::new(false);
        let result = push_task(&mut worker, activation, "taskbroker:50051".to_string()).await;

        assert!(result.is_err(), "invalid payload should fail decoding");
        assert!(
            worker.captured_requests.is_empty(),
            "worker should not be called if decode fails"
        );
    }

    #[tokio::test]
    async fn push_task_propagates_client_error() {
        let activation = make_activations(1).remove(0);
        let mut worker = MockWorkerClient::new(true);

        let result = push_task(&mut worker, activation, "taskbroker:50051".to_string()).await;
        assert!(result.is_err(), "worker send errors should propagate");
        assert_eq!(worker.captured_requests.len(), 1);
    }

    #[tokio::test]
    async fn push_pool_submit_enqueues_item() {
        let config = Arc::new(Config {
            push_queue_size: 2,
            ..Config::default()
        });

        let pool = PushPool::new(config);
        let activation = make_activations(1).remove(0);

        let result = pool.submit(activation).await;
        assert!(result.is_ok(), "submit should enqueue activation");
    }

    #[tokio::test]
    async fn push_pool_submit_backpressures_when_queue_full() {
        let config = Arc::new(Config {
            push_queue_size: 1,
            ..Config::default()
        });

        let pool = PushPool::new(config);

        let first = make_activations(1).remove(0);
        let second = make_activations(1).remove(0);

        pool.submit(first)
            .await
            .expect("first submit should fill queue");

        let second_submit = timeout(Duration::from_millis(50), pool.submit(second)).await;
        assert!(
            second_submit.is_err(),
            "second submit should block when queue is full"
        );
    }
}
