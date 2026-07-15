use std::collections::BTreeMap;

use anyhow::{Context, Error, anyhow};
use chrono::Utc;
use prost::Message as _;
use sentry_protos::taskbroker::v1::{OnAttemptsExceeded, TaskActivation};
use serde::Serialize;
use tracing::info;
use uuid::Uuid;

use crate::config::Config;
use crate::kafka::deserialize_activation::bucket_from_id;
use crate::store::activation::{Activation, ActivationStatus};
use crate::store::traits::ActivationStore;

const CANARY_NAMESPACE: &str = "internal";
const CANARY_TASKNAME: &str = "canary_task";
const CANARY_PROCESSING_DEADLINE_SECONDS: i32 = 10;

#[derive(Serialize)]
struct CanaryParameters {
    args: Vec<()>,
    kwargs: BTreeMap<String, ()>,
}

/// Add the configured number of canary tasks to the activation store for every
/// application in `worker_map`.
pub async fn enqueue(config: &Config, store: &dyn ActivationStore) -> Result<(), Error> {
    if config.canary_tasks == 0 || config.worker_map.is_empty() {
        return Ok(());
    }

    let topic = config
        .consumable_topics()
        .map_err(|error| anyhow!(error.to_string()))?
        .first()
        .map(|(name, _)| *name)
        .ok_or_else(|| anyhow!("No consumable topic configured"))?;

    let activations = build_activations(config, topic)?;
    let batch_size = config.store.insert_batch_max_length.max(1);
    let mut stored = 0;

    for batch in activations.chunks(batch_size) {
        stored += store.store(batch).await?;
    }

    if stored > 0 {
        info!(
            count = stored,
            worker_pools = config.worker_map.len(),
            "Stored startup canary tasks"
        );
    }

    Ok(())
}

fn build_activations(config: &Config, topic: &str) -> Result<Vec<Activation>, Error> {
    let parameters_bytes = rmp_serde::to_vec_named(&CanaryParameters {
        args: Vec::new(),
        kwargs: BTreeMap::new(),
    })
    .context("Failed to serialize canary task parameters")?;

    let mut activations = Vec::new();

    for application in config.worker_map.keys() {
        for offset in 0..config.canary_tasks {
            let now = Utc::now();
            let id = Uuid::new_v4().to_string();
            let task_activation = TaskActivation {
                id: id.clone(),
                application: Some(application.clone()),
                namespace: CANARY_NAMESPACE.to_owned(),
                taskname: CANARY_TASKNAME.to_owned(),
                parameters_bytes: parameters_bytes.clone(),
                processing_deadline_duration: CANARY_PROCESSING_DEADLINE_SECONDS as u64,
                received_at: Some(prost_types::Timestamp {
                    seconds: now.timestamp(),
                    nanos: now.timestamp_subsec_nanos() as i32,
                }),
                ..Default::default()
            };

            activations.push(Activation {
                id: id.clone(),
                application: application.clone(),
                namespace: CANARY_NAMESPACE.to_owned(),
                taskname: CANARY_TASKNAME.to_owned(),
                activation: task_activation.encode_to_vec(),
                status: ActivationStatus::Pending,
                topic: topic.to_owned(),
                partition: 0,
                offset: offset.into(),
                added_at: now,
                received_at: now,
                processing_attempts: 0,
                processing_deadline_duration: CANARY_PROCESSING_DEADLINE_SECONDS,
                expires_at: None,
                delay_until: None,
                processing_deadline: None,
                claim_expires_at: None,
                on_attempts_exceeded: OnAttemptsExceeded::Discard,
                at_most_once: false,
                bucket: bucket_from_id(&id),
            });
        }
    }

    Ok(activations)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use prost::Message as _;
    use sentry_protos::taskbroker::v1::TaskActivation;
    use serde::Deserialize;

    use super::{CANARY_NAMESPACE, CANARY_TASKNAME, build_activations};
    use crate::config::Config;

    #[derive(Debug, Deserialize, PartialEq)]
    struct CanaryParameters {
        args: Vec<()>,
        kwargs: BTreeMap<String, ()>,
    }

    #[test]
    fn builds_configured_number_of_canaries_per_worker_pool() {
        let config = Config {
            canary_tasks: 3,
            worker_map: BTreeMap::from([
                ("launchpad".to_owned(), "http://launchpad".to_owned()),
                ("sentry".to_owned(), "http://sentry".to_owned()),
            ]),
            ..Default::default()
        };

        let activations = build_activations(&config, "taskworker").unwrap();
        assert_eq!(activations.len(), 6);

        let mut counts = HashMap::new();
        for activation in activations {
            *counts.entry(activation.application.clone()).or_insert(0) += 1;
            assert_eq!(activation.namespace, CANARY_NAMESPACE);
            assert_eq!(activation.taskname, CANARY_TASKNAME);
            assert_eq!(activation.topic, "taskworker");
            assert_eq!(activation.processing_deadline_duration, 10);

            let task = TaskActivation::decode(activation.activation.as_slice()).unwrap();
            assert_eq!(task.id, activation.id);
            assert_eq!(
                task.application.as_deref(),
                Some(activation.application.as_str())
            );
            assert_eq!(task.namespace, CANARY_NAMESPACE);
            assert_eq!(task.taskname, CANARY_TASKNAME);
            assert_eq!(
                rmp_serde::from_slice::<CanaryParameters>(&task.parameters_bytes).unwrap(),
                CanaryParameters {
                    args: Vec::new(),
                    kwargs: BTreeMap::new(),
                }
            );
        }

        assert_eq!(counts.get("launchpad"), Some(&3));
        assert_eq!(counts.get("sentry"), Some(&3));
    }

    #[test]
    fn builds_no_canaries_when_disabled() {
        let config = Config::default();
        assert!(build_activations(&config, "taskworker").unwrap().is_empty());
    }
}
