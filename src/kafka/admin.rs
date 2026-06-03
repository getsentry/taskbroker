use anyhow::{Error, anyhow};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::types::RDKafkaErrorCode;
use tracing::info;

/// Create the given topics on the cluster described by `admin_config` if they
/// don't already exist. `topics` pairs each topic name with its partition count.
///
/// `admin_config` should carry only `bootstrap.servers` + auth (see
/// [`Config::kafka_admin_config`](crate::config::Config::kafka_admin_config)) so
/// the topics are created on the right cluster. A pre-existing topic
/// (`TopicAlreadyExists`) is treated as success; any other per-topic failure is
/// surfaced so a misconfigured or unreachable cluster fails loudly at startup.
pub async fn create_missing_topics(
    admin_config: ClientConfig,
    topics: &[(&str, i32)],
) -> Result<(), Error> {
    if topics.is_empty() {
        return Ok(());
    }

    let admin_client: AdminClient<_> = admin_config
        .create()
        .map_err(|e| anyhow!("Unable to create rdkafka admin client: {e}"))?;

    info!(
        "Creating topics {:?} if they do not already exist",
        topics
    );
    let new_topics: Vec<NewTopic> = topics
        .iter()
        .map(|(name, partitions)| NewTopic::new(name, *partitions, TopicReplication::Fixed(1)))
        .collect();

    let results = admin_client
        .create_topics(&new_topics, &AdminOptions::new())
        .await?;

    // `create_topics` returns one result per topic; a request-level error is
    // already propagated above by `?`. Tolerate topics that already exist, but
    // surface every other per-topic failure (auth denied, invalid config, ...).
    for result in results {
        match result {
            Ok(_) => {}
            Err((topic, RDKafkaErrorCode::TopicAlreadyExists)) => {
                info!("Topic {:?} already exists, skipping", topic);
            }
            Err((topic, code)) => {
                return Err(anyhow!("Failed to create topic {:?}: {}", topic, code));
            }
        }
    }

    Ok(())
}
