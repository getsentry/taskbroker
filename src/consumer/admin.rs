use anyhow::Error;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    metadata::Metadata,
    ClientConfig,
};
use std::time::Duration;
use tracing::info;

pub async fn create_missing_topics(
    kafka_client_config: ClientConfig,
    topics: &[&str],
    default_topic_partitions: i32,
) -> Result<(), Error> {
    let admin_client: AdminClient<_> = kafka_client_config.create().unwrap();
    let mut topics_to_create = Vec::new();
    for topic in topics {
        // Weirdly, this call will actually create the topic if it doesn't exist. So it needs to be overridden in the next steps.
        let topic_metadata: Metadata = admin_client
            .inner()
            .fetch_metadata(Some(topic), Duration::new(5, 0))?;

        if !topic_metadata.topics().is_empty() {
            for meta_topic in topic_metadata.topics() {
                if meta_topic.error().is_some()
                    && meta_topic.partitions().len() as i32 != default_topic_partitions
                {
                    info!(
                        "Creating topic: {} with partitions: {}",
                        meta_topic.name(),
                        default_topic_partitions
                    );
                    topics_to_create.push(NewTopic::new(
                        topic,
                        default_topic_partitions,
                        TopicReplication::Fixed(1),
                    ));
                } else {
                    info!(
                        "Using topic: {}, partitions: {}, error: {:?}",
                        meta_topic.name(),
                        meta_topic.partitions().len(),
                        meta_topic.error()
                    );
                }
            }
        }
    }
    if !topics_to_create.is_empty() {
        admin_client
            .create_topics(&topics_to_create, &AdminOptions::new())
            .await?;
    }

    Ok(())
}
