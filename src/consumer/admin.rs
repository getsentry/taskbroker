use anyhow::Error;
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    ClientConfig,
};
use tracing::info;

pub async fn create_missing_topics(
    kafka_client_config: ClientConfig,
    topic: &str,
    default_topic_partitions: i32,
) -> Result<(), Error> {
    let admin_client: AdminClient<_> = kafka_client_config
        .create()
        .expect("Unable to reate rdkafka admin client");

    info!(
        "Creating topic {:?} with {} partitions if it does not already exists",
        topic, default_topic_partitions
    );
    admin_client
        .create_topics(
            &vec![NewTopic::new(
                topic,
                default_topic_partitions,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new(),
        )
        .await?;

    Ok(())
}
