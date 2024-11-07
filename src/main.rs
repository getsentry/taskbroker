use anyhow::Error;
use inflight_task_store::InflightTaskStore;

#[allow(dead_code)]
mod inflight_task_store;
mod config;

#[tokio::main]
async fn main() -> Result<(), Error> {
    InflightTaskStore::new("hello_world.sqlite").await?;
    Ok(())
}
