use tonic::transport::Server;

use anyhow::Error;
use inflight_task_store::InflightTaskStore;
use grpc_server::MyConsumerService;
use sentry_protos::sentry::v1::consumer_service_server::ConsumerServiceServer;

mod config;
#[allow(dead_code)]
mod inflight_task_store;
mod grpc_server;

#[tokio::main]
async fn main() -> Result<(), Error> {
    InflightTaskStore::new("hello_world.sqlite").await?;

    let addr = "[::1]:50051".parse()?;
    let service = MyConsumerService::default();

    Server::builder()
        .add_service(ConsumerServiceServer::new(service))
        .serve(addr)
        .await?;


    Ok(())
}
