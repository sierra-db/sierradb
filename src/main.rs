use std::time::Duration;

use commitlog::{
    actor::AppendToStream,
    server::{eventstore::event_store_server::EventStoreServer, DefaultEventStoreServer},
    CommitLog, ExpectedVersion, LogOptions, NewEvent,
};
use tokio::signal;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Define the address and port for the server to listen on
    let addr = "[::1]:50051".parse()?;

    let opts = LogOptions::new(".log");
    let log = CommitLog::new(opts).await.unwrap();

    let log_actor = kameo::actor::spawn_unsync(log);
    // tokio::spawn({
    //     let log_actor = log_actor.clone();
    //     async move {
    //         loop {
    //             tokio::time::sleep(Duration::from_millis(500)).await;
    //             log_actor
    //                 .send(AppendToStream {
    //                     stream_id: "user-123".to_string(),
    //                     expected_version: ExpectedVersion::Any,
    //                     events: vec![NewEvent {
    //                         event_name: "UserClicked".into(),
    //                         event_data: vec![].into(),
    //                         metadata: vec![].into(),
    //                     }],
    //                 })
    //                 .await
    //                 .unwrap();
    //         }
    //     }
    // });

    // Create an instance of your EventStore implementation
    let event_store = DefaultEventStoreServer::new(log_actor.clone());

    // Start the gRPC server
    tokio::spawn(async move {
        Server::builder()
            .add_service(EventStoreServer::new(event_store))
            .serve(addr)
            .await
    });

    signal::ctrl_c().await.expect("failed to listen for event");
    println!("shutting down...");

    log_actor.stop_gracefully()?;
    log_actor.wait_for_stop().await;

    println!("goodbye.");

    Ok(())
}
