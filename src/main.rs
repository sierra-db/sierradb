use eventus::{
    actor::Flush,
    cli::init_args,
    server::{eventstore::event_store_server::EventStoreServer, DefaultEventStoreServer},
    EventLog, LogOptions,
};
use tokio::signal;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = init_args();

    // console_subscriber::init();
    tracing_subscriber::fmt()
        .without_time()
        .with_target(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let opts = LogOptions::new(".log");
    let log = EventLog::new(opts).unwrap();

    let log_actor = kameo::actor::spawn_unsync_in_thread(log);

    // Create an instance of your EventStore implementation
    let event_store = DefaultEventStoreServer::new(log_actor.clone());

    // Start the gRPC server
    // tokio::task::Builder::new()
    //     .name("grpc_server")
    tokio::spawn(async move {
        info!("listening on {}", args.addr);
        Server::builder()
            .add_service(EventStoreServer::new(event_store))
            .serve(args.addr)
            .await
    });

    signal::ctrl_c().await.expect("failed to listen for event");
    info!("shutting down...");

    log_actor.tell(Flush).send().await?;
    log_actor.stop_gracefully().await?;
    log_actor.wait_for_stop().await;

    info!("goodbye.");

    Ok(())
}
