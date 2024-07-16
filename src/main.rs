use eventus::{
    actor::Flush,
    cli::init_args,
    server::{
        eventstore::event_store_server::EventStoreServer, DefaultEventStoreServer,
        ServerAuthInterceptor,
    },
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
        .with_env_filter(EnvFilter::builder().parse_lossy(&args.log))
        .init();

    let opts = LogOptions::new(&args.log_path);
    let log = EventLog::new(opts)?;

    let log_actor = kameo::actor::spawn_unsync_in_thread(log);

    // Create an instance of your EventStore implementation
    let event_store = DefaultEventStoreServer::new(log_actor.clone());

    // Start the gRPC server
    let interceptor = ServerAuthInterceptor::new(&args.auth_token)?;
    // tokio::task::Builder::new()
    //     .name("grpc_server")
    tokio::spawn(async move {
        info!("listening on {}", args.listen_addr);
        Server::builder()
            .add_service(EventStoreServer::with_interceptor(event_store, interceptor))
            .serve(args.listen_addr)
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
