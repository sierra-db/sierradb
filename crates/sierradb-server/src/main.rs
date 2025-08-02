use std::time::Duration;

use clap::Parser;
use kameo::Actor;
use libp2p::identity::Keypair;
use sierradb::database::DatabaseBuilder;
use sierradb_cluster::{ClusterActor, ClusterArgs};
use sierradb_server::config::{AppConfig, Args};
use sierradb_server::server::Server;
use tracing::{debug, error};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    tracing_subscriber::fmt::SubscriberBuilder::default()
        .without_time()
        .with_target(false)
        .with_env_filter(EnvFilter::new(args.log.as_deref().unwrap_or(
            "sierradb_cluster=DEBUG,sierradb_server=DEBUG,sierradb=DEBUG,INFO",
        )))
        // .with_span_events(FmtSpan::ENTER)
        .init();

    let config = AppConfig::load(args)?;
    debug!("Configuration:\n{config}");
    let errs = config.validate()?;
    let has_errs = !errs.is_empty();
    for err in errs {
        error!("config error: {err}");
    }
    if has_errs {
        std::process::exit(1);
    }

    let assigned_buckets = config.assigned_buckets()?;
    let assigned_partitions = config.assigned_partitions(&assigned_buckets);

    let mut builder = DatabaseBuilder::new();
    builder
        .segment_size(config.segment.size_bytes)
        .total_buckets(config.bucket.count)
        .bucket_ids(assigned_buckets.into_iter().collect::<Vec<_>>())
        .flush_interval_duration(Duration::from_millis(config.flush.interval_ms))
        .flush_interval_events(config.flush.events_threshold);

    if let Some(count) = config.threads.read {
        builder.reader_threads(count);
    }
    if let Some(count) = config.threads.write {
        builder.writer_threads(count);
    }

    let node_count = config.node_count()?;
    let database = builder.open(config.dir)?;

    let keypair = Keypair::generate_ed25519();

    let listen_addrs = if config.network.cluster_enabled {
        vec![config.network.cluster_address]
    } else {
        vec![]
    };

    let swarm_ref = ClusterActor::spawn(ClusterArgs {
        keypair,
        database,
        listen_addrs,
        node_count,
        node_index: config.node.index as usize,
        bucket_count: config.bucket.count,
        partition_count: config.partition.count,
        replication_factor: config.replication.factor,
        assigned_partitions,
        heartbeat_timeout: Duration::from_millis(config.heartbeat.timeout_ms),
        heartbeat_interval: Duration::from_millis(config.heartbeat.interval_ms),
        replication_buffer_size: config.replication.buffer_size,
        replication_buffer_timeout: Duration::from_millis(config.replication.buffer_timeout_ms),
        replication_catchup_timeout: Duration::from_millis(config.replication.catchup_timeout_ms),
    });

    Server::new(swarm_ref, config.partition.count)
        .listen(config.network.client_address)
        .await?;

    Ok(())
}
