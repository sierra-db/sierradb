use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use kameo::Actor;
use libp2p::Multiaddr;
use libp2p::identity::Keypair;
use sierradb::StreamId;
use sierradb::bucket::{BucketId, PartitionId};
use sierradb::database::{DatabaseBuilder, ExpectedVersion};
use sierradb::id::uuid_to_partition_id;
use sierradb::writer_thread_pool::{AppendEventsBatch, WriteEventRequest};
use sierradb_cluster::swarm_actor::Swarm;
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::{Uuid, uuid};

/// Distributed event store
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Database data directory
    #[arg(long, default_value = "target/db")]
    data_dir: PathBuf,

    /// Max segment size in bytes
    #[arg(long, default_value_t = 256_000_000)]
    segment_size: usize,

    /// Total number of buckets
    #[arg(long, default_value_t = 64)]
    total_buckets: u16,

    /// Number of worker threads for readers
    #[arg(long, default_value_t = 8)]
    read_pool_num_threads: u16,

    /// Number of worker threads for writers
    #[arg(long, default_value_t = 16)]
    writer_pool_num_threads: u16,

    /// Maximum time between flushes
    #[arg(long, default_value_t = u64::MAX)]
    flush_interval_ms: u64,

    /// Maximum events between flushes
    #[arg(long, default_value_t = 1)]
    flush_interval_events: u32,

    /// Number of partitions in the system
    #[arg(short = 'p', long, default_value_t = 1024)]
    partitions: u16,

    /// Replication factor
    #[arg(short = 'r', long, default_value_t = 3)]
    replication_factor: u8,

    /// Listen address
    #[arg(long, default_value = "/ip4/0.0.0.0/udp/0/quic-v1")]
    listen_address: Multiaddr,

    /// Node index
    #[arg(short = 'i', long, default_value_t = 0)]
    node_index: usize,

    /// Total number of nodes
    #[arg(short = 'n', long, default_value_t = 1)]
    num_nodes: usize,

    /// Explicit partition list (comma separated)
    #[arg(short = 'l', long)]
    partitions_list: Option<String>,

    /// Explicit bucket list (comma separated)
    #[arg(short = 'b', long)]
    buckets_list: Option<String>,

    /// Heartbeat interval in milliseconds
    #[arg(long, default_value_t = 1000)]
    heartbeat_interval_ms: u64,

    /// Heartbeat timeout in milliseconds
    #[arg(long, default_value_t = 6000)]
    heartbeat_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .without_time()
        .with_env_filter(EnvFilter::new("INFO"))
        .init();

    let args = Args::parse();

    let assigned_partitions =
        calculate_partition_assignment(args.num_nodes, args.node_index, args.partitions);
    let assigned_buckets =
        calculate_bucket_assignment(args.num_nodes, args.node_index, args.total_buckets);

    let db = DatabaseBuilder::new(args.data_dir)
        .segment_size(args.segment_size)
        .total_buckets(args.total_buckets)
        .bucket_ids(assigned_buckets.into_iter().collect::<Vec<_>>())
        .reader_pool_num_threads(args.read_pool_num_threads)
        .writer_pool_num_threads(args.writer_pool_num_threads)
        .flush_interval_duration(Duration::from_millis(args.flush_interval_ms))
        .flush_interval_events(args.flush_interval_events)
        .open()?;

    let local_key = Keypair::generate_ed25519();
    let local_peer_id = local_key.public().to_peer_id();
    info!("local peer id: {local_peer_id}");

    let mut swarm = Swarm::new(
        local_key,
        db,
        args.partitions,
        args.replication_factor,
        assigned_partitions.into_iter().collect(),
        Duration::from_millis(args.heartbeat_timeout_ms),
        Duration::from_millis(args.heartbeat_interval_ms),
    )?;

    swarm.listen_on(args.listen_address)?;

    let swarm_ref = Swarm::spawn(swarm);

    if args.node_index == 1 {
        tokio::time::sleep(Duration::from_secs(6)).await;
        let partition_key = uuid!("0d73f574-3d91-4491-a152-181bd04f9ab2"); // Uuid::new_v4();
        let res = swarm_ref
            .ask(AppendEventsBatch::single(WriteEventRequest {
                event_id: Uuid::new_v4(),
                partition_key,
                partition_id: uuid_to_partition_id(partition_key) % args.partitions,
                stream_id: StreamId::new("my-stream")?,
                stream_version: ExpectedVersion::Any,
                event_name: "HelloWorld".to_string(),
                timestamp: 0,
                metadata: vec![1, 2, 3],
                payload: b"hellow!".to_vec(),
            })?)
            .await?
            .await?;

        let _ = dbg!(res);
    }

    swarm_ref.wait_for_shutdown().await;

    Ok(())
}

fn calculate_partition_assignment(
    num_nodes: usize,
    node_index: usize,
    partitions: u16,
) -> HashSet<PartitionId> {
    // Assign partitions based on modulo of partition ID
    if num_nodes == 0 {
        info!("num_nodes is 0, assigning no partitions");
        return HashSet::new();
    }

    let partitions: HashSet<_> = (0..partitions)
        .filter(|p| (*p as usize % num_nodes) == node_index)
        .collect();

    info!(
        "modulo assignment: node {}/{} gets {} partitions",
        node_index + 1,
        num_nodes,
        partitions.len()
    );

    partitions
}

fn calculate_bucket_assignment(
    num_nodes: usize,
    node_index: usize,
    total_buckets: u16,
) -> HashSet<BucketId> {
    if num_nodes == 0 {
        return HashSet::new();
    }

    let buckets: HashSet<_> = (0..total_buckets)
        .filter(|b| (*b as usize % num_nodes) == node_index)
        .collect();

    buckets
}
