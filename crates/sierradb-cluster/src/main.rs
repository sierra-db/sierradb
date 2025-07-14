use std::{collections::HashSet, time::Duration};

use kameo::Actor;
use libp2p::identity::Keypair;
use sierradb::database::Database;
use sierradb_cluster::{ClusterActor, ClusterArgs};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .without_time()
        .with_env_filter(EnvFilter::new("INFO"))
        .init();

    let num_partitions = 6;
    let replication_factor = 2;
    let assigned_partitions = std::env::args()
        .nth(1)
        .unwrap()
        .split(',')
        .map(|n| n.parse().unwrap())
        .collect::<HashSet<u16>>();
    let heartbeat_timeout = Duration::from_secs(10);
    let heartbeat_interval = Duration::from_secs(2);

    let keypair = Keypair::generate_ed25519();

    let database = Database::open("./target/db")?;

    ClusterActor::prepare()
        .run(ClusterArgs {
            keypair,
            database,
            listen_addrs: vec![],
            partition_count: num_partitions,
            replication_factor,
            assigned_partitions,
            heartbeat_timeout,
            heartbeat_interval,
        })
        .await?;

    Ok(())
}
