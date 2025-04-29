use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::PathBuf,
};

use clap::Parser;
use config::{Config, ConfigError, Environment, File, Value, ValueKind};
use directories::ProjectDirs;
use libp2p::Multiaddr;
use serde::Deserialize;
use sierradb::bucket::{BucketId, PartitionId};

/// A distributed, partitioned event store with configurable replication
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to database data directory
    #[arg(long, short = 'd')]
    pub dir: Option<String>,

    /// Network address for inter-node cluster communication (QUIC/libp2p)
    #[arg(long)]
    pub cluster_address: Option<String>,

    /// Network address for client connections (e.g., "0.0.0.0:9090")
    #[arg(long)]
    pub client_address: Option<String>,

    /// Path to configuration file (TOML, YAML, or JSON)
    #[arg(short = 'c', long)]
    pub config_file: Option<PathBuf>,

    /// A log filter string
    #[arg(short = 'l', long)]
    pub log: Option<String>,

    /// Total number of nodes in the cluster
    #[arg(short = 'n', long)]
    pub node_count: Option<u32>,

    /// Index of this node in the cluster (0-based)
    #[arg(short = 'i', long)]
    pub node_index: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub bucket: BucketConfig,
    pub dir: PathBuf,
    pub flush: FlushConfig,
    pub heartbeat: HeartbeatConfig,
    pub network: NetworkConfig,
    pub node: NodeConfig,
    pub partition: PartitionConfig,
    pub replication_factor: u8,
    pub segment: SegmentConfig,
    #[serde(default)]
    pub threads: Threads,

    pub nodes: Option<Vec<Value>>,
}

#[derive(Debug, Deserialize)]
pub struct BucketConfig {
    pub count: u16,
    pub ids: Option<Vec<BucketId>>,
}

#[derive(Debug, Deserialize)]
pub struct FlushConfig {
    pub events_threshold: u32,
    pub interval_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatConfig {
    pub interval_ms: u64,
    pub timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub cluster_enabled: bool,
    pub cluster_address: Multiaddr, // For libp2p inter-node traffic
    pub client_address: String,     // For client connections
}

#[derive(Debug, Deserialize)]
pub struct NodeConfig {
    pub count: Option<u32>,
    pub index: u32,
}

#[derive(Debug, Deserialize)]
pub struct PartitionConfig {
    pub count: u16,
    pub ids: Option<Vec<PartitionId>>,
}

#[derive(Debug, Deserialize)]
pub struct SegmentConfig {
    pub size_bytes: usize,
}

#[derive(Debug, Default, Deserialize)]
pub struct Threads {
    pub read: Option<u16>,
    pub write: Option<u16>,
}

impl AppConfig {
    // Load configuration with priority: CLI args > Environment vars > Config file >
    // Default values
    pub fn load(args: Args) -> Result<Self, ConfigError> {
        let project_dirs = ProjectDirs::from("io", "sierradb", "sierradb");

        let mut builder = Config::builder();

        if let Some(dirs) = &project_dirs {
            // Linux:   /home/alice/.config/sierradb/db
            //
            // Windows: C:\Users\Alice\AppData\Roaming\sierradb\sierradb\db
            //
            // macOS:   /Users/Alice/Library/Application
            // Support/io.sierradb.sierradb/db
            builder = builder.set_default(
                "dir",
                dirs.data_dir().join("db").to_string_lossy().into_owned(),
            )?
        }

        // Add config file if specified
        if let Some(config_path) = args.config_file {
            builder = builder.add_source(File::from(config_path));
        } else {
            // Try standard config locations if no explicit file provided
            builder = builder.add_source(File::with_name("sierra").required(false));
            if let Some(dirs) = &project_dirs {
                // Linux:   /home/alice/.config/sierradb/sierra.toml
                //
                // Windows: C:\Users\Alice\AppData\Roaming\sierradb\sierradb\
                // sierra.toml
                //
                // macOS:   /Users/Alice/Library/
                // Application Support/io.sierradb.sierradb/
                // sierra.toml
                builder = builder
                    .add_source(File::from(dirs.config_dir().join("sierra")).required(false));
            }
        }

        let overrides = builder.build_cloned()?;

        builder = builder
            .set_default("bucket.count", 64)?
            .set_default("flush.events_threshold", 1)?
            .set_default("flush.interval_ms", 0)?
            .set_default("heartbeat.interval_ms", 1000)?
            .set_default("heartbeat.timeout_ms", 6000)?
            .set_default("network.cluster_enabled", true)?
            .set_default("network.cluster_address", "/ip4/0.0.0.0/udp/0/quic-v1")?
            .set_default("network.client_address", "0.0.0.0:9090")?
            .set_default("partition.count", 1024)?
            .set_default("replication_factor", 3)?
            .set_default("segment.size_bytes", 256_000_000)?;

        // Apply any overrides from the node config
        {
            let mut nodes = overrides.get_array("nodes").ok().unwrap_or_default();
            let nodes_count = nodes.len() as u32;
            builder = builder.set_default("node.count", nodes_count)?;
            let node_index = args
                .node_index
                .or_else(|| overrides.get_int("node.index").map(|n| n as u32).ok());
            if let Some(node_index) = node_index {
                if (node_index as usize) < nodes.len() {
                    let overrides = nodes.remove(node_index as usize);
                    for (key, value) in flatten_value(overrides) {
                        builder = builder.set_override(key, value)?;
                    }
                }
            }
        }

        // Add environment variables prefixed with "SIERRA_"
        builder = builder.add_source(Environment::with_prefix("SIERRA"));

        // Override with CLI arguments if provided
        builder = builder
            .set_override_option("dir", args.dir)?
            .set_override_option("network.cluster_address", args.cluster_address)?
            .set_override_option("network.client_address", args.client_address)?
            .set_override_option("node.index", args.node_index)?
            .set_override_option("node.count", args.node_count)?;

        let config: AppConfig = builder.build()?.try_deserialize()?;

        let node_count = config.node_count()?;

        if node_count == 0 {
            return Err(ConfigError::Message(
                "node.count must be greater than zero".to_string(),
            ));
        }

        if config.node.index >= node_count as u32 {
            return Err(ConfigError::Message(
                "node.index must be less than node.count".to_string(),
            ));
        }

        Ok(config)
    }

    pub fn assigned_buckets(&self) -> Result<HashSet<BucketId>, ConfigError> {
        match &self.bucket.ids {
            Some(ids) => Ok(ids.iter().copied().collect()),
            None => {
                let node_count = self.node_count()?;
                Ok((0..self.bucket.count)
                    .filter(|b| (*b as usize % node_count) == self.node.index as usize)
                    .collect())
            }
        }
    }

    pub fn assigned_partitions(&self, bucket_ids: &HashSet<BucketId>) -> HashSet<PartitionId> {
        match &self.partition.ids {
            Some(ids) => ids.iter().copied().collect(),
            None => (0..self.partition.count)
                .filter(|p| bucket_ids.contains(&(p % self.bucket.count)))
                .collect(),
        }
    }

    pub fn node_count(&self) -> Result<usize, ConfigError> {
        self.node
            .count
            .map(|node_count| node_count as usize)
            .or(self.nodes.as_ref().map(|nodes| nodes.len()))
            .ok_or_else(|| ConfigError::Message("node.count not specified".to_string()))
    }
}

impl fmt::Display for AppConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "bucket.count = {}", self.bucket.count)?;
        match &self.bucket.ids {
            Some(ids) => writeln!(
                f,
                "bucket.ids = [{}]",
                ids.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?,
            None => writeln!(f, "bucket.ids = <none>")?,
        }

        writeln!(f, "dir = {}", self.dir.to_string_lossy())?;

        writeln!(
            f,
            "flush.events_threshold = {}",
            self.flush.events_threshold
        )?;
        writeln!(f, "flush.interval_ms = {}", self.flush.interval_ms)?;

        writeln!(f, "heartbeat.interval_ms = {}", self.heartbeat.interval_ms)?;
        writeln!(f, "heartbeat.timeout_ms = {}", self.heartbeat.timeout_ms)?;

        writeln!(
            f,
            "network.cluster_enabled = {}",
            self.network.cluster_enabled
        )?;
        writeln!(
            f,
            "network.cluster_address = {}",
            self.network.cluster_address
        )?;
        writeln!(
            f,
            "network.client_address = {}",
            self.network.client_address
        )?;

        match self.node_count() {
            Ok(count) => writeln!(f, "node.count = {count}")?,
            Err(_) => writeln!(f, "node.count = <none>")?,
        }
        writeln!(f, "node.index = {}", self.node.index)?;

        writeln!(f, "partition.count = {}", self.partition.count)?;
        match &self.partition.ids {
            Some(ids) => writeln!(
                f,
                "partition.ids = [{}]",
                ids.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )?,
            None => writeln!(f, "partition.ids = <none>")?,
        }

        writeln!(f, "replication_factor = {}", self.replication_factor)?;

        writeln!(f, "segment.size_bytes = {}", self.segment.size_bytes)?;

        match self.threads.read {
            Some(count) => writeln!(f, "threads.read = {count}")?,
            None => writeln!(f, "threads.read = <none>")?,
        }
        match self.threads.write {
            Some(count) => write!(f, "threads.write = {count}")?,
            None => write!(f, "threads.write = <none>")?,
        }

        Ok(())
    }
}

fn flatten_value(value: Value) -> HashMap<String, Value> {
    let mut result = HashMap::new();
    flatten_value_recursive(value, "", &mut result);
    result
}

fn flatten_value_recursive(value: Value, prefix: &str, result: &mut HashMap<String, Value>) {
    match value.kind {
        ValueKind::Table(table) => {
            for (key, val) in table {
                let new_prefix = if prefix.is_empty() {
                    key
                } else {
                    format!("{}.{}", prefix, key)
                };

                match val.kind {
                    ValueKind::Table(_) => {
                        flatten_value_recursive(val, &new_prefix, result);
                    }
                    _ => {
                        result.insert(new_prefix, val);
                    }
                }
            }
        }
        _ => {
            if !prefix.is_empty() {
                result.insert(prefix.to_string(), value);
            }
        }
    }
}
