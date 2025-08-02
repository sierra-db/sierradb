use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;
use config::{Config, ConfigError, Environment, File, Value, ValueKind};
use directories::ProjectDirs;
use libp2p::Multiaddr;
use serde::Deserialize;
use sierradb::bucket::{BucketId, PartitionId};
use thiserror::Error;

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
    pub replication: ReplicationConfig,
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
pub struct ReplicationConfig {
    pub buffer_size: usize,
    pub buffer_timeout_ms: u64,
    pub catchup_timeout_ms: u64,
    pub factor: u8,
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
            .set_default("network.cluster_address", "/ip4/0.0.0.0/tcp/0")?
            .set_default("network.client_address", "0.0.0.0:9090")?
            .set_default("partition.count", 1024)?
            .set_default("replication.buffer_size", 1000)?
            .set_default("replication.buffer_timeout_ms", 8000)?
            .set_default("replication.catchup_timeout_ms", 2000)?
            .set_default("replication.factor", 3)?
            .set_default("segment.size_bytes", 256_000_000)?;

        // Apply any overrides from the node config
        {
            let mut nodes = overrides.get_array("nodes").ok().unwrap_or_default();
            let nodes_count = nodes.len() as u32;
            builder = builder.set_default("node.count", nodes_count)?;
            let node_index = args
                .node_index
                .or_else(|| overrides.get_int("node.index").map(|n| n as u32).ok());
            if let Some(node_index) = node_index
                && (node_index as usize) < nodes.len()
            {
                let overrides = nodes.remove(node_index as usize);
                for (key, value) in flatten_value(overrides) {
                    builder = builder.set_override(key, value)?;
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

    pub fn validate(&self) -> Result<Vec<ValidationError>, ConfigError> {
        let mut errs = Vec::new();

        // Bucket validation
        if self.bucket.count == 0 {
            errs.push(ValidationError::BucketCountZero);
        }
        if let Some(ids) = &self.bucket.ids {
            if ids.len() != self.bucket.count as usize {
                errs.push(ValidationError::BucketIdCountMismatch {
                    expected: self.bucket.count,
                    actual: ids.len(),
                });
            }
            let unique_ids: HashSet<_> = ids.iter().collect();
            if unique_ids.len() != ids.len() {
                errs.push(ValidationError::DuplicateBucketIds);
            }
        }

        // Heartbeat validation
        if self.heartbeat.interval_ms == 0 {
            errs.push(ValidationError::HeartbeatIntervalZero);
        }
        if self.heartbeat.timeout_ms == 0 {
            errs.push(ValidationError::HeartbeatTimeoutZero);
        }
        if self.heartbeat.timeout_ms <= self.heartbeat.interval_ms {
            errs.push(ValidationError::HeartbeatTimeoutTooShort {
                interval: self.heartbeat.interval_ms,
                timeout: self.heartbeat.timeout_ms,
            });
        }

        // Network validation
        if SocketAddr::from_str(&self.network.client_address).is_err() {
            errs.push(ValidationError::InvalidClientAddress {
                address: self.network.client_address.clone(),
            });
        }

        // Node validation
        if let Some(count) = self.node.count {
            if count == 0 {
                errs.push(ValidationError::NodeCountZero);
            }
            if self.node.index >= count {
                errs.push(ValidationError::NodeIndexOutOfBounds {
                    index: self.node.index,
                    count,
                });
            }
        }

        // Cluster mode validation
        if !self.network.cluster_enabled {
            // Non-cluster mode
            if let Some(count) = self.node.count
                && count > 1
            {
                errs.push(ValidationError::MultipleNodesWithoutCluster { count });
            }
        }

        // Partition validation
        if self.partition.count == 0 {
            errs.push(ValidationError::PartitionCountZero);
        }
        if let Some(ids) = &self.partition.ids {
            if ids.len() != self.partition.count as usize {
                errs.push(ValidationError::PartitionIdCountMismatch {
                    expected: self.partition.count,
                    actual: ids.len(),
                });
            }
            let unique_ids: HashSet<_> = ids.iter().collect();
            if unique_ids.len() != ids.len() {
                errs.push(ValidationError::DuplicatePartitionIds);
            }
        }

        // Replication validation
        if self.replication.factor == 0 {
            errs.push(ValidationError::ReplicationFactorZero);
        }
        if self.replication.buffer_size == 0 {
            errs.push(ValidationError::ReplicationBufferSizeZero);
        }
        if self.replication.buffer_timeout_ms == 0 {
            errs.push(ValidationError::ReplicationBufferTimeoutZero);
        }
        if self.replication.catchup_timeout_ms == 0 {
            errs.push(ValidationError::ReplicationCatchupTimeoutZero);
        }

        // Replication factor vs node count
        let node_count = self.node_count()?;
        if self.replication.factor as usize > node_count {
            errs.push(ValidationError::ReplicationFactorExceedsNodeCount {
                factor: self.replication.factor,
                node_count,
            });
        }

        // Segment validation
        if self.segment.size_bytes == 0 {
            errs.push(ValidationError::SegmentSizeZero);
        }
        const MIN_SEGMENT_SIZE: usize = 1024; // 1KB
        const MAX_SEGMENT_SIZE: usize = 1024 * 1024 * 1024 * 10; // 10GB
        if self.segment.size_bytes < MIN_SEGMENT_SIZE {
            errs.push(ValidationError::SegmentSizeTooSmall {
                size: self.segment.size_bytes,
                min: MIN_SEGMENT_SIZE,
            });
        }
        if self.segment.size_bytes > MAX_SEGMENT_SIZE {
            errs.push(ValidationError::SegmentSizeTooLarge {
                size: self.segment.size_bytes,
                max: MAX_SEGMENT_SIZE,
            });
        }

        // Thread validation
        if let Some(read_threads) = self.threads.read {
            if read_threads == 0 {
                errs.push(ValidationError::ReadThreadsZero);
            }
            const MAX_THREADS: u16 = 1024;
            if read_threads > MAX_THREADS {
                errs.push(ValidationError::TooManyReadThreads {
                    count: read_threads,
                    max: MAX_THREADS,
                });
            }
        }
        if let Some(write_threads) = self.threads.write {
            if write_threads == 0 {
                errs.push(ValidationError::WriteThreadsZero);
            }
            const MAX_THREADS: u16 = 1024;
            if write_threads > MAX_THREADS {
                errs.push(ValidationError::TooManyWriteThreads {
                    count: write_threads,
                    max: MAX_THREADS,
                });
            }
        }

        // Cross-field validations for distribution
        if (self.partition.count as usize) < node_count {
            errs.push(ValidationError::TooFewPartitionsForNodes {
                partitions: self.partition.count,
                nodes: node_count,
            });
        }
        if self.partition.count < self.bucket.count {
            errs.push(ValidationError::TooFewPartitionsForBuckets {
                buckets: self.bucket.count,
                partitions: self.partition.count,
            });
        }

        Ok(errs)
    }

    pub fn assigned_buckets(&self) -> Result<HashSet<BucketId>, ConfigError> {
        match &self.bucket.ids {
            Some(ids) => Ok(ids.iter().copied().collect()),
            None => {
                let node_count = self.node_count()?;
                let replication_factor = self.replication.factor as usize;
                let current_node_index = self.node.index as usize;

                // Cap replication factor at node count (every node owns everything if RF >=
                // node count)
                let effective_replication_factor = replication_factor.min(node_count);

                let mut assigned = HashSet::new();

                for bucket_id in 0..self.bucket.count {
                    let primary_node = bucket_id as usize % node_count;

                    // Check if current node is one of the replica nodes for this bucket
                    for replica_offset in 0..effective_replication_factor {
                        let replica_node = (primary_node + replica_offset) % node_count;
                        if replica_node == current_node_index {
                            assigned.insert(bucket_id);
                            break;
                        }
                    }
                }

                Ok(assigned)
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

        writeln!(
            f,
            "replication.buffer_size = {}",
            self.replication.buffer_size
        )?;
        writeln!(
            f,
            "replication.buffer_timeout_ms = {}",
            self.replication.buffer_timeout_ms
        )?;
        // writeln!(
        //     f,
        //     "replication.catch_up_threshold = {}",
        //     self.replication.catch_up_threshold
        // )?;
        writeln!(f, "replication.factor = {}", self.replication.factor)?;

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
                    format!("{prefix}.{key}")
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

#[derive(Clone, Debug, Error)]
pub enum ValidationError {
    // Bucket errors
    #[error("bucket count cannot be zero")]
    BucketCountZero,
    #[error("bucket ID count mismatch: expected {expected}, got {actual}")]
    BucketIdCountMismatch { expected: u16, actual: usize },
    #[error("duplicate bucket IDs found")]
    DuplicateBucketIds,

    // Heartbeat errors
    #[error("heartbeat interval cannot be zero")]
    HeartbeatIntervalZero,
    #[error("heartbeat timeout cannot be zero")]
    HeartbeatTimeoutZero,
    #[error("heartbeat timeout ({timeout}ms) must be greater than interval ({interval}ms)")]
    HeartbeatTimeoutTooShort { interval: u64, timeout: u64 },

    // Network errors
    #[error("invalid client address: {address}")]
    InvalidClientAddress { address: String },

    // Node errors
    #[error("node count cannot be zero")]
    NodeCountZero,
    #[error("node index {index} is out of bounds for count {count}")]
    NodeIndexOutOfBounds { index: u32, count: u32 },
    #[error("multiple nodes ({count}) configured but cluster is disabled")]
    MultipleNodesWithoutCluster { count: u32 },

    // Partition errors
    #[error("partition count cannot be zero")]
    PartitionCountZero,
    #[error("partition ID count mismatch: expected {expected}, got {actual}")]
    PartitionIdCountMismatch { expected: u16, actual: usize },
    #[error("duplicate partition IDs found")]
    DuplicatePartitionIds,

    // Replication errors
    #[error("replication factor cannot be zero")]
    ReplicationFactorZero,
    #[error("replication factor {factor} exceeds node count {node_count}")]
    ReplicationFactorExceedsNodeCount { factor: u8, node_count: usize },
    #[error("replication buffer size cannot be zero")]
    ReplicationBufferSizeZero,
    #[error("replication buffer timeout cannot be zero")]
    ReplicationBufferTimeoutZero,
    #[error("replication catchup timeout cannot be zero")]
    ReplicationCatchupTimeoutZero,

    // Segment errors
    #[error("segment size cannot be zero")]
    SegmentSizeZero,
    #[error("segment size {size} is too small (minimum: {min} bytes)")]
    SegmentSizeTooSmall { size: usize, min: usize },
    #[error("segment size {size} is too large (maximum: {max} bytes)")]
    SegmentSizeTooLarge { size: usize, max: usize },

    // Thread errors
    #[error("read thread count cannot be zero")]
    ReadThreadsZero,
    #[error("write thread count cannot be zero")]
    WriteThreadsZero,
    #[error("too many read threads: {count} (maximum: {max})")]
    TooManyReadThreads { count: u16, max: u16 },
    #[error("too many write threads: {count} (maximum: {max})")]
    TooManyWriteThreads { count: u16, max: u16 },

    // Cross-field errors
    #[error("too few partitions ({partitions}) for {nodes} nodes")]
    TooFewPartitionsForNodes { partitions: u16, nodes: usize },
    #[error("too few partitions ({partitions}) for {buckets} buckets")]
    TooFewPartitionsForBuckets { buckets: u16, partitions: u16 },
}
