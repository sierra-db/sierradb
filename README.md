# SierraDB

**A high-performance, distributed event store designed for scalable event sourcing applications**

SierraDB is a modern, horizontally-scalable database specifically built for event sourcing workloads. It combines the simplicity of Redis protocol compatibility with the distributed architecture principles of Cassandra/ScyllaDB, providing developers with a powerful foundation for building event-driven systems.

## Key Features

- **Redis Protocol Compatible** - Uses RESP3 protocol, making it compatible with existing Redis clients and tools
- **Horizontally Scalable** - Distributed architecture that scales across multiple nodes with configurable replication
- **High Performance** - Optimized for event sourcing with consistent write performance regardless of database size
- **Real-time Subscriptions** - Seamless transition from historical events to live streaming for projections and event handlers
- **Event Sourcing Optimized** - Purpose-built for append-only event patterns with strong ordering guarantees
- **Data Integrity** - CRC32C checksums ensure corruption detection with automatic recovery capabilities
- **Actively Developed** - Under active development with extensive testing and stability improvements

## Architecture Overview

SierraDB organizes data using a three-tier hierarchy designed for optimal performance and scalability:

### Buckets and Partitions
- **Default Configuration**: 4 buckets containing 32 partitions (8 partitions per bucket)
- **Scalability**: Can be scaled up across many nodes for virtually unlimited horizontal performance
- **Distribution**: Events are distributed across partitions using consistent hashing of partition keys

### Segmented Storage
- **Segment Files**: Events are written to segment files that are sealed at 256MB (configurable)
- **Consistent Performance**: New segments ensure write performance remains constant regardless of database size
- **Parallel Processing**: Writes within a partition are sequential, but parallel across different partitions

### Event Ordering Guarantees
- **Partition Sequences**: Gapless monotonic sequence numbers within each partition
- **Stream Versions**: Gapless monotonic version numbers within each stream
- **Consistency**: Events within the same partition maintain strict ordering while allowing parallelism across partitions

## Performance Characteristics

SierraDB is optimized for three primary read patterns:

1. **Event Lookup by ID** - Fast UUID-based event retrieval using event indexes
2. **Stream Scanning** - Efficient sequential reading of events within a stream using stream indexes  
3. **Partition Scanning** - High-throughput scanning of events within a partition using partition indexes

Each segment file is accompanied by specialized index files and bloom filters for maximum query performance.

## Distributed Architecture

SierraDB operates as a distributed cluster similar to Cassandra/ScyllaDB:

- **Cluster Coordination** - Automatic node discovery and coordination using libp2p and optional mDNS
- **Replication Factor** - Configurable data replication (defaults to min(node_count, 3))
- **Fault Tolerance** - Automatic failover and data recovery across cluster nodes
- **Consistent Hashing** - Automatic data distribution and rebalancing

## Real-time Subscriptions

One of SierraDB's most powerful features is its subscription system, designed specifically for event sourcing projections:

- **Historical + Live**: Subscriptions start from any point in history and automatically transition to live events
- **Guaranteed Delivery**: Events are delivered in order with acknowledgment tracking
- **Multiple Stream Types**: Subscribe to individual streams, partitions, or combinations
- **Backpressure Handling**: Configurable windowing prevents overwhelmed consumers

Perfect for building:
- Event sourcing projections that need to catch up from the beginning
- Real-time event handlers that process new events as they arrive
- Read models that require both historical rebuild and live updates

## Configuration

SierraDB offers extensive configuration options. Key settings include:

```toml
[bucket]
count = 4                    # Number of buckets

[partition] 
count = 32                   # Number of partitions (8 per bucket by default)

[segment]
size_bytes = 268435456       # Segment size in bytes (256MB default)

[replication]
factor = 3                   # Data replication factor
buffer_size = 1000           # Replication buffer size
buffer_timeout_ms = 8000     # Buffer timeout

[network]
client_address = "0.0.0.0:9090"           # Client connection address
cluster_address = "/ip4/0.0.0.0/tcp/0"   # Inter-node communication

[threads]
read = 8                     # Read thread pool size
write = 4                    # Write thread pool size
```

See `crates/sierradb-server/src/config.rs` for the complete configuration reference.

## Supported Operations

SierraDB implements a comprehensive set of RESP3 commands for event operations:

### Core Event Operations
- **`EAPPEND`** - Append event to a stream with optional expected version
- **`EMAPPEND`** - Transactionally append multiple events across streams within same partition
- **`EGET`** - Retrieve event by UUID
- **`ESCAN`** - Scan events in a stream by version range

### Stream Operations  
- **`ESVER`** - Get current version of a stream
- **`ESUB`** - Subscribe to real-time events from one or more streams

### Partition Operations
- **`EPSCAN`** - Scan events in a partition by sequence range
- **`EPSEQ`** - Get current sequence number of a partition
- **`EPSUB`** - Subscribe to real-time events from one or more partitions

### Subscription Management
- **`EACK`** - Acknowledge processed events for subscription tracking

### System Operations
- **`HELLO`** - Protocol handshake and server information
- **`PING`** - Health check and connectivity test

## Data Integrity & Transactions

SierraDB ensures data reliability through multiple mechanisms:

- **CRC32C Checksums**: Every event includes integrity checksums to detect corruption
- **Atomic Transactions**: Events within the same partition can be written transactionally
- **Corruption Recovery**: Automatic detection and recovery from data corruption scenarios
- **Graceful Shutdown Handling**: Proper recovery even after sudden system shutdowns

## Getting Started

### Single Node Setup
```bash
# Start SierraDB with default configuration
sierradb-server --dir ./data --client-address 0.0.0.0:9090
```

### Cluster Setup
```bash
# Node 1
sierradb-server --dir ./data1 --node-index 0 --node-count 3 --client-address 0.0.0.0:9090

# Node 2  
sierradb-server --dir ./data2 --node-index 1 --node-count 3 --client-address 0.0.0.0:9091

# Node 3
sierradb-server --dir ./data3 --node-index 2 --node-count 3 --client-address 0.0.0.0:9092
```

### Embedded Usage

For Rust applications, SierraDB can be embedded directly using the core `sierradb` crate:

```rust
use std::time::{SystemTime, UNIX_EPOCH};
use sierradb::{Database, Transaction, NewEvent, StreamId};
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash, uuid_v7_with_partition_hash};
use sierradb_protocol::ExpectedVersion;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database directly
    let db = Database::open("./data")?;
    
    // Create and append events
    let stream_id = StreamId::new("user-123")?;
    let partition_key = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
    let partition_hash = uuid_to_partition_hash(partition_key);
    let partition_id = uuid_v7_with_partition_hash(partition_hash);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_nanos() as u64;

    let transaction = Transaction::new(
        partition_key,
        partition_id,
        vec![NewEvent {
            event_id: Uuid::new_v4(),
            stream_id,
            stream_version: ExpectedVersion::Any,
            event_name: "UserCreated".into(),
            timestamp,
            payload: br#"{"name":"john"}"#.to_vec(),
            metadata: vec![],
        }]
    )?;
    
    let result = db.append_events(transaction).await?;
    println!("Event appended with sequence: {}", result.first_partition_sequence);
    
    Ok(())
}
```

### Using the Rust Client

SierraDB provides a native Rust client built on top of the redis crate with full type support for SierraDB commands and subscriptions:

```rust
use sierradb_client::{SierraClient, SierraAsyncClientExt, SierraMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = SierraClient::connect("redis://localhost:9090").await?;
    
    // Append an event with type safety
    let options = EAppendOptions::new()
        .payload(br#"{"name":"john"}"#)
        .metadata(br#"{"source":"api"}"#);
    let result = client.eappend("user-123", "UserCreated", options)?;
    
    // Scan events from stream
    let events = client.escan("user-123", "-", None, Some(50))
        .execute()
        .await?;
    
    // Subscribe to real-time events
    let mut manager = client.subscription_manager().await?;
    let mut subscription = manager
        .subscribe_to_stream_from_version("user-123", 0)
        .await?;
    while let Some(msg) = subscription.next_message().await? {
        println!("Received event: {msg:?}");
    }
    
    Ok(())
}
```

### Using with Redis Clients

Since SierraDB uses the RESP3 protocol, you can also connect with any Redis-compatible client:

```python
import redis

# Connect to SierraDB
client = redis.Redis(host='localhost', port=9090, protocol=3)

# Append an event
result = client.execute_command('EAPPEND', 'user-123', 'UserCreated', 
                               'PAYLOAD', '{"name":"John","email":"john@example.com"}')

# Read events from stream
events = client.execute_command('ESCAN', 'user-123', '-', '+', 'COUNT', '100')
```

## Development Status

SierraDB is under active development and testing:

- **Stability Testing**: Extensive fuzzing campaigns ensure robustness under edge conditions
- **Performance Validation**: Consistent performance characteristics validated across various workloads  
- **Current Usage**: Being used to build systems in development and testing environments
- **Data Safety**: Comprehensive corruption detection and recovery mechanisms implemented

**Note**: While SierraDB demonstrates strong stability in testing, it is not yet recommended for production use. The project is actively working toward production readiness.

## Contributing

SierraDB is being prepared for public release. Contribution guidelines and development setup will be available once the repository is made public.

## License

SierraDB is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

---

**Ready to scale your event sourcing architecture?** SierraDB provides the performance, reliability, and developer experience you need to build robust event-driven systems.
