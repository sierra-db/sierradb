# SierraDB

**A high-performance, distributed event store designed for scalable event sourcing applications**

SierraDB is a modern, horizontally-scalable database specifically built for event sourcing workloads. It combines the simplicity of Redis protocol compatibility with the distributed architecture principles of Cassandra/ScyllaDB, providing developers with a powerful foundation for building event-driven systems.

## 📖 Learn More

Want to understand how SierraDB works under the hood? Read the detailed blog post: [Building SierraDB: A Distributed Event Store Built in Rust](https://tqwewe.com/blog/building-sierradb/)

The post covers the architecture decisions, watermark system for consistency, distributed consensus, and the journey of building an event store from scratch.

## 🚀 Try it in 30 seconds

Get SierraDB running with Docker and start storing events immediately:

```bash
# Start SierraDB
docker run -p 9090:9090 tqwewe/sierradb

# In another terminal, use redis-cli to interact with SierraDB
redis-cli -p 9090

# Append some events to a user stream
EAPPEND user-123 UserRegistered PAYLOAD '{"email":"alice@example.com","name":"Alice"}'
EAPPEND user-123 EmailVerified PAYLOAD '{"timestamp":"2024-10-18T10:30:00Z"}'
EAPPEND user-123 ProfileUpdated PAYLOAD '{"bio":"Software engineer"}'

# Read all events from the stream
ESCAN user-123 - +

# Get the current version of the stream
ESVER user-123

# Subscribe to new events (will stream in real-time)
ESUB user-123 FROM LATEST
```

That's it! You're now running a distributed event store and can append/read events using any Redis client.

## Quick Start

### Using Docker (Recommended)
```bash
# Run with persistent data
docker run -p 9090:9090 -v ./data:/app/data tqwewe/sierradb
```

### Install with Cargo
```bash
# Setup toolchain to use nightly feature
rustup toolchain install nightly
rustup override set nightly
cargo install sierradb-server
sierradb --dir ./data --client-address 0.0.0.0:9090
```

### Server Startup

```bash
# Startup Logs
INFO sierradb_topology::manager: recalculating partition assignments with 1 active nodes
INFO sierradb_topology::behaviour: node 0 owns 32/32 partitions as replicas
INFO libp2p_swarm: local_peer_id=12D3KooWDm7sMssZnyq77Dcpzg7t1xZDs2pq6N5mwR3wbMhMscbN
WARN libp2p_kad::behaviour: Failed to trigger bootstrap: No known peers.
```

### Basic Event Operations
```bash
# Connect with any Redis client
redis-cli -p 9090

# Append events to streams
EAPPEND orders order-456 OrderCreated PAYLOAD '{"total":99.99,"items":["laptop"]}'

# Read events back
ESCAN orders - + COUNT 100

# Subscribe to real-time events
ESUB orders FROM LATEST
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

#### `EAPPEND` - Append Event to Stream
Append an event to a stream with optional expected version control.

**Syntax:**
```
EAPPEND <stream_id> <event_name> [EVENT_ID <event_id>] [PARTITION_KEY <partition_key>] [EXPECTED_VERSION <version>] [TIMESTAMP <timestamp>] [PAYLOAD <payload>] [METADATA <metadata>]
```

**Parameters:**
- `stream_id` - Stream identifier to append the event to
- `event_name` - Name/type of the event
- `event_id` (optional) - UUID for the event (auto-generated if not provided)
- `partition_key` (optional) - UUID to determine event partitioning
- `expected_version` (optional) - Expected stream version (number, "any", "exists", "empty")
- `timestamp` (optional) - Event timestamp in milliseconds
- `payload` (optional) - Event payload data
- `metadata` (optional) - Event metadata

**Examples:**
```
EAPPEND my-stream UserCreated PAYLOAD '{"name":"john"}' METADATA '{"source":"api"}'
EAPPEND orders OrderPlaced EVENT_ID 550e8400-e29b-41d4-a716-446655440000 EXPECTED_VERSION empty
```

#### `EMAPPEND` - Multi-Stream Transactional Append
Append multiple events across different streams within the same partition atomically.

**Syntax:**
```
EMAPPEND <partition_key> <stream_id1> <event_name1> [EVENT_ID <event_id1>] [EXPECTED_VERSION <version1>] [TIMESTAMP <timestamp>] [PAYLOAD <payload1>] [METADATA <metadata1>] [<stream_id2> <event_name2> ...]
```

**Parameters:**
- `partition_key` - UUID that determines which partition all events will be written to
- For each event: `stream_id`, `event_name`, and optional `EVENT_ID`, `EXPECTED_VERSION`, `TIMESTAMP`, `PAYLOAD`, `METADATA`

**Examples:**
```
EMAPPEND 550e8400-e29b-41d4-a716-446655440000 stream1 EventA PAYLOAD '{"data":"value1"}' stream2 EventB PAYLOAD '{"data":"value2"}'
```

**Note:** All events succeed or fail together as a single atomic transaction.

#### `EGET` - Get Event by ID
Retrieve an event by its unique identifier.

**Syntax:**
```
EGET <event_id>
```

**Parameters:**
- `event_id` - UUID of the event to retrieve

**Examples:**
```
EGET 550e8400-e29b-41d4-a716-446655440000
```

#### `ESCAN` - Scan Stream Events
Scan events in a stream by version range.

**Syntax:**
```
ESCAN <stream_id> <start_version> <end_version> [PARTITION_KEY <partition_key>] [COUNT <count>]
```

**Parameters:**
- `stream_id` - Stream identifier to scan
- `start_version` - Starting version number (use "-" for beginning)
- `end_version` - Ending version number (use "+" for end, or specific number)
- `partition_key` (optional) - UUID to scan specific partition
- `count` (optional) - Maximum number of events to return

**Examples:**
```
ESCAN my-stream 0 100 COUNT 50
ESCAN my-stream - + PARTITION_KEY 550e8400-e29b-41d4-a716-446655440000
```

### Stream Operations

#### `ESVER` - Get Stream Version
Get the current version number for a stream.

**Syntax:**
```
ESVER <stream_id> [PARTITION_KEY <partition_key>]
```

**Parameters:**
- `stream_id` - Stream identifier to get version for
- `partition_key` (optional) - UUID to check specific partition

**Examples:**
```
ESVER my-stream
ESVER my-stream PARTITION_KEY 550e8400-e29b-41d4-a716-446655440000
```

#### `ESUB` - Subscribe to Stream Events
Subscribe to events from one or more streams with real-time delivery.

**Syntax:**
```
# Single stream
ESUB <stream_id> [PARTITION_KEY <partition_key>] [FROM <version>] [WINDOW <size>]

# Multiple streams
ESUB <stream_id_1> [PARTITION_KEY <pk_1>] <stream_id_2> [PARTITION_KEY <pk_2>] ... [FROM LATEST | FROM <version> | FROM MAP <stream>=<ver>...] [WINDOW <size>]
```

**Parameters:**
- `stream_id` - Stream identifier(s) to subscribe to
- `partition_key` (optional) - UUID for specific partition
- `FROM` (optional) - Starting position (LATEST, version number, or MAP for per-stream positions)
- `WINDOW` (optional) - Maximum number of unacknowledged events

**Examples:**
```
ESUB user-123                                         # Single stream, latest, no window
ESUB user-123 WINDOW 100                              # Single stream, latest, window 100
ESUB user-123 FROM 50 WINDOW 100                      # Single stream, from version 50
ESUB user-1 user-2 user-3 FROM MAP user-1=10 user-2=20 user-3=30 WINDOW 50
```

**Note:** Establishes a persistent connection for real-time event delivery.

### Partition Operations

#### `EPSCAN` - Scan Partition Events
Scan events in a partition by sequence number range.

**Syntax:**
```
EPSCAN <partition> <start_sequence> <end_sequence> [COUNT <count>]
```

**Parameters:**
- `partition` - Partition selector (partition ID 0-65535 or UUID key)
- `start_sequence` - Starting sequence number (use "-" for beginning)
- `end_sequence` - Ending sequence number (use "+" for end, or specific number)
- `count` (optional) - Maximum number of events to return

**Examples:**
```
EPSCAN 42 100 200 COUNT 50
EPSCAN 550e8400-e29b-41d4-a716-446655440000 - + COUNT 100
```

#### `EPSEQ` - Get Partition Sequence
Get the current sequence number for a partition.

**Syntax:**
```
EPSEQ <partition>
```

**Parameters:**
- `partition` - Partition selector (partition ID 0-65535 or UUID key)

**Examples:**
```
EPSEQ 42
EPSEQ 550e8400-e29b-41d4-a716-446655440000
```

#### `EPSUB` - Subscribe to Partition Events
Subscribe to events from one or more partitions with real-time delivery.

**Syntax:**
```
# All partitions
EPSUB * [FROM LATEST | FROM <sequence> | FROM MAP <p1>=<s1> <p2>=<s2>... [DEFAULT <seq>]] [WINDOW <size>]

# Single partition
EPSUB <partition_id> [FROM <sequence>] [WINDOW <size>]

# Multiple partitions
EPSUB <p1>,<p2>,<p3> [FROM LATEST | FROM <sequence> | FROM MAP <p1>=<s1> <p2>=<s2>... [DEFAULT <seq>]] [WINDOW <size>]
```

**Parameters:**
- `partition_id` - Partition identifier(s) (* for all, comma-separated list for multiple)
- `FROM` (optional) - Starting position (LATEST, sequence number, or MAP for per-partition positions)
- `WINDOW` (optional) - Maximum number of unacknowledged events

**Examples:**
```
EPSUB *                                               # All partitions, latest
EPSUB * WINDOW 100                                    # All partitions, latest, window 100
EPSUB * FROM 1000 WINDOW 100                          # All partitions, from seq 1000
EPSUB 5 FROM 100 WINDOW 50                            # Partition 5, from seq 100
EPSUB 1,2,3 FROM MAP 1=100 2=200 DEFAULT 0 WINDOW 500
```

**Note:** Establishes a persistent connection for real-time event delivery.

### Subscription Management

#### `EACK` - Acknowledge Events
Acknowledge events up to a cursor for a subscription.

**Syntax:**
```
EACK <subscription_id> <sequence_or_version>
```

**Parameters:**
- `subscription_id` - UUID of the subscription
- `sequence_or_version` - Sequence or version number to acknowledge up to

**Examples:**
```
EACK 550e8400-e29b-41d4-a716-446655440000 1000
```

**Note:** Tells SierraDB that events up to the specified position have been processed successfully.

### System Operations

#### `HELLO` - Protocol Handshake
Perform protocol handshake and retrieve server information.

**Syntax:**
```
HELLO <version>
```

**Parameters:**
- `version` - Protocol version (currently only 3 is supported)

**Examples:**
```
HELLO 3
```

**Returns:**
- `server` - Server name ("sierradb")
- `version` - Server version
- `peer_id` - Cluster peer ID
- `num_partitions` - Number of partitions

#### `PING` - Health Check
Simple health check and connectivity test.

**Syntax:**
```
PING
```

**Returns:** `PONG`

**Examples:**
```
PING
```

## Data Integrity & Transactions

SierraDB ensures data reliability through multiple mechanisms:

- **CRC32C Checksums**: Every event includes integrity checksums to detect corruption
- **Atomic Transactions**: Events within the same partition can be written transactionally
- **Corruption Recovery**: Automatic detection and recovery from data corruption scenarios
- **Graceful Shutdown Handling**: Proper recovery even after sudden system shutdowns

## Cluster Setup

For distributed deployments across multiple nodes:

```bash
# Node 1
sierradb --dir ./data1 --node-index 0 --node-count 3 --client-address 0.0.0.0:9090

# Node 2
sierradb --dir ./data2 --node-index 1 --node-count 3 --client-address 0.0.0.0:9091

# Node 3
sierradb --dir ./data3 --node-index 2 --node-count 3 --client-address 0.0.0.0:9092
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
