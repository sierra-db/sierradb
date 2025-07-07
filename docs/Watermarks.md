## Problem Statement

**Context**: Eventus is a distributed event sourcing database built in Rust, inspired by Cassandra/ScyllaDB, using libp2p for distributed communication. It already implements distributed writes with quorum consensus.

**Challenge**: How to implement efficient read operations while maintaining strong consistency guarantees in a distributed system where:

- Events must be confirmed by a quorum before being visible to readers
- Confirmation updates happen asynchronously in the background and can fail
- The system needs to avoid network overhead of distributed read quorums
- Event sourcing requires strict consistency (no invalid states tolerated)

**Key Design Question**: Should confirmation tracking enforce strict ordering (no gaps) or allow independent confirmation, and how to handle scenarios where confirmation updates fail?

## Solution Design

### Core Approach: Local Confirmation State with Watermarks

Instead of implementing distributed read quorums, we leverage local confirmation state tracking:

1. **Confirmation Count Storage**: Each event stores a confirmation count that tracks how many replicas have successfully written it
2. **Local Reads**: Read operations check the local confirmation count rather than querying multiple nodes
3. **Watermark System**: Implement a "confirmed watermark" per partition that represents the highest contiguously confirmed event version

### Hybrid Confirmation Strategy

**Problem Solved**: Balance between resilience and consistency by using:

- **Independent Physical Confirmation**: Events can be confirmed independently in the background
- **Ordered Logical Watermark**: Watermark only advances when there are no gaps in confirmation
- **Gap-Free Reader View**: Clients only see events up to the watermark, ensuring consistent ordering

### Technical Implementation

**Storage Architecture**:

- Confirmation state organized by buckets (physical storage units) rather than partitions (logical units)
- **Confirmation Data Location**: Stored in `/data/buckets/{bucket_id}/confirmation/` directory alongside event data
    - `bucket_state.current.json` - Current confirmation state
    - `bucket_state.previous.json` - Backup state file
    - `bucket_state.temp.json` - Temporary file during updates
- State persisted using bincode for efficiency with checksums for integrity
- Atomic watermarks using `AcqRel`/`Acquire` memory ordering for thread safety

**Key Components**:

1. **BucketConfirmationManager**: Manages confirmation state per bucket, tracking multiple partitions
2. **PartitionConfirmationState**: Tracks watermarks and unconfirmed events per partition
3. **ConfirmationActor**: Background process that handles confirmation updates and reconciliation
4. **Recovery Mechanisms**: Administrative tools to handle stuck events and corrupted data

### Benefits Achieved

1. **Performance**: Fast local reads without network overhead
2. **Consistency**: Gap-free watermarks ensure ordered event visibility
3. **Resilience**: Individual confirmation failures don't block partition progress
4. **Fault Tolerance**: Background reconciliation and administrative recovery tools
5. **Scalability**: Bucket-based organization aligns with physical storage boundaries

### Operational Guarantees

- **For Readers**: Always see a consistent, gap-free sequence of confirmed events
- **For Writers**: Events are durably stored immediately, confirmed asynchronously
- **For Operations**: Monitoring and recovery tools for handling edge cases
- **For Recovery**: Multiple fallback mechanisms for handling failures and corruption

This solution provides event sourcing systems with the strong consistency they require while maintaining the performance and availability characteristics needed for a distributed database.