## Overview

SierraDB is a distributed database designed specifically for storing events, providing a robust, scalable architecture for event streaming applications. The write protocol ensures events are stored in a consistent, ordered manner across a distributed cluster while maintaining fault tolerance.

## Core Principles

- **Single Writer Per Partition**: Each partition has a designated leader that serializes all writes 
- **Strong Consistency**: Events have monotonic, gap-less versioning within streams and partitions
- **Quorum-Based Confirmation**: Writes require confirmation from a majority of replicas
- **Durability First**: Events are written to disk immediately, then marked confirmed after quorum
- **Fault Tolerance**: The system can recover from node failures and network partitions

## Distributed Write Protocol

### Phase 1: Leader Selection & Version Assignment

1. **Client Request Forwarding**
   - Client sends write request to any node in the cluster
   - Request is forwarded to the partition leader (determined by hashing the stream ID)
   - Leaders are assigned using a term-based leadership protocol

2. **Version Reservation**
   - Leader validates its leadership term
   - Leader reserves the next sequential stream version for the event
   - Leader reserves the next sequential partition version for the event
   - These ensure monotonic, gap-less ordering within both stream and partition contexts

3. **Event Hydration**
   - Leader assigns these versions to all events in the append request
   - Leader prepares the events for distribution across the cluster

### Phase 2: Distributed Replication

4. **Replication Distribution**
   - Leader determines which partitions should store replicas based on the replication factor
   - Leader broadcasts write requests to all replica partitions 
   - The distribution follows a deterministic algorithm to ensure even data placement

5. **Immediate Persistence**
   - Each replica immediately writes events to disk in an "unconfirmed" state
   - This ensures durability even if a node fails before confirmation
   - Unconfirmed events are not yet visible to consumers

6. **Response Collection**
   - Replicas respond to the leader with success or failure of their write operation
   - Failures might occur due to leadership conflicts, version mismatches, or disk issues
   - Leader collects these responses until a quorum is reached

7. **Quorum Determination**
   - A successful quorum is defined as (replication_factor / 2) + 1 successful writes
   - For example, with replication factor 3, at least 2 successful writes are required
   - Leader tracks which replicas succeeded and which failed

### Phase 3: Commitment & Response

8. **Client Response**
   - If quorum is achieved, leader responds to client with success and assigned versions
   - If quorum cannot be achieved (due to too many failures), leader responds with an error
   - The client now knows if their write was successful and what versions were assigned

9. **Confirmation Broadcasting**
   - Leader broadcasts a confirmation message to all replicas that successfully wrote the data
   - This tells the replicas that quorum was reached and the write is officially committed

10. **Confirmation Marking**
    - Replicas mark the events as "confirmed" in their local storage
    - Confirmed events become visible to consumers
    - The write operation is now complete for this event

## Failure Handling & Recovery

### Failure Conditions

- **Leadership Contention**: Replica rejects write if it believes another node is the leader
- **Version Mismatch**: Replica rejects if stream/partition versions don't align with local state
- **Timeout**: Leader fails the operation if quorum isn't reached within timeout period
- **Node Failure**: Background reconciliation process handles any partially confirmed writes

### Recovery Mechanisms

- **Event State Tracking**: Events explicitly track "unconfirmed" vs "confirmed" state on disk
- **Background Reconciliation**: Processes run to resolve the state of unconfirmed events
- **Selective Visibility**: Only confirmed events are made visible to consumers
- **Leader Failover**: If a leader fails, a new leader is elected based on the hash ring
- **Version Reconciliation**: New leaders validate and potentially correct version inconsistencies

## Implementation with Actors

The protocol is implemented using an actor model, with one actor per partition:

- Each partition is managed by a dedicated actor that handles all writes to that partition
- The actor serves as the "single writer" for the partition, serializing all operations
- Writes are processed one at a time, with each write completing (reaching quorum) before the next is processed
- Different partition actors operate independently, providing horizontal scalability
- The actor model naturally supports the message-passing nature of the distributed protocol

## Performance Considerations

While the protocol prioritizes consistency and durability over raw performance, several factors help maintain good throughput:

- **Partition Distribution**: Work is spread across many partitions, allowing horizontal scaling
- **Independent Processing**: Different streams in different partitions can progress independently
- **Immediate Disk Writes**: No waiting for confirmation before writing to disk reduces recovery complexity
- **Background Reconciliation**: Recovery processes happen asynchronously without blocking normal operations
- **Batching Potential**: Multiple events can be batched into a single write operation within a stream

This protocol strikes a balance between strong consistency guarantees, fault tolerance, and performance, making it suitable for event sourcing applications where data integrity is critical.
