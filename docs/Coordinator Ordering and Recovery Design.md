## Problem Statement

Eventus uses a deterministic coordinator model where any node can coordinate writes for a partition, but nodes are tried in a specific order. The original design used simple peer ID ordering, which created two major issues:

### 1. Recovery Complexity

When a node restarts, it potentially has sequence gaps and cannot safely coordinate writes until it catches up. This creates complex recovery scenarios:

- Node must physically write all missing events in sequence order (due to disk ordering requirements)
- In high-throughput systems, the "moving target" problem emerges where the node can never catch up
- Requires complex state management (recovery mode, gap detection, etc.)

### 2. Read-Your-Own-Writes Problem

Since any node can coordinate a write, clients may read from a different node than the one that coordinated their write, potentially not seeing their own events if confirmation propagation is slow.

## Solution: Startup Timestamp-Based Ordering

Instead of complex recovery modes, we solve this by making the coordinator selection algorithm naturally avoid recently restarted nodes.

### Core Concept

Each node advertises its startup timestamp along with partition ownership. Coordinator ordering becomes:

1. **Primary sort**: Startup timestamp (ascending) - older nodes preferred
2. **Tie-breaker**: Peer ID (ascending) - deterministic fallback

```rust
fn get_coordinator_order(&self, partition_id: PartitionId) -> Vec<RemoteActorRef<ClusterActor>> {
    let mut replicas = self.get_replicas_for_partition(partition_id);
    
    replicas.sort_by(|a, b| {
        let a_info = self.get_node_info(a.peer_id());
        let b_info = self.get_node_info(b.peer_id());
        
        match a_info.startup_timestamp.cmp(&b_info.startup_timestamp) {
            Ordering::Equal => a_info.peer_id.cmp(&b_info.peer_id),
            other => other, // Earlier timestamp wins
        }
    });
    
    replicas
}
```

## What Timestamp Ordering Actually Achieves

The timestamp approach provides significant benefits but doesn't eliminate all recovery complexity:

### ✅ What It Solves

1. **Coordinator selection complexity** - stable nodes naturally become coordinators
2. **Reduces recovery urgency** - nodes don't need to coordinate immediately upon restart
3. **Graceful degradation** - system operates with fewer coordinators during recovery
4. **Maintains high availability** - any node can still coordinate if others fail

### ⚠️ What It Doesn't Solve

1. **Quorum participation** - nodes still need to catch up to participate in writes
2. **Physical ordering constraints** - events must still be written sequentially to disk
3. **Availability during recovery** - too many recovering nodes can impact quorum achievement

### The Real Benefit

Instead of complex recovery state management, the system naturally:

- **Prefers stable coordinators** while allowing recovering nodes to gradually participate
- **Handles the moving target problem** through per-write participation decisions
- **Provides clear operational visibility** - recovery progress measured by successful write participation

## Implementation Details

### Node Advertisement

```rust
#[derive(Debug, Serialize, Deserialize)]
struct NodeAdvertisement {
    peer_id: PeerId,
    startup_timestamp: u64,  // Unix timestamp when node started
    partitions: HashSet<PartitionId>,
    heartbeat_timestamp: u64,
}
```

### Startup Timestamp Generation

```rust
let startup_timestamp = SystemTime::now()
    .duration_since(UNIX_EPOCH)?
    .as_secs();
```

## Clock Skew Considerations

Clock skew between nodes may occasionally cause incorrect coordinator preference ordering. However:

### Impact Assessment

- **Correctness**: Zero impact - system remains deterministic and consistent
- **Performance**: Minimal impact - slightly suboptimal coordinator selection in edge cases
- **Availability**: Zero impact - fault tolerance preserved
- **Frequency**: Very low - most nodes have reasonably synchronized clocks

### Why This Is Acceptable

- 99%+ of coordinator selections will be correct
- Edge cases only occur near restart boundaries with significant clock skew
- Much simpler than alternatives (NTP synchronization, logical clocks, etc.)
- System self-corrects over time as timestamps age

## Recovery: Simplified but Still Required

While timestamp-based ordering significantly reduces recovery complexity, nodes still need to catch up on missing events to participate in quorum.

### Why Recovery is Still Needed

Even though recently restarted nodes don't coordinate writes, they must still **participate** as replicas:

```rust
// Scenario: Node A restarted with gaps
// Node A: events 1-1000 (missing 1001-5000)
// Node B: events 1-5000 (up to date, becomes coordinator due to older timestamp)
// Node C: events 1-5000 (up to date)

// New write for event 5001:
// Node B coordinates ✅
// Node C participates ✅  
// Node A cannot participate ❌ (missing prerequisite events 1001-5000)
// Result: Only 2/3 replicas participate, but quorum still achieved
```

### Per-Write Participation Strategy

Instead of complex recovery state management, use **per-write participation**:

```rust
async fn handle_replicate_write(&mut self, msg: ReplicateWrite) -> Result<AppendResult> {
    let required_sequence = msg.transaction.expected_partition_sequence();
    let my_latest = self.database.get_last_sequence(msg.transaction.partition_id()).await?;
    
    if my_latest + 1 == required_sequence {
        // I have all prerequisite events, can participate
        tokio::spawn(self.database.append_events(msg.transaction));
    } else {
        // I'm missing events, cannot participate - continue/start recovery
        tokio::spawn(start_or_continue_recovery());
        
        Err(ClusterError::SequenceGap)
    }
}
```

### Benefits of This Approach

1. **No explicit "recovery complete" state** - node gradually increases participation
2. **Handles moving target problem** - each write decision is independent
3. **Self-correcting** - if node falls behind again, automatically reduces participation
4. **Graceful degradation** - system works with reduced replica count during catch-up

### Recovery Process

- **Background catch-up** continues until node can participate in new writes
- **Natural completion detection** - when node successfully participates, it's caught up
- **Indefinite recovery acceptable** - system operates with reduced replicas during high-throughput periods
- **Monitoring critical** - alert when healthy replica count approaches quorum threshold

## Read-Your-Own-Writes Solution

For the read consistency issue, implement coordinator hints in write responses:

```rust
pub struct WriteResult {
    pub append_result: AppendResult,
    pub coordinator_hint: NodeId, // Node that coordinated this write
}

// Clients try coordinator first, then fall back to normal replica ordering
```

This provides fast read-your-own-writes without complex consensus mechanisms.

## Key Design Principles

1. **Simplicity Over Perfection**: Accept minor clock skew impact vs complex recovery state management
2. **Per-Write Decisions**: Each write participation is independent, avoiding complex global state
3. **Graceful Degradation**: System operates with reduced replicas rather than blocking on recovery
4. **Natural Coordination**: Timestamp ordering makes stable nodes preferred coordinators without forced handoffs
5. **Self-Healing**: System naturally optimizes over time as nodes age and catch up

This design significantly reduces the complexity of coordinator selection and recovery management while maintaining the robustness and availability of a distributed event sourcing system.