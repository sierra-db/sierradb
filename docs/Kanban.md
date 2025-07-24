## ✅ DONE

### Core Infrastructure

- [x] **Basic ClusterActor with libp2p integration** - Actor framework with remote messaging
- [x] **Write request routing (`ExecuteTransaction`)** - Routes writes to appropriate coordinator
- [x] **Replica write handling (`ReplicateWrite`)** - Handles replication requests from coordinator
- [x] **Write confirmation protocol (`ConfirmTransaction`)** - Confirms successful writes to replicas
- [x] **Circuit breaker pattern** - Prevents cascade failures during high error rates
- [x] **Watermark tracking system** - Tracks confirmed events per partition
- [x] **Write timeout handling** - Prevents hung writes with configurable timeouts
- [x] **Quorum calculation logic** - Determines required confirmations for success
- [x] **Request forwarding with loop prevention** - Prevents infinite forwarding loops
- [x] **Confirmation persistence with retry** - Retries confirmation updates on failure

### Error Handling & Validation

- [x] **Basic partition ownership validation** - Checks if node owns partition
- [x] **Stale write detection** - Prevents processing writes from restarted coordinators
- [x] **Invalid sender validation** - Validates coordinator identity
- [x] **Transaction validation in replicas** - Validates event IDs and sequences
- [x] **WriteError enum with comprehensive error types** - Complete error taxonomy

### Persistence & Recovery

- [x] **Confirmation state persistence** - Saves confirmation state to disk
- [x] **Bucketed confirmation management** - Organizes confirmations by bucket
- [x] **Atomic watermark updates** - Thread-safe watermark advancement
- [x] **Admin operations for recovery** - Force watermark and skip event operations

## 🔄 DOING

### Coordinator Selection & Node Management

- [ ] **Deterministic coordinator selection algorithm**
    - Implement oldest-running-node selection based on startup timestamp
    - Add tiebreaker logic using peer_id
    - Current: Basic replica selection, needs deterministic ordering

### Write Buffer Management

- [ ] **Sequential write buffering per partition**
    - Implement `PartitionWriteBuffer` for out-of-order writes
    - Add buffer processing with strict sequence ordering
    - Current: Basic validation, no buffering for gaps

## 📋 TODO

### Phase 1: Core Protocol Implementation

#### Coordinator Selection & Topology

- [ ] **Implement startup timestamp tracking**
    - Add startup_timestamp to node metadata
    - Integrate with topology manager
    - Sort replicas by (startup_timestamp, peer_id)
- [ ] **Enhanced coordinator selection algorithm**
    - Implement `get_coordinator_for_partition()` function
    - Add automatic failover when coordinator becomes unhealthy
    - Add coordinator validation in all write phases
- [ ] **Node health monitoring integration**
    - Integrate health status with coordinator selection
    - Add heartbeat-based health detection
    - Handle network partition scenarios

#### Sequential Processing & Buffering

- [ ] **Partition write buffer implementation**
    ```rust
    struct PartitionWriteBuffer {
        partition_id: PartitionId,
        next_expected_seq: AtomicU64,
        buffered_writes: BTreeMap<u64, BufferedWrite>,
        processing_task: Option<JoinHandle<()>>,
    }
    ```
    
- [ ] **Buffer processing algorithm**
    - Process writes in strict sequence order
    - Handle retry logic for failed writes
    - Implement buffer timeout and overflow handling
- [ ] **Sequence gap detection and handling**
    - Detect when replica is behind coordinator
    - Implement catch-up requests to coordinator
    - Buffer out-of-order writes correctly

#### Write Coordination Improvements

- [ ] **Enhanced write validation in replicas**
    - Implement comprehensive coordinator validation
    - Add startup timestamp verification
    - Strengthen sequence validation logic
- [ ] **Improved partition sequence handling**
    - Add expected version checks in transaction creation
    - Implement gapless sequence guarantee
    - Add sequence mismatch recovery

### Phase 2: Failure Recovery & Resilience

#### Replica Catch-Up Protocol

- [ ] **Partition sync request/response**
    ```rust
    struct PartitionSyncRequest {
        partition_id: PartitionId,
        from_sequence: u64,
        to_sequence: Option<u64>,
    }
    ```
    
- [ ] **Missing event retrieval**
    - Request missing events from coordinator
    - Apply events in correct order
    - Resume normal processing after catch-up
- [ ] **Background catch-up mechanism**
    - Detect when replica falls behind
    - Automatically initiate catch-up process
    - Handle catch-up failures gracefully

#### Coordinator Failover

- [ ] **Coordinator failure detection**
    - Monitor coordinator health and availability
    - Detect network partitions affecting coordinator
    - Trigger failover when coordinator is unreachable
- [ ] **New coordinator initialization**
    - Assess current state across replicas
    - Sync to highest committed sequence
    - Resume write coordination seamlessly
- [ ] **State reconciliation during failover**
    - Query replica states to find highest committed sequence
    - Handle partial writes during coordinator failure
    - Ensure consistency across replica set

#### Recovery Mechanisms

- [ ] **Failed replica recovery**
    - Background reconciliation for failed replicas
    - Retry failed replications with exponential backoff
    - Mark degraded replicas and schedule recovery
- [ ] **Partition state validation**
    - Validate confirmation state against actual events
    - Repair inconsistent watermarks
    - Generate validation reports

### Phase 3: Performance Optimizations

#### Concurrent Write Optimization

- [ ] **Immediate sequence assignment**
    - Assign sequences without waiting for previous writes
    - Enable coordinator to work ahead of replicas
    - Maintain sequential processing guarantee
- [ ] **Pipelined write coordination**
    - Start replicating write N+1 while write N is confirming
    - Optimize coordinator throughput
    - Maintain ordered completion semantics
- [ ] **Parallel replication**
    - Send to all replicas simultaneously
    - Optimize network utilization
    - Reduce write latency

#### Buffer Management Optimization

- [ ] **Buffer size management**
    - Implement configurable buffer limits
    - Add memory pressure handling
    - Evict expired buffered writes
- [ ] **Batch confirmation optimization**
    - Group multiple confirmations in single message
    - Reduce network overhead
    - Optimize confirmation processing

### Phase 4: Monitoring & Operations

#### Metrics & Observability

- [ ] **Write protocol metrics**
    - Track write latency and throughput
    - Monitor replication lag per partition
    - Measure coordinator failover times
- [ ] **Health monitoring**
    - Confirmation gap monitoring
    - Stuck event detection
    - Replica synchronization status
- [ ] **Administrative operations**
    - Force coordinator failover
    - Manual partition sync operations
    - Degraded replica management

#### Testing & Validation

- [ ] **Chaos engineering tests**
    - Coordinator failure during writes
    - Network partition scenarios
    - Replica crash recovery
- [ ] **Performance benchmarks**
    - Write throughput under various loads
    - Latency measurements across protocol phases
    - Memory usage optimization
- [ ] **Integration testing**
    - End-to-end write protocol testing
    - Multi-node cluster testing
    - Long-running stability tests

### Phase 5: Advanced Features

#### Protocol Enhancements

- [ ] **Write batching**
    - Batch multiple transactions in single replication
    - Optimize for high-throughput scenarios
    - Maintain individual transaction atomicity
- [ ] **Adaptive quorum sizing**
    - Adjust quorum requirements based on cluster health
    - Handle scenarios with reduced replica availability
    - Maintain consistency guarantees
- [ ] **Cross-partition transaction support**
    - Coordinate writes across multiple partitions
    - Implement distributed commit protocol
    - Handle cross-partition consistency

#### Operational Features

- [ ] **Online schema evolution**
    - Handle schema changes during replication
    - Version compatibility checking
    - Graceful schema migration
- [ ] **Partition splitting/merging**
    - Handle partition topology changes
    - Maintain write ordering during transitions
    - Coordinate state transfer

## Priority Order

### High Priority (Core Protocol)

1. Deterministic coordinator selection algorithm
2. Sequential write buffering per partition
3. Sequence gap detection and handling
4. Coordinator failover detection

### Medium Priority (Resilience)

1. Replica catch-up protocol
2. Failed replica recovery
3. State reconciliation during failover
4. Performance optimizations

### Low Priority (Advanced Features)

1. Monitoring and metrics
2. Administrative operations
3. Advanced protocol features
4. Cross-partition transactions

---

## Notes

- **Current code quality**: The foundation is solid with good error handling and circuit breaker patterns
- **Major gap**: Sequential buffering and coordinator selection need implementation
- **Architecture strength**: Good separation of concerns with actor model
- **Focus area**: Complete Phase 1 before moving to optimizations
- **Testing**: Each phase should have comprehensive tests before proceeding