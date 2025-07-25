use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrayvec::ArrayVec;
use libp2p::PeerId;
use sierradb::MAX_REPLICATION_FACTOR;
use sierradb::bucket::{PartitionHash, PartitionId};
use tracing::{info, warn};

use crate::ClusterKey;

use super::messages::OwnershipMessage;

/// Manages replication-based assignment of partitions to multiple nodes
#[derive(Debug, Clone)]
pub struct TopologyManager<T: ClusterKey> {
    pub local_cluster_ref: T,
    pub local_node_index: usize,
    pub total_node_count: usize,
    pub num_partitions: u16,
    pub bucket_count: u16,
    pub replication_factor: u8,
    pub heartbeat_timeout: Duration,
    pub alive_since: u64,

    // Partitions where this node is a replica
    pub assigned_partitions: HashSet<PartitionId>,

    // Global state tracking
    pub partition_replicas: HashMap<PartitionId, ArrayVec<T, MAX_REPLICATION_FACTOR>>,
    pub active_nodes: HashMap<PeerId, (u64, usize)>, // PeerId -> (alive_since, node_index)
    pub node_heartbeats: HashMap<PeerId, Instant>,
    pub cluster_nodes: HashMap<PeerId, T>, // Track cluster refs by peer_id
}

impl<T: ClusterKey> TopologyManager<T> {
    pub fn new(
        local_cluster_ref: T,
        local_node_index: usize,
        total_node_count: usize,
        num_partitions: u16,
        bucket_count: u16,
        replication_factor: u8,
        heartbeat_timeout: Duration,
    ) -> Self {
        let alive_since = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_secs();

        let local_peer_id = *local_cluster_ref.id().peer_id().unwrap();

        // Calculate which partitions this node should own
        let assigned_partitions = Self::calculate_assigned_partitions(
            local_node_index,
            total_node_count,
            num_partitions,
            bucket_count,
            replication_factor,
        );

        // Initialize active nodes with just this node
        let active_nodes = HashMap::from_iter([(local_peer_id, (alive_since, local_node_index))]);

        // Initialize cluster nodes tracking
        let cluster_nodes = HashMap::from_iter([(local_peer_id, local_cluster_ref.clone())]);

        // Initialize partition replicas - we'll populate this as we discover other
        // nodes
        let partition_replicas = HashMap::new();

        let mut manager = Self {
            local_cluster_ref,
            local_node_index,
            total_node_count,
            num_partitions,
            bucket_count,
            replication_factor,
            heartbeat_timeout,
            alive_since,
            assigned_partitions,
            partition_replicas,
            active_nodes,
            node_heartbeats: HashMap::new(),
            cluster_nodes,
        };

        // Initialize our own heartbeat
        manager
            .node_heartbeats
            .insert(local_peer_id, Instant::now());

        // Recalculate partition assignments to include ourselves
        manager.recalculate_partition_assignments();

        manager
    }

    /// Calculate which partitions a node should own based on bucket assignment
    /// This uses the exact same logic as the original
    /// assigned_buckets/assigned_partitions code
    fn calculate_assigned_partitions(
        node_index: usize,
        total_node_count: usize,
        num_partitions: u16,
        bucket_count: u16,
        replication_factor: u8,
    ) -> HashSet<PartitionId> {
        let effective_replication_factor = replication_factor.min(total_node_count as u8) as usize;

        // First, find which buckets this node owns
        let mut assigned_buckets = HashSet::new();
        for bucket_id in 0..bucket_count {
            let primary_node = bucket_id as usize % total_node_count;

            // Check if current node is one of the replica nodes for this bucket
            for replica_offset in 0..effective_replication_factor {
                let replica_node = (primary_node + replica_offset) % total_node_count;
                if replica_node == node_index {
                    assigned_buckets.insert(bucket_id);
                    break;
                }
            }
        }

        // Then, find which partitions map to those buckets
        (0..num_partitions)
            .filter(|p| assigned_buckets.contains(&(p % bucket_count)))
            .collect()
    }

    /// Calculate which nodes should be replicas for a partition
    fn calculate_partition_replicas(
        partition_id: PartitionId,
        bucket_count: u16,
        total_node_count: usize,
        replication_factor: u8,
        known_nodes: &HashMap<usize, T>, // node_index -> cluster_ref
    ) -> ArrayVec<T, MAX_REPLICATION_FACTOR> {
        let mut replicas = ArrayVec::new();

        if known_nodes.is_empty() {
            return replicas;
        }

        // Use the same bucket logic as assigned_partitions
        let bucket_id = partition_id % bucket_count;
        let primary_node = bucket_id as usize % total_node_count;
        let effective_replication_factor = replication_factor.min(total_node_count as u8) as usize;

        for replica_offset in 0..effective_replication_factor {
            let replica_node_index = (primary_node + replica_offset) % total_node_count;

            // Find the node with this configured index
            if let Some(node_ref) = known_nodes.get(&replica_node_index) {
                replicas.push(node_ref.clone());
            }
        }

        replicas
    }

    pub fn get_assigned_partitions(&self) -> &HashSet<PartitionId> {
        &self.assigned_partitions
    }

    /// Determine whether this node has the specified partition
    pub fn has_partition(&self, partition_id: PartitionId) -> bool {
        self.assigned_partitions.contains(&partition_id)
    }

    /// Get the availability status of a partition (true if any replica is
    /// active)
    pub fn is_partition_available(&self, partition_id: PartitionId) -> bool {
        self.partition_replicas
            .get(&partition_id)
            .map(|replicas| {
                replicas.iter().any(|replica| {
                    self.active_nodes
                        .contains_key(replica.id().peer_id().unwrap())
                })
            })
            .unwrap_or(false)
    }

    /// Get all available replica nodes for a partition, sorted by alive_since
    /// for consistency
    pub fn get_available_replicas(
        &self,
        partition_id: PartitionId,
    ) -> ArrayVec<(T, u64), MAX_REPLICATION_FACTOR> {
        self.partition_replicas
            .get(&partition_id)
            .map(|replicas| {
                let mut available: ArrayVec<_, MAX_REPLICATION_FACTOR> = replicas
                    .iter()
                    .filter_map(|replica| {
                        self.active_nodes
                            .get(replica.id().peer_id().unwrap())
                            .map(|(alive_since, _)| (replica.clone(), *alive_since))
                    })
                    .collect();

                // Sort by alive_since for consistent ordering
                available.sort_by(|(a_replica, a_alive_since), (b_replica, b_alive_since)| {
                    a_alive_since
                        .cmp(b_alive_since)
                        .then(a_replica.cmp(b_replica))
                });

                available
            })
            .unwrap_or_default()
    }

    /// Get the primary replica (first in deterministic order) for a partition
    pub fn get_primary_replica(&self, partition_id: PartitionId) -> Option<T> {
        self.partition_replicas
            .get(&partition_id)
            .and_then(|replicas| replicas.first())
            .cloned()
    }

    /// Determine if a partition key is available on this node
    pub fn is_key_available_locally(&self, partition_hash: PartitionHash) -> bool {
        let partition_id = partition_hash % self.num_partitions;
        self.has_partition(partition_id)
    }

    /// Get a list of available replica nodes for a given partition hash
    pub fn get_available_replicas_for_key(
        &self,
        partition_hash: PartitionHash,
    ) -> ArrayVec<(T, u64), MAX_REPLICATION_FACTOR> {
        let partition_id = partition_hash % self.num_partitions;
        self.get_available_replicas(partition_id)
    }

    /// Handle node coming online
    pub fn on_node_connected(
        &mut self,
        cluster_ref: T,
        owned_partitions: &HashSet<PartitionId>,
        alive_since: u64,
        node_index: usize,
        total_node_count: usize,
    ) -> Option<OwnershipMessage<T>> {
        let peer_id = *cluster_ref.id().peer_id().unwrap();
        info!("node connected: {peer_id} (index: {node_index})");

        // Validate node count consistency
        if total_node_count != self.total_node_count {
            warn!(
                "Node {peer_id} reports total_node_count={total_node_count} but expected {}",
                self.total_node_count
            );
        }

        // Register this node as active and track cluster ref
        self.active_nodes.insert(peer_id, (alive_since, node_index));
        self.node_heartbeats.insert(peer_id, Instant::now());
        self.cluster_nodes.insert(peer_id, cluster_ref.clone());

        // Recalculate partition assignments with the new node
        self.recalculate_partition_assignments();

        // Validate that the node's claimed partitions match our expected assignments
        let expected_partitions = Self::calculate_assigned_partitions(
            node_index,
            self.total_node_count,
            self.num_partitions,
            self.bucket_count,
            self.replication_factor,
        );

        if owned_partitions != &expected_partitions {
            warn!(
                "Node {peer_id} claims partitions ({} items) but expected ({} items)",
                owned_partitions.len(),
                expected_partitions.len()
            );
        }

        // Create ownership response message
        Some(OwnershipMessage::OwnershipResponse {
            partition_replicas: self
                .partition_replicas
                .iter()
                .flat_map(|(partition_id, cluster_refs)| {
                    cluster_refs
                        .into_iter()
                        .map(move |cluster_ref| (partition_id, cluster_ref.clone()))
                })
                .fold(
                    HashMap::<T, HashSet<PartitionId>>::new(),
                    |mut acc, (partition_id, cluster_ref)| {
                        acc.entry(cluster_ref).or_default().insert(*partition_id);
                        acc
                    },
                ),
            active_nodes: self.active_nodes.clone(),
        })
    }

    /// Handle node going offline
    pub fn on_node_disconnected(&mut self, peer_id: &PeerId) {
        if let Some((_, node_index)) = self.active_nodes.remove(peer_id) {
            info!("node disconnected: {peer_id} (index: {node_index})");
        }
        self.node_heartbeats.remove(peer_id);
        self.cluster_nodes.remove(peer_id);

        // Recalculate partition assignments without this node
        self.recalculate_partition_assignments();
    }

    /// Process a heartbeat from a node
    pub fn on_heartbeat(
        &mut self,
        cluster_ref: T,
        owned_partitions: &HashSet<PartitionId>,
        alive_since: u64,
        node_index: usize,
        total_node_count: usize,
    ) -> bool {
        let peer_id = *cluster_ref.id().peer_id().unwrap();

        // Update heartbeat timestamp and track cluster ref
        self.node_heartbeats.insert(peer_id, Instant::now());
        self.cluster_nodes.insert(peer_id, cluster_ref.clone());

        let mut status_changed = false;

        // Validate node count consistency
        if total_node_count != self.total_node_count {
            warn!(
                "Node {peer_id} reports total_node_count={total_node_count} but expected {}",
                self.total_node_count
            );
        }

        // If the node wasn't active before, mark it active now
        if let Some((_existing_alive_since, existing_index)) = self.active_nodes.get(&peer_id) {
            if *existing_index != node_index {
                warn!(
                    "Node {peer_id} changed index from {} to {}",
                    existing_index, node_index
                );
                self.active_nodes.insert(peer_id, (alive_since, node_index));
                self.recalculate_partition_assignments();
                status_changed = true;
            }
        } else {
            info!("node {peer_id} is now active due to heartbeat (index: {node_index})");
            self.active_nodes.insert(peer_id, (alive_since, node_index));
            self.recalculate_partition_assignments();
            status_changed = true;
        }

        // Validate that the node's partition assignments are correct
        let expected_partitions = Self::calculate_assigned_partitions(
            node_index,
            self.total_node_count,
            self.num_partitions,
            self.bucket_count,
            self.replication_factor,
        );

        if owned_partitions != &expected_partitions {
            warn!(
                "Node {peer_id} heartbeat shows {} partitions but expected {}",
                owned_partitions.len(),
                expected_partitions.len(),
            );
        }

        status_changed
    }

    /// Check for node failures based on heartbeat timeouts
    pub fn check_heartbeat_timeouts(&mut self) -> bool {
        let now = Instant::now();
        let local_peer_id = *self.local_cluster_ref.id().peer_id().unwrap();

        let timed_out_peers: Vec<_> = self
            .node_heartbeats
            .iter()
            .filter(|&(peer_id, last_heartbeat)| {
                peer_id != &local_peer_id
                    && self.active_nodes.contains_key(peer_id)
                    && now.duration_since(*last_heartbeat) > self.heartbeat_timeout
            })
            .map(|(peer_id, _)| *peer_id)
            .collect();

        let mut status_changed = false;
        for peer_id in timed_out_peers {
            if let Some((_, node_index)) = self.active_nodes.remove(&peer_id) {
                info!("node {peer_id} timed out - marking inactive (index: {node_index})");
                self.cluster_nodes.remove(&peer_id);
                status_changed = true;
            }
        }

        if status_changed {
            self.recalculate_partition_assignments();
        }

        status_changed
    }

    /// Handle an ownership response message (synchronize state)
    pub fn handle_ownership_response(
        &mut self,
        partition_replicas: &HashMap<PartitionId, ArrayVec<T, MAX_REPLICATION_FACTOR>>,
        active_nodes: HashMap<PeerId, (u64, usize)>,
    ) {
        // Update our partition replica assignments
        self.partition_replicas = partition_replicas.clone();

        // Update active nodes, but keep ourselves active
        let local_peer_id = *self.local_cluster_ref.id().peer_id().unwrap();
        self.active_nodes = active_nodes;
        self.active_nodes
            .insert(local_peer_id, (self.alive_since, self.local_node_index));

        // Extract cluster refs from partition replicas to update our tracking
        for replicas in partition_replicas.values() {
            for cluster_ref in replicas {
                self.cluster_nodes
                    .insert(*cluster_ref.id().peer_id().unwrap(), cluster_ref.clone());
            }
        }

        // Update heartbeats for all active nodes
        let now = Instant::now();
        for peer_id in self.active_nodes.keys() {
            self.node_heartbeats.insert(*peer_id, now);
        }

        info!("updated partition replica assignments from remote information");
    }

    /// Recalculate partition assignments when cluster membership changes
    fn recalculate_partition_assignments(&mut self) {
        // Build a map of known nodes by their configured index
        let known_nodes: HashMap<usize, T> = self
            .active_nodes
            .iter()
            .filter_map(|(peer_id, (_, node_index))| {
                self.cluster_nodes
                    .get(peer_id)
                    .map(|cluster_ref| (*node_index, cluster_ref.clone()))
            })
            .collect();

        info!(
            "recalculating partition assignments with {} active nodes",
            known_nodes.len()
        );

        // Recalculate all partition assignments
        for partition_id in 0..self.num_partitions {
            let replicas = Self::calculate_partition_replicas(
                partition_id,
                self.bucket_count,
                self.total_node_count,
                self.replication_factor,
                &known_nodes,
            );
            self.partition_replicas.insert(partition_id, replicas);
        }

        // Log a sample for debugging
        if let Some((partition_id, replicas)) = self.partition_replicas.iter().next() {
            info!("partition {partition_id} has {} replicas", replicas.len());
        }
    }

    /// Ensure local partition assignments are consistent with our configuration
    pub fn ensure_local_partitions(&mut self) {
        let expected_partitions = Self::calculate_assigned_partitions(
            self.local_node_index,
            self.total_node_count,
            self.num_partitions,
            self.bucket_count,
            self.replication_factor,
        );

        if self.assigned_partitions != expected_partitions {
            warn!(
                "Local partition assignments ({} items) don't match expected ({} items)",
                self.assigned_partitions.len(),
                expected_partitions.len()
            );
            self.assigned_partitions = expected_partitions;
        }
    }
}

#[cfg(test)]
mod replica_manager_tests {
    use kameo::actor::ActorId;

    use crate::test_helpers::{
        create_test_manager, create_test_manager_with_params, create_test_peer_id,
        validate_replication_invariants,
    };

    use super::*;

    #[test]
    fn test_new_manager_initializes_correctly() {
        let (manager, peer_id) = create_test_manager(0, 3);

        assert_eq!(manager.local_node_index, 0);
        assert_eq!(manager.total_node_count, 3);
        assert_eq!(manager.num_partitions, 100);
        assert_eq!(manager.bucket_count, 10);
        assert_eq!(manager.replication_factor, 3);
        assert!(manager.active_nodes.contains_key(&peer_id));
        assert!(manager.cluster_nodes.contains_key(&peer_id));
        assert!(!manager.assigned_partitions.is_empty());
    }

    #[test]
    fn test_partition_assignment_deterministic() {
        let (manager1, _) = create_test_manager(1, 3);
        let (manager2, _) = create_test_manager(1, 3);

        assert_eq!(manager1.assigned_partitions, manager2.assigned_partitions);
    }

    #[test]
    fn test_partition_assignment_different_indices() {
        let (manager0, _) = create_test_manager(0, 3);
        let (manager1, _) = create_test_manager(1, 3);

        // With replication_factor=3 and total_node_count=3, all nodes should have
        // identical partition assignments (full replication)
        assert_eq!(manager0.assigned_partitions, manager1.assigned_partitions);

        // Verify they both own all partitions
        assert_eq!(manager0.assigned_partitions.len(), 100); // All 100 partitions
        assert_eq!(manager1.assigned_partitions.len(), 100);
    }

    #[test]
    fn test_partition_assignment_partial_replication() {
        // Test scenario where nodes have different assignments
        let (manager0, _) = create_test_manager_with_params(0, 5, 100, 10, 3); // 5 nodes, RF=3
        let (manager1, _) = create_test_manager_with_params(1, 5, 100, 10, 3);
        let (manager2, _) = create_test_manager_with_params(2, 5, 100, 10, 3);
        let (manager3, _) = create_test_manager_with_params(3, 5, 100, 10, 3);
        let (manager4, _) = create_test_manager_with_params(4, 5, 100, 10, 3);

        // With RF=3 and 5 nodes, assignments should be different but overlapping
        assert_ne!(manager0.assigned_partitions, manager1.assigned_partitions);
        assert_ne!(manager1.assigned_partitions, manager2.assigned_partitions);

        // But they should have significant overlap due to replication
        let overlap_0_1: HashSet<_> = manager0
            .assigned_partitions
            .intersection(&manager1.assigned_partitions)
            .collect();
        assert!(
            !overlap_0_1.is_empty(),
            "Should have some overlapping partitions"
        );

        // Each partition should be owned by exactly 3 nodes
        let all_partitions: HashSet<u16> = (0..100).collect();
        for partition_id in &all_partitions {
            let owners = [
                manager0.assigned_partitions.contains(partition_id),
                manager1.assigned_partitions.contains(partition_id),
                manager2.assigned_partitions.contains(partition_id),
                manager3.assigned_partitions.contains(partition_id),
                manager4.assigned_partitions.contains(partition_id),
            ]
            .iter()
            .filter(|&&x| x)
            .count();

            assert_eq!(
                owners, 3,
                "Partition {partition_id} should have exactly 3 replicas"
            );
        }
    }

    #[test]
    fn test_calculate_assigned_partitions_single_node() {
        let partitions =
            TopologyManager::<ActorId>::calculate_assigned_partitions(0, 1, 100, 10, 3);

        // With only 1 node, it should own all partitions
        assert_eq!(partitions.len(), 100);
        assert_eq!(partitions, (0..100).collect::<HashSet<_>>());
    }

    #[test]
    fn test_calculate_assigned_partitions_three_nodes() {
        let partitions0 =
            TopologyManager::<ActorId>::calculate_assigned_partitions(0, 3, 100, 10, 3);
        let partitions1 =
            TopologyManager::<ActorId>::calculate_assigned_partitions(1, 3, 100, 10, 3);
        let partitions2 =
            TopologyManager::<ActorId>::calculate_assigned_partitions(2, 3, 100, 10, 3);

        // Each partition should be owned by exactly 3 nodes (replication factor)
        let all_partitions: HashSet<u16> = (0..100).collect();
        for partition_id in &all_partitions {
            let owners = [
                partitions0.contains(partition_id),
                partitions1.contains(partition_id),
                partitions2.contains(partition_id),
            ]
            .iter()
            .filter(|&&x| x)
            .count();

            assert_eq!(
                owners, 3,
                "Partition {partition_id} should have exactly 3 replicas"
            );
        }
    }

    #[test]
    fn test_replication_invariants_with_multiple_nodes() {
        let managers = vec![
            create_test_manager(0, 3),
            create_test_manager(1, 3),
            create_test_manager(2, 3),
        ];

        validate_replication_invariants(&managers, 3);
    }

    #[test]
    fn test_node_connection_updates_state() {
        let (mut manager, _) = create_test_manager(0, 3);
        let (remote_manager, remote_peer_id) = create_test_manager(1, 3);

        let response = manager.on_node_connected(
            remote_manager.local_cluster_ref,
            &remote_manager.assigned_partitions,
            12345,
            1,
            3,
        );

        assert!(response.is_some());
        assert!(manager.active_nodes.contains_key(&remote_peer_id));
        assert!(manager.cluster_nodes.contains_key(&remote_peer_id));
        assert_eq!(manager.active_nodes[&remote_peer_id], (12345, 1));
    }

    #[test]
    fn test_node_disconnection_removes_state() {
        let local_peer_id = create_test_peer_id(0);
        let remote_peer_id = create_test_peer_id(1);
        let local_cluster_ref = ActorId::new_with_peer_id(0, local_peer_id);
        let remote_cluster_ref = ActorId::new_with_peer_id(0, remote_peer_id);

        let mut manager =
            TopologyManager::new(local_cluster_ref, 0, 3, 100, 10, 3, Duration::from_secs(30));

        // First connect the node
        let remote_partitions =
            TopologyManager::<ActorId>::calculate_assigned_partitions(1, 3, 100, 10, 3);
        manager.on_node_connected(remote_cluster_ref, &remote_partitions, 12345, 1, 3);

        assert!(manager.active_nodes.contains_key(&remote_peer_id));
        assert!(manager.cluster_nodes.contains_key(&remote_peer_id));

        // Then disconnect it
        manager.on_node_disconnected(&remote_peer_id);

        assert!(!manager.active_nodes.contains_key(&remote_peer_id));
        assert!(!manager.cluster_nodes.contains_key(&remote_peer_id));
    }

    #[test]
    fn test_heartbeat_activates_inactive_node() {
        let local_peer_id = create_test_peer_id(0);
        let remote_peer_id = create_test_peer_id(1);
        let local_cluster_ref = ActorId::new_with_peer_id(0, local_peer_id);
        let remote_cluster_ref = ActorId::new_with_peer_id(0, remote_peer_id);

        let mut manager =
            TopologyManager::new(local_cluster_ref, 0, 3, 100, 10, 3, Duration::from_secs(30));

        let remote_partitions =
            TopologyManager::<ActorId>::calculate_assigned_partitions(1, 3, 100, 10, 3);

        assert!(!manager.active_nodes.contains_key(&remote_peer_id));

        let status_changed =
            manager.on_heartbeat(remote_cluster_ref, &remote_partitions, 12345, 1, 3);

        assert!(status_changed);
        assert!(manager.active_nodes.contains_key(&remote_peer_id));
        assert_eq!(manager.active_nodes[&remote_peer_id], (12345, 1));
    }

    #[test]
    fn test_heartbeat_detects_index_change() {
        let local_peer_id = create_test_peer_id(0);
        let remote_peer_id = create_test_peer_id(1);
        let local_cluster_ref = ActorId::new_with_peer_id(0, local_peer_id);
        let remote_cluster_ref = ActorId::new_with_peer_id(0, remote_peer_id);

        let mut manager =
            TopologyManager::new(local_cluster_ref, 0, 3, 100, 10, 3, Duration::from_secs(30));

        let remote_partitions =
            TopologyManager::<ActorId>::calculate_assigned_partitions(1, 3, 100, 10, 3);

        // First heartbeat with index 1
        manager.on_heartbeat(remote_cluster_ref, &remote_partitions, 12345, 1, 3);
        assert_eq!(manager.active_nodes[&remote_peer_id].1, 1);

        // Second heartbeat with different index
        let status_changed =
            manager.on_heartbeat(remote_cluster_ref, &remote_partitions, 12345, 2, 3);

        assert!(status_changed);
        assert_eq!(manager.active_nodes[&remote_peer_id].1, 2);
    }

    #[test]
    fn test_heartbeat_timeout_detection() {
        let local_peer_id = create_test_peer_id(0);
        let remote_peer_id = create_test_peer_id(1);
        let local_cluster_ref = ActorId::new_with_peer_id(0, local_peer_id);
        let remote_cluster_ref = ActorId::new_with_peer_id(0, remote_peer_id);

        let mut manager = TopologyManager::new(
            local_cluster_ref,
            0,
            3,
            100,
            10,
            3,
            Duration::from_millis(100), // Short timeout
        );

        let remote_partitions =
            TopologyManager::<ActorId>::calculate_assigned_partitions(1, 3, 100, 10, 3);

        // Connect the node
        manager.on_heartbeat(remote_cluster_ref, &remote_partitions, 12345, 1, 3);
        assert!(manager.active_nodes.contains_key(&remote_peer_id));

        // Manually set an old heartbeat timestamp to simulate timeout
        let old_time = Instant::now() - Duration::from_secs(1);
        manager.node_heartbeats.insert(remote_peer_id, old_time);

        // Check for timeouts
        let status_changed = manager.check_heartbeat_timeouts();

        assert!(status_changed);
        assert!(!manager.active_nodes.contains_key(&remote_peer_id));
    }

    #[test]
    fn test_ownership_response_updates_state() {
        let local_peer_id = create_test_peer_id(0);
        let remote_peer_id = create_test_peer_id(1);
        let local_cluster_ref = ActorId::new_with_peer_id(0, local_peer_id);
        let remote_cluster_ref = ActorId::new_with_peer_id(0, remote_peer_id);

        let mut manager =
            TopologyManager::new(local_cluster_ref, 0, 3, 100, 10, 3, Duration::from_secs(30));

        // Create mock partition replicas
        let mut partition_replicas = HashMap::new();
        let mut replicas = ArrayVec::new();
        replicas.push(remote_cluster_ref);
        partition_replicas.insert(0, replicas);

        let mut active_nodes = HashMap::new();
        active_nodes.insert(remote_peer_id, (12345, 1));

        manager.handle_ownership_response(&partition_replicas, active_nodes);

        assert!(manager.active_nodes.contains_key(&remote_peer_id));
        assert!(manager.cluster_nodes.contains_key(&remote_peer_id));
        assert!(manager.partition_replicas.contains_key(&0));
    }

    #[test]
    fn test_get_available_replicas_with_multiple_nodes() {
        let local_peer_id = create_test_peer_id(0);
        let remote_peer_id1 = create_test_peer_id(1);
        let remote_peer_id2 = create_test_peer_id(2);
        let local_cluster_ref = ActorId::new_with_peer_id(0, local_peer_id);
        let remote_cluster_ref1 = ActorId::new_with_peer_id(0, remote_peer_id1);
        let remote_cluster_ref2 = ActorId::new_with_peer_id(0, remote_peer_id2);

        let mut manager =
            TopologyManager::new(local_cluster_ref, 0, 3, 100, 10, 3, Duration::from_secs(30));

        // Add the other nodes
        let partitions1 =
            TopologyManager::<ActorId>::calculate_assigned_partitions(1, 3, 100, 10, 3);
        let partitions2 =
            TopologyManager::<ActorId>::calculate_assigned_partitions(2, 3, 100, 10, 3);

        manager.on_node_connected(remote_cluster_ref1, &partitions1, 12345, 1, 3);
        manager.on_node_connected(remote_cluster_ref2, &partitions2, 12346, 2, 3);

        // Test that we get the expected number of replicas
        let replicas = manager.get_available_replicas(0);
        assert_eq!(replicas.len(), 3); // Should have 3 replicas (all nodes)
    }

    #[test]
    fn test_partition_availability() {
        let local_peer_id = create_test_peer_id(0);
        let local_cluster_ref = ActorId::new_with_peer_id(0, local_peer_id);

        let manager =
            TopologyManager::new(local_cluster_ref, 0, 3, 100, 10, 3, Duration::from_secs(30));

        // Initially, partitions should not be available (only local node, need 3
        // replicas) Actually, let's test a partition this node owns
        let owned_partition = *manager.assigned_partitions.iter().next().unwrap();

        // Should be available because we have at least one replica
        assert!(manager.is_partition_available(owned_partition));
    }

    #[test]
    fn test_inconsistent_node_count_warning() {
        let local_peer_id = create_test_peer_id(0);
        let remote_peer_id = create_test_peer_id(1);
        let local_cluster_ref = ActorId::new_with_peer_id(0, local_peer_id);
        let remote_cluster_ref = ActorId::new_with_peer_id(0, remote_peer_id);

        let mut manager =
            TopologyManager::new(local_cluster_ref, 0, 3, 100, 10, 3, Duration::from_secs(30));

        let remote_partitions = TopologyManager::<ActorId>::calculate_assigned_partitions(
            1, 5, 100, 10, 3, // Different total_node_count
        );

        // This should work but generate warnings (captured in logs)
        let response = manager.on_node_connected(
            remote_cluster_ref,
            &remote_partitions,
            12345,
            1,
            5, // Different total_node_count
        );

        assert!(response.is_some());
        assert!(manager.active_nodes.contains_key(&remote_peer_id));
    }

    #[test]
    fn test_wrong_partition_assignments_warning() {
        let local_peer_id = create_test_peer_id(0);
        let remote_peer_id = create_test_peer_id(1);
        let local_cluster_ref = ActorId::new_with_peer_id(0, local_peer_id);
        let remote_cluster_ref = ActorId::new_with_peer_id(0, remote_peer_id);

        let mut manager =
            TopologyManager::new(local_cluster_ref, 0, 3, 100, 10, 3, Duration::from_secs(30));

        // Provide wrong partitions for node 1
        let wrong_partitions: HashSet<u16> = vec![99, 98, 97].into_iter().collect();

        // This should work but generate warnings
        let response =
            manager.on_node_connected(remote_cluster_ref, &wrong_partitions, 12345, 1, 3);

        assert!(response.is_some());
        assert!(manager.active_nodes.contains_key(&remote_peer_id));
    }
}
