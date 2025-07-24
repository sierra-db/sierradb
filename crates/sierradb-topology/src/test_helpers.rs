use std::time::Duration;

use kameo::actor::ActorId;
use libp2p::PeerId;

use crate::{TopologyManager, messages::Heartbeat};

/// Create a test PeerId deterministically
pub fn create_test_peer_id(index: usize) -> PeerId {
    // Create a simple deterministic PeerId for testing
    // This creates a valid PeerId by using a deterministic key
    use libp2p::identity::{Keypair, ed25519};

    // Create a deterministic 32-byte seed
    let mut seed = [0u8; 32];
    seed[0] = index as u8;
    seed[1] = (index >> 8) as u8;

    // Generate keypair from seed
    let secret_key = ed25519::SecretKey::try_from_bytes(seed).expect("32 bytes should be valid");
    let keypair = ed25519::Keypair::from(secret_key);
    let keypair = Keypair::from(keypair);

    keypair.public().to_peer_id()
}

/// Helper to create a test manager
pub fn create_test_manager(
    node_index: usize,
    total_node_count: usize,
) -> (TopologyManager<ActorId>, PeerId) {
    let peer_id = create_test_peer_id(node_index);
    let cluster_ref = ActorId::new_with_peer_id(0, peer_id);

    let manager = TopologyManager::new(
        cluster_ref,
        node_index,
        total_node_count,
        100, // num_partitions
        10,  // bucket_count
        3,   // replication_factor
        Duration::from_secs(30),
    );

    (manager, peer_id)
}

pub fn create_test_manager_with_params(
    node_index: usize,
    total_node_count: usize,
    num_partitions: u16,
    bucket_count: u16,
    replication_factor: u8,
) -> (TopologyManager<ActorId>, PeerId) {
    let peer_id = create_test_peer_id(node_index);
    let cluster_ref = ActorId::new_with_peer_id(0, peer_id);

    let manager = TopologyManager::new(
        cluster_ref,
        node_index,
        total_node_count,
        num_partitions,
        bucket_count,
        replication_factor,
        Duration::from_secs(30),
    );

    (manager, peer_id)
}

/// Validate that partition assignments follow replication rules
pub fn validate_replication_invariants(
    managers: &[(TopologyManager<ActorId>, PeerId)],
    expected_replication_factor: usize,
) {
    if managers.is_empty() {
        return;
    }

    let num_partitions = managers[0].0.num_partitions;
    let total_nodes = managers.len();
    let effective_rf = expected_replication_factor.min(total_nodes);

    // Check that each partition is owned by exactly RF nodes
    for partition_id in 0..num_partitions {
        let owners: Vec<_> = managers
            .iter()
            .filter(|(manager, _)| manager.assigned_partitions.contains(&partition_id))
            .collect();

        assert_eq!(
            owners.len(),
            effective_rf,
            "Partition {} should be owned by exactly {} nodes, but owned by {}",
            partition_id,
            effective_rf,
            owners.len()
        );
    }
}

/// Helper to simulate message passing between managers
pub fn simulate_heartbeat_exchange(managers: &mut [(TopologyManager<ActorId>, PeerId)]) {
    // Collect all heartbeats
    let heartbeats: Vec<_> = managers
        .iter()
        .map(|(manager, peer_id)| {
            (
                *peer_id,
                Heartbeat {
                    cluster_ref: manager.local_cluster_ref,
                    owned_partitions: manager.assigned_partitions.clone(),
                    alive_since: manager.alive_since,
                    node_index: manager.local_node_index,
                    total_node_count: manager.total_node_count,
                },
            )
        })
        .collect();

    // Deliver each heartbeat to all other managers
    for (sender_peer_id, heartbeat) in heartbeats {
        for (manager, peer_id) in managers.iter_mut() {
            if *peer_id != sender_peer_id {
                manager.on_heartbeat(
                    heartbeat.cluster_ref,
                    &heartbeat.owned_partitions,
                    heartbeat.alive_since,
                    heartbeat.node_index,
                    heartbeat.total_node_count,
                );
            }
        }
    }
}

/// Helper to verify all managers have consistent partition replica assignments
pub fn verify_consistent_state(managers: &[(TopologyManager<ActorId>, PeerId)]) {
    if managers.is_empty() {
        return;
    }

    let first_manager = &managers[0].0;
    let num_partitions = first_manager.num_partitions;

    // Check that all managers have the same view of active nodes
    for (manager, _) in &managers[1..] {
        assert_eq!(
            manager.active_nodes.len(),
            first_manager.active_nodes.len(),
            "All managers should have the same number of active nodes"
        );
    }

    // Check that all managers have consistent partition replica counts
    for partition_id in 0..num_partitions.min(10) {
        // Test first 10 partitions for performance
        let first_replica_count = first_manager
            .partition_replicas
            .get(&partition_id)
            .map(|r| r.len())
            .unwrap_or(0);

        for (i, (manager, _)) in managers[1..].iter().enumerate() {
            let replica_count = manager
                .partition_replicas
                .get(&partition_id)
                .map(|r| r.len())
                .unwrap_or(0);

            assert_eq!(
                replica_count,
                first_replica_count,
                "Partition {} should have consistent replica count across all managers (manager {} differs)",
                partition_id,
                i + 1
            );
        }
    }
}

/// Helper to create managers for a full cluster and establish connections
pub fn create_connected_cluster(node_count: usize) -> Vec<(TopologyManager<ActorId>, PeerId)> {
    let mut managers = Vec::new();

    // Create all managers
    for i in 0..node_count {
        managers.push(create_test_manager(i, node_count));
    }

    // Simulate connection establishment
    simulate_heartbeat_exchange(&mut managers);

    managers
}

/// Verify that the cluster is properly formed with expected replica counts
pub fn verify_cluster_health(
    managers: &[(TopologyManager<ActorId>, PeerId)],
    expected_node_count: usize,
    expected_replication_factor: usize,
) {
    assert_eq!(managers.len(), expected_node_count);

    verify_consistent_state(managers);
    validate_replication_invariants(managers, expected_replication_factor);

    // Check that all partitions have the expected number of replicas in the global view
    if !managers.is_empty() {
        let first_manager = &managers[0].0;
        let effective_rf = expected_replication_factor.min(expected_node_count);

        for partition_id in 0..first_manager.num_partitions.min(10) {
            let replica_count = first_manager
                .partition_replicas
                .get(&partition_id)
                .map(|replicas| replicas.len())
                .unwrap_or(0);

            assert_eq!(
                replica_count, effective_rf,
                "Partition {partition_id} should have {effective_rf} replicas in global view"
            );
        }
    }
}

/// Helper function for testing cluster health with potentially reduced replicas
/// This is more realistic for failure scenarios where some nodes are unavailable
pub fn verify_cluster_health_with_reduced_replicas(
    managers: &[(TopologyManager<ActorId>, PeerId)],
    expected_node_count: usize,
    original_replication_factor: usize,
    min_replica_count: usize,
) {
    assert_eq!(managers.len(), expected_node_count);

    if managers.is_empty() {
        return;
    }

    let first_manager = &managers[0].0;

    // Check that all managers have consistent active node counts
    for (manager, _) in managers {
        assert_eq!(
            manager.active_nodes.len(),
            expected_node_count,
            "All managers should see same number of active nodes"
        );
    }

    // Check that partitions have at least minimum replica count
    for partition_id in 0..first_manager.num_partitions.min(20) {
        let replica_count = first_manager
            .partition_replicas
            .get(&partition_id)
            .map(|replicas| replicas.len())
            .unwrap_or(0);

        assert!(
            replica_count >= min_replica_count,
            "Partition {partition_id} should have at least {min_replica_count} replicas, got {replica_count}"
        );

        // Should not exceed original replication factor
        assert!(
            replica_count <= original_replication_factor,
            "Partition {partition_id} should not exceed original replication factor {original_replication_factor}, got {replica_count}"
        );
    }

    // Verify consistency across managers
    for (manager, _) in &managers[1..] {
        for partition_id in 0..first_manager.num_partitions.min(10) {
            let first_count = first_manager
                .partition_replicas
                .get(&partition_id)
                .map(|r| r.len())
                .unwrap_or(0);
            let manager_count = manager
                .partition_replicas
                .get(&partition_id)
                .map(|r| r.len())
                .unwrap_or(0);

            assert_eq!(
                first_count, manager_count,
                "Partition {partition_id} replica count should be consistent across managers"
            );
        }
    }
}

/// Verify that partitions are available with reduced but sufficient replicas
/// Used for testing failure scenarios where some replicas are unavailable
pub fn verify_reduced_availability(
    managers: &[(TopologyManager<ActorId>, PeerId)],
    min_available_replicas: usize,
) {
    if managers.is_empty() {
        return;
    }

    let first_manager = &managers[0].0;

    // Check that most partitions remain available
    let mut available_count = 0;
    let mut total_checked = 0;

    for partition_id in 0..first_manager.num_partitions.min(20) {
        total_checked += 1;

        if first_manager.is_partition_available(partition_id) {
            available_count += 1;

            let replica_count = first_manager.get_available_replicas(partition_id).len();

            assert!(
                replica_count >= min_available_replicas,
                "Available partition {partition_id} should have at least {min_available_replicas} replicas, got {replica_count}"
            );
        }
    }

    // At least 50% of partitions should remain available during failures
    let availability_ratio = available_count as f64 / total_checked as f64;
    assert!(
        availability_ratio >= 0.5,
        "At least 50% of partitions should remain available, got {:.1}%",
        availability_ratio * 100.0
    );
}
