use std::{collections::HashSet, time::Duration};

use kameo::actor::ActorId;
use sierradb_topology::{
    TopologyManager,
    test_helpers::{
        create_connected_cluster, create_test_manager, create_test_peer_id,
        simulate_heartbeat_exchange, validate_replication_invariants, verify_cluster_health,
    },
};

#[tokio::test]
async fn test_empty_partition_assignment_bug() {
    // This test verifies the fix for the original bug where
    // expected partitions were empty

    let managers = create_connected_cluster(3);
    verify_cluster_health(&managers, 3, 3);

    // Each node should own some partitions
    for (manager, _) in &managers {
        assert!(
            !manager.assigned_partitions.is_empty(),
            "Node should own some partitions"
        );
    }

    // Global view should show all partitions have replicas
    let first_manager = &managers[0].0;
    for partition_id in 0..first_manager.num_partitions {
        let replica_count = first_manager
            .partition_replicas
            .get(&partition_id)
            .map(|r| r.len())
            .unwrap_or(0);

        assert_eq!(
            replica_count, 3,
            "Partition {partition_id} should have exactly 3 replicas, got {replica_count}"
        );
    }
}

#[tokio::test]
async fn test_node_index_consistency() {
    // Verify that node indices are used consistently
    // throughout the system

    let mut managers = Vec::new();
    let node_indices = [2, 0, 1]; // Non-sequential order

    for &index in &node_indices {
        managers.push(create_test_manager(index, 3));
    }

    simulate_heartbeat_exchange(&mut managers);

    // Verify each manager reports correct index
    for (manager, _) in &managers {
        assert!(node_indices.contains(&manager.local_node_index));
        assert_eq!(manager.total_node_count, 3);
    }

    // Verify partition assignments are based on indices, not connection order
    // With 3 nodes and RF=3, all nodes get all partitions
    // Let's test with RF=1 to ensure different assignments
    let mut rf1_managers = Vec::new();
    for &index in &node_indices {
        let peer_id = create_test_peer_id(index);
        let cluster_ref = ActorId::new_with_peer_id(0, peer_id);
        let manager =
            TopologyManager::new(cluster_ref, index, 3, 100, 10, 1, Duration::from_secs(30));
        rf1_managers.push((manager, peer_id));
    }

    simulate_heartbeat_exchange(&mut rf1_managers);

    let manager_0 = rf1_managers
        .iter()
        .find(|(m, _)| m.local_node_index == 0)
        .unwrap();
    let manager_2 = rf1_managers
        .iter()
        .find(|(m, _)| m.local_node_index == 2)
        .unwrap();

    assert_ne!(
        manager_0.0.assigned_partitions, manager_2.0.assigned_partitions,
        "With RF=1, nodes with different indices should have different partition sets"
    );
}

#[tokio::test]
async fn test_replication_factor_boundary_conditions() {
    // Test edge cases around replication factor

    // RF = 1 (minimal replication)
    let mut rf1_managers = Vec::new();
    for i in 0..3 {
        let peer_id = create_test_peer_id(i);
        let cluster_ref = ActorId::new_with_peer_id(0, peer_id);
        let manager = TopologyManager::new(cluster_ref, i, 3, 100, 10, 1, Duration::from_secs(30));
        rf1_managers.push((manager, peer_id));
    }

    simulate_heartbeat_exchange(&mut rf1_managers);
    validate_replication_invariants(&rf1_managers, 1);

    // Verify that with RF=1, each partition has exactly 1 replica
    let first_manager = &rf1_managers[0].0;
    for partition_id in 0..10u16 {
        let replica_count = first_manager
            .partition_replicas
            .get(&partition_id)
            .map(|r| r.len())
            .unwrap_or(0);
        assert_eq!(
            replica_count, 1,
            "With RF=1, partition {partition_id} should have exactly 1 replica"
        );
    }

    // RF > node count (should cap at node count)
    let mut rf_high_managers = Vec::new();
    for i in 0..2 {
        let peer_id = create_test_peer_id(i);
        let cluster_ref = ActorId::new_with_peer_id(0, peer_id);
        let manager = TopologyManager::new(cluster_ref, i, 2, 100, 10, 5, Duration::from_secs(30));
        rf_high_managers.push((manager, peer_id));
    }

    simulate_heartbeat_exchange(&mut rf_high_managers);
    validate_replication_invariants(&rf_high_managers, 2); // Should cap at node count

    // Verify that with RF=5 but only 2 nodes, each partition has exactly 2 replicas
    let first_manager = &rf_high_managers[0].0;
    for partition_id in 0..10u16 {
        let replica_count = first_manager
            .partition_replicas
            .get(&partition_id)
            .map(|r| r.len())
            .unwrap_or(0);
        assert_eq!(
            replica_count, 2,
            "With RF=5 but 2 nodes, partition {partition_id} should have exactly 2 replicas"
        );
    }
}

#[tokio::test]
async fn test_partition_availability_during_churn() {
    // Verify partitions remain available during node churn
    let mut managers = create_connected_cluster(5);
    verify_cluster_health(&managers, 5, 3);

    // Track which partitions are available throughout churn
    let mut availability_history = Vec::new();

    for round in 0..10 {
        // Record current availability
        let first_manager = &managers[0].0;
        let available_partitions: HashSet<u16> = (0..first_manager.num_partitions)
            .filter(|&p| first_manager.is_partition_available(p))
            .collect();
        availability_history.push(available_partitions);

        // Introduce churn
        if round % 2 == 0 {
            // Remove a node
            if managers.len() > 3 {
                let removed_peer = managers.pop().unwrap().1;
                for (manager, _) in &mut managers {
                    manager.on_node_disconnected(&removed_peer);
                }
            }
        } else {
            // Add a node back
            let new_index = managers.len();
            managers.push(create_test_manager(new_index, 5));
            simulate_heartbeat_exchange(&mut managers);
        }
    }

    // Verify that most partitions remained available throughout
    let all_partitions: HashSet<u16> = (0..100).collect();
    for (round, available) in availability_history.iter().enumerate() {
        let availability_ratio = available.len() as f64 / all_partitions.len() as f64;
        assert!(
            availability_ratio > 0.8,
            "Round {}: Only {:.1}% partitions available",
            round,
            availability_ratio * 100.0
        );
    }
}
