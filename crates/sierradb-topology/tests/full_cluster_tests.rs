use sierradb_topology::test_helpers::{
    create_connected_cluster, create_test_manager, simulate_heartbeat_exchange,
    verify_cluster_health, verify_cluster_health_with_reduced_replicas,
    verify_reduced_availability,
};

#[tokio::test]
async fn test_complete_cluster_lifecycle() {
    // Test complete cluster formation, operation, and failure scenarios

    // Phase 1: Initial cluster formation
    let mut managers = create_connected_cluster(5);
    verify_cluster_health(&managers, 5, 3);

    // Phase 2: Query operations
    let first_manager = &managers[0].0;

    // Test partition queries work correctly
    for partition_id in 0..10u16 {
        assert!(first_manager.is_partition_available(partition_id));
        let replicas = first_manager.get_available_replicas(partition_id);
        assert_eq!(replicas.len(), 3); // Should have 3 replicas each
    }

    // Test key-based queries
    for key_hash in 0..20u16 {
        let replicas = first_manager.get_available_replicas_for_key(key_hash);
        assert_eq!(replicas.len(), 3);

        // Primary replica should be deterministic
        let primary = first_manager.get_primary_replica(key_hash % first_manager.num_partitions);
        assert!(primary.is_some());
    }

    // Phase 3: Node failure simulation
    let failed_peer_id = managers[2].1;
    for (manager, peer_id) in &mut managers {
        if *peer_id != failed_peer_id {
            manager.on_node_disconnected(&failed_peer_id);
        }
    }
    managers.retain(|(_, peer_id)| *peer_id != failed_peer_id);

    // Verify cluster adapts to failure
    // With 4 nodes remaining and replication factor 3, we expect:
    // - Some partitions will have 3 replicas (if failed node wasn't assigned)
    // - Some partitions will have 2 replicas (if failed node was assigned)
    // - Minimum should be 2 replicas for any partition
    verify_cluster_health_with_reduced_replicas(&managers, 4, 3, 2);

    // Phase 4: New node joins
    managers.push(create_test_manager(5, 5)); // New node with different index
    simulate_heartbeat_exchange(&mut managers);

    // Verify cluster accepts new node
    assert_eq!(managers.len(), 5);
    let first_manager = &managers[0].0;
    assert_eq!(first_manager.active_nodes.len(), 5);

    // Phase 5: Partition availability during churn
    // Even during membership changes, partitions should remain available
    // (though potentially with reduced replication)
    for partition_id in 0..10u16 {
        assert!(first_manager.is_partition_available(partition_id));
        let replicas = first_manager.get_available_replicas(partition_id);
        assert!(
            replicas.len() >= 2,
            "Should have at least 2 replicas for partition {partition_id}"
        );
    }
}

#[tokio::test]
async fn test_consensus_and_consistency() {
    // Verify all nodes converge to same view of cluster state
    let mut managers = create_connected_cluster(7);

    // Multiple rounds of heartbeat exchange to ensure convergence
    for _ in 0..5 {
        simulate_heartbeat_exchange(&mut managers);
    }

    verify_cluster_health(&managers, 7, 3);

    // Verify all nodes have identical views
    let first_manager = &managers[0].0;

    for (manager, _) in &managers[1..] {
        // Same active nodes
        assert_eq!(manager.active_nodes.len(), first_manager.active_nodes.len());

        // Same partition replica counts
        for partition_id in 0..20u16 {
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
                "Partition {partition_id} replica count differs"
            );
        }
    }
}

#[tokio::test]
async fn test_network_partition_recovery() {
    let mut managers = create_connected_cluster(6);
    verify_cluster_health(&managers, 6, 3);

    // Simulate network partition: split into two groups
    let group1_peers: Vec<_> = managers[0..3].iter().map(|(_, peer_id)| *peer_id).collect();
    let group2_peers: Vec<_> = managers[3..6].iter().map(|(_, peer_id)| *peer_id).collect();

    // Group 1 nodes disconnect from Group 2 nodes
    for (manager, _peer_id) in &mut managers[0..3] {
        for &disconnected_peer in &group2_peers {
            manager.on_node_disconnected(&disconnected_peer);
        }
    }

    // Group 2 nodes disconnect from Group 1 nodes
    for (manager, _peer_id) in &mut managers[3..6] {
        for &disconnected_peer in &group1_peers {
            manager.on_node_disconnected(&disconnected_peer);
        }
    }

    // Verify each partition has reduced but potentially available replicas
    for (manager, _) in &managers {
        assert_eq!(manager.active_nodes.len(), 3); // Only sees own partition now
    }

    // Use the helper to verify reduced availability
    // During network partition, we expect at least 1 replica available for most
    // partitions
    verify_reduced_availability(&managers[0..3], 1);
    verify_reduced_availability(&managers[3..6], 1);

    // Simulate network healing - reconnect all nodes
    simulate_heartbeat_exchange(&mut managers);

    // Verify full cluster recovery
    verify_cluster_health(&managers, 6, 3);

    // All nodes should see full cluster again
    for (manager, _) in &managers {
        assert_eq!(manager.active_nodes.len(), 6);
    }
}

#[tokio::test]
async fn test_rolling_restarts() {
    let mut managers = create_connected_cluster(5);
    verify_cluster_health(&managers, 5, 3);

    // Simulate rolling restart: restart nodes one by one
    for restart_index in 0..5 {
        let restarting_peer = managers[restart_index].1;

        // Remove restarting node from other nodes' view
        for (manager, peer_id) in &mut managers {
            if *peer_id != restarting_peer {
                manager.on_node_disconnected(&restarting_peer);
            }
        }

        // Verify cluster still functions with 4 nodes
        let active_managers: Vec<_> = managers
            .iter()
            .filter(|(_, peer_id)| *peer_id != restarting_peer)
            .collect();

        for (manager, _) in &active_managers {
            assert_eq!(manager.active_nodes.len(), 4);
        }

        // Verify partitions are still available (though potentially with reduced
        // replication)
        for partition_id in 0..10u16 {
            let available_count = active_managers
                .iter()
                .filter(|(manager, _)| manager.is_partition_available(partition_id))
                .count();

            // During rolling restart, at least some nodes should still have the partition
            // available
            assert!(
                available_count > 0,
                "Partition {partition_id} should be available on at least some nodes during rolling restart"
            );
        }

        // "Restart" the node (create new instance with same index)
        managers[restart_index] = create_test_manager(restart_index, 5);

        // Rejoin cluster
        simulate_heartbeat_exchange(&mut managers);

        // Verify full cluster recovery
        verify_cluster_health(&managers, 5, 3);
    }
}
