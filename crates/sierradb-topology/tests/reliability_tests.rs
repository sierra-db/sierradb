use std::time::Duration;

use sierradb_topology::test_helpers::{
    create_connected_cluster, create_test_manager, simulate_heartbeat_exchange,
    verify_cluster_health,
};
use tokio::sync::Mutex;

#[tokio::test]
async fn test_byzantine_failure_tolerance() {
    let mut managers = create_connected_cluster(7);
    verify_cluster_health(&managers, 7, 3);

    // Simulate a node reporting wrong partition ownership
    let (byzantine_manager, byzantine_peer) = &mut managers[3];
    let byzantine_peer = *byzantine_peer;

    // Corrupt the byzantine node's partition assignments
    byzantine_manager.assigned_partitions = (90..100).collect(); // Wrong partitions

    // Other nodes should detect and handle inconsistency
    simulate_heartbeat_exchange(&mut managers);

    // System should remain stable despite byzantine node
    let non_byzantine_managers: Vec<_> = managers
        .iter()
        .filter(|(_, peer_id)| *peer_id != byzantine_peer)
        .collect();

    for (manager, _) in &non_byzantine_managers {
        assert_eq!(manager.active_nodes.len(), 7); // All nodes still tracked

        // Partitions should still be available
        for partition_id in 0..10u16 {
            assert!(manager.is_partition_available(partition_id));
        }
    }
}

#[tokio::test]
async fn test_timeout_edge_cases() {
    let (mut manager, _) = create_test_manager(0, 3);
    let (remote_manager, remote_peer) = create_test_manager(1, 3);

    // Set very short timeout
    manager.heartbeat_timeout = Duration::from_millis(1);

    // Establish connection
    manager.on_heartbeat(
        remote_manager.local_cluster_ref,
        &remote_manager.assigned_partitions,
        remote_manager.alive_since,
        1,
        3,
    );

    assert!(manager.active_nodes.contains_key(&remote_peer));

    // Wait longer than timeout
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Manually check timeouts (normally done by polling)
    let status_changed = manager.check_heartbeat_timeouts();

    assert!(status_changed);
    assert!(!manager.active_nodes.contains_key(&remote_peer));
}

#[tokio::test]
async fn test_concurrent_modifications() {
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let manager = Arc::new(Mutex::new({
        let (manager, _) = create_test_manager(0, 10);
        manager
    }));

    let mut join_set = JoinSet::new();

    // Spawn multiple tasks doing concurrent operations
    for i in 1..10 {
        let manager_clone = manager.clone();
        let (test_manager, _test_peer) = create_test_manager(i, 10);

        join_set.spawn(async move {
            for _ in 0..100 {
                let mut mg = manager_clone.lock().await;
                mg.on_heartbeat(
                    test_manager.local_cluster_ref,
                    &test_manager.assigned_partitions,
                    test_manager.alive_since,
                    test_manager.local_node_index,
                    test_manager.total_node_count,
                );
                drop(mg);

                // Small delay to allow interleaving
                tokio::time::sleep(Duration::from_micros(1)).await;
            }
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = join_set.join_next().await {
        result.unwrap();
    }

    // Verify final state is consistent
    let final_manager = manager.lock().await;
    assert_eq!(final_manager.active_nodes.len(), 10);
}

#[tokio::test]
async fn test_message_ordering_resilience() {
    let (mut manager, _) = create_test_manager(0, 3);
    let (remote_manager, remote_peer) = create_test_manager(1, 3);

    // Process messages in different orders

    // Scenario 1: Heartbeat before connection
    manager.on_heartbeat(
        remote_manager.local_cluster_ref,
        &remote_manager.assigned_partitions,
        remote_manager.alive_since,
        1,
        3,
    );

    assert!(manager.active_nodes.contains_key(&remote_peer));

    // Scenario 2: Connection after heartbeat
    manager.on_node_connected(
        remote_manager.local_cluster_ref,
        &remote_manager.assigned_partitions,
        remote_manager.alive_since,
        1,
        3,
    );

    // Should remain stable
    assert!(manager.active_nodes.contains_key(&remote_peer));

    // Scenario 3: Multiple rapid disconnects/reconnects
    for _ in 0..10 {
        manager.on_node_disconnected(&remote_peer);
        assert!(!manager.active_nodes.contains_key(&remote_peer));

        manager.on_heartbeat(
            remote_manager.local_cluster_ref,
            &remote_manager.assigned_partitions,
            remote_manager.alive_since,
            1,
            3,
        );
        assert!(manager.active_nodes.contains_key(&remote_peer));
    }
}
