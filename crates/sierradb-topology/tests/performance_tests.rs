use std::time::Duration;

use kameo::actor::ActorId;
use sierradb_topology::{
    TopologyManager,
    test_helpers::{
        create_connected_cluster, create_test_manager, create_test_manager_with_params,
        create_test_peer_id, simulate_heartbeat_exchange, verify_cluster_health,
    },
};
use tokio::time::Instant;

#[tokio::test]
async fn test_large_cluster_formation() {
    let node_count = 50;
    let mut managers = Vec::new();

    // Measure cluster formation time
    let start = Instant::now();

    // Create large cluster
    for i in 0..node_count {
        managers.push(create_test_manager(i, node_count));
    }

    // Simulate network convergence
    simulate_heartbeat_exchange(&mut managers);

    let formation_time = start.elapsed();
    println!("50-node cluster formation took: {formation_time:?}");

    // Verify large cluster health
    verify_cluster_health(&managers, node_count, 3);

    // Performance assertions
    assert!(
        formation_time < Duration::from_secs(5),
        "Cluster formation should be fast"
    );

    // Memory usage should be reasonable
    let first_manager = &managers[0].0;
    assert!(first_manager.partition_replicas.len() <= first_manager.num_partitions as usize);
    assert_eq!(first_manager.active_nodes.len(), node_count);
}

#[tokio::test]
async fn test_high_partition_count_performance() {
    let peer_id = create_test_peer_id(0);
    let cluster_ref = ActorId::new_with_peer_id(0, peer_id);

    let start = Instant::now();

    // Create manager with high partition count
    let manager = TopologyManager::new(
        cluster_ref,
        0,
        10,    // 10 nodes
        50000, // 50k partitions
        5000,  // 5k buckets
        3,     // RF=3
        Duration::from_secs(30),
    );

    let creation_time = start.elapsed();
    println!("50k partition manager creation took: {creation_time:?}");

    // Test query performance
    let start = Instant::now();
    for i in 0..1000u16 {
        let _ = manager.has_partition(i);
        let _ = manager.is_key_available_locally(i);
    }
    let query_time = start.elapsed();
    println!("1000 partition queries took: {query_time:?}");

    // Performance assertions
    assert!(
        creation_time < Duration::from_secs(1),
        "Manager creation should be fast"
    );
    assert!(
        query_time < Duration::from_millis(10),
        "Queries should be very fast"
    );
    assert!(!manager.assigned_partitions.is_empty());
}

#[tokio::test]
async fn test_rapid_heartbeat_processing() {
    let (mut manager, _) = create_test_manager(0, 10);
    let heartbeat_nodes: Vec<_> = (1..10).map(|i| create_test_manager(i, 10)).collect();

    let start = Instant::now();

    // Process 1000 heartbeats rapidly
    for _ in 0..1000 {
        for (heartbeat_manager, _) in &heartbeat_nodes {
            manager.on_heartbeat(
                heartbeat_manager.local_cluster_ref,
                &heartbeat_manager.assigned_partitions,
                heartbeat_manager.alive_since,
                heartbeat_manager.local_node_index,
                heartbeat_manager.total_node_count,
            );
        }
    }

    let processing_time = start.elapsed();
    println!("9000 heartbeat processing took: {processing_time:?}");

    // Verify correctness after rapid processing
    assert_eq!(manager.active_nodes.len(), 10); // Self + 9 others
    assert!(
        processing_time < Duration::from_secs(1),
        "Heartbeat processing should be fast"
    );
}

#[tokio::test]
async fn test_memory_usage_stability() {
    let mut managers = create_connected_cluster(10);

    // Simulate long-running operation with many state changes
    for iteration in 0..100 {
        // Add some churn
        if iteration % 10 == 0 {
            // Disconnect random node
            let disconnect_idx = iteration % 10;
            let disconnect_peer = managers[disconnect_idx].1;

            for (manager, peer_id) in &mut managers {
                if *peer_id != disconnect_peer {
                    manager.on_node_disconnected(&disconnect_peer);
                }
            }
        }

        // Process heartbeats
        simulate_heartbeat_exchange(&mut managers);

        // Reconnect if disconnected
        if iteration % 10 == 5 {
            simulate_heartbeat_exchange(&mut managers);
        }
    }

    // Verify memory usage didn't grow excessively
    let first_manager = &managers[0].0;
    assert!(first_manager.active_nodes.len() <= 10);
    assert!(first_manager.node_heartbeats.len() <= 10);
    assert!(first_manager.cluster_nodes.len() <= 10);

    // Partition replica count should be bounded
    assert!(first_manager.partition_replicas.len() <= first_manager.num_partitions as usize);
}

#[tokio::test]
async fn test_partition_distribution_balance() {
    // Create managers with parameters that ensure all nodes get partitions
    let node_count = 10; // Use fewer nodes to ensure good distribution
    let mut managers = Vec::new();

    // Create cluster with parameters that give good distribution
    for i in 0..node_count {
        managers.push(create_test_manager_with_params(
            i, node_count, 1000, // More partitions
            100,  // More buckets (ensures all nodes get some)
            3,    // RF=3
        ));
    }

    // Simulate network convergence
    simulate_heartbeat_exchange(&mut managers);

    let mut partition_counts = std::collections::HashMap::new();

    // Count partitions per node
    for (manager, peer_id) in &managers {
        partition_counts.insert(*peer_id, manager.assigned_partitions.len());
    }

    let min_partitions = *partition_counts.values().min().unwrap();
    let max_partitions = *partition_counts.values().max().unwrap();

    // Ensure no node has zero partitions
    assert!(
        min_partitions > 0,
        "All nodes should have at least some partitions, but found node with 0"
    );

    let imbalance_ratio = max_partitions as f64 / min_partitions as f64;

    // With replication factor 3 and good parameters, expect reasonable balance
    assert!(
        imbalance_ratio < 2.0,
        "Partition distribution should be reasonably balanced, got ratio {imbalance_ratio:.2}"
    );

    println!(
        "Partition distribution: min={min_partitions}, max={max_partitions}, ratio={imbalance_ratio:.2}"
    );

    // Also verify total partition coverage
    let total_unique_partitions: std::collections::HashSet<_> = managers
        .iter()
        .flat_map(|(manager, _)| manager.assigned_partitions.iter())
        .collect();

    assert_eq!(
        total_unique_partitions.len(),
        1000,
        "All partitions should be assigned to at least one node"
    );
}

#[tokio::test]
async fn test_query_performance_scaling() {
    // Test that query performance doesn't degrade with cluster size
    let test_cases = vec![(5, "small"), (20, "medium"), (50, "large")];

    for (node_count, label) in test_cases {
        let managers = create_connected_cluster(node_count);
        let manager = &managers[0].0;

        let start = Instant::now();
        for i in 0..1000u16 {
            let _ = manager.get_available_replicas(i % manager.num_partitions);
            let _ = manager.is_partition_available(i % manager.num_partitions);
        }
        let query_time = start.elapsed();

        println!("{label} cluster ({node_count} nodes): 1000 queries took {query_time:?}");

        // Query time should remain roughly constant regardless of cluster size
        assert!(
            query_time < Duration::from_millis(50),
            "Query performance should not degrade with cluster size"
        );
    }
}

#[tokio::test]
async fn test_concurrent_cluster_operations() {
    // Test performance under concurrent operations
    let managers = create_connected_cluster(10);

    let start = Instant::now();

    // Simulate concurrent operations
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let manager = managers[i].0.clone(); // Assuming Clone is implemented
            tokio::spawn(async move {
                // Simulate concurrent queries
                for j in 0..100 {
                    let partition_id = (i * 100 + j) as u16 % 100;
                    let _ = manager.has_partition(partition_id);
                    let _ = manager.get_available_replicas(partition_id);

                    // Add small delay to simulate realistic load
                    tokio::time::sleep(Duration::from_micros(10)).await;
                }
            })
        })
        .collect();

    // Wait for all concurrent operations
    for handle in handles {
        handle.await.unwrap();
    }

    let concurrent_time = start.elapsed();
    println!("Concurrent operations took: {concurrent_time:?}");

    // Should complete reasonably fast even under concurrency
    assert!(
        concurrent_time < Duration::from_secs(2),
        "Concurrent operations should be efficient"
    );
}

#[tokio::test]
async fn benchmark_different_replication_factors() {
    // Compare performance with different replication factors
    let replication_factors = vec![1, 3, 5];

    for rf in replication_factors {
        let peer_id = create_test_peer_id(0);
        let cluster_ref = ActorId::new_with_peer_id(0, peer_id);

        let start = Instant::now();
        let manager = TopologyManager::new(
            cluster_ref,
            0,
            10,    // 10 nodes
            10000, // 10k partitions
            1000,  // 1k buckets
            rf,    // Variable replication factor
            Duration::from_secs(30),
        );
        let creation_time = start.elapsed();

        let start = Instant::now();
        for i in 0..1000u16 {
            let _ = manager.has_partition(i);
        }
        let query_time = start.elapsed();

        println!("RF={rf}: creation={creation_time:?}, queries={query_time:?}");

        // Performance should not degrade significantly with higher RF
        assert!(creation_time < Duration::from_millis(500));
        assert!(query_time < Duration::from_millis(10));
    }
}
