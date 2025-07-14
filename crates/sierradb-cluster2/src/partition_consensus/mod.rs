use std::cmp;

use arrayvec::ArrayVec;
use sierradb::MAX_REPLICATION_FACTOR;
use sierradb::bucket::{PartitionHash, PartitionId};

mod behaviour;
mod manager;
mod messages;
mod store;

pub use behaviour::{Behaviour, PartitionBehaviourEvent};
pub use manager::PartitionManager;

/// Distributes a partition across the cluster for replication
///
/// This function takes a partition key and determines which partitions should
/// store replicas of that data according to the configured replication factor.
///
/// # Arguments
/// * `partition_key` - The primary partition identifier (without moduloing yet,
///   just the raw value from uuid_to_partition_id)
/// * `num_partitions` - The total number of partitions in the system
/// * `replication_factor` - How many copies of the data should be stored
///
/// # Returns
/// An array of partition IDs where the data should be stored
pub fn distribute_partition(
    partition_hash: PartitionHash,
    num_partitions: u16,
    replication_factor: u8,
) -> ArrayVec<PartitionId, MAX_REPLICATION_FACTOR> {
    let mut result = ArrayVec::new();

    // Validate inputs
    if num_partitions == 0 {
        return result;
    }

    // Ensure replication_factor is within bounds
    let actual_replication = cmp::min(
        replication_factor as usize,
        cmp::min(num_partitions as usize, MAX_REPLICATION_FACTOR),
    );

    if actual_replication == 0 {
        return result;
    }

    // Start with the primary partition (simple modulo distribution)
    let primary_partition = partition_hash % num_partitions;
    result.push(primary_partition);

    // For additional replicas, use a deterministic distribution method
    if actual_replication > 1 {
        // Compute a jump value that's relatively prime to num_partitions
        let jump = if num_partitions <= 2 {
            1
        } else {
            // For prime numbers, using num_partitions/2 gives good distribution
            let candidate = num_partitions / 2 + 1;

            // If num_partitions is even and candidate is even, add 1 to make it odd
            if num_partitions % 2 == 0 && candidate % 2 == 0 {
                candidate + 1
            } else {
                candidate
            }
        };

        let mut current = primary_partition;
        for _ in 1..actual_replication {
            // Jump to the next partition, wrapping around if necessary
            current = (current + jump) % num_partitions;

            // Safety check to avoid potential infinite loop
            if result.contains(&current) || result.is_full() {
                break;
            }

            result.push(current);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_distribute_partition_empty_when_zero_partitions() {
        let result = distribute_partition(123, 0, 3);
        assert!(result.is_empty());
    }

    #[test]
    fn test_distribute_partition_empty_when_zero_replication() {
        let result = distribute_partition(123, 16, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_distribute_partition_single_replica() {
        let result = distribute_partition(5, 10, 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 5); // 5 % 10 = 5
    }

    #[test]
    fn test_distribute_partition_wraps_partition_key() {
        let result = distribute_partition(15, 10, 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 5); // 15 % 10 = 5
    }

    #[test]
    fn test_distribute_partition_multiple_replicas() {
        let result = distribute_partition(7, 10, 3);
        assert_eq!(result.len(), 3);
        // First partition is the primary
        assert_eq!(result[0], 7);
        // Check that all values are distinct
        assert!(result.iter().collect::<HashSet<_>>().len() == 3);
    }

    #[test]
    fn test_distribute_partition_caps_at_max_replication_factor() {
        // Try to get more replicas than MAX_REPLICATION_FACTOR
        let result = distribute_partition(3, 100, 20);
        assert_eq!(result.len(), MAX_REPLICATION_FACTOR);
        // Check all are distinct
        assert_eq!(
            result.iter().collect::<HashSet<_>>().len(),
            MAX_REPLICATION_FACTOR
        );
    }

    #[test]
    fn test_distribute_partition_caps_at_num_partitions() {
        // Try to get more replicas than available partitions
        let result = distribute_partition(2, 5, 10);
        assert_eq!(result.len(), 5);
        // Check all partitions are used
        assert_eq!(result.iter().collect::<HashSet<_>>().len(), 5);
    }

    #[test]
    fn test_distribute_partition_all_partitions_distinct() {
        for num_partitions in 2..=20 {
            for replication in 2..=cmp::min(num_partitions, 8) {
                for partition_key in 0..20 {
                    let result =
                        distribute_partition(partition_key, num_partitions, replication as u8);
                    let unique_count = result.iter().collect::<HashSet<_>>().len();
                    assert_eq!(
                        unique_count,
                        result.len(),
                        "Duplicate partitions found with partition_key={partition_key}, num_partitions={num_partitions}, replication={replication}",
                    );
                }
            }
        }
    }

    #[test]
    fn test_distribute_partition_first_partition_is_modulo() {
        for num_partitions in 1..=20 {
            for partition_key in 0..30 {
                let result = distribute_partition(partition_key, num_partitions, 1);
                assert_eq!(result[0], partition_key % num_partitions);
            }
        }
    }

    #[test]
    fn test_distribute_partition_jump_prime_to_even_partitions() {
        // Test for even number of partitions
        let num_partitions = 10;

        let result = distribute_partition(0, num_partitions, 4);

        // Check distribution pattern follows our expected jump
        assert_eq!(result[0], 0); // Primary
        assert_eq!(result[1], 7); // 0 + 7 = 7
        assert_eq!(result[2], 4); // 7 + 7 = 14 % 10 = 4
        assert_eq!(result[3], 1); // 4 + 7 = 11 % 10 = 1
    }

    #[test]
    fn test_distribute_partition_jump_prime_to_odd_partitions() {
        // Test for odd number of partitions
        let num_partitions = 11;
        // Expected jump should be 6 (num_partitions/2 + 1 = 6)
        let result = distribute_partition(0, num_partitions, 4);

        // Check distribution pattern follows our expected jump
        assert_eq!(result[0], 0); // Primary
        assert_eq!(result[1], 6); // 0 + 6 = 6
        assert_eq!(result[2], 1); // 6 + 6 = 12 % 11 = 1
        assert_eq!(result[3], 7); // 1 + 6 = 7
    }

    #[test]
    fn test_distribute_partition_small_partitions() {
        // Test with 1 partition
        let result = distribute_partition(42, 1, 3);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], 0);

        // Test with 2 partitions
        let result = distribute_partition(5, 2, 2);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], 1); // 5 % 2 = 1
        assert_eq!(result[1], 0); // 1 + 1 = 2 % 2 = 0
    }

    #[test]
    fn test_distribute_partition_handles_edge_cases() {
        // Edge case - max u16 as partition key
        let result = distribute_partition(u16::MAX, 100, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], u16::MAX % 100);

        // Edge case - large but safe partition count
        let result = distribute_partition(42, 1000, 3);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], 42);
    }

    #[test]
    fn test_distribute_partition_deterministic() {
        // Run multiple times with the same inputs
        let first_result = distribute_partition(123, 10, 3);

        for _ in 0..10 {
            let result = distribute_partition(123, 10, 3);
            assert_eq!(result, first_result, "Results should be deterministic");
        }
    }

    #[test]
    fn test_distribute_partition_different_keys_different_distribution() {
        let key1_result = distribute_partition(5, 10, 3);
        let key2_result = distribute_partition(6, 10, 3);

        // They should have different distributions
        assert_ne!(key1_result, key2_result);
    }

    #[test]
    fn test_distribute_partition_avoids_cycles() {
        // Test case where jump value might cause a cycle
        // For 6 partitions, jump = 4, starting from 0
        // This would give 0->4->2->0->4->... which creates a cycle
        // The function should detect and break out of this

        let result = distribute_partition(0, 6, 6); // Try to get all 6 partitions

        // Should still have 6 unique partitions, avoiding the cycle
        assert_eq!(result.len(), 6);
        assert_eq!(result.iter().collect::<HashSet<_>>().len(), 6);
    }
}
