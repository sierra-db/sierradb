pub mod behaviour;
pub mod error;
pub mod partition_actor;
pub mod partition_consensus;
pub mod swarm_actor;
pub mod write_actor;

use arrayvec::ArrayVec;
use sierradb::MAX_REPLICATION_FACTOR;
use sierradb::bucket::{BucketId, PartitionId};
use sierradb::id::partition_id_to_bucket;
use thiserror::Error;

// pub fn replication_factor(&mut self, n: u8) -> &mut Self {
//     assert!(n > 0, "replication factor must be greater than 0");
//     assert!(
//         n <= MAX_REPLICATION_FACTOR as u8,
//         "replication factor cannot be not be greater than 12",
//     );
//     self.replication_factor = n;
//     self
// }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quorum {
    /// Only one replica needs to acknowledge the write.
    One,
    /// A majority of replicas (more than half) must acknowledge the write.
    Majority,
    /// Every replica must acknowledge the write.
    All,
}

impl Quorum {
    /// Given the total number of nodes, returns how many nodes are required to
    /// satisfy the quorum.
    pub fn required_buckets(self, replication_factor: u8) -> u8 {
        match self {
            Quorum::One => 1,
            Quorum::Majority => (replication_factor / 2) + 1,
            Quorum::All => replication_factor,
        }
    }
}

pub struct QuorumResult<T, E> {
    results: ArrayVec<(BucketId, Result<T, E>), MAX_REPLICATION_FACTOR>,
    quorum: Quorum,
    replication_factor: u8,
}

impl<T, E> QuorumResult<T, E> {
    // / Converts into a quorum result by checking if the number of successful
    // responses / meets the quorum. If so, returns an `Ok` containing all the
    // successful responses. / Otherwise, returns an `Err` with the original
    // `QuorumResult`. pub fn into_quorum_result(self) ->
    // Result<ArrayVec<(BucketId, T), MAX_REPLICATION_FACTOR>, Self> {     //
    // Determine how many successful results are required.     let required =
    // self.quorum.required_buckets(self.replication_factor) as usize;
    //     let success_count = self.results.iter().filter(|(_, res)|
    // res.is_ok()).count();

    //     if success_count >= required {
    //         // Collect the successful outcomes.
    //         let successes = self
    //             .results
    //             .into_iter()
    //             .filter_map(|(bucket_id, res)| res.ok().map(|value| (bucket_id,
    // value)))             .collect();
    //         Ok(successes)
    //     } else {
    //         Err(self)
    //     }
    // }

    pub fn into_quorum_result(self) -> Result<T, QuorumError<E>> {
        let required = self.quorum.required_buckets(self.replication_factor) as usize;
        let mut successes = ArrayVec::<(BucketId, T), MAX_REPLICATION_FACTOR>::new();
        let mut errors = ArrayVec::<(BucketId, E), MAX_REPLICATION_FACTOR>::new();

        for (bucket_id, result) in self.results {
            match result {
                Ok(val) => successes.push((bucket_id, val)),
                Err(e) => errors.push((bucket_id, e)),
            }
        }

        if successes.len() >= required {
            // Assuming that all successful writes return a consistent T,
            // we simply return the first one.
            Ok(successes.into_iter().next().unwrap().1)
        } else {
            Err(QuorumError::InsufficientSuccesses {
                successes: successes.len() as u8,
                required: required as u8,
                errors,
            })
        }
    }

    pub fn into_quorum_result_compare(self) -> Result<T, QuorumError<E>>
    where
        T: PartialEq,
    {
        let required = self.quorum.required_buckets(self.replication_factor) as usize;
        let mut successes = ArrayVec::<(BucketId, T), MAX_REPLICATION_FACTOR>::new();
        let mut errors = ArrayVec::<(BucketId, E), MAX_REPLICATION_FACTOR>::new();

        for (bucket_id, result) in self.results {
            match result {
                Ok(val) => successes.push((bucket_id, val)),
                Err(e) => errors.push((bucket_id, e)),
            }
        }

        if successes.len() >= required {
            // Assuming that all successful writes return a consistent T,
            // we simply return the first one.
            Ok(successes.into_iter().next().unwrap().1)
        } else {
            Err(QuorumError::InsufficientSuccesses {
                successes: successes.len() as u8,
                required: required as u8,
                errors,
            })
        }
    }
}

#[derive(Debug, Error)]
pub enum QuorumError<E> {
    #[error("quorum failed with {successes}/{required} successes")]
    InsufficientSuccesses {
        successes: u8,
        required: u8,
        errors: ArrayVec<(BucketId, E), MAX_REPLICATION_FACTOR>,
    },
}

/// Determines all buckets where an event should be stored based on replication
/// factor.
///
/// # Arguments
/// * `partition_id` - The partition ID of the event
/// * `num_buckets` - Total number of buckets in the system
/// * `replication_factor` - Number of copies to maintain (replication factor)
///
/// # Returns
/// A vector of bucket IDs where the event should be stored
pub fn get_replication_buckets(
    partition_id: PartitionId,
    num_buckets: u16,
    replication_factor: u8,
) -> ArrayVec<BucketId, MAX_REPLICATION_FACTOR> {
    assert!(num_buckets > 0);
    assert!(replication_factor > 0);

    // Ensure replication_factor doesn't exceed available buckets
    let actual_redundancy = replication_factor.min(num_buckets.try_into().unwrap_or(u8::MAX));

    // Handle special case - if we only have one bucket or replication is 1
    if num_buckets == 1 || actual_redundancy == 1 {
        return ArrayVec::from_iter([partition_id_to_bucket(partition_id, num_buckets)]);
    }

    let mut buckets = ArrayVec::new();

    // Add the primary bucket first
    let primary_bucket = partition_id_to_bucket(partition_id, num_buckets);
    buckets.push(primary_bucket);

    // Use a deterministic but well-distributed pattern for additional replicas
    // We'll use prime numbers as offsets to avoid collision patterns
    let offsets = [17, 31, 43, 67, 89, 101, 127, 151, 173, 197, 223, 241];

    let mut i = 0;
    while buckets.len() < actual_redundancy as usize {
        // Calculate next bucket with a prime number offset
        let offset = offsets[i % offsets.len()];
        let next_bucket = (primary_bucket + offset as u16) % num_buckets;

        // Only add if not already in our list
        if !buckets.contains(&next_bucket) {
            buckets.push(next_bucket);
        }

        i += 1;

        // Safety check to prevent infinite loop (very unlikely with prime offsets)
        if i > num_buckets as usize * 2 {
            break;
        }
    }

    buckets
}

pub fn get_replication_partitions(
    partition_id: PartitionId,
    num_partitions: u16,
    replication_factor: u8,
) -> ArrayVec<PartitionId, MAX_REPLICATION_FACTOR> {
    assert!(num_partitions > 0);
    assert!(replication_factor > 0);

    // Ensure replication_factor doesn't exceed available partitions
    let actual_redundancy = replication_factor.min(num_partitions.try_into().unwrap_or(u8::MAX));

    // Handle special case - if we only have one partition or replication is 1
    if num_partitions == 1 || actual_redundancy == 1 {
        return ArrayVec::from_iter([partition_id]);
    }

    let mut partition_ids = ArrayVec::new();

    // Add the primary partition first
    partition_ids.push(partition_id);

    // Use a deterministic but well-distributed pattern for additional replicas
    // We'll use prime numbers as offsets to avoid collision patterns
    let offsets = [17, 31, 43, 67, 89, 101, 127, 151, 173, 197, 223, 241];

    let mut i = 0;
    while partition_ids.len() < actual_redundancy as usize {
        // Calculate next partition with a prime number offset
        let offset = offsets[i % offsets.len()];
        let next_partition = (partition_id + offset as u16) % num_partitions;

        // Only add if not already in our list
        if !partition_ids.contains(&next_partition) {
            partition_ids.push(next_partition);
        }

        i += 1;

        // Safety check to prevent infinite loop (very unlikely with prime offsets)
        if i > num_partitions as usize * 2 {
            break;
        }
    }

    partition_ids
}
