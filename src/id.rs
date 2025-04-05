use std::time::{SystemTime, UNIX_EPOCH};

use arrayvec::ArrayVec;
use rand::Rng;
use uuid::Uuid;

use crate::{MAX_REDUNDANCY, RANDOM_STATE, bucket::BucketId};

/// Hashes the stream id, and performs a modulo on the lowest 16 bits of the hash.
pub fn stream_id_partition_id(stream_id: &str) -> u16 {
    (RANDOM_STATE.hash_one(stream_id) & 0xFFFF) as u16
}

pub fn stream_id_bucket(stream_id: &str, num_buckets: u16) -> BucketId {
    if num_buckets == 1 {
        return 0;
    }

    stream_id_partition_id(stream_id) % num_buckets
}

/// Determines all buckets where an event should be stored based on replication factor.
///
/// # Arguments
/// * `partition_id` - The partition ID of the event
/// * `num_buckets` - Total number of buckets in the system
/// * `replication_factor` - Number of copies to maintain (replication factor)
///
/// # Returns
/// A vector of bucket IDs where the event should be stored
pub fn get_replication_buckets(
    partition_id: u16,
    num_buckets: u16,
    replication_factor: u8,
) -> ArrayVec<u16, MAX_REDUNDANCY> {
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
    partition_id: u16,
    num_partitions: u16,
    replication_factor: u8,
) -> ArrayVec<u16, MAX_REDUNDANCY> {
    assert!(num_partitions > 0);
    assert!(replication_factor > 0);

    // Ensure replication_factor doesn't exceed available buckets
    let actual_redundancy = replication_factor.min(num_partitions.try_into().unwrap_or(u8::MAX));

    // Handle special case - if we only have one bucket or replication is 1
    if num_partitions == 1 || actual_redundancy == 1 {
        return ArrayVec::from_iter([partition_id]);
    }

    let mut buckets = ArrayVec::new();

    // Add the primary bucket first
    let primary_bucket = partition_id_to_bucket(partition_id, num_partitions);
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

/// Returns a UUID “inspired” by v7, except that 16 bits from the stream-id hash
/// are embedded in it (bits 46–61 of the final 128-bit value).
///
/// Layout (from MSB to LSB):
/// - 48 bits: timestamp (ms since Unix epoch)
/// - 12 bits: random
/// - 4 bits: version (0x7)
/// - 2 bits: variant (binary 10)
/// - 16 bits: stream-id hash (lower 16 bits)
/// - 46 bits: random
pub fn uuid_v7_with_stream_hash(stream_id: &str) -> Uuid {
    // Get current timestamp in milliseconds (48 bits)
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let timestamp_ms = now.as_millis() as u64;
    let timestamp48 = timestamp_ms & 0xFFFFFFFFFFFF; // mask to 48 bits

    // Compute stream-id hash
    let stream_hash = stream_id_partition_id(stream_id);

    let mut rng = rand::rng();
    // 12 bits of randomness
    let rand12: u16 = rng.random::<u16>() & 0x0FFF;
    // 46 bits of randomness
    let rand46: u64 = rng.random::<u64>() & ((1u64 << 46) - 1);

    // Assemble our 128-bit value. Bit layout (MSB = bit 127):
    // [timestamp:48] [rand12:12] [version:4] [variant:2] [stream_hash:16] [rand46:46]
    let uuid_u128: u128 = ((timestamp48 as u128) << 80)       // bits 127..80: timestamp (48 bits)
        | ((rand12 as u128) << 68)            // bits 79..68: 12-bit random
        | (0x7u128 << 64)                     // bits 67..64: version (4 bits, value 7)
        | (0x2u128 << 62)                     // bits 63..62: variant (2 bits, binary 10)
        | ((stream_hash as u128) << 46)        // bits 61..46: stream-id hash (16 bits)
        | (rand46 as u128); // bits 45..0: 46-bit random

    // Convert the u128 into a big-endian 16-byte array and create a UUID.
    Uuid::from_bytes(uuid_u128.to_be_bytes())
}

/// Extracts the embedded 16-bit stream hash from a UUID.
pub fn id_to_partition(uuid: Uuid) -> u16 {
    let uuid_u128 = u128::from_be_bytes(uuid.into_bytes());
    ((uuid_u128 >> 46) & 0xFFFF) as u16
}

pub fn extract_event_id_bucket(uuid: Uuid, num_buckets: u16) -> BucketId {
    if num_buckets == 1 {
        return 0;
    }

    id_to_partition(uuid) % num_buckets
}

pub fn partition_id_to_bucket(partition_id: u16, num_buckets: u16) -> BucketId {
    if num_buckets == 1 {
        return 0;
    }

    partition_id % num_buckets
}

pub fn validate_event_id(event_id: Uuid, stream_id: &str) -> bool {
    id_to_partition(event_id) == stream_id_partition_id(stream_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, time::Duration};

    #[test]
    fn test_uuid_monotonicity() {
        let id1 = uuid_v7_with_stream_hash("my-stream");
        let id2 = uuid_v7_with_stream_hash("my-stream");

        // Ensure the first few bytes (timestamp) are increasing
        let ts1 = u64::from_be_bytes([
            0,
            0,
            id1.as_bytes()[0],
            id1.as_bytes()[1],
            id1.as_bytes()[2],
            id1.as_bytes()[3],
            id1.as_bytes()[4],
            id1.as_bytes()[5],
        ]);
        let ts2 = u64::from_be_bytes([
            0,
            0,
            id2.as_bytes()[0],
            id2.as_bytes()[1],
            id2.as_bytes()[2],
            id2.as_bytes()[3],
            id2.as_bytes()[4],
            id2.as_bytes()[5],
        ]);

        assert!(ts1 <= ts2, "Timestamps should be increasing or equal");
    }

    #[test]
    fn test_extract_bucket_is_deterministic() {
        let id1 = uuid_v7_with_stream_hash("my-stream-abc");
        let id2 = uuid_v7_with_stream_hash("my-stream-abc");
        std::thread::sleep(Duration::from_millis(2));
        let id3 = uuid_v7_with_stream_hash("my-stream-abc");
        let id4 = uuid_v7_with_stream_hash("my-stream-abc");

        let hash1 = id_to_partition(id1);
        let hash2 = id_to_partition(id2);
        let hash3 = id_to_partition(id3);
        let hash4 = id_to_partition(id4);

        assert_eq!(hash1, hash2, "Same partition key should have the same hash");
        assert_eq!(hash2, hash3, "Same partition key should have the same hash");
        assert_eq!(hash3, hash4, "Same partition key should have the same hash");
    }

    #[test]
    fn test_uuid_uniqueness() {
        let mut seen = HashSet::new();

        for _ in 0..10_000 {
            let id = uuid_v7_with_stream_hash("my-stream");
            assert!(seen.insert(id), "Duplicate UUID generated!");
        }
    }

    #[test]
    fn test_bucket_distribution() {
        let num_buckets = 64;
        let mut counts = vec![0; num_buckets as usize];

        // Generate 10,000 unique ids
        for i in 0..10_000 {
            let stream_id = format!("my-stream-{i}");
            let id = uuid_v7_with_stream_hash(&stream_id);
            let bucket = id_to_partition(id) % num_buckets;
            counts[bucket as usize] += 1;
        }

        let avg = counts.iter().sum::<u32>() as f64 / num_buckets as f64;
        let std_dev = (counts
            .iter()
            .map(|&c| (c as f64 - avg).powi(2))
            .sum::<f64>()
            / num_buckets as f64)
            .sqrt();

        // Ensure that the distribution is roughly even
        assert!(std_dev < avg * 0.1, "Buckets should be evenly distributed");
    }

    #[test]
    fn test_redundant_buckets() {
        for r in 1..5 {
            for i in 0..u16::MAX {
                // Test with replication factor of 3, 64 buckets
                let buckets = get_replication_buckets(i, 64, r);
                assert_eq!(buckets.len(), r as usize);
                assert_eq!(buckets[0], partition_id_to_bucket(i, 64)); // Primary bucket
                assert!(buckets.iter().all(|&b| b < 64));
                assert!(buckets.iter().collect::<HashSet<_>>().len() == r as usize); // All unique

                // Test with replication factor exceeding buckets
                let buckets = get_replication_buckets(i, 3, r);
                assert_eq!(buckets.len(), 3.min(r as usize)); // Should be limited to number of buckets

                // Test with single bucket
                let buckets = get_replication_buckets(i, 1, r);
                assert_eq!(&buckets, [0].as_slice());
            }
        }
    }
}
