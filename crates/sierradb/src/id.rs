use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;
use uuid::Uuid;

use crate::RANDOM_STATE;
use crate::bucket::{BucketId, PartitionId};

/// Hashes the stream id, and performs a modulo on the lowest 16 bits of the
/// hash.
pub fn stream_id_partition_id(stream_id: &str) -> PartitionId {
    (RANDOM_STATE.hash_one(stream_id) & 0xFFFF) as u16
}

pub fn stream_id_bucket(stream_id: &str, num_buckets: u16) -> BucketId {
    if num_buckets == 1 {
        return 0;
    }

    stream_id_partition_id(stream_id) % num_buckets
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
    // [timestamp:48] [rand12:12] [version:4] [variant:2] [stream_hash:16]
    // [rand46:46]
    let uuid_u128: u128 = ((timestamp48 as u128) << 80) // bits 127..80: timestamp (48 bits)
        | ((rand12 as u128) << 68)      // bits 79..68: 12-bit random
        | (0x7u128 << 64)               // bits 67..64: version (4 bits, value 7)
        | (0x2u128 << 62)               // bits 63..62: variant (2 bits, binary 10)
        | ((stream_hash as u128) << 46) // bits 61..46: stream-id hash (16 bits)
        | (rand46 as u128); // bits 45..0: 46-bit random

    // Convert the u128 into a big-endian 16-byte array and create a UUID.
    Uuid::from_bytes(uuid_u128.to_be_bytes())
}

/// Extracts the embedded 16-bit stream hash from a UUID.
pub fn uuid_to_partition_id(uuid: Uuid) -> u16 {
    let uuid_u128 = u128::from_be_bytes(uuid.into_bytes());
    ((uuid_u128 >> 46) & 0xFFFF) as u16
}

pub fn extract_event_id_bucket(uuid: Uuid, num_buckets: u16) -> BucketId {
    if num_buckets == 1 {
        return 0;
    }

    uuid_to_partition_id(uuid) % num_buckets
}

pub fn partition_id_to_bucket(partition_id: PartitionId, num_buckets: u16) -> BucketId {
    if num_buckets == 1 {
        return 0;
    }

    partition_id % num_buckets
}

pub fn validate_event_id(event_id: Uuid, stream_id: &str) -> bool {
    uuid_to_partition_id(event_id) == stream_id_partition_id(stream_id)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use super::*;

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

        let hash1 = uuid_to_partition_id(id1);
        let hash2 = uuid_to_partition_id(id2);
        let hash3 = uuid_to_partition_id(id3);
        let hash4 = uuid_to_partition_id(id4);

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
            let bucket = uuid_to_partition_id(id) % num_buckets;
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
}
