use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;
use uuid::Uuid;

use crate::{RANDOM_STATE, bucket::BucketId};

// pub fn generate_correlation_id(stream_id: &str) -> Uuid {
//     // Compute a 64-bit hash (using, say, xxh3 or ahash) of the stream id.
//     let full_hash = RANDOM_STATE.hash_one(stream_id);
//     // Take the lower 16 bits as the partition key.
//     let stream_hash: u16 = (full_hash & 0xFFFF) as u16;

//     // Create a 128-bit value (using, for example, a custom UUID format)
//     // where bytes 8-9 are reserved for the stream_hash.
//     let mut bytes = [0u8; 16];

//     // Fill in other parts of the correlation ID (e.g., a timestamp and random data)
//     let timestamp = SystemTime::now()
//         .duration_since(UNIX_EPOCH)
//         .expect("Time went backwards")
//         .as_millis() as u64;
//     bytes[0..6].copy_from_slice(&timestamp.to_be_bytes()[2..]);

//     // Embed the stream_hash into bytes 8-10
//     bytes[8..10].copy_from_slice(&stream_hash.to_be_bytes());
//     // Fill in the remaining bytes with random entropy, etc.
//     let random_part: u64 = rand::random::<u64>() & 0xFFFFFFFFFFFF; // 48 bits
//     bytes[10..16].copy_from_slice(&random_part.to_be_bytes()[2..]);

//     Uuid::from_bytes(bytes)
// }

// pub fn generate_event_id(correlation_id: &Uuid) -> Uuid {
//     // Get current timestamp in milliseconds (48 bits)
//     let timestamp = SystemTime::now()
//         .duration_since(UNIX_EPOCH)
//         .expect("Time went backwards")
//         .as_millis() as u64;
//     let timestamp = timestamp & 0xFFFFFFFFFFFF; // Take only 48 bits

//     // Increment atomic counter (16 bits, avoids collisions within the same millisecond)
//     let counter = COUNTER.fetch_add(1, Ordering::Relaxed);

//     // Hash correlation_id and take 16 bits for bucket mapping
//     let correlation_hash = (RANDOM_STATE.hash_one(correlation_id) & 0xFFFF) as u16; // Take lowest 16 bits

//     // Generate 48 bits of random entropy
//     let random_part: u64 = rand::rng().random::<u64>() & 0xFFFFFFFFFFFF;

//     // Construct the UUID bytes
//     let mut bytes = [0u8; 16];
//     bytes[0..6].copy_from_slice(&timestamp.to_be_bytes()[2..]); // 48-bit timestamp
//     bytes[6..8].copy_from_slice(&counter.to_be_bytes()); // 16-bit counter
//     bytes[8..10].copy_from_slice(&correlation_hash.to_be_bytes()); // 16-bit correlation hash
//     bytes[10..16].copy_from_slice(&random_part.to_be_bytes()[2..]); // 48-bit random entropy

//     Uuid::from_bytes(bytes)
// }

/// Hashes the stream id, and performs a modulo on the lowest 16 bits of the hash.
pub fn stream_id_hash(stream_id: &str) -> u16 {
    (RANDOM_STATE.hash_one(stream_id) & 0xFFFF) as u16
}

pub fn stream_id_bucket(stream_id: &str, num_buckets: u16) -> BucketId {
    if num_buckets == 1 {
        return 0;
    }

    stream_id_hash(stream_id) % num_buckets
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
    let stream_hash = stream_id_hash(stream_id);

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
pub fn extract_stream_hash(uuid: Uuid) -> u16 {
    let uuid_u128 = u128::from_be_bytes(uuid.into_bytes());
    ((uuid_u128 >> 46) & 0xFFFF) as u16
}

pub fn extract_event_id_bucket(uuid: Uuid, num_buckets: u16) -> BucketId {
    if num_buckets == 1 {
        return 0;
    }

    extract_stream_hash(uuid) % num_buckets
}

// pub fn extract_bucket_from_correlation_id(correlation_id: &Uuid, num_buckets: u16) -> u16 {
//     let bytes = correlation_id.as_bytes();
//     let stream_hash = u16::from_be_bytes([bytes[8], bytes[9]]);
//     stream_hash % num_buckets
// }

// pub fn extract_bucket_from_id(event_id: &Uuid, num_buckets: u16) -> u16 {
//     let bytes = event_id.as_bytes();
//     let correlation_hash = u16::from_be_bytes([bytes[8], bytes[9]]);
//     correlation_hash % num_buckets
// }

// pub fn extract_buckets_with_nodes(
//     event_id: &Uuid,
//     num_buckets: u16,
//     num_nodes: u16,
//     rf: u16
// ) -> Vec<(u16, u16)> {
//     let bytes = event_id.as_bytes();
//     let correlation_hash = u16::from_be_bytes([bytes[8], bytes[9]]);

//     let mut primary_bucket = correlation_hash % num_buckets;
//     let mut buckets = Vec::with_capacity(rf as usize);

//     for i in 0..rf {
//         let node_id = primary_bucket % num_nodes; // Assign bucket to a node
//         buckets.push((primary_bucket, node_id));
//         primary_bucket = (primary_bucket + (num_buckets / num_nodes)) % num_buckets; // Jump to next node
//     }

//     buckets
// }

pub fn validate_event_id(event_id: Uuid, stream_id: &str) -> bool {
    extract_stream_hash(event_id) == stream_id_hash(stream_id)
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

        let hash1 = extract_stream_hash(id1);
        let hash2 = extract_stream_hash(id2);
        let hash3 = extract_stream_hash(id3);
        let hash4 = extract_stream_hash(id4);

        assert_eq!(
            hash1, hash2,
            "Same correlation ID should have the same hash"
        );
        assert_eq!(
            hash2, hash3,
            "Same correlation ID should have the same hash"
        );
        assert_eq!(
            hash3, hash4,
            "Same correlation ID should have the same hash"
        );
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
            let bucket = extract_stream_hash(id) % num_buckets;
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
