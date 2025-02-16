use std::{
    sync::atomic::{AtomicU16, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use rand::Rng;
use uuid::Uuid;

use crate::RANDOM_STATE;

static COUNTER: AtomicU16 = AtomicU16::new(0);

pub fn generate_event_id(correlation_id: &Uuid) -> Uuid {
    // Get current timestamp in milliseconds (48 bits)
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;
    let timestamp = timestamp & 0xFFFFFFFFFFFF; // Take only 48 bits

    // Increment atomic counter (16 bits, avoids collisions within the same millisecond)
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);

    // Hash correlation_id and take 16 bits for bucket mapping
    let correlation_hash = (RANDOM_STATE.hash_one(correlation_id) & 0xFFFF) as u16; // Take lowest 16 bits

    // Generate 48 bits of random entropy
    let random_part: u64 = rand::rng().random::<u64>() & 0xFFFFFFFFFFFF;

    // Construct the UUID bytes
    let mut bytes = [0u8; 16];
    bytes[0..6].copy_from_slice(&timestamp.to_be_bytes()[2..]); // 48-bit timestamp
    bytes[6..8].copy_from_slice(&counter.to_be_bytes()); // 16-bit counter
    bytes[8..10].copy_from_slice(&correlation_hash.to_be_bytes()); // 16-bit correlation hash
    bytes[10..16].copy_from_slice(&random_part.to_be_bytes()[2..]); // 48-bit random entropy

    Uuid::from_bytes(bytes)
}

pub fn extract_bucket_from_id(event_id: &Uuid, num_buckets: u16) -> u16 {
    let bytes = event_id.as_bytes();
    let correlation_hash = u16::from_be_bytes([bytes[8], bytes[9]]);
    correlation_hash % num_buckets
}

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

pub fn validate_event_id(event_id: &Uuid, correlation_id: &Uuid) -> bool {
    let event_id_bytes = event_id.as_bytes();
    let event_id_correlation_hash = u16::from_be_bytes([event_id_bytes[8], event_id_bytes[9]]);
    let correlation_hash = (RANDOM_STATE.hash_one(correlation_id) & 0xFFFF) as u16; // Take lowest 16 bits
    event_id_correlation_hash == correlation_hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, time::Duration};

    #[test]
    fn test_uuid_monotonicity() {
        let correlation_id = Uuid::new_v4();
        let id1 = generate_event_id(&correlation_id);
        let id2 = generate_event_id(&correlation_id);

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
        let correlation_id = Uuid::new_v4();

        let id1 = generate_event_id(&correlation_id);
        let id2 = generate_event_id(&correlation_id);
        std::thread::sleep(Duration::from_millis(2));
        let id3 = generate_event_id(&correlation_id);
        let id4 = generate_event_id(&correlation_id);

        let bucket1 = extract_bucket_from_id(&id1, 64);
        let bucket2 = extract_bucket_from_id(&id2, 64);
        let bucket3 = extract_bucket_from_id(&id3, 64);
        let bucket4 = extract_bucket_from_id(&id4, 64);

        assert_eq!(
            bucket1, bucket2,
            "Same correlation ID should map to the same bucket"
        );
        assert_eq!(
            bucket2, bucket3,
            "Same correlation ID should map to the same bucket"
        );
        assert_eq!(
            bucket3, bucket4,
            "Same correlation ID should map to the same bucket"
        );
    }

    #[test]
    fn test_uuid_uniqueness() {
        let correlation_id = Uuid::new_v4();
        let mut seen = HashSet::new();

        for _ in 0..10_000 {
            let id = generate_event_id(&correlation_id);
            assert!(seen.insert(id), "Duplicate UUID generated!");
        }
    }

    #[test]
    fn test_bucket_distribution() {
        let num_buckets = 64;
        let mut counts = vec![0; num_buckets as usize];

        // Generate 10,000 unique correlation IDs
        for _ in 0..10_000 {
            let correlation_id = Uuid::new_v4();
            let id = generate_event_id(&correlation_id);
            let bucket = extract_bucket_from_id(&id, num_buckets);
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
