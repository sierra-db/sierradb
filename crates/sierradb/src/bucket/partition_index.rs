//! The file format for an MPHF-based partition index is defined as follows:
//! - `[0..4]`     : magic marker: `b"PIDX"`
//! - `[4..12]`    : number of keys (n) as a `u64`
//! - `[12..20]`   : length of serialized MPHF (L) as a `u64`
//! - `[20..20+L]` : serialized MPHF bytes (using bincode)
//! - `[20+L..]`   : records array, exactly n records of RECORD_SIZE bytes each.

use std::mem;

use crate::bucket::PartitionId;

mod closed;
mod open;

pub use self::closed::{ClosedIndex, ClosedOffsetKind, ClosedPartitionIndex};
pub use self::open::OpenPartitionIndex;

// Partition ID, sequence min, sequence max, event count, events offset, events
// length
const PARTITION_ID_SIZE: usize = mem::size_of::<PartitionId>();
const SEQUENCE_SIZE: usize = mem::size_of::<u64>();
const EVENTS_OFFSET_SIZE: usize = mem::size_of::<u64>();
const EVENTS_LEN_SIZE: usize = mem::size_of::<u32>();
const RECORD_SIZE: usize = PARTITION_ID_SIZE
    + SEQUENCE_SIZE
    + SEQUENCE_SIZE
    + SEQUENCE_SIZE
    + EVENTS_OFFSET_SIZE
    + EVENTS_LEN_SIZE;

const MPHF_GAMMA: f64 = 1.4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionIndexRecord<T> {
    pub sequence_min: u64,
    pub sequence_max: u64,
    pub sequence: u64,
    pub offsets: T,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionOffsets {
    Offsets(Vec<PartitionSequenceOffset>), // Its cached
    ExternalBucket,                        // This partition lives in a different bucket
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionSequenceOffset {
    pub sequence: u64,
    pub offset: u64,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub use sierradb_protocol::ExpectedVersion;
    use smallvec::smallvec;
    use uuid::Uuid;

    use super::*;
    use crate::StreamId;
    use crate::bucket::BucketSegmentId;
    use crate::bucket::segment::CommittedEvents;
    use crate::cache::BLOCK_SIZE;
    use crate::database::{Database, DatabaseBuilder, NewEvent, Transaction};
    use crate::id::{uuid_to_partition_hash, uuid_v7_with_partition_hash};

    const SEGMENT_SIZE: usize = 256_000_000; // 256 MB
    const SMALL_SEGMENT_SIZE: usize = BLOCK_SIZE * 2; // 2 blocks (128KB), forcing multiple segments

    fn temp_file_path() -> PathBuf {
        // Create a unique filename for each test to avoid conflicts
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        tempfile::Builder::new()
            .prefix(&format!("test_{timestamp}_"))
            .make(|path| Ok(path.to_path_buf()))
            .unwrap()
            .path()
            .to_path_buf()
    }

    fn temp_dir() -> PathBuf {
        tempfile::tempdir()
            .expect("failed to create temp dir")
            .path()
            .to_path_buf()
    }

    /// Create a test database with configurable segment size for forcing
    /// multiple segments
    async fn create_test_database(segment_size_bytes: usize) -> Database {
        let dir = temp_dir();
        DatabaseBuilder::new()
            .segment_size_bytes(segment_size_bytes)
            .total_buckets(1) // Use single bucket for predictable testing
            .bucket_ids(Arc::from(vec![0])) // Single bucket with ID 0
            .reader_threads(1)
            .writer_threads(1)
            .open(dir)
            .expect("failed to create test database")
    }

    /// Helper to create test events for a stream
    fn create_test_event(stream_id: &str, partition_key: Uuid, partition_id: u16) -> NewEvent {
        let partition_hash = uuid_to_partition_hash(partition_key);
        let event_id = uuid_v7_with_partition_hash(partition_hash);

        NewEvent {
            event_id,
            stream_id: StreamId::new(stream_id).expect("invalid stream id"),
            stream_version: ExpectedVersion::Any,
            event_name: "TestEvent".to_string(),
            timestamp: 1000 + (partition_id as u64 * 1000), /* Different timestamp per event and
                                                             * partition */
            metadata: vec![],
            payload: b"payload".to_vec(),
        }
    }

    /// Helper to append events to database and return their details
    async fn append_events_to_stream(
        db: &Database,
        stream_id: &str,
        count: usize,
        partition_key: Uuid,
        partition_id: u16,
    ) -> Result<Vec<(u64, Uuid)>, Box<dyn std::error::Error>> {
        let mut results = Vec::new();

        for i in 0..count {
            let event = create_test_event(stream_id, partition_key, partition_id);
            let event_id = event.event_id;
            let transaction = Transaction::new(partition_key, partition_id, smallvec![event])?;
            let _result = db.append_events(transaction).await?;
            results.push((i as u64, event_id));
        }

        Ok(results)
    }

    /// Helper to append multiple events in a single transaction
    async fn append_transaction_events(
        db: &Database,
        events: Vec<NewEvent>,
        partition_key: Uuid,
        partition_id: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let transaction = Transaction::new(partition_key, partition_id, events.into())?;
        db.append_events(transaction).await?;
        Ok(())
    }

    #[test]
    fn test_open_partition_index_insert_and_get() {
        let path = temp_file_path();
        let mut index = OpenPartitionIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();

        let partition_id = 42;
        let offsets = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 1000,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 2000,
            },
            PartitionSequenceOffset {
                sequence: 2,
                offset: 3000,
            },
        ];

        for (i, offset) in offsets.iter().enumerate() {
            index.insert(partition_id, i as u64, offset.offset).unwrap();
        }

        let record = index.get(partition_id).unwrap();
        assert_eq!(
            record,
            &PartitionIndexRecord {
                sequence_min: 0,
                sequence_max: 2,
                sequence: 3,
                offsets: PartitionOffsets::Offsets(offsets),
            }
        );
        assert_eq!(index.get(99), None);
    }

    #[test]
    fn test_closed_partition_index_lookup() {
        let path = temp_file_path();
        let mut index = OpenPartitionIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();

        let partition_id1 = 1;
        let partition_id2 = 2;
        let offsets1 = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 100,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 200,
            },
        ];
        let offsets2 = vec![PartitionSequenceOffset {
            sequence: 0,
            offset: 300,
        }];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(partition_id1, i as u64, offset.offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(partition_id2, i as u64, offset.offset)
                .unwrap();
        }

        // Flush with the new MPHF format
        let (mphf, _) = index.flush().unwrap();
        assert!(mphf.try_hash(&partition_id1).is_some());
        assert!(mphf.try_hash(&partition_id2).is_some());
        assert!(mphf.try_hash(&999).is_none());

        // Open with the closed index
        let mut closed_index =
            ClosedPartitionIndex::open(BucketSegmentId::new(0, 0), &path).unwrap();

        // Test get_key with MPHF implementation
        let key1 = closed_index.get_key(partition_id1).unwrap().unwrap();
        assert_eq!(key1.sequence_min, 0);
        assert_eq!(key1.sequence_max, 1);
        assert_eq!(key1.sequence, 2);

        // Test get with MPHF implementation
        assert_eq!(
            closed_index.get(partition_id1).unwrap(),
            Some(PartitionOffsets::Offsets(offsets1)),
        );

        // Test get_key for second partition ID
        let key2 = closed_index.get_key(partition_id2).unwrap().unwrap();
        assert_eq!(key2.sequence_min, 0);
        assert_eq!(key2.sequence_max, 0);
        assert_eq!(key2.sequence, 1);

        // Test get for second partition ID
        assert_eq!(
            closed_index.get(partition_id2).unwrap(),
            Some(PartitionOffsets::Offsets(offsets2)),
        );

        // Test unknown partition ID
        assert_eq!(closed_index.get_key(999).unwrap(), None);
        assert_eq!(closed_index.get(999).unwrap(), None);
    }

    #[test]
    fn test_mphf_collision_handling() {
        let path = temp_file_path();
        let mut index = OpenPartitionIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();

        // Add multiple partitions to test MPHF collision handling
        let partition_id1 = 1;
        let partition_id2 = 2;
        let partition_id3 = 3;
        let partition_id4 = 4;

        let offsets1 = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 1001,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 1002,
            },
        ];
        let offsets2 = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 2001,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 2002,
            },
            PartitionSequenceOffset {
                sequence: 2,
                offset: 2003,
            },
        ];
        let offsets3 = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 3001,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 3002,
            },
        ];
        let offsets4 = vec![PartitionSequenceOffset {
            sequence: 0,
            offset: 4001,
        }];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(partition_id1, i as u64, offset.offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(partition_id2, i as u64, offset.offset)
                .unwrap();
        }
        for (i, offset) in offsets3.iter().enumerate() {
            index
                .insert(partition_id3, i as u64, offset.offset)
                .unwrap();
        }
        for (i, offset) in offsets4.iter().enumerate() {
            index
                .insert(partition_id4, i as u64, offset.offset)
                .unwrap();
        }

        // Flush with the new MPHF format
        let (mphf, _) = index.flush().unwrap();

        // Verify MPHF correctness - all partitions should have different hash values
        let hash1 = mphf.hash(&partition_id1);
        let hash2 = mphf.hash(&partition_id2);
        let hash3 = mphf.hash(&partition_id3);
        let hash4 = mphf.hash(&partition_id4);

        assert_ne!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_ne!(hash1, hash4);
        assert_ne!(hash2, hash3);
        assert_ne!(hash2, hash4);
        assert_ne!(hash3, hash4);

        // Test with closed index
        let mut closed_index =
            ClosedPartitionIndex::open(BucketSegmentId::new(0, 0), &path).unwrap();

        // Verify all partitions can be retrieved correctly
        assert_eq!(
            closed_index.get(partition_id1).unwrap(),
            Some(PartitionOffsets::Offsets(offsets1))
        );
        assert_eq!(
            closed_index.get(partition_id2).unwrap(),
            Some(PartitionOffsets::Offsets(offsets2))
        );
        assert_eq!(
            closed_index.get(partition_id3).unwrap(),
            Some(PartitionOffsets::Offsets(offsets3))
        );
        assert_eq!(
            closed_index.get(partition_id4).unwrap(),
            Some(PartitionOffsets::Offsets(offsets4))
        );

        // Unknown partition ID should not be found
        assert_eq!(closed_index.get(999).unwrap(), None);
    }

    #[test]
    fn test_insert_external_bucket() {
        let path = temp_file_path();

        let partition_id = 123;

        let mut index = OpenPartitionIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();
        index.insert_external_bucket(partition_id).unwrap();
        assert_eq!(
            index.get(partition_id),
            Some(&PartitionIndexRecord {
                sequence_min: 0,
                sequence_max: 0,
                sequence: 0,
                offsets: PartitionOffsets::ExternalBucket
            })
        );
        index.flush().unwrap();

        let mut index = ClosedPartitionIndex::open(BucketSegmentId::new(0, 0), &path).unwrap();
        assert_eq!(
            index.get(partition_id).unwrap(),
            Some(PartitionOffsets::ExternalBucket)
        );
    }

    #[tokio::test]
    async fn test_partition_iter_single_segment() {
        let db = create_test_database(SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Append 10 events to a single partition
        let event_count = 10;
        append_events_to_stream(&db, "stream-a", event_count, partition_key, partition_id)
            .await
            .expect("failed to append events");

        // Read all events in partition
        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Verify all events were read in sequence order
        assert_eq!(collected_sequences.len(), event_count);
        for (i, &seq) in collected_sequences.iter().enumerate() {
            assert_eq!(seq, i as u64, "sequences should be in order");
        }
    }

    #[tokio::test]
    async fn test_partition_iter_multiple_segments() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Append enough events to force multiple segments
        let event_count = 1500;
        append_events_to_stream(&db, "stream-a", event_count, partition_key, partition_id)
            .await
            .expect("failed to append events");

        // Force segments to close
        let _other_events =
            append_events_to_stream(&db, "stream-b", 100, Uuid::new_v4(), partition_id + 1).await;

        // Read all events from partition
        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Verify all events were read in order across segments
        assert_eq!(collected_sequences.len(), event_count);
        for (i, &seq) in collected_sequences.iter().enumerate() {
            assert_eq!(
                seq, i as u64,
                "sequences should be sequential across segments"
            );
        }
    }

    #[tokio::test]
    async fn test_partition_iter_batch_reading() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        let event_count = 1500;
        append_events_to_stream(&db, "stream-a", event_count, partition_key, partition_id)
            .await
            .expect("failed to append events");

        // Test next_batch with different limits
        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut all_sequences = Vec::new();
        let batch_size = 5;

        while let Some(batch) = partition_iter
            .next_batch(batch_size)
            .await
            .expect("read err")
        {
            assert!(!batch.is_empty());
            for committed_events in batch {
                match committed_events {
                    CommittedEvents::Single(event) => {
                        all_sequences.push(event.partition_sequence);
                    }
                    CommittedEvents::Transaction { events, .. } => {
                        for event in events.iter() {
                            all_sequences.push(event.partition_sequence);
                        }
                    }
                }
            }
        }

        assert_eq!(all_sequences.len(), event_count);
        for (i, &seq) in all_sequences.iter().enumerate() {
            assert_eq!(seq, i as u64);
        }
    }

    #[tokio::test]
    async fn test_partition_iter_from_middle_sequence() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        let event_count = 20;
        append_events_to_stream(&db, "stream-a", event_count, partition_key, partition_id)
            .await
            .expect("failed to append events");

        // Start reading from sequence 10
        let start_sequence = 10;
        let mut partition_iter = db
            .read_partition(partition_id, start_sequence)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Should read sequences from 10 to 19
        let expected_count = event_count - start_sequence as usize;
        assert_eq!(collected_sequences.len(), expected_count);
        assert_eq!(collected_sequences[0], start_sequence);
        assert_eq!(
            collected_sequences[collected_sequences.len() - 1],
            event_count as u64 - 1
        );
    }

    #[tokio::test]
    async fn test_partition_iter_multiple_streams_single_transaction() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Create transaction with events for multiple streams in same partition
        let events = vec![
            create_test_event("stream-a", partition_key, partition_id),
            create_test_event("stream-b", partition_key, partition_id),
            create_test_event("stream-a", partition_key, partition_id),
            create_test_event("stream-c", partition_key, partition_id),
            create_test_event("stream-a", partition_key, partition_id),
        ];

        append_transaction_events(&db, events, partition_key, partition_id)
            .await
            .expect("failed to append transaction events");

        // Read partition - should get ALL events
        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        let mut stream_counts = std::collections::HashMap::new();

        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                    *stream_counts
                        .entry(event.stream_id.to_string())
                        .or_insert(0) += 1;
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                        *stream_counts
                            .entry(event.stream_id.to_string())
                            .or_insert(0) += 1;
                    }
                }
            }
        }

        // Should have all 5 events
        assert_eq!(collected_sequences, vec![0, 1, 2, 3, 4]);
        assert_eq!(stream_counts.get("stream-a"), Some(&3));
        assert_eq!(stream_counts.get("stream-b"), Some(&1));
        assert_eq!(stream_counts.get("stream-c"), Some(&1));
    }

    #[tokio::test]
    async fn test_partition_iter_transaction_deduplication() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Create transaction with multiple events in same partition
        let events = vec![
            create_test_event("stream-a", partition_key, partition_id),
            create_test_event("stream-a", partition_key, partition_id),
            create_test_event("stream-a", partition_key, partition_id),
        ];

        append_transaction_events(&db, events, partition_key, partition_id)
            .await
            .expect("failed to append transaction events");

        // Add another single event
        append_events_to_stream(&db, "stream-a", 1, partition_key, partition_id)
            .await
            .expect("failed to append single event");

        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        let mut transaction_count = 0;
        let mut single_event_count = 0;

        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(_) => {
                    single_event_count += 1;
                    if let CommittedEvents::Single(event) = committed_events {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
                CommittedEvents::Transaction { events, .. } => {
                    transaction_count += 1;
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Should have one transaction (with 3 events) and one single event
        assert_eq!(transaction_count, 1, "should have exactly one transaction");
        assert_eq!(
            single_event_count, 1,
            "should have exactly one single event"
        );
        assert_eq!(
            collected_sequences,
            vec![0, 1, 2, 3],
            "sequences should be in order"
        );
    }

    #[tokio::test]
    async fn test_partition_iter_mixed_streams_across_segments() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Append events for multiple streams in the same partition
        append_events_to_stream(&db, "stream-a", 500, partition_key, partition_id)
            .await
            .expect("failed to append stream-a events");
        append_events_to_stream(&db, "stream-b", 500, partition_key, partition_id)
            .await
            .expect("failed to append stream-b events");
        append_events_to_stream(&db, "stream-c", 500, partition_key, partition_id)
            .await
            .expect("failed to append stream-c events");

        // Force segments to close
        let _other_events =
            append_events_to_stream(&db, "other-stream", 100, Uuid::new_v4(), partition_id + 1)
                .await;

        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Should have all 1500 events in sequence order
        assert_eq!(collected_sequences.len(), 1500);
        for (i, &seq) in collected_sequences.iter().enumerate() {
            assert_eq!(seq, i as u64, "sequences should be sequential");
        }
    }

    #[tokio::test]
    async fn test_partition_iter_no_gaps_across_segments() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Append enough events to span multiple segments
        let total_events = 100;
        for _ in 0..10 {
            for _ in 0..10 {
                let event = create_test_event("stream-a", partition_key, partition_id);
                let transaction = Transaction::new(partition_key, partition_id, smallvec![event])
                    .expect("failed to create transaction");
                db.append_events(transaction)
                    .await
                    .expect("failed to append event");
            }
        }

        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Verify no gaps and correct count
        assert_eq!(collected_sequences.len(), total_events);

        // Check for any gaps or duplicates
        for (i, &seq) in collected_sequences.iter().enumerate() {
            assert_eq!(
                seq, i as u64,
                "gap or duplicate detected: expected={i}, actual={seq}"
            );
        }
    }

    #[tokio::test]
    async fn test_partition_iter_large_partition_completeness() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Create a large partition with mixed streams and transactions
        let mut expected_count = 0;

        // Add 50 single events to stream-a
        append_events_to_stream(&db, "stream-a", 50, partition_key, partition_id)
            .await
            .expect("failed to append stream-a events");
        expected_count += 50;

        // Add 10 transactions with 3 events each to stream-b
        for _ in 0..10 {
            let transaction_events = vec![
                create_test_event("stream-b", partition_key, partition_id),
                create_test_event("stream-b", partition_key, partition_id),
                create_test_event("stream-b", partition_key, partition_id),
            ];
            append_transaction_events(&db, transaction_events, partition_key, partition_id)
                .await
                .expect("failed to append transaction");
            expected_count += 3;
        }

        // Add 20 more single events to stream-c
        append_events_to_stream(&db, "stream-c", 20, partition_key, partition_id)
            .await
            .expect("failed to append stream-c events");
        expected_count += 20;

        // Read all events from partition
        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Verify completeness
        assert_eq!(collected_sequences.len(), expected_count);
        for (i, &seq) in collected_sequences.iter().enumerate() {
            assert_eq!(seq, i as u64);
        }
    }

    #[tokio::test]
    async fn test_partition_iter_batch_vs_single_consistency() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Add mixed events
        append_events_to_stream(&db, "stream-a", 10, partition_key, partition_id)
            .await
            .expect("failed to append events");

        let transaction_events = vec![
            create_test_event("stream-b", partition_key, partition_id),
            create_test_event("stream-b", partition_key, partition_id),
        ];
        append_transaction_events(&db, transaction_events, partition_key, partition_id)
            .await
            .expect("failed to append transaction");

        // Read using next() method
        let mut partition_iter1 = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut single_read_sequences = Vec::new();
        while let Some(committed_events) = partition_iter1.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    single_read_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        single_read_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Read using next_batch() method
        let mut partition_iter2 = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut batch_read_sequences = Vec::new();
        while let Some(batch) = partition_iter2.next_batch(5).await.expect("read err") {
            for committed_events in batch {
                match committed_events {
                    CommittedEvents::Single(event) => {
                        batch_read_sequences.push(event.partition_sequence);
                    }
                    CommittedEvents::Transaction { events, .. } => {
                        for event in events.iter() {
                            batch_read_sequences.push(event.partition_sequence);
                        }
                    }
                }
            }
        }

        // Both methods should return identical results
        assert_eq!(single_read_sequences, batch_read_sequences);
        assert_eq!(single_read_sequences.len(), 12); // 10 single + 2 from transaction
    }

    #[tokio::test]
    async fn test_partition_iter_cache_behavior() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Create enough events to force multiple segments and cache operations
        let event_count = 1500;
        append_events_to_stream(&db, "stream-a", event_count, partition_key, partition_id)
            .await
            .expect("failed to append events");

        // First pass - populate cache
        let mut partition_iter1 = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut first_pass_sequences = Vec::new();
        while let Some(committed_events) = partition_iter1.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    first_pass_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        first_pass_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Second pass - should use cached data
        let mut partition_iter2 = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut second_pass_sequences = Vec::new();
        while let Some(committed_events) = partition_iter2.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    second_pass_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        second_pass_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Both passes should return identical results
        assert_eq!(first_pass_sequences, second_pass_sequences);
        assert_eq!(first_pass_sequences.len(), event_count);
    }

    #[tokio::test]
    async fn test_partition_iter_empty_partition() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_id = 0;

        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let result = partition_iter.next().await.expect("read err");
        assert!(result.is_none(), "empty partition should return None");

        // Test batch reading on empty partition
        let batch = partition_iter.next_batch(10).await.expect("read err");
        assert!(batch.is_none(), "empty partition should return empty batch");
    }

    #[tokio::test]
    async fn test_partition_iter_start_beyond_partition_end() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Add only 5 events
        append_events_to_stream(&db, "stream-a", 5, partition_key, partition_id)
            .await
            .expect("failed to append events");

        // Try to start reading from sequence 10 (beyond partition end)
        let mut partition_iter = db
            .read_partition(partition_id, 10)
            .await
            .expect("failed to create partition iterator");

        let result = partition_iter.next().await.expect("read err");
        assert!(
            result.is_none(),
            "reading beyond partition end should return None"
        );
    }

    #[tokio::test]
    async fn test_partition_iter_very_large_batch() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        let event_count = 20;
        append_events_to_stream(&db, "stream-a", event_count, partition_key, partition_id)
            .await
            .expect("failed to append events");

        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        // Request more events than available
        let batch = partition_iter
            .next_batch(100)
            .await
            .expect("read err")
            .expect("not empty batch");

        let mut collected_sequences = Vec::new();
        for committed_events in batch {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Should return all available events
        assert_eq!(collected_sequences.len(), event_count);
        for (i, &seq) in collected_sequences.iter().enumerate() {
            assert_eq!(seq, i as u64);
        }

        // Subsequent batch should be empty
        let empty_batch = partition_iter.next_batch(10).await.expect("read err");
        assert!(empty_batch.is_none());
    }

    #[tokio::test]
    async fn test_partition_iter_partial_partition_reading() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        let event_count = 50;
        append_events_to_stream(&db, "stream-a", event_count, partition_key, partition_id)
            .await
            .expect("failed to append events");

        // Read only first 25 events
        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        let target_count = 25;

        while collected_sequences.len() < target_count {
            if let Some(committed_events) = partition_iter.next().await.expect("read err") {
                match committed_events {
                    CommittedEvents::Single(event) => {
                        collected_sequences.push(event.partition_sequence);
                    }
                    CommittedEvents::Transaction { events, .. } => {
                        for event in events.iter() {
                            collected_sequences.push(event.partition_sequence);
                        }
                    }
                }
            } else {
                break;
            }
        }

        assert_eq!(collected_sequences.len(), target_count);
        for (i, &seq) in collected_sequences.iter().enumerate() {
            assert_eq!(seq, i as u64);
        }
    }

    #[tokio::test]
    async fn test_partition_iter_mixed_transactions() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Create multiple transactions with different streams
        for _ in 0..5 {
            let mixed_events = vec![
                create_test_event("stream-a", partition_key, partition_id),
                create_test_event("stream-b", partition_key, partition_id),
                create_test_event("stream-a", partition_key, partition_id),
                create_test_event("stream-c", partition_key, partition_id),
            ];
            append_transaction_events(&db, mixed_events, partition_key, partition_id)
                .await
                .expect("failed to append mixed transaction");
        }

        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut all_sequences = Vec::new();
        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    all_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        all_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Should have 20 events total (4 per transaction * 5 transactions)
        assert_eq!(all_sequences.len(), 20);
        for (i, &seq) in all_sequences.iter().enumerate() {
            assert_eq!(seq, i as u64);
        }
    }

    #[tokio::test]
    async fn test_partition_iter_ordering_with_mixed_events() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Mix of single events and transactions
        append_events_to_stream(&db, "stream-a", 3, partition_key, partition_id)
            .await
            .expect("failed to append initial events");

        // Transaction with multiple events
        let transaction_events = vec![
            create_test_event("stream-b", partition_key, partition_id),
            create_test_event("stream-b", partition_key, partition_id),
        ];
        append_transaction_events(&db, transaction_events, partition_key, partition_id)
            .await
            .expect("failed to append transaction");

        // More single events
        append_events_to_stream(&db, "stream-c", 2, partition_key, partition_id)
            .await
            .expect("failed to append final events");

        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();
        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Should read: 0,1,2 (stream-a), then 3,4 (transaction), then 5,6 (stream-c)
        let expected = vec![0, 1, 2, 3, 4, 5, 6];
        assert_eq!(collected_sequences, expected);
    }

    #[tokio::test]
    async fn test_partition_iter_concurrent_writing_reading() {
        let db = create_test_database(SMALL_SEGMENT_SIZE).await;
        let partition_key = Uuid::new_v4();
        let partition_id = 0;

        // Add initial events
        append_events_to_stream(&db, "stream-a", 10, partition_key, partition_id)
            .await
            .expect("failed to append initial events");

        // Start reading
        let mut partition_iter = db
            .read_partition(partition_id, 0)
            .await
            .expect("failed to create partition iterator");

        let mut collected_sequences = Vec::new();

        // Read first few events
        for _ in 0..5 {
            if let Some(committed_events) = partition_iter.next().await.expect("read err") {
                match committed_events {
                    CommittedEvents::Single(event) => {
                        collected_sequences.push(event.partition_sequence);
                    }
                    CommittedEvents::Transaction { events, .. } => {
                        for event in events.iter() {
                            collected_sequences.push(event.partition_sequence);
                        }
                    }
                }
            }
        }

        // Write more events while reading
        append_events_to_stream(&db, "stream-a", 5, partition_key, partition_id)
            .await
            .expect("failed to append additional events");

        // Continue reading
        while let Some(committed_events) = partition_iter.next().await.expect("read err") {
            match committed_events {
                CommittedEvents::Single(event) => {
                    collected_sequences.push(event.partition_sequence);
                }
                CommittedEvents::Transaction { events, .. } => {
                    for event in events.iter() {
                        collected_sequences.push(event.partition_sequence);
                    }
                }
            }
        }

        // Should read initial 10 events, additional 5 may or may not be visible
        assert!(
            collected_sequences.len() >= 10,
            "should read at least initial 10 events"
        );
        assert!(
            collected_sequences.len() <= 15,
            "should read at most 15 events"
        );

        // Events should be in order
        for (i, &seq) in collected_sequences.iter().enumerate() {
            assert_eq!(seq, i as u64);
        }
    }
}
