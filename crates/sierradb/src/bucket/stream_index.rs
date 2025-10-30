//! The file format for an MPHF-based stream index is defined as follows:
//! - `[0..4]`     : magic marker: `b"SIDX"`
//! - `[4..12]`    : number of keys (n) as a `u64`
//! - `[12..20]`   : length of serialized MPHF (L) as a `u64`
//! - `[20..20+L]` : serialized MPHF bytes (using bincode)
//! - `[20+L..]`   : records array, exactly n records of RECORD_SIZE bytes each.

mod closed;
mod iter;
mod open;

use std::mem;

use uuid::Uuid;

pub use self::closed::{ClosedIndex, ClosedStreamIndex};
pub use self::iter::StreamIter;
pub use self::open::OpenStreamIndex;
use crate::STREAM_ID_SIZE;

const VERSION_SIZE: usize = mem::size_of::<u64>();
const PARTITION_KEY_SIZE: usize = mem::size_of::<Uuid>();
const OFFSET_SIZE: usize = mem::size_of::<u64>();
const LEN_SIZE: usize = mem::size_of::<u32>();
// Stream ID, version min, version max, partition key, offset, len
const RECORD_SIZE: usize =
    STREAM_ID_SIZE + VERSION_SIZE + VERSION_SIZE + PARTITION_KEY_SIZE + OFFSET_SIZE + LEN_SIZE;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamIndexRecord<T> {
    pub partition_key: Uuid,
    pub version_min: u64,
    pub version_max: u64,
    pub offsets: T,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamOffsets {
    Offsets(Vec<u64>), // Its cached
    ExternalBucket,    // This stream lives in a different bucket
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::StreamId;
    use crate::bucket::BucketSegmentId;
    use crate::bucket::stream_index::closed::ClosedStreamIndex;
    use crate::bucket::stream_index::open::OpenStreamIndex;

    const SEGMENT_SIZE: usize = 256_000_000; // 64 MB

    fn temp_file_path() -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};

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

    #[test]
    fn test_open_stream_index_insert_and_get() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), path, SEGMENT_SIZE).unwrap();

        let stream_id = StreamId::new("stream-a").unwrap();
        let partition_key = Uuid::new_v4();
        let offsets = vec![42, 105];
        for (i, offset) in offsets.iter().enumerate() {
            index
                .insert(stream_id.clone(), partition_key, i as u64, *offset)
                .unwrap();
        }

        assert_eq!(
            index.get(&stream_id),
            Some(&StreamIndexRecord {
                version_min: 0,
                version_max: 1,
                partition_key,
                offsets: StreamOffsets::Offsets(offsets),
            })
        );
        assert_eq!(index.get("unknown"), None);
    }

    #[test]
    fn test_closed_stream_index_lookup() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        let stream_id1 = StreamId::new("stream-a").unwrap();
        let stream_id2 = StreamId::new("stream-b").unwrap();
        let partition_key1 = Uuid::new_v4();
        let partition_key2 = Uuid::new_v4();
        let offsets1 = vec![1111, 2222];
        let offsets2 = vec![3333];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(stream_id1.clone(), partition_key1, i as u64, *offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(stream_id2.clone(), partition_key2, i as u64, *offset)
                .unwrap();
        }

        // Flush with the new MPHF format
        let (mphf, _) = index.flush().unwrap();
        assert!(mphf.try_hash(&stream_id1).is_some());
        assert!(mphf.try_hash(&stream_id2).is_some());
        assert!(mphf.try_hash(&StreamId::new("unknown").unwrap()).is_none());

        // Open with the closed index
        let mut closed_index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        // Test get_key with MPHF implementation
        let key1 = closed_index.get_key(&stream_id1).unwrap().unwrap();
        assert_eq!(key1.version_min, 0);
        assert_eq!(key1.version_max, 1);
        assert_eq!(key1.partition_key, partition_key1);

        // Test get with MPHF implementation
        assert_eq!(
            closed_index.get(&stream_id1).unwrap(),
            Some(StreamOffsets::Offsets(offsets1)),
        );

        // Test get_key for second stream ID
        let key2 = closed_index.get_key(&stream_id2).unwrap().unwrap();
        assert_eq!(key2.version_min, 0);
        assert_eq!(key2.version_max, 0);
        assert_eq!(key2.partition_key, partition_key2);

        // Test get for second stream ID
        assert_eq!(
            closed_index.get(&stream_id2).unwrap(),
            Some(StreamOffsets::Offsets(offsets2)),
        );

        // Test unknown stream ID
        assert_eq!(closed_index.get_key("unknown").unwrap(), None);
        assert_eq!(closed_index.get("unknown").unwrap(), None);
    }

    #[test]
    fn test_mphf_collision_handling() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        // These two IDs might have collisions in a regular hash table,
        // but MPHF should handle them perfectly
        let stream_id1 = StreamId::new("stream-a").unwrap();
        let stream_id2 = StreamId::new("stream-b").unwrap();
        let stream_id3 = StreamId::new("stream-k").unwrap();
        let stream_id4 = StreamId::new("stream-l").unwrap();

        let partition_key1 = Uuid::new_v4();
        let partition_key2 = Uuid::new_v4();
        let partition_key3 = Uuid::new_v4();
        let partition_key4 = Uuid::new_v4();

        let offsets1 = vec![883, 44];
        let offsets2 = vec![39, 1, 429];
        let offsets3 = vec![1111, 2222];
        let offsets4 = vec![3333];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(stream_id1.clone(), partition_key1, i as u64, *offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(stream_id2.clone(), partition_key2, i as u64, *offset)
                .unwrap();
        }
        for (i, offset) in offsets3.iter().enumerate() {
            index
                .insert(stream_id3.clone(), partition_key3, i as u64, *offset)
                .unwrap();
        }
        for (i, offset) in offsets4.iter().enumerate() {
            index
                .insert(stream_id4.clone(), partition_key4, i as u64, *offset)
                .unwrap();
        }

        // Flush with the new MPHF format
        let (mphf, _) = index.flush().unwrap();

        // Verify MPHF correctness - all streams should have different hash values
        let hash1 = mphf.hash(&stream_id1);
        let hash2 = mphf.hash(&stream_id2);
        let hash3 = mphf.hash(&stream_id3);
        let hash4 = mphf.hash(&stream_id4);

        assert_ne!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_ne!(hash1, hash4);
        assert_ne!(hash2, hash3);
        assert_ne!(hash2, hash4);
        assert_ne!(hash3, hash4);

        // Test with closed index
        let mut closed_index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        // For this test, we'll just check that we can retrieve all the stream IDs
        // without checking the exact offsets, since the file format has been updated
        assert_eq!(
            closed_index.get(&stream_id1).unwrap(),
            Some(StreamOffsets::Offsets(offsets1))
        );
        assert_eq!(
            closed_index.get(&stream_id2).unwrap(),
            Some(StreamOffsets::Offsets(offsets2))
        );
        assert_eq!(
            closed_index.get(&stream_id3).unwrap(),
            Some(StreamOffsets::Offsets(offsets3))
        );
        assert_eq!(
            closed_index.get(&stream_id4).unwrap(),
            Some(StreamOffsets::Offsets(offsets4))
        );

        // Unknown stream ID should not be found
        assert_eq!(closed_index.get("unknown").unwrap(), None);
    }

    #[test]
    fn test_non_existent_stream_lookup() {
        let path = temp_file_path();

        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        index.flush().unwrap();

        let mut index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), path, SEGMENT_SIZE).unwrap();
        assert_eq!(index.get("unknown").unwrap(), None);
    }

    #[test]
    fn test_insert_external_bucket() {
        let path = temp_file_path();

        let stream_id = StreamId::new("my-stream").unwrap();
        let partition_key = Uuid::new_v4();

        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        index
            .insert_external_bucket(stream_id.clone(), partition_key)
            .unwrap();
        assert_eq!(
            index.get(&stream_id),
            Some(&StreamIndexRecord {
                partition_key,
                version_min: 0,
                version_max: 0,
                offsets: StreamOffsets::ExternalBucket
            })
        );
        index.flush().unwrap();

        let mut index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        assert_eq!(
            index.get(&stream_id).unwrap(),
            Some(StreamOffsets::ExternalBucket)
        );
    }
}
