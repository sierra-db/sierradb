mod reader;
mod writer;

use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, mem};

use bincode::config::{Fixint, LittleEndian, NoLimit};
use uuid::Uuid;

pub use self::reader::{
    BucketSegmentIter, BucketSegmentReader, CommitRecord, CommittedEvents, CommittedEventsIntoIter,
    EventRecord, Record,
};
pub use self::writer::{AppendEvent, BucketSegmentWriter};
use crate::error::WriteError;

const BINCODE_CONFIG: bincode::config::Configuration<LittleEndian, Fixint, NoLimit> =
    bincode::config::legacy();

use super::{BucketId, PartitionId};

// Segment header constants
const MAGIC_BYTES: u32 = 0x53545645; // EVTS
const MAGIC_BYTES_SIZE: usize = mem::size_of::<u32>();
const VERSION_SIZE: usize = mem::size_of::<u16>();
const BUCKET_ID_SIZE: usize = mem::size_of::<BucketId>();
const CREATED_AT_SIZE: usize = mem::size_of::<u64>();
const PADDING_SIZE: usize = 32; // For adding headers in future versions
pub const SEGMENT_HEADER_SIZE: usize =
    MAGIC_BYTES_SIZE + VERSION_SIZE + BUCKET_ID_SIZE + CREATED_AT_SIZE + PADDING_SIZE;

// Record sizes
pub const RECORD_HEADER_SIZE: usize = mem::size_of::<u64>() // Timestamp nanoseconds
        + mem::size_of::<Uuid>() // Transaction ID
        + mem::size_of::<u32>() // CRC32C hash
        + mem::size_of::<u8>() // Confirmation count
        + mem::size_of::<u32>(); // Confirmation count CRC32C hash
pub const EVENT_HEADER_SIZE: usize = RECORD_HEADER_SIZE
    + mem::size_of::<Uuid>() // Event ID
    + mem::size_of::<Uuid>() // Partition key
    + mem::size_of::<PartitionId>() // Partition ID
    + mem::size_of::<u64>() // Partition Sequence
    + mem::size_of::<u64>() // Stream Version
    + mem::size_of::<u8>() // Stream ID length
    + mem::size_of::<u8>() // Event name length
    + mem::size_of::<u32>() // Metadata length
    + mem::size_of::<u32>(); // Payload length
pub const COMMIT_SIZE: usize = RECORD_HEADER_SIZE + mem::size_of::<u32>(); // Event count

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BucketSegmentHeader {
    version: u16,
    bucket_id: BucketId,
    created_at: u64,
}

#[derive(Clone, Debug)]
pub struct FlushedOffset(Arc<AtomicU64>);

impl FlushedOffset {
    pub fn new(offset: Arc<AtomicU64>) -> Self {
        FlushedOffset(offset)
    }

    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

#[must_use]
#[allow(clippy::too_many_arguments)]
fn calculate_event_crc32c(
    timestamp: u64,
    transaction_id: &Uuid,
    event_id: &Uuid,
    partition_key: &Uuid,
    partition_id: PartitionId,
    partition_sequence: u64,
    stream_version: u64,
    stream_id: &str,
    event_name: &str,
    metadata: &[u8],
    payload: &[u8],
) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&timestamp.to_le_bytes());
    hasher.update(transaction_id.as_bytes());
    hasher.update(event_id.as_bytes());
    hasher.update(partition_key.as_bytes());
    hasher.update(&partition_id.to_le_bytes());
    hasher.update(&partition_sequence.to_le_bytes());
    hasher.update(&stream_version.to_le_bytes());
    hasher.update(stream_id.as_bytes());
    hasher.update(event_name.as_bytes());
    hasher.update(metadata);
    hasher.update(payload);
    hasher.finalize()
}

#[must_use]
fn calculate_commit_crc32c(transaction_id: &Uuid, encoded_timestamp: u64, event_count: u32) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(transaction_id.as_bytes());
    hasher.update(&encoded_timestamp.to_le_bytes());
    hasher.update(&event_count.to_le_bytes());
    hasher.finalize()
}

#[must_use]
fn calculate_confirmation_count_crc32c(transaction_id: &Uuid, confirmation_count: u8) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(transaction_id.as_bytes());
    hasher.update(&confirmation_count.to_le_bytes());
    hasher.finalize()
}

fn set_confirmations(
    file: &fs::File,
    mut offset: u64,
    transaction_id: &Uuid,
    confirmation_count: u8,
) -> Result<(), WriteError> {
    offset += 8 + 16 + 4; // timestamp + transaction id + main crc32c

    let confirmation_count_crc32c =
        calculate_confirmation_count_crc32c(transaction_id, confirmation_count);

    let mut buf = [0; mem::size_of::<u8>() + mem::size_of::<u32>()];
    buf[0] = confirmation_count;
    buf[1..5].copy_from_slice(&confirmation_count_crc32c.to_le_bytes());

    file.write_all_at(&buf, offset)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rand::rng;
    use rand::seq::SliceRandom;
    use uuid::Uuid;
    use writer::AppendEvent;

    use super::*;
    use crate::StreamId;

    fn temp_file_path() -> PathBuf {
        tempfile::Builder::new()
            .make(|path| Ok(path.to_path_buf()))
            .unwrap()
            .path()
            .to_path_buf()
    }

    #[test]
    fn test_write_and_read_segment_header() {
        let path = temp_file_path();

        BucketSegmentWriter::create(&path, 62).unwrap();

        let mut reader = BucketSegmentReader::open(&path, None).unwrap();
        let read_header = reader.read_segment_header().unwrap();

        assert_eq!(read_header.version, 0);
        assert_eq!(read_header.bucket_id, 62);
    }

    #[test]
    fn test_validate_magic_bytes() {
        let path = temp_file_path();

        let header = BucketSegmentHeader {
            version: 1,
            bucket_id: 42,
            created_at: 1700000000,
        };

        {
            let mut writer = BucketSegmentWriter::create(&path, 0).unwrap();
            writer.write_segment_header(&header).unwrap();
            writer.flush().unwrap();
        }

        let mut reader = BucketSegmentReader::open(&path, None).unwrap();
        assert!(reader.validate_magic_bytes().unwrap());
    }

    #[test]
    fn test_write_and_read_record() {
        let path = temp_file_path();

        let event_id = Uuid::new_v4();
        let transaction_id = Uuid::new_v4();
        let partition_key = Uuid::new_v4();
        let partition_id = 42;
        let timestamp = 1700000000;
        let partition_sequence = 69_420;
        let stream_version = 1;
        let stream_id = StreamId::new("test_stream").unwrap();
        let event_name = "TestEvent";
        let metadata = b"{}";
        let payload = b"payload_data";

        let mut offsets = Vec::with_capacity(10_000);
        let flushed_offset;

        {
            let mut writer = BucketSegmentWriter::create(&path, 0).unwrap();

            for i in 0..10_000 {
                if i % 5 == 0 {
                    let (offset, _) = writer
                        .append_commit(&transaction_id, timestamp, 1, 1)
                        .unwrap();
                    offsets.push((1u8, offset));
                } else {
                    let append = AppendEvent {
                        event_id: &event_id,
                        partition_key: &partition_key,
                        partition_id,
                        partition_sequence,
                        stream_version,
                        stream_id: &stream_id,
                        event_name,
                        metadata,
                        payload,
                    };
                    let (offset, _) = writer
                        .append_event(&transaction_id, timestamp, 1, append)
                        .unwrap();
                    offsets.push((0u8, offset));
                }
            }

            writer.flush().unwrap();
            flushed_offset = writer.flushed_offset();
        }

        offsets.shuffle(&mut rng());

        let mut reader = BucketSegmentReader::open(&path, Some(flushed_offset.clone())).unwrap();

        for (i, (kind, offset)) in offsets.into_iter().enumerate() {
            let record = reader.read_record(offset, i % 2 == 0).unwrap().unwrap();
            match record {
                Record::Event(EventRecord {
                    offset: _,
                    event_id: rid,
                    partition_key: rpk,
                    partition_id: rpid,
                    transaction_id: rtid,
                    partition_sequence: rps,
                    stream_version: rsv,
                    timestamp: rts,
                    confirmation_count: _,
                    stream_id: rsid,
                    event_name: ren,
                    metadata: rm,
                    payload: rp,
                }) => {
                    assert_eq!(kind, 0);
                    assert_eq!(rid, event_id);
                    assert_eq!(rpk, partition_key);
                    assert_eq!(rpid, partition_id);
                    assert_eq!(rtid, transaction_id);
                    assert_eq!(rps, partition_sequence);
                    assert_eq!(rsv, stream_version);
                    assert_eq!(rts, timestamp);
                    assert_eq!(rsid, stream_id);
                    assert_eq!(ren, event_name);
                    assert_eq!(rm, metadata);
                    assert_eq!(rp, payload);
                }
                Record::Commit(CommitRecord {
                    offset: _,
                    transaction_id: rtid,
                    timestamp: rts,
                    event_count,
                    confirmation_count: _,
                }) => {
                    assert_eq!(kind, 1);
                    assert_eq!(rtid, transaction_id);
                    assert_eq!(rts & !0b1, timestamp);
                    assert_eq!(event_count, 1);
                }
            }
        }
    }
}
