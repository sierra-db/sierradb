mod format;
mod iter;
mod reader;
mod writer;

use std::os::unix::fs::FileExt;
use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};
use std::{fs, mem};

use bincode::config::{Fixint, LittleEndian, NoLimit};
use bincode::{Decode, Encode};
use seglog::RECORD_HEAD_SIZE;
use uuid::Uuid;

pub use self::format::{
    InvalidTimestamp, LongBytes, RawCommit, RawEvent, RecordHeader, RecordKind,
    RecordKindTimestamp, ShortString,
};
pub use self::iter::SegmentIter;
pub use self::reader::{
    BucketSegmentIter, BucketSegmentReader, CommitRecord, CommittedEvents, CommittedEventsIntoIter,
    EventRecord, Record, SegmentBlock, SegmentBlockIter,
};
pub use self::writer::BucketSegmentWriter;
use crate::error::{InvalidHeaderError, ReadError};

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
pub const CONFIRMATION_HEADER_SIZE: usize = mem::size_of::<u8>(); // Confirmation count encoded
const RECORD_HEADER_SIZE: usize = RECORD_HEAD_SIZE // seglog head size (len + crc32c)
    + CONFIRMATION_HEADER_SIZE
    + mem::size_of::<u64>() // Timestamp nanoseconds
    + mem::size_of::<Uuid>(); // Transaction ID

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Encode, Decode)]
pub struct BucketSegmentHeader {
    pub magic_bytes: u32,
    pub version: u16,
    pub bucket_id: BucketId,
    pub created_at: u64,
}

impl BucketSegmentHeader {
    const VERSION: u16 = 0;

    pub fn new(bucket_id: BucketId) -> Result<Self, SystemTimeError> {
        Ok(BucketSegmentHeader {
            magic_bytes: MAGIC_BYTES,
            version: Self::VERSION,
            bucket_id,
            created_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64,
        })
    }

    pub fn load_from_file(file: &fs::File) -> Result<Self, ReadError> {
        let mut header_buf = [0; SEGMENT_HEADER_SIZE];
        file.read_exact_at(&mut header_buf, 0)?;
        let (header, _) =
            bincode::decode_from_slice::<BucketSegmentHeader, _>(&header_buf, BINCODE_CONFIG)?;
        Ok(header)
    }

    pub fn validate(&self) -> Result<(), InvalidHeaderError> {
        if self.magic_bytes != MAGIC_BYTES {
            return Err(InvalidHeaderError::InvalidMagicBytes {
                expected: MAGIC_BYTES,
                actual: self.magic_bytes,
            });
        }

        #[allow(clippy::absurd_extreme_comparisons)]
        if self.version < Self::VERSION {
            return Err(InvalidHeaderError::IncompatibleVersion {
                expected: Self::VERSION,
                actual: self.version,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rand::rng;
    use rand::seq::SliceRandom;
    use seglog::read::ReadHint;
    use uuid::Uuid;

    use super::*;
    use crate::StreamId;
    use crate::bucket::segment::format::{
        LongBytes, RawCommit, RawEvent, RecordHeader, ShortString,
    };

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

        BucketSegmentWriter::create(&path, 62, 256 * 1024).unwrap();

        let reader = BucketSegmentReader::open(&path, None).unwrap();
        let read_header = reader.read_header().unwrap();

        assert_eq!(read_header.version, 0);
        assert_eq!(read_header.bucket_id, 62);
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
            let mut writer = BucketSegmentWriter::create(&path, 0, 256 * 1024 * 5).unwrap();

            for i in 0..10_000 {
                if i % 5 == 0 {
                    let (offset, _) = writer
                        .append_commit(
                            1,
                            &RawCommit {
                                header: RecordHeader::new_commit(timestamp, transaction_id)
                                    .unwrap(),
                                event_count: 1,
                            },
                        )
                        .unwrap();
                    offsets.push((1u8, offset));
                } else {
                    let append = RawEvent {
                        header: RecordHeader::new_event(timestamp, transaction_id).unwrap(),
                        event_id: event_id.into_bytes(),
                        partition_key: partition_key.into_bytes(),
                        partition_id,
                        partition_sequence,
                        stream_version,
                        stream_id: stream_id.clone(),
                        event_name: ShortString(event_name.to_string()),
                        metadata: LongBytes(metadata.to_vec()),
                        payload: LongBytes(payload.to_vec()),
                    };
                    let (offset, _) = writer.append_event(1, &append).unwrap();
                    offsets.push((0u8, offset));
                }
            }

            writer.sync().unwrap();
            flushed_offset = writer.flushed_offset();
        }

        offsets.shuffle(&mut rng());

        let mut reader = BucketSegmentReader::open(&path, Some(flushed_offset.clone())).unwrap();

        for (i, (kind, offset)) in offsets.into_iter().enumerate() {
            let record = reader
                .read_record(
                    offset,
                    if i.is_multiple_of(2) {
                        ReadHint::Sequential
                    } else {
                        ReadHint::Random
                    },
                )
                .unwrap()
                .unwrap();
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
                    size,
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
                    assert_eq!(
                        size,
                        EVENT_HEADER_SIZE as u64
                            + stream_id.len() as u64
                            + event_name.len() as u64
                            + metadata.len() as u64
                            + payload.len() as u64
                    );
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
