mod reader;
mod writer;

use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fs, mem};

use bincode::config::{Fixint, LittleEndian, NoLimit};
use tracing::trace;
use uuid::Uuid;

pub use self::reader::{
    BucketSegmentIter, BucketSegmentReader, CommitRecord, CommittedEvents, CommittedEventsIntoIter,
    EventRecord, ReadHint, Record,
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
    let hash = hasher.finalize();

    trace!(
        timestamp,
        %transaction_id,
        %event_id,
        %partition_key,
        partition_id,
        partition_sequence,
        stream_version,
        %stream_id,
        %event_name,
        ?metadata,
        ?payload,
        hash,
        "calculating event crc32c"
    );

    hash
}

#[must_use]
fn calculate_commit_crc32c(transaction_id: &Uuid, encoded_timestamp: u64, event_count: u32) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(transaction_id.as_bytes());
    hasher.update(&encoded_timestamp.to_le_bytes());
    hasher.update(&event_count.to_le_bytes());
    let hash = hasher.finalize();

    trace!(
        %transaction_id,
        encoded_timestamp,
        event_count,
        hash,
        "calculating commit crc32c"
    );

    hash
}

#[must_use]
fn calculate_confirmation_count_crc32c(transaction_id: &Uuid, confirmation_count: u8) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(transaction_id.as_bytes());
    hasher.update(&confirmation_count.to_le_bytes());
    let hash = hasher.finalize();

    trace!(
        %transaction_id,
        confirmation_count,
        hash,
        "calculating confirmation count crc32c"
    );

    hash
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

        BucketSegmentWriter::create(&path, 62, 256 * 1024).unwrap();

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
            let mut writer = BucketSegmentWriter::create(&path, 0, 256 * 1024).unwrap();
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
            let mut writer = BucketSegmentWriter::create(&path, 0, 256 * 1024).unwrap();

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

    // Recovery tests
    mod recovery_tests {
        use std::io::{Seek, SeekFrom, Write};

        use super::*;

        /// Test utility to create a file with valid records followed by
        /// corruption
        fn create_test_file_with_corruption(
            record_count: usize,
            corruption_type: CorruptionType,
        ) -> PathBuf {
            let path = temp_file_path();

            // First create a valid file with records
            {
                let mut writer = BucketSegmentWriter::create(&path, 0, 256 * 1024).unwrap();

                for i in 0..record_count {
                    let event_id = Uuid::new_v4();
                    let transaction_id = Uuid::new_v4();
                    let partition_key = Uuid::new_v4();
                    let stream_id = StreamId::new(format!("stream_{i}")).unwrap();
                    let payload_data = format!("payload_{i}");

                    let append = AppendEvent {
                        event_id: &event_id,
                        partition_key: &partition_key,
                        partition_id: i as u16 % 100,
                        partition_sequence: i as u64,
                        stream_version: 1,
                        stream_id: &stream_id,
                        event_name: "TestEvent",
                        metadata: &[],
                        payload: payload_data.as_bytes(),
                    };

                    writer
                        .append_event(&transaction_id, 1700000000, 1, append)
                        .unwrap();
                }

                writer.flush().unwrap();
            }

            // Now apply corruption
            apply_corruption(&path, corruption_type);

            // Small delay to ensure file handles are released
            std::thread::sleep(std::time::Duration::from_millis(10));

            path
        }

        #[derive(Clone)]
        enum CorruptionType {
            TrailingZeros(usize), // Add N zero bytes to end
            TruncatedRecord,      // Cut off last record mid-way
            CorruptedCrc,         // Change CRC bytes to make record invalid
            RandomBytes(usize),   // Add random garbage bytes
        }

        fn apply_corruption(path: &PathBuf, corruption_type: CorruptionType) {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();

            match corruption_type {
                CorruptionType::TrailingZeros(count) => {
                    if count > 0 {
                        // Only add zeros if count > 0
                        file.seek(SeekFrom::End(0)).unwrap();
                        let zeros = vec![0u8; count];
                        file.write_all(&zeros).unwrap();
                        file.sync_data().unwrap();
                    }
                }
                CorruptionType::TruncatedRecord => {
                    // Truncate file to cut off the last 20 bytes (partial record)
                    let len = file.metadata().unwrap().len();
                    file.set_len(len - 20).unwrap();
                    file.sync_data().unwrap();
                }
                CorruptionType::CorruptedCrc => {
                    // Find a record and corrupt its CRC bytes (at offset 8+16 = 24 from record
                    // start)
                    file.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64 + 24))
                        .unwrap();
                    file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap(); // Corrupt CRC32
                    file.sync_data().unwrap();
                }
                CorruptionType::RandomBytes(count) => {
                    file.seek(SeekFrom::End(0)).unwrap();
                    let random_bytes = vec![0xAB; count]; // Predictable "random" bytes
                    file.write_all(&random_bytes).unwrap();
                    file.sync_data().unwrap();
                }
            }

            // Explicitly drop the file handle to ensure it's closed
            drop(file);
        }

        /// Verify a file can be opened and read without errors after recovery
        fn verify_file_integrity_after_recovery(path: &PathBuf) -> usize {
            // Add debug info about the file
            if let Ok(metadata) = std::fs::metadata(path) {
                println!(
                    "File size: {} bytes, readonly: {}",
                    metadata.len(),
                    metadata.permissions().readonly()
                );
            } else {
                println!("Could not read file metadata");
                return 0;
            }

            // Try to read the first few bytes to see if file is readable
            if let Ok(mut file) = std::fs::File::open(path) {
                let mut header = vec![0u8; 64];
                match std::io::Read::read(&mut file, &mut header) {
                    Ok(bytes_read) => println!("Read {bytes_read} header bytes successfully"),
                    Err(err) => println!("Failed to read file header: {err}"),
                }
            } else {
                println!("Could not open file directly");
                return 0;
            }

            let mut reader = BucketSegmentReader::open(path, None)
                .unwrap_or_else(|err| panic!("Failed to open file {path:?}: {err}"));
            let mut iter = reader.iter();
            let mut count = 0;

            while let Ok(Some(_)) = iter.next_record() {
                count += 1;
            }

            count
        }

        #[test]
        fn test_recovery_clean_file_no_action() {
            // Create a clean file with 10 records
            let path = create_test_file_with_corruption(10, CorruptionType::TrailingZeros(0)); // No corruption
            let original_size = std::fs::metadata(&path).unwrap().len();

            // Recovery should do nothing
            let records_read = verify_file_integrity_after_recovery(&path);
            let final_size = std::fs::metadata(&path).unwrap().len();

            assert_eq!(records_read, 10, "Should read all 10 records");
            assert_eq!(final_size, original_size, "File size should be unchanged");
        }

        #[test]
        fn test_recovery_header_only_file() {
            let path = temp_file_path();
            BucketSegmentWriter::create(&path, 0, 256 * 1024).unwrap(); // Just header, no records

            let records_read = verify_file_integrity_after_recovery(&path);
            assert_eq!(
                records_read, 0,
                "Should read 0 records from header-only file"
            );
        }

        #[test]
        fn test_recovery_trailing_zeros_64kb() {
            // Simulate the exact corruption scenario: 10 records + 64KB zero padding
            let path =
                create_test_file_with_corruption(10, CorruptionType::TrailingZeros(64 * 1024));
            let original_size = std::fs::metadata(&path).unwrap().len();

            let records_read = verify_file_integrity_after_recovery(&path);
            let final_size = std::fs::metadata(&path).unwrap().len();

            assert_eq!(records_read, 10, "Should recover all 10 valid records");
            assert!(final_size < original_size, "File should be truncated");
            assert_eq!(
                final_size,
                original_size - 64 * 1024,
                "Should remove exactly 64KB"
            );
        }

        #[test]
        fn test_recovery_trailing_zeros_small() {
            // Test smaller zero padding (1KB)
            let path = create_test_file_with_corruption(5, CorruptionType::TrailingZeros(1024));
            let original_size = std::fs::metadata(&path).unwrap().len();

            let records_read = verify_file_integrity_after_recovery(&path);
            let final_size = std::fs::metadata(&path).unwrap().len();

            assert_eq!(records_read, 5, "Should recover all 5 valid records");
            assert!(final_size < original_size, "File should be truncated");
            assert_eq!(
                final_size,
                original_size - 1024,
                "Should remove exactly 1KB"
            );
        }

        #[test]
        fn test_recovery_trailing_zeros_large() {
            // Test large zero padding (128KB)
            let path =
                create_test_file_with_corruption(15, CorruptionType::TrailingZeros(128 * 1024));

            let records_read = verify_file_integrity_after_recovery(&path);
            assert_eq!(
                records_read, 15,
                "Should recover all 15 valid records despite large corruption"
            );
        }

        #[test]
        fn test_recovery_truncated_record() {
            // Test partial record corruption (truncated mid-record)
            let path = create_test_file_with_corruption(8, CorruptionType::TruncatedRecord);

            let records_read = verify_file_integrity_after_recovery(&path);
            // Should recover fewer than 8 since the last record was truncated
            assert!(
                (7..8).contains(&records_read),
                "Should recover valid records before truncation"
            );
        }

        #[test]
        fn test_recovery_corrupted_crc() {
            // Test CRC corruption in a record
            let path = create_test_file_with_corruption(6, CorruptionType::CorruptedCrc);

            let records_read = verify_file_integrity_after_recovery(&path);
            // Should recover records before the corrupted one
            assert!(
                records_read < 6,
                "Should recover only valid records before corruption"
            );
        }

        #[test]
        fn test_recovery_random_bytes_corruption() {
            // Test random garbage at end of file
            let path = create_test_file_with_corruption(7, CorruptionType::RandomBytes(2048));

            let records_read = verify_file_integrity_after_recovery(&path);
            assert_eq!(
                records_read, 7,
                "Should recover all valid records before garbage"
            );
        }

        #[test]
        fn test_recovery_chunk_boundary_corruption() {
            // Test corruption exactly at 4KB boundary
            let path = create_test_file_with_corruption(12, CorruptionType::TrailingZeros(4096));

            let records_read = verify_file_integrity_after_recovery(&path);
            assert_eq!(
                records_read, 12,
                "Should handle 4KB boundary corruption correctly"
            );
        }

        #[test]
        fn test_post_recovery_normal_operations() {
            // Ensure file works normally after recovery
            let path = create_test_file_with_corruption(5, CorruptionType::TrailingZeros(8192));

            // First recovery pass
            let _ = verify_file_integrity_after_recovery(&path);

            // Second open should not trigger recovery again
            let records_read = verify_file_integrity_after_recovery(&path);
            assert_eq!(
                records_read, 5,
                "Should read same records after multiple opens"
            );
        }

        #[test]
        fn test_recovery_entire_file_corrupted() {
            let path = temp_file_path();

            // Create file with only header, then corrupt even the header area
            {
                let mut writer = BucketSegmentWriter::create(&path, 0, 256 * 1024).unwrap();
                writer.flush().unwrap();
            }

            // Corrupt by overwriting with zeros
            {
                let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
                file.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))
                    .unwrap();
                let zeros = vec![0u8; 1024];
                file.write_all(&zeros).unwrap();
                file.sync_data().unwrap();
            }

            let records_read = verify_file_integrity_after_recovery(&path);
            assert_eq!(records_read, 0, "Should handle completely corrupted file");
        }
    }
}
