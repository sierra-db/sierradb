//! A simple, high-performance segment log implementation.
//!
//! `seglog` provides low-level read and write operations for fixed-size segment files
//! with built-in CRC32C validation. It's designed for use in event sourcing systems,
//! write-ahead logs, and other append-only storage use cases.
//!
//! # Architecture
//!
//! ## Single Writer, Multiple Readers
//!
//! The segment log follows a **single writer, multiple concurrent readers** model:
//!
//! - **One [`Writer`]** can append records to a segment at a time
//! - **Multiple [`Reader`]s** can concurrently read from the same segment across threads
//! - The [`FlushedOffset`] provides thread-safe coordination between writers and readers
//!
//! This design ensures data consistency without locks on the read path, making it
//! highly efficient for workloads with many concurrent readers.
//!
//! ## Record Format
//!
//! Each record consists of:
//! ```text
//! ┌─────────────┬─────────────┬────────────────┬─────────────────────┐
//! │ Length (4B) │ CRC32C (4B) │ Header (H B)   │ Data (N bytes)      │
//! └─────────────┴─────────────┴────────────────┴─────────────────────┘
//! ```
//!
//! - **Length**: 32-bit little-endian total payload length (H + N bytes). MSB indicates compression.
//! - **CRC32C**: 32-bit checksum over length + header + data
//! - **Header**: Fixed-size metadata (H bytes, never compressed)
//! - **Data**: Variable-length record payload (optionally compressed)
//!
//! Total record header size is [`RECORD_HEAD_SIZE`] (8 bytes), not including the user header.
//!
//! ## Concurrent Safety via FlushedOffset
//!
//! The [`FlushedOffset`] is an atomic counter that tracks how much data has been
//! safely written to disk:
//!
//! - Writers update it after calling [`Writer::sync`]
//! - Readers check it to avoid reading uncommitted data
//! - It's shared via `Arc` for efficient cloning across threads
//!
//! This ensures readers never see partial writes or corrupted data.
//!
//! # Performance Optimizations
//!
//! ## Read Hints
//!
//! The [`ReadHint`] enum allows optimizing for different access patterns:
//!
//! - **[`ReadHint::Sequential`]**: Uses a 64KB read-ahead buffer for streaming access
//! - **[`ReadHint::Random`]**: Uses optimistic reads to reduce syscalls for small records
//!
//! ## Optimistic Reads
//!
//! For random access, the reader performs an optimistic read of the header plus 2KB
//! of data in a single syscall. If the record fits (most events in event sourcing do),
//! this eliminates one syscall per read, improving performance by ~40% for small records.
//!
//! # Examples
//!
//! ## Writing Records
//!
//! ```rust
//! use seglog::write::Writer;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::TempDir::new()?;
//! # let temp = dir.path().join("segment.log");
//! let segment_size = 1024 * 1024; // 1 MB
//! // Writer with no header (H = 0)
//! let mut writer = Writer::<0>::create(&temp, segment_size, 0)?;
//!
//! // Append records with empty header
//! let (offset, len) = writer.append(&[], b"event data")?;
//! writer.append(&[], b"more events")?;
//!
//! // Sync to make data visible to readers
//! writer.sync()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Reading Records (Sequential)
//!
//! ```rust
//! use seglog::read::Reader;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::TempDir::new()?;
//! # let temp = dir.path().join("segment.log");
//! # let mut writer = seglog::write::Writer::<0>::create(&temp, 1024, 0)?;
//! # writer.append(&[], b"event 1")?;
//! # writer.append(&[], b"event 2")?;
//! # writer.sync()?;
//! # let flushed = writer.flushed_offset();
//! # drop(writer);
//! let mut reader = Reader::<0>::open(&temp, Some(flushed))?;
//!
//! // Iterate over all records
//! let mut iter = reader.iter(0);
//! while let Some(record) = iter.next_record()? {
//!     println!("Record: {record:?}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Reading Records (Random Access)
//!
//! ```rust
//! use seglog::read::{Reader, ReadHint};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::TempDir::new()?;
//! # let temp = dir.path().join("segment.log");
//! # let mut writer = seglog::write::Writer::<0>::create(&temp, 1024, 0)?;
//! # let (offset, _) = writer.append(&[], b"specific event")?;
//! # writer.sync()?;
//! # let flushed = writer.flushed_offset();
//! # drop(writer);
//! let mut reader = Reader::<0>::open(&temp, Some(flushed))?;
//!
//! // Read specific record by offset
//! let record = reader.read_record(offset, ReadHint::Random)?;
//! println!("Record: {record:?}");
//! # Ok(())
//! # }
//! ```
//!
//! ## Concurrent Readers
//!
//! ```rust
//! use seglog::read::Reader;
//! use std::thread;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::TempDir::new()?;
//! # let temp = dir.path().join("segment.log");
//! # let mut writer = seglog::write::Writer::<0>::create(&temp, 1024, 0)?;
//! # writer.append(&[], b"shared data")?;
//! # writer.sync()?;
//! # let flushed = writer.flushed_offset();
//! # let path = temp.clone();
//! # drop(writer);
//! let reader = Reader::<0>::open(&path, Some(flushed))?;
//!
//! // Clone reader for use in another thread
//! let reader2 = reader.try_clone()?;
//!
//! let handle = thread::spawn(move || {
//!     // Read from the segment in a different thread
//!     // Both readers share the same FlushedOffset
//! });
//! # handle.join().unwrap();
//! # Ok(())
//! # }
//! ```
//!
//! # Segment Lifecycle
//!
//! 1. **Create**: Use [`Writer::<0>::create`] to initialize a new segment
//! 2. **Write**: Append records with [`Writer::append`]
//! 3. **Sync**: Periodically call [`Writer::sync`] to flush data to disk
//! 4. **Read**: Open readers with [`Reader::<0>::open`] using the shared [`FlushedOffset`]
//! 5. **Truncate** (optional): Use [`Writer::set_len`] to truncate the segment
//! 6. **Close**: Call [`Writer::close`] or [`Reader::close`] to ensure cleanup
//!
//! # Error Handling
//!
//! The crate defines two main error types:
//!
//! - [`ReadError`]: CRC mismatch, out of bounds reads, I/O errors
//! - [`WriteError`]: Segment full, I/O errors, read errors during recovery
//!
//! [`Writer`]: write::Writer
//! [`Reader`]: read::Reader
//! [`ReadHint`]: read::ReadHint
//! [`ReadHint::Sequential`]: read::ReadHint::Sequential
//! [`ReadHint::Random`]: read::ReadHint::Random
//! [`Writer::<0>::create`]: write::Writer::<0>::create
//! [`Writer::append`]: write::Writer::append
//! [`Writer::sync`]: write::Writer::sync
//! [`Writer::set_len`]: write::Writer::set_len
//! [`Writer::close`]: write::Writer::close
//! [`Reader::<0>::open`]: read::Reader::<0>::open
//! [`Reader::close`]: read::Reader::close
//! [`ReadError`]: read::ReadError
//! [`WriteError`]: write::WriteError

use std::{
    mem,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub mod parse;
pub mod read;
pub mod write;

const LEN_SIZE: usize = mem::size_of::<u32>();
const CRC32C_SIZE: usize = mem::size_of::<u32>();

/// Size of the record header in bytes, consisting of length and CRC32C checksum.
pub const RECORD_HEAD_SIZE: usize = LEN_SIZE + CRC32C_SIZE;

/// Compression flag bit in the length field (MSB).
pub const COMPRESSION_FLAG: u32 = 0x8000_0000;

/// Mask to extract actual length from length field (all bits except MSB).
pub const LENGTH_MASK: u32 = 0x7FFF_FFFF;

/// Minimum data size (in bytes) for compression to be applied.
/// Records smaller than this threshold will not be compressed to avoid overhead.
pub const MIN_COMPRESSION_SIZE: usize = 128;

/// Default zstd compression level (3 provides good balance of speed and ratio).
pub const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Thread-safe atomic offset tracking for flushed data.
///
/// Represents the byte offset in a segment file up to which all data has been safely
/// flushed to disk and can be read concurrently.
#[derive(Clone, Debug)]
pub struct FlushedOffset(Arc<AtomicU64>);

impl FlushedOffset {
    pub(crate) fn new(offset: u64) -> Self {
        FlushedOffset(Arc::new(AtomicU64::new(offset)))
    }

    pub(crate) fn set(&self, offset: u64) {
        self.0.store(offset, Ordering::Release)
    }

    /// Returns the current flushed offset value.
    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

pub fn calculate_crc32c(len_bytes: &[u8; 4], header: &[u8], data: &[u8]) -> u32 {
    let mut crc_hasher = crc32fast::Hasher::new();
    crc_hasher.update(len_bytes);
    crc_hasher.update(header);
    crc_hasher.update(data);
    crc_hasher.finalize()
}

#[cfg(test)]
mod tests {
    use crate::{read::ReadError, write::WriteError};

    use super::*;
    use read::{ReadHint, Reader};
    use std::io::{Seek, Write as _};
    use write::Writer;

    const SEGMENT_SIZE: usize = 1024 * 1024; // 1 MB

    fn temp_path() -> std::path::PathBuf {
        tempfile::Builder::new()
            .suffix(".seg")
            .tempfile()
            .expect("failed to create temp file")
            .into_temp_path()
            .to_path_buf()
    }

    // Writer Basic Operations Tests

    #[test]
    fn test_writer_create() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        assert_eq!(writer.write_offset(), 0);
        assert_eq!(writer.remaining_bytes(), SEGMENT_SIZE as u64);
    }

    #[test]
    fn test_writer_append_single_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"hello world";
        let (offset, len) = writer.append(&[], data).expect("failed to append");

        assert_eq!(offset, 0);
        assert_eq!(len, RECORD_HEAD_SIZE + data.len());
        assert_eq!(writer.write_offset(), len as u64);
    }

    #[test]
    fn test_writer_append_multiple_records() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let records = vec![b"first".as_slice(), b"second", b"third"];
        let mut expected_offset = 0u64;

        for data in &records {
            let (offset, len) = writer.append(&[], data).expect("failed to append");
            assert_eq!(offset, expected_offset);
            expected_offset += len as u64;
        }

        assert_eq!(writer.write_offset(), expected_offset);
    }

    #[test]
    fn test_writer_remaining_bytes() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let initial_remaining = writer.remaining_bytes();
        assert_eq!(initial_remaining, SEGMENT_SIZE as u64);

        let data = b"test data";
        writer.append(&[], data).expect("failed to append");

        let after_remaining = writer.remaining_bytes();
        assert_eq!(
            after_remaining,
            SEGMENT_SIZE as u64 - (RECORD_HEAD_SIZE + data.len()) as u64
        );
    }

    #[test]
    fn test_writer_write_offset() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        assert_eq!(writer.write_offset(), 0);

        let data = b"data";
        let (_, len) = writer.append(&[], data).expect("failed to append");
        assert_eq!(writer.write_offset(), len as u64);
    }

    // Writer Error Cases Tests

    #[test]
    fn test_writer_segment_full() {
        let small_size = 100;
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, small_size, 0).expect("failed to create writer");

        let large_data = vec![0u8; small_size];
        let result = writer.append(&[], &large_data);

        assert!(matches!(
            result,
            Err(WriteError::SegmentFull {
                attempted: 0,
                available: 100,
            })
        ));
    }

    #[test]
    fn test_writer_create_already_exists() {
        let temp = temp_path();
        Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let result = Writer::<0>::create(&temp, SEGMENT_SIZE, 0);
        assert!(result.is_err());
    }

    // Writer Sync & Flush Tests

    #[test]
    fn test_writer_sync() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"sync test";
        writer.append(&[], data).expect("failed to append");

        let flushed_before = writer.flushed_offset().load();
        let synced_offset = writer.sync().expect("failed to sync");

        assert_eq!(synced_offset, writer.write_offset());
        assert_eq!(writer.flushed_offset().load(), synced_offset);
        assert!(writer.flushed_offset().load() > flushed_before);
    }

    #[test]
    fn test_writer_flush_writer() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"data").expect("failed to append");
        writer.flush_writer().expect("failed to flush");
    }

    #[test]
    fn test_writer_close() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"data").expect("failed to append");
        let write_offset = writer.write_offset();
        let flushed = writer.flushed_offset();

        writer.close().expect("failed to close");

        // Verify data was synced by opening a reader
        let reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        assert_eq!(reader.flushed_offset().load(), write_offset);
    }

    // Writer Truncation Tests

    #[test]
    fn test_writer_set_len() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        let truncate_offset = writer.write_offset();
        writer.append(&[], b"second").expect("failed to append");

        writer.set_len(truncate_offset).expect("failed to set_len");
        assert_eq!(writer.write_offset(), truncate_offset);
        assert_eq!(writer.flushed_offset().load(), truncate_offset);
    }

    #[test]
    fn test_writer_set_len_noop() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"data").expect("failed to append");
        let offset = writer.write_offset();

        writer.set_len(offset + 100).expect("failed to set_len");
        assert_eq!(writer.write_offset(), offset);
    }

    // Writer Open & Recovery Tests

    #[test]
    fn test_writer_open_existing() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        writer.append(&[], b"second").expect("failed to append");
        let offset_before = writer.write_offset();
        writer.sync().expect("failed to sync");
        drop(writer);

        let mut writer = Writer::<0>::open(&temp, SEGMENT_SIZE, 0).expect("failed to open writer");
        assert_eq!(writer.write_offset(), offset_before);

        writer.append(&[], b"third").expect("failed to append");
        assert!(writer.write_offset() > offset_before);
    }

    #[test]
    fn test_writer_open_with_corruption() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"good data").expect("failed to append");
        let good_offset = writer.write_offset();
        writer.sync().expect("failed to sync");

        // Manually corrupt the file by writing garbage after valid data
        drop(writer);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&temp)
            .expect("failed to open file");
        file.seek(std::io::SeekFrom::Start(good_offset))
            .expect("failed to seek");
        file.write_all(&[0xFF; 100])
            .expect("failed to write garbage");
        drop(file);

        // Opening should detect corruption and stop at the last valid record
        let writer = Writer::<0>::open(&temp, SEGMENT_SIZE, 0).expect("failed to open writer");
        assert_eq!(writer.write_offset(), good_offset);
    }

    // Reader Basic Operations Tests

    #[test]
    fn test_reader_open() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.append(&[], b"data").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        assert!(reader.flushed_offset().load() > 0);
    }

    #[test]
    fn test_reader_read_record_random() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"test data";
        writer.append(&[], data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + data.len());
        assert_eq!(&*record.header, &[]);
        assert!(record.compressed_data.is_none());
    }

    #[test]
    fn test_reader_read_record_sequential() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"sequential data";
        writer.append(&[], data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read");
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + data.len());
        assert_eq!(&*record.header, &[]);
        assert!(record.compressed_data.is_none());
    }

    #[test]
    fn test_reader_read_bytes() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"raw bytes";
        writer.append(&[], data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut buf = vec![0; RECORD_HEAD_SIZE + data.len()];
        reader
            .read_bytes(0, &mut buf)
            .expect("failed to read bytes");
        assert_eq!(buf.len(), RECORD_HEAD_SIZE + data.len());
    }

    #[test]
    fn test_reader_try_clone() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.append(&[], b"data").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let cloned = reader.try_clone().expect("failed to clone");

        assert_eq!(
            reader.flushed_offset().load(),
            cloned.flushed_offset().load()
        );
    }

    #[test]
    fn test_reader_close() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.close().expect("failed to close writer");

        let reader = Reader::<0>::open(&temp, None).expect("failed to open reader");
        reader.close();
    }

    // Reader Error Cases Tests

    #[test]
    fn test_reader_crc_mismatch() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"data").expect("failed to append");
        let _offset = writer.write_offset();
        writer.sync().expect("failed to sync");
        drop(writer);

        // Corrupt the data by modifying a byte in the data portion
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&temp)
            .expect("failed to open file");
        file.seek(std::io::SeekFrom::Start(RECORD_HEAD_SIZE as u64))
            .expect("failed to seek");
        file.write_all(&[0xFF]).expect("failed to write");
        drop(file);

        let mut reader = Reader::<0>::open(&temp, None).expect("failed to open reader");
        let result = reader.read_record(0, ReadHint::Random);

        assert!(matches!(
            result,
            Err(ReadError::Crc32cMismatch { offset: 0 })
        ));
    }

    #[test]
    fn test_reader_out_of_bounds() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        assert_eq!(flushed.load(), 0);
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let result = reader.read_record(1000, ReadHint::Random);

        assert!(matches!(
            result,
            Err(ReadError::OutOfBounds {
                offset: 1000,
                length: 8,
                flushed_offset: 0,
            })
        ));
    }

    #[test]
    fn test_reader_truncation_marker() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        let truncate_offset = writer.write_offset();
        assert_eq!(truncate_offset, 13);
        writer.append(&[], b"second").expect("failed to append");
        writer.set_len(truncate_offset).expect("failed to truncate");

        let flushed = writer.flushed_offset();
        assert_eq!(flushed.load(), 13);
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let result = reader.read_record(truncate_offset, ReadHint::Random);

        assert!(matches!(
            result,
            Err(ReadError::OutOfBounds {
                offset: 13,
                length: 8,
                flushed_offset: 13,
            })
        ));
    }

    // Iterator Tests

    #[test]
    fn test_iter_all_records() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let records = vec![b"first".as_slice(), b"second", b"third"];
        for data in &records {
            writer.append(&[], data).expect("failed to append");
        }
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let mut expected_offset = 0u64;
        for expected in &records {
            let record = iter
                .next_record()
                .expect("failed to read")
                .expect("no record");
            assert_eq!(&*record.data, *expected);
            assert_eq!(record.offset, expected_offset);
            assert_eq!(record.len, RECORD_HEAD_SIZE + expected.len());
            assert_eq!(&*record.header, &[]);
            assert!(record.compressed_data.is_none());
            expected_offset += record.len as u64;
        }

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_from_offset() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        let second_offset = writer.write_offset();
        writer.append(&[], b"second").expect("failed to append");
        writer.append(&[], b"third").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(second_offset);

        let record = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*record.data, b"second");
        assert_eq!(record.offset, second_offset);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"second".len());
        assert_eq!(&*record.header, &[]);
        assert!(record.compressed_data.is_none());

        let third_offset = second_offset + record.len as u64;
        let record = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*record.data, b"third");
        assert_eq!(record.offset, third_offset);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"third".len());
        assert_eq!(&*record.header, &[]);
        assert!(record.compressed_data.is_none());

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_empty_segment() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_single_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"only one").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let record = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*record.data, b"only one");
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"only one".len());
        assert_eq!(&*record.header, &[]);
        assert!(record.compressed_data.is_none());

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    // Concurrent Read/Write Tests

    #[test]
    fn test_flushed_offset_sync() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let flushed = writer.flushed_offset();
        assert_eq!(flushed.load(), 0);

        writer.append(&[], b"data").expect("failed to append");
        assert_eq!(flushed.load(), 0); // Not yet flushed

        writer.sync().expect("failed to sync");
        assert!(flushed.load() > 0); // Now flushed
    }

    #[test]
    fn test_concurrent_read_write() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let flushed = writer.flushed_offset();
        let mut reader =
            Reader::<0>::open(&temp, Some(flushed.clone())).expect("failed to open reader");

        // Reader can't read unflushed data
        assert!(reader.read_record(0, ReadHint::Random).is_err());

        writer.append(&[], b"data").expect("failed to append");
        writer.sync().expect("failed to sync");

        // Now reader can read
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, b"data");
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"data".len());
        assert_eq!(&*record.header, &[]);
        assert!(record.compressed_data.is_none());
    }

    // Edge Cases Tests

    #[test]
    fn test_empty_segment() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        assert_eq!(reader.flushed_offset().load(), 0);

        let result = reader.read_record(0, ReadHint::Random);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        // Create a record larger than internal buffers
        let large_data = vec![0x42u8; 100_000];
        writer.append(&[], &large_data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, &large_data[..]);
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + large_data.len());
        assert_eq!(&*record.header, &[]);
        assert!(record.compressed_data.is_none());
    }

    #[test]
    fn test_max_segment_size() {
        let small_size = 1000;
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, small_size, 0).expect("failed to create writer");

        // Fill segment almost completely
        let data = vec![0u8; 100];
        while writer.remaining_bytes() > (RECORD_HEAD_SIZE + data.len()) as u64 {
            writer.append(&[], &data).expect("failed to append");
        }

        let remaining = writer.remaining_bytes();
        assert!(remaining < (RECORD_HEAD_SIZE + data.len()) as u64);

        // Should not be able to fit another record
        let result = writer.append(&[], &data);
        assert!(matches!(
            result,
            Err(WriteError::SegmentFull {
                attempted: 972,
                available: 28,
            })
        ));
    }

    #[test]
    fn test_record_boundaries() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        // Test records of varying sizes
        let sizes = vec![0, 1, 7, 8, 15, 16, 255, 256, 4095, 4096];

        for size in sizes {
            let data = vec![0xAAu8; size];
            let (offset, len) = writer.append(&[], &data).expect("failed to append");
            assert_eq!(len, RECORD_HEAD_SIZE + size);

            // Verify we can read it back immediately
            writer.sync().expect("failed to sync");
            let flushed = writer.flushed_offset();

            let mut reader =
                Reader::<0>::open(&temp, Some(flushed.clone())).expect("failed to open reader");
            let record = reader
                .read_record(offset, ReadHint::Random)
                .expect("failed to read");
            assert_eq!(record.data.len(), size);
            assert_eq!(record.offset, offset);
            assert_eq!(record.len, len);
            assert_eq!(&*record.header, &[]);
            assert!(record.compressed_data.is_none());
        }
    }

    // Start Offset Tests

    #[test]
    fn test_writer_create_with_start_offset() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        assert_eq!(writer.write_offset(), HEADER_SIZE);
        assert_eq!(writer.remaining_bytes(), SEGMENT_SIZE as u64 - HEADER_SIZE);

        // First record should start at HEADER_SIZE
        let (offset, len) = writer.append(&[], b"data").expect("failed to append");
        assert_eq!(offset, HEADER_SIZE);
        assert_eq!(writer.write_offset(), HEADER_SIZE + len as u64);
    }

    #[test]
    fn test_writer_with_header_data() {
        use std::os::unix::fs::FileExt;

        let temp = temp_path();
        const HEADER_SIZE: u64 = 32;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        // Write header data before start_offset
        let magic = b"MAGIC";
        let version = 1u16.to_le_bytes();
        writer
            .file()
            .write_all_at(magic, 0)
            .expect("failed to write magic");
        writer
            .file()
            .write_all_at(&version, magic.len() as u64)
            .expect("failed to write version");

        // Append records (should start at HEADER_SIZE)
        let (offset, _) = writer.append(&[], b"record1").expect("failed to append");
        assert_eq!(offset, HEADER_SIZE);
        writer.append(&[], b"record2").expect("failed to append");
        writer.sync().expect("failed to sync");
        drop(writer);

        // Verify header is intact
        let file = std::fs::File::open(&temp).expect("failed to open file");
        let mut magic_read = vec![0u8; magic.len()];
        file.read_exact_at(&mut magic_read, 0)
            .expect("failed to read magic");
        assert_eq!(&magic_read, magic);

        let mut version_read = [0u8; 2];
        file.read_exact_at(&mut version_read, magic.len() as u64)
            .expect("failed to read version");
        assert_eq!(version_read, version);
    }

    #[test]
    fn test_writer_open_with_start_offset() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        writer.append(&[], b"second").expect("failed to append");
        let offset_before = writer.write_offset();
        writer.sync().expect("failed to sync");
        drop(writer);

        // Open with same start_offset
        let mut writer =
            Writer::<0>::open(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to open writer");
        assert_eq!(writer.write_offset(), offset_before);

        // Should be able to continue appending
        writer.append(&[], b"third").expect("failed to append");
        assert!(writer.write_offset() > offset_before);
    }

    #[test]
    fn test_reader_with_start_offset() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        let (first_offset, _) = writer.append(&[], b"first").expect("failed to append");
        let (second_offset, _) = writer.append(&[], b"second").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        assert_eq!(first_offset, HEADER_SIZE);

        // Read records starting from HEADER_SIZE
        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(first_offset, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, b"first");
        assert_eq!(record.offset, first_offset);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"first".len());
        assert_eq!(&*record.header, &[]);
        assert!(record.compressed_data.is_none());

        let record = reader
            .read_record(second_offset, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, b"second");
        assert_eq!(record.offset, second_offset);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"second".len());
        assert_eq!(&*record.header, &[]);
        assert!(record.compressed_data.is_none());
    }

    #[test]
    fn test_iter_with_start_offset() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        let records = vec![b"first".as_slice(), b"second", b"third"];
        for data in &records {
            writer.append(&[], data).expect("failed to append");
        }
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        // Iterator should start from HEADER_SIZE
        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(HEADER_SIZE);

        let mut expected_offset = HEADER_SIZE;
        for expected in &records {
            let record = iter
                .next_record()
                .expect("failed to read")
                .expect("no record");
            assert_eq!(record.offset, expected_offset);
            assert!(record.offset >= HEADER_SIZE);
            assert_eq!(&*record.data, *expected);
            assert_eq!(record.len, RECORD_HEAD_SIZE + expected.len());
            assert_eq!(&*record.header, &[]);
            assert!(record.compressed_data.is_none());
            expected_offset += record.len as u64;
        }

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_start_offset_remaining_bytes() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 100;
        let segment_size = 1000;
        let mut writer =
            Writer::<0>::create(&temp, segment_size, HEADER_SIZE).expect("failed to create writer");

        // Remaining should not include header space
        assert_eq!(writer.remaining_bytes(), segment_size as u64 - HEADER_SIZE);

        let data = vec![0u8; 50];
        writer.append(&[], &data).expect("failed to append");

        assert_eq!(
            writer.remaining_bytes(),
            segment_size as u64 - HEADER_SIZE - (RECORD_HEAD_SIZE + data.len()) as u64
        );
    }

    #[test]
    fn test_open_with_corruption_after_header() {
        use std::io::Seek;
        use std::io::Write as _;

        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        writer.append(&[], b"good data").expect("failed to append");
        let good_offset = writer.write_offset();
        writer.sync().expect("failed to sync");

        // Manually corrupt the file after valid data
        drop(writer);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&temp)
            .expect("failed to open file");
        file.seek(std::io::SeekFrom::Start(good_offset))
            .expect("failed to seek");
        file.write_all(&[0xFF; 100])
            .expect("failed to write garbage");
        drop(file);

        // Opening should detect corruption and stop at the last valid record
        let writer =
            Writer::<0>::open(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to open writer");
        assert_eq!(writer.write_offset(), good_offset);
    }

    // Header Tests

    #[test]
    fn test_append_with_header_h8() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let data = b"hello world";
        let (offset, len) = writer.append(&header, data).expect("failed to append");
        assert_eq!(offset, 0);
        assert_eq!(len, RECORD_HEAD_SIZE + 8 + data.len());

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(offset, ReadHint::Random)
            .expect("failed to read");

        assert_eq!(&*record.header, &header);
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, offset);
        assert_eq!(record.len, len);
        assert!(record.compressed_data.is_none());
    }

    #[test]
    fn test_append_with_header_h16() {
        let temp = temp_path();
        let mut writer =
            Writer::<16>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [0xAAu8; 16];
        let data = b"test data";
        let (offset, len) = writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<16>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(offset, ReadHint::Random)
            .expect("failed to read");

        assert_eq!(&*record.header, &header);
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, offset);
        assert_eq!(record.len, len);
        assert!(record.compressed_data.is_none());
    }

    #[test]
    fn test_append_with_header_h32() {
        let temp = temp_path();
        let mut writer =
            Writer::<32>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [0x42u8; 32];
        let data = b"larger header test";
        let (offset, len) = writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<32>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(offset, ReadHint::Random)
            .expect("failed to read");

        assert_eq!(&*record.header, &header);
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, offset);
        assert_eq!(record.len, len);
        assert!(record.compressed_data.is_none());
    }

    #[test]
    fn test_append_multiple_records_with_headers() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let records = vec![
            ([1u8, 0, 0, 0, 0, 0, 0, 0], b"first".as_slice()),
            ([2u8, 0, 0, 0, 0, 0, 0, 0], b"second".as_slice()),
            ([3u8, 0, 0, 0, 0, 0, 0, 0], b"third".as_slice()),
        ];

        let mut offsets = Vec::new();
        for (header, data) in &records {
            let (offset, _) = writer.append(header, data).expect("failed to append");
            offsets.push(offset);
        }

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        for (i, (expected_header, expected_data)) in records.iter().enumerate() {
            let record = reader
                .read_record(offsets[i], ReadHint::Random)
                .expect("failed to read");

            assert_eq!(&*record.header, expected_header);
            assert_eq!(&*record.data, *expected_data);
            assert_eq!(record.offset, offsets[i]);
            assert!(record.compressed_data.is_none());
        }
    }

    #[test]
    fn test_header_preserved_in_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        // Use distinct header bytes to ensure they're preserved
        let header = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
        let data = b"check header preservation";
        writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Verify exact header bytes
        assert_eq!(&*record.header, &header);
        assert_eq!(record.header[0], 0x01);
        assert_eq!(record.header[7], 0xEF);
    }

    #[test]
    fn test_iter_with_headers() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let records = vec![
            ([10u8, 0, 0, 0, 0, 0, 0, 0], b"first".as_slice()),
            ([20u8, 0, 0, 0, 0, 0, 0, 0], b"second".as_slice()),
            ([30u8, 0, 0, 0, 0, 0, 0, 0], b"third".as_slice()),
        ];

        for (header, data) in &records {
            writer.append(header, data).expect("failed to append");
        }

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let mut expected_offset = 0u64;
        for (expected_header, expected_data) in &records {
            let record = iter
                .next_record()
                .expect("failed to read")
                .expect("no record");

            assert_eq!(&*record.header, expected_header);
            assert_eq!(&*record.data, *expected_data);
            assert_eq!(record.offset, expected_offset);
            assert_eq!(record.len, RECORD_HEAD_SIZE + 8 + expected_data.len());
            assert!(record.compressed_data.is_none());
            expected_offset += record.len as u64;
        }

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_random_and_sequential_read_with_headers() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [0xFFu8; 8];
        let data = b"test both read hints";
        writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Test Random hint
        let record_random = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read with random hint");
        assert_eq!(&*record_random.header, &header);
        assert_eq!(&*record_random.data, data);

        // Test Sequential hint
        let record_sequential = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read with sequential hint");
        assert_eq!(&*record_sequential.header, &header);
        assert_eq!(&*record_sequential.data, data);
    }

    #[test]
    fn test_writer_open_with_headers() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header1 = [1u8; 8];
        let header2 = [2u8; 8];
        writer.append(&header1, b"first").expect("failed to append");
        writer
            .append(&header2, b"second")
            .expect("failed to append");
        let expected_offset = writer.write_offset();
        writer.sync().expect("failed to sync");
        drop(writer);

        // Reopen and verify recovery
        let mut writer =
            Writer::<8>::open(&temp, SEGMENT_SIZE, 0).expect("failed to reopen writer");
        assert_eq!(writer.write_offset(), expected_offset);

        // Append another record
        let header3 = [3u8; 8];
        writer.append(&header3, b"third").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        // Verify all three records
        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let r1 = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*r1.header, &header1);

        let r2 = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*r2.header, &header2);

        let r3 = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*r3.header, &header3);
    }

    // Compression Tests

    #[test]
    fn test_compression_disabled_by_default() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        // Write data >= MIN_COMPRESSION_SIZE but compression is not enabled
        let data = vec![0x42u8; MIN_COMPRESSION_SIZE];
        writer.append(&[], &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Data should not be compressed (compressed_data should be None)
        assert!(record.compressed_data.is_none());
        assert_eq!(record.data.len(), MIN_COMPRESSION_SIZE);
    }

    #[test]
    fn test_enable_disable_compression() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = vec![0xAAu8; MIN_COMPRESSION_SIZE];

        // Write without compression
        let (offset1, _) = writer.append(&[], &data).expect("failed to append");

        // Enable compression
        writer.enable_compression();
        let (offset2, _) = writer.append(&[], &data).expect("failed to append");

        // Disable compression
        writer.disable_compression();
        let (offset3, _) = writer.append(&[], &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");

        // First record: not compressed
        let r1 = reader
            .read_record(offset1, ReadHint::Random)
            .expect("failed to read");
        assert!(r1.compressed_data.is_none());

        // Second record: compressed
        let r2 = reader
            .read_record(offset2, ReadHint::Random)
            .expect("failed to read");
        assert!(r2.compressed_data.is_some());
        assert_eq!(r2.data.len(), MIN_COMPRESSION_SIZE);

        // Third record: not compressed again
        let r3 = reader
            .read_record(offset3, ReadHint::Random)
            .expect("failed to read");
        assert!(r3.compressed_data.is_none());
    }

    #[test]
    fn test_compression_large_data() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        // Data >= MIN_COMPRESSION_SIZE should be compressed
        let data = vec![0x42u8; 200];
        writer.append(&[], &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Should be compressed
        assert!(record.compressed_data.is_some());
        // Data should be decompressed correctly
        assert_eq!(&*record.data, &data);
    }

    #[test]
    fn test_no_compression_small_data() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        // Data < MIN_COMPRESSION_SIZE should not be compressed
        let data = vec![0x42u8; MIN_COMPRESSION_SIZE - 1];
        writer.append(&[], &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Should NOT be compressed (data too small)
        assert!(record.compressed_data.is_none());
        assert_eq!(&*record.data, &data);
    }

    #[test]
    fn test_no_compression_when_disabled() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        // Compression is disabled by default

        // Large data but compression disabled
        let data = vec![0x42u8; 500];
        writer.append(&[], &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Should NOT be compressed (compression disabled)
        assert!(record.compressed_data.is_none());
        assert_eq!(&*record.data, &data);
    }

    #[test]
    fn test_read_compressed_record_random() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let original_data = vec![0x55u8; 300];
        writer
            .append(&[], &original_data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read with random hint");

        assert!(record.compressed_data.is_some());
        assert_eq!(&*record.data, &original_data);
        assert_eq!(record.offset, 0);
    }

    #[test]
    fn test_read_compressed_record_sequential() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let original_data = vec![0x77u8; 300];
        writer
            .append(&[], &original_data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read with sequential hint");

        assert!(record.compressed_data.is_some());
        assert_eq!(&*record.data, &original_data);
        assert_eq!(record.offset, 0);
    }

    #[test]
    fn test_compressed_record_has_compressed_data_field() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let original_data = vec![0xBBu8; 256];
        writer
            .append(&[], &original_data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // compressed_data field should be populated
        assert!(record.compressed_data.is_some());
        let compressed_data = record.compressed_data.unwrap();

        // Compressed data should be smaller than original for compressible data
        // (we're using repeated 0xBB which should compress well)
        assert!(compressed_data.len() < original_data.len());

        // Decompressed data should match original
        assert_eq!(&*record.data, &original_data);
    }

    #[test]
    fn test_iter_compressed_records() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let records = vec![vec![0x11u8; 200], vec![0x22u8; 250], vec![0x33u8; 300]];

        for data in &records {
            writer.append(&[], data).expect("failed to append");
        }

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let mut expected_offset = 0u64;
        for expected_data in &records {
            let record = iter
                .next_record()
                .expect("failed to read")
                .expect("no record");

            assert!(record.compressed_data.is_some());
            assert_eq!(&*record.data, expected_data);
            assert_eq!(record.offset, expected_offset);
            expected_offset += record.len as u64;
        }

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_mixed_compressed_uncompressed() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let small_data = vec![0xAAu8; 50]; // Too small to compress
        let large_data = vec![0xBBu8; 200]; // Large enough to compress

        // Write small data (no compression)
        let (offset1, _) = writer.append(&[], &small_data).expect("failed to append");

        // Enable compression and write large data
        writer.enable_compression();
        let (offset2, _) = writer.append(&[], &large_data).expect("failed to append");

        // Write small data with compression enabled (still won't compress due to size)
        let (offset3, _) = writer.append(&[], &small_data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");

        // First record: not compressed
        let r1 = reader
            .read_record(offset1, ReadHint::Random)
            .expect("failed to read");
        assert!(r1.compressed_data.is_none());
        assert_eq!(&*r1.data, &small_data);

        // Second record: compressed
        let r2 = reader
            .read_record(offset2, ReadHint::Random)
            .expect("failed to read");
        assert!(r2.compressed_data.is_some());
        assert_eq!(&*r2.data, &large_data);

        // Third record: not compressed (too small)
        let r3 = reader
            .read_record(offset3, ReadHint::Random)
            .expect("failed to read");
        assert!(r3.compressed_data.is_none());
        assert_eq!(&*r3.data, &small_data);
    }

    #[test]
    fn test_compression_with_headers_h8() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let header = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let data = vec![0xCCu8; 200];
        writer.append(&header, &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Should be compressed
        assert!(record.compressed_data.is_some());
        // Header should be preserved (never compressed)
        assert_eq!(&*record.header, &header);
        // Data should be decompressed correctly
        assert_eq!(&*record.data, &data);
    }

    #[test]
    fn test_compression_header_never_compressed() {
        let temp = temp_path();
        let mut writer =
            Writer::<16>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        // Header with distinct pattern
        let header = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10,
        ];
        let data = vec![0xDDu8; 256];
        writer.append(&header, &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<16>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Verify header is preserved exactly (never compressed)
        assert_eq!(&*record.header, &header);
        assert_eq!(record.header[0], 0x01);
        assert_eq!(record.header[15], 0x10);

        // Data should be compressed
        assert!(record.compressed_data.is_some());
        assert_eq!(&*record.data, &data);
    }

    #[test]
    fn test_compression_exactly_128_bytes() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        // Exactly at MIN_COMPRESSION_SIZE threshold
        let data = vec![0x99u8; MIN_COMPRESSION_SIZE];
        writer.append(&[], &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Should be compressed (>= threshold)
        assert!(record.compressed_data.is_some());
        assert_eq!(&*record.data, &data);
    }

    #[test]
    fn test_compression_127_bytes() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        // One byte below MIN_COMPRESSION_SIZE threshold
        let data = vec![0x88u8; MIN_COMPRESSION_SIZE - 1];
        writer.append(&[], &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Should NOT be compressed (< threshold)
        assert!(record.compressed_data.is_none());
        assert_eq!(&*record.data, &data);
    }

    #[test]
    fn test_compression_large_1mb_data() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        // Very large compressible data (1MB of repeated bytes)
        let data = vec![0xEEu8; 1024 * 1024];
        writer.append(&[], &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Should be compressed
        assert!(record.compressed_data.is_some());
        // Data should decompress correctly
        assert_eq!(record.data.len(), 1024 * 1024);
        assert_eq!(&*record.data, &data);

        // Verify compression actually worked (compressed size should be much smaller)
        let compressed_len = record.compressed_data.unwrap().len();
        assert!(compressed_len < data.len() / 10); // Should compress to < 10% of original
    }

    #[test]
    fn test_writer_open_compressed_segment() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let data1 = vec![0xF1u8; 200];
        let data2 = vec![0xF2u8; 250];
        writer.append(&[], &data1).expect("failed to append");
        writer.append(&[], &data2).expect("failed to append");
        let expected_offset = writer.write_offset();
        writer.sync().expect("failed to sync");
        drop(writer);

        // Reopen the segment with compressed records
        let mut writer =
            Writer::<0>::open(&temp, SEGMENT_SIZE, 0).expect("failed to reopen writer");
        assert_eq!(writer.write_offset(), expected_offset);

        // Append another compressed record
        writer.enable_compression();
        let data3 = vec![0xF3u8; 300];
        writer.append(&[], &data3).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        // Verify all three records can be read
        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let r1 = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert!(r1.compressed_data.is_some());
        assert_eq!(&*r1.data, &data1);

        let r2 = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert!(r2.compressed_data.is_some());
        assert_eq!(&*r2.data, &data2);

        let r3 = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert!(r3.compressed_data.is_some());
        assert_eq!(&*r3.data, &data3);
    }

    // replace_header Tests

    #[test]
    fn test_replace_header_h0_simple() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"test data for header replacement";
        writer.append(&[], data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace empty header with another empty header (should work)
        reader
            .replace_header(0, [])
            .expect("failed to replace header");

        // Verify record is still readable
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, data);
        assert_eq!(&*record.header, &[]);
    }

    #[test]
    fn test_replace_header_h8() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let data = b"test data for header replacement";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace with new header
        let new_header = [10u8, 20, 30, 40, 50, 60, 70, 80];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Verify new header is in place
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_h16() {
        let temp = temp_path();
        let mut writer =
            Writer::<16>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0xAAu8; 16];
        let data = b"data with 16-byte header";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<16>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace header
        let new_header = [0xBBu8; 16];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Verify replacement
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_updates_crc() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [1u8; 8];
        let data = b"test crc update";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace header
        let new_header = [2u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // CRC should be updated - record should be readable without CRC error
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read - CRC should be valid");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_preserves_data() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0x11u8; 8];
        let data = b"this data should remain unchanged";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Read original data
        let original_record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        let original_data = original_record.data.to_vec();

        // Replace header
        let new_header = [0x22u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Verify data is unchanged
        let new_record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*new_record.data, &original_data);
        assert_eq!(&*new_record.data, data);
    }

    #[test]
    fn test_replace_header_on_compressed_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let original_header = [0xAAu8; 8];
        let data = vec![0x42u8; 256]; // Large enough to compress
        writer
            .append(&original_header, &data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Verify it's compressed
        let original_record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert!(original_record.compressed_data.is_some());

        // Replace header
        let new_header = [0xBBu8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header on compressed record");

        // Verify replacement worked
        let new_record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*new_record.header, &new_header);
        assert_eq!(&*new_record.data, &data);
        assert!(new_record.compressed_data.is_some()); // Should still be compressed
    }

    #[test]
    fn test_replace_header_preserves_compression() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let original_header = [1u8; 8];
        let data = vec![0xCCu8; 300];
        writer
            .append(&original_header, &data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Get compressed data before replacement
        let before = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        let compressed_before = before.compressed_data.as_ref().unwrap().to_vec();

        // Replace header
        let new_header = [2u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Get compressed data after replacement
        let after = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        let compressed_after = after.compressed_data.as_ref().unwrap().to_vec();

        // Compressed data should be identical
        assert_eq!(compressed_before, compressed_after);
        // Decompressed data should match original
        assert_eq!(&*after.data, &data);
    }

    #[test]
    fn test_replaced_header_readable_random() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0xFFu8; 8];
        let data = b"readable after replacement";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        let new_header = [0x00u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Read with Random hint
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read with random hint");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replaced_header_readable_sequential() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0x11u8; 8];
        let data = b"sequential read after replacement";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        let new_header = [0x22u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Read with Sequential hint
        let record = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read with sequential hint");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_multiple_times() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [1u8; 8];
        let data = b"replace multiple times";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace multiple times
        for i in 2u8..10u8 {
            let new_header = [i; 8];
            reader
                .replace_header(0, new_header)
                .expect("failed to replace header");

            let record = reader
                .read_record(0, ReadHint::Random)
                .expect("failed to read");
            assert_eq!(&*record.header, &new_header);
            assert_eq!(&*record.data, data);
        }
    }

    #[test]
    fn test_replace_header_survives_reopen() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0xAAu8; 8];
        let data = b"persistence test";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        // Open reader and replace header
        let mut reader =
            Reader::<8>::open(&temp, Some(flushed.clone())).expect("failed to open reader");
        let new_header = [0xBBu8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");
        drop(reader);

        // Reopen reader and verify header change persists
        let mut reader2 = Reader::<8>::open(&temp, Some(flushed)).expect("failed to reopen reader");
        let record = reader2
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_invalid_offset() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [1u8; 8];
        let data = b"test data";
        writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Try to replace header at non-existent offset
        let new_header = [2u8; 8];
        let result = reader.replace_header(9999, new_header);
        assert!(result.is_err());
    }

    #[test]
    fn test_replace_header_out_of_bounds() {
        let temp = temp_path();
        let writer = Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Try to replace header beyond flushed offset
        let new_header = [1u8; 8];
        let result = reader.replace_header(0, new_header);
        assert!(result.is_err());
    }

    // Integration Tests

    #[test]
    fn test_headers_compression_replace_all_together() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let original_header = [0x11u8; 8];
        let data = vec![0x99u8; 256]; // Compressible data
        writer
            .append(&original_header, &data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Verify compression
        let record1 = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert!(record1.compressed_data.is_some());
        assert_eq!(&*record1.header, &original_header);
        assert_eq!(&*record1.data, &data);

        // Replace header on compressed record
        let new_header = [0x22u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Verify all three features work together
        let record2 = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record2.header, &new_header); // Header replaced
        assert_eq!(&*record2.data, &data); // Data unchanged
        assert!(record2.compressed_data.is_some()); // Still compressed
    }

    #[test]
    fn test_concurrent_read_with_headers_and_compression() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        let header = [0xABu8; 8];
        let data = vec![0xCDu8; 200];
        writer.append(&header, &data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();

        // Create multiple readers
        let mut reader1 =
            Reader::<8>::open(&temp, Some(flushed.clone())).expect("failed to open reader1");
        let mut reader2 =
            Reader::<8>::open(&temp, Some(flushed.clone())).expect("failed to open reader2");

        // Both readers should be able to read the compressed record
        let r1 = reader1
            .read_record(0, ReadHint::Random)
            .expect("reader1 failed");
        let r2 = reader2
            .read_record(0, ReadHint::Sequential)
            .expect("reader2 failed");

        assert_eq!(&*r1.header, &header);
        assert_eq!(&*r1.data, &data);
        assert!(r1.compressed_data.is_some());

        assert_eq!(&*r2.header, &header);
        assert_eq!(&*r2.data, &data);
        assert!(r2.compressed_data.is_some());
    }

    #[test]
    fn test_compression_ratio_verification() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.enable_compression();

        // Highly compressible data (1000 repeated bytes)
        let data = vec![0xABu8; 1000];
        writer.append(&[], &data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Verify compression worked
        assert!(record.compressed_data.is_some());
        let compressed_len = record.compressed_data.unwrap().len();

        // Compressed data should be significantly smaller (< 5% of original)
        assert!(compressed_len < data.len() / 20);

        // Decompressed data should match exactly
        assert_eq!(&*record.data, &data);
    }
}
