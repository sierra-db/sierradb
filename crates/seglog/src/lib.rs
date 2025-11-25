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
//! ┌─────────────┬─────────────┬────────────────┐
//! │ Length (4B) │ CRC32C (4B) │ Data (N bytes) │
//! └─────────────┴─────────────┴────────────────┘
//! ```
//!
//! - **Length**: 32-bit little-endian data length
//! - **CRC32C**: 32-bit checksum over length + data
//! - **Data**: Variable-length record payload
//!
//! Total header size is [`RECORD_HEAD_SIZE`] (8 bytes).
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
//! let mut writer = Writer::create(&temp, segment_size)?;
//!
//! // Append records
//! let (offset, len) = writer.append(b"event data")?;
//! writer.append(b"more events")?;
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
//! # let mut writer = seglog::write::Writer::create(&temp, 1024)?;
//! # writer.append(b"event 1")?;
//! # writer.append(b"event 2")?;
//! # writer.sync()?;
//! # let flushed = writer.flushed_offset();
//! # drop(writer);
//! let mut reader = Reader::open(&temp, Some(flushed))?;
//!
//! // Iterate over all records
//! let mut iter = reader.iter();
//! while let Some((offset, data)) = iter.next_record()? {
//!     println!("Record at {}: {:?}", offset, data);
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
//! # let mut writer = seglog::write::Writer::create(&temp, 1024)?;
//! # let (offset, _) = writer.append(b"specific event")?;
//! # writer.sync()?;
//! # let flushed = writer.flushed_offset();
//! # drop(writer);
//! let mut reader = Reader::open(&temp, Some(flushed))?;
//!
//! // Read specific record by offset
//! let data = reader.read_record(offset, ReadHint::Random)?;
//! println!("Record: {:?}", data);
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
//! # let mut writer = seglog::write::Writer::create(&temp, 1024)?;
//! # writer.append(b"shared data")?;
//! # writer.sync()?;
//! # let flushed = writer.flushed_offset();
//! # let path = temp.clone();
//! # drop(writer);
//! let reader = Reader::open(&path, Some(flushed))?;
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
//! 1. **Create**: Use [`Writer::create`] to initialize a new segment
//! 2. **Write**: Append records with [`Writer::append`]
//! 3. **Sync**: Periodically call [`Writer::sync`] to flush data to disk
//! 4. **Read**: Open readers with [`Reader::open`] using the shared [`FlushedOffset`]
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
//! [`Writer::create`]: write::Writer::create
//! [`Writer::append`]: write::Writer::append
//! [`Writer::sync`]: write::Writer::sync
//! [`Writer::set_len`]: write::Writer::set_len
//! [`Writer::close`]: write::Writer::close
//! [`Reader::open`]: read::Reader::open
//! [`Reader::close`]: read::Reader::close

use std::{
    io, mem,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use thiserror::Error;

pub mod read;
pub mod write;

const LEN_SIZE: usize = mem::size_of::<u32>();
const CRC32C_SIZE: usize = mem::size_of::<u32>();

/// Size of the record header in bytes, consisting of length and CRC32C checksum.
pub const RECORD_HEAD_SIZE: usize = LEN_SIZE + CRC32C_SIZE;

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

/// Errors that can occur during segment reading operations.
#[derive(Debug, Error)]
pub enum ReadError {
    #[error("crc32c hash mismatch")]
    Crc32cMismatch,
    #[error("read offset and length is out of bounds")]
    OutOfBounds,
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Errors that can occur during segment writing operations.
#[derive(Debug, Error)]
pub enum WriteError {
    #[error("segment full: attempted to write {attempted} bytes, only {available} bytes remaining")]
    SegmentFull { attempted: u64, available: u64 },
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[cfg(target_os = "linux")]
    #[error(transparent)]
    Nix(#[from] nix::errno::Errno),
}

fn calculate_crc32c(len_bytes: &[u8], data: &[u8]) -> u32 {
    let mut crc_hasher = crc32fast::Hasher::new();
    crc_hasher.update(len_bytes);
    crc_hasher.update(data);
    crc_hasher.finalize()
}

#[cfg(test)]
mod tests {
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
        let writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
        assert_eq!(writer.write_offset(), 0);
        assert_eq!(writer.remaining_bytes(), SEGMENT_SIZE as u64);
    }

    #[test]
    fn test_writer_append_single_record() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let data = b"hello world";
        let (offset, len) = writer.append(data).expect("failed to append");

        assert_eq!(offset, 0);
        assert_eq!(len, RECORD_HEAD_SIZE + data.len());
        assert_eq!(writer.write_offset(), len as u64);
    }

    #[test]
    fn test_writer_append_multiple_records() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let records = vec![b"first".as_slice(), b"second", b"third"];
        let mut expected_offset = 0u64;

        for data in &records {
            let (offset, len) = writer.append(data).expect("failed to append");
            assert_eq!(offset, expected_offset);
            expected_offset += len as u64;
        }

        assert_eq!(writer.write_offset(), expected_offset);
    }

    #[test]
    fn test_writer_remaining_bytes() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let initial_remaining = writer.remaining_bytes();
        assert_eq!(initial_remaining, SEGMENT_SIZE as u64);

        let data = b"test data";
        writer.append(data).expect("failed to append");

        let after_remaining = writer.remaining_bytes();
        assert_eq!(
            after_remaining,
            SEGMENT_SIZE as u64 - (RECORD_HEAD_SIZE + data.len()) as u64
        );
    }

    #[test]
    fn test_writer_write_offset() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        assert_eq!(writer.write_offset(), 0);

        let data = b"data";
        let (_, len) = writer.append(data).expect("failed to append");
        assert_eq!(writer.write_offset(), len as u64);
    }

    // Writer Error Cases Tests

    #[test]
    fn test_writer_segment_full() {
        let small_size = 100;
        let temp = temp_path();
        let mut writer = Writer::create(&temp, small_size).expect("failed to create writer");

        let large_data = vec![0u8; small_size];
        let result = writer.append(&large_data);

        assert!(matches!(result, Err(WriteError::SegmentFull { .. })));
    }

    #[test]
    fn test_writer_create_already_exists() {
        let temp = temp_path();
        Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let result = Writer::create(&temp, SEGMENT_SIZE);
        assert!(result.is_err());
    }

    // Writer Sync & Flush Tests

    #[test]
    fn test_writer_sync() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let data = b"sync test";
        writer.append(data).expect("failed to append");

        let flushed_before = writer.flushed_offset().load();
        let synced_offset = writer.sync().expect("failed to sync");

        assert_eq!(synced_offset, writer.write_offset());
        assert_eq!(writer.flushed_offset().load(), synced_offset);
        assert!(writer.flushed_offset().load() > flushed_before);
    }

    #[test]
    fn test_writer_flush_writer() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"data").expect("failed to append");
        writer.flush_writer().expect("failed to flush");
    }

    #[test]
    fn test_writer_close() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"data").expect("failed to append");
        let write_offset = writer.write_offset();
        let flushed = writer.flushed_offset();

        writer.close().expect("failed to close");

        // Verify data was synced by opening a reader
        let reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        assert_eq!(reader.flushed_offset().load(), write_offset);
    }

    // Writer Truncation Tests

    #[test]
    fn test_writer_set_len() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"first").expect("failed to append");
        let truncate_offset = writer.write_offset();
        writer.append(b"second").expect("failed to append");

        writer.set_len(truncate_offset).expect("failed to set_len");
        assert_eq!(writer.write_offset(), truncate_offset);
        assert_eq!(writer.flushed_offset().load(), truncate_offset);
    }

    #[test]
    fn test_writer_set_len_noop() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"data").expect("failed to append");
        let offset = writer.write_offset();

        writer.set_len(offset + 100).expect("failed to set_len");
        assert_eq!(writer.write_offset(), offset);
    }

    // Writer Open & Recovery Tests

    #[test]
    fn test_writer_open_existing() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"first").expect("failed to append");
        writer.append(b"second").expect("failed to append");
        let offset_before = writer.write_offset();
        writer.sync().expect("failed to sync");
        drop(writer);

        let mut writer = Writer::open(&temp, SEGMENT_SIZE).expect("failed to open writer");
        assert_eq!(writer.write_offset(), offset_before);

        writer.append(b"third").expect("failed to append");
        assert!(writer.write_offset() > offset_before);
    }

    #[test]
    fn test_writer_open_with_corruption() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"good data").expect("failed to append");
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
        let writer = Writer::open(&temp, SEGMENT_SIZE).expect("failed to open writer");
        assert_eq!(writer.write_offset(), good_offset);
    }

    // Reader Basic Operations Tests

    #[test]
    fn test_reader_open() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
        writer.append(b"data").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        assert!(reader.flushed_offset().load() > 0);
    }

    #[test]
    fn test_reader_read_record_random() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let data = b"test data";
        writer.append(data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record, data);
    }

    #[test]
    fn test_reader_read_record_sequential() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let data = b"sequential data";
        writer.append(data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read");
        assert_eq!(&*record, data);
    }

    #[test]
    fn test_reader_read_bytes() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let data = b"raw bytes";
        writer.append(data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let bytes = reader
            .read_bytes(0, RECORD_HEAD_SIZE + data.len())
            .expect("failed to read bytes");
        assert_eq!(bytes.len(), RECORD_HEAD_SIZE + data.len());
    }

    #[test]
    fn test_reader_try_clone() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
        writer.append(b"data").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let cloned = reader.try_clone().expect("failed to clone");

        assert_eq!(
            reader.flushed_offset().load(),
            cloned.flushed_offset().load()
        );
    }

    #[test]
    fn test_reader_close() {
        let temp = temp_path();
        let writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
        writer.close().expect("failed to close writer");

        let reader = Reader::open(&temp, None).expect("failed to open reader");
        reader.close();
    }

    // Reader Error Cases Tests

    #[test]
    fn test_reader_crc_mismatch() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"data").expect("failed to append");
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

        let mut reader = Reader::open(&temp, None).expect("failed to open reader");
        let result = reader.read_record(0, ReadHint::Random);

        assert!(matches!(result, Err(ReadError::Crc32cMismatch)));
    }

    #[test]
    fn test_reader_out_of_bounds() {
        let temp = temp_path();
        let writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let result = reader.read_record(1000, ReadHint::Random);

        assert!(matches!(result, Err(ReadError::OutOfBounds)));
    }

    #[test]
    fn test_reader_truncation_marker() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"first").expect("failed to append");
        let truncate_offset = writer.write_offset();
        writer.append(b"second").expect("failed to append");
        writer.set_len(truncate_offset).expect("failed to truncate");

        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let result = reader.read_record(truncate_offset, ReadHint::Random);

        assert!(matches!(result, Err(ReadError::OutOfBounds)));
    }

    // Iterator Tests

    #[test]
    fn test_iter_all_records() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let records = vec![b"first".as_slice(), b"second", b"third"];
        for data in &records {
            writer.append(data).expect("failed to append");
        }
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter();

        for expected in &records {
            let (_, data) = iter
                .next_record()
                .expect("failed to read")
                .expect("no record");
            assert_eq!(&*data, *expected);
        }

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_from_offset() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"first").expect("failed to append");
        let second_offset = writer.write_offset();
        writer.append(b"second").expect("failed to append");
        writer.append(b"third").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter_from(second_offset);

        let (_, data) = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*data, b"second");

        let (_, data) = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*data, b"third");

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_empty_segment() {
        let temp = temp_path();
        let writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter();

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_single_record() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"only one").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter();

        let (_, data) = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*data, b"only one");

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    // Concurrent Read/Write Tests

    #[test]
    fn test_flushed_offset_sync() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let flushed = writer.flushed_offset();
        assert_eq!(flushed.load(), 0);

        writer.append(b"data").expect("failed to append");
        assert_eq!(flushed.load(), 0); // Not yet flushed

        writer.sync().expect("failed to sync");
        assert!(flushed.load() > 0); // Now flushed
    }

    #[test]
    fn test_concurrent_read_write() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let flushed = writer.flushed_offset();
        let mut reader = Reader::open(&temp, Some(flushed.clone())).expect("failed to open reader");

        // Reader can't read unflushed data
        assert!(reader.read_record(0, ReadHint::Random).is_err());

        writer.append(b"data").expect("failed to append");
        writer.sync().expect("failed to sync");

        // Now reader can read
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record, b"data");
    }

    // Edge Cases Tests

    #[test]
    fn test_empty_segment() {
        let temp = temp_path();
        let writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        assert_eq!(reader.flushed_offset().load(), 0);

        let result = reader.read_record(0, ReadHint::Random);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_record() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        // Create a record larger than internal buffers
        let large_data = vec![0x42u8; 100_000];
        writer.append(&large_data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record, &large_data[..]);
    }

    #[test]
    fn test_max_segment_size() {
        let small_size = 1000;
        let temp = temp_path();
        let mut writer = Writer::create(&temp, small_size).expect("failed to create writer");

        // Fill segment almost completely
        let data = vec![0u8; 100];
        while writer.remaining_bytes() > (RECORD_HEAD_SIZE + data.len()) as u64 {
            writer.append(&data).expect("failed to append");
        }

        let remaining = writer.remaining_bytes();
        assert!(remaining < (RECORD_HEAD_SIZE + data.len()) as u64);

        // Should not be able to fit another record
        let result = writer.append(&data);
        assert!(matches!(result, Err(WriteError::SegmentFull { .. })));
    }

    #[test]
    fn test_record_boundaries() {
        let temp = temp_path();
        let mut writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        // Test records of varying sizes
        let sizes = vec![0, 1, 7, 8, 15, 16, 255, 256, 4095, 4096];

        for size in sizes {
            let data = vec![0xAAu8; size];
            let (offset, len) = writer.append(&data).expect("failed to append");
            assert_eq!(len, RECORD_HEAD_SIZE + size);

            // Verify we can read it back immediately
            writer.sync().expect("failed to sync");
            let flushed = writer.flushed_offset();

            let mut reader =
                Reader::open(&temp, Some(flushed.clone())).expect("failed to open reader");
            let record = reader
                .read_record(offset, ReadHint::Random)
                .expect("failed to read");
            assert_eq!(record.len(), size);
        }
    }
}
