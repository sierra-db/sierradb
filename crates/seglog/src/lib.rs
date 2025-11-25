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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let data = b"hello world";
        let (offset, len) = writer.append(data).expect("failed to append");

        assert_eq!(offset, 0);
        assert_eq!(len, RECORD_HEAD_SIZE + data.len());
        assert_eq!(writer.write_offset(), len as u64);
    }

    #[test]
    fn test_writer_append_multiple_records() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"data").expect("failed to append");
        writer.flush_writer().expect("failed to flush");
    }

    #[test]
    fn test_writer_close() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"data").expect("failed to append");
        let offset = writer.write_offset();

        writer.set_len(offset + 100).expect("failed to set_len");
        assert_eq!(writer.write_offset(), offset);
    }

    // Writer Open & Recovery Tests

    #[test]
    fn test_writer_open_existing() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        file.write_all(&[0xFF; 100]).expect("failed to write garbage");
        drop(file);

        // Opening should detect corruption and stop at the last valid record
        let writer = Writer::open(&temp, SEGMENT_SIZE).expect("failed to open writer");
        assert_eq!(writer.write_offset(), good_offset);
    }

    // Reader Basic Operations Tests

    #[test]
    fn test_reader_open() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
        writer.append(b"data").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        assert!(reader.flushed_offset().load() > 0);
    }

    #[test]
    fn test_reader_read_record_random() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let data = b"test data";
        writer.append(data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record, data);
    }

    #[test]
    fn test_reader_read_record_sequential() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let data = b"sequential data";
        writer.append(data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read");
        assert_eq!(&*record, data);
    }

    #[test]
    fn test_reader_read_bytes() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let result = reader.read_record(1000, ReadHint::Random);

        assert!(matches!(result, Err(ReadError::OutOfBounds)));
    }

    #[test]
    fn test_reader_truncation_marker() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"first").expect("failed to append");
        let truncate_offset = writer.write_offset();
        writer.append(b"second").expect("failed to append");
        writer.set_len(truncate_offset).expect("failed to truncate");

        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let result = reader.read_record(truncate_offset, ReadHint::Random);

        assert!(matches!(result, Err(ReadError::OutOfBounds)));
    }

    // Iterator Tests

    #[test]
    fn test_iter_all_records() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let records = vec![b"first".as_slice(), b"second", b"third"];
        for data in &records {
            writer.append(data).expect("failed to append");
        }
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter();

        for expected in &records {
            let (_, data) = iter.next_record().expect("failed to read").expect("no record");
            assert_eq!(&*data, *expected);
        }

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_from_offset() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"first").expect("failed to append");
        let second_offset = writer.write_offset();
        writer.append(b"second").expect("failed to append");
        writer.append(b"third").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter_from(second_offset);

        let (_, data) = iter.next_record().expect("failed to read").expect("no record");
        assert_eq!(&*data, b"second");

        let (_, data) = iter.next_record().expect("failed to read").expect("no record");
        assert_eq!(&*data, b"third");

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_empty_segment() {
        let temp = temp_path();
        let writer = Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter();

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_single_record() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        writer.append(b"only one").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter();

        let (_, data) = iter.next_record().expect("failed to read").expect("no record");
        assert_eq!(&*data, b"only one");

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    // Concurrent Read/Write Tests

    #[test]
    fn test_flushed_offset_sync() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        let flushed = writer.flushed_offset();
        let mut reader =
            Reader::open(&temp, Some(flushed.clone())).expect("failed to open reader");

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

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
        assert_eq!(reader.flushed_offset().load(), 0);

        let result = reader.read_record(0, ReadHint::Random);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_record() {
        let temp = temp_path();
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        // Create a record larger than internal buffers
        let large_data = vec![0x42u8; 100_000];
        writer.append(&large_data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader =
            Reader::open(&temp, Some(flushed)).expect("failed to open reader");
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
        let mut writer =
            Writer::create(&temp, SEGMENT_SIZE).expect("failed to create writer");

        // Test records of varying sizes
        let sizes = vec![0, 1, 7, 8, 15, 16, 255, 256, 4095, 4096];

        for size in sizes {
            let data = vec![0xAAu8; size];
            let (offset, len) = writer.append(&data).expect("failed to append");
            assert_eq!(len, RECORD_HEAD_SIZE + size);

            // Verify we can read it back immediately
            writer.sync().expect("failed to sync");
            let flushed = writer.flushed_offset();

            let mut reader = Reader::open(&temp, Some(flushed.clone()))
                .expect("failed to open reader");
            let record = reader
                .read_record(offset, ReadHint::Random)
                .expect("failed to read");
            assert_eq!(record.len(), size);
        }
    }
}
