use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use thiserror::Error;
use tracing::{trace, warn};

use crate::read::{ReadError, ReadHint, Reader};
use crate::{
    COMPRESSION_FLAG, FlushedOffset, MIN_COMPRESSION_SIZE, RECORD_HEAD_SIZE,
    ZSTD_COMPRESSION_LEVEL, calculate_crc32c,
};

const WRITE_BUF_SIZE: usize = 16 * 1024; // 16 KB buffer

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

/// Writer for appending records to a segment file.
///
/// Manages buffered writes to a segment file with a fixed maximum size.
/// Tracks both the current write offset and the last flushed offset for concurrent read safety.
///
/// The generic parameter `H` specifies the size of the fixed header in bytes.
#[derive(Debug)]
pub struct Writer<const H: usize> {
    writer: BufWriter<File>,
    size: usize,
    write_offset: u64,
    flushed_offset: FlushedOffset,
    dirty: bool,
    compression_enabled: bool,
}

impl Writer<0> {
    #[inline]
    pub fn append_data(&mut self, data: &[u8]) -> Result<(u64, usize), WriteError> {
        self.append(&[], data)
    }
}

impl<const H: usize> Writer<H> {
    /// Creates a new segment file at the specified path with the given size.
    ///
    /// # Arguments
    ///
    /// * `path` - The file path where the segment will be created
    /// * `size` - The total size of the segment file in bytes
    /// * `start_offset` - The byte offset where records will begin (reserves space for headers)
    ///
    /// The `start_offset` parameter allows reserving space at the beginning of the file for
    /// application-specific headers (e.g., magic bytes, version, metadata). Records will be
    /// written starting from this offset. Use `file()` to write header data to offsets before
    /// `start_offset`.
    ///
    /// On Linux, pre-allocates disk space using `fallocate` for better performance.
    /// Returns an error if the file already exists.
    ///
    /// # Example
    ///
    /// ```rust
    /// use seglog::write::Writer;
    /// # use std::io::Write;
    /// # use std::os::unix::fs::FileExt;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::TempDir::new()?;
    /// # let temp = dir.path().join("segment.log");
    /// const START_OFFSET: u64 = 16;
    /// let mut writer = Writer::<8>::create(&temp, 1024 * 1024, START_OFFSET)?;
    ///
    /// // Write file header before start_offset
    /// let magic_bytes = b"MYSEG";
    /// writer.file().write_all_at(magic_bytes, 0)?;
    ///
    /// // Append records with 8-byte headers (automatically start at START_OFFSET)
    /// let header = [0u8; 8];
    /// writer.append(&header, b"event data")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create(
        path: impl AsRef<Path>,
        size: usize,
        start_offset: u64,
    ) -> Result<Self, WriteError> {
        assert!(
            size as u64 > start_offset,
            "segment size cannot be less than or equal to start offset"
        );

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        // Pre-allocate on Linux
        #[cfg(target_os = "linux")]
        {
            nix::fcntl::fallocate(&file, nix::fcntl::FallocateFlags::empty(), 0, size as i64)?;
        }

        let write_offset = start_offset;
        let flushed_offset = FlushedOffset::new(write_offset);

        let mut writer = BufWriter::with_capacity(WRITE_BUF_SIZE, file);
        writer.seek(SeekFrom::Start(write_offset))?;

        Ok(Writer {
            writer,
            size,
            write_offset,
            flushed_offset,
            dirty: false,
            compression_enabled: false,
        })
    }

    /// Opens an existing segment file for writing.
    ///
    /// # Arguments
    ///
    /// * `path` - The file path of the existing segment
    /// * `size` - The total size of the segment file in bytes
    /// * `start_offset` - The byte offset where records begin (must match the value used in `create`)
    ///
    /// Scans the file starting from `start_offset` to find the last valid record and positions
    /// the write offset accordingly. If corruption is detected, all data after the corruption
    /// point is considered invalid and the write offset is positioned at the last valid record.
    ///
    /// The `start_offset` must match the value used when the segment was created with `create()`.
    /// This allows the segment to skip over any header data when scanning for valid records.
    ///
    /// # Example
    ///
    /// ```rust
    /// use seglog::write::Writer;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::TempDir::new()?;
    /// # let temp = dir.path().join("segment.log");
    /// # let mut writer = Writer::<0>::create(&temp, 1024, 64)?;
    /// # writer.append(&[], b"data")?;
    /// # writer.sync()?;
    /// # drop(writer);
    /// const HEADER_SIZE: u64 = 64;
    ///
    /// // Open existing segment (skips header when scanning)
    /// let mut writer = Writer::<0>::open(&temp, 1024, HEADER_SIZE)?;
    ///
    /// // Continue appending records
    /// writer.append(&[], b"more data")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(
        path: impl AsRef<Path>,
        size: usize,
        start_offset: u64,
    ) -> Result<Self, WriteError> {
        assert!(
            size as u64 > start_offset,
            "segment size cannot be less than or equal to start offset"
        );

        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut write_offset = start_offset;

        // We need to scan forward to find the latest valid record, and consider that to
        // be the write offset
        let mut reader = Reader::<H>::open(path, None)?;
        let mut current_offset = start_offset;
        loop {
            match reader.read_record(current_offset, ReadHint::Sequential) {
                Ok(record) => {
                    current_offset += record.len as u64;
                    write_offset = current_offset;
                }
                Err(err) => match err {
                    ReadError::Crc32cMismatch { .. } => {
                        warn!(
                            "corruption found at {write_offset}: {err} - all data after this point will be considered invalid"
                        );
                        break;
                    }
                    ReadError::OutOfBounds { .. } => {
                        trace!("reached out of bounds at {write_offset}");
                        break;
                    }
                    ReadError::TruncationMarker { .. } => {
                        trace!("found truncation marker at {write_offset}");
                        break;
                    }
                    ReadError::ReplaceLengthMismatch { .. } => {
                        unreachable!("no replacing is done during iteration")
                    }
                    ReadError::Io(err) => return Err(WriteError::Io(err)),
                },
            }
        }

        let mut writer = BufWriter::with_capacity(WRITE_BUF_SIZE, file);
        writer.seek(SeekFrom::Start(write_offset))?;

        let flushed_offset = FlushedOffset::new(write_offset);

        Ok(Writer {
            writer,
            size,
            write_offset,
            flushed_offset,
            dirty: false,
            compression_enabled: false,
        })
    }

    /// Enables compression for future append operations.
    ///
    /// Once enabled, all data appended via `append()` will be compressed using zstd
    /// if the data size is >= [`MIN_COMPRESSION_SIZE`].
    pub fn enable_compression(&mut self) {
        self.compression_enabled = true;
    }

    /// Disables compression for future append operations.
    pub fn disable_compression(&mut self) {
        self.compression_enabled = false;
    }

    /// Returns a reference to the file handle.
    pub fn file(&self) -> &File {
        self.writer.get_ref()
    }

    /// Returns the last flushed read only atomic offset.
    ///
    /// Any content before this value at any given time is immutable and safe to be read concurrently.
    #[inline]
    pub fn flushed_offset(&self) -> FlushedOffset {
        self.flushed_offset.clone()
    }

    /// Appends a record to the segment.
    ///
    /// # Arguments
    ///
    /// * `header` - Fixed-size header metadata (H bytes, never compressed)
    /// * `data` - Variable-length data payload (optionally compressed based on `compression_enabled`)
    ///
    /// Returns the offset where the record was written and the total bytes written (including all headers and data).
    /// Returns an error if the segment does not have enough space remaining.
    ///
    /// If compression is enabled and `data.len() >= MIN_COMPRESSION_SIZE`, the data will be compressed
    /// using zstd. The header is never compressed.
    pub fn append(&mut self, header: &[u8; H], data: &[u8]) -> Result<(u64, usize), WriteError> {
        let offset = self.write_offset;

        let (final_data, length_with_flag) = self.prepare_data(data)?;

        let total_record_len = RECORD_HEAD_SIZE + H + final_data.len();
        if offset as usize + total_record_len > self.size {
            return Err(WriteError::SegmentFull {
                attempted: offset,
                available: self.size as u64 - offset,
            });
        }

        self.dirty = true;

        let length_bytes = length_with_flag.to_le_bytes();
        let crc = calculate_crc32c(&length_bytes, header, &final_data);

        self.writer.write_all(&length_bytes)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(header)?;
        self.writer.write_all(&final_data)?;

        self.write_offset += total_record_len as u64;

        Ok((offset, total_record_len))
    }

    /// Returns the current write offset where the next record will be written.
    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    /// Returns the number of bytes remaining in the segment.
    pub fn remaining_bytes(&self) -> u64 {
        self.size as u64 - self.write_offset
    }

    /// Truncates the segment to the specified offset.
    ///
    /// Writes a zero-filled header as a truncation marker at the offset and updates
    /// both the write offset and flushed offset. No-op if the offset is >= current write offset.
    pub fn set_len(&mut self, offset: u64) -> Result<(), WriteError> {
        if offset >= self.write_offset {
            // core::hint::cold_path();
            return Ok(());
        }

        self.sync()?;

        self.flushed_offset.set(offset);
        self.write_offset = offset;

        // Write full zero header as clear truncation marker
        let zero_header = [0u8; RECORD_HEAD_SIZE];
        self.writer.get_ref().write_all_at(&zero_header, offset)?;
        self.writer.get_ref().sync_data()?;

        Ok(())
    }

    /// Flushes the buffered writer without syncing to disk.
    ///
    /// This ensures data is written to the OS but does not guarantee persistence to disk.
    pub fn flush_writer(&mut self) -> Result<(), WriteError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Flushes the file, ensuring all data is persisted to disk.
    pub fn sync(&mut self) -> Result<u64, WriteError> {
        if self.dirty {
            trace!("flushing writer");
            self.writer.flush()?;
            self.writer.get_ref().sync_data()?;
            self.flushed_offset.set(self.write_offset);
            self.dirty = false;
        }

        Ok(self.write_offset)
    }

    /// Closes the writer, ensuring all data is synced to disk.
    ///
    /// This is equivalent to calling `sync()` and then dropping the writer.
    pub fn close(mut self) -> Result<(), WriteError> {
        self.sync()?;
        Ok(())
    }

    fn prepare_data<'d>(&self, data: &'d [u8]) -> Result<(Cow<'d, [u8]>, u32), WriteError> {
        // Decide whether to compress
        let should_compress = self.compression_enabled && data.len() >= MIN_COMPRESSION_SIZE;

        // Prepare data (with compression if needed)
        if should_compress {
            // Compress data and prepend original size
            let original_size = data.len() as u32;
            let compressed = zstd::bulk::compress(data, ZSTD_COMPRESSION_LEVEL)?;

            let mut final_data = Vec::with_capacity(4 + compressed.len());
            final_data.extend_from_slice(&original_size.to_le_bytes());
            final_data.extend_from_slice(&compressed);

            let total_payload_len = H + final_data.len();
            let length_with_flag = (total_payload_len as u32) | COMPRESSION_FLAG;

            Ok((Cow::Owned(final_data), length_with_flag))
        } else {
            let total_payload_len = H + data.len();
            let length_with_flag = total_payload_len as u32;
            Ok((Cow::Borrowed(data), length_with_flag))
        }
    }
}
