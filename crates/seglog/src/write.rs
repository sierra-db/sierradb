use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use thiserror::Error;
use tracing::{trace, warn};

use crate::read::{ReadError, Reader};
use crate::{FlushedOffset, RECORD_HEAD_SIZE, calculate_crc32c};

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
#[derive(Debug)]
pub struct Writer {
    writer: BufWriter<File>,
    size: usize,
    write_offset: u64,
    flushed_offset: FlushedOffset,
    dirty: bool,
}

impl Writer {
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
    /// const HEADER_SIZE: u64 = 16;
    /// let mut writer = Writer::create(&temp, 1024 * 1024, HEADER_SIZE)?;
    ///
    /// // Write header before start_offset
    /// let magic_bytes = b"MYSEG";
    /// writer.file().write_all_at(magic_bytes, 0)?;
    ///
    /// // Append records (automatically start at HEADER_SIZE)
    /// writer.append(b"event data")?;
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
    /// # let mut writer = Writer::create(&temp, 1024, 64)?;
    /// # writer.append(b"data")?;
    /// # writer.sync()?;
    /// # drop(writer);
    /// const HEADER_SIZE: u64 = 64;
    ///
    /// // Open existing segment (skips header when scanning)
    /// let mut writer = Writer::open(&temp, 1024, HEADER_SIZE)?;
    ///
    /// // Continue appending records
    /// writer.append(b"more data")?;
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
        let mut reader = Reader::open(path, None)?;
        let mut iter = reader.iter(start_offset);
        while let Some(res) = iter.next_record().transpose() {
            match res {
                Ok((offset, data)) => {
                    write_offset = offset + (RECORD_HEAD_SIZE + data.len()) as u64;
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
        })
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
    /// Returns the offset where the record was written and the total bytes written (including header).
    /// Returns an error if the segment does not have enough space remaining.
    pub fn append(&mut self, data: &[u8]) -> Result<(u64, usize), WriteError> {
        let offset = self.write_offset;
        let len = RECORD_HEAD_SIZE + data.len();
        if offset as usize + len > self.size {
            return Err(WriteError::SegmentFull {
                attempted: offset,
                available: self.size as u64 - offset,
            });
        }

        self.dirty = true;

        let data_len = data.len() as u32;
        let data_len_bytes = data_len.to_le_bytes();
        let crc = calculate_crc32c(&data_len_bytes, data);

        self.writer.write_all(&data_len_bytes)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(data)?;

        self.write_offset += len as u64;

        Ok((offset, len))
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

        // Write full zero header as clear truncation marker
        let zero_header = [0u8; RECORD_HEAD_SIZE];
        self.writer.get_ref().write_all_at(&zero_header, offset)?;
        self.writer.get_ref().sync_data()?;

        self.write_offset = offset;
        self.flushed_offset.set(offset);

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
}
