use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use tracing::{trace, warn};

use crate::read::Reader;
use crate::{FlushedOffset, RECORD_HEAD_SIZE, ReadError, WriteError, calculate_crc32c};

const WRITE_BUF_SIZE: usize = 16 * 1024; // 16 KB buffer

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
    /// On Linux, pre-allocates disk space using `fallocate` for better performance.
    /// Returns an error if the file already exists.
    pub fn create(path: impl AsRef<Path>, size: usize) -> Result<Self, WriteError> {
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(path)?;

        // Pre-allocate on Linux
        #[cfg(target_os = "linux")]
        {
            nix::fcntl::fallocate(&file, nix::fcntl::FallocateFlags::empty(), 0, size as i64)?;
        }

        let write_offset = 0;
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
    /// Scans the file to find the last valid record and positions the write offset accordingly.
    /// If corruption is detected, all data after the corruption point is considered invalid.
    pub fn open(path: impl AsRef<Path>, size: usize) -> Result<Self, WriteError> {
        let file = OpenOptions::new().read(false).write(true).open(&path)?;
        let mut write_offset = 0;

        // We need to scan forward to find the latest valid record, and consider that to
        // be the write offset
        let mut reader = Reader::open(path, None)?;
        let mut iter = reader.iter();
        while let Some(res) = iter.next_record().transpose() {
            match res {
                Ok((offset, data)) => {
                    write_offset = offset + (RECORD_HEAD_SIZE + data.len()) as u64;
                }
                Err(err) => match err {
                    ReadError::Crc32cMismatch | ReadError::OutOfBounds => {
                        warn!(
                            "corruption found at {write_offset}: {err} - all data after this point will be considered invalid"
                        );
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
