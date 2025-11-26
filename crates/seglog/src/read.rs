use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use thiserror::Error;
use tracing::warn;

use crate::{CRC32C_SIZE, FlushedOffset, LEN_SIZE, RECORD_HEAD_SIZE, calculate_crc32c};

const PAGE_SIZE: usize = 4096; // Usually a page is 4KB on Linux
const OPTIMISTIC_DATA_SIZE: usize = 2048; // Optimistic read size for random access: read header + this much data in one syscall
const FALLBACK_BUF_SIZE: usize = PAGE_SIZE;
const READ_AHEAD_SIZE: usize = 64 * 1024; // 64 KB read ahead buffer

/// Errors that can occur during segment reading operations.
#[derive(Debug, Error)]
pub enum ReadError {
    #[error("crc32c hash mismatch at offset {offset}")]
    Crc32cMismatch { offset: u64 },
    #[error("read at offset {offset} with length {length} exceeds flushed offset {flushed_offset}")]
    OutOfBounds {
        offset: u64,
        length: usize,
        flushed_offset: u64,
    },
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Hint for optimizing read operations based on access pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadHint {
    /// Random access pattern, no read-ahead buffering.
    Random,
    /// Sequential access pattern, uses read-ahead buffering.
    Sequential,
}

/// Reader for reading records from a segment file.
///
/// Supports both random and sequential access patterns with appropriate optimizations.
/// Records are read with CRC32C validation to ensure data integrity.
pub struct Reader {
    file: File,
    // L1 cache: if a record fits in this buffer, it'll result in 1 syscall, 0 allocations
    optimistic_buf: [u8; RECORD_HEAD_SIZE + OPTIMISTIC_DATA_SIZE],
    // L2 cache: if a record fits in this buffer, it'll result in 2 syscalls, 0 allocations
    fallback_buf: [u8; FALLBACK_BUF_SIZE],
    // Sequential read cache
    read_ahead_buf: ReadAheadBuf,
    flushed_offset: FlushedOffset,
}

impl Reader {
    /// Opens a segment as read only.
    pub fn open(
        path: impl AsRef<Path>,
        flushed_offset: Option<FlushedOffset>,
    ) -> Result<Self, ReadError> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(false);

        #[cfg(target_os = "macos")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // On OSX, gives ~5% better performance for both random and sequential reads
            const O_DIRECT: i32 = 0o0040000;
            opts.custom_flags(O_DIRECT);
        }

        let file = opts.open(path)?;
        let fallback_buf = [0u8; FALLBACK_BUF_SIZE];

        let flushed_offset = match flushed_offset {
            Some(flushed_offset) => flushed_offset,
            None => {
                let len = file.metadata()?.len();
                FlushedOffset::new(len)
            }
        };

        let reader = Reader {
            file,
            optimistic_buf: [0u8; RECORD_HEAD_SIZE + OPTIMISTIC_DATA_SIZE],
            fallback_buf,
            read_ahead_buf: ReadAheadBuf::new(),
            flushed_offset,
        };

        Ok(reader)
    }

    /// Creates a new reader by cloning the underlying file handle.
    ///
    /// The cloned reader shares the same flushed offset but has independent read buffers.
    pub fn try_clone(&self) -> Result<Self, ReadError> {
        Ok(Reader {
            file: self.file.try_clone()?,
            optimistic_buf: [0u8; RECORD_HEAD_SIZE + OPTIMISTIC_DATA_SIZE],
            fallback_buf: self.fallback_buf,
            read_ahead_buf: ReadAheadBuf::new(),
            flushed_offset: self.flushed_offset.clone(),
        })
    }

    /// Returns a reference to the file handle.
    pub fn file(&self) -> &File {
        &self.file
    }

    /// Returns a reference to the flushed offset tracker.
    pub fn flushed_offset(&self) -> &FlushedOffset {
        &self.flushed_offset
    }

    /// Prefetches data at the given offset into the OS page cache.
    ///
    /// On Linux, uses `posix_fadvise` to hint the kernel to load the page. No-op on other platforms.
    pub fn prefetch(&self, offset: u64) {
        #[cfg(all(unix, target_os = "linux"))]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                nix::libc::posix_fadvise(
                    self.file.as_raw_fd(),
                    offset as i64,
                    PAGE_SIZE as i64,
                    nix::libc::POSIX_FADV_WILLNEED,
                );
            }
        }
    }

    /// Creates an iterator over all records starting from offset 0.
    pub fn iter(&mut self) -> Iter<'_> {
        self.iter_from(0)
    }

    /// Creates an iterator over records starting from the specified offset.
    pub fn iter_from(&mut self, start_offset: u64) -> Iter<'_> {
        Iter {
            reader: self,
            offset: start_offset,
        }
    }

    /// Reads a single record at the given offset.
    ///
    /// Returns the record data after validating the CRC32C checksum. The hint parameter
    /// can optimize performance for sequential vs random access patterns.
    pub fn read_record(&mut self, offset: u64, hint: ReadHint) -> Result<Cow<'_, [u8]>, ReadError> {
        let flushed_offset = self.flushed_offset.load();
        if offset + RECORD_HEAD_SIZE as u64 > flushed_offset {
            return Err(ReadError::OutOfBounds {
                offset,
                length: RECORD_HEAD_SIZE,
                flushed_offset,
            });
        }

        if matches!(hint, ReadHint::Sequential) {
            // Sequential reads use the read-ahead buffer
            return self.read_record_sequential(offset, flushed_offset);
        }

        // Random reads: optimistic read of header + data in one syscall
        let optimistic_read_len =
            (RECORD_HEAD_SIZE + OPTIMISTIC_DATA_SIZE).min((flushed_offset - offset) as usize);

        self.file
            .read_exact_at(&mut self.optimistic_buf[..optimistic_read_len], offset)?;

        let header_buf = &self.optimistic_buf[..RECORD_HEAD_SIZE];

        if is_truncation_marker(header_buf) {
            return Err(ReadError::OutOfBounds {
                offset,
                length: RECORD_HEAD_SIZE,
                flushed_offset,
            });
        }

        let data_len_bytes: [u8; LEN_SIZE] = header_buf[..LEN_SIZE].try_into().unwrap();
        let data_len = u32::from_le_bytes(data_len_bytes) as usize;
        let crc = u32::from_le_bytes(
            header_buf[LEN_SIZE..LEN_SIZE + CRC32C_SIZE]
                .try_into()
                .unwrap(),
        );

        let data_offset = offset + RECORD_HEAD_SIZE as u64;
        if data_offset + data_len as u64 > flushed_offset {
            return Err(ReadError::OutOfBounds {
                offset,
                length: RECORD_HEAD_SIZE,
                flushed_offset,
            });
        }

        let (data, new_crc) = if data_len <= OPTIMISTIC_DATA_SIZE
            && optimistic_read_len >= RECORD_HEAD_SIZE + data_len
        {
            // Data fits in the optimistic buffer - we got it all in one read!
            let data = &self.optimistic_buf[RECORD_HEAD_SIZE..RECORD_HEAD_SIZE + data_len];
            let new_crc = calculate_crc32c(&data_len_bytes, data);
            (Cow::Borrowed(data), new_crc)
        } else if data_len <= self.fallback_buf.len() {
            // Data fits in fallback_buf but not in optimistic buffer
            self.file
                .read_exact_at(&mut self.fallback_buf[..data_len], data_offset)?;
            let new_crc = calculate_crc32c(&data_len_bytes, &self.fallback_buf[..data_len]);
            (Cow::Borrowed(&self.fallback_buf[..data_len]), new_crc)
        } else {
            // Data is large, allocate a buffer
            let mut buf = vec![0u8; data_len];
            self.file.read_exact_at(&mut buf, data_offset)?;
            let new_crc = calculate_crc32c(&data_len_bytes, &buf);
            (Cow::Owned(buf), new_crc)
        };

        if crc != new_crc {
            return Err(ReadError::Crc32cMismatch { offset });
        }

        Ok(data)
    }

    fn read_record_sequential(
        &mut self,
        offset: u64,
        flushed_offset: u64,
    ) -> Result<Cow<'_, [u8]>, ReadError> {
        let header_buf = self
            .read_ahead_buf
            .read(&self.file, offset, RECORD_HEAD_SIZE)?;

        if is_truncation_marker(&header_buf[..RECORD_HEAD_SIZE]) {
            return Err(ReadError::OutOfBounds {
                offset,
                length: RECORD_HEAD_SIZE,
                flushed_offset,
            });
        }

        let data_len_bytes: [u8; LEN_SIZE] = header_buf[..LEN_SIZE].try_into().unwrap();
        let data_len = u32::from_le_bytes(data_len_bytes) as usize;
        let crc = u32::from_le_bytes(
            header_buf[LEN_SIZE..LEN_SIZE + CRC32C_SIZE]
                .try_into()
                .unwrap(),
        );

        let data_offset = offset + RECORD_HEAD_SIZE as u64;
        if data_offset + data_len as u64 > flushed_offset {
            return Err(ReadError::OutOfBounds {
                offset,
                length: RECORD_HEAD_SIZE,
                flushed_offset,
            });
        }

        let data = self
            .read_ahead_buf
            .read(&self.file, data_offset, data_len)?;
        let new_crc = calculate_crc32c(&data_len_bytes, data);

        if crc != new_crc {
            return Err(ReadError::Crc32cMismatch { offset });
        }

        Ok(Cow::Borrowed(data))
    }

    /// Reads raw bytes from the file at the specified offset and length.
    ///
    /// This bypasses record structure and CRC validation, reading directly from the file.
    pub fn read_bytes(&self, offset: u64, buf: &mut [u8]) -> Result<(), ReadError> {
        let end = offset.checked_add(buf.len() as u64).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("block offset {offset} + len {} overflows u64", buf.len()),
            )
        })?;

        let flushed_offset = self.flushed_offset.load();
        if end > flushed_offset {
            return Err(ReadError::OutOfBounds {
                offset,
                length: buf.len(),
                flushed_offset,
            });
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(buf, offset)?;
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.file.seek_read(buf, offset)?;
        }

        #[cfg(not(any(unix, windows)))]
        {
            compile_error!("Unsupported platform for positioned file I/O");
        }

        Ok(())
    }

    /// Closes the reader.
    ///
    /// This explicitly consumes the reader. All resources are released when the reader is dropped.
    pub fn close(self) {}
}

/// Type alias for a result returned from [`Iter::next_record`].
pub type IterResult<'a> = Result<Option<(u64, Cow<'a, [u8]>)>, ReadError>;

/// Iterator for sequentially reading records from a segment.
pub struct Iter<'a> {
    reader: &'a mut Reader,
    offset: u64,
}

impl Iter<'_> {
    /// Returns the next record as a tuple of (offset, data).
    ///
    /// Returns `Ok(None)` when reaching the end of valid records or a truncation marker.
    /// Uses sequential read hints for optimized performance.
    pub fn next_record(&mut self) -> IterResult<'_> {
        let offset = self.offset;
        match self.reader.read_record(self.offset, ReadHint::Sequential) {
            Ok(data) => {
                self.offset += (RECORD_HEAD_SIZE + data.len()) as u64;
                Ok(Some((offset, data)))
            }
            Err(ReadError::OutOfBounds { .. }) => Ok(None),
            Err(err) => {
                warn!("unexpected read error at offset {}: {err}", self.offset);
                Err(err)
            }
        }
    }
}

struct ReadAheadBuf {
    buf: Vec<u8>,
    offset: u64, // File offset of the buffer start
    pos: usize,  // Current read position in buffer
    valid_len: usize,
}

impl ReadAheadBuf {
    fn new() -> Self {
        ReadAheadBuf {
            buf: Vec::with_capacity(READ_AHEAD_SIZE),
            offset: 0,
            pos: 0,
            valid_len: 0,
        }
    }

    fn read(&mut self, file: &File, offset: u64, length: usize) -> Result<&[u8], ReadError> {
        let end_offset = offset + length as u64;

        // If offset is within the valid read-ahead range
        if offset >= self.offset && end_offset <= (self.offset + self.valid_len as u64) {
            let start = (offset - self.offset) as usize;
            return Ok(&self.buf[start..start + length]);
        }

        // Fill the read-ahead buffer for the requested offset & length
        self.fill(file, offset, length)?;

        // Ensure we now have enough valid data
        if offset < self.offset || end_offset > (self.offset + self.valid_len as u64) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "requested data exceeds available read-ahead buffer",
            )
            .into());
        }

        let start = (offset - self.offset) as usize;
        Ok(&self.buf[start..start + length])
    }

    fn fill(&mut self, file: &File, offset: u64, mut length: usize) -> Result<(), ReadError> {
        let end_offset = offset + length as u64;

        // Set the new read-ahead offset aligned to 64KB
        self.offset = offset - (offset % READ_AHEAD_SIZE as u64);
        self.pos = 0;
        length = (end_offset - self.offset) as usize;

        // If the requested read is larger than READ_AHEAD_SIZE, expand the buffer to
        // the next leargest interval of 4096
        let required_size = (length.max(READ_AHEAD_SIZE) + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);

        // Resize buffer if necessary
        if self.buf.len() != required_size {
            self.buf.resize(required_size, 0);
            self.buf.shrink_to_fit();
        }

        let mut total_read = 0;
        while total_read < required_size {
            let bytes_read =
                file.read_at(&mut self.buf[total_read..], self.offset + total_read as u64)?;
            if bytes_read == 0 {
                break; // EOF reached
            }
            total_read += bytes_read;
        }

        self.valid_len = total_read;

        Ok(())
    }
}

#[inline]
pub(crate) fn is_truncation_marker(header: &[u8]) -> bool {
    header.iter().all(|&b| b == 0)
}
