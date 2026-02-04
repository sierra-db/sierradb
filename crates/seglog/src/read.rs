use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use thiserror::Error;
use tracing::warn;

use crate::{
    COMPRESSION_FLAG, CRC32C_SIZE, FlushedOffset, LEN_SIZE, LENGTH_MASK, RECORD_HEAD_SIZE,
    calculate_crc32c,
};

const PAGE_SIZE: usize = 4096; // Usually a page is 4KB on Linux
const OPTIMISTIC_DATA_SIZE: usize = 2048; // Optimistic read size for random access: read header + this much data in one syscall
const FALLBACK_BUF_SIZE: usize = PAGE_SIZE;
const READ_AHEAD_SIZE: usize = 64 * 1024; // 64 KB read ahead buffer

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Record<'a, const H: usize> {
    pub header: Cow<'a, [u8]>,
    pub data: Cow<'a, [u8]>,
    pub compressed_data: Option<Cow<'a, [u8]>>,
    pub offset: u64,
    pub len: usize,
}

// impl<const H: usize> Record<'_, H> {
//     #[allow(clippy::len_without_is_empty)]
//     pub fn len(&self) -> usize {
//         RECORD_HEAD_SIZE + self.header.len() + self.data.len()
//     }
// }

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
    #[error("truncation marker found at offset {offset}")]
    TruncationMarker { offset: u64 },
    #[error(
        "data length of {new_length} does not match existing data length of {existing_length} for replace"
    )]
    ReplaceLengthMismatch {
        existing_length: usize,
        new_length: usize,
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
///
/// The generic parameter `H` specifies the size of the fixed header in bytes.
pub struct Reader<const H: usize> {
    file: File,
    // L1 cache: if a record fits in this buffer, it'll result in 1 syscall, 0 allocations
    optimistic_buf: [u8; RECORD_HEAD_SIZE + OPTIMISTIC_DATA_SIZE],
    // L2 cache: if a record fits in this buffer, it'll result in 2 syscalls, 0 allocations
    fallback_buf: [u8; FALLBACK_BUF_SIZE],
    // Sequential read cache
    read_ahead_buf: ReadAheadBuf,
    flushed_offset: FlushedOffset,
    // Decompression buffer (reused across reads to avoid allocations)
    decompress_buf: Vec<u8>,
}

impl<const H: usize> Reader<H> {
    /// Opens a segment as read only.
    pub fn open(
        path: impl AsRef<Path>,
        flushed_offset: Option<FlushedOffset>,
    ) -> Result<Self, ReadError> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

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
            decompress_buf: Vec::new(),
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
            decompress_buf: Vec::new(),
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

    /// Creates an iterator over records starting from the specified offset.
    pub fn iter(&mut self, start_offset: u64) -> Iter<'_, H> {
        Iter {
            reader: self,
            offset: start_offset,
        }
    }

    /// Reads a single record at the given offset.
    ///
    /// Returns a tuple of (header, data) after validating the CRC32C checksum.
    /// The hint parameter can optimize performance for sequential vs random access patterns.
    ///
    /// If the data is compressed, it will be automatically decompressed.
    pub fn read_record(&mut self, offset: u64, hint: ReadHint) -> Result<Record<'_, H>, ReadError> {
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

        // Random reads: optimistic read of record header + some data in one syscall
        let optimistic_read_len =
            (RECORD_HEAD_SIZE + OPTIMISTIC_DATA_SIZE).min((flushed_offset - offset) as usize);

        self.file
            .read_exact_at(&mut self.optimistic_buf[..optimistic_read_len], offset)?;

        let record_header_buf = &self.optimistic_buf[..RECORD_HEAD_SIZE];

        if is_truncation_marker(record_header_buf) {
            return Err(ReadError::TruncationMarker { offset });
        }

        let length_bytes: [u8; LEN_SIZE] = record_header_buf[..LEN_SIZE].try_into().unwrap();
        let length_with_flag = u32::from_le_bytes(length_bytes);
        let is_compressed = length_with_flag & COMPRESSION_FLAG != 0;
        let payload_len = (length_with_flag & LENGTH_MASK) as usize; // H + data_len

        let crc = u32::from_le_bytes(
            record_header_buf[LEN_SIZE..LEN_SIZE + CRC32C_SIZE]
                .try_into()
                .unwrap(),
        );

        let payload_offset = offset + RECORD_HEAD_SIZE as u64;
        if payload_offset + payload_len as u64 > flushed_offset {
            return Err(ReadError::OutOfBounds {
                offset,
                length: RECORD_HEAD_SIZE + payload_len,
                flushed_offset,
            });
        }

        // Read header + data payload
        let (header, compressed_data) = if payload_len <= OPTIMISTIC_DATA_SIZE
            && optimistic_read_len >= RECORD_HEAD_SIZE + payload_len
        {
            // Payload fits in the optimistic buffer - we got it all in one read!
            let payload = &self.optimistic_buf[RECORD_HEAD_SIZE..RECORD_HEAD_SIZE + payload_len];
            let header = &payload[..H];
            let data = &payload[H..];

            // Validate CRC over header + compressed data
            let new_crc = calculate_crc32c(&length_bytes, header, data);
            if crc != new_crc {
                return Err(ReadError::Crc32cMismatch { offset });
            }

            (Cow::Borrowed(header), Cow::Borrowed(data))
        } else if payload_len <= self.fallback_buf.len() {
            // Payload fits in fallback_buf but not in optimistic buffer
            self.file
                .read_exact_at(&mut self.fallback_buf[..payload_len], payload_offset)?;
            let header = &self.fallback_buf[..H];
            let data = &self.fallback_buf[H..payload_len];

            // Validate CRC
            let new_crc = calculate_crc32c(&length_bytes, header, data);
            if crc != new_crc {
                return Err(ReadError::Crc32cMismatch { offset });
            }

            (Cow::Borrowed(header), Cow::Borrowed(data))
        } else {
            // Payload is large, allocate a buffer
            let mut buf = vec![0u8; payload_len];
            self.file.read_exact_at(&mut buf, payload_offset)?;
            let data = buf[H..].to_vec();
            buf.truncate(H);
            let header = buf;

            // Validate CRC
            let new_crc = calculate_crc32c(&length_bytes, &header, &data);
            if crc != new_crc {
                return Err(ReadError::Crc32cMismatch { offset });
            }

            (Cow::Owned(header), Cow::Owned(data))
        };

        // Decompress if needed
        let (final_data, compressed_data) = if is_compressed {
            // First 4 bytes are the original size
            if compressed_data.len() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "compressed data too short to contain original size",
                )
                .into());
            }
            let original_size_bytes: [u8; 4] = compressed_data[..4].try_into().unwrap();
            let original_size = u32::from_le_bytes(original_size_bytes) as usize;

            // Decompress into reusable buffer
            self.decompress_buf.clear();
            self.decompress_buf.reserve(original_size);
            zstd::stream::copy_decode(&compressed_data[4..], &mut self.decompress_buf)?;

            (
                Cow::Borrowed(self.decompress_buf.as_slice()),
                Some(compressed_data),
            )
        } else {
            (compressed_data, None)
        };

        Ok(Record {
            header,
            data: final_data,
            compressed_data,
            offset,
            len: RECORD_HEAD_SIZE + payload_len,
        })
    }

    fn read_record_sequential(
        &mut self,
        offset: u64,
        flushed_offset: u64,
    ) -> Result<Record<'_, H>, ReadError> {
        let record_header_buf = self
            .read_ahead_buf
            .read(&self.file, offset, RECORD_HEAD_SIZE)?;

        if is_truncation_marker(&record_header_buf[..RECORD_HEAD_SIZE]) {
            return Err(ReadError::TruncationMarker { offset });
        }

        let length_bytes: [u8; LEN_SIZE] = record_header_buf[..LEN_SIZE].try_into().unwrap();
        let length_with_flag = u32::from_le_bytes(length_bytes);
        let is_compressed = length_with_flag & COMPRESSION_FLAG != 0;
        let payload_len = (length_with_flag & LENGTH_MASK) as usize;
        let crc = u32::from_le_bytes(
            record_header_buf[LEN_SIZE..LEN_SIZE + CRC32C_SIZE]
                .try_into()
                .unwrap(),
        );

        let payload_offset = offset + RECORD_HEAD_SIZE as u64;
        if payload_offset + payload_len as u64 > flushed_offset {
            return Err(ReadError::OutOfBounds {
                offset,
                length: RECORD_HEAD_SIZE + payload_len,
                flushed_offset,
            });
        }

        let payload = self
            .read_ahead_buf
            .read(&self.file, payload_offset, payload_len)?;

        let header = &payload[..H];
        let compressed_data = &payload[H..];

        let new_crc = calculate_crc32c(&length_bytes, header, compressed_data);
        if crc != new_crc {
            return Err(ReadError::Crc32cMismatch { offset });
        }

        // Decompress if needed
        let (final_data, compressed_data) = if is_compressed {
            if compressed_data.len() < 4 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "compressed data too short to contain original size",
                )
                .into());
            }
            let original_size_bytes: [u8; 4] = compressed_data[..4].try_into().unwrap();
            let original_size = u32::from_le_bytes(original_size_bytes) as usize;

            self.decompress_buf.clear();
            self.decompress_buf.reserve(original_size);
            zstd::stream::copy_decode(&compressed_data[4..], &mut self.decompress_buf)?;

            (
                Cow::Borrowed(self.decompress_buf.as_slice()),
                Some(Cow::Borrowed(compressed_data)),
            )
        } else {
            (Cow::Borrowed(compressed_data), None)
        };

        Ok(Record {
            header: Cow::Borrowed(header),
            data: final_data,
            compressed_data,
            offset,
            len: RECORD_HEAD_SIZE + payload_len,
        })
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

    /// Replaces the header portion of a record at the given offset.
    ///
    /// This method reads the full record to get the data portion, calculates a new CRC
    /// with the new header, and writes back only the CRC and header bytes. The data
    /// portion on disk is left untouched.
    ///
    /// This is safe to use even with compressed records since the data is not modified.
    pub fn replace_header(&mut self, offset: u64, new_header: [u8; H]) -> Result<(), ReadError> {
        self.replace_header_with(offset, |_| Some(new_header))?;
        Ok(())
    }

    /// Conditionally replaces the header portion of a record at the given offset.
    ///
    /// This method reads the full record and passes it to the closure `f`. The closure
    /// can inspect the current record state (header, data, compression status, etc.) and
    /// decide whether to replace the header by returning `Some([u8; H])` or skip the
    /// replacement by returning `None`.
    ///
    /// If the closure returns `Some(new_header)`, this method calculates a new CRC with
    /// the new header and writes back only the CRC and header bytes. The data portion on
    /// disk is left untouched.
    ///
    /// This is safe to use even with compressed records since the data is not modified.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use seglog::read::Reader;
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let temp = std::path::Path::new("segment.log");
    /// let mut reader = Reader::<8>::open(temp, None)?;
    ///
    /// // Conditionally increment a counter in the header
    /// reader.replace_header_with(0, |record| {
    ///     let current_header: [u8; 8] = record.header.to_vec().try_into().ok()?;
    ///     let counter = u64::from_le_bytes(current_header);
    ///
    ///     // Only update if counter is less than 100
    ///     if counter < 100 {
    ///         Some((counter + 1).to_le_bytes())
    ///     } else {
    ///         None // Skip replacement
    ///     }
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn replace_header_with<F>(&mut self, offset: u64, f: F) -> Result<bool, ReadError>
    where
        F: FnOnce(&Record<'_, H>) -> Option<[u8; H]>,
    {
        // Read the full record to validate it exists and is readable
        let record = self.read_record(offset, ReadHint::Random)?;

        let Some(new_header) = f(&record) else {
            return Ok(false);
        };

        // Calculate payload_len from the record's len field
        let payload_len = record.len - RECORD_HEAD_SIZE;

        // Reconstruct the length_with_flag for CRC calculation
        let length_with_flag = if record.compressed_data.is_some() {
            (payload_len as u32) | COMPRESSION_FLAG
        } else {
            payload_len as u32
        };
        let length_bytes = length_with_flag.to_le_bytes();

        // Get the data as it exists on disk (compressed or not)
        let data_on_disk = record.compressed_data.as_ref().unwrap_or(&record.data);

        // Calculate new CRC over new_header + data_on_disk
        let new_crc = calculate_crc32c(&length_bytes, &new_header, data_on_disk);

        // Write CRC + header (leave data untouched)
        let mut write_buf = Vec::with_capacity(CRC32C_SIZE + H);
        write_buf.extend_from_slice(&new_crc.to_le_bytes());
        write_buf.extend_from_slice(&new_header);

        self.file
            .write_all_at(&write_buf, offset + LEN_SIZE as u64)?;

        // Invalidate cache if overlapping
        if self
            .read_ahead_buf
            .overlaps(offset, RECORD_HEAD_SIZE + payload_len)
        {
            self.read_ahead_buf.invalidate();
        }

        // Sync to ensure durability
        self.file.sync_data()?;

        Ok(true)
    }

    /// Closes the reader.
    ///
    /// This explicitly consumes the reader. All resources are released when the reader is dropped.
    pub fn close(self) {}
}

/// Type alias for a result returned from [`Iter::next_record`].
pub type IterResult<'a, const H: usize> = Result<Option<Record<'a, H>>, ReadError>;

/// Iterator for sequentially reading records from a segment.
pub struct Iter<'a, const H: usize> {
    reader: &'a mut Reader<H>,
    offset: u64,
}

impl<const H: usize> Iter<'_, H> {
    /// Returns the next record as a tuple of (offset, header, data).
    ///
    /// Returns `Ok(None)` when reaching the end of valid records or a truncation marker.
    /// Uses sequential read hints for optimized performance.
    pub fn next_record(&mut self) -> IterResult<'_, H> {
        match self.reader.read_record(self.offset, ReadHint::Sequential) {
            Ok(record) => {
                self.offset += record.len as u64;
                Ok(Some(record))
            }
            Err(ReadError::OutOfBounds { .. } | ReadError::TruncationMarker { .. }) => Ok(None),
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

    fn overlaps(&self, offset: u64, len: usize) -> bool {
        if self.valid_len == 0 {
            return false;
        }

        let end = offset + len as u64;
        let buf_end = self.offset + self.valid_len as u64;

        // Two ranges [a, b) and [c, d) overlap if: a < d && c < b
        offset < buf_end && self.offset < end
    }

    fn invalidate(&mut self) {
        self.valid_len = 0;
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
