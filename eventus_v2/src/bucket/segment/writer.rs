use std::{
    fs::{File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use super::{
    calculate_commit_crc32c, calculate_event_crc32c, BucketSegmentHeader, FlushSender,
    FlushedOffset, COMMIT_SIZE, EVENT_HEADER_SIZE, MAGIC_BYTES, PADDING_SIZE, SEGMENT_HEADER_SIZE,
};

const WRITE_BUF_SIZE: usize = 16 * 1024; // 16 KB buffer

macro_rules! write_bytes {
    ($this:ident, [ $( $buf:expr $( => $len:expr )? $(,)? )* ]) => {
       $(
           write_bytes!($this, $buf $(, $len )?);
       )*
    };
    ($this:ident, $buf:expr) => {{
        let buf = $buf;
        let len = buf.len();
        write_bytes!($this, buf, len);
    }};
    ($this:ident, $buf:expr, $len:expr) => {{
        $this.buf[$this.pos..$this.pos + $len].copy_from_slice($buf);
        $this.pos += $len;
    }};
}

#[derive(Debug)]
pub struct BucketSegmentWriter {
    path: PathBuf,
    file: File,
    buf: Box<[u8]>,
    pos: usize,
    file_size: u64,
    flushed_offset: Arc<AtomicU64>,
    flusher_tx: FlushSender,
}

impl BucketSegmentWriter {
    /// Creates a new segment for writing.
    pub fn create(
        path: impl Into<PathBuf>,
        bucket_id: u16,
        flusher_tx: FlushSender,
    ) -> io::Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let buf = vec![0u8; WRITE_BUF_SIZE].into_boxed_slice();
        let pos = 0;
        let file_size = 0;
        let flushed_offset = Arc::new(AtomicU64::new(file_size));

        let mut writer = BucketSegmentWriter {
            path,
            file,
            buf,
            pos,
            file_size,
            flushed_offset,
            flusher_tx,
        };

        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "time went backwards"))?
            .as_nanos() as u64;
        writer.write_segment_header(&BucketSegmentHeader {
            version: 0,
            bucket_id,
            created_at,
        })?;
        writer.flush()?;

        Ok(writer)
    }

    /// Opens a segment for writing.
    pub fn open(path: impl Into<PathBuf>, flusher_tx: FlushSender) -> io::Result<Self> {
        let path = path.into();
        let mut file = OpenOptions::new().read(false).write(true).open(&path)?;
        let buf = vec![0u8; WRITE_BUF_SIZE].into_boxed_slice();
        let pos = 0;
        let file_size = file.seek(SeekFrom::End(SEGMENT_HEADER_SIZE as i64))?;
        let flushed_offset = Arc::new(AtomicU64::new(file_size));

        Ok(BucketSegmentWriter {
            path,
            file,
            buf,
            pos,
            file_size,
            flushed_offset,
            flusher_tx,
        })
    }

    /// Returns the path of the segment file.
    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the last flushed read only atomic offset.
    ///
    /// Any content before this value at any given time is immutable and safe to be read concurrently.
    #[inline]
    pub fn flushed_offset(&self) -> FlushedOffset {
        FlushedOffset::new(Arc::clone(&self.flushed_offset))
    }

    /// Writes the segment's header.
    pub fn write_segment_header(&mut self, header: &BucketSegmentHeader) -> io::Result<()> {
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        let mut pos = 0;

        for field in [
            MAGIC_BYTES.to_le_bytes().as_slice(),
            header.version.to_le_bytes().as_slice(),
            header.bucket_id.to_le_bytes().as_slice(),
            header.created_at.to_le_bytes().as_slice(),
            [0u8; PADDING_SIZE].as_slice(),
        ] {
            buf[pos..pos + field.len()].copy_from_slice(field);
            pos += field.len();
        }

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&buf)?;
        self.file_size = self.file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    /// Appends an event to the end of the segment.
    pub fn append_event(
        &mut self,
        AppendEvent {
            header,
            body,
            crc32c,
        }: AppendEvent,
    ) -> io::Result<(u64, usize)> {
        let offset = self.file_size + self.pos as u64;
        let encoded_timestamp = header.timestamp & !0b1;

        let record_len = EVENT_HEADER_SIZE + body.len();
        if self.pos + EVENT_HEADER_SIZE > self.buf.len() {
            self.flush_buf()?;
        }

        write_bytes!(
            self,
            [
                header.event_id.as_bytes() => 16,
                header.transaction_id.as_bytes() => 16,
                &encoded_timestamp.to_le_bytes() => 8,
                &crc32c.to_le_bytes() => 4,
                &header.stream_version.to_le_bytes() => 8,
                header.correlation_id.as_bytes() => 16,
                &header.stream_id_len.to_le_bytes() => 2,
                &header.event_name_len.to_le_bytes() => 2,
                &header.metadata_len.to_le_bytes() => 4,
                &header.payload_len.to_le_bytes() => 4,
            ]
        );

        for field in [body.stream_id, body.event_name, body.metadata, body.payload] {
            if self.pos + field.len() > self.buf.len() {
                // The field is too long, lets flush the buffer and write directly to the file
                self.flush_buf()?;
                self.file.write_all(field)?;
                self.file_size += field.len() as u64;
            } else {
                write_bytes!(self, field, field.len());
            }
        }

        if self.pos > self.buf.len() / 2 {
            // Flush if the buffer is at least half filled
            self.flush_buf()?;
        }

        Ok((offset, record_len))
    }

    /// Appends a commit to the end of the segment.
    pub fn append_commit(
        &mut self,
        transaction_id: &Uuid,
        timestamp: u64,
        event_count: u32,
    ) -> io::Result<(u64, usize)> {
        let offset = self.file_size + self.pos as u64;
        let event_id = Uuid::nil(); // Always a zero UUID for commits
        let encoded_timestamp = timestamp | 0b1;

        let crc32c = calculate_commit_crc32c(transaction_id, timestamp, event_count);

        if self.pos + COMMIT_SIZE > self.buf.len() {
            self.flush_buf()?;
        }

        write_bytes!(
            self,
            [
                event_id.as_bytes() => 16,
                transaction_id.as_bytes() => 16,
                &encoded_timestamp.to_le_bytes() => 8,
                &crc32c.to_le_bytes() => 4,
                &event_count.to_le_bytes() => 4,
            ]
        );

        if self.pos > self.buf.len() / 2 {
            // Flush if the buffer is at least half filled
            self.flush_buf()?;
        }

        Ok((offset, COMMIT_SIZE))
    }

    /// Flushes the segment, ensuring all data is persisted to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        self.file.flush()?;
        self.flushed_offset.store(self.file_size, Ordering::Release);
        Ok(())
    }

    /// Asynchronously flushes data, ensuring all data is persisted to disk.
    ///
    /// If the `FlushSender` is not local, then the flush will occur in a background thread, and will not be persisted
    /// when the function returns.
    pub fn flush_async(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        self.flusher_tx
            .flush_async(&mut self.file, self.file_size, &self.flushed_offset)
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        if self.pos > 0 {
            let pos = self.pos;
            self.file.write_all(&self.buf[..pos])?;
            self.pos = 0;
            self.file_size += pos as u64;
        }

        Ok(())
    }
}

pub struct AppendEvent<'a> {
    header: AppendEventHeader<'a>,
    body: AppendEventBody<'a>,
    crc32c: u32,
}

pub struct AppendEventHeader<'a> {
    pub event_id: &'a Uuid,
    pub correlation_id: &'a Uuid,
    pub transaction_id: &'a Uuid,
    pub stream_version: u64,
    pub timestamp: u64,
    pub stream_id_len: u16,
    pub event_name_len: u16,
    pub metadata_len: u32,
    pub payload_len: u32,
}

impl AppendEventHeader<'_> {
    pub fn validate_id(&self) -> bool {
        crate::id::validate_event_id(self.event_id, self.correlation_id)
    }
}

pub struct AppendEventBody<'a> {
    pub stream_id: &'a [u8],
    pub event_name: &'a [u8],
    pub metadata: &'a [u8],
    pub payload: &'a [u8],
}

impl<'a> AppendEventBody<'a> {
    #[must_use]
    pub fn len(&self) -> usize {
        self.stream_id.len() + self.event_name.len() + self.metadata.len() + self.payload.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> AppendEvent<'a> {
    #[must_use]
    pub fn new(header: AppendEventHeader<'a>, body: AppendEventBody<'a>) -> AppendEvent<'a> {
        let crc32c = calculate_event_crc32c(
            header.event_id,
            header.transaction_id,
            header.timestamp,
            header.stream_version,
            header.correlation_id,
            body.stream_id,
            body.event_name,
            body.metadata,
            body.payload,
        );

        AppendEvent {
            header,
            body,
            crc32c,
        }
    }
}
