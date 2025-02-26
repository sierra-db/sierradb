use std::{
    fs::{self, File, OpenOptions},
    io::{self, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use tracing::trace;
use uuid::Uuid;

use crate::{
    bucket::{BucketId, BucketSegmentId, SegmentId, SegmentKind, file_name},
    copy_bytes,
};

use super::{
    BucketSegmentHeader, COMMIT_SIZE, EVENT_HEADER_SIZE, FlushSender, FlushedOffset, MAGIC_BYTES,
    PADDING_SIZE, SEGMENT_HEADER_SIZE, calculate_commit_crc32c, calculate_event_crc32c,
};

const WRITE_BUF_SIZE: usize = 16 * 1024; // 16 KB buffer

#[derive(Debug)]
pub struct BucketSegmentWriter {
    file: File,
    buf: Box<[u8]>,
    pos: usize,
    file_size: u64,
    flushed_offset: Arc<AtomicU64>,
    flush_tx: FlushSender,
    dirty: bool,
}

impl BucketSegmentWriter {
    /// Creates a new segment for writing.
    pub fn create(
        path: impl AsRef<Path>,
        bucket_id: BucketId,
        flush_tx: FlushSender,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(path)?;
        let buf = vec![0u8; WRITE_BUF_SIZE].into_boxed_slice();
        let pos = 0;
        let file_size = 0;
        let flushed_offset = Arc::new(AtomicU64::new(file_size));

        let mut writer = BucketSegmentWriter {
            file,
            buf,
            pos,
            file_size,
            flushed_offset,
            flush_tx,
            dirty: false,
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
    pub fn open(path: impl AsRef<Path>, flush_tx: FlushSender) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(false).write(true).open(path)?;
        let buf = vec![0u8; WRITE_BUF_SIZE].into_boxed_slice();
        let pos = 0;
        let file_size = file.seek(SeekFrom::End(0))?;
        let flushed_offset = Arc::new(AtomicU64::new(file_size));

        Ok(BucketSegmentWriter {
            file,
            buf,
            pos,
            file_size,
            flushed_offset,
            flush_tx,
            dirty: false,
        })
    }

    pub fn latest(
        bucket_id: BucketId,
        dir: impl AsRef<Path>,
        flush_tx: FlushSender,
    ) -> io::Result<(BucketSegmentId, Self)> {
        let dir = dir.as_ref();
        let prefix = format!("{:05}-", bucket_id);
        let suffix = format!(".{}.dat", SegmentKind::Events);
        let mut latest_segment: Option<(SegmentId, PathBuf)> = None;

        // Iterate through the directory and look for matching segment files.
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // Check if the file name starts with the bucket prefix and ends with the events file suffix.
            if file_name_str.starts_with(&prefix) && file_name_str.ends_with(&suffix) {
                // Extract the segment id from the file name.
                // File format: {bucket_id:05}-{segment_id:010}.events.dat
                let start = prefix.len();
                let end = file_name_str.len() - suffix.len();
                if let Ok(segment_id) = file_name_str[start..end].parse::<u32>() {
                    latest_segment = match latest_segment {
                        Some((current_max, _)) if segment_id > current_max => {
                            Some((segment_id, entry.path()))
                        }
                        None => Some((segment_id, entry.path())),
                        _ => latest_segment,
                    };
                }
            }
        }

        // If we found an existing segment, open it for writing (append mode).
        if let Some((segment_id, path)) = latest_segment {
            Self::open(path, flush_tx)
                .map(|writer| (BucketSegmentId::new(bucket_id, segment_id), writer))
        } else {
            let bucket_segment_id = BucketSegmentId::new(bucket_id, 0);
            let new_file_name = file_name(bucket_segment_id, SegmentKind::Events);
            let path = dir.join(new_file_name);
            Self::create(path, bucket_id, flush_tx).map(|writer| (bucket_segment_id, writer))
        }
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
        self.dirty = true;

        Ok(())
    }

    /// Appends an event to the end of the segment.
    pub fn append_event(
        &mut self,
        AppendEvent {
            header,
            body,
            crc32c,
        }: AppendEvent<'_>,
    ) -> io::Result<(u64, usize)> {
        let offset = self.file_size + self.pos as u64;
        let encoded_timestamp = header.timestamp & !0b1;

        let record_len = EVENT_HEADER_SIZE + body.len();
        if self.pos + EVENT_HEADER_SIZE > self.buf.len() {
            self.flush_buf()?;
        }

        copy_bytes!(
            self.buf,
            self.pos,
            [
                header.event_id.as_bytes() => 16,
                header.transaction_id.as_bytes() => 16,
                &encoded_timestamp.to_le_bytes() => 8,
                &crc32c.to_le_bytes() => 4,
                &header.stream_version.to_le_bytes() => 8,
                header.partition_key.as_bytes() => 16,
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
                copy_bytes!(self.buf, self.pos, field);
            }
        }

        if self.pos > self.buf.len() / 2 {
            // Flush if the buffer is at least half filled
            self.flush_buf()?;
        }

        self.dirty = true;

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

        copy_bytes!(
            self.buf,
            self.pos,
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

        self.dirty = true;

        Ok((offset, COMMIT_SIZE))
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn buf_len(&self) -> usize {
        self.pos
    }

    pub fn set_len(&mut self, offset: u64) -> io::Result<()> {
        if offset < self.file_size {
            self.file.set_len(offset)?;
        }
        Ok(())
    }

    /// Flushes the segment, ensuring all data is persisted to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.dirty {
            trace!("flushing writer");
            self.flush_buf()?;
            self.file.flush()?;
            self.flushed_offset.store(self.file_size, Ordering::Release);
        }
        Ok(())
    }

    /// Asynchronously flushes data, ensuring all data is persisted to disk.
    ///
    /// If the `FlushSender` is not local, then the flush will occur in a background thread, and will not be persisted
    /// when the function returns.
    pub fn flush_async(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        self.flush_tx
            .flush_async(&mut self.file, self.file_size, &self.flushed_offset)
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        if self.pos > 0 {
            let pos = self.pos;
            self.file.write_all(&self.buf[..pos])?;
            self.pos = 0;
            self.file_size += pos as u64;
            self.dirty = true;
        }

        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct AppendEvent<'a> {
    header: AppendEventHeader<'a>,
    body: AppendEventBody<'a>,
    crc32c: u32,
}

#[derive(Clone, Copy)]
pub struct AppendEventHeader<'a> {
    event_id: &'a Uuid,
    partition_key: &'a Uuid,
    transaction_id: &'a Uuid,
    stream_version: u64,
    timestamp: u64,
    stream_id_len: u16,
    event_name_len: u16,
    metadata_len: u32,
    payload_len: u32,
}

impl<'a> AppendEventHeader<'a> {
    pub fn new(
        event_id: &'a Uuid,
        partition_key: &'a Uuid,
        transaction_id: &'a Uuid,
        stream_version: u64,
        timestamp: u64,
        body: AppendEventBody<'a>,
    ) -> io::Result<Self> {
        Ok(AppendEventHeader {
            event_id,
            partition_key,
            transaction_id,
            stream_version,
            timestamp,
            stream_id_len: body
                .stream_id
                .len()
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "stream id too long"))?,
            event_name_len: body
                .event_name
                .len()
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "event id too long"))?,
            metadata_len: body
                .metadata
                .len()
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "metadata too big"))?,
            payload_len: body
                .payload
                .len()
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "payload too big"))?,
        })
    }
}

#[derive(Clone, Copy)]
pub struct AppendEventBody<'a> {
    stream_id: &'a [u8],
    event_name: &'a [u8],
    metadata: &'a [u8],
    payload: &'a [u8],
}

impl<'a> AppendEventBody<'a> {
    pub fn new(
        stream_id: &'a str,
        event_name: &'a str,
        metadata: &'a [u8],
        payload: &'a [u8],
    ) -> Self {
        AppendEventBody {
            stream_id: stream_id.as_bytes(),
            event_name: event_name.as_bytes(),
            metadata,
            payload,
        }
    }

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
            header.partition_key,
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
