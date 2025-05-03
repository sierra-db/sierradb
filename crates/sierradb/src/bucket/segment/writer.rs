use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bincode::Encode;
use bincode::enc::Encoder;
use bincode::enc::write::Writer;
use bincode::error::EncodeError;
use tracing::trace;
use uuid::Uuid;

use super::{
    BucketSegmentHeader, FlushedOffset, MAGIC_BYTES, PADDING_SIZE, SEGMENT_HEADER_SIZE,
    calculate_commit_crc32c, calculate_confirmation_count_crc32c, calculate_event_crc32c,
};
use crate::StreamId;
use crate::bucket::segment::BINCODE_CONFIG;
use crate::bucket::{BucketId, BucketSegmentId, PartitionId, SegmentId, SegmentKind};
use crate::error::WriteError;

const WRITE_BUF_SIZE: usize = 16 * 1024; // 16 KB buffer

#[derive(Debug)]
pub struct BucketSegmentWriter {
    writer: BufWriter<File>,
    file_size: u64,
    flushed_offset: Arc<AtomicU64>,
    dirty: bool,
}

impl BucketSegmentWriter {
    /// Creates a new segment for writing.
    pub fn create(path: impl AsRef<Path>, bucket_id: BucketId) -> Result<Self, WriteError> {
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(path)?;
        let file_size = 0;
        let flushed_offset = Arc::new(AtomicU64::new(file_size));

        let mut writer = BucketSegmentWriter {
            writer: BufWriter::with_capacity(WRITE_BUF_SIZE, file),
            file_size,
            flushed_offset,
            dirty: false,
        };

        let created_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
        writer.write_segment_header(&BucketSegmentHeader {
            version: 0,
            bucket_id,
            created_at,
        })?;
        writer.flush()?;

        Ok(writer)
    }

    /// Opens a segment for writing.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WriteError> {
        let mut file = OpenOptions::new().read(false).write(true).open(path)?;
        let file_size = file.seek(SeekFrom::End(0))?;
        let flushed_offset = Arc::new(AtomicU64::new(file_size));

        Ok(BucketSegmentWriter {
            writer: BufWriter::with_capacity(WRITE_BUF_SIZE, file),
            file_size,
            flushed_offset,
            dirty: false,
        })
    }

    pub fn latest(
        bucket_id: BucketId,
        dir: impl AsRef<Path>,
    ) -> Result<(BucketSegmentId, Self), WriteError> {
        let dir = dir.as_ref();
        let bucket_dir = dir.join("buckets").join(format!("{:05}", bucket_id));
        let segments_dir = bucket_dir.join("segments");

        // Create the directories if they don't exist
        if !segments_dir.exists() {
            fs::create_dir_all(&segments_dir)?;
        }

        let mut latest_segment_id: Option<SegmentId> = None;

        // Iterate through segment directories to find the latest one
        if segments_dir.exists() {
            for entry in fs::read_dir(&segments_dir)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }

                let segment_dir_name = entry.file_name();
                let segment_dir_str = segment_dir_name.to_string_lossy();

                // Try to parse the segment ID from the directory name
                if let Ok(segment_id) = segment_dir_str.parse::<u32>() {
                    // Check if this segment has an events file
                    let events_file = entry.path().join(SegmentKind::Events.file_name());
                    if events_file.exists() {
                        latest_segment_id = match latest_segment_id {
                            Some(current_max) if segment_id > current_max => Some(segment_id),
                            None => Some(segment_id),
                            _ => latest_segment_id,
                        };
                    }
                }
            }
        }

        // If we found an existing segment, open it for writing (append mode)
        if let Some(segment_id) = latest_segment_id {
            let bucket_segment_id = BucketSegmentId::new(bucket_id, segment_id);
            let events_path = segments_dir
                .join(format!("{:010}", segment_id))
                .join(SegmentKind::Events.file_name());

            Self::open(events_path).map(|writer| (bucket_segment_id, writer))
        } else {
            // Create a new segment with ID 0
            let bucket_segment_id = BucketSegmentId::new(bucket_id, 0);

            // Ensure the segment directory exists
            let segment_dir = segments_dir.join(format!("{:010}", 0));
            fs::create_dir_all(&segment_dir)?;

            let events_path = segment_dir.join(SegmentKind::Events.file_name());
            Self::create(events_path, bucket_id).map(|writer| (bucket_segment_id, writer))
        }
    }

    /// Returns the last flushed read only atomic offset.
    ///
    /// Any content before this value at any given time is immutable and safe to
    /// be read concurrently.
    #[inline]
    pub fn flushed_offset(&self) -> FlushedOffset {
        FlushedOffset::new(Arc::clone(&self.flushed_offset))
    }

    /// Writes the segment's header.
    pub fn write_segment_header(&mut self, header: &BucketSegmentHeader) -> Result<(), WriteError> {
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

        self.writer.seek(SeekFrom::Start(0))?;
        self.writer.write_all(&buf)?;
        self.writer.seek(SeekFrom::End(0))?;
        self.file_size = self.writer.stream_position()?;
        assert_eq!(self.file_size, SEGMENT_HEADER_SIZE as u64);
        self.dirty = true;

        Ok(())
    }

    /// Appends an event to the end of the segment.
    pub fn append_event(
        &mut self,
        transaction_id: &Uuid,
        timestamp: u64,
        confirmation_count: u8,
        event: AppendEvent<'_>,
    ) -> Result<(u64, usize), WriteError> {
        let offset = self.writer.stream_position()? + self.writer.buffer().len() as u64;

        let len = bincode::encode_into_std_write(
            AppendRecord {
                timestamp,
                transaction_id,
                confirmation_count,
                record_kind: RecordKind::Event(event),
            },
            &mut self.writer,
            BINCODE_CONFIG,
        )?;

        self.dirty = true;

        Ok((offset, len))
    }

    /// Appends a commit to the end of the segment.
    pub fn append_commit(
        &mut self,
        transaction_id: &Uuid,
        timestamp: u64,
        event_count: u32,
        confirmation_count: u8,
    ) -> Result<(u64, usize), WriteError> {
        let offset = self.writer.stream_position()? + self.writer.buffer().len() as u64;

        let len = bincode::encode_into_std_write(
            AppendRecord {
                timestamp,
                transaction_id,
                record_kind: RecordKind::Commit { event_count },
                confirmation_count,
            },
            &mut self.writer,
            BINCODE_CONFIG,
        )?;

        self.dirty = true;

        Ok((offset, len))
    }

    pub fn set_confirmations(
        &self,
        offset: u64,
        transaction_id: &Uuid,
        confirmation_count: u8,
    ) -> Result<(), WriteError> {
        if offset > self.file_size {
            return Err(WriteError::OffsetExceedsFileSize {
                offset,
                size: self.file_size,
            });
        }
        super::set_confirmations(
            self.writer.get_ref(),
            offset,
            transaction_id,
            confirmation_count,
        )
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn buf_len(&self) -> usize {
        self.writer.buffer().len()
    }

    pub fn set_len(&mut self, offset: u64) -> Result<(), WriteError> {
        if offset < self.file_size {
            self.writer.get_ref().set_len(offset)?;
            self.file_size = self.writer.stream_position()?;
        }

        Ok(())
    }

    /// Flushes the segment, ensuring all data is persisted to disk.
    pub fn flush(&mut self) -> Result<(), WriteError> {
        if self.dirty {
            trace!("flushing writer");
            self.writer.flush()?;
            self.file_size = self.writer.stream_position()?;
            self.flushed_offset.store(self.file_size, Ordering::Release);
            self.dirty = false;
        }

        Ok(())
    }
}

struct AppendRecord<'a> {
    timestamp: u64,
    transaction_id: &'a Uuid,
    confirmation_count: u8,
    record_kind: RecordKind<'a>,
}

impl<'a> Encode for AppendRecord<'a> {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        if (self.timestamp >> 63) == 1 {
            return Err(EncodeError::Other("timestamp must be less than 2^63"));
        }

        // Record Kind + Timestamp (8 bytes)
        let encoded_timestamp = match &self.record_kind {
            RecordKind::Event { .. } => self.timestamp & !(1u64 << 63),
            RecordKind::Commit { .. } => self.timestamp | (1u64 << 63),
        };
        encoded_timestamp.encode(encoder)?;

        // Transaction ID (16 bytes)
        self.transaction_id.as_bytes().encode(encoder)?;

        // Main data CRC32C (4 bytes)
        let crc32c = match &self.record_kind {
            RecordKind::Event(AppendEvent {
                event_id,
                partition_key,
                partition_id,
                partition_sequence,
                stream_version,
                stream_id,
                event_name,
                metadata,
                payload,
            }) => calculate_event_crc32c(
                self.timestamp,
                self.transaction_id,
                event_id,
                partition_key,
                *partition_id,
                *partition_sequence,
                *stream_version,
                stream_id,
                event_name,
                metadata,
                payload,
            ),
            RecordKind::Commit { event_count } => {
                calculate_commit_crc32c(self.transaction_id, self.timestamp, *event_count)
            }
        };
        crc32c.encode(encoder)?;

        // Confirmation Count (1 byte)
        self.confirmation_count.encode(encoder)?;

        // Confirmation Count CRC32C (4 bytes)
        let confirmation_count_crc32c =
            calculate_confirmation_count_crc32c(self.transaction_id, self.confirmation_count);
        confirmation_count_crc32c.encode(encoder)?;

        //  Payload fields (event-specific or commit-specific data)
        match &self.record_kind {
            RecordKind::Event(AppendEvent {
                event_id,
                partition_key,
                partition_id,
                partition_sequence,
                stream_version,
                stream_id,
                event_name,
                metadata,
                payload,
            }) => {
                event_id.as_bytes().encode(encoder)?;
                partition_key.as_bytes().encode(encoder)?;
                partition_id.encode(encoder)?;
                partition_sequence.encode(encoder)?;
                stream_version.encode(encoder)?;
                u8::try_from(stream_id.len())
                    .map_err(|_| {
                        EncodeError::OtherString(format!(
                            "stream id too long: length is {} characters, maximum allowed is {}",
                            stream_id.len(),
                            u8::MAX
                        ))
                    })?
                    .encode(encoder)?;
                u8::try_from(event_name.len())
                    .map_err(|_| {
                        EncodeError::OtherString(format!(
                            "event name too long: length is {} characters, maximum allowed is {}",
                            event_name.len(),
                            u8::MAX
                        ))
                    })?
                    .encode(encoder)?;
                u32::try_from(metadata.len())
                    .map_err(|_| {
                        EncodeError::OtherString(format!(
                            "metadata too long: length is {}, maximum allowed is {}",
                            metadata.len(),
                            u32::MAX
                        ))
                    })?
                    .encode(encoder)?;
                u32::try_from(payload.len())
                    .map_err(|_| {
                        EncodeError::OtherString(format!(
                            "payload too long: length is {}, maximum allowed is {}",
                            payload.len(),
                            u32::MAX
                        ))
                    })?
                    .encode(encoder)?;
                encoder.writer().write(stream_id.as_bytes())?;
                encoder.writer().write(event_name.as_bytes())?;
                encoder.writer().write(metadata)?;
                encoder.writer().write(payload)?;
            }
            RecordKind::Commit { event_count } => {
                event_count.encode(encoder)?;
            }
        }

        Ok(())
    }
}

pub struct AppendEvent<'a> {
    pub event_id: &'a Uuid,
    pub partition_key: &'a Uuid,
    pub partition_id: PartitionId,
    pub partition_sequence: u64,
    pub stream_version: u64,
    pub stream_id: &'a StreamId,
    pub event_name: &'a str,
    pub metadata: &'a [u8],
    pub payload: &'a [u8],
}

enum RecordKind<'a> {
    Event(AppendEvent<'a>),
    Commit { event_count: u32 },
}
