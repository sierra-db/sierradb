use std::fs;
use std::os::unix::fs::FileExt;
use std::path::Path;

use seglog::FlushedOffset;
use uuid::Uuid;

use super::BucketSegmentHeader;
use crate::bucket::segment::format::{RawCommit, RawEvent};
use crate::bucket::segment::{BINCODE_CONFIG, SEGMENT_HEADER_SIZE};
use crate::bucket::{BucketId, BucketSegmentId, SegmentId, SegmentKind};
use crate::error::WriteError;

#[derive(Debug)]
pub struct BucketSegmentWriter {
    writer: seglog::write::Writer,
}

impl BucketSegmentWriter {
    /// Creates a new segment for writing.
    pub fn create(
        path: impl AsRef<Path>,
        bucket_id: BucketId,
        segment_size: usize,
    ) -> Result<Self, WriteError> {
        let writer = seglog::write::Writer::create(path, segment_size, SEGMENT_HEADER_SIZE as u64)?;

        let header_bytes =
            bincode::encode_to_vec(BucketSegmentHeader::new(bucket_id)?, BINCODE_CONFIG)?;
        writer.file().write_all_at(&header_bytes, 0)?;
        writer.file().sync_data()?;

        Ok(BucketSegmentWriter { writer })
    }

    /// Opens a segment for writing.
    pub fn open(path: impl AsRef<Path>, segment_size: usize) -> Result<Self, WriteError> {
        let writer = seglog::write::Writer::open(path, segment_size, SEGMENT_HEADER_SIZE as u64)?;
        Ok(BucketSegmentWriter { writer })
    }

    pub fn latest(
        bucket_id: BucketId,
        dir: impl AsRef<Path>,
        segment_size: usize,
    ) -> Result<(BucketSegmentId, Self), WriteError> {
        let dir = dir.as_ref();
        let bucket_dir = dir.join("buckets").join(format!("{bucket_id:05}"));
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
                .join(format!("{segment_id:010}"))
                .join(SegmentKind::Events.file_name());

            Self::open(events_path, segment_size).map(|writer| (bucket_segment_id, writer))
        } else {
            // Create a new segment with ID 0
            let bucket_segment_id = BucketSegmentId::new(bucket_id, 0);

            // Ensure the segment directory exists
            let segment_dir = segments_dir.join(format!("{:010}", 0));
            fs::create_dir_all(&segment_dir)?;

            let events_path = segment_dir.join(SegmentKind::Events.file_name());
            Self::create(events_path, bucket_id, segment_size)
                .map(|writer| (bucket_segment_id, writer))
        }
    }

    /// Returns the last flushed read only atomic offset.
    ///
    /// Any content before this value at any given time is immutable and safe to
    /// be read concurrently.
    #[inline]
    pub fn flushed_offset(&self) -> FlushedOffset {
        self.writer.flushed_offset()
    }

    /// Appends an event to the end of the segment.
    pub fn append_event(&mut self, event: &RawEvent) -> Result<(u64, usize), WriteError> {
        let bytes = bincode::encode_to_vec(event, BINCODE_CONFIG)?;
        let (offset, len) = self.writer.append(&bytes)?;
        Ok((offset, len))
    }

    /// Appends a commit to the end of the segment.
    pub fn append_commit(&mut self, commit: &RawCommit) -> Result<(u64, usize), WriteError> {
        let bytes = bincode::encode_to_vec(commit, BINCODE_CONFIG)?;
        let (offset, len) = self.writer.append(&bytes)?;
        Ok((offset, len))
    }

    pub fn set_confirmations(
        &self,
        offset: u64,
        transaction_id: &Uuid,
        confirmation_count: u8,
    ) -> Result<(), WriteError> {
        if offset > self.writer.write_offset() {
            return Err(WriteError::OffsetExceedsFileSize {
                offset,
                size: self.writer.write_offset(),
            });
        }
        super::set_confirmations(
            self.writer.file(),
            offset,
            transaction_id,
            confirmation_count,
        )
    }

    pub fn write_offset(&self) -> u64 {
        self.writer.write_offset()
    }

    pub fn set_len(&mut self, offset: u64) -> Result<(), WriteError> {
        self.writer.set_len(offset)?;
        Ok(())
    }

    pub fn flush_writer(&mut self) -> Result<(), WriteError> {
        self.writer.flush_writer()?;
        Ok(())
    }

    /// Flushes the segment, ensuring all data is persisted to disk.
    pub fn sync(&mut self) -> Result<u64, WriteError> {
        let write_offset = self.writer.sync()?;
        Ok(write_offset)
    }
}
