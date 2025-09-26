use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::{mem, option, vec};

use bincode::Decode;
use bincode::de::Decoder;
use bincode::de::read::Reader;
use bincode::error::DecodeError;
use polonius_the_crab::{exit_polonius, polonius, polonius_return, polonius_try};
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};
use tracing::{info, trace, warn};
use uuid::Uuid;

use super::{
    BINCODE_CONFIG, BUCKET_ID_SIZE, BucketSegmentHeader, COMMIT_SIZE, CREATED_AT_SIZE,
    EVENT_HEADER_SIZE, FlushedOffset, MAGIC_BYTES, MAGIC_BYTES_SIZE, RECORD_HEADER_SIZE,
    SEGMENT_HEADER_SIZE, VERSION_SIZE, calculate_commit_crc32c,
    calculate_confirmation_count_crc32c, calculate_event_crc32c,
};
use crate::bucket::{BucketId, PartitionId};
use crate::error::{ReadError, WriteError};
use crate::id::{get_uuid_flag, uuid_to_partition_hash};
use crate::{MAX_REPLICATION_FACTOR, STREAM_ID_SIZE, StreamId};

const HEADER_BUF_SIZE: usize = EVENT_HEADER_SIZE - RECORD_HEADER_SIZE;
const PAGE_SIZE: usize = 4096; // Usually a page is 4KB on Linux
const READ_AHEAD_SIZE: usize = 64 * 1024; // 64 KB read ahead buffer
const READ_BUF_SIZE: usize = PAGE_SIZE - COMMIT_SIZE;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommittedEvents {
    Single(EventRecord),
    Transaction {
        events: Box<SmallVec<[EventRecord; 4]>>,
        commit: CommitRecord,
    },
}

impl CommittedEvents {
    pub fn confirmation_count(&self) -> u8 {
        match self {
            CommittedEvents::Single(event) => event.confirmation_count,
            CommittedEvents::Transaction { commit, .. } => commit.confirmation_count,
        }
    }

    pub fn transaction_id(&self) -> &Uuid {
        match self {
            CommittedEvents::Single(event) => &event.transaction_id,
            CommittedEvents::Transaction { commit, .. } => &commit.transaction_id,
        }
    }

    pub fn first_partition_sequence(&self) -> Option<u64> {
        match self {
            CommittedEvents::Single(event) => Some(event.partition_sequence),
            CommittedEvents::Transaction { events, .. } => {
                events.first().map(|event| event.partition_sequence)
            }
        }
    }

    pub fn last_partition_sequence(&self) -> Option<u64> {
        match self {
            CommittedEvents::Single(event) => Some(event.partition_sequence),
            CommittedEvents::Transaction { events, .. } => {
                events.last().map(|event| event.partition_sequence)
            }
        }
    }

    pub fn first(&self) -> Option<&EventRecord> {
        match self {
            CommittedEvents::Single(event) => Some(event),
            CommittedEvents::Transaction { events, .. } => events.first(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            CommittedEvents::Single(_) => 1,
            CommittedEvents::Transaction { events, .. } => events.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            CommittedEvents::Single(_) => false,
            CommittedEvents::Transaction { events, .. } => events.is_empty(),
        }
    }
}

impl IntoIterator for CommittedEvents {
    type IntoIter = CommittedEventsIntoIter;
    type Item = EventRecord;

    fn into_iter(self) -> Self::IntoIter {
        let inner = match self {
            CommittedEvents::Single(event) => {
                CommittedEventsIntoIterInner::Single(Some(event).into_iter())
            }
            CommittedEvents::Transaction { events, .. } => {
                CommittedEventsIntoIterInner::Transaction(Box::new(events.into_iter()))
            }
        };
        CommittedEventsIntoIter { inner }
    }
}

pub struct CommittedEventsIntoIter {
    inner: CommittedEventsIntoIterInner,
}

enum CommittedEventsIntoIterInner {
    Single(option::IntoIter<EventRecord>),
    Transaction(Box<smallvec::IntoIter<[EventRecord; 4]>>),
}

impl Iterator for CommittedEventsIntoIter {
    type Item = EventRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            CommittedEventsIntoIterInner::Single(iter) => iter.next(),
            CommittedEventsIntoIterInner::Transaction(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.inner {
            CommittedEventsIntoIterInner::Single(iter) => iter.size_hint(),
            CommittedEventsIntoIterInner::Transaction(iter) => iter.size_hint(),
        }
    }
}

impl DoubleEndedIterator for CommittedEventsIntoIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            CommittedEventsIntoIterInner::Single(iter) => iter.next_back(),
            CommittedEventsIntoIterInner::Transaction(iter) => iter.next_back(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadHint {
    Random,
    Sequential,
}

pub struct BucketSegmentReader {
    file: File,
    header_buf: [u8; HEADER_BUF_SIZE],
    body_buf: [u8; READ_BUF_SIZE],
    flushed_offset: FlushedOffset,

    // Read-ahead buffer for sequential reads
    read_ahead_buf: Vec<u8>,
    read_ahead_offset: u64, // File offset of the buffer start
    read_ahead_pos: usize,  // Current read position in buffer
    read_ahead_valid_len: usize,
}

impl BucketSegmentReader {
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
        let header_buf = [0u8; HEADER_BUF_SIZE];
        let body_buf = [0u8; READ_BUF_SIZE];

        let flushed_offset = match flushed_offset {
            Some(flushed_offset) => flushed_offset,
            None => {
                let len = file.metadata()?.len();
                FlushedOffset::new(Arc::new(AtomicU64::new(len)))
            }
        };

        Ok(BucketSegmentReader {
            file,
            header_buf,
            body_buf,
            flushed_offset,
            read_ahead_buf: Vec::new(),
            read_ahead_offset: 0,
            read_ahead_pos: 0,
            read_ahead_valid_len: 0,
        })
    }

    pub fn try_clone(&self) -> Result<Self, ReadError> {
        Ok(BucketSegmentReader {
            file: self.file.try_clone()?,
            header_buf: self.header_buf,
            body_buf: self.body_buf,
            flushed_offset: self.flushed_offset.clone(),
            read_ahead_buf: Vec::with_capacity(READ_AHEAD_SIZE),
            read_ahead_offset: self.read_ahead_offset,
            read_ahead_pos: self.read_ahead_pos,
            read_ahead_valid_len: self.read_ahead_valid_len,
        })
    }

    /// Reads the segments header.
    pub fn read_segment_header(&mut self) -> Result<BucketSegmentHeader, ReadError> {
        let mut header_bytes = [0u8; VERSION_SIZE + BUCKET_ID_SIZE + CREATED_AT_SIZE];

        self.file.seek(SeekFrom::Start(MAGIC_BYTES_SIZE as u64))?;
        self.file.read_exact(&mut header_bytes)?;

        let version_bytes = header_bytes[0..VERSION_SIZE].try_into().unwrap();
        let version = u16::from_le_bytes(version_bytes);

        let bucket_id_bytes = header_bytes[VERSION_SIZE..VERSION_SIZE + BUCKET_ID_SIZE]
            .try_into()
            .unwrap();
        let bucket_id = BucketId::from_le_bytes(bucket_id_bytes);

        let created_at_bytes = header_bytes
            [VERSION_SIZE + BUCKET_ID_SIZE..VERSION_SIZE + BUCKET_ID_SIZE + CREATED_AT_SIZE]
            .try_into()
            .unwrap();
        let created_at = u64::from_le_bytes(created_at_bytes);

        Ok(BucketSegmentHeader {
            version,
            bucket_id,
            created_at,
        })
    }

    pub fn iter(&mut self) -> BucketSegmentIter<'_> {
        BucketSegmentIter {
            reader: self,
            offset: SEGMENT_HEADER_SIZE as u64,
        }
    }

    pub fn iter_from(&mut self, start_offset: u64) -> BucketSegmentIter<'_> {
        BucketSegmentIter {
            reader: self,
            offset: start_offset,
        }
    }

    #[cfg(all(unix, target_os = "linux"))]
    pub fn prefetch(&self, offset: u64) {
        use std::os::fd::AsRawFd;
        unsafe {
            libc::posix_fadvise(
                self.file.as_raw_fd(),
                offset as i64,
                PAGE_SIZE as i64,
                libc::POSIX_FADV_WILLNEED,
            );
        }
    }

    #[cfg(not(all(unix, target_os = "linux")))]
    pub fn prefetch(&self, _offset: u64) {}

    /// Validates the segments magic bytes.
    pub fn validate_magic_bytes(&mut self) -> Result<bool, ReadError> {
        let mut magic_bytes = [0u8; MAGIC_BYTES_SIZE];

        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut magic_bytes)?;

        Ok(u32::from_le_bytes(magic_bytes) == MAGIC_BYTES)
    }

    /// Reads the segments version.
    pub fn read_version(&mut self) -> Result<u16, ReadError> {
        let mut version_bytes = [0u8; VERSION_SIZE];

        self.file.seek(SeekFrom::Start(MAGIC_BYTES_SIZE as u64))?;
        self.file.read_exact(&mut version_bytes)?;

        Ok(u16::from_le_bytes(version_bytes))
    }

    /// Reads the segments bucket ID.
    pub fn read_bucket_id(&mut self) -> Result<BucketId, ReadError> {
        let mut bucket_id_bytes = [0u8; BUCKET_ID_SIZE];

        self.file
            .seek(SeekFrom::Start((MAGIC_BYTES_SIZE + VERSION_SIZE) as u64))?;
        self.file.read_exact(&mut bucket_id_bytes)?;

        Ok(u16::from_le_bytes(bucket_id_bytes))
    }

    /// Reads the segments created at date.
    pub fn read_created_at(&mut self) -> Result<u64, ReadError> {
        let mut created_at_bytes = [0u8; CREATED_AT_SIZE];

        self.file.seek(SeekFrom::Start(
            (MAGIC_BYTES_SIZE + VERSION_SIZE + BUCKET_ID_SIZE) as u64,
        ))?;
        self.file.read_exact(&mut created_at_bytes)?;

        Ok(u64::from_le_bytes(created_at_bytes))
    }

    pub fn set_confirmations(
        &self,
        offset: u64,
        transaction_id: &Uuid,
        confirmation_count: u8,
    ) -> Result<(), WriteError> {
        super::set_confirmations(&self.file, offset, transaction_id, confirmation_count)
    }

    pub fn read_committed_events(
        &mut self,
        mut offset: u64,
        hint: ReadHint,
    ) -> Result<(Option<CommittedEvents>, Option<u64>), ReadError> {
        let mut this = self;
        let mut events = SmallVec::new();
        let mut pending_transaction_id = Uuid::nil();
        loop {
            (events, offset) = polonius!(|this| -> Result<
                (Option<CommittedEvents>, Option<u64>),
                ReadError,
            > {
                let record = polonius_try!(this.read_record(offset, hint));
                match record {
                    Some(Record::Event(
                        event @ EventRecord {
                            offset,
                            transaction_id,
                            ..
                        },
                    )) => {
                        let next_offset = offset + event.size;

                        if get_uuid_flag(&transaction_id) {
                            // Events with a true transaction id flag are always approved
                            if events.is_empty() {
                                // If its the first event we encountered, then return it alone
                                polonius_return!(Ok((
                                    Some(CommittedEvents::Single(event)),
                                    Some(next_offset),
                                )));
                            }

                            events.push(event);
                        } else if transaction_id != pending_transaction_id {
                            // Unexpected transaction, we'll start a new pending transaction
                            events = smallvec![event];
                            pending_transaction_id = transaction_id;
                        } else {
                            // Event belongs to the transaction
                            events.push(event);
                        }

                        exit_polonius!((events, next_offset))
                    }
                    Some(Record::Commit(commit)) => {
                        let next_offset = commit.offset + COMMIT_SIZE as u64;
                        if commit.transaction_id == pending_transaction_id && !events.is_empty() {
                            polonius_return!(Ok((
                                Some(CommittedEvents::Transaction {
                                    events: Box::new(events),
                                    commit
                                }),
                                Some(next_offset)
                            )));
                        }

                        polonius_return!(Ok((None, Some(next_offset))));
                    }
                    None => polonius_return!(Ok((None, None))),
                }
            });
        }
    }

    /// Reads a record at the given offset, returning either an event or commit.
    ///
    /// If sequential is `true`, the read will be optimized for future
    /// sequential reads. Random reads should have `sequential` set to
    /// `false`.
    ///
    /// Borrowed data will be returned in the record where possible. If the
    /// event length exceeds 4KB, then the event will be read directly from
    /// the file, and the returned `Record` will contain owned data.
    pub fn read_record(
        &mut self,
        start_offset: u64,
        hint: ReadHint,
    ) -> Result<Option<Record>, ReadError> {
        // This is the only check needed. We don't need to check for the event body,
        // since if the offset supports this header read, then the event body would have
        // also been written too for the flush.
        if start_offset + COMMIT_SIZE as u64 > self.flushed_offset.load() {
            return Ok(None);
        }

        let mut offset = start_offset;
        let header_buf = if matches!(hint, ReadHint::Sequential) {
            self.read_from_read_ahead(offset, COMMIT_SIZE)?
        } else {
            self.file
                .read_exact_at(&mut self.header_buf[..COMMIT_SIZE], offset)?;
            self.header_buf.as_slice()
        };
        offset += COMMIT_SIZE as u64;

        let (record_header, _) = bincode::decode_from_slice::<RecordHeader, _>(
            &header_buf[..COMMIT_SIZE],
            BINCODE_CONFIG,
        )?;

        if record_header.record_kind == 0 {
            // Backtrack the event count
            offset -= mem::size_of::<u32>() as u64;

            self.read_event_body(start_offset, record_header, offset, hint)
                .map(|event| Some(Record::Event(event)))
        } else if record_header.record_kind == 1 {
            let event_count = u32::from_le_bytes(
                header_buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + 4]
                    .try_into()
                    .unwrap(),
            );
            Ok(Some(Record::Commit(CommitRecord::from_parts(
                start_offset,
                record_header,
                event_count,
            )?)))
        } else {
            Err(ReadError::UnknownRecordType(record_header.record_kind))
        }
    }

    fn read_event_body(
        &mut self,
        start_offset: u64,
        record_header: RecordHeader,
        mut offset: u64,
        hint: ReadHint,
    ) -> Result<EventRecord, ReadError> {
        let length = EVENT_HEADER_SIZE - RECORD_HEADER_SIZE;
        let header_buf = if matches!(hint, ReadHint::Sequential) {
            self.read_from_read_ahead(offset, length)?
        } else {
            self.file
                .read_exact_at(&mut self.header_buf[..length], offset)?;
            self.header_buf.as_slice()
        };
        offset += length as u64;

        let (event_header, _) =
            bincode::decode_from_slice::<EventHeader, _>(header_buf, BINCODE_CONFIG)?;

        let body_len = event_header.body_len();
        let body = if matches!(hint, ReadHint::Sequential) {
            let body_buf = self.read_from_read_ahead(offset, body_len)?;
            bincode::decode_from_slice_with_context(body_buf, BINCODE_CONFIG, &event_header)?.0
        } else if body_len > self.body_buf.len() {
            let mut body_buf = vec![0u8; body_len];
            self.file.read_exact_at(&mut body_buf, offset)?;
            bincode::decode_from_slice_with_context(&body_buf, BINCODE_CONFIG, &event_header)?.0
        } else {
            self.file
                .read_exact_at(&mut self.body_buf[..body_len], offset)?;
            bincode::decode_from_slice_with_context(&self.body_buf, BINCODE_CONFIG, &event_header)?
                .0
        };

        EventRecord::from_parts(start_offset, record_header, event_header, body)
    }

    fn fill_read_ahead(&mut self, offset: u64, mut length: usize) -> Result<(), ReadError> {
        let end_offset = offset + length as u64;

        // Set the new read-ahead offset aligned to 64KB
        self.read_ahead_offset = offset - (offset % READ_AHEAD_SIZE as u64);
        self.read_ahead_pos = 0;
        length = (end_offset - self.read_ahead_offset) as usize;

        // If the requested read is larger than READ_AHEAD_SIZE, expand the buffer to
        // the next leargest interval of 4096
        let required_size = (length.max(READ_AHEAD_SIZE) + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);

        // Resize buffer if necessary
        if self.read_ahead_buf.len() != required_size {
            self.read_ahead_buf.resize(required_size, 0);
            self.read_ahead_buf.shrink_to_fit();
        }

        let mut total_read = 0;
        while total_read < required_size {
            let bytes_read = self.file.read_at(
                &mut self.read_ahead_buf[total_read..],
                self.read_ahead_offset + total_read as u64,
            )?;
            if bytes_read == 0 {
                break; // EOF reached
            }
            total_read += bytes_read;
        }

        self.read_ahead_valid_len = total_read; // Track the actual valid bytes

        Ok(())
    }

    fn read_from_read_ahead(&mut self, offset: u64, length: usize) -> Result<&[u8], ReadError> {
        let end_offset = offset + length as u64;

        // If offset is within the valid read-ahead range
        if offset >= self.read_ahead_offset
            && end_offset <= (self.read_ahead_offset + self.read_ahead_valid_len as u64)
        {
            let start = (offset - self.read_ahead_offset) as usize;
            return Ok(&self.read_ahead_buf[start..start + length]);
        }

        // Fill the read-ahead buffer for the requested offset & length
        self.fill_read_ahead(offset, length)?;

        // Ensure we now have enough valid data
        if offset < self.read_ahead_offset
            || end_offset > (self.read_ahead_offset + self.read_ahead_valid_len as u64)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "requested data exceeds available read-ahead buffer",
            )
            .into());
        }

        let start = (offset - self.read_ahead_offset) as usize;
        Ok(&self.read_ahead_buf[start..start + length])
    }

    // Recovery related methods
    fn read_u64_at(&mut self, offset: u64) -> Result<u64, ReadError> {
        let mut bytes = [0u8; 8];
        self.file.read_exact_at(&mut bytes, offset)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn try_read_record_header(&mut self, offset: u64) -> Result<RecordHeader, ReadError> {
        // Check if we have enough bytes for at least a record header
        if offset + RECORD_HEADER_SIZE as u64 > self.flushed_offset.load() {
            return Err(ReadError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "requested data exceeds available read-ahead buffer",
            )));
        }

        let mut header_buf = [0u8; RECORD_HEADER_SIZE];
        self.file.read_exact_at(&mut header_buf, offset)?;

        let (record_header, _) =
            bincode::decode_from_slice::<RecordHeader, _>(&header_buf, BINCODE_CONFIG)?;

        Ok(record_header)
    }

    fn peek_bytes_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, ReadError> {
        let mut bytes = vec![0u8; len];
        self.file.read_exact_at(&mut bytes, offset)?;
        Ok(bytes)
    }

    fn try_read_event_header(&mut self, offset: u64) -> Result<EventHeader, ReadError> {
        // Check if we have enough bytes for event header
        let header_size = EVENT_HEADER_SIZE - RECORD_HEADER_SIZE;
        if offset + header_size as u64 > self.flushed_offset.load() {
            return Err(ReadError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "insufficient bytes for event header",
            )));
        }

        let mut header_buf = vec![0u8; header_size];
        self.file.read_exact_at(&mut header_buf, offset)?;

        let (event_header, _) =
            bincode::decode_from_slice::<EventHeader, _>(&header_buf, BINCODE_CONFIG)?;
        Ok(event_header)
    }
}

#[derive(Debug, Clone)]
enum CorruptionPattern {
    /// Valid record found
    ValidRecord { offset: u64, size: u64 },
    /// Zero padding (likely from incomplete write)
    ZeroPadding { start: u64, end: u64 },
    /// Random corruption with non-zero data
    Corruption { start: u64, end: u64 },
    /// End of file reached
    EndOfFile { offset: u64 },
}

struct CorruptionAnalysis {
    patterns: Vec<CorruptionPattern>,
    last_valid_offset: Option<u64>,
    safe_to_truncate_at: Option<u64>,
}

impl CorruptionAnalysis {
    fn new() -> Self {
        Self {
            patterns: Vec::new(),
            last_valid_offset: None,
            safe_to_truncate_at: None,
        }
    }

    fn should_truncate(&self) -> bool {
        // Only truncate if we have a clear truncation point and it's safe
        if let Some(truncate_at) = self.safe_to_truncate_at {
            // Check if everything after truncation point is zero padding or corruption
            // with no valid records
            let has_valid_after_truncation = self.patterns.iter().any(|pattern| match pattern {
                CorruptionPattern::ValidRecord { offset, .. } => *offset >= truncate_at,
                _ => false,
            });
            !has_valid_after_truncation
        } else {
            false
        }
    }
}

pub struct BucketSegmentIter<'a> {
    reader: &'a mut BucketSegmentReader,
    offset: u64,
}

impl BucketSegmentIter<'_> {
    pub fn next_committed_events(&mut self) -> Result<Option<CommittedEvents>, ReadError> {
        let mut this = self;
        loop {
            polonius!(|this| -> Result<Option<CommittedEvents>, ReadError> {
                match this
                    .reader
                    .read_committed_events(this.offset, ReadHint::Sequential)
                {
                    Ok((Some(events), _)) => {
                        match &events {
                            CommittedEvents::Single(event) => {
                                this.offset = event.offset + event.size;
                            }
                            CommittedEvents::Transaction { commit, .. } => {
                                this.offset = commit.offset + COMMIT_SIZE as u64;
                            }
                        }
                        polonius_return!(Ok(Some(events)));
                    }
                    Ok((None, Some(next_offset))) => {
                        this.offset = next_offset;
                        exit_polonius!();
                    }
                    Ok((None, None)) => polonius_return!(Ok(None)),
                    Err(err) => polonius_return!(Err(err)),
                }
            });
        }
    }

    pub fn next_record(&mut self) -> Result<Option<Record>, ReadError> {
        loop {
            match self.reader.read_record(self.offset, ReadHint::Sequential) {
                Ok(Some(record)) => {
                    // Record successfully parsed and CRC32C validated - it's good
                    self.offset = record.offset() + record.len();
                    return Ok(Some(record));
                }
                Ok(None) => return Ok(None),
                Err(err) => {
                    // Check if this looks like end-of-file zero padding vs corruption
                    let file_size = self.reader.file.metadata()?.len();
                    // Based on analysis of real corrupted files, we see 64KB of zero padding
                    if self.offset > file_size.saturating_sub(65536)
                        && let Ok(peek) = self.reader.peek_bytes_at(self.offset, 64)
                        && peek.iter().all(|&b| b == 0)
                    {
                        // Detected zero padding at end of file - truncate it
                        warn!(
                            "detected zero padding at offset {}, truncating file",
                            self.offset
                        );
                        self.truncate_to_offset(self.offset)?;
                        return Ok(None);
                    }

                    warn!(
                        "read error at offset {}: {err}, attempting recovery",
                        self.offset
                    );

                    if let Some(recovered_offset) = self.find_next_valid_record()? {
                        info!("recovered at offset {recovered_offset}, continuing");
                        continue; // Try reading from the recovered position
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    fn find_next_valid_record(&mut self) -> Result<Option<u64>, ReadError> {
        let start_offset = self.offset;
        let file_size = self.reader.file.metadata()?.len();

        // Phase 1: Analyze corruption patterns
        let analysis = self.analyze_corruption_patterns(start_offset, file_size)?;

        // Phase 2: Make recovery decision based on analysis
        if analysis.should_truncate()
            && let Some(truncate_at) = analysis.safe_to_truncate_at
        {
            warn!("detected incomplete write corruption, truncating file at offset {truncate_at}");
            self.truncate_to_offset(truncate_at)?;
            return Ok(None);
        }

        // If we have a valid record, jump to it
        if let Some(valid_offset) = analysis.last_valid_offset {
            trace!("found valid record at offset {valid_offset} after corruption");
            self.offset = valid_offset;
            return Ok(Some(valid_offset));
        }

        // No recovery possible
        warn!("no valid record found and no safe truncation point identified");
        Ok(None)
    }

    fn analyze_corruption_patterns(
        &mut self,
        start_offset: u64,
        file_size: u64,
    ) -> Result<CorruptionAnalysis, ReadError> {
        let mut analysis = CorruptionAnalysis::new();
        let remaining_file = file_size.saturating_sub(start_offset);

        // Use adaptive scan distance
        let max_scan_distance = if remaining_file < 64 * 1024 {
            remaining_file
        } else {
            (remaining_file / 10).min(500_000) // Increased for thorough analysis
        };

        trace!(
            "analyzing corruption patterns from offset {start_offset}, distance: {max_scan_distance}"
        );

        let mut current_offset = start_offset;
        let end_offset = start_offset + max_scan_distance;

        while current_offset < end_offset {
            if let Some(pattern) = self.identify_pattern_at_offset(current_offset, end_offset)? {
                match &pattern {
                    CorruptionPattern::ValidRecord { offset, size } => {
                        analysis.last_valid_offset = Some(*offset);
                        current_offset = offset + size;
                    }
                    CorruptionPattern::ZeroPadding { start, end } => {
                        // Zero padding at end of file is safe to truncate (we see 64KB blocks)
                        if analysis.safe_to_truncate_at.is_none()
                            && *end >= file_size.saturating_sub(65536)
                        {
                            analysis.safe_to_truncate_at = Some(*start);
                        }
                        current_offset = *end;
                    }
                    CorruptionPattern::Corruption { end, .. } => {
                        current_offset = *end;
                    }
                    CorruptionPattern::EndOfFile { .. } => {
                        break;
                    }
                }
                analysis.patterns.push(pattern);
            } else {
                current_offset += 1;
            }
        }

        // If we only found zero padding after the last valid record, it's safe to
        // truncate
        if analysis.safe_to_truncate_at.is_none() {
            if let Some(last_valid) = analysis.last_valid_offset {
                let only_padding_after = analysis.patterns.iter().all(|pattern| match pattern {
                    CorruptionPattern::ValidRecord { offset, .. } => *offset <= last_valid,
                    CorruptionPattern::ZeroPadding { .. } => true,
                    CorruptionPattern::Corruption { .. } => false,
                    CorruptionPattern::EndOfFile { .. } => true,
                });

                if only_padding_after {
                    analysis.safe_to_truncate_at = Some(last_valid);
                }
            }
        }

        // Log the analysis results for debugging
        trace!(
            "corruption analysis complete: {} patterns found",
            analysis.patterns.len()
        );
        for (i, pattern) in analysis.patterns.iter().enumerate() {
            match pattern {
                CorruptionPattern::ValidRecord { offset, size } => {
                    trace!("  pattern[{i}]: valid record at offset {offset}, size {size}");
                }
                CorruptionPattern::ZeroPadding { start, end } => {
                    trace!(
                        "  pattern[{i}]: zero padding from {start} to {end} ({} bytes)",
                        end - start
                    );
                }
                CorruptionPattern::Corruption { start, end } => {
                    trace!(
                        "  pattern[{i}]: corruption from {start} to {end} ({} bytes)",
                        end - start
                    );
                }
                CorruptionPattern::EndOfFile { offset } => {
                    trace!("  pattern[{i}]: end of file at {offset}");
                }
            }
        }

        if let Some(truncate_at) = analysis.safe_to_truncate_at {
            trace!("safe truncation point identified at offset {truncate_at}");
        }

        Ok(analysis)
    }

    fn identify_pattern_at_offset(
        &mut self,
        offset: u64,
        max_offset: u64,
    ) -> Result<Option<CorruptionPattern>, ReadError> {
        let remaining = max_offset - offset;
        if remaining < 8 {
            return Ok(Some(CorruptionPattern::EndOfFile { offset }));
        }

        // Try to read as a valid record first
        if let Ok(candidate_timestamp) = self.reader.read_u64_at(offset) {
            if self.is_valid_timestamp(candidate_timestamp) {
                if let Ok(header) = self.reader.try_read_record_header(offset) {
                    if self.validate_record_header(&header) {
                        if let Ok(()) = self.validate_record_structure(offset, &header) {
                            // Calculate record size
                            let record_size = if header.record_kind == 0 {
                                // Event record - need to read header to get size
                                if let Ok(event_header) = self
                                    .reader
                                    .try_read_event_header(offset + RECORD_HEADER_SIZE as u64)
                                {
                                    EVENT_HEADER_SIZE as u64 + event_header.body_len() as u64
                                } else {
                                    RECORD_HEADER_SIZE as u64 // Fallback
                                }
                            } else {
                                COMMIT_SIZE as u64
                            };

                            return Ok(Some(CorruptionPattern::ValidRecord {
                                offset,
                                size: record_size,
                            }));
                        }
                    }
                }
            }
        }

        // Check for zero padding (we typically see 64KB blocks from shutdown corruption)
        let check_size = 128.min(remaining as usize); // Check larger initial block
        if let Ok(bytes) = self.reader.peek_bytes_at(offset, check_size) {
            if bytes.iter().all(|&b| b == 0) {
                // Found zero padding, find the end of it
                let mut padding_end = offset + check_size as u64;
                while padding_end < max_offset {
                    let peek_size = 128.min((max_offset - padding_end) as usize);
                    if let Ok(next_bytes) = self.reader.peek_bytes_at(padding_end, peek_size) {
                        if next_bytes.iter().all(|&b| b == 0) {
                            padding_end += peek_size as u64;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                return Ok(Some(CorruptionPattern::ZeroPadding {
                    start: offset,
                    end: padding_end,
                }));
            } else {
                // Found corruption (non-zero, non-valid data)
                return Ok(Some(CorruptionPattern::Corruption {
                    start: offset,
                    end: offset + check_size as u64,
                }));
            }
        }

        Ok(None)
    }

    fn truncate_to_offset(&mut self, offset: u64) -> Result<(), ReadError> {
        let original_size = self.reader.file.metadata()?.len();
        let bytes_removed = original_size.saturating_sub(offset);

        info!(
            "truncating file from {original_size} to {offset} bytes (removing {bytes_removed} bytes)"
        );

        // Get mutable access to the file through the reader
        if let Err(e) = self.reader.file.set_len(offset) {
            warn!("failed to truncate file to offset {offset}: {e}");
            return Err(ReadError::Io(e));
        }

        // Sync the truncation to disk
        if let Err(e) = self.reader.file.sync_data() {
            warn!("failed to sync truncation to disk: {e}");
            return Err(ReadError::Io(e));
        }

        // Note: flushed_offset will be updated by the writer on next flush
        // The truncation is immediately synced to disk above

        info!("successfully truncated file to offset {offset}");
        Ok(())
    }

    fn is_valid_timestamp(&self, timestamp: u64) -> bool {
        const MIN_TIMESTAMP: u64 = 1_600_000_000_000_000_000; // ~2020 in nanoseconds  
        const MAX_TIMESTAMP: u64 = 2_000_000_000_000_000_000; // ~2033 in nanoseconds

        // Check if it's an event (bit 63 = 0) or commit (bit 63 = 1)
        let actual_timestamp = timestamp & !(1u64 << 63);
        (MIN_TIMESTAMP..=MAX_TIMESTAMP).contains(&actual_timestamp)
    }

    fn validate_record_header(&self, header: &RecordHeader) -> bool {
        // Confirmation count should be reasonable
        if header.confirmation_count > MAX_REPLICATION_FACTOR as u8 {
            return false;
        }

        // Transaction ID shouldn't be all zeros (though it can be nil UUID)
        // This helps filter out regions of zero-padding
        let tx_bytes = header.transaction_id.as_bytes();
        if tx_bytes.iter().all(|&b| b == 0) && header.crc32c == 0 {
            return false;
        }

        // Record kind should be valid (0 = event, 1 = commit)
        header.record_kind <= 1
    }

    fn validate_record_structure(
        &mut self,
        offset: u64,
        header: &RecordHeader,
    ) -> Result<(), ReadError> {
        // For events, we need to validate the structure more thoroughly
        if header.record_kind == 0 {
            // Try to read the event header to validate lengths make sense
            let event_header_offset = offset + RECORD_HEADER_SIZE as u64;

            // Check we have enough bytes for event header
            if event_header_offset + (EVENT_HEADER_SIZE - RECORD_HEADER_SIZE) as u64
                > self.reader.flushed_offset.load()
            {
                return Err(ReadError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "insufficient bytes for event header",
                )));
            }

            // Try to decode the event header
            if let Ok(event_header) = self.reader.try_read_event_header(event_header_offset) {
                // Validate field lengths are reasonable
                if event_header.stream_id_len == 0 || event_header.stream_id_len > STREAM_ID_SIZE {
                    return Err(ReadError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid stream_id_len: {}", event_header.stream_id_len),
                    )));
                }

                if event_header.event_name_len == 0 || event_header.event_name_len > 255 {
                    return Err(ReadError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid event_name_len: {}", event_header.event_name_len),
                    )));
                }

                // Check total body length is reasonable
                let total_body_len = event_header.body_len();
                if total_body_len > 10_000_000 {
                    return Err(ReadError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("event body too large: {} bytes", total_body_len),
                    )));
                }

                // Check we have enough remaining bytes for the complete record
                let total_record_size = EVENT_HEADER_SIZE as u64 + total_body_len as u64;
                if offset + total_record_size > self.reader.flushed_offset.load() {
                    return Err(ReadError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "insufficient bytes for complete event record",
                    )));
                }
            } else {
                return Err(ReadError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "failed to decode event header",
                )));
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Event(EventRecord),
    Commit(CommitRecord),
}

impl Record {
    pub fn into_event(self) -> Option<EventRecord> {
        match self {
            Record::Event(event) => Some(event),
            Record::Commit(_) => None,
        }
    }

    pub fn into_commit(self) -> Option<CommitRecord> {
        match self {
            Record::Event(_) => None,
            Record::Commit(commit) => Some(commit),
        }
    }

    pub fn offset(&self) -> u64 {
        match self {
            Record::Event(EventRecord { offset, .. }) => *offset,
            Record::Commit(CommitRecord { offset, .. }) => *offset,
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        match self {
            Record::Event(event) => event.size,
            Record::Commit(_) => COMMIT_SIZE as u64,
        }
    }
}

/// Represents a single event record in the event store.
///
/// Each record contains both system metadata for efficient storage and
/// retrieval, and domain-specific data in the payload and metadata fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRecord {
    /// The byte offset location of this event in its storage segment file.
    /// Used for direct access to event data during reads.
    pub offset: u64,

    /// Globally unique identifier for this specific event.
    /// Generated once when the event is created and never changes.
    pub event_id: Uuid,

    /// Determines which partition this event belongs to.
    ///
    /// This is typically a domain-significant identifier (like customer ID,
    /// tenant ID) that groups related events together. All events for the
    /// same stream must share the same partition key.
    pub partition_key: Uuid,

    /// The numeric partition identifier (0-1023) derived from the
    /// partition_key.
    ///
    /// Events with the same partition_id have a guaranteed total ordering
    /// defined by their partition_sequence, regardless of which stream they
    /// belong to.
    pub partition_id: PartitionId,

    /// Identifier for multi-event transactions.
    ///
    /// When multiple events are saved as part of a single transaction, they
    /// share this identifier. For events not part of a transaction, this
    /// may be a null UUID.
    pub transaction_id: Uuid,

    /// The monotonic, gapless sequence number within the partition.
    ///
    /// This defines the total ordering of events within a partition. Each new
    /// event in a partition receives a sequence number exactly one higher
    /// than the previous event.
    pub partition_sequence: u64,

    /// The version number of the entity/aggregate after this event is applied.
    ///
    /// This is a monotonic, gapless counter specific to the stream. It starts
    /// at 0 and increments by 1 for each event in the stream. Used for
    /// optimistic concurrency control and to determine the current state
    /// version of an entity.
    pub stream_version: u64,

    /// Unix timestamp (in nanoseconds) when the event was created.
    ///
    /// Useful for time-based queries and analysis, though not used for event
    /// ordering.
    pub timestamp: u64,

    /// Number of nodes/partitions that have confirmed storing this event.
    ///
    /// Used to determine whether the event has reached the required replication
    /// factor quorum. The event is only considered persisted if this meets
    /// the replication factor quorum.
    pub confirmation_count: u8,

    /// Identifier for the stream (entity/aggregate) this event belongs to.
    ///
    /// Typically corresponds to a domain entity ID, like "account-123" or
    /// "order-456". All events for the same entity share the same
    /// stream_id.
    pub stream_id: StreamId,

    /// Name of the event type, used for deserialization and event handling.
    ///
    /// Examples: "AccountCreated", "OrderShipped", "PaymentRefunded".
    /// Should be meaningful in the domain context.
    pub event_name: String,

    /// Additional system or application metadata about the event.
    ///
    /// May include information like user ID, correlation IDs, causation IDs,
    /// or other contextual data not part of the event payload itself.
    pub metadata: Vec<u8>,

    /// The actual event data serialized as bytes.
    ///
    /// Contains the domain-specific information that constitutes the event.
    /// Must be deserializable based on the event_name.
    pub payload: Vec<u8>,

    /// Size in bytes the event takes on disk.
    pub size: u64,
}

impl EventRecord {
    // pub fn into_owned(self) -> EventRecord<'static> {
    //     EventRecord {
    //         offset: self.offset,
    //         event_id: self.event_id,
    //         partition_key: self.partition_key,
    //         partition_id: self.partition_id,
    //         transaction_id: self.transaction_id,
    //         partition_sequence: self.partition_sequence,
    //         stream_version: self.stream_version,
    //         timestamp: self.timestamp,
    //         confirmation_count: self.confirmation_count,
    //         stream_id: Cow::Owned(self.stream_id.into_owned()),
    //         event_name: Cow::Owned(self.event_name.into_owned()),
    //         metadata: Cow::Owned(self.metadata.into_owned()),
    //         payload: Cow::Owned(self.payload.into_owned()),
    //     }
    // }

    pub fn primary_partition_id(&self, num_partitions: u16) -> PartitionId {
        uuid_to_partition_hash(self.partition_key) % num_partitions
    }

    // This method no longer is valid when reading just the header!
    // #[allow(clippy::len_without_is_empty)]
    // pub fn len(&self) -> u64 {
    //     EVENT_HEADER_SIZE as u64
    //         + self.stream_id.len() as u64
    //         + self.event_name.len() as u64
    //         + self.metadata.len() as u64
    //         + self.payload.len() as u64
    // }

    fn from_parts(
        offset: u64,
        record_header: RecordHeader,
        event_header: EventHeader,
        body: EventBody,
    ) -> Result<Self, ReadError> {
        let new_confirmation_count_crc32c = calculate_confirmation_count_crc32c(
            &record_header.transaction_id,
            record_header.confirmation_count,
        );
        if record_header.confirmation_count_crc32c != new_confirmation_count_crc32c {
            return Err(ReadError::ConfirmationCountCrc32cMismatch { offset });
        }

        let new_crc32c = calculate_event_crc32c(
            record_header.timestamp,
            &record_header.transaction_id,
            &event_header.event_id,
            &event_header.partition_key,
            event_header.partition_id,
            event_header.partition_sequence,
            event_header.stream_version,
            &body.stream_id,
            &body.event_name,
            &body.metadata,
            &body.payload,
        );
        if record_header.crc32c != new_crc32c {
            return Err(ReadError::Crc32cMismatch { offset });
        }

        let size = EVENT_HEADER_SIZE as u64
            + event_header.stream_id_len as u64
            + event_header.event_name_len as u64
            + event_header.metadata_len as u64
            + event_header.payload_len as u64;

        Ok(EventRecord {
            offset,
            event_id: event_header.event_id,
            partition_key: event_header.partition_key,
            partition_id: event_header.partition_id,
            transaction_id: record_header.transaction_id,
            partition_sequence: event_header.partition_sequence,
            stream_version: event_header.stream_version,
            timestamp: record_header.timestamp,
            confirmation_count: record_header.confirmation_count,
            stream_id: body.stream_id,
            event_name: body.event_name,
            metadata: body.metadata,
            payload: body.payload,
            size,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    pub offset: u64,
    pub transaction_id: Uuid,
    pub timestamp: u64,
    pub confirmation_count: u8,
    pub event_count: u32,
}

impl CommitRecord {
    fn from_parts(
        offset: u64,
        record_header: RecordHeader,
        event_count: u32,
    ) -> Result<Self, ReadError> {
        let new_confirmation_count_crc32c = calculate_confirmation_count_crc32c(
            &record_header.transaction_id,
            record_header.confirmation_count,
        );
        if record_header.confirmation_count_crc32c != new_confirmation_count_crc32c {
            return Err(ReadError::ConfirmationCountCrc32cMismatch { offset });
        }

        let new_crc32c = calculate_commit_crc32c(
            &record_header.transaction_id,
            record_header.timestamp,
            event_count,
        );
        if record_header.crc32c != new_crc32c {
            return Err(ReadError::Crc32cMismatch { offset });
        }

        Ok(CommitRecord {
            offset,
            transaction_id: record_header.transaction_id,
            timestamp: record_header.timestamp,
            confirmation_count: record_header.confirmation_count,
            event_count,
        })
    }
}

#[derive(Debug)]
struct RecordHeader {
    timestamp: u64,
    transaction_id: Uuid,
    crc32c: u32,
    confirmation_count: u8,
    confirmation_count_crc32c: u32,
    record_kind: u8,
}

impl<C> Decode<C> for RecordHeader {
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        // Record Kind + Timestamp (8 bytes)
        let encoded_timestamp = u64::decode(decoder)?;
        let record_kind = ((encoded_timestamp >> 63) & 1) as u8;
        let timestamp = encoded_timestamp & !(1u64 << 63);

        // Transaction ID (16 bytes)
        let transaction_id_bytes = <[u8; 16]>::decode(decoder)?;
        let transaction_id = Uuid::from_bytes(transaction_id_bytes);

        // Main data CRC32C (4 bytes)
        let crc32c = u32::decode(decoder)?;

        // Confirmation Count (1 byte)
        let confirmation_count = u8::decode(decoder)?;

        // Confirmation Count CRC32C (4 bytes)
        let confirmation_count_crc32c = u32::decode(decoder)?;

        Ok(RecordHeader {
            timestamp,
            transaction_id,
            crc32c,
            confirmation_count,
            confirmation_count_crc32c,
            record_kind,
        })
    }
}

#[derive(Debug)]
struct EventHeader {
    event_id: Uuid,
    partition_key: Uuid,
    partition_id: PartitionId,
    partition_sequence: u64,
    stream_version: u64,
    stream_id_len: usize,
    event_name_len: usize,
    metadata_len: usize,
    payload_len: usize,
}

impl<C> Decode<C> for EventHeader {
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let event_id_bytes = <[u8; 16]>::decode(decoder)?;
        let event_id = Uuid::from_bytes(event_id_bytes);

        let partition_key_bytes = <[u8; 16]>::decode(decoder)?;
        let partition_key = Uuid::from_bytes(partition_key_bytes);

        let partition_id = u16::decode(decoder)?;

        let partition_sequence = u64::decode(decoder)?;

        let stream_version = u64::decode(decoder)?;

        let stream_id_len = u8::decode(decoder)? as usize;
        let event_name_len = u8::decode(decoder)? as usize;
        let metadata_len = u32::decode(decoder)? as usize;
        let payload_len = u32::decode(decoder)? as usize;

        Ok(EventHeader {
            event_id,
            partition_key,
            partition_id,
            partition_sequence,
            stream_version,
            stream_id_len,
            event_name_len,
            metadata_len,
            payload_len,
        })
    }
}

impl EventHeader {
    fn body_len(&self) -> usize {
        self.stream_id_len + self.event_name_len + self.metadata_len + self.payload_len
    }
}

#[derive(Debug, Default)]
struct EventBody {
    stream_id: StreamId,
    event_name: String,
    metadata: Vec<u8>,
    payload: Vec<u8>,
}

impl<'c> Decode<&'c EventHeader> for EventBody {
    fn decode<D: Decoder<Context = &'c EventHeader>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let mut stream_id_bytes = vec![0; decoder.context().stream_id_len];
        decoder.reader().read(&mut stream_id_bytes)?;
        let stream_id =
            StreamId::new(
                String::from_utf8(stream_id_bytes).map_err(|err| DecodeError::Utf8 {
                    inner: err.utf8_error(),
                })?,
            )
            .map_err(|err| DecodeError::OtherString(err.to_string()))?;

        let mut event_name_bytes = vec![0; decoder.context().event_name_len];
        decoder.reader().read(&mut event_name_bytes)?;
        let event_name = String::from_utf8(event_name_bytes).map_err(|err| DecodeError::Utf8 {
            inner: err.utf8_error(),
        })?;

        let mut metadata = vec![0; decoder.context().metadata_len];
        decoder.reader().read(&mut metadata)?;

        let mut payload = vec![0; decoder.context().payload_len];
        decoder.reader().read(&mut payload)?;

        Ok(EventBody {
            stream_id,
            event_name,
            metadata,
            payload,
        })
    }
}

// impl<'a, 'c> BorrowDecode<'a, &'c EventHeader> for EventBody<'a> {
//     fn borrow_decode<D: BorrowDecoder<'a, Context = &'c EventHeader>>(
//         decoder: &mut D,
//     ) -> Result<Self, DecodeError> {
//         let EventHeader {
//             stream_id_len,
//             event_name_len,
//             metadata_len,
//             payload_len,
//             ..
//         } = decoder.context();

//         decoder.claim_bytes_read(*stream_id_len)?;
//         let stream_id_bytes =
// decoder.borrow_reader().take_bytes(*stream_id_len)?;         let stream_id =
//             std::str::from_utf8(stream_id_bytes).map_err(|err|
// DecodeError::Utf8 { inner: err })?;

//         decoder.claim_bytes_read(*event_name_len)?;
//         let event_name_bytes =
// decoder.borrow_reader().take_bytes(*event_name_len)?;         let event_name
// = std::str::from_utf8(event_name_bytes)             .map_err(|err|
// DecodeError::Utf8 { inner: err })?;

//         decoder.claim_bytes_read(*metadata_len)?;
//         let metadata = decoder.borrow_reader().take_bytes(*metadata_len)?;

//         decoder.claim_bytes_read(*payload_len)?;
//         let payload = decoder.borrow_reader().take_bytes(*payload_len)?;

//         Ok(EventBody {
//             stream_id: Cow::Borrowed(stream_id),
//             event_name: Cow::Borrowed(event_name),
//             metadata: Cow::Borrowed(metadata),
//             payload: Cow::Borrowed(payload),
//         })
//     }
// }
