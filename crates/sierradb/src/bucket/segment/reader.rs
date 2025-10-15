use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::{mem, vec};

use bincode::Decode;
use bincode::de::Decoder;
use bincode::de::read::Reader;
use bincode::error::DecodeError;
use polonius_the_crab::{exit_polonius, polonius, polonius_return, polonius_try};
use uuid::Uuid;

use super::{
    BINCODE_CONFIG, BUCKET_ID_SIZE, BucketSegmentHeader, COMMIT_SIZE, CREATED_AT_SIZE,
    EVENT_HEADER_SIZE, FlushedOffset, MAGIC_BYTES, MAGIC_BYTES_SIZE, RECORD_HEADER_SIZE,
    SEGMENT_HEADER_SIZE, VERSION_SIZE, calculate_commit_crc32c,
    calculate_confirmation_count_crc32c, calculate_event_crc32c,
};
use crate::StreamId;
use crate::bucket::{BucketId, PartitionId};
use crate::error::{ReadError, WriteError};
use crate::id::get_uuid_flag;

const HEADER_BUF_SIZE: usize = EVENT_HEADER_SIZE - RECORD_HEADER_SIZE;
const PAGE_SIZE: usize = 4096; // Usually a page is 4KB on Linux
const READ_AHEAD_SIZE: usize = 64 * 1024; // 64 KB read ahead buffer
const READ_BUF_SIZE: usize = PAGE_SIZE - COMMIT_SIZE;

pub enum CommittedEvents {
    None {
        next_offset: u64,
    },
    Single(EventRecord),
    Transaction {
        events: Vec<EventRecord>,
        commit: CommitRecord,
    },
}

impl IntoIterator for CommittedEvents {
    type Item = EventRecord;
    type IntoIter = CommittedEventsIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let inner = match self {
            CommittedEvents::None { .. } => CommittedEventsIntoIterInner::Single(None),
            CommittedEvents::Single(event) => CommittedEventsIntoIterInner::Single(Some(event)),
            CommittedEvents::Transaction { events, .. } => {
                CommittedEventsIntoIterInner::Transaction(events.into_iter())
            }
        };
        CommittedEventsIntoIter { inner }
    }
}

pub struct CommittedEventsIntoIter {
    inner: CommittedEventsIntoIterInner,
}

enum CommittedEventsIntoIterInner {
    Single(Option<EventRecord>),
    Transaction(vec::IntoIter<EventRecord>),
}

impl Iterator for CommittedEventsIntoIter {
    type Item = EventRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            CommittedEventsIntoIterInner::Single(event) => event.take(),
            CommittedEventsIntoIterInner::Transaction(iter) => iter.next(),
        }
    }
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
    pub fn open(path: impl AsRef<Path>, flushed_offset: FlushedOffset) -> Result<Self, ReadError> {
        // On OSX, gives ~5% better performance for both random and sequential reads
        const O_DIRECT: i32 = 0o0040000;
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .custom_flags(O_DIRECT)
            .open(path)?;
        let header_buf = [0u8; HEADER_BUF_SIZE];
        let body_buf = [0u8; READ_BUF_SIZE];

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
        sequential: bool,
    ) -> Result<Option<CommittedEvents>, ReadError> {
        let mut this = self;
        let mut events = Vec::new();
        let mut pending_transaction_id = Uuid::nil();
        loop {
            (events, offset) = polonius!(|this| -> Result<Option<CommittedEvents>, ReadError> {
                let record = polonius_try!(this.read_record(offset, sequential));
                match record {
                    Some(Record::Event(
                        event @ EventRecord {
                            offset,
                            transaction_id,
                            ..
                        },
                    )) => {
                        let next_offset = offset + event.len();

                        if get_uuid_flag(&transaction_id) {
                            // Events with a true transaction id flag are always approved
                            if events.is_empty() {
                                // If its the first event we encountered, then return it alone
                                polonius_return!(Ok(Some(CommittedEvents::Single(event))));
                            }

                            events.push(event);
                        } else if transaction_id != pending_transaction_id {
                            // Unexpected transaction, we'll start a new pending transaction
                            events = vec![event];
                            pending_transaction_id = transaction_id;
                        } else {
                            // Event belongs to the transaction
                            events.push(event);
                        }

                        exit_polonius!((events, next_offset))
                    }
                    Some(Record::Commit(commit)) => {
                        if commit.transaction_id == pending_transaction_id && !events.is_empty() {
                            polonius_return!(Ok(Some(CommittedEvents::Transaction {
                                events,
                                commit,
                            })));
                        }

                        polonius_return!(Ok(Some(CommittedEvents::None {
                            next_offset: commit.offset + COMMIT_SIZE as u64
                        })));
                    }
                    None => polonius_return!(Ok(None)),
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
        sequential: bool,
    ) -> Result<Option<Record>, ReadError> {
        // This is the only check needed. We don't need to check for the event body,
        // since if the offset supports this header read, then the event body would have
        // also been written too for the flush.
        if start_offset + COMMIT_SIZE as u64 > self.flushed_offset.load() {
            return Ok(None);
        }

        let mut offset = start_offset;
        let header_buf = if sequential {
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

            self.read_event_body(start_offset, record_header, offset, sequential)
                .map(|event| Some(Record::Event(event)))
        } else if record_header.record_kind == 1 {
            let event_count = u32::from_le_bytes(
                header_buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + 4]
                    .try_into()
                    .unwrap(),
            );
            Ok(Some(Record::Commit(CommitRecord::from_parts(
                offset,
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
        sequential: bool,
    ) -> Result<EventRecord, ReadError> {
        let length = EVENT_HEADER_SIZE - RECORD_HEADER_SIZE;
        let header_buf = if sequential {
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
        let body = if sequential {
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
                match this.reader.read_committed_events(this.offset, true) {
                    Ok(Some(events)) => {
                        match &events {
                            CommittedEvents::None { next_offset } => {
                                this.offset = *next_offset;
                                exit_polonius!();
                            }
                            CommittedEvents::Single(event) => {
                                this.offset = event.offset + event.len();
                            }
                            CommittedEvents::Transaction { commit, .. } => {
                                this.offset = commit.offset + COMMIT_SIZE as u64;
                            }
                        }
                        polonius_return!(Ok(Some(events)));
                    }
                    Ok(None) => polonius_return!(Ok(None)),
                    Err(err) => polonius_return!(Err(err)),
                }
            });
        }
    }

    pub fn next_record(&mut self) -> Result<Option<Record>, ReadError> {
        match self.reader.read_record(self.offset, true) {
            Ok(Some(record)) => {
                self.offset = record.offset() + record.len();
                Ok(Some(record))
            }
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
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
            Record::Event(event) => event.len(),
            Record::Commit(_) => COMMIT_SIZE as u64,
        }
    }
}

/// Represents a single event record in the event store.
///
/// Each record contains both system metadata for efficient storage and
/// retrieval, and domain-specific data in the payload and metadata fields.
#[derive(Clone, Debug, PartialEq, Eq)]
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

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        EVENT_HEADER_SIZE as u64
            + self.stream_id.len() as u64
            + self.event_name.len() as u64
            + self.metadata.len() as u64
            + self.payload.len() as u64
    }

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
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Debug)]
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
