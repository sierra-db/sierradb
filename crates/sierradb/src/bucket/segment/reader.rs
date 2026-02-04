use std::io;
use std::path::Path;
use std::{fmt, mem, ops, option, vec};

use polonius_the_crab::{exit_polonius, polonius, polonius_return, polonius_try};
use seglog::FlushedOffset;
use seglog::read::ReadHint;
use serde::{Deserialize, Serialize};
use smallvec::{SmallVec, smallvec};
use tracing::warn;
use uuid::Uuid;

use super::{BINCODE_CONFIG, BucketSegmentHeader, COMMIT_SIZE};
use crate::StreamId;
use crate::bucket::PartitionId;
use crate::bucket::segment::format::{ConfirmationCount, RawCommit, RawEvent};
use crate::bucket::segment::{CONFIRMATION_HEADER_SIZE, RecordKindTimestamp, SEGMENT_HEADER_SIZE};
use crate::cache::BLOCK_SIZE;
use crate::error::ReadError;
use crate::id::{get_uuid_flag, uuid_to_partition_hash};

#[derive(Clone)]
pub struct SegmentBlock {
    offset: u64,
    block: Box<[u8]>, // Guaranteed to have length of BLOCK_SIZE
}

impl SegmentBlock {
    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    #[inline]
    pub fn next_segment_block_offset(&self) -> u64 {
        self.offset + BLOCK_SIZE as u64
    }

    pub fn read_committed_events(
        &self,
        mut offset: u64,
    ) -> Result<(Option<CommittedEvents>, Option<u64>), ReadError> {
        let mut events = SmallVec::new();
        let mut pending_transaction_id = Uuid::nil();
        loop {
            let record = self.read_record(offset)?;
            match record {
                Some(Record::Event(
                    event @ EventRecord {
                        offset: event_offset,
                        transaction_id,
                        ..
                    },
                )) => {
                    let next_offset = event_offset + event.size;

                    if get_uuid_flag(&transaction_id) {
                        // Events with a true transaction id flag are always approved
                        if events.is_empty() {
                            // If its the first event we encountered, then return it alone
                            return Ok((Some(CommittedEvents::Single(event)), Some(next_offset)));
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

                    offset = next_offset;
                }
                Some(Record::Commit(commit)) => {
                    let next_offset = commit.offset + COMMIT_SIZE as u64;
                    if commit.transaction_id == pending_transaction_id && !events.is_empty() {
                        return Ok((
                            Some(CommittedEvents::Transaction {
                                events: Box::new(events),
                                commit,
                            }),
                            Some(next_offset),
                        ));
                    }

                    return Ok((None, Some(next_offset)));
                }
                None => return Ok((None, None)),
            }
        }
    }

    pub fn read_record(&self, start_offset: u64) -> Result<Option<Record>, ReadError> {
        let offset = (start_offset - self.offset) as usize;
        let ([confirmation_count_byte], bytes, record_len) =
            seglog::parse::parse_record::<CONFIRMATION_HEADER_SIZE>(&self.block, offset)?;

        let confirmation_count = ConfirmationCount::from_byte(confirmation_count_byte)?;

        if bytes.len() < mem::size_of::<u64>() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "insufficient bytes for record",
            )
            .into());
        }

        let is_commit = u64::from_le_bytes(bytes[..8].try_into().unwrap()) & (1u64 << 63) != 0;

        if is_commit {
            let (record, _) = bincode::decode_from_slice::<RawCommit, _>(&bytes, BINCODE_CONFIG)?;
            let commit = Record::Commit(CommitRecord::from_raw(
                start_offset,
                confirmation_count.get(),
                record,
            ));
            Ok(Some(commit))
        } else {
            let (record, _) = bincode::decode_from_slice::<RawEvent, _>(&bytes, BINCODE_CONFIG)?;
            let event = Record::Event(EventRecord::from_raw(
                start_offset,
                confirmation_count.get(),
                record,
                record_len as u64,
            ));
            Ok(Some(event))
        }
    }
}

impl fmt::Debug for SegmentBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentBlock")
            .field("offset", &self.offset)
            .field("block", &format_args!("[{BLOCK_SIZE}b]"))
            .finish()
    }
}

impl ops::Deref for SegmentBlock {
    type Target = [u8]; // ; BLOCK_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

pub struct SegmentBlockIter {
    reader: SegmentBlock,
    offset: u64,
}

impl SegmentBlockIter {
    pub fn next_committed_events(&mut self) -> Result<Option<CommittedEvents>, ReadError> {
        loop {
            match self.reader.read_committed_events(self.offset) {
                Ok((Some(events), _)) => {
                    match &events {
                        CommittedEvents::Single(event) => {
                            self.offset = event.offset + event.size;
                        }
                        CommittedEvents::Transaction { commit, .. } => {
                            self.offset = commit.offset + COMMIT_SIZE as u64;
                        }
                    }
                    return Ok(Some(events));
                }
                Ok((None, Some(next_offset))) => {
                    self.offset = next_offset;
                }
                Ok((None, None)) => return Ok(None),
                Err(err) => return Err(err),
            }
        }
    }

    pub fn next_record(&mut self) -> Result<Option<Record>, ReadError> {
        match self.reader.read_record(self.offset) {
            Ok(Some(record)) => {
                self.offset = record.offset() + record.len();
                Ok(Some(record))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                warn!("unexpected read error at offset {}: {err}", self.offset);
                Err(err)
            }
        }
    }
}

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

    pub fn first_stream_version(&self) -> Option<u64> {
        match self {
            CommittedEvents::Single(event) => Some(event.stream_version),
            CommittedEvents::Transaction { events, .. } => {
                events.first().map(|event| event.stream_version)
            }
        }
    }

    pub fn last_stream_version(&self) -> Option<u64> {
        match self {
            CommittedEvents::Single(event) => Some(event.stream_version),
            CommittedEvents::Transaction { events, .. } => {
                events.last().map(|event| event.stream_version)
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

pub struct BucketSegmentReader {
    reader: seglog::read::Reader<CONFIRMATION_HEADER_SIZE>,
}

impl BucketSegmentReader {
    pub fn open(
        path: impl AsRef<Path>,
        flushed_offset: Option<FlushedOffset>,
    ) -> Result<Self, ReadError> {
        let reader = seglog::read::Reader::open(path, flushed_offset)?;
        BucketSegmentHeader::load_from_file(reader.file())?.validate()?;

        Ok(BucketSegmentReader { reader })
    }

    pub fn try_clone(&self) -> Result<Self, ReadError> {
        Ok(BucketSegmentReader {
            reader: self.reader.try_clone()?,
        })
    }

    pub fn read_header(&self) -> Result<BucketSegmentHeader, ReadError> {
        BucketSegmentHeader::load_from_file(self.reader.file())
    }

    pub fn set_confirmations(
        &mut self,
        offset: u64,
        transaction_id: &Uuid,
        confirmation_count: u8,
    ) -> Result<bool, ReadError> {
        let confirmation_count_byte = ConfirmationCount::new(confirmation_count)?.to_byte();
        let found = self.reader.replace_header_with(offset, |record| {
            let transaction_id_offset = mem::size_of::<RecordKindTimestamp>();
            if record.data.len() < transaction_id_offset + 16 {
                warn!("not enough data to read transaction id");
            }
            if &Uuid::from_bytes(
                record.data[transaction_id_offset..transaction_id_offset + 16]
                    .try_into()
                    .unwrap(),
            ) != transaction_id
            {
                return None;
            }

            Some([confirmation_count_byte])
        })?;

        Ok(found)
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

    /// Creates an iterator over all records starting from offset 0.
    pub fn iter(&mut self) -> BucketSegmentIter<'_> {
        self.iter_from(SEGMENT_HEADER_SIZE as u64)
    }

    /// Creates an iterator over records starting from the specified offset.
    pub fn iter_from(&mut self, offset: u64) -> BucketSegmentIter<'_> {
        BucketSegmentIter {
            reader: self,
            offset,
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
        let record = match self.reader.read_record(start_offset, hint) {
            Ok(record) => record,
            Err(seglog::read::ReadError::OutOfBounds { .. }) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        let confirmation_count = ConfirmationCount::from_byte(record.header[0])?;
        let record_len = record.len as u64;

        let bytes = record.data;
        if bytes.len() < mem::size_of::<u64>() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "insufficient bytes for record",
            )
            .into());
        }

        let is_commit = u64::from_le_bytes(bytes[..8].try_into().unwrap()) & (1u64 << 63) != 0;

        if is_commit {
            let (record, _) = bincode::decode_from_slice::<RawCommit, _>(&bytes, BINCODE_CONFIG)?;
            let commit = Record::Commit(CommitRecord::from_raw(
                start_offset,
                confirmation_count.get(),
                record,
            ));
            Ok(Some(commit))
        } else {
            let (record, _) = bincode::decode_from_slice::<RawEvent, _>(&bytes, BINCODE_CONFIG)?;
            let event = Record::Event(EventRecord::from_raw(
                start_offset,
                confirmation_count.get(),
                record,
                record_len,
            ));
            Ok(Some(event))
        }
    }

    pub fn read_block(&self, offset: u64) -> Result<Option<SegmentBlock>, ReadError> {
        let mut block = vec![0; BLOCK_SIZE].into_boxed_slice();
        match self.reader.read_bytes(offset, &mut block) {
            Ok(()) => Ok(Some(SegmentBlock { offset, block })),
            Err(seglog::read::ReadError::OutOfBounds { .. }) => Ok(None),
            Err(err) => Err(err.into()),
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
        match self.reader.read_record(self.offset, ReadHint::Sequential) {
            Ok(Some(record)) => {
                self.offset = record.offset() + record.len();
                Ok(Some(record))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                warn!("unexpected read error at offset {}: {err}", self.offset);
                Err(err)
            }
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
    pub fn primary_partition_id(&self, num_partitions: u16) -> PartitionId {
        uuid_to_partition_hash(self.partition_key) % num_partitions
    }

    fn from_raw(offset: u64, confirmation_count: u8, record: RawEvent, size: u64) -> Self {
        EventRecord {
            offset,
            event_id: Uuid::from_bytes(record.event_id),
            partition_key: Uuid::from_bytes(record.partition_key),
            partition_id: record.partition_id,
            transaction_id: Uuid::from_bytes(record.header.transaction_id),
            partition_sequence: record.partition_sequence,
            stream_version: record.stream_version,
            timestamp: record.header.timestamp.timestamp(),
            confirmation_count,
            stream_id: record.stream_id,
            event_name: record.event_name.into_inner(),
            metadata: record.metadata.into_inner(),
            payload: record.payload.into_inner(),
            size,
        }
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
    fn from_raw(offset: u64, confirmation_count: u8, raw: RawCommit) -> Self {
        CommitRecord {
            offset,
            transaction_id: Uuid::from_bytes(raw.header.transaction_id),
            timestamp: raw.header.timestamp.timestamp(),
            confirmation_count,
            event_count: raw.event_count,
        }
    }
}
