use std::fs::File;
use std::io;
use std::str::Utf8Error;
use std::sync::Arc;
use std::time::SystemTimeError;

use arc_swap::ArcSwap;
use rayon::ThreadPoolBuildError;
use thiserror::Error;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::bucket::segment::InvalidTimestamp;
use crate::bucket::{
    BucketId, BucketSegmentId, PartitionId, event_index, partition_index, stream_index,
};
use crate::database::{CurrentVersion, ExpectedVersion};
use crate::{MAX_REPLICATION_FACTOR, StreamId};

/// Errors which can occur in background threads.
#[derive(Debug, Error)]
pub enum ThreadPoolError {
    #[error("failed to flush event index for {id}: {err}")]
    FlushEventIndex {
        id: BucketSegmentId,
        file: File,
        index: Arc<ArcSwap<event_index::ClosedIndex>>,
        err: EventIndexError,
    },
    #[error("failed to flush partition index for {id}: {err}")]
    FlushPartitionIndex {
        id: BucketSegmentId,
        file: File,
        index: Arc<ArcSwap<partition_index::ClosedIndex>>,
        err: PartitionIndexError,
    },
    #[error("failed to flush stream index for {id}: {err}")]
    FlushStreamIndex {
        id: BucketSegmentId,
        file: File,
        index: Arc<ArcSwap<stream_index::ClosedIndex>>,
        err: StreamIndexError,
    },
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Write(#[from] WriteError),
    #[error(transparent)]
    EventIndex(#[from] EventIndexError),
    #[error(transparent)]
    PartitionIndex(#[from] PartitionIndexError),
    #[error(transparent)]
    StreamIndex(#[from] StreamIndexError),
    #[error(transparent)]
    ThreadPool(#[from] ThreadPoolBuildError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("no reply from the reader thread")]
    NoThreadReply,
    #[error("bucket id {bucket_id} not found")]
    BucketIdNotFound { bucket_id: BucketId },
    #[error("transaction id {transaction_id} not found at offset {offset} in any segment files")]
    TransactionIdNotFoundAtOffset { transaction_id: Uuid, offset: u64 },
    #[error(
        "failed to set confirmations: {}", errors.iter().map(|err| err.to_string()).collect::<Vec<_>>().join(", ")
    )]
    SetConfirmations { errors: Vec<ReadError> },
    #[error(transparent)]
    ConfirmationCount(#[from] ConfirmationCountError),
    #[error(transparent)]
    InvalidHeader(#[from] InvalidHeaderError),
    #[error(transparent)]
    Bincode(#[from] bincode::error::DecodeError),
    #[error(transparent)]
    EventIndex(#[from] Box<EventIndexError>),
    #[error(transparent)]
    Reader(#[from] seglog::read::ReadError),
}

impl From<io::Error> for ReadError {
    fn from(err: io::Error) -> Self {
        ReadError::Reader(seglog::read::ReadError::Io(err))
    }
}

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("system time is incorrect")]
    BadSystemTime,
    #[error("bucket writer not found")]
    BucketWriterNotFound,
    #[error("stream version too high")]
    StreamVersionTooHigh,
    #[error("writer thread is not running for bucket id {bucket_id}")]
    WriterThreadNotRunning { bucket_id: BucketId },
    #[error("events exceed the size of a single segment")]
    EventsExceedSegmentSize,
    #[error("offset {offset} exceeds the file size {size}")]
    OffsetExceedsFileSize { offset: u64, size: u64 },
    /// Wrong expected version
    #[error(
        "current partition sequence is {current} but expected {expected} for partition {partition_id}"
    )]
    WrongExpectedSequence {
        partition_id: PartitionId,
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
    /// Wrong expected version
    #[error("current stream version is {current} but expected {expected} for stream {stream_id}")]
    WrongExpectedVersion {
        partition_key: Uuid,
        stream_id: StreamId,
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
    #[error("wrong event id at offset {offset}: expected {expected} but found {found}")]
    WrongEventId {
        offset: u64,
        found: Uuid,
        expected: Uuid,
    },
    #[error("wrong transaction id at offset {offset}: expected {expected} but found {found}")]
    WrongTransactionId {
        offset: u64,
        found: Uuid,
        expected: Uuid,
    },
    #[error("no reply from the writer thread")]
    NoThreadReply,
    #[error(transparent)]
    ConfirmationCount(#[from] ConfirmationCountError),
    #[error(transparent)]
    Encode(#[from] bincode::error::EncodeError),
    #[error(transparent)]
    Decode(#[from] bincode::error::DecodeError),
    #[error(transparent)]
    Validation(#[from] EventValidationError),
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    EventIndex(#[from] EventIndexError),
    #[error(transparent)]
    StreamIndex(#[from] StreamIndexError),
    #[error(transparent)]
    PartitionIndex(#[from] PartitionIndexError),
    #[error(transparent)]
    Writer(#[from] seglog::write::WriteError),
}

impl From<io::Error> for WriteError {
    fn from(err: io::Error) -> Self {
        WriteError::Writer(seglog::write::WriteError::Io(err))
    }
}

impl From<SystemTimeError> for WriteError {
    fn from(_: SystemTimeError) -> Self {
        WriteError::BadSystemTime
    }
}

impl From<InvalidTimestamp> for WriteError {
    fn from(_: InvalidTimestamp) -> Self {
        WriteError::BadSystemTime
    }
}

impl From<InvalidHeaderError> for WriteError {
    fn from(err: InvalidHeaderError) -> Self {
        WriteError::Read(ReadError::InvalidHeader(err))
    }
}

#[derive(Debug, Error)]
pub enum EventIndexError {
    #[error("failed to deserialize MPHF: {0}")]
    DeserializeMphf(bincode::error::DecodeError),
    #[error("failed to serialize MPHF: {0:?}")]
    SerializeMphf(bincode::error::EncodeError),
    #[error("corrupt magic bytes header")]
    CorruptHeader,
    #[error("corrupt number of slots section in event index")]
    CorruptNumSlots,
    #[error("corrupt record in event index at offset {offset}")]
    CorruptRecord { offset: u64 },
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum PartitionIndexError {
    #[error("bloom filter error: {err}")]
    Bloom { err: &'static str },
    #[error("failed to deserialize MPHF: {0}")]
    DeserializeMphf(bincode::error::DecodeError),
    #[error("failed to serialize MPHF: {0}")]
    SerializeMphf(bincode::error::EncodeError),
    #[error("corrupt magic bytes header in partition index")]
    CorruptHeader,
    #[error("corrupt number of slots section in partition index")]
    CorruptNumSlots,
    #[error("corrupt partition index length")]
    CorruptLen,
    #[error("corrupt record in partition index at offset {offset}")]
    CorruptRecord { offset: u64 },
    #[error("event count overflow")]
    EventCountOverflow,
    #[error("invalid partition id: {0}")]
    InvalidStreamIdUtf8(Utf8Error),
    #[error("partition id already exists with an offset")]
    PartitionIdOffsetExists,
    #[error("bucket segment not found: {bucket_segment_id}")]
    SegmentNotFound { bucket_segment_id: BucketSegmentId },
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Validation(#[from] EventValidationError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum StreamIndexError {
    #[error("bloom filter error: {err}")]
    Bloom { err: &'static str },
    #[error("failed to deserialize MPHF: {0}")]
    DeserializeMphf(bincode::error::DecodeError),
    #[error("failed to serialize MPHF: {0}")]
    SerializeMphf(bincode::error::EncodeError),
    #[error("corrupt magic bytes header in stream index")]
    CorruptHeader,
    #[error("corrupt number of slots section in stream index")]
    CorruptNumSlots,
    #[error("corrupt stream index length")]
    CorruptLen,
    #[error("corrupt record in stream index at offset {offset}")]
    CorruptRecord { offset: u64 },
    #[error("invalid stream id: {0}")]
    InvalidStreamIdUtf8(Utf8Error),
    #[error("stream id already exists with an offset")]
    StreamIdOffsetExists,
    #[error("bucket segment not found: {bucket_segment_id}")]
    SegmentNotFound { bucket_segment_id: BucketSegmentId },
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Validation(#[from] EventValidationError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum EventValidationError {
    #[error("the event id must embed the partition hash")]
    InvalidEventId,
    #[error(
        "partition key must be the same for all events in a stream: expected {existing_partition_key}, got {new_partition_key}"
    )]
    PartitionKeyMismatch {
        existing_partition_key: Uuid,
        new_partition_key: Uuid,
    },
    #[error("transaction has no events")]
    EmptyTransaction,
}

#[derive(Clone, Debug, Error)]
pub enum StreamIdError {
    #[error("stream id must be between 1 and 64 characters in length, but got {len} for '{input}'")]
    InvalidLength { input: String, len: usize },
    #[error("stream id cannot contain null bytes")]
    ContainsNullByte,
}

#[derive(Clone, Debug, Error)]
pub enum ConfirmationCountError {
    #[error("confirmation count exceeds max replication factor of {MAX_REPLICATION_FACTOR}")]
    ExceedsMaxReplicationFactor,
    #[error("confirmation count redundancy check failed: found {upper} and {lower}")]
    RedundancyCheck { upper: u8, lower: u8 },
}

impl From<oneshot::error::RecvError> for ReadError {
    fn from(_: oneshot::error::RecvError) -> Self {
        ReadError::NoThreadReply
    }
}

impl From<oneshot::error::RecvError> for PartitionIndexError {
    fn from(err: oneshot::error::RecvError) -> Self {
        PartitionIndexError::Read(err.into())
    }
}

impl From<oneshot::error::RecvError> for StreamIndexError {
    fn from(err: oneshot::error::RecvError) -> Self {
        StreamIndexError::Read(err.into())
    }
}

#[derive(Debug, Error)]
pub enum InvalidHeaderError {
    #[error("invalid magic bytes: expected {expected}, got {actual}")]
    InvalidMagicBytes { expected: u32, actual: u32 },
    #[error("incompatible segment format version: expected {expected}, got {actual}")]
    IncompatibleVersion { expected: u16, actual: u16 },
}
