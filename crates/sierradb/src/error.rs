use std::fs::File;
use std::io;
use std::str::Utf8Error;
use std::sync::Arc;
use std::time::SystemTimeError;

use arc_swap::ArcSwap;
use rayon::ThreadPoolBuildError;
use thiserror::Error;
use uuid::Uuid;

use crate::StreamId;
use crate::bucket::{
    BucketId, BucketSegmentId, PartitionId, event_index, partition_index, stream_index,
};
use crate::database::{CurrentVersion, ExpectedVersion};

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
    #[error("crc32c hash mismatch")]
    Crc32cMismatch { offset: u64 },
    #[error("confirmation count crc32c hash mismatch")]
    ConfirmationCountCrc32cMismatch { offset: u64 },
    #[error("invalid stream id: {0}")]
    InvalidStreamIdUtf8(Utf8Error),
    #[error("invalid event name: {0}")]
    InvalidEventNameUtf8(Utf8Error),
    #[error("unknown record type: {0}")]
    UnknownRecordType(u8),
    #[error("no reply from the reader thread")]
    NoThreadReply,
    #[error(transparent)]
    Bincode(#[from] bincode::error::DecodeError),
    #[error(transparent)]
    EventIndex(#[from] Box<EventIndexError>),
    #[error(transparent)]
    Io(#[from] io::Error),
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
    Bincode(#[from] bincode::error::EncodeError),
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
    Io(#[from] io::Error),
}

impl From<SystemTimeError> for WriteError {
    fn from(_: SystemTimeError) -> Self {
        WriteError::BadSystemTime
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
    #[error("partition id is already mapped to another bucket")]
    PartitionIdMappedToExternalBucket,
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
    #[error("stream id is already mapped to another bucket")]
    StreamIdMappedToExternalBucket,
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

#[derive(Clone, Copy, Debug, Error)]
pub enum StreamIdError {
    #[error("stream id must be between 1 and 64 characters in length")]
    InvalidLength,
    #[error("stream id cannot contain null bytes")]
    ContainsNullByte,
}
