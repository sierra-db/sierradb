use std::{collections::BTreeMap, fs::File, io, str::Utf8Error, sync::Arc, time::SystemTimeError};

use arc_swap::ArcSwap;
use rayon::ThreadPoolBuildError;
use thiserror::Error;

use crate::{
    bucket::{
        BucketSegmentId,
        event_index::ClosedIndex,
        stream_index::{StreamIndexRecord, StreamOffsets},
    },
    database::{CurrentVersion, ExpectedVersion},
};

/// Errors which can occur in background threads.
#[derive(Debug, Error)]
pub enum ThreadPoolError {
    #[error("failed to flush event index for {id}: {err}")]
    FlushEventIndex {
        id: BucketSegmentId,
        file: File,
        index: Arc<ArcSwap<ClosedIndex>>,
        err: EventIndexError,
    },
    #[error("failed to flush stream index for {id}: {err}")]
    FlushStreamIndex {
        id: BucketSegmentId,
        file: File,
        index: Arc<BTreeMap<Arc<str>, StreamIndexRecord<StreamOffsets>>>,
        num_slots: u64,
        err: StreamIndexError,
    },
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    EventIndex(#[from] EventIndexError),
    #[error(transparent)]
    StreamIndex(#[from] StreamIndexError),
    #[error(transparent)]
    Write(#[from] WriteError),
    #[error(transparent)]
    ThreadPool(#[from] ThreadPoolBuildError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("crc32c hash mismatch")]
    Crc32cMismatch { offset: u64 },
    #[error("invalid stream id: {0}")]
    InvalidStreamIdUtf8(Utf8Error),
    #[error("invalid event name: {0}")]
    InvalidEventNameUtf8(Utf8Error),
    #[error("unknown record type: {0}")]
    UnknownRecordType(u8),
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
    #[error("writer thread is not running")]
    WriterThreadNotRunning,
    #[error("events exceed the size of a single segment")]
    EventsExceedSegmentSize,
    /// Wrong expected version
    #[error("current stream version is {current} but expected {expected} for stream {stream_id}")]
    WrongExpectedVersion {
        stream_id: Arc<str>,
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
    #[error(transparent)]
    Validation(#[from] EventValidationError),
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    EventIndex(#[from] EventIndexError),
    #[error(transparent)]
    StreamIndex(#[from] StreamIndexError),
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
    DeserializeMphf(bincode::Error),
    #[error("failed to serialize MPHF: {0}")]
    SerializeMphf(bincode::Error),
    #[error("corrupt magic bytes header")]
    CorruptHeader,
    #[error("corrupt number of slots section in event index")]
    CorruptNumSlots,
    #[error("corrupt record in event index")]
    CorruptRecord { offset: u64 },
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum StreamIndexError {
    #[error("bloom filter error: {err}")]
    Bloom { err: &'static str },
    #[error("corrupt number of slots section in stream index")]
    CorruptNumSlots,
    #[error("corrupt stream index length")]
    CorruptLen,
    #[error("corrupt record in stream index")]
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
    #[error("event name too long")]
    EventNameTooLong,
    #[error("metadata too long")]
    MetadataTooLong,
    #[error("payload too long")]
    PayloadTooLong,
    #[error("invalid event id: bits 61..46 should embed stream id hash")]
    InvalidEventId,
    #[error("stream id must be between 1 and 64 characters in length")]
    InvalidStreamIdLen,
    #[error("partition key must be the same for all events in a stream")]
    PartitionKeyMismatch,
    #[error("transaction has no events")]
    EmptyTransaction,
}
