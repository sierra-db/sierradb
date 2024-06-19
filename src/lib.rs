//! The event log is an append-only data structure that can be used in a
//! variety of use-cases, such as tracking sequences of events, transactions
//! or replicated state machines.
//!
//! This implementation of the event log data structure uses log segments
//! that roll over at pre-defined maximum size boundaries. The messages appended
//! to the log have a unique, monotonically increasing offset that can be used
//! as a pointer to a log entry.
//!
//! The index of the event log logically stores the offset to a position in a
//! log segment. The index and segments are separated, in that a
//! segment file does not necessarily correspond to one particular segment file,
//! it could contain file pointers to many segment files. In addition, index
//! files are memory-mapped for efficient read and write access.
//!
//! ## Example
//!
//! ```rust
//! use eventus::*;
//! use eventus::message::*;
//!
//! #[tokio::main]
//! fn main() {
//!     // open a directory called 'log' for segment and index storage
//!     let opts = LogOptions::new("log");
//!     let mut log = EventLog::new(opts).unwrap();
//!
//!     // append to the log
//!     log.append_msg("my_stream", "hello world").unwrap(); // offset 0
//!     log.append_msg("my_stream", "second message").unwrap(); // offset 1
//!
//!     // read the messages
//!     let messages = log.read(0, ReadLimit::default()).unwrap();
//!     for msg in messages.iter() {
//!         println!("{} - {}", msg.offset(), String::from_utf8_lossy(msg.payload()));
//!     }
//!
//!     // prints:
//!     //    0 - hello world
//!     //    1 - second message
//! }
//! ```

pub mod actor;
pub mod cli;
mod file_set;
mod index;
pub mod message;
pub mod reader;
mod segment;
pub mod server;
pub mod stream_index;
mod subscription_store;

#[cfg(test)]
mod testutil;

use std::{
    borrow::Cow,
    fmt, fs, io,
    iter::{DoubleEndedIterator, ExactSizeIterator},
    mem,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use file_set::{FileSet, SegmentSet};
use index::{IndexBuf, RangeFindError, INDEX_ENTRY_BYTES};
use message::{MessageBuf, MessageError, MessageSet, MessageSetMut};
use reader::{LogSliceReader, MessageBufReader};
use segment::SegmentAppendError;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::broadcast;
use tracing::{debug, info, trace};

#[cfg(feature = "internals")]
pub use crate::{index::Index, index::IndexBuf, segment::Segment, stream_index::StreamIndex};

/// Offset of an appended log segment.
pub type Offset = u64;

/// Offset range of log append.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct OffsetRange(u64, usize);

impl OffsetRange {
    /// Starting offset of the range.
    pub fn first(&self) -> Offset {
        self.0
    }

    /// Number of offsets within the range.
    pub fn len(&self) -> usize {
        self.1
    }

    /// Boolean indicating whether the range has offsets.
    pub fn is_empty(&self) -> bool {
        self.1 == 0
    }

    /// Iterator containing all offsets within the offset range.
    pub fn iter(&self) -> OffsetRangeIter {
        OffsetRangeIter {
            pos: self.0,
            end: self.0 + (self.1 as u64),
            size: self.1,
        }
    }
}

/// Iterator of offsets within an `OffsetRange`.
#[derive(Copy, Clone, Debug)]
pub struct OffsetRangeIter {
    pos: u64,
    end: u64,
    size: usize,
}

impl Iterator for OffsetRangeIter {
    type Item = Offset;
    fn next(&mut self) -> Option<Offset> {
        if self.pos >= self.end {
            None
        } else {
            let v = self.pos;
            self.pos += 1;
            Some(v)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.size))
    }
}

impl ExactSizeIterator for OffsetRangeIter {
    fn len(&self) -> usize {
        self.size
    }
}

impl DoubleEndedIterator for OffsetRangeIter {
    fn next_back(&mut self) -> Option<Offset> {
        if self.pos >= self.end {
            None
        } else {
            let v = self.end - 1;
            self.end -= 1;
            Some(v)
        }
    }
}

/// Error enum for event log Append operation.
#[derive(Error, Debug)]
pub enum AppendError {
    /// The underlying file operations failed during the append attempt.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// A new index was created, but was unable to receive writes
    /// during the append operation. This could point to exhaustion
    /// of machine resources or other I/O issue.
    #[error("fresh index not writable")]
    FreshIndexNotWritable,
    /// A new segment was created, but was unable to receive writes
    /// during the append operation. This could point to exhaustion
    /// of machine resources or other I/O issue.
    #[error("fresh segment not writable")]
    FreshSegmentNotWritable,
    /// If a message that is larger than the per message size is tried to be
    /// appended it will not be allowed an will return an error.
    #[error("maximum message size exceeded")]
    MessageSizeExceeded,
    /// The buffer contains an invalid offset value.
    #[error("invalid offset")]
    InvalidOffset,
    #[error(transparent)]
    ReadError(#[from] ReadError),
    /// An event failed to serialize.
    #[error(transparent)]
    SerializeEvent(#[from] rmp_serde::encode::Error),
    /// Wrong expected version
    #[error("current stream position is {current:?} but expected {expected:?}")]
    WrongExpectedVersion {
        current: CurrentVersion,
        expected: ExpectedVersion,
    },
}

/// Error enum for event log read operation.
#[derive(Error, Debug)]
pub enum ReadError {
    /// Underlying IO error encountered by reading from the log
    #[error("transparent")]
    Io(#[from] io::Error),
    /// A segment in the log is corrupt, or the index itself is corrupt
    #[error("Corrupt log")]
    CorruptLog,
    /// Offset supplied was not invalid.
    #[error("Offset does not exist")]
    NoSuchSegment,
    /// Failed to deserialize message.
    #[error(transparent)]
    Deserialize(#[from] rmp_serde::decode::Error),
}

/// Batch size limitation on read.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Ord, PartialOrd)]
pub struct ReadLimit(usize);
impl ReadLimit {
    /// Read limit byte number of bytes.
    pub fn max_bytes(n: usize) -> ReadLimit {
        ReadLimit(n)
    }
}

impl Default for ReadLimit {
    fn default() -> ReadLimit {
        // 8kb default
        ReadLimit(8 * 1024)
    }
}

impl From<MessageError> for ReadError {
    fn from(err: MessageError) -> ReadError {
        match err {
            MessageError::IoError(err) => ReadError::Io(err),
            MessageError::InvalidHash | MessageError::InvalidPayloadLength => ReadError::CorruptLog,
        }
    }
}

impl From<RangeFindError> for ReadError {
    fn from(err: RangeFindError) -> ReadError {
        match err {
            RangeFindError::OffsetNotAppended => ReadError::NoSuchSegment,
            RangeFindError::MessageExceededMaxBytes => ReadError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Message exceeded max byte size",
            )),
        }
    }
}

/// Event log options allow customization of the event log behavior.
#[derive(Clone, Debug)]
pub struct LogOptions {
    log_dir: PathBuf,
    log_max_bytes: usize,
    log_max_entries: u64,
    index_max_bytes: usize,
    message_max_bytes: usize,
}

impl LogOptions {
    /// Creates minimal log options value with a directory containing the log.
    ///
    /// The default values are:
    /// - *segment_max_bytes*: 256MB
    /// - *index_max_entries*: 800,000
    /// - *message_max_bytes*: 1mb
    pub fn new<P>(log_dir: P) -> LogOptions
    where
        P: AsRef<Path>,
    {
        LogOptions {
            log_dir: log_dir.as_ref().to_owned(),
            log_max_bytes: 256 * 1024 * 1024,
            log_max_entries: 256_000,
            index_max_bytes: 800_000,
            message_max_bytes: 1_000_000,
        }
    }

    /// Bounds the size of a log segment to a number of bytes.
    #[inline]
    pub fn segment_max_bytes(&mut self, bytes: usize) -> &mut LogOptions {
        self.log_max_bytes = bytes;
        self
    }

    /// Bounds the size of a log segment to a number of bytes.
    #[inline]
    pub fn segment_max_entries(&mut self, entries: u64) -> &mut LogOptions {
        self.log_max_entries = entries;
        self
    }

    /// Bounds the size of an individual memory-mapped index file.
    #[inline]
    pub fn index_max_items(&mut self, items: usize) -> &mut LogOptions {
        // TODO: this should be renamed to starting bytes
        self.index_max_bytes = items * INDEX_ENTRY_BYTES;
        self
    }

    /// Bounds the size of a message to a number of bytes.
    #[inline]
    pub fn message_max_bytes(&mut self, bytes: usize) -> &mut LogOptions {
        self.message_max_bytes = bytes;
        self
    }
}

/// The event log is an append-only sequence of messages.
pub struct EventLog {
    file_set: FileSet,
    unflushed: Vec<Event<'static>>,
    broadcaster: broadcast::Sender<Vec<Event<'static>>>,
    conn: rusqlite::Connection,
}

impl EventLog {
    /// Creates or opens an existing event log.
    pub fn new(opts: LogOptions) -> io::Result<EventLog> {
        let _ = fs::create_dir_all(&opts.log_dir);
        let conn = subscription_store::setup_db(&opts.log_dir)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        debug!(
            "opening log in directory {:?}",
            &opts.log_dir.to_str().unwrap_or_default()
        );

        let fs = FileSet::load_log(opts)?;
        Ok(EventLog {
            file_set: fs,
            unflushed: Vec::with_capacity(256),
            broadcaster: broadcast::channel(32).0,
            conn,
        })
    }

    pub fn append_to_stream(
        &mut self,
        stream_id: impl Into<String>,
        expected_version: ExpectedVersion,
        events: Vec<NewEvent<'static>>,
        timestamp: DateTime<Utc>,
    ) -> Result<OffsetRange, AppendError> {
        let mut offsets = OffsetRange(0, 0);

        if events.is_empty() {
            return Ok(offsets);
        }

        let stream_id = stream_id.into();
        let current_version = match self.read_last_stream_msg(&stream_id)? {
            Some(event) => CurrentVersion::Current(event.stream_version),
            None => CurrentVersion::NoStream,
        };
        expected_version.validate(current_version)?;

        for (i, event) in events.into_iter().enumerate() {
            let i = i as u64;
            let stream_version = match current_version {
                CurrentVersion::Current(n) => n + 1 + i,
                CurrentVersion::NoStream => i,
            };
            let offset =
                self.append_to_stream_single(stream_id.clone(), stream_version, event, timestamp)?;
            if offsets.is_empty() {
                offsets = OffsetRange(offset, 1);
            } else {
                offsets.1 += 1;
            }
        }

        Ok(offsets)
    }

    /// Appends events to the log, returning the offsets appended.
    fn append_to_stream_single(
        &mut self,
        stream_id: String,
        stream_version: u64,
        event: NewEvent<'static>,
        timestamp: DateTime<Utc>,
    ) -> Result<Offset, AppendError> {
        let id = self.file_set.active_index_mut().next_offset();
        let event = Event {
            id,
            stream_id: Cow::Owned(stream_id),
            stream_version,
            event_name: Cow::Owned(event.event_name.into_owned()),
            event_data: Cow::Owned(event.event_data.into_owned()),
            metadata: Cow::Owned(event.metadata.into_owned()),
            timestamp,
        };
        let bytes = rmp_serde::to_vec(&event)?;
        let mut buf = MessageBuf::default();
        buf.push(bytes)
            .expect("Serialized event exceeds usize::MAX");
        message::set_offsets(&mut buf, id);
        let res = self.append_with_offsets(&event.stream_id, &buf)?;
        assert_eq!(res.len(), 1);

        self.unflushed.push(event);

        Ok(res.first())
    }

    /// Appends a single message to the log, returning the offset appended.
    #[inline]
    pub fn append_msg<B: AsRef<[u8]>>(
        &mut self,
        stream_name: &str,
        payload: B,
    ) -> Result<Offset, AppendError> {
        let mut buf = MessageBuf::default();
        buf.push(payload).expect("Payload size exceeds usize::MAX");
        let res = self.append(stream_name, &mut buf)?;
        assert_eq!(res.len(), 1);
        Ok(res.first())
    }

    /// Appends log entrites to the event log, returning the offsets appended.
    #[inline]
    pub fn append<T>(&mut self, stream_name: &str, buf: &mut T) -> Result<OffsetRange, AppendError>
    where
        T: MessageSetMut,
    {
        let start_off = self.file_set.active_index_mut().next_offset();
        message::set_offsets(buf, start_off);
        self.append_with_offsets(stream_name, buf)
    }

    /// Appends log entrites to the event log, returning the offsets appended.
    ///
    /// The offsets are expected to already be set within the buffer.
    pub fn append_with_offsets<T>(
        &mut self,
        stream_name: &str,
        buf: &T,
    ) -> Result<OffsetRange, AppendError>
    where
        T: MessageSet,
    {
        let buf_len = buf.len();
        if buf_len == 0 {
            return Ok(OffsetRange(0, 0));
        }

        // Check if given message exceeded the max size
        if buf.bytes().len() > self.file_set.log_options().message_max_bytes {
            return Err(AppendError::MessageSizeExceeded);
        }

        // first write to the current segment
        let start_off = self.next_offset();

        // check to make sure the first message matches the starting offset
        if buf.iter().next().unwrap().offset() != start_off {
            return Err(AppendError::InvalidOffset);
        }

        // Check to make sure we aren't exceeding the log max entries
        let starting_offset = self.file_set.active_index().starting_offset();
        let ending_offset = start_off + buf.len() as u64;
        if ending_offset - starting_offset > self.file_set.log_options().log_max_entries {
            self.file_set.roll_segment()?;
        }

        let meta = match self.file_set.active_segment_mut().append(buf) {
            Ok(meta) => meta,
            // if the log is full, gracefully close the current segment
            // and create new one starting from the new offset
            Err(SegmentAppendError::LogFull) => {
                self.file_set.roll_segment()?;

                // try again, giving up if we have to
                self.file_set
                    .active_segment_mut()
                    .append(buf)
                    .map_err(|_| AppendError::FreshSegmentNotWritable)?
            }
            Err(SegmentAppendError::IoError(err)) => return Err(AppendError::Io(err)),
        };
        self.file_set.active_segment_mut().flush_writer()?;

        // write to the index
        {
            // TODO: reduce indexing of every message
            let index = self.file_set.active_index_mut();
            let mut index_pos_buf = IndexBuf::new(buf_len, index.starting_offset());
            let mut pos = meta.starting_position;
            for m in buf.iter() {
                index_pos_buf.push(m.offset(), pos as u32);
                pos += m.total_bytes();
            }
            // TODO: what happens when this errors out? Do we truncate the log...?
            index.append(index_pos_buf)?;
        }

        // write to the stream index
        {
            let stream_index = self.file_set.active_stream_index_mut();
            for m in buf.iter() {
                stream_index.insert(stream_name, m.offset())?;
            }
        }

        Ok(OffsetRange(start_off, buf_len))
    }

    /// Gets the last written offset.
    pub fn last_offset(&self) -> Option<Offset> {
        let next_off = self.file_set.active_index().next_offset();
        if next_off == 0 {
            None
        } else {
            Some(next_off - 1)
        }
    }

    /// Gets the latest offset
    #[inline]
    pub fn next_offset(&self) -> Offset {
        self.file_set.active_index().next_offset()
    }

    pub fn read_last_stream_msg<'a>(
        &'a mut self,
        stream_name: &'a str,
    ) -> Result<Option<Event>, ReadError> {
        let Some(last_offset) = self
            .file_set
            .active_stream_index()
            .last(stream_name)?
            .map(Ok)
            .or_else(|| {
                self.file_set.closed().values().rev().find_map(
                    |SegmentSet { stream_index, .. }| match stream_index.last(stream_name) {
                        Ok(offset) => Ok(offset).transpose(),
                        Err(err) => Some(Err(err)),
                    },
                )
            })
            .transpose()?
        else {
            return Ok(None);
        };

        // if last_offset >= self.file_set.active_index().next_offset() {}

        let events = self.read(last_offset, ReadLimit::default())?;
        let event = match events.into_iter().next() {
            Some(event) => event,
            None => {
                // In this case the active stream index was flushed before the segment file
                // (memory mapped files can automatically flush). And so we need to truncate this stream at the next offset - 1.
                // TODO: ...
                // Then call read_last_stream_msg again after truncating
                // todo!();
                // return self.read_last_stream_msg(stream_name);
                return Err(ReadError::CorruptLog);
            }
        };

        Ok(Some(event))
    }

    pub fn read_stream<'a>(
        &'a self,
        stream_name: &'a str,
        stream_version_start: u64,
    ) -> ReadStreamIter<'a, impl Iterator<Item = u64> + 'a> {
        let stream_index_iter = self
            .file_set
            .closed()
            .values()
            .flat_map(|SegmentSet { stream_index, .. }| stream_index.iter(stream_name))
            .chain(self.file_set.active_stream_index().iter(stream_name))
            .skip(stream_version_start as usize);

        ReadStreamIter {
            log: self,
            stream_index_iter,
        }
    }

    /// Reads a portion of the log, starting with the `start` offset, inclusive,
    /// up to the limit.
    #[inline]
    pub fn read(&self, start: Offset, limit: ReadLimit) -> Result<Vec<Event<'static>>, ReadError> {
        let mut rd = MessageBufReader;
        match self.reader(&mut rd, start, limit)? {
            Some(buf) => buf
                .iter()
                .map(|msg| rmp_serde::from_slice(msg.payload()).map_err(ReadError::Deserialize))
                .collect(),
            None => Ok(vec![]),
        }
    }

    /// Reads a portion of the log, starting with the `start` offset, inclusive,
    /// up to the limit via the reader.
    pub fn reader<R: LogSliceReader>(
        &self,
        reader: &mut R,
        mut start: Offset,
        limit: ReadLimit,
    ) -> Result<Option<R::Result>, ReadError> {
        // TODO: can this be caught at the index level insead?
        if start >= self.file_set.active_index().next_offset() {
            return Ok(None);
        }

        // adjust for the minimum offset (e.g. reader requests an offset that was
        // truncated)
        match self.file_set.min_offset() {
            Some(min_off) if min_off > start => {
                start = min_off;
            }
            None => return Ok(None),
            _ => {}
        }

        let max_bytes = limit.0 as u32;

        // find the correct segment
        let SegmentSet { segment, index, .. } = self.file_set.find(start);
        let seg_bytes = segment.size() as u32;

        // grab the range from the contained index
        let range = index.find_segment_range(start, max_bytes, seg_bytes)?;
        if range.bytes() == 0 {
            Ok(None)
        } else {
            Ok(Some(segment.read_slice(
                reader,
                range.file_position(),
                range.bytes(),
            )?))
        }
    }

    /// Truncates a file after the offset supplied. The resulting log will
    /// contain entries up to the offset.
    pub fn truncate(&mut self, offset: Offset) -> io::Result<()> {
        info!("Truncating log to offset {}", offset);

        // remove index/segment files rolled after the offset
        let segments_to_remove = self.file_set.remove_after(offset);
        Self::delete_segments(segments_to_remove)?;

        // truncate the current index
        match self.file_set.active_index_mut().truncate(offset) {
            Some(len) => self.file_set.active_segment_mut().truncate(len),
            // index outside of appended range
            None => Ok(()),
        }
    }

    /// Removes segment files that are before (strictly less than) the specified
    /// offset. The log might contain some messages before the offset
    /// provided is in the middle of a segment.
    pub fn trim_segments_before(&mut self, offset: Offset) -> io::Result<()> {
        let segments_to_remove = self.file_set.remove_before(offset);
        Self::delete_segments(segments_to_remove)
    }

    /// Removes segment files that are read-only.
    pub fn trim_inactive_segments(&mut self) -> io::Result<()> {
        let active_offset_start = self.file_set.active_index().starting_offset();
        self.trim_segments_before(active_offset_start)
    }

    /// Forces a flush of the log.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.unflushed.is_empty() {
            return Ok(());
        }
        info!("flushing {} events", self.unflushed.len());
        self.file_set.active_index_mut().flush()?;
        self.file_set.active_stream_index_mut().flush()?;
        self.file_set.active_segment_mut().flush()?; // After this line, the events are considered as persisted.
        let _ = self.broadcaster.send(mem::take(&mut self.unflushed));
        assert!(self.unflushed.is_empty());
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Vec<Event<'static>>> {
        self.broadcaster.subscribe()
    }

    fn delete_segments(segments: Vec<SegmentSet>) -> io::Result<()> {
        for p in segments {
            trace!(
                "Removing segment and index starting at {}",
                p.segment.starting_offset()
            );
            p.segment.remove()?;
            p.index.remove()?;
            p.stream_index.remove()?;
        }
        Ok(())
    }

    pub fn load_subscription(&mut self, id: &str) -> rusqlite::Result<Option<u64>> {
        tokio::task::block_in_place(move || subscription_store::load_subscription(&self.conn, id))
    }

    pub fn update_subscription(&mut self, id: &str, last_event_id: u64) -> rusqlite::Result<()> {
        tokio::task::block_in_place(move || {
            subscription_store::update_subscription(&self.conn, id, last_event_id)
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event<'a> {
    pub id: u64,
    pub stream_id: Cow<'a, str>,
    pub stream_version: u64,
    pub event_name: Cow<'a, str>,
    pub event_data: Cow<'a, [u8]>,
    pub metadata: Cow<'a, [u8]>,
    pub timestamp: DateTime<Utc>,
}

impl<'a> Event<'a> {
    pub fn borrowed(&'a self) -> Event<'a> {
        Event {
            id: self.id,
            stream_id: Cow::Borrowed(&self.stream_id),
            stream_version: self.stream_version,
            event_name: Cow::Borrowed(&self.event_name),
            event_data: Cow::Borrowed(&self.event_data),
            metadata: Cow::Borrowed(&self.metadata),
            timestamp: self.timestamp,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NewEvent<'a> {
    pub event_name: Cow<'a, str>,
    pub event_data: Cow<'a, [u8]>,
    pub metadata: Cow<'a, [u8]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedVersion {
    /// This write should not conflict with anything and should always succeed.
    Any,
    /// The stream should exist. If it or a metadata stream does not exist,
    /// treats that as a concurrency problem.
    StreamExists,
    /// The stream being written to should not yet exist. If it does exist,
    /// treats that as a concurrency problem.
    NoStream,
    /// States that the last event written to the stream should have an event
    /// number matching your expected value.
    Exact(u64),
}

impl ExpectedVersion {
    pub fn validate(&self, current: CurrentVersion) -> Result<(), AppendError> {
        use CurrentVersion::NoStream as CurrentNoStream;
        use CurrentVersion::*;
        use ExpectedVersion::NoStream as ExpectedNoStream;
        use ExpectedVersion::*;

        match (self, current) {
            (Any, _) | (StreamExists, Current(_)) | (ExpectedNoStream, CurrentNoStream) => Ok(()),
            (Exact(e), Current(c)) if *e == c => Ok(()),
            (Exact(_), Current(_))
            | (StreamExists, CurrentNoStream)
            | (ExpectedNoStream, Current(_))
            | (Exact(_), CurrentNoStream) => Err(AppendError::WrongExpectedVersion {
                current,
                expected: *self,
            }),
        }
    }
}

impl fmt::Display for ExpectedVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpectedVersion::Any => write!(f, "any"),
            ExpectedVersion::StreamExists => write!(f, "stream exists"),
            ExpectedVersion::NoStream => write!(f, "no stream"),
            ExpectedVersion::Exact(version) => version.fmt(f),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Actual position of a stream.
pub enum CurrentVersion {
    /// The last event's number.
    Current(u64),
    /// The stream doesn't exist.
    NoStream,
}

impl CurrentVersion {
    pub fn as_expected_version(&self) -> ExpectedVersion {
        match self {
            CurrentVersion::Current(version) => ExpectedVersion::Exact(*version),
            CurrentVersion::NoStream => ExpectedVersion::NoStream,
        }
    }
}

impl fmt::Display for CurrentVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CurrentVersion::Current(version) => version.fmt(f),
            CurrentVersion::NoStream => write!(f, "<no stream>"),
        }
    }
}

pub struct ReadStreamIter<'a, I> {
    log: &'a EventLog,
    stream_index_iter: I,
}

impl<'a, I> Iterator for ReadStreamIter<'a, I>
where
    I: Iterator<Item = u64>,
{
    type Item = Result<Event<'static>, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.stream_index_iter.next()?;
        self.log
            .read(offset, ReadLimit::default())
            .map(|read| read.into_iter().next())
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::{message::*, testutil::*, *};

    use std::{collections::HashSet, fs};

    #[test]
    pub fn offset_range() {
        let range = OffsetRange(2, 6);

        assert_eq!(vec![2, 3, 4, 5, 6, 7], range.iter().collect::<Vec<u64>>());

        assert_eq!(
            vec![7, 6, 5, 4, 3, 2],
            range.iter().rev().collect::<Vec<u64>>()
        );
    }

    #[test]
    pub fn append() {
        let dir = TestDir::new();
        let mut log = EventLog::new(LogOptions::new(&dir)).unwrap();
        assert_eq!(log.append_msg("my_stream", "123456").unwrap(), 0);
        assert_eq!(log.append_msg("my_stream", "abcdefg").unwrap(), 1);
        assert_eq!(log.append_msg("my_stream", "foobarbaz").unwrap(), 2);
        assert_eq!(log.append_msg("my_stream", "bing").unwrap(), 3);
        log.flush().unwrap();
    }

    #[test]
    pub fn append_multiple() {
        let dir = TestDir::new();
        let mut log = EventLog::new(LogOptions::new(&dir)).unwrap();
        let mut buf = {
            let mut buf = MessageBuf::default();
            buf.push(b"123456").unwrap();
            buf.push(b"789012").unwrap();
            buf.push(b"345678").unwrap();
            buf
        };
        let range = log.append("my_stream", &mut buf).unwrap();
        assert_eq!(0, range.first());
        assert_eq!(3, range.len());
        assert_eq!(vec![0, 1, 2], range.iter().collect::<Vec<u64>>());
    }

    #[test]
    pub fn append_new_segment() {
        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.segment_max_bytes(62);

        {
            let mut log = EventLog::new(opts).unwrap();
            // first 2 entries fit (both 30 bytes with encoding)
            log.append_msg("my_stream", "0123456789").unwrap();
            log.append_msg("my_stream", "0123456789").unwrap();

            // this one should roll the log
            log.append_msg("my_stream", "0123456789").unwrap();
            log.flush().unwrap();
        }

        expect_files(
            &dir,
            vec![
                "00000000000000000000.index",
                "00000000000000000000.log",
                "00000000000000000000.streams",
                "00000000000000000002.log",
                "00000000000000000002.index",
                "00000000000000000002.streams",
            ],
        );
    }

    #[test]
    pub fn read_entries() {
        env_logger::try_init().unwrap_or(());

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(1000);
        let mut log = EventLog::new(opts).unwrap();

        for i in 0..100 {
            let s = format!("-data {}", i);
            log.append_msg("my_stream", s.as_str()).unwrap();
        }
        log.flush().unwrap();

        {
            let active_index_read = log.read(82, ReadLimit::max_bytes(168)).unwrap();
            assert_eq!(6, active_index_read.len());
            assert_eq!(
                vec![82, 83, 84, 85, 86, 87],
                active_index_read.iter().map(|v| v.id).collect::<Vec<_>>()
            );
        }

        {
            let old_index_read = log.read(5, ReadLimit::max_bytes(112)).unwrap();
            assert_eq!(4, old_index_read.len());
            assert_eq!(
                vec![5, 6, 7, 8],
                old_index_read.iter().map(|v| v.id).collect::<Vec<_>>()
            );
        }

        // read at the boundary (not going to get full message limit)
        {
            // log rolls at offset 36
            let boundary_read = log.read(33, ReadLimit::max_bytes(100)).unwrap();
            assert_eq!(3, boundary_read.len());
            assert_eq!(
                vec![33, 34, 35],
                boundary_read.iter().map(|v| v.id).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    pub fn reopen_log() {
        env_logger::try_init().unwrap_or(());

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(1000);

        {
            let mut log = EventLog::new(opts.clone()).unwrap();

            for i in 0..99 {
                let s = format!("some data {}", i);
                let off = log.append_msg("my_stream", s.as_str()).unwrap();
                assert_eq!(i, off);
            }
            log.flush().unwrap();
        }

        {
            let mut log = EventLog::new(opts).unwrap();

            let active_index_read = log.read(82, ReadLimit::max_bytes(130)).unwrap();

            assert_eq!(4, active_index_read.len());
            assert_eq!(
                vec![82, 83, 84, 85],
                active_index_read.iter().map(|v| v.id).collect::<Vec<_>>()
            );

            let off = log.append_msg("my_stream", "moar data").unwrap();
            assert_eq!(99, off);
        }
    }

    #[test]
    pub fn reopen_log_without_segment_write() {
        env_logger::try_init().unwrap_or(());

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(1000);

        {
            let mut log = EventLog::new(opts.clone()).unwrap();
            log.flush().unwrap();
        }

        {
            EventLog::new(opts.clone()).expect("Should be able to reopen log without writes");
        }

        {
            EventLog::new(opts).expect("Should be able to reopen log without writes");
        }
    }

    #[test]
    pub fn reopen_log_with_one_segment_write() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        let opts = LogOptions::new(&dir);
        {
            let mut log = EventLog::new(opts.clone()).unwrap();
            log.append_msg("my_stream", "Test").unwrap();
            log.flush().unwrap();
        }
        {
            let log = EventLog::new(opts).unwrap();
            assert_eq!(1, log.next_offset());
        }
    }

    #[test]
    pub fn append_message_greater_than_max() {
        let dir = TestDir::new();
        let mut log = EventLog::new(LogOptions::new(&dir)).unwrap();
        //create vector with 1.2mb of size, u8 = 1 byte thus,
        //1mb = 1000000 bytes, 1200000 items needed
        let mut value = String::new();
        let mut target = 0;
        while target != 2000000 {
            value.push('a');
            target += 1;
        }
        let res = log.append_msg("my_stream", value);
        //will fail if no error is found which means a message greater than the limit
        // passed through
        assert!(res.is_err());
        log.flush().unwrap();
    }

    #[test]
    pub fn truncate_from_active() {
        let dir = TestDir::new();
        let mut log = EventLog::new(LogOptions::new(&dir)).unwrap();

        // append 5 messages
        {
            let mut buf = MessageBuf::default();
            buf.push(b"123456").unwrap();
            buf.push(b"789012").unwrap();
            buf.push(b"345678").unwrap();
            buf.push(b"aaaaaa").unwrap();
            buf.push(b"bbbbbb").unwrap();
            log.append("my_stream", &mut buf).unwrap();
        }

        // truncate to offset 2 (should remove 2 messages)
        log.truncate(2).expect("Unable to truncate file");

        assert_eq!(Some(2), log.last_offset());
    }

    #[test]
    pub fn truncate_after_offset_removes_segments() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = EventLog::new(opts).unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").unwrap();
            }
        }

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000000.index",
                "00000000000000000000.log",
                "00000000000000000000.streams",
                "00000000000000000002.log",
                "00000000000000000002.streams",
                "00000000000000000002.index",
                "00000000000000000004.log",
                "00000000000000000004.streams",
                "00000000000000000004.index",
                "00000000000000000006.log",
                "00000000000000000006.streams",
                "00000000000000000006.index",
            ],
        );

        // truncate to offset 2 (should remove 2 messages)
        log.truncate(3).expect("Unable to truncate file");

        assert_eq!(Some(3), log.last_offset());

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000000.index",
                "00000000000000000000.log",
                "00000000000000000000.streams",
                "00000000000000000002.log",
                "00000000000000000002.index",
                "00000000000000000002.streams",
            ],
        );
    }

    #[test]
    pub fn truncate_at_segment_boundary_removes_segments() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = EventLog::new(opts).unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").unwrap();
            }
        }

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000000.index",
                "00000000000000000000.log",
                "00000000000000000000.streams",
                "00000000000000000002.log",
                "00000000000000000002.index",
                "00000000000000000002.streams",
                "00000000000000000004.log",
                "00000000000000000004.index",
                "00000000000000000004.streams",
                "00000000000000000006.log",
                "00000000000000000006.index",
                "00000000000000000006.streams",
            ],
        );

        // truncate to offset 2 (should remove 2 messages)
        log.truncate(2).expect("Unable to truncate file");

        assert_eq!(Some(2), log.last_offset());

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000000.index",
                "00000000000000000000.log",
                "00000000000000000000.streams",
                "00000000000000000002.log",
                "00000000000000000002.index",
                "00000000000000000002.streams",
            ],
        );
    }

    #[test]
    pub fn truncate_after_last_append_does_nothing() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = EventLog::new(opts).unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").unwrap();
            }
        }

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000000.index",
                "00000000000000000000.log",
                "00000000000000000000.streams",
                "00000000000000000002.log",
                "00000000000000000002.streams",
                "00000000000000000002.index",
                "00000000000000000004.log",
                "00000000000000000004.streams",
                "00000000000000000004.index",
                "00000000000000000006.log",
                "00000000000000000006.streams",
                "00000000000000000006.index",
            ],
        );

        // truncate to offset 2 (should remove 2 messages)
        log.truncate(7).expect("Unable to truncate file");

        assert_eq!(Some(6), log.last_offset());

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000000.index",
                "00000000000000000000.log",
                "00000000000000000000.streams",
                "00000000000000000002.log",
                "00000000000000000002.index",
                "00000000000000000002.streams",
                "00000000000000000004.log",
                "00000000000000000004.index",
                "00000000000000000004.streams",
                "00000000000000000006.log",
                "00000000000000000006.index",
                "00000000000000000006.streams",
            ],
        );
    }

    #[test]
    pub fn trim_segments_before_removes_segments() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = EventLog::new(opts).unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").unwrap();
            }
        }

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000000.index",
                "00000000000000000000.log",
                "00000000000000000000.streams",
                "00000000000000000002.log",
                "00000000000000000002.index",
                "00000000000000000002.streams",
                "00000000000000000004.log",
                "00000000000000000004.index",
                "00000000000000000004.streams",
                "00000000000000000006.log",
                "00000000000000000006.index",
                "00000000000000000006.streams",
            ],
        );

        // remove segments < 3 which is just segment 0
        log.trim_segments_before(3)
            .expect("Unable to truncate file");

        assert_eq!(Some(6), log.last_offset());

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000002.index",
                "00000000000000000002.log",
                "00000000000000000002.streams",
                "00000000000000000004.log",
                "00000000000000000004.index",
                "00000000000000000004.streams",
                "00000000000000000006.log",
                "00000000000000000006.index",
                "00000000000000000006.streams",
            ],
        );

        // make sure the messages are really gone
        let reader = log
            .read(0, ReadLimit::default())
            .expect("Unabled to grab reader");
        assert_eq!(2, reader.iter().next().unwrap().id);
    }

    #[test]
    pub fn trim_segments_before_removes_segments_at_boundary() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = EventLog::new(opts).unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").unwrap();
            }
        }

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000000.index",
                "00000000000000000000.log",
                "00000000000000000000.streams",
                "00000000000000000002.log",
                "00000000000000000002.index",
                "00000000000000000002.streams",
                "00000000000000000004.log",
                "00000000000000000004.index",
                "00000000000000000004.streams",
                "00000000000000000006.log",
                "00000000000000000006.index",
                "00000000000000000006.streams",
            ],
        );

        // remove segments < 3 which is just segment 0
        log.trim_segments_before(4)
            .expect("Unable to truncate file");

        assert_eq!(Some(6), log.last_offset());

        // ensure we have the expected index/logs
        expect_files(
            &dir,
            vec![
                "00000000000000000004.log",
                "00000000000000000004.index",
                "00000000000000000004.streams",
                "00000000000000000006.log",
                "00000000000000000006.index",
                "00000000000000000006.streams",
            ],
        );

        // make sure the messages are really gone
        let reader = log
            .read(0, ReadLimit::default())
            .expect("Unabled to grab reader");
        assert_eq!(4, reader.iter().next().unwrap().id);
    }

    #[test]
    pub fn trim_start_logic_check() {
        env_logger::try_init().unwrap_or(());
        const TOTAL_MESSAGES: u64 = 20;
        const TESTED_TRIM_START: u64 = TOTAL_MESSAGES + 1;

        for trim_off in 0..TESTED_TRIM_START {
            let dir = TestDir::new();
            let mut opts = LogOptions::new(&dir);
            opts.index_max_items(20);
            opts.segment_max_bytes(52);
            let mut log = EventLog::new(opts).unwrap();

            // append the messages
            {
                for _ in 0..TOTAL_MESSAGES {
                    log.append_msg("my_stream", b"12345").unwrap();
                }
            }

            log.trim_segments_before(trim_off)
                .expect("Unable to truncate file");
            assert_eq!(Some(TOTAL_MESSAGES - 1), log.last_offset());

            // make sure the messages are really gone
            let reader = log
                .read(0, ReadLimit::default())
                .expect("Unabled to grab reader");
            let start_off = reader.iter().next().unwrap().id;
            assert!(start_off <= trim_off);
        }
    }

    #[test]
    pub fn multiple_trim_start_calls() {
        env_logger::try_init().unwrap_or(());
        const TOTAL_MESSAGES: u64 = 20;
        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = EventLog::new(opts).unwrap();

        // append the messages
        {
            for _ in 0..TOTAL_MESSAGES {
                log.append_msg("my_stream", b"12345").unwrap();
            }
        }

        log.trim_segments_before(2).unwrap();

        {
            let reader = log
                .read(0, ReadLimit::default())
                .expect("Unabled to grab reader");
            assert_eq!(2, reader.iter().next().unwrap().id);
        }

        log.trim_segments_before(10).unwrap();

        {
            let reader = log
                .read(0, ReadLimit::default())
                .expect("Unabled to grab reader");
            assert_eq!(10, reader.iter().next().unwrap().id);
        }
    }

    #[test]
    pub fn trim_inactive_logic_check() {
        env_logger::try_init().unwrap_or(());
        const TOTAL_MESSAGES: u64 = 20;

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = EventLog::new(opts).unwrap();

        // append the messages
        {
            for _ in 0..TOTAL_MESSAGES {
                log.append_msg("my_stream", b"12345").unwrap();
            }
        }

        log.trim_inactive_segments()
            .expect("Unable to truncate file");
        assert_eq!(Some(TOTAL_MESSAGES - 1), log.last_offset());

        // make sure the messages are really gone
        let reader = log
            .read(0, ReadLimit::default())
            .expect("Unabled to grab reader");
        let start_off = reader.iter().next().unwrap().id;
        assert_eq!(16, start_off);
    }

    #[test]
    pub fn trim_inactive_logic_check_zero_messages() {
        env_logger::try_init().unwrap_or(());

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = EventLog::new(opts).unwrap();

        log.trim_inactive_segments()
            .expect("Unable to truncate file");
        assert_eq!(None, log.last_offset());

        // append the messages
        log.append_msg("my_stream", b"12345").unwrap();

        // make sure the messages are really gone
        let reader = log
            .read(0, ReadLimit::default())
            .expect("Unabled to grab reader");
        let start_off = reader.iter().next().unwrap().id;
        assert_eq!(0, start_off);
    }

    fn expect_files<P: AsRef<Path>, I>(dir: P, files: I)
    where
        I: IntoIterator<Item = &'static str>,
    {
        let dir_files = fs::read_dir(&dir)
            .unwrap()
            .map(|res| {
                res.unwrap()
                    .path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<HashSet<String>>();
        let expected = files
            .into_iter()
            .map(|s| s.to_string())
            .collect::<HashSet<String>>();
        assert_eq!(
            dir_files.len(),
            expected.len(),
            "Invalid file count, expected {:?} got {:?}",
            expected,
            dir_files
        );
        assert_eq!(
            dir_files.intersection(&expected).count(),
            expected.len(),
            "Invalid file count, expected {:?} got {:?}",
            expected,
            dir_files
        );
    }
}

#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
