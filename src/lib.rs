//! The commit log is an append-only data structure that can be used in a
//! variety of use-cases, such as tracking sequences of events, transactions
//! or replicated state machines.
//!
//! This implementation of the commit log data structure uses log segments
//! that roll over at pre-defined maximum size boundaries. The messages appended
//! to the log have a unique, monotonically increasing offset that can be used
//! as a pointer to a log entry.
//!
//! The index of the commit log logically stores the offset to a position in a
//! log segment. The index and segments are separated, in that a
//! segment file does not necessarily correspond to one particular segment file,
//! it could contain file pointers to many segment files. In addition, index
//! files are memory-mapped for efficient read and write access.
//!
//! ## Example
//!
//! ```rust
//! use commitlog::*;
//! use commitlog::message::*;
//!
//! #[tokio::main]
//! async fn main() {
//!     // open a directory called 'log' for segment and index storage
//!     let opts = LogOptions::new("log");
//!     let mut log = CommitLog::new(opts).await.unwrap();
//!
//!     // append to the log
//!     log.append_msg("my_stream", "hello world").await.unwrap(); // offset 0
//!     log.append_msg("my_stream", "second message").await.unwrap(); // offset 1
//!
//!     // read the messages
//!     let messages = log.read(0, ReadLimit::default()).await.unwrap();
//!     for msg in messages.iter() {
//!         println!("{} - {}", msg.offset(), String::from_utf8_lossy(msg.payload()));
//!     }
//!
//!     // prints:
//!     //    0 - hello world
//!     //    1 - second message
//! }
//! ```

mod actor;
mod file_set;
mod index;
pub mod message;
pub mod reader;
mod segment;
pub mod server;
mod stream_index;
mod subscription_store;

#[cfg(test)]
mod testutil;

use std::{
    borrow::Cow,
    iter::{DoubleEndedIterator, ExactSizeIterator},
    mem,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use index::{IndexBuf, RangeFindError, INDEX_ENTRY_BYTES};
use log::{info, trace};
use serde::{Deserialize, Serialize};

use file_set::{FileSet, SegmentSet};
use message::{MessageBuf, MessageError, MessageSet, MessageSetMut};
use reader::{LogSliceReader, MessageBufReader};
use segment::SegmentAppendError;
use thiserror::Error;
use tokio::{fs, io, sync::broadcast};

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

/// Error enum for commit log Append operation.
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

/// Error enum for commit log read operation.
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

/// Commit log options allow customization of the commit
/// log behavior.
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

/// The commit log is an append-only sequence of messages.
pub struct CommitLog {
    file_set: FileSet,
    unflushed: Vec<Event<'static>>,
    broadcaster: broadcast::Sender<Vec<Event<'static>>>,
    conn: rusqlite::Connection,
}

impl CommitLog {
    /// Creates or opens an existing commit log.
    pub async fn new(opts: LogOptions) -> io::Result<CommitLog> {
        let _ = fs::create_dir_all(&opts.log_dir).await;
        let conn = tokio::task::spawn_blocking(move || subscription_store::setup_db())
            .await?
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        info!("Opening log in directory {:?}", &opts.log_dir.to_str());

        let fs = FileSet::load_log(opts).await?;
        Ok(CommitLog {
            file_set: fs,
            unflushed: Vec::with_capacity(256),
            broadcaster: broadcast::channel(32).0,
            conn,
        })
    }

    pub async fn append_to_stream(
        &mut self,
        stream_id: impl Into<String>,
        expected_version: ExpectedVersion,
        events: Vec<NewEvent<'static>>,
    ) -> Result<OffsetRange, AppendError> {
        let mut offsets = OffsetRange(0, 0);

        if events.is_empty() {
            return Ok(offsets);
        }

        let stream_id = stream_id.into();
        let current_version = match self.read_last_stream_msg(&stream_id).await? {
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
            let offset = self
                .append_to_stream_single(stream_id.clone(), stream_version, event)
                .await?;
            if offsets.is_empty() {
                offsets = OffsetRange(offset, 1);
            } else {
                offsets.1 += 1;
            }
        }

        Ok(offsets)
    }

    /// Appends events to the log, returning the offsets appended.
    async fn append_to_stream_single(
        &mut self,
        stream_id: String,
        stream_version: u64,
        event: NewEvent<'static>,
    ) -> Result<Offset, AppendError> {
        let id = self.file_set.active_index_mut().next_offset();
        let event = Event {
            id,
            stream_id: Cow::Owned(stream_id),
            stream_version,
            event_name: Cow::Owned(event.event_name.into_owned()),
            event_data: Cow::Owned(event.event_data.into_owned()),
            metadata: Cow::Owned(event.metadata.into_owned()),
            timestamp: Utc::now(),
        };
        let bytes = rmp_serde::to_vec(&event)?;
        let mut buf = MessageBuf::default();
        buf.push(bytes)
            .expect("Serialized event exceeds usize::MAX");
        message::set_offsets(&mut buf, id);
        let res = self.append_with_offsets(&event.stream_id, &buf).await?;
        assert_eq!(res.len(), 1);

        self.unflushed.push(event);

        Ok(res.first())
    }

    /// Appends a single message to the log, returning the offset appended.
    #[inline]
    pub async fn append_msg<B: AsRef<[u8]>>(
        &mut self,
        stream_name: &str,
        payload: B,
    ) -> Result<Offset, AppendError> {
        let mut buf = MessageBuf::default();
        buf.push(payload).expect("Payload size exceeds usize::MAX");
        let res = self.append(stream_name, &mut buf).await?;
        assert_eq!(res.len(), 1);
        Ok(res.first())
    }

    /// Appends log entrites to the commit log, returning the offsets appended.
    #[inline]
    pub async fn append<T>(
        &mut self,
        stream_name: &str,
        buf: &mut T,
    ) -> Result<OffsetRange, AppendError>
    where
        T: MessageSetMut,
    {
        let start_off = self.file_set.active_index_mut().next_offset();
        message::set_offsets(buf, start_off);
        self.append_with_offsets(stream_name, buf).await
    }

    /// Appends log entrites to the commit log, returning the offsets appended.
    ///
    /// The offsets are expected to already be set within the buffer.
    pub async fn append_with_offsets<T>(
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
            self.file_set.roll_segment().await?;
        }

        let meta = match self.file_set.active_segment_mut().append(buf).await {
            Ok(meta) => meta,
            // if the log is full, gracefully close the current segment
            // and create new one starting from the new offset
            Err(SegmentAppendError::LogFull) => {
                self.file_set.roll_segment().await?;

                // try again, giving up if we have to
                self.file_set
                    .active_segment_mut()
                    .append(buf)
                    .await
                    .map_err(|_| AppendError::FreshSegmentNotWritable)?
            }
            Err(SegmentAppendError::IoError(err)) => return Err(AppendError::Io(err)),
        };

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
            index.append(index_pos_buf).await?;
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

    pub async fn read_last_stream_msg(
        &mut self,
        stream_name: &str,
    ) -> Result<Option<Event>, ReadError> {
        let Some(last_offset) = self
            .file_set
            .active_stream_index()
            .get(stream_name)?
            .last()
            .copied()
            .map(Ok)
            .or_else(|| {
                self.file_set.closed().values().rev().find_map(
                    |SegmentSet { stream_index, .. }| match stream_index.get(stream_name) {
                        Ok(offsets) => offsets.last().copied().map(Ok),
                        Err(err) => Some(Err(err)),
                    },
                )
            })
            .transpose()?
        else {
            return Ok(None);
        };

        let events = self.read(last_offset, ReadLimit::default()).await?;
        let event = events.into_iter().next().ok_or(ReadError::CorruptLog)?;
        Ok(Some(event))
    }

    pub async fn read_stream(&mut self, stream_name: &str) -> Result<Vec<Event>, ReadError> {
        // Iterate stream indexes
        let mut offsets: Vec<_> = self
            .file_set
            .closed()
            .values()
            .map(|SegmentSet { stream_index, .. }| stream_index.get(stream_name))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
        offsets.extend_from_slice(&self.file_set.active_stream_index().get(stream_name)?);

        let mut log = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let events = self.read(offset, ReadLimit::default()).await?;
            log.extend(events.into_iter());
        }

        Ok(log)
    }

    /// Reads a portion of the log, starting with the `start` offset, inclusive,
    /// up to the limit.
    #[inline]
    pub async fn read(
        &mut self,
        start: Offset,
        limit: ReadLimit,
    ) -> Result<Vec<Event<'static>>, ReadError> {
        todo!()
        // let mut rd = MessageBufReader;
        // match self.reader(&mut rd, start, limit).await? {
        //     Some(buf) => buf
        //         .iter()
        //         .map(|msg| rmp_serde::from_slice(msg.payload()).map_err(ReadError::Deserialize))
        //         .collect(),
        //     None => Ok(vec![]),
        // }
    }

    /// Reads a portion of the log, starting with the `start` offset, inclusive,
    /// up to the limit via the reader.
    pub async fn reader<R: LogSliceReader>(
        &mut self,
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
            Ok(Some(
                segment
                    .read_slice(reader, range.file_position(), range.bytes())
                    .await?,
            ))
        }
    }

    /// Truncates a file after the offset supplied. The resulting log will
    /// contain entries up to the offset.
    pub async fn truncate(&mut self, offset: Offset) -> io::Result<()> {
        info!("Truncating log to offset {}", offset);

        // remove index/segment files rolled after the offset
        let segments_to_remove = self.file_set.remove_after(offset);
        Self::delete_segments(segments_to_remove).await?;

        // truncate the current index
        match self.file_set.active_index_mut().truncate(offset) {
            Some(len) => self.file_set.active_segment_mut().truncate(len).await,
            // index outside of appended range
            None => Ok(()),
        }
    }

    /// Removes segment files that are before (strictly less than) the specified
    /// offset. The log might contain some messages before the offset
    /// provided is in the middle of a segment.
    pub async fn trim_segments_before(&mut self, offset: Offset) -> io::Result<()> {
        let segments_to_remove = self.file_set.remove_before(offset);
        Self::delete_segments(segments_to_remove).await
    }

    /// Removes segment files that are read-only.
    pub async fn trim_inactive_segments(&mut self) -> io::Result<()> {
        let active_offset_start = self.file_set.active_index().starting_offset();
        self.trim_segments_before(active_offset_start).await
    }

    /// Forces a flush of the log.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.file_set.active_segment_mut().flush().await?;
        self.file_set.active_index_mut().flush().await?;
        self.file_set.active_stream_index_mut().flush()?;
        let _ = self.broadcaster.send(mem::take(&mut self.unflushed));
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Vec<Event<'static>>> {
        self.broadcaster.subscribe()
    }

    async fn delete_segments(segments: Vec<SegmentSet>) -> io::Result<()> {
        for p in segments {
            trace!(
                "Removing segment and index starting at {}",
                p.segment.starting_offset()
            );
            p.segment.remove().await?;
            p.index.remove().await?;
            p.stream_index.remove().await?;
        }
        Ok(())
    }

    pub async fn load_subscription(&mut self, id: &str) -> rusqlite::Result<Option<u64>> {
        tokio::task::block_in_place(move || subscription_store::load_subscription(&self.conn, id))
    }

    pub async fn update_subscription(
        &mut self,
        id: &str,
        last_event_id: u64,
    ) -> rusqlite::Result<()> {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Actual position of a stream.
pub enum CurrentVersion {
    /// The last event's number.
    Current(u64),
    /// The stream doesn't exist.
    NoStream,
}

// impl<'a, T> AsRef<EventRef<'a, T>> for Event<T> {
//     fn as_ref(&self) -> &EventRef<'a, T> {
//         &EventRef {
//             id: self.id,
//             stream_id: &self.stream_id,
//             stream_version: self.stream_version,
//             event_name: &self.event_name,
//             event_data: &self.event_data,
//             metadata: &self.metadata,
//             timestamp: self.timestamp,
//         }
//     }
// }

// #[derive(Clone, Debug, PartialEq, Eq, Serialize)]
// pub struct EventRef<'a, T> {
//     pub id: u64,
//     pub stream_id: &'a str,
//     pub stream_version: u64,
//     pub event_name: &'a str,
//     pub event_data: &'a T,
//     pub metadata: &'a [u8],
//     pub timestamp: DateTime<Utc>,
// }

#[cfg(test)]
mod tests {
    use super::{message::*, testutil::*, *};

    use std::{collections::HashSet, fs};

    #[tokio::test]
    pub async fn offset_range() {
        let range = OffsetRange(2, 6);

        assert_eq!(vec![2, 3, 4, 5, 6, 7], range.iter().collect::<Vec<u64>>());

        assert_eq!(
            vec![7, 6, 5, 4, 3, 2],
            range.iter().rev().collect::<Vec<u64>>()
        );
    }

    #[tokio::test]
    pub async fn append() {
        let dir = TestDir::new();
        let mut log = CommitLog::new(LogOptions::new(&dir)).await.unwrap();
        assert_eq!(log.append_msg("my_stream", "123456").await.unwrap(), 0);
        assert_eq!(log.append_msg("my_stream", "abcdefg").await.unwrap(), 1);
        assert_eq!(log.append_msg("my_stream", "foobarbaz").await.unwrap(), 2);
        assert_eq!(log.append_msg("my_stream", "bing").await.unwrap(), 3);
        log.flush().await.unwrap();
    }

    #[tokio::test]
    pub async fn append_multiple() {
        let dir = TestDir::new();
        let mut log = CommitLog::new(LogOptions::new(&dir)).await.unwrap();
        let mut buf = {
            let mut buf = MessageBuf::default();
            buf.push(b"123456").unwrap();
            buf.push(b"789012").unwrap();
            buf.push(b"345678").unwrap();
            buf
        };
        let range = log.append("my_stream", &mut buf).await.unwrap();
        assert_eq!(0, range.first());
        assert_eq!(3, range.len());
        assert_eq!(vec![0, 1, 2], range.iter().collect::<Vec<u64>>());
    }

    #[tokio::test]
    pub async fn append_new_segment() {
        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.segment_max_bytes(62);

        {
            let mut log = CommitLog::new(opts).await.unwrap();
            // first 2 entries fit (both 30 bytes with encoding)
            log.append_msg("my_stream", "0123456789").await.unwrap();
            log.append_msg("my_stream", "0123456789").await.unwrap();

            // this one should roll the log
            log.append_msg("my_stream", "0123456789").await.unwrap();
            log.flush().await.unwrap();
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

    #[tokio::test]
    pub async fn read_entries() {
        env_logger::try_init().unwrap_or(());

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(1000);
        let mut log = CommitLog::new(opts).await.unwrap();

        for i in 0..100 {
            let s = format!("-data {}", i);
            log.append_msg("my_stream", s.as_str()).await.unwrap();
        }
        log.flush().await.unwrap();

        {
            let active_index_read = log.read(82, ReadLimit::max_bytes(168)).await.unwrap();
            assert_eq!(6, active_index_read.len());
            assert_eq!(
                vec![82, 83, 84, 85, 86, 87],
                active_index_read
                    .iter()
                    .map(|v| v.offset())
                    .collect::<Vec<_>>()
            );
        }

        {
            let old_index_read = log.read(5, ReadLimit::max_bytes(112)).await.unwrap();
            assert_eq!(4, old_index_read.len());
            assert_eq!(
                vec![5, 6, 7, 8],
                old_index_read
                    .iter()
                    .map(|v| v.offset())
                    .collect::<Vec<_>>()
            );
        }

        // read at the boundary (not going to get full message limit)
        {
            // log rolls at offset 36
            let boundary_read = log.read(33, ReadLimit::max_bytes(100)).await.unwrap();
            assert_eq!(3, boundary_read.len());
            assert_eq!(
                vec![33, 34, 35],
                boundary_read.iter().map(|v| v.offset()).collect::<Vec<_>>()
            );
        }
    }

    #[tokio::test]
    pub async fn reopen_log() {
        env_logger::try_init().unwrap_or(());

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(1000);

        {
            let mut log = CommitLog::new(opts.clone()).await.unwrap();

            for i in 0..99 {
                let s = format!("some data {}", i);
                let off = log.append_msg("my_stream", s.as_str()).await.unwrap();
                assert_eq!(i, off);
            }
            log.flush().await.unwrap();
        }

        {
            let mut log = CommitLog::new(opts).await.unwrap();

            let active_index_read = log.read(82, ReadLimit::max_bytes(130)).await.unwrap();

            assert_eq!(4, active_index_read.len());
            assert_eq!(
                vec![82, 83, 84, 85],
                active_index_read
                    .iter()
                    .map(|v| v.offset())
                    .collect::<Vec<_>>()
            );

            let off = log.append_msg("my_stream", "moar data").await.unwrap();
            assert_eq!(99, off);
        }
    }

    #[tokio::test]
    pub async fn reopen_log_without_segment_write() {
        env_logger::try_init().unwrap_or(());

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(1000);

        {
            let mut log = CommitLog::new(opts.clone()).await.unwrap();
            log.flush().await.unwrap();
        }

        {
            CommitLog::new(opts.clone())
                .await
                .expect("Should be able to reopen log without writes");
        }

        {
            CommitLog::new(opts)
                .await
                .expect("Should be able to reopen log without writes");
        }
    }

    #[tokio::test]
    pub async fn reopen_log_with_one_segment_write() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();
        let opts = LogOptions::new(&dir);
        {
            let mut log = CommitLog::new(opts.clone()).await.unwrap();
            log.append_msg("my_stream", "Test").await.unwrap();
            log.flush().await.unwrap();
        }
        {
            let log = CommitLog::new(opts).await.unwrap();
            assert_eq!(1, log.next_offset());
        }
    }

    #[tokio::test]
    pub async fn append_message_greater_than_max() {
        let dir = TestDir::new();
        let mut log = CommitLog::new(LogOptions::new(&dir)).await.unwrap();
        //create vector with 1.2mb of size, u8 = 1 byte thus,
        //1mb = 1000000 bytes, 1200000 items needed
        let mut value = String::new();
        let mut target = 0;
        while target != 2000000 {
            value.push('a');
            target += 1;
        }
        let res = log.append_msg("my_stream", value).await;
        //will fail if no error is found which means a message greater than the limit
        // passed through
        assert!(res.is_err());
        log.flush().await.unwrap();
    }

    #[tokio::test]
    pub async fn truncate_from_active() {
        let dir = TestDir::new();
        let mut log = CommitLog::new(LogOptions::new(&dir)).await.unwrap();

        // append 5 messages
        {
            let mut buf = MessageBuf::default();
            buf.push(b"123456").unwrap();
            buf.push(b"789012").unwrap();
            buf.push(b"345678").unwrap();
            buf.push(b"aaaaaa").unwrap();
            buf.push(b"bbbbbb").unwrap();
            log.append("my_stream", &mut buf).await.unwrap();
        }

        // truncate to offset 2 (should remove 2 messages)
        log.truncate(2).await.expect("Unable to truncate file");

        assert_eq!(Some(2), log.last_offset());
    }

    #[tokio::test]
    pub async fn truncate_after_offset_removes_segments() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = CommitLog::new(opts).await.unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").await.unwrap();
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
        log.truncate(3).await.expect("Unable to truncate file");

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

    #[tokio::test]
    pub async fn truncate_at_segment_boundary_removes_segments() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = CommitLog::new(opts).await.unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").await.unwrap();
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
        log.truncate(2).await.expect("Unable to truncate file");

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

    #[tokio::test]
    pub async fn truncate_after_last_append_does_nothing() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = CommitLog::new(opts).await.unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").await.unwrap();
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
        log.truncate(7).await.expect("Unable to truncate file");

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

    #[tokio::test]
    pub async fn trim_segments_before_removes_segments() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = CommitLog::new(opts).await.unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").await.unwrap();
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
            .await
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
            .await
            .expect("Unabled to grab reader");
        assert_eq!(2, reader.iter().next().unwrap().offset());
    }

    #[tokio::test]
    pub async fn trim_segments_before_removes_segments_at_boundary() {
        env_logger::try_init().unwrap_or(());
        let dir = TestDir::new();

        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = CommitLog::new(opts).await.unwrap();

        // append 6 messages (4 segments)
        {
            for _ in 0..7 {
                log.append_msg("my_stream", b"12345").await.unwrap();
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
            .await
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
            .await
            .expect("Unabled to grab reader");
        assert_eq!(4, reader.iter().next().unwrap().offset());
    }

    #[tokio::test]
    pub async fn trim_start_logic_check() {
        env_logger::try_init().unwrap_or(());
        const TOTAL_MESSAGES: u64 = 20;
        const TESTED_TRIM_START: u64 = TOTAL_MESSAGES + 1;

        for trim_off in 0..TESTED_TRIM_START {
            let dir = TestDir::new();
            let mut opts = LogOptions::new(&dir);
            opts.index_max_items(20);
            opts.segment_max_bytes(52);
            let mut log = CommitLog::new(opts).await.unwrap();

            // append the messages
            {
                for _ in 0..TOTAL_MESSAGES {
                    log.append_msg("my_stream", b"12345").await.unwrap();
                }
            }

            log.trim_segments_before(trim_off)
                .await
                .expect("Unable to truncate file");
            assert_eq!(Some(TOTAL_MESSAGES - 1), log.last_offset());

            // make sure the messages are really gone
            let reader = log
                .read(0, ReadLimit::default())
                .await
                .expect("Unabled to grab reader");
            let start_off = reader.iter().next().unwrap().offset();
            assert!(start_off <= trim_off);
        }
    }

    #[tokio::test]
    pub async fn multiple_trim_start_calls() {
        env_logger::try_init().unwrap_or(());
        const TOTAL_MESSAGES: u64 = 20;
        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = CommitLog::new(opts).await.unwrap();

        // append the messages
        {
            for _ in 0..TOTAL_MESSAGES {
                log.append_msg("my_stream", b"12345").await.unwrap();
            }
        }

        log.trim_segments_before(2).await.unwrap();

        {
            let reader = log
                .read(0, ReadLimit::default())
                .await
                .expect("Unabled to grab reader");
            assert_eq!(2, reader.iter().next().unwrap().offset());
        }

        log.trim_segments_before(10).await.unwrap();

        {
            let reader = log
                .read(0, ReadLimit::default())
                .await
                .expect("Unabled to grab reader");
            assert_eq!(10, reader.iter().next().unwrap().offset());
        }
    }

    #[tokio::test]
    pub async fn trim_inactive_logic_check() {
        env_logger::try_init().unwrap_or(());
        const TOTAL_MESSAGES: u64 = 20;

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = CommitLog::new(opts).await.unwrap();

        // append the messages
        {
            for _ in 0..TOTAL_MESSAGES {
                log.append_msg("my_stream", b"12345").await.unwrap();
            }
        }

        log.trim_inactive_segments()
            .await
            .expect("Unable to truncate file");
        assert_eq!(Some(TOTAL_MESSAGES - 1), log.last_offset());

        // make sure the messages are really gone
        let reader = log
            .read(0, ReadLimit::default())
            .await
            .expect("Unabled to grab reader");
        let start_off = reader.iter().next().unwrap().offset();
        assert_eq!(16, start_off);
    }

    #[tokio::test]
    pub async fn trim_inactive_logic_check_zero_messages() {
        env_logger::try_init().unwrap_or(());

        let dir = TestDir::new();
        let mut opts = LogOptions::new(&dir);
        opts.index_max_items(20);
        opts.segment_max_bytes(52);
        let mut log = CommitLog::new(opts).await.unwrap();

        log.trim_inactive_segments()
            .await
            .expect("Unable to truncate file");
        assert_eq!(None, log.last_offset());

        // append the messages
        log.append_msg("my_stream", b"12345").await.unwrap();

        // make sure the messages are really gone
        let reader = log
            .read(0, ReadLimit::default())
            .await
            .expect("Unabled to grab reader");
        let start_off = reader.iter().next().unwrap().offset();
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
