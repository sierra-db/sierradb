use std::{
    borrow::Cow,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    mem,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::Path,
    vec,
};

use polonius_the_crab::{exit_polonius, polonius, polonius_return, polonius_try};
use uuid::Uuid;

use crate::{
    bucket::BucketId,
    error::{ReadError, WriteError},
};

use super::{
    BUCKET_ID_SIZE, BucketSegmentHeader, COMMIT_SIZE, CREATED_AT_SIZE, EVENT_HEADER_SIZE,
    FlushedOffset, MAGIC_BYTES, MAGIC_BYTES_SIZE, RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE,
    VERSION_SIZE, calculate_commit_crc32c, calculate_event_crc32c,
};

const PAGE_SIZE: usize = 4096; // Usually a page is 4KB on Linux
const READ_AHEAD_SIZE: usize = 64 * 1024; // 64 KB read ahead buffer
const READ_BUF_SIZE: usize = PAGE_SIZE - RECORD_HEADER_SIZE;

macro_rules! read_bytes {
    ($buf:expr, $pos:ident, $len:expr) => {{
        let pos = $pos;
        let len = $len;
        let buf = $buf[pos..pos + len].try_into().unwrap();
        #[allow(unused_assignments)]
        {
            $pos += len;
        }
        buf
    }};
}

pub enum CommittedEvents<'a> {
    None {
        next_offset: u64,
    },
    Single(EventRecord<'a>),
    Transaction {
        events: Vec<EventRecord<'static>>,
        commit: CommitRecord,
    },
}

impl CommittedEvents<'_> {
    pub fn into_owned(self) -> CommittedEvents<'static> {
        match self {
            CommittedEvents::None { next_offset } => CommittedEvents::None { next_offset },
            CommittedEvents::Single(event) => CommittedEvents::Single(event.into_owned()),
            CommittedEvents::Transaction { events, commit } => {
                CommittedEvents::Transaction { events, commit }
            }
        }
    }
}

impl<'a> IntoIterator for CommittedEvents<'a> {
    type Item = EventRecord<'static>;
    type IntoIter = CommittedEventsIntoIter<'a>;

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

pub struct CommittedEventsIntoIter<'a> {
    inner: CommittedEventsIntoIterInner<'a>,
}

enum CommittedEventsIntoIterInner<'a> {
    Single(Option<EventRecord<'a>>),
    Transaction(vec::IntoIter<EventRecord<'static>>),
}

impl Iterator for CommittedEventsIntoIter<'_> {
    type Item = EventRecord<'static>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            CommittedEventsIntoIterInner::Single(event) => {
                event.take().map(EventRecord::into_owned)
            }
            CommittedEventsIntoIterInner::Transaction(iter) => iter.next(),
        }
    }
}

pub struct BucketSegmentReader {
    file: File,
    header_buf: [u8; COMMIT_SIZE],
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
        let header_buf = [0u8; COMMIT_SIZE];
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
        let bucket_id = u16::from_le_bytes(bucket_id_bytes);

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

    pub fn set_commit_confirmations(
        &self,
        mut offset: u64,
        confirmation_count: u8,
    ) -> Result<(), WriteError> {
        offset += (RECORD_HEADER_SIZE + mem::size_of::<u32> as usize) as u64;
        self.file
            .write_all_at(&confirmation_count.to_le_bytes(), offset)?;

        Ok(())
    }

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
        event_id: &Uuid,
        transaction_id: &Uuid,
        confirmation_count: u8,
    ) -> Result<(), WriteError> {
        super::set_confirmations(
            &self.file,
            offset,
            event_id,
            transaction_id,
            confirmation_count,
        )
    }

    pub fn read_committed_events(
        &mut self,
        mut offset: u64,
        sequential: bool,
    ) -> Result<Option<CommittedEvents<'_>>, ReadError> {
        let mut this = self;
        let mut events = Vec::new();
        let mut pending_transaction_id = Uuid::nil();
        loop {
            (events, offset) = polonius!(|this| -> Result<
                Option<CommittedEvents<'polonius>>,
                ReadError,
            > {
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

                        if transaction_id.is_nil() {
                            // Events with nil transaction ids are always approved
                            if events.is_empty() {
                                // If its the first event we encountered, then return it alone
                                polonius_return!(Ok(Some(CommittedEvents::Single(event))));
                            }

                            events.push(event.into_owned());
                        } else if transaction_id != pending_transaction_id {
                            // Unexpected transaction, we'll start a new pending transaction
                            events = vec![event.into_owned()];
                            pending_transaction_id = transaction_id;
                        } else {
                            // Event belongs to the transaction
                            events.push(event.into_owned());
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
    /// If sequential is `true`, the read will be optimized for future sequential reads.
    /// Random reads should have `sequential` set to `false`.
    ///
    /// Borrowed data will be returned in the record where possible. If the event length exceeds 4KB, then the event
    /// will be read directly from the file, and the returned `Record` will contain owned data.
    pub fn read_record(
        &mut self,
        start_offset: u64,
        sequential: bool,
    ) -> Result<Option<Record<'_>>, ReadError> {
        // This is the only check needed. We don't need to check for the event body,
        // since if the offset supports this header read, then the event body would have also been written too for the flush.
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

        let record_header = RecordHeader::from_bytes(header_buf);

        if record_header.record_type == 0 {
            offset -= mem::size_of::<u32>() as u64;
            self.read_event_body(start_offset, record_header, offset, sequential)
                .map(|event| Some(Record::Event(event)))
        } else if record_header.record_type == 1 {
            let event_count = u32::from_le_bytes(
                header_buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + 4]
                    .try_into()
                    .unwrap(),
            );
            let confirmation_count = u8::from_le_bytes(
                header_buf[RECORD_HEADER_SIZE + mem::size_of::<u32>()
                    ..RECORD_HEADER_SIZE + mem::size_of::<u32>() + mem::size_of::<u8>()]
                    .try_into()
                    .unwrap(),
            );
            self.read_commit_body(start_offset, record_header, event_count, confirmation_count)
                .map(|commit| Some(Record::Commit(commit)))
        } else {
            Err(ReadError::UnknownRecordType(record_header.record_type))
        }
    }

    fn read_event_body(
        &mut self,
        start_offset: u64,
        record_header: RecordHeader,
        mut offset: u64,
        sequential: bool,
    ) -> Result<EventRecord<'_>, ReadError> {
        let length = EVENT_HEADER_SIZE - RECORD_HEADER_SIZE;
        let header_buf = if sequential {
            self.read_from_read_ahead(offset, length)?
        } else {
            self.file
                .read_exact_at(&mut self.header_buf[..length], offset)?;
            self.header_buf.as_slice()
        };
        offset += length as u64;

        let event_header = EventHeader::from_bytes(header_buf);
        let body_len = event_header.body_len();
        let body = if sequential {
            let body_buf = self.read_from_read_ahead(offset, body_len)?;
            EventBody::from_bytes(&event_header, body_buf)?
        } else if body_len > self.body_buf.len() {
            let mut body_buf = vec![0u8; body_len];
            self.file.read_exact_at(&mut body_buf, offset)?;
            EventBody::from_bytes_owned(&event_header, body_buf)?
        } else {
            self.file
                .read_exact_at(&mut self.body_buf[..body_len], offset)?;
            EventBody::from_bytes(&event_header, self.body_buf.as_slice())?
        };

        validate_and_combine_event(start_offset, record_header, event_header, body)
    }

    fn read_commit_body(
        &self,
        offset: u64,
        record_header: RecordHeader,
        event_count: u32,
        confirmation_count: u8,
    ) -> Result<CommitRecord, ReadError> {
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
            event_count,
            confirmation_count,
        })
    }

    fn fill_read_ahead(&mut self, offset: u64, mut length: usize) -> Result<(), ReadError> {
        let end_offset = offset + length as u64;

        // Set the new read-ahead offset aligned to 64KB
        self.read_ahead_offset = offset - (offset % READ_AHEAD_SIZE as u64);
        self.read_ahead_pos = 0;
        length = (end_offset - self.read_ahead_offset) as usize;

        // If the requested read is larger than READ_AHEAD_SIZE, expand the buffer to the next leargest interval of 4096
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
    pub fn next_committed_events(&mut self) -> Result<Option<CommittedEvents<'_>>, ReadError> {
        let mut this = self;
        loop {
            polonius!(
                |this| -> Result<Option<CommittedEvents<'polonius>>, ReadError> {
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
                }
            );
        }
    }

    pub fn next_record(&mut self) -> Result<Option<Record<'_>>, ReadError> {
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
pub enum Record<'a> {
    Event(EventRecord<'a>),
    Commit(CommitRecord),
}

impl<'a> Record<'a> {
    pub fn into_event(self) -> Option<EventRecord<'a>> {
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

    pub fn into_owned(self) -> Record<'static> {
        match self {
            Record::Event(event) => Record::Event(event.into_owned()),
            Record::Commit(commit) => Record::Commit(commit),
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRecord<'a> {
    pub offset: u64,
    pub event_id: Uuid,
    pub partition_key: Uuid,
    pub transaction_id: Uuid,
    pub stream_version: u64,
    pub timestamp: u64,
    pub confirmation_count: u8,
    pub stream_id: Cow<'a, str>,
    pub event_name: Cow<'a, str>,
    pub metadata: Cow<'a, [u8]>,
    pub payload: Cow<'a, [u8]>,
}

impl EventRecord<'_> {
    pub fn into_owned(self) -> EventRecord<'static> {
        EventRecord {
            offset: self.offset,
            event_id: self.event_id,
            partition_key: self.partition_key,
            transaction_id: self.transaction_id,
            stream_version: self.stream_version,
            timestamp: self.timestamp,
            confirmation_count: self.confirmation_count,
            stream_id: Cow::Owned(self.stream_id.into_owned()),
            event_name: Cow::Owned(self.event_name.into_owned()),
            metadata: Cow::Owned(self.metadata.into_owned()),
            payload: Cow::Owned(self.payload.into_owned()),
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        EVENT_HEADER_SIZE as u64
            + self.stream_id.len() as u64
            + self.event_name.len() as u64
            + self.metadata.len() as u64
            + self.payload.len() as u64
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

struct RecordHeader {
    event_id: Uuid,
    transaction_id: Uuid,
    timestamp: u64,
    crc32c: u32,
    confirmation_count: u8,
    record_type: u8,
}

impl RecordHeader {
    fn from_bytes(buf: &[u8]) -> Self {
        let mut pos = 0;
        let event_id = Uuid::from_bytes(read_bytes!(buf, pos, 16));
        let transaction_id = Uuid::from_bytes(read_bytes!(buf, pos, 16));
        let timestamp = u64::from_le_bytes(read_bytes!(buf, pos, 8));
        let crc32c = u32::from_le_bytes(read_bytes!(buf, pos, 4));
        let confirmation_count = u8::from_le_bytes(read_bytes!(buf, pos, 1));
        let record_type = (timestamp & 0b1) as u8;
        RecordHeader {
            event_id,
            transaction_id,
            timestamp,
            crc32c,
            confirmation_count,
            record_type,
        }
    }
}

struct EventHeader {
    stream_version: u64,
    partition_key: Uuid,
    stream_id_len: usize,
    event_name_len: usize,
    metadata_len: usize,
    payload_len: usize,
}

impl EventHeader {
    fn from_bytes(buf: &[u8]) -> Self {
        let mut pos = 0;
        EventHeader {
            stream_version: u64::from_le_bytes(read_bytes!(buf, pos, 8)),
            partition_key: Uuid::from_bytes(read_bytes!(buf, pos, 16)),
            stream_id_len: u16::from_le_bytes(read_bytes!(buf, pos, 2)) as usize,
            event_name_len: u16::from_le_bytes(read_bytes!(buf, pos, 2)) as usize,
            metadata_len: u32::from_le_bytes(read_bytes!(buf, pos, 4)) as usize,
            payload_len: u32::from_le_bytes(read_bytes!(buf, pos, 4)) as usize,
        }
    }

    fn body_len(&self) -> usize {
        self.stream_id_len + self.event_name_len + self.metadata_len + self.payload_len
    }
}

struct EventBody<'a> {
    stream_id: Cow<'a, str>,
    event_name: Cow<'a, str>,
    metadata: Cow<'a, [u8]>,
    payload: Cow<'a, [u8]>,
}

impl<'a> EventBody<'a> {
    fn from_bytes(event_header: &EventHeader, buf: &'a [u8]) -> Result<EventBody<'a>, ReadError> {
        let mut pos = 0;

        let stream_id = std::str::from_utf8(&buf[pos..pos + event_header.stream_id_len])
            .map_err(ReadError::InvalidStreamIdUtf8)?;
        pos += event_header.stream_id_len;

        let event_name = std::str::from_utf8(&buf[pos..pos + event_header.event_name_len])
            .map_err(ReadError::InvalidEventNameUtf8)?;
        pos += event_header.event_name_len;

        let metadata = &buf[pos..pos + event_header.metadata_len];
        pos += event_header.metadata_len;

        let payload = &buf[pos..pos + event_header.payload_len];

        Ok(EventBody {
            stream_id: Cow::Borrowed(stream_id),
            event_name: Cow::Borrowed(event_name),
            metadata: Cow::Borrowed(metadata),
            payload: Cow::Borrowed(payload),
        })
    }

    fn from_bytes_owned(
        event_header: &EventHeader,
        mut buf: Vec<u8>,
    ) -> Result<EventBody<'static>, ReadError> {
        let mut pos = 0;

        let stream_id = std::str::from_utf8(&buf[pos..pos + event_header.stream_id_len])
            .map_err(ReadError::InvalidStreamIdUtf8)?
            .to_owned();
        pos += event_header.stream_id_len;

        let event_name = std::str::from_utf8(&buf[pos..pos + event_header.event_name_len])
            .map_err(ReadError::InvalidEventNameUtf8)?
            .to_owned();
        pos += event_header.event_name_len;

        let metadata = buf[pos..pos + event_header.metadata_len].to_owned();
        pos += event_header.metadata_len;

        buf.drain(..pos);

        Ok(EventBody {
            stream_id: Cow::Owned(stream_id),
            event_name: Cow::Owned(event_name),
            metadata: Cow::Owned(metadata),
            payload: Cow::Owned(buf),
        })
    }
}

fn validate_and_combine_event(
    offset: u64,
    record_header: RecordHeader,
    event_header: EventHeader,
    body: EventBody<'_>,
) -> Result<EventRecord<'_>, ReadError> {
    let new_crc32c = calculate_event_crc32c(
        &record_header.event_id,
        &record_header.transaction_id,
        record_header.timestamp,
        event_header.stream_version,
        &event_header.partition_key,
        body.stream_id.as_bytes(),
        body.event_name.as_bytes(),
        &body.metadata,
        &body.payload,
    );
    if record_header.crc32c != new_crc32c {
        return Err(ReadError::Crc32cMismatch { offset });
    }

    Ok(EventRecord {
        offset,
        event_id: record_header.event_id,
        partition_key: event_header.partition_key,
        transaction_id: record_header.transaction_id,
        stream_version: event_header.stream_version,
        timestamp: record_header.timestamp,
        confirmation_count: record_header.confirmation_count,
        stream_id: body.stream_id,
        event_name: body.event_name,
        metadata: body.metadata,
        payload: body.payload,
    })
}
