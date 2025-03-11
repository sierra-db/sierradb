use std::{
    collections::{HashMap, hash_map::Entry},
    mem,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    thread::{self},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rayon::ThreadPool;
use smallvec::{SmallVec, smallvec};
use thread_priority::ThreadBuilderExt;
use tokio::sync::{
    RwLock,
    mpsc::{self, error::TrySendError},
    oneshot,
};
use tracing::{error, trace};
use uuid::Uuid;

use crate::{
    bucket::segment::{COMMIT_SIZE, EVENT_HEADER_SIZE},
    database::{CurrentVersion, ExpectedVersion, StreamLatestVersion},
    error::{EventValidationError, StreamIndexError, WriteError},
    id::validate_event_id,
};

use super::{
    BucketId, BucketSegmentId, SegmentKind,
    event_index::OpenEventIndex,
    file_name,
    flusher::FlushSender,
    reader_thread_pool::ReaderThreadPool,
    segment::{
        AppendEvent, AppendEventBody, AppendEventHeader, BucketSegmentReader, BucketSegmentWriter,
        SEGMENT_HEADER_SIZE,
    },
    stream_index::{OpenStreamIndex, STREAM_ID_SIZE, StreamIndexRecord, StreamOffsets},
};

pub type LiveIndexes = Arc<RwLock<(OpenEventIndex, OpenStreamIndex)>>;

const TOTAL_BUFFERED_WRITES: usize = 1_000; // At most, there can be 1,000 writes buffered across all threads
const CHANNEL_BUFFER_MIN: usize = 16;

type Sender = mpsc::Sender<WriteRequest>;
type Receiver = mpsc::Receiver<WriteRequest>;

#[derive(Clone)]
pub struct WriterThreadPool {
    bucket_ids: Arc<[BucketId]>,
    num_threads: u16,
    senders: Arc<[Sender]>,
    indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
}

impl WriterThreadPool {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dir: impl Into<PathBuf>,
        segment_size: usize,
        bucket_ids: Arc<[BucketId]>,
        num_threads: u16,
        flush_interval_duration: Duration,
        flush_interval_events: u32,
        reader_pool: &ReaderThreadPool,
        thread_pool: &Arc<ThreadPool>,
    ) -> Result<Self, WriteError> {
        assert!(num_threads > 0);
        assert!(
            bucket_ids.len() >= num_threads as usize,
            "number of buckets cannot be less than number of threads"
        );

        let mut senders = Vec::with_capacity(num_threads as usize);
        let mut indexes = HashMap::new();

        let dir = dir.into();
        for thread_id in 0..num_threads {
            let worker = Worker::new(
                dir.clone(),
                segment_size,
                thread_id,
                &bucket_ids,
                num_threads,
                flush_interval_duration,
                flush_interval_events,
                reader_pool,
                thread_pool,
            )?;
            for (bucket_id, writer_set) in &worker.writers {
                indexes.insert(
                    *bucket_id,
                    (
                        Arc::clone(&writer_set.index_segment_id),
                        Arc::clone(&writer_set.indexes),
                    ),
                );
            }

            let (tx, rx) = mpsc::channel(
                (TOTAL_BUFFERED_WRITES / num_threads as usize).max(CHANNEL_BUFFER_MIN),
            );
            senders.push(tx);
            thread::Builder::new()
                .name(format!("writer-{thread_id}"))
                .spawn_with_priority(
                    thread_priority::ThreadPriority::Crossplatform(62.try_into().unwrap()),
                    |_| worker.run(rx),
                )?;
        }

        // Spawn flusher thread
        if flush_interval_duration < Duration::MAX {
            thread::Builder::new()
                .name("writer-pool-flusher".to_string())
                .spawn({
                    let mut senders: Vec<_> =
                        senders.iter().map(|sender| sender.downgrade()).collect();
                    move || {
                        let mut last_ran = Instant::now();
                        loop {
                            thread::sleep(
                                flush_interval_duration.saturating_sub(last_ran.elapsed()),
                            );
                            last_ran = Instant::now();

                            senders.retain(|sender| {
                                let Some(sender) = sender.upgrade() else {
                                    return false;
                                };
                                match sender.try_send(WriteRequest::FlushPoll) {
                                    Ok(()) => true,
                                    Err(TrySendError::Full(_)) => true,
                                    Err(TrySendError::Closed(_)) => false,
                                }
                            });

                            if senders.is_empty() {
                                trace!(
                                    "writer pool flusher stopping due to all workers being stopped"
                                );
                                break;
                            }
                        }
                    }
                })?;
        }

        Ok(WriterThreadPool {
            bucket_ids,
            num_threads,
            senders: Arc::from(senders),
            indexes: Arc::new(indexes),
        })
    }

    pub fn indexes(&self) -> &Arc<HashMap<u16, (Arc<AtomicU32>, LiveIndexes)>> {
        &self.indexes
    }

    pub async fn append_events(
        &self,
        bucket_id: BucketId,
        batch: Arc<AppendEventsBatch>,
    ) -> Result<(), WriteError> {
        let target_thread = bucket_id_to_thread_id(bucket_id, &self.bucket_ids, self.num_threads)
            .ok_or(WriteError::BucketWriterNotFound)?;

        let sender = self
            .senders
            .get(target_thread as usize)
            .ok_or(WriteError::BucketWriterNotFound)?;

        let (reply_tx, reply_rx) = oneshot::channel();
        sender
            .send(WriteRequest::AppendEvents {
                bucket_id,
                batch,
                reply_tx,
            })
            .await
            .map_err(|_| WriteError::WriterThreadNotRunning)?;
        reply_rx.await.map_err(|_| WriteError::NoThreadReply)?
    }

    /// Marks a transaction commit as confirmed by all replication buckets, meeting the quorum.
    pub async fn confirm_transaction(
        &self,
        bucket_id: BucketId,
        offset: u64,
        event_id: Uuid,
        transaction_id: Uuid,
        confirmation_count: u8,
    ) -> Result<(), WriteError> {
        let target_thread = bucket_id_to_thread_id(bucket_id, &self.bucket_ids, self.num_threads)
            .ok_or(WriteError::BucketWriterNotFound)?;

        let sender = self
            .senders
            .get(target_thread as usize)
            .ok_or(WriteError::BucketWriterNotFound)?;

        let (reply_tx, reply_rx) = oneshot::channel();
        sender
            .send(WriteRequest::ConfirmTransaction {
                bucket_id,
                offset,
                event_id,
                transaction_id,
                confirmation_count,
                reply_tx,
            })
            .await
            .map_err(|_| WriteError::WriterThreadNotRunning)?;
        reply_rx.await.map_err(|_| WriteError::NoThreadReply)?
    }

    pub async fn with_event_index<F, R>(&self, bucket_id: BucketId, f: F) -> Option<R>
    where
        F: FnOnce(&Arc<AtomicU32>, &OpenEventIndex) -> R,
    {
        let (segment_id, index) = self.indexes.get(&bucket_id)?;
        let lock = index.read().await;

        Some(f(segment_id, &lock.0))
    }

    pub async fn with_stream_index<F, R>(&self, bucket_id: BucketId, f: F) -> Option<R>
    where
        F: FnOnce(&Arc<AtomicU32>, &OpenStreamIndex) -> R,
    {
        let (segment_id, index) = self.indexes.get(&bucket_id)?;
        let lock = index.read().await;

        Some(f(segment_id, &lock.1))
    }
}

pub struct AppendEventsBatch {
    partition_key: Uuid,
    transaction_id: Uuid,
    events: SmallVec<[WriteEventRequest; 4]>,
}

impl AppendEventsBatch {
    pub fn single(event: WriteEventRequest) -> Result<Self, EventValidationError> {
        if !validate_event_id(event.event_id, &event.stream_id) {
            return Err(EventValidationError::InvalidEventId);
        }

        if !(1..=STREAM_ID_SIZE).contains(&event.stream_id.len()) {
            return Err(EventValidationError::InvalidStreamIdLen);
        }

        Ok(AppendEventsBatch {
            partition_key: event.partition_key,
            transaction_id: Uuid::nil(),
            events: smallvec![event],
        })
    }

    pub fn transaction(
        events: SmallVec<[WriteEventRequest; 4]>,
        transaction_id: Uuid,
    ) -> Result<Self, EventValidationError> {
        let Some(partition_key) =
            events
                .iter()
                .try_fold(None, |partition_key, event| match partition_key {
                    Some(partition_key) => {
                        if event.partition_key != partition_key {
                            return Err(EventValidationError::PartitionKeyMismatch);
                        }

                        if !validate_event_id(event.event_id, &event.stream_id) {
                            return Err(EventValidationError::InvalidEventId);
                        }

                        if !(1..=STREAM_ID_SIZE).contains(&event.stream_id.len()) {
                            return Err(EventValidationError::InvalidStreamIdLen);
                        }

                        Ok(Some(partition_key))
                    }
                    None => Ok(Some(event.partition_key)),
                })?
        else {
            return Err(EventValidationError::EmptyTransaction);
        };

        Ok(AppendEventsBatch {
            partition_key,
            transaction_id,
            events,
        })
    }

    pub fn partition_key(&self) -> Uuid {
        self.partition_key
    }

    pub fn events(&self) -> &SmallVec<[WriteEventRequest; 4]> {
        &self.events
    }
}

enum WriteRequest {
    // ValidateEvents(Box<ValidateEventsRequest>),
    AppendEvents {
        bucket_id: BucketId,
        batch: Arc<AppendEventsBatch>,
        reply_tx: oneshot::Sender<Result<(), WriteError>>,
    },
    ConfirmTransaction {
        bucket_id: BucketId,
        offset: u64,
        event_id: Uuid,
        transaction_id: Uuid,
        confirmation_count: u8,
        reply_tx: oneshot::Sender<Result<(), WriteError>>,
    },
    FlushPoll,
}

// struct ValidateEventsRequest {
//     bucket_id: BucketId,
//     batch: Arc<AppendEventsBatch>,
//     reply_tx: oneshot::Sender<Result<HashMap<Arc<str>, CurrentVersion>, WriteError>>,
// }

#[derive(Clone)]
pub struct WriteEventRequest {
    pub event_id: Uuid,
    pub partition_key: Uuid,
    pub stream_id: Arc<str>,
    pub stream_version: ExpectedVersion,
    pub event_name: String,
    pub timestamp: u64,
    pub metadata: Vec<u8>,
    pub payload: Vec<u8>,
}

struct Worker {
    thread_id: u16,
    writers: HashMap<BucketId, WriterSet>,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
    fn new(
        dir: PathBuf,
        segment_size: usize,
        thread_id: u16,
        bucket_ids: &[BucketId],
        num_threads: u16,
        flush_interval_duration: Duration,
        flush_interval_events: u32,
        reader_pool: &ReaderThreadPool,
        thread_pool: &Arc<ThreadPool>,
    ) -> Result<Self, WriteError> {
        let mut writers = HashMap::new();
        let now = Instant::now();
        for &bucket_id in bucket_ids.iter() {
            if bucket_id_to_thread_id(bucket_id, bucket_ids, num_threads) == Some(thread_id) {
                let (bucket_segment_id, writer) =
                    BucketSegmentWriter::latest(bucket_id, &dir, FlushSender::local())?;
                let mut reader = BucketSegmentReader::open(
                    dir.join(file_name(bucket_segment_id, SegmentKind::Events)),
                    writer.flushed_offset(),
                )?;

                let mut event_index = OpenEventIndex::open(
                    bucket_segment_id,
                    dir.join(file_name(bucket_segment_id, SegmentKind::EventIndex)),
                )?;
                let mut stream_index = OpenStreamIndex::open(
                    bucket_segment_id,
                    dir.join(file_name(bucket_segment_id, SegmentKind::StreamIndex)),
                    segment_size,
                )?;

                event_index.hydrate(&mut reader)?;
                stream_index.hydrate(&mut reader)?;

                reader_pool.add_bucket_segment(bucket_segment_id, &reader, None, None);

                let indexes = Arc::new(RwLock::new((event_index, stream_index)));

                let writer_set = WriterSet {
                    dir: dir.clone(),
                    reader,
                    reader_pool: reader_pool.clone(),
                    bucket_segment_id,
                    segment_size,
                    writer,
                    index_segment_id: Arc::new(AtomicU32::new(bucket_segment_id.segment_id)),
                    indexes,
                    pending_indexes: Vec::with_capacity(128),
                    last_flushed: now,
                    unflushed_events: 0,
                    flush_interval_duration,
                    flush_interval_events,
                    thread_pool: Arc::clone(thread_pool),
                };

                writers.insert(bucket_id, writer_set);
            }
        }

        println!(
            "Thread {thread_id} has writers: {:?}",
            writers.keys().collect::<Vec<_>>()
        );

        Ok(Worker { thread_id, writers })
    }

    fn run(mut self, mut rx: Receiver) {
        while let Some(req) = rx.blocking_recv() {
            match req {
                WriteRequest::AppendEvents {
                    bucket_id,
                    batch,
                    reply_tx,
                } => {
                    let AppendEventsBatch {
                        transaction_id,
                        events,
                        ..
                    } = &*batch;

                    let Some(writer_set) = self.writers.get_mut(&bucket_id) else {
                        error!(
                            "thread {} received a request for bucket {bucket_id} that isn't assigned here",
                            self.thread_id
                        );
                        let _ = reply_tx.send(Err(WriteError::BucketWriterNotFound));
                        continue;
                    };

                    writer_set.flush_if_necessary();

                    let file_size = writer_set.writer.file_size();

                    let res = writer_set.handle_write(*transaction_id, events);
                    if res.is_err() {
                        if let Err(err) = writer_set.writer.set_len(file_size) {
                            error!("failed to set segment file length after write error: {err}");
                        }
                    }

                    let _ = reply_tx.send(res);
                }
                WriteRequest::ConfirmTransaction {
                    bucket_id,
                    offset,
                    event_id,
                    transaction_id,
                    confirmation_count,
                    reply_tx,
                } => {
                    let Some(writer_set) = self.writers.get_mut(&bucket_id) else {
                        error!(
                            "thread {} received a request for bucket {bucket_id} that isn't assigned here",
                            self.thread_id
                        );
                        let _ = reply_tx.send(Err(WriteError::BucketWriterNotFound));
                        continue;
                    };

                    writer_set.flush_if_necessary();

                    let res = writer_set.writer.set_confirmations(
                        offset,
                        &event_id,
                        &transaction_id,
                        confirmation_count,
                    );

                    let _ = reply_tx.send(res);
                }
                WriteRequest::FlushPoll => {
                    for writer_set in self.writers.values_mut() {
                        writer_set.flush_if_necessary();
                    }
                }
            }
        }

        // Flush any remaining data on shutdown.
        for mut writer_set in self.writers.into_values() {
            if let Err(err) = writer_set.writer.flush() {
                error!("failed to flush writer during shutdown: {err}");
            }
        }
    }
}

struct WriterSet {
    dir: PathBuf,
    reader: BucketSegmentReader,
    reader_pool: ReaderThreadPool,
    bucket_segment_id: BucketSegmentId,
    segment_size: usize,
    writer: BucketSegmentWriter,
    index_segment_id: Arc<AtomicU32>,
    indexes: LiveIndexes,
    pending_indexes: Vec<PendingIndex>,
    last_flushed: Instant,
    unflushed_events: u32,
    flush_interval_duration: Duration,
    flush_interval_events: u32,
    thread_pool: Arc<ThreadPool>,
}

impl WriterSet {
    fn handle_write(
        &mut self,
        transaction_id: Uuid,
        events: &SmallVec<[WriteEventRequest; 4]>,
    ) -> Result<(), WriteError> {
        let file_size = self.writer.file_size();

        let event_versions = self.validate_event_versions(events)?;

        let events_size = events
            .iter()
            .map(|event| {
                EVENT_HEADER_SIZE
                    + event.stream_id.len()
                    + event.event_name.len()
                    + event.metadata.len()
                    + event.payload.len()
            })
            .sum::<usize>()
            + if transaction_id.is_nil() {
                0
            } else {
                COMMIT_SIZE
            };

        if events_size + SEGMENT_HEADER_SIZE > self.segment_size {
            return Err(WriteError::EventsExceedSegmentSize);
        }

        if file_size as usize + self.writer.buf_len() + events_size > self.segment_size {
            self.rollover()?;
        }

        let mut new_pending_indexes: Vec<PendingIndex> = Vec::with_capacity(events.len());

        let event_count = events.len();
        for (event, stream_version) in events.into_iter().zip(event_versions) {
            let stream_version = stream_version.next_version();
            let body = AppendEventBody::new(
                &event.stream_id,
                &event.event_name,
                &event.metadata,
                &event.payload,
            );
            let header = AppendEventHeader::new(
                &event.event_id,
                &event.partition_key,
                &transaction_id,
                stream_version,
                event.timestamp,
                body,
            )?;
            let append = AppendEvent::new(header, body);
            let (offset, _) = self.writer.append_event(append)?;
            new_pending_indexes.push(PendingIndex {
                event_id: event.event_id,
                partition_key: event.partition_key,
                stream_id: Arc::clone(&event.stream_id),
                stream_version,
                offset,
            });
        }

        if !transaction_id.is_nil() {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| WriteError::BadSystemTime)?
                .as_nanos() as u64;
            self.writer
                .append_commit(&transaction_id, timestamp, event_count as u32)?;
        }

        self.pending_indexes.extend(new_pending_indexes);

        self.unflushed_events += event_count as u32;
        if self.unflushed_events >= self.flush_interval_events {
            if let Err(err) = self.flush() {
                error!("failed to flush writer: {err}");
            }
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<(), WriteError> {
        self.last_flushed = Instant::now();
        self.writer.flush()?;
        let mut indexes = self.indexes.blocking_write();
        self.unflushed_events = 0;
        for PendingIndex {
            event_id,
            partition_key,
            stream_id,
            stream_version,
            offset,
        } in self.pending_indexes.drain(..)
        {
            indexes.0.insert(event_id, offset);
            if let Err(err) = indexes
                .1
                .insert(stream_id, partition_key, stream_version, offset)
            {
                error!("failed to insert index: {err}");
            }
        }

        Ok(())
    }

    fn flush_if_necessary(&mut self) {
        if self.last_flushed.elapsed() >= self.flush_interval_duration {
            if let Err(err) = self.flush() {
                error!("failed to flush writer: {err}");
            }
        }
    }

    fn rollover(&mut self) -> Result<(), WriteError> {
        self.last_flushed = Instant::now();
        self.writer.flush()?;

        // Open new segment
        let old_bucket_segment_id = self.bucket_segment_id;
        self.bucket_segment_id = self.bucket_segment_id.increment_segment_id();

        self.writer = BucketSegmentWriter::create(
            self.dir
                .join(file_name(self.bucket_segment_id, SegmentKind::Events)),
            self.bucket_segment_id.bucket_id,
            FlushSender::local(),
        )?;
        let old_reader = mem::replace(
            &mut self.reader,
            BucketSegmentReader::open(
                self.dir
                    .join(file_name(self.bucket_segment_id, SegmentKind::Events)),
                self.writer.flushed_offset(),
            )?,
        );

        let event_index = OpenEventIndex::create(
            self.bucket_segment_id,
            self.dir
                .join(file_name(self.bucket_segment_id, SegmentKind::EventIndex)),
        )?;
        let stream_index = OpenStreamIndex::create(
            self.bucket_segment_id,
            self.dir
                .join(file_name(self.bucket_segment_id, SegmentKind::StreamIndex)),
            self.segment_size,
        )?;

        let (closed_event_index, closed_stream_index) = {
            let mut indexes = self.indexes.blocking_write();
            for PendingIndex {
                event_id,
                partition_key,
                stream_id,
                stream_version,
                offset,
            } in self.pending_indexes.drain(..)
            {
                indexes.0.insert(event_id, offset);
                if let Err(err) = indexes
                    .1
                    .insert(stream_id, partition_key, stream_version, offset)
                {
                    error!("failed to insert index: {err}");
                }
            }

            let old_event_index = mem::replace(&mut indexes.0, event_index);
            let old_stream_index = mem::replace(&mut indexes.1, stream_index);

            let closed_event_index = old_event_index.close(&self.thread_pool)?;
            let closed_stream_index = old_stream_index.close(&self.thread_pool)?;

            self.index_segment_id
                .store(self.bucket_segment_id.segment_id, Ordering::Release);

            (closed_event_index, closed_stream_index)
        };

        self.reader_pool.add_bucket_segment(
            old_bucket_segment_id,
            &old_reader,
            Some(&closed_event_index),
            Some(&closed_stream_index),
        );
        self.reader_pool
            .add_bucket_segment(self.bucket_segment_id, &self.reader, None, None);

        Ok(())
    }

    fn validate_event_versions(
        &self,
        events: &SmallVec<[WriteEventRequest; 4]>,
    ) -> Result<Vec<CurrentVersion>, WriteError> {
        struct StreamVersion {
            next: u64,
            partition_key: Uuid,
        }

        let mut stream_versions: HashMap<&Arc<str>, StreamVersion> = HashMap::new();
        let mut event_versions = Vec::with_capacity(events.len());

        for event in events {
            match stream_versions.entry(&event.stream_id) {
                Entry::Occupied(mut entry) => {
                    if entry.get().partition_key != event.partition_key {
                        return Err(WriteError::Validation(
                            EventValidationError::PartitionKeyMismatch,
                        ));
                    }

                    match event.stream_version {
                        ExpectedVersion::Any => {
                            event_versions.push(CurrentVersion::Current(entry.get_mut().next));
                            entry.get_mut().next += 1;
                        }
                        ExpectedVersion::StreamExists => {
                            todo!()
                        }
                        ExpectedVersion::NoStream => {
                            return Err(WriteError::WrongExpectedVersion {
                                stream_id: Arc::clone(&event.stream_id),
                                current: CurrentVersion::Current(entry.get().next),
                                expected: event.stream_version,
                            });
                        }
                        ExpectedVersion::Exact(expected_version) => {
                            event_versions.push(CurrentVersion::Current(entry.get_mut().next));

                            if entry.get().next != expected_version {
                                return Err(WriteError::WrongExpectedVersion {
                                    stream_id: Arc::clone(&event.stream_id),
                                    current: CurrentVersion::Current(entry.get().next),
                                    expected: ExpectedVersion::Exact(expected_version),
                                });
                            }
                        }
                    }
                }
                Entry::Vacant(entry) => match event.stream_version {
                    ExpectedVersion::Any => {
                        let latest_stream_version = self
                            .pending_indexes
                            .iter()
                            .rev()
                            .find_map(|pending| {
                                if pending.stream_id == event.stream_id {
                                    Some(StreamLatestVersion::LatestVersion {
                                        partition_key: pending.partition_key,
                                        version: pending.stream_version,
                                    })
                                } else {
                                    None
                                }
                            })
                            .map(Ok)
                            .or_else(|| {
                                self.read_stream_latest_version(&event.stream_id)
                                    .transpose()
                            })
                            .transpose()?;

                        match latest_stream_version {
                            Some(StreamLatestVersion::LatestVersion {
                                partition_key,
                                version,
                            }) => {
                                if partition_key != event.partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch,
                                    ));
                                }

                                event_versions.push(CurrentVersion::Current(version));
                                entry.insert(StreamVersion {
                                    next: version,
                                    partition_key,
                                });
                            }
                            Some(StreamLatestVersion::ExternalBucket { .. }) => {
                                todo!()
                            }
                            None => {
                                event_versions.push(CurrentVersion::NoStream);
                                entry.insert(StreamVersion {
                                    next: 0,
                                    partition_key: event.partition_key,
                                });
                            }
                        }
                    }
                    ExpectedVersion::StreamExists => todo!(),
                    ExpectedVersion::NoStream => {
                        let latest_stream_version = self
                            .pending_indexes
                            .iter()
                            .rev()
                            .find_map(|pending| {
                                if pending.stream_id == event.stream_id {
                                    Some(StreamLatestVersion::LatestVersion {
                                        partition_key: pending.partition_key,
                                        version: pending.stream_version,
                                    })
                                } else {
                                    None
                                }
                            })
                            .map(Ok)
                            .or_else(|| {
                                self.read_stream_latest_version(&event.stream_id)
                                    .transpose()
                            })
                            .transpose()?;

                        match latest_stream_version {
                            Some(StreamLatestVersion::LatestVersion {
                                partition_key,
                                version,
                            }) => {
                                if partition_key != event.partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch,
                                    ));
                                }

                                return Err(WriteError::WrongExpectedVersion {
                                    stream_id: Arc::clone(&event.stream_id),
                                    current: CurrentVersion::Current(version),
                                    expected: event.stream_version,
                                });
                            }
                            Some(StreamLatestVersion::ExternalBucket { .. }) => {
                                todo!()
                            }
                            None => {
                                event_versions.push(CurrentVersion::NoStream);
                                entry.insert(StreamVersion {
                                    next: 0,
                                    partition_key: event.partition_key,
                                });
                            }
                        }
                    }
                    ExpectedVersion::Exact(expected_version) => {
                        let latest_stream_version = self
                            .pending_indexes
                            .iter()
                            .rev()
                            .find_map(|pending| {
                                if pending.stream_id == event.stream_id {
                                    Some(StreamLatestVersion::LatestVersion {
                                        partition_key: pending.partition_key,
                                        version: pending.stream_version,
                                    })
                                } else {
                                    None
                                }
                            })
                            .map(Ok)
                            .or_else(|| {
                                self.read_stream_latest_version(&event.stream_id)
                                    .transpose()
                            })
                            .transpose()?;

                        match latest_stream_version {
                            Some(StreamLatestVersion::LatestVersion {
                                partition_key,
                                version,
                            }) => {
                                if partition_key != event.partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch,
                                    ));
                                }

                                if version != expected_version {
                                    return Err(WriteError::WrongExpectedVersion {
                                        stream_id: Arc::clone(&event.stream_id),
                                        current: CurrentVersion::Current(version),
                                        expected: ExpectedVersion::Exact(expected_version),
                                    });
                                }

                                event_versions.push(CurrentVersion::Current(version));
                                entry.insert(StreamVersion {
                                    next: expected_version,
                                    partition_key,
                                });
                            }
                            Some(StreamLatestVersion::ExternalBucket { .. }) => {
                                todo!()
                            }
                            None => {
                                return Err(WriteError::WrongExpectedVersion {
                                    stream_id: Arc::clone(&event.stream_id),
                                    current: CurrentVersion::NoStream,
                                    expected: ExpectedVersion::Exact(expected_version),
                                });
                            }
                        }
                    }
                },
            }
        }

        Ok(event_versions)
    }

    fn read_stream_latest_version(
        &self,
        stream_id: &Arc<str>,
    ) -> Result<Option<StreamLatestVersion>, StreamIndexError> {
        let latest = self.indexes.blocking_read().1.get(stream_id).map(
            |StreamIndexRecord {
                 partition_key,
                 version_max,
                 offsets,
                 ..
             }| {
                match offsets {
                    StreamOffsets::Offsets(_) => StreamLatestVersion::LatestVersion {
                        partition_key: *partition_key,
                        version: *version_max,
                    },
                    StreamOffsets::ExternalBucket => StreamLatestVersion::ExternalBucket {
                        partition_key: *partition_key,
                    },
                }
            },
        );

        if let Some(latest) = latest {
            return Ok(Some(latest));
        }

        let bucket_id = self.bucket_segment_id.bucket_id;

        let stream_index_version = self.reader_pool.install({
            let stream_id = Arc::clone(stream_id);
            move |with_readers| {
                with_readers(move |readers| {
                    readers
                        .get_mut(&bucket_id)
                        .and_then(|segments| {
                            segments.iter_mut().rev().find_map(|(_, reader_set)| {
                                let stream_index = reader_set.stream_index.as_mut()?;
                                let record = match stream_index.get_key(&stream_id).transpose()? {
                                    Ok(record) => record,
                                    Err(err) => return Some(Err(err)),
                                };
                                match stream_index.get(&stream_id).transpose()? {
                                    Ok(offsets) => Some(Ok((record, offsets))),
                                    Err(err) => Some(Err(err)),
                                }
                            })
                        })
                        .transpose()
                })
            }
        })?;

        Ok(stream_index_version.map(
            |(
                StreamIndexRecord {
                    partition_key,
                    version_max,
                    ..
                },
                offsets,
            )| match offsets {
                StreamOffsets::Offsets(_) => StreamLatestVersion::LatestVersion {
                    partition_key,
                    version: version_max,
                },
                StreamOffsets::ExternalBucket => {
                    StreamLatestVersion::ExternalBucket { partition_key }
                }
            },
        ))
    }
}

struct PendingIndex {
    event_id: Uuid,
    partition_key: Uuid,
    stream_id: Arc<str>,
    stream_version: u64,
    offset: u64,
}

fn bucket_id_to_thread_id(
    bucket_id: BucketId,
    bucket_ids: &[BucketId],
    num_threads: u16,
) -> Option<u16> {
    let num_buckets = bucket_ids.len() as u16;

    assert!(
        num_buckets >= num_threads,
        "number of buckets cannot be less than number of threads"
    );

    if num_threads == 1 {
        return bucket_ids.contains(&bucket_id).then_some(0);
    }

    // Find the position/index of this bucket_id in the bucket_ids array
    let position = bucket_ids.iter().position(|&id| id == bucket_id)? as u16;

    let buckets_per_thread = num_buckets / num_threads;
    let extra = num_buckets % num_threads;

    if position < (buckets_per_thread + 1) * extra {
        Some(position / (buckets_per_thread + 1))
    } else {
        Some(extra + (position - (buckets_per_thread + 1) * extra) / buckets_per_thread)
    }
}
