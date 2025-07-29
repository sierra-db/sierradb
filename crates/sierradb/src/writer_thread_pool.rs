use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread::{self};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rayon::ThreadPool;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use thread_priority::ThreadBuilderExt;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self};
use tokio::sync::{RwLock, oneshot};
use tracing::{error, trace};
use uuid::Uuid;

use crate::StreamId;
use crate::bucket::event_index::OpenEventIndex;
use crate::bucket::partition_index::{OpenPartitionIndex, PartitionIndexRecord, PartitionOffsets};
use crate::bucket::segment::{
    AppendEvent, BucketSegmentReader, BucketSegmentWriter, COMMIT_SIZE, EVENT_HEADER_SIZE,
    SEGMENT_HEADER_SIZE,
};
use crate::bucket::stream_index::{OpenStreamIndex, StreamIndexRecord, StreamOffsets};
use crate::bucket::{BucketId, BucketSegmentId, PartitionId, SegmentKind};
use crate::database::{
    CurrentVersion, ExpectedVersion, NewEvent, PartitionLatestSequence, StreamLatestVersion,
    Transaction,
};
use crate::error::{EventValidationError, PartitionIndexError, StreamIndexError, WriteError};
use crate::id::get_uuid_flag;
use crate::reader_thread_pool::ReaderThreadPool;

pub type LiveIndexes = Arc<RwLock<LiveIndexSet>>;

#[derive(Debug)]
pub struct LiveIndexSet {
    pub event_index: OpenEventIndex,
    pub partition_index: OpenPartitionIndex,
    pub stream_index: OpenStreamIndex,
}

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
        if flush_interval_duration > Duration::ZERO && flush_interval_duration < Duration::MAX {
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

    pub fn indexes(&self) -> &Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>> {
        &self.indexes
    }

    pub async fn append_events(
        &self,
        bucket_id: BucketId,
        batch: Transaction,
    ) -> Result<AppendResult, WriteError> {
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
                batch: Box::new(batch),
                reply_tx,
            })
            .await
            .map_err(|_| WriteError::WriterThreadNotRunning)?;
        reply_rx.await.map_err(|_| WriteError::NoThreadReply)?
    }

    /// Marks an event/events as confirmed by `confirmation_count` partitions,
    /// meeting quorum.
    pub async fn set_confirmations(
        &self,
        bucket_id: BucketId,
        offsets: SmallVec<[u64; 4]>,
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
            .send(WriteRequest::SetConfirmations {
                bucket_id,
                offsets,
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

        Some(f(segment_id, &lock.event_index))
    }

    pub async fn with_partition_index<F, R>(&self, bucket_id: BucketId, f: F) -> Option<R>
    where
        F: FnOnce(&Arc<AtomicU32>, &OpenPartitionIndex) -> R,
    {
        let (segment_id, index) = self.indexes.get(&bucket_id)?;
        let lock = index.read().await;

        Some(f(segment_id, &lock.partition_index))
    }

    pub async fn with_stream_index<F, R>(&self, bucket_id: BucketId, f: F) -> Option<R>
    where
        F: FnOnce(&Arc<AtomicU32>, &OpenStreamIndex) -> R,
    {
        let (segment_id, index) = self.indexes.get(&bucket_id)?;
        let lock = index.read().await;

        Some(f(segment_id, &lock.stream_index))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResult {
    pub offsets: SmallVec<[u64; 4]>,
    pub first_partition_sequence: u64,
    pub last_partition_sequence: u64,
    pub stream_versions: HashMap<StreamId, u64>,
}

enum WriteRequest {
    AppendEvents {
        bucket_id: BucketId,
        batch: Box<Transaction>,
        reply_tx: oneshot::Sender<Result<AppendResult, WriteError>>,
    },
    SetConfirmations {
        bucket_id: BucketId,
        offsets: SmallVec<[u64; 4]>,
        transaction_id: Uuid,
        confirmation_count: u8,
        reply_tx: oneshot::Sender<Result<(), WriteError>>,
    },
    FlushPoll,
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
                let (bucket_segment_id, writer) = BucketSegmentWriter::latest(bucket_id, &dir)?;
                let mut reader = BucketSegmentReader::open(
                    SegmentKind::Events.get_path(&dir, bucket_segment_id),
                    Some(writer.flushed_offset()),
                )?;

                let mut event_index = OpenEventIndex::open(
                    bucket_segment_id,
                    SegmentKind::EventIndex.get_path(&dir, bucket_segment_id),
                )?;
                let mut partition_index = OpenPartitionIndex::open(
                    bucket_segment_id,
                    SegmentKind::PartitionIndex.get_path(&dir, bucket_segment_id),
                )?;
                let mut stream_index = OpenStreamIndex::open(
                    bucket_segment_id,
                    SegmentKind::StreamIndex.get_path(&dir, bucket_segment_id),
                    segment_size,
                )?;

                if let Err(err) = event_index.hydrate(&mut reader) {
                    error!("failed to hydrate event index for {bucket_segment_id}: {err}");
                }
                if let Err(err) = partition_index.hydrate(&mut reader) {
                    error!("failed to hydrate partition index for {bucket_segment_id}: {err}");
                }
                if let Err(err) = stream_index.hydrate(&mut reader) {
                    error!("failed to hydrate stream index for {bucket_segment_id}: {err}");
                }

                reader_pool.add_bucket_segment(bucket_segment_id, &reader, None, None, None);

                let indexes = Arc::new(RwLock::new(LiveIndexSet {
                    event_index,
                    partition_index,
                    stream_index,
                }));

                let writer_set = WriterSet {
                    dir: dir.clone(),
                    reader,
                    reader_pool: reader_pool.clone(),
                    bucket_segment_id,
                    segment_size,
                    writer,
                    next_partition_sequences: HashMap::new(),
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
                    self.handle_append_events(bucket_id, *batch, reply_tx);
                }
                WriteRequest::SetConfirmations {
                    bucket_id,
                    offsets,
                    transaction_id,
                    confirmation_count,
                    reply_tx,
                } => {
                    self.handle_set_confirmations(
                        bucket_id,
                        offsets,
                        transaction_id,
                        confirmation_count,
                        reply_tx,
                    );
                }
                WriteRequest::FlushPoll => {
                    self.handle_flush_poll();
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

    fn handle_append_events(
        &mut self,
        bucket_id: BucketId,
        batch: Transaction,
        reply_tx: oneshot::Sender<Result<AppendResult, WriteError>>,
    ) {
        let Transaction {
            partition_key,
            partition_id,
            transaction_id,
            events,
            expected_partition_sequence,
            ..
        } = batch;

        let Some(writer_set) = self.writers.get_mut(&bucket_id) else {
            error!(
                "thread {} received a request for bucket {bucket_id} that isn't assigned here",
                self.thread_id
            );
            let _ = reply_tx.send(Err(WriteError::BucketWriterNotFound));
            return;
        };

        writer_set.flush_if_necessary();

        let event_versions = match writer_set.validate_event_versions(partition_key, &events) {
            Ok(event_versions) => event_versions,
            Err(err) => {
                let _ = reply_tx.send(Err(err));
                return;
            }
        };
        let latest_stream_versions = events.iter().zip(event_versions.iter()).fold(
            HashMap::new(),
            |mut acc, (event, version)| {
                acc.entry(event.stream_id.clone())
                    .and_modify(|latest: &mut CurrentVersion| {
                        *latest = (*latest).max(*version);
                    })
                    .or_insert(*version);
                acc
            },
        );

        let file_size = writer_set.writer.file_size();
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
            + if get_uuid_flag(&transaction_id) {
                0
            } else {
                COMMIT_SIZE
            };

        if events_size + SEGMENT_HEADER_SIZE > writer_set.segment_size {
            let _ = reply_tx.send(Err(WriteError::EventsExceedSegmentSize));
            return;
        }

        if file_size as usize + writer_set.writer.buf_len() + events_size > writer_set.segment_size
            && let Err(err) = writer_set.rollover()
        {
            let _ = reply_tx.send(Err(err));
            return;
        }

        let res = writer_set.handle_write(
            partition_key,
            partition_id,
            transaction_id,
            &events,
            event_versions,
            expected_partition_sequence,
            latest_stream_versions.len(),
            batch.confirmation_count,
        );
        if res.is_err()
            && let Err(err) = writer_set.writer.set_len(file_size)
        {
            error!("failed to set segment file length after write error: {err}");
        }

        let _ = reply_tx.send(res);
    }

    fn handle_set_confirmations(
        &mut self,
        bucket_id: BucketId,
        offsets: SmallVec<[u64; 4]>,
        transaction_id: Uuid,
        confirmation_count: u8,
        reply_tx: oneshot::Sender<Result<(), WriteError>>,
    ) {
        let Some(writer_set) = self.writers.get_mut(&bucket_id) else {
            error!(
                "thread {} received a request for bucket {bucket_id} that isn't assigned here",
                self.thread_id
            );
            let _ = reply_tx.send(Err(WriteError::BucketWriterNotFound));
            return;
        };

        writer_set.flush_if_necessary();

        for offset in offsets {
            let res =
                writer_set
                    .writer
                    .set_confirmations(offset, &transaction_id, confirmation_count);
            if let Err(err) = res {
                let _ = reply_tx.send(Err(err));
                return;
            }
        }

        let _ = reply_tx.send(Ok(()));
    }

    fn handle_flush_poll(&mut self) {
        for writer_set in self.writers.values_mut() {
            writer_set.flush_if_necessary();
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
    next_partition_sequences: HashMap<PartitionId, u64>,
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
        partition_key: Uuid,
        partition_id: PartitionId,
        transaction_id: Uuid,
        events: &SmallVec<[NewEvent; 4]>,
        event_versions: Vec<CurrentVersion>,
        expected_partition_sequence: ExpectedVersion,
        unique_streams: usize,
        confirmation_count: u8,
    ) -> Result<AppendResult, WriteError> {
        if events.is_empty() {
            unreachable!("append event batch does not allow empty transactions");
        }

        let mut next_partition_sequence = self.next_partition_sequence(partition_id)?;
        validate_partition_sequence(
            partition_id,
            expected_partition_sequence,
            next_partition_sequence,
        )?;
        let first_partition_sequence = next_partition_sequence;
        let mut new_pending_indexes: Vec<PendingIndex> = Vec::with_capacity(events.len());
        let mut offsets = SmallVec::with_capacity(events.len());
        let mut stream_versions = HashMap::with_capacity(unique_streams);

        let event_count = events.len();
        for (event, stream_version) in events.into_iter().zip(event_versions) {
            let partition_sequence = next_partition_sequence;
            let stream_version = stream_version.next();
            stream_versions.insert(event.stream_id.clone(), stream_version);
            let append = AppendEvent {
                event_id: &event.event_id,
                partition_key: &partition_key,
                partition_id,
                partition_sequence,
                stream_version,
                stream_id: &event.stream_id,
                event_name: &event.event_name,
                metadata: &event.metadata,
                payload: &event.payload,
            };
            let (offset, _) = self.writer.append_event(
                &transaction_id,
                event.timestamp,
                confirmation_count,
                append,
            )?;
            offsets.push(offset);
            // We need to guarantee:
            // - partition key is the same as previous event partition keys in the stream
            // - partition sequence doesn't reach u64::MAX
            new_pending_indexes.push(PendingIndex {
                event_id: event.event_id,
                partition_key,
                partition_id,
                partition_sequence,
                stream_id: event.stream_id.clone(),
                stream_version,
                offset,
            });

            next_partition_sequence = next_partition_sequence.checked_add(1).unwrap();
        }

        if !get_uuid_flag(&transaction_id) {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| WriteError::BadSystemTime)?
                .as_nanos() as u64;
            self.writer
                .append_commit(&transaction_id, timestamp, event_count as u32, 0)?;
        }

        self.next_partition_sequences
            .insert(partition_id, next_partition_sequence);
        self.pending_indexes.extend(new_pending_indexes);

        self.unflushed_events += event_count as u32;
        if self.unflushed_events >= self.flush_interval_events
            && let Err(err) = self.flush()
        {
            error!("failed to flush writer: {err}");
        }

        Ok(AppendResult {
            offsets,
            first_partition_sequence,
            last_partition_sequence: next_partition_sequence
                .checked_sub(1)
                .expect("next partition sequence should be greather than zero"),
            stream_versions,
        })
    }

    fn flush(&mut self) -> Result<(), WriteError> {
        self.last_flushed = Instant::now();
        self.writer.flush()?;
        let mut indexes = self.indexes.blocking_write();
        self.unflushed_events = 0;
        for PendingIndex {
            event_id,
            partition_key,
            partition_id,
            partition_sequence,
            stream_id,
            stream_version,
            offset,
        } in self.pending_indexes.drain(..)
        {
            indexes.event_index.insert(event_id, offset);
            if let Err(err) =
                indexes
                    .partition_index
                    .insert(partition_id, partition_sequence, offset)
            {
                panic!("failed to insert partition index: {err}");
            }
            if let Err(err) =
                indexes
                    .stream_index
                    .insert(stream_id, partition_key, stream_version, offset)
            {
                panic!("failed to insert stream index: {err}");
            }
        }

        Ok(())
    }

    fn flush_if_necessary(&mut self) {
        if self.last_flushed.elapsed() >= self.flush_interval_duration
            && let Err(err) = self.flush()
        {
            error!("failed to flush writer: {err}");
        }
    }

    fn rollover(&mut self) -> Result<(), WriteError> {
        self.last_flushed = Instant::now();
        self.writer.flush()?;

        // Open new segment
        let old_bucket_segment_id = self.bucket_segment_id;
        self.bucket_segment_id = self.bucket_segment_id.increment_segment_id();

        SegmentKind::ensure_segment_dir(&self.dir, self.bucket_segment_id)?;

        self.writer = BucketSegmentWriter::create(
            SegmentKind::Events.get_path(&self.dir, self.bucket_segment_id),
            self.bucket_segment_id.bucket_id,
        )?;
        let old_reader = mem::replace(
            &mut self.reader,
            BucketSegmentReader::open(
                SegmentKind::Events.get_path(&self.dir, self.bucket_segment_id),
                Some(self.writer.flushed_offset()),
            )?,
        );

        let event_index = OpenEventIndex::create(
            self.bucket_segment_id,
            SegmentKind::EventIndex.get_path(&self.dir, self.bucket_segment_id),
        )?;
        let partition_index = OpenPartitionIndex::create(
            self.bucket_segment_id,
            SegmentKind::PartitionIndex.get_path(&self.dir, self.bucket_segment_id),
        )?;
        let stream_index = OpenStreamIndex::create(
            self.bucket_segment_id,
            SegmentKind::StreamIndex.get_path(&self.dir, self.bucket_segment_id),
            self.segment_size,
        )?;

        let (closed_event_index, closed_partition_index, closed_stream_index) = {
            let mut indexes = self.indexes.blocking_write();
            for PendingIndex {
                event_id,
                partition_key,
                partition_id,
                partition_sequence,
                stream_id,
                stream_version,
                offset,
            } in self.pending_indexes.drain(..)
            {
                indexes.event_index.insert(event_id, offset);
                if let Err(err) =
                    indexes
                        .partition_index
                        .insert(partition_id, partition_sequence, offset)
                {
                    panic!("failed to insert partition index: {err}");
                }
                if let Err(err) =
                    indexes
                        .stream_index
                        .insert(stream_id, partition_key, stream_version, offset)
                {
                    panic!("failed to insert stream index: {err}");
                }
            }

            let old_event_index = mem::replace(&mut indexes.event_index, event_index);
            let old_partition_index = mem::replace(&mut indexes.partition_index, partition_index);
            let old_stream_index = mem::replace(&mut indexes.stream_index, stream_index);

            let closed_event_index = old_event_index.close(&self.thread_pool)?;
            let closed_partition_index = old_partition_index.close(&self.thread_pool)?;
            let closed_stream_index = old_stream_index.close(&self.thread_pool)?;

            self.index_segment_id
                .store(self.bucket_segment_id.segment_id, Ordering::Release);

            (
                closed_event_index,
                closed_partition_index,
                closed_stream_index,
            )
        };

        self.reader_pool.add_bucket_segment(
            old_bucket_segment_id,
            &old_reader,
            Some(&closed_event_index),
            Some(&closed_partition_index),
            Some(&closed_stream_index),
        );
        self.reader_pool
            .add_bucket_segment(self.bucket_segment_id, &self.reader, None, None, None);

        Ok(())
    }

    fn validate_event_versions(
        &self,
        partition_key: Uuid,
        events: &SmallVec<[NewEvent; 4]>,
    ) -> Result<Vec<CurrentVersion>, WriteError> {
        let mut stream_versions: HashMap<StreamId, u64> = HashMap::new();
        let mut event_versions = Vec::with_capacity(events.len());

        for event in events {
            match stream_versions.entry(event.stream_id.clone()) {
                Entry::Occupied(mut entry) => match event.stream_version {
                    ExpectedVersion::Any => {
                        event_versions.push(CurrentVersion::Current(*entry.get()));
                        *entry.get_mut() += 1;
                    }
                    ExpectedVersion::Exists => {
                        event_versions.push(CurrentVersion::Current(*entry.get()));
                        *entry.get_mut() += 1;
                    }
                    ExpectedVersion::Empty => {
                        return Err(WriteError::WrongExpectedVersion {
                            stream_id: event.stream_id.clone(),
                            current: CurrentVersion::Current(*entry.get()),
                            expected: event.stream_version,
                        });
                    }
                    ExpectedVersion::Exact(expected_version) => {
                        event_versions.push(CurrentVersion::Current(*entry.get()));

                        if *entry.get() != expected_version {
                            return Err(WriteError::WrongExpectedVersion {
                                stream_id: event.stream_id.clone(),
                                current: CurrentVersion::Current(*entry.get()),
                                expected: ExpectedVersion::Exact(expected_version),
                            });
                        }
                    }
                },
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
                            Some(StreamLatestVersion::LatestVersion { version, .. }) => {
                                event_versions.push(CurrentVersion::Current(version));
                                entry.insert(version);
                            }
                            Some(StreamLatestVersion::ExternalBucket { .. }) => {
                                todo!()
                            }
                            None => {
                                event_versions.push(CurrentVersion::Empty);
                                entry.insert(0);
                            }
                        }
                    }
                    ExpectedVersion::Exists => {
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
                                partition_key: latest_partition_key,
                                version,
                            }) => {
                                // Stream exists, so this is fine
                                if partition_key != latest_partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch,
                                    ));
                                }

                                event_versions.push(CurrentVersion::Current(version));
                                entry.insert(version + 1);
                            }
                            Some(StreamLatestVersion::ExternalBucket { .. }) => {
                                todo!()
                            }
                            None => {
                                // Stream doesn't exist, which violates the StreamExists expectation
                                return Err(WriteError::WrongExpectedVersion {
                                    stream_id: event.stream_id.clone(),
                                    current: CurrentVersion::Empty,
                                    expected: ExpectedVersion::Exists,
                                });
                            }
                        }
                    }
                    ExpectedVersion::Empty => {
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
                                partition_key: latest_partition_key,
                                version,
                            }) => {
                                if partition_key != latest_partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch,
                                    ));
                                }

                                return Err(WriteError::WrongExpectedVersion {
                                    stream_id: event.stream_id.clone(),
                                    current: CurrentVersion::Current(version),
                                    expected: event.stream_version,
                                });
                            }
                            Some(StreamLatestVersion::ExternalBucket { .. }) => {
                                todo!()
                            }
                            None => {
                                event_versions.push(CurrentVersion::Empty);
                                entry.insert(0);
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
                                partition_key: latest_partition_key,
                                version,
                            }) => {
                                if partition_key != latest_partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch,
                                    ));
                                }

                                if version != expected_version {
                                    return Err(WriteError::WrongExpectedVersion {
                                        stream_id: event.stream_id.clone(),
                                        current: CurrentVersion::Current(version),
                                        expected: ExpectedVersion::Exact(expected_version),
                                    });
                                }

                                event_versions.push(CurrentVersion::Current(version));
                                entry.insert(expected_version);
                            }
                            Some(StreamLatestVersion::ExternalBucket { .. }) => {
                                todo!()
                            }
                            None => {
                                return Err(WriteError::WrongExpectedVersion {
                                    stream_id: event.stream_id.clone(),
                                    current: CurrentVersion::Empty,
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

    fn next_partition_sequence(
        &self,
        partition_id: PartitionId,
    ) -> Result<u64, PartitionIndexError> {
        match self.next_partition_sequences.get(&partition_id) {
            Some(sequence) => Ok(*sequence),
            None => match self.read_partition_latest_sequence(partition_id)? {
                Some(PartitionLatestSequence::LatestSequence { sequence, .. }) => Ok(sequence + 1),
                Some(PartitionLatestSequence::ExternalBucket { .. }) => todo!(),
                None => Ok(0),
            },
        }
    }

    fn read_partition_latest_sequence(
        &self,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionLatestSequence>, PartitionIndexError> {
        let latest = self
            .indexes
            .blocking_read()
            .partition_index
            .get(partition_id)
            .map(
                |PartitionIndexRecord {
                     sequence_max,
                     offsets,
                     ..
                 }| match offsets {
                    PartitionOffsets::Offsets(_) => PartitionLatestSequence::LatestSequence {
                        partition_id,
                        sequence: *sequence_max,
                    },
                    PartitionOffsets::ExternalBucket => {
                        PartitionLatestSequence::ExternalBucket { partition_id }
                    }
                },
            );

        if let Some(latest) = latest {
            return Ok(Some(latest));
        }

        let bucket_id = self.bucket_segment_id.bucket_id;

        let partition_index_sequence = self.reader_pool.install({
            move |with_readers| {
                with_readers(move |readers| {
                    readers
                        .get_mut(&bucket_id)
                        .and_then(|segments| {
                            segments.iter_mut().rev().find_map(|(_, reader_set)| {
                                let partition_index = reader_set.partition_index.as_mut()?;
                                let record =
                                    match partition_index.get_key(partition_id).transpose()? {
                                        Ok(record) => record,
                                        Err(err) => return Some(Err(err)),
                                    };
                                match partition_index.get(partition_id).transpose()? {
                                    Ok(offsets) => Some(Ok((record, offsets))),
                                    Err(err) => Some(Err(err)),
                                }
                            })
                        })
                        .transpose()
                })
            }
        })?;

        Ok(
            partition_index_sequence.map(|(PartitionIndexRecord { sequence_max, .. }, offsets)| {
                match offsets {
                    PartitionOffsets::Offsets(_) => PartitionLatestSequence::LatestSequence {
                        partition_id,
                        sequence: sequence_max,
                    },
                    PartitionOffsets::ExternalBucket => {
                        PartitionLatestSequence::ExternalBucket { partition_id }
                    }
                }
            }),
        )
    }

    fn read_stream_latest_version(
        &self,
        stream_id: &StreamId,
    ) -> Result<Option<StreamLatestVersion>, StreamIndexError> {
        let latest = self
            .indexes
            .blocking_read()
            .stream_index
            .get(stream_id)
            .map(
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
            let stream_id = stream_id.clone();
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
    partition_id: PartitionId,
    partition_sequence: u64,
    stream_id: StreamId,
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

fn validate_partition_sequence(
    partition_id: PartitionId,
    expected: ExpectedVersion,
    next_partition_sequence: u64,
) -> Result<(), WriteError> {
    match expected {
        ExpectedVersion::Any => Ok(()),
        ExpectedVersion::Exists => {
            if next_partition_sequence == 0 {
                Err(WriteError::WrongExpectedSequence {
                    partition_id,
                    current: CurrentVersion::Empty,
                    expected,
                })
            } else {
                Ok(())
            }
        }
        ExpectedVersion::Empty => {
            if next_partition_sequence == 0 {
                Ok(())
            } else {
                Err(WriteError::WrongExpectedSequence {
                    partition_id,
                    current: CurrentVersion::Current(next_partition_sequence - 1),
                    expected,
                })
            }
        }
        ExpectedVersion::Exact(sequence) => {
            if next_partition_sequence == 0 {
                Err(WriteError::WrongExpectedSequence {
                    partition_id,
                    current: CurrentVersion::Empty,
                    expected,
                })
            } else if next_partition_sequence - 1 == sequence {
                Ok(())
            } else {
                Err(WriteError::WrongExpectedSequence {
                    partition_id,
                    current: CurrentVersion::Current(next_partition_sequence - 1),
                    expected,
                })
            }
        }
    }
}
