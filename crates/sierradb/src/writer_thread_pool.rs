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
use tokio::sync::{RwLock, oneshot, watch};
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
        sync_interval: Duration,
        max_batch_size: usize,
        min_sync_bytes: usize,
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
                sync_interval,
                max_batch_size,
                min_sync_bytes,
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

        // Spawn syncer thread
        if sync_interval > Duration::ZERO && sync_interval < Duration::MAX {
            thread::Builder::new()
                .name("writer-pool-syncer".to_string())
                .spawn({
                    let mut senders: Vec<_> =
                        senders.iter().map(|sender| sender.downgrade()).collect();
                    move || {
                        let mut last_ran = Instant::now();
                        loop {
                            thread::sleep(sync_interval.saturating_sub(last_ran.elapsed()));
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
                                    "writer pool syncer stopping due to all workers being stopped"
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
            .map_err(|_| WriteError::WriterThreadNotRunning { bucket_id })?;
        let mut full_append = reply_rx.await.map_err(|_| WriteError::NoThreadReply)??;

        full_append
            .sync_rx
            .wait_for(|write_offset| *write_offset >= full_append.write_offset)
            .await
            .map_err(|_| WriteError::NoThreadReply)?;

        Ok(full_append.append)
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
            .map_err(|_| WriteError::WriterThreadNotRunning { bucket_id })?;
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

struct FullAppendResult {
    append: AppendResult,
    write_offset: u64,
    sync_rx: watch::Receiver<u64>,
}

enum WriteRequest {
    AppendEvents {
        bucket_id: BucketId,
        batch: Box<Transaction>,
        reply_tx: oneshot::Sender<Result<FullAppendResult, WriteError>>,
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
        sync_interval: Duration,
        max_batch_size: usize,
        min_sync_bytes: usize,
        reader_pool: &ReaderThreadPool,
        thread_pool: &Arc<ThreadPool>,
    ) -> Result<Self, WriteError> {
        let mut writers = HashMap::new();
        let now = Instant::now();
        for &bucket_id in bucket_ids.iter() {
            if bucket_id_to_thread_id(bucket_id, bucket_ids, num_threads) == Some(thread_id) {
                let (bucket_segment_id, writer) =
                    BucketSegmentWriter::latest(bucket_id, &dir, segment_size)?;
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

                let (sync_tx, _) = watch::channel(writer.write_offset());
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
                    sync_tx,
                    last_synced: now,
                    unflushed_events: 0,
                    bytes_since_sync: 0,
                    sync_interval,
                    max_batch_size,
                    min_sync_bytes,
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
        reply_tx: oneshot::Sender<Result<FullAppendResult, WriteError>>,
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

        let write_offset = writer_set.writer.write_offset();
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

        if write_offset as usize + events_size > writer_set.segment_size
            && let Err(err) = writer_set.rollover()
        {
            let _ = reply_tx.send(Err(err));
            return;
        }

        let bytes_since_sync = writer_set.bytes_since_sync;
        let res = writer_set.handle_write(WriteOperation {
            partition_key,
            partition_id,
            transaction_id,
            events: &events,
            event_versions,
            expected_partition_sequence,
            unique_streams: latest_stream_versions.len(),
            confirmation_count: batch.confirmation_count,
        });
        if res.is_err()
            && let Err(err) = writer_set.writer.set_len(write_offset)
        {
            writer_set.bytes_since_sync = bytes_since_sync;
            error!("failed to set segment file length after write error: {err}");
        }

        let _ = reply_tx.send(res.map(|append| FullAppendResult {
            append,
            write_offset: writer_set.writer.write_offset(),
            sync_rx: writer_set.sync_tx.subscribe(),
        }));
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
            writer_set.sync_if_necessary();
        }
    }
}

/// Parameters for a write operation
pub struct WriteOperation<'a> {
    pub partition_key: Uuid,
    pub partition_id: PartitionId,
    pub transaction_id: Uuid,
    pub events: &'a [NewEvent],
    pub event_versions: Vec<CurrentVersion>,
    pub expected_partition_sequence: ExpectedVersion,
    pub unique_streams: usize,
    pub confirmation_count: u8,
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
    sync_tx: watch::Sender<u64>,
    last_synced: Instant,
    unflushed_events: u32,
    bytes_since_sync: usize,
    sync_interval: Duration,
    max_batch_size: usize,
    min_sync_bytes: usize,
    thread_pool: Arc<ThreadPool>,
}

impl WriterSet {
    fn handle_write(&mut self, req: WriteOperation<'_>) -> Result<AppendResult, WriteError> {
        if req.events.is_empty() {
            unreachable!("append event batch does not allow empty transactions");
        }

        let mut next_partition_sequence = self.next_partition_sequence(req.partition_id)?;
        validate_partition_sequence(
            req.partition_id,
            req.expected_partition_sequence,
            next_partition_sequence,
        )?;

        let first_partition_sequence = next_partition_sequence;
        let mut new_pending_indexes: Vec<PendingIndex> = Vec::with_capacity(req.events.len());
        let mut offsets = SmallVec::with_capacity(req.events.len());
        let mut stream_versions = HashMap::with_capacity(req.unique_streams);

        let event_count = req.events.len();
        debug_assert_eq!(req.events.len(), req.event_versions.len());
        for (event, stream_version) in req.events.iter().zip(req.event_versions) {
            let partition_sequence = next_partition_sequence;
            let stream_version = stream_version.next();
            stream_versions.insert(event.stream_id.clone(), stream_version);
            let append = AppendEvent {
                event_id: &event.event_id,
                partition_key: &req.partition_key,
                partition_id: req.partition_id,
                partition_sequence,
                stream_version,
                stream_id: &event.stream_id,
                event_name: &event.event_name,
                metadata: &event.metadata,
                payload: &event.payload,
            };
            let (offset, len) = self.writer.append_event(
                &req.transaction_id,
                event.timestamp,
                req.confirmation_count,
                append,
            )?;
            offsets.push(offset);
            self.bytes_since_sync += len;
            // We need to guarantee:
            // - partition key is the same as previous event partition keys in the stream
            // - partition sequence doesn't reach u64::MAX
            new_pending_indexes.push(PendingIndex {
                event_id: event.event_id,
                partition_key: req.partition_key,
                partition_id: req.partition_id,
                partition_sequence,
                stream_id: event.stream_id.clone(),
                stream_version,
                offset,
            });

            next_partition_sequence = next_partition_sequence.checked_add(1).unwrap();
        }

        if !get_uuid_flag(&req.transaction_id) {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| WriteError::BadSystemTime)?
                .as_nanos() as u64;
            let (_, len) =
                self.writer
                    .append_commit(&req.transaction_id, timestamp, event_count as u32, 0)?;
            self.bytes_since_sync += len;
        }

        self.next_partition_sequences
            .insert(req.partition_id, next_partition_sequence);
        self.pending_indexes.extend(new_pending_indexes);

        self.writer.flush_writer()?;
        self.sync_if_necessary();

        Ok(AppendResult {
            offsets,
            first_partition_sequence,
            last_partition_sequence: next_partition_sequence
                .checked_sub(1)
                .expect("next partition sequence should be greather than zero"),
            stream_versions,
        })
    }

    fn sync(&mut self) -> Result<(), WriteError> {
        let write_offset = self.writer.flush()?;
        self.last_synced = Instant::now();
        self.unflushed_events = 0;
        self.bytes_since_sync = 0;
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
        self.sync_tx.send_replace(write_offset);

        Ok(())
    }

    fn should_sync(&self) -> bool {
        self.bytes_since_sync >= self.min_sync_bytes
            || self.unflushed_events as usize >= self.max_batch_size
            || self.last_synced.elapsed() >= self.sync_interval
    }

    fn sync_if_necessary(&mut self) {
        if self.should_sync()
            && let Err(err) = self.sync()
        {
            error!("failed to sync writer: {err}");
        }
    }

    fn rollover(&mut self) -> Result<(), WriteError> {
        self.sync()?;

        // Open new segment
        let old_bucket_segment_id = self.bucket_segment_id;
        self.bucket_segment_id = self.bucket_segment_id.increment_segment_id();

        SegmentKind::ensure_segment_dir(&self.dir, self.bucket_segment_id)?;

        self.writer = BucketSegmentWriter::create(
            SegmentKind::Events.get_path(&self.dir, self.bucket_segment_id),
            self.bucket_segment_id.bucket_id,
            self.segment_size,
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
        events: &[NewEvent],
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
                            partition_key,
                            stream_id: event.stream_id.clone(),
                            current: CurrentVersion::Current(*entry.get()),
                            expected: event.stream_version,
                        });
                    }
                    ExpectedVersion::Exact(expected_version) => {
                        event_versions.push(CurrentVersion::Current(*entry.get()));

                        if *entry.get() != expected_version {
                            return Err(WriteError::WrongExpectedVersion {
                                partition_key,
                                stream_id: event.stream_id.clone(),
                                current: CurrentVersion::Current(*entry.get()),
                                expected: ExpectedVersion::Exact(expected_version),
                            });
                        }

                        *entry.get_mut() += 1;
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
                            Some(StreamLatestVersion::LatestVersion {
                                partition_key: existing_partition_key,
                                version,
                            }) => {
                                if partition_key != existing_partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch {
                                            existing_partition_key,
                                            new_partition_key: partition_key,
                                        },
                                    ));
                                }

                                event_versions.push(CurrentVersion::Current(version));
                                entry.insert(version + 1);
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
                                partition_key: existing_partition_key,
                                version,
                            }) => {
                                // Stream exists, so this is fine
                                if partition_key != existing_partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch {
                                            existing_partition_key,
                                            new_partition_key: partition_key,
                                        },
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
                                    partition_key,
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
                                partition_key: existing_partition_key,
                                version,
                            }) => {
                                if partition_key != existing_partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch {
                                            existing_partition_key,
                                            new_partition_key: partition_key,
                                        },
                                    ));
                                }

                                return Err(WriteError::WrongExpectedVersion {
                                    partition_key,
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
                                partition_key: existing_partition_key,
                                version,
                            }) => {
                                if partition_key != existing_partition_key {
                                    return Err(WriteError::Validation(
                                        EventValidationError::PartitionKeyMismatch {
                                            existing_partition_key,
                                            new_partition_key: partition_key,
                                        },
                                    ));
                                }

                                if version != expected_version {
                                    return Err(WriteError::WrongExpectedVersion {
                                        partition_key,
                                        stream_id: event.stream_id.clone(),
                                        current: CurrentVersion::Current(version),
                                        expected: ExpectedVersion::Exact(expected_version),
                                    });
                                }

                                event_versions.push(CurrentVersion::Current(version));
                                entry.insert(expected_version + 1);
                            }
                            Some(StreamLatestVersion::ExternalBucket { .. }) => {
                                todo!()
                            }
                            None => {
                                return Err(WriteError::WrongExpectedVersion {
                                    partition_key,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::slice;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::time::{Duration, Instant};

    use proptest::collection::vec;
    use proptest::prelude::*;
    use rayon::ThreadPoolBuilder;
    use sierradb_protocol::{CurrentVersion, ExpectedVersion};
    use tempfile::{TempDir, tempdir};
    use tokio::sync::{RwLock, watch};
    use uuid::Uuid;

    use crate::StreamId;
    use crate::bucket::event_index::OpenEventIndex;
    use crate::bucket::partition_index::OpenPartitionIndex;
    use crate::bucket::segment::{BucketSegmentReader, BucketSegmentWriter};
    use crate::bucket::stream_index::OpenStreamIndex;
    use crate::bucket::{PartitionHash, SegmentKind};
    use crate::database::NewEvent;
    use crate::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash, uuid_v7_with_partition_hash};
    use crate::reader_thread_pool::ReaderThreadPool;
    use crate::writer_thread_pool::{LiveIndexSet, WriteOperation, WriterSet};

    fn setup_writer_set() -> (WriterSet, TempDir) {
        let dir = tempdir().unwrap();
        let bucket_id = 0;
        let segment_size = 1024 * 1024;
        let reader_threads = 1;
        let now = Instant::now();

        let thread_pool = Arc::new(ThreadPoolBuilder::new().build().unwrap());
        let reader_pool = ReaderThreadPool::new(reader_threads);

        let (bucket_segment_id, writer) =
            BucketSegmentWriter::latest(bucket_id, &dir, segment_size).unwrap();
        let mut reader = BucketSegmentReader::open(
            SegmentKind::Events.get_path(&dir, bucket_segment_id),
            Some(writer.flushed_offset()),
        )
        .unwrap();

        let mut event_index = OpenEventIndex::open(
            bucket_segment_id,
            SegmentKind::EventIndex.get_path(&dir, bucket_segment_id),
        )
        .unwrap();
        let mut partition_index = OpenPartitionIndex::open(
            bucket_segment_id,
            SegmentKind::PartitionIndex.get_path(&dir, bucket_segment_id),
        )
        .unwrap();
        let mut stream_index = OpenStreamIndex::open(
            bucket_segment_id,
            SegmentKind::StreamIndex.get_path(&dir, bucket_segment_id),
            segment_size,
        )
        .unwrap();

        if let Err(err) = event_index.hydrate(&mut reader) {
            panic!("failed to hydrate event index for {bucket_segment_id}: {err}");
        }
        if let Err(err) = partition_index.hydrate(&mut reader) {
            panic!("failed to hydrate partition index for {bucket_segment_id}: {err}");
        }
        if let Err(err) = stream_index.hydrate(&mut reader) {
            panic!("failed to hydrate stream index for {bucket_segment_id}: {err}");
        }

        reader_pool.add_bucket_segment(bucket_segment_id, &reader, None, None, None);

        let indexes = Arc::new(RwLock::new(LiveIndexSet {
            event_index,
            partition_index,
            stream_index,
        }));

        let (sync_tx, _) = watch::channel(writer.write_offset());
        let writer_set = WriterSet {
            dir: dir.path().to_owned(),
            reader,
            reader_pool: reader_pool.clone(),
            bucket_segment_id,
            segment_size,
            writer,
            bytes_since_sync: 0,
            next_partition_sequences: HashMap::new(),
            index_segment_id: Arc::new(AtomicU32::new(bucket_segment_id.segment_id)),
            indexes,
            pending_indexes: Vec::with_capacity(128),
            sync_tx,
            last_synced: now,
            unflushed_events: 0,
            sync_interval: Duration::from_millis(5),
            max_batch_size: 50,
            min_sync_bytes: 4096,
            thread_pool,
        };

        (writer_set, dir)
    }

    fn events_from_expected_fn(
        partition_hash: PartitionHash,
    ) -> impl Fn(&[(&StreamId, ExpectedVersion)]) -> Vec<NewEvent> {
        move |expected| {
            expected
                .iter()
                .map(|(stream_id, expected)| NewEvent {
                    event_id: uuid_v7_with_partition_hash(partition_hash),
                    stream_id: (*stream_id).clone(),
                    stream_version: *expected,
                    event_name: "MyEvent".to_string(),
                    timestamp: 1069,
                    metadata: vec![],
                    payload: vec![],
                })
                .collect()
        }
    }

    fn test_validate_event_versions(
        from_empty: [ExpectedVersion; 3],
        from_one: [ExpectedVersion; 3],
        from_four: [ExpectedVersion; 3],
        random_from_one: [ExpectedVersion; 3],
    ) {
        let (mut writer_set, _dir) = setup_writer_set();

        let stream_id = StreamId::new("my-stream").unwrap();
        let random_stream_id = StreamId::new("random").unwrap();
        let partition_key = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = 0;
        let events_from_expected = events_from_expected_fn(partition_hash);

        // Any tests
        let versions = writer_set
            .validate_event_versions(
                partition_key,
                &events_from_expected(
                    &from_empty
                        .iter()
                        .map(|expected| (&stream_id, *expected))
                        .collect::<Vec<_>>(),
                ),
            )
            .unwrap();
        assert_eq!(
            versions,
            [
                CurrentVersion::Empty,
                CurrentVersion::Current(0),
                CurrentVersion::Current(1),
            ]
        );

        writer_set
            .handle_write(WriteOperation {
                partition_key,
                partition_id,
                transaction_id: Uuid::new_v4(),
                events: &events_from_expected(&[(&stream_id, ExpectedVersion::Empty)]),
                event_versions: vec![CurrentVersion::Empty],
                expected_partition_sequence: ExpectedVersion::Empty,
                unique_streams: 1,
                confirmation_count: 1,
            })
            .unwrap();

        let versions = writer_set
            .validate_event_versions(
                partition_key,
                &events_from_expected(
                    &from_one
                        .iter()
                        .map(|expected| (&stream_id, *expected))
                        .collect::<Vec<_>>(),
                ),
            )
            .unwrap();
        assert_eq!(
            versions,
            [
                CurrentVersion::Current(0),
                CurrentVersion::Current(1),
                CurrentVersion::Current(2),
            ]
        );

        writer_set
            .handle_write(WriteOperation {
                partition_key,
                partition_id,
                transaction_id: Uuid::new_v4(),
                events: &events_from_expected(&[
                    (&stream_id, ExpectedVersion::Exact(0)),
                    (&stream_id, ExpectedVersion::Exact(1)),
                    (&random_stream_id, ExpectedVersion::Empty),
                    (&stream_id, ExpectedVersion::Exact(2)),
                    (&stream_id, ExpectedVersion::Exact(3)),
                ]),
                event_versions: vec![
                    CurrentVersion::Current(0),
                    CurrentVersion::Current(1),
                    CurrentVersion::Empty,
                    CurrentVersion::Current(2),
                    CurrentVersion::Current(3),
                ],
                expected_partition_sequence: ExpectedVersion::Exact(0),
                unique_streams: 2,
                confirmation_count: 1,
            })
            .unwrap();

        let versions = writer_set
            .validate_event_versions(
                partition_key,
                &events_from_expected(
                    &from_four
                        .iter()
                        .map(|expected| (&stream_id, *expected))
                        .collect::<Vec<_>>(),
                ),
            )
            .unwrap();
        assert_eq!(
            versions,
            [
                CurrentVersion::Current(4),
                CurrentVersion::Current(5),
                CurrentVersion::Current(6),
            ]
        );

        let versions = writer_set
            .validate_event_versions(
                partition_key,
                &events_from_expected(
                    &random_from_one
                        .iter()
                        .map(|expected| (&random_stream_id, *expected))
                        .collect::<Vec<_>>(),
                ),
            )
            .unwrap();
        assert_eq!(
            versions,
            [
                CurrentVersion::Current(0),
                CurrentVersion::Current(1),
                CurrentVersion::Current(2),
            ]
        );
    }

    #[test]
    fn test_validate_event_versions_any() {
        use ExpectedVersion::*;
        test_validate_event_versions([Any; 3], [Any; 3], [Any; 3], [Any; 3]);
    }

    #[test]
    fn test_validate_event_versions_vague() {
        use ExpectedVersion::*;
        test_validate_event_versions(
            [Empty, Exists, Exists],
            [Exists; 3],
            [Exists; 3],
            [Exists; 3],
        );
    }

    #[test]
    fn test_validate_event_versions_precise() {
        use ExpectedVersion::*;
        test_validate_event_versions(
            [Empty, Exact(0), Exact(1)],
            [Exact(0), Exact(1), Exact(2)],
            [Exact(4), Exact(5), Exact(6)],
            [Exact(0), Exact(1), Exact(2)],
        );
    }

    // Helper function to create a NewEvent with given properties
    fn new_event(
        partition_hash: PartitionHash,
        stream_id: StreamId,
        expected_version: ExpectedVersion,
    ) -> NewEvent {
        NewEvent {
            event_id: uuid_v7_with_partition_hash(partition_hash),
            stream_id,
            stream_version: expected_version,
            event_name: "TestEvent".to_string(),
            timestamp: 1000,
            metadata: vec![],
            payload: vec![],
        }
    }

    // Strategy for generating StreamIds
    prop_compose! {
        fn arb_stream_id()(name in "[a-z][a-z0-9_-]{0,19}") -> StreamId {
            StreamId::new(name).unwrap()
        }
    }

    // Strategy for generating ExpectedVersions
    fn arb_expected_version() -> impl Strategy<Value = ExpectedVersion> {
        prop_oneof![
            Just(ExpectedVersion::Any),
            Just(ExpectedVersion::Exists),
            Just(ExpectedVersion::Empty),
            (0u64..10).prop_map(ExpectedVersion::Exact),
        ]
    }

    // Strategy for generating events with the same partition
    prop_compose! {
        fn arb_events_same_partition(max_events: usize)(
            stream_ids in vec(arb_stream_id(), 1..=3),
            events in vec(
                (any::<prop::sample::Index>(), arb_expected_version()),
                1..=max_events
            )
        ) -> (PartitionHash, Uuid, Vec<NewEvent>) {
            let partition_key = Uuid::new_v4();
            let partition_hash = uuid_to_partition_hash(partition_key);

            let events: Vec<NewEvent> = events
                .into_iter()
                .map(|(stream_idx, expected_version)| {
                    let stream_id = stream_ids[stream_idx.index(stream_ids.len())].clone();
                    new_event(partition_hash, stream_id, expected_version)
                })
                .collect();

            (partition_hash, partition_key, events)
        }
    }

    proptest! {
        #[test]
        fn prop_validate_returns_correct_number_of_versions(
            (_, partition_key, events) in arb_events_same_partition(20)
        ) {
            let (writer_set, _dir) = setup_writer_set();

            match writer_set.validate_event_versions(partition_key, &events) {
                Ok(versions) => {
                    prop_assert_eq!(versions.len(), events.len());
                },
                Err(_) => {
                    // Validation can fail, which is expected for some inputs
                }
            }
        }

        #[test]
        fn prop_any_version_never_fails_on_empty_writer(
            stream_ids in vec(arb_stream_id(), 1..=5),
            num_events in 1usize..=10
        ) {
            let (writer_set, _dir) = setup_writer_set();
            let partition_key = Uuid::new_v4();
            let partition_hash = uuid_to_partition_hash(partition_key);

            let events: Vec<NewEvent> = (0..num_events)
                .map(|i| {
                    let stream_id = stream_ids[i % stream_ids.len()].clone();
                    new_event(partition_hash, stream_id, ExpectedVersion::Any)
                })
                .collect();

            let result = writer_set.validate_event_versions(partition_key, &events);
            prop_assert!(result.is_ok());

            let versions = result.unwrap();
            prop_assert_eq!(versions.len(), events.len());
        }

        #[test]
        fn prop_empty_version_fails_after_events_written(
            stream_id in arb_stream_id()
        ) {
            let (mut writer_set, _dir) = setup_writer_set();
            let partition_key = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
            let partition_hash = uuid_to_partition_hash(partition_key);

            // First write an event to make the stream non-empty
            let initial_event = new_event(partition_hash, stream_id.clone(), ExpectedVersion::Empty);
            let versions = writer_set.validate_event_versions(partition_key, slice::from_ref(&initial_event)).unwrap();

            writer_set.handle_write(WriteOperation {
                partition_key,
                partition_id: 0,
                transaction_id: Uuid::new_v4(),
                events: &[initial_event],
                event_versions: versions,
                expected_partition_sequence: ExpectedVersion::Empty,
                unique_streams: 1,
                confirmation_count: 1,
            }).unwrap();

            // Now try to write with Empty expectation - should fail
            let empty_event = new_event(partition_hash, stream_id, ExpectedVersion::Empty);
            let result = writer_set.validate_event_versions(partition_key, &[empty_event]);
            prop_assert!(result.is_err());
        }

        #[test]
        fn prop_exists_version_fails_on_nonexistent_stream(
            stream_id in arb_stream_id()
        ) {
            let (writer_set, _dir) = setup_writer_set();
            let partition_key = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
            let partition_hash = uuid_to_partition_hash(partition_key);

            let event = new_event(partition_hash, stream_id, ExpectedVersion::Exists);
            let result = writer_set.validate_event_versions(partition_key, &[event]);
            prop_assert!(result.is_err());
        }

        #[test]
        fn prop_exact_version_must_match_current(
            stream_id in arb_stream_id(),
            wrong_version in 1u64..10
        ) {
            let (mut writer_set, _dir) = setup_writer_set();
            let partition_key = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
            let partition_hash = uuid_to_partition_hash(partition_key);

            // Write initial event
            let initial_event = new_event(partition_hash, stream_id.clone(), ExpectedVersion::Empty);
            let versions = writer_set.validate_event_versions(partition_key, slice::from_ref(&initial_event)).unwrap();

            writer_set.handle_write(WriteOperation {
                partition_key,
                partition_id: 0,
                transaction_id: Uuid::new_v4(),
                events: &[initial_event],
                event_versions: versions,
                expected_partition_sequence: ExpectedVersion::Empty,
                unique_streams: 1,
                confirmation_count: 1,
            }).unwrap();

            // Current version should be 0, so wrong_version (1-9) should fail
            let exact_event = new_event(partition_hash, stream_id, ExpectedVersion::Exact(wrong_version));
            let result = writer_set.validate_event_versions(partition_key, &[exact_event]);
            prop_assert!(result.is_err());
        }

        #[test]
        fn prop_versions_increment_within_batch(
            stream_id in arb_stream_id(),
            num_events in 2usize..=10
        ) {
            let (writer_set, _dir) = setup_writer_set();
            let partition_key = Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes());
            let partition_hash = uuid_to_partition_hash(partition_key);

            // Create multiple events for the same stream with Any version
            let events: Vec<NewEvent> = (0..num_events)
                .map(|_| new_event(partition_hash, stream_id.clone(), ExpectedVersion::Any))
                .collect();

            let result = writer_set.validate_event_versions(partition_key, &events);
            prop_assert!(result.is_ok());

            let versions = result.unwrap();
            prop_assert_eq!(versions.len(), num_events);

            // First version should be Empty for a new stream
            prop_assert_eq!(versions[0], CurrentVersion::Empty);

            // Subsequent versions should increment
            for i in 1..versions.len() {
                match (&versions[i-1], &versions[i]) {
                    (CurrentVersion::Empty, CurrentVersion::Current(0)) => {}, // First transition
                    (CurrentVersion::Current(prev), CurrentVersion::Current(curr)) => {
                        prop_assert_eq!(*curr, prev + 1);
                    },
                    _ => prop_assert!(false, "Unexpected version sequence: {:?} -> {:?}", versions[i-1], versions[i])
                }
            }
        }

        #[test]
        fn prop_multiple_streams_maintain_independent_versions(
            stream_ids in vec(arb_stream_id(), 2..=4),
            events_per_stream in 1usize..=5
        ) {
            let (writer_set, _dir) = setup_writer_set();
            let partition_key = Uuid::new_v4();
            let partition_hash = uuid_to_partition_hash(partition_key);

            let mut all_events = Vec::new();
            let mut expected_stream_positions: HashMap<StreamId, usize> = HashMap::new();

            // Create events distributed across streams
            for _ in 0..events_per_stream * stream_ids.len() {
                for stream_id in &stream_ids {
                    let event = new_event(partition_hash, stream_id.clone(), ExpectedVersion::Any);
                    all_events.push(event);
                    *expected_stream_positions.entry(stream_id.clone()).or_insert(0) += 1;
                }
            }

            let result = writer_set.validate_event_versions(partition_key, &all_events);
            prop_assert!(result.is_ok());

            let versions = result.unwrap();
            prop_assert_eq!(versions.len(), all_events.len());

            // Track actual stream positions as we go through versions
            let mut actual_stream_positions: std::collections::HashMap<StreamId, u64> = std::collections::HashMap::new();

            for (i, event) in all_events.iter().enumerate() {
                let expected_pos = *actual_stream_positions.get(&event.stream_id).unwrap_or(&0);

                match versions[i] {
                    CurrentVersion::Empty if expected_pos == 0 => {
                        actual_stream_positions.insert(event.stream_id.clone(), 0);
                    },
                    CurrentVersion::Current(pos) if pos == expected_pos => {
                        actual_stream_positions.insert(event.stream_id.clone(), pos + 1);
                    },
                    _ => prop_assert!(false, "Version mismatch for stream {:?} at position {}: expected {}, got {:?}",
                                   event.stream_id, i, expected_pos, versions[i])
                }
            }
        }

        #[test]
        fn prop_batch_consistency_maintained(
            (_, partition_key, events) in arb_events_same_partition(15)
        ) {
            let (writer_set, _dir) = setup_writer_set();

            // Validate twice with same input - should get same result
            let result1 = writer_set.validate_event_versions(partition_key, &events);
            let result2 = writer_set.validate_event_versions(partition_key, &events);

            match (result1, result2) {
                (Ok(versions1), Ok(versions2)) => {
                    prop_assert_eq!(versions1, versions2);
                },
                (Err(_), Err(_)) => {
                    // Both failed, which is consistent
                },
                _ => prop_assert!(false, "Inconsistent validation results")
            }
        }
    }
}
