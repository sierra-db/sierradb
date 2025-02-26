use std::{
    collections::HashMap,
    io, mem,
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
    id::{extract_event_id_bucket, validate_event_id},
};

use super::{
    BucketId, BucketSegmentId, SegmentKind,
    event_index::OpenEventIndex,
    file_name,
    flusher::FlushSender,
    reader_thread_pool::ReaderThreadPool,
    segment::{
        AppendEvent, AppendEventBody, AppendEventHeader, BucketSegmentReader, BucketSegmentWriter,
    },
    stream_index::OpenStreamIndex,
};

pub type LiveIndexes = Arc<RwLock<(OpenEventIndex, OpenStreamIndex)>>;

const TOTAL_BUFFERED_WRITES: usize = 1_000; // At most, there can be 1,000 writes buffered across all threads
const CHANNEL_BUFFER_MIN: usize = 16;

type Sender = mpsc::Sender<WriteRequest>;
type Receiver = mpsc::Receiver<WriteRequest>;

#[derive(Clone)]
pub struct WriterThreadPool {
    num_buckets: u16,
    num_threads: u16,
    senders: Arc<[Sender]>,
    indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
}

impl WriterThreadPool {
    pub fn new(
        dir: impl Into<PathBuf>,
        segment_size: usize,
        num_buckets: u16,
        num_threads: u16,
        flush_interval: Duration,
        reader_pool: &ReaderThreadPool,
        thread_pool: &Arc<ThreadPool>,
    ) -> io::Result<Self> {
        assert!(num_threads > 0);
        assert!(
            num_buckets >= num_threads,
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
                num_buckets,
                num_threads,
                flush_interval,
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
        thread::Builder::new()
            .name("writer-pool-flusher".to_string())
            .spawn({
                let mut senders: Vec<_> = senders.iter().map(|sender| sender.downgrade()).collect();
                move || {
                    let mut last_ran = Instant::now();
                    loop {
                        thread::sleep(flush_interval.saturating_sub(last_ran.elapsed()));
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
                            trace!("writer pool flusher stopping due to all workers being stopped");
                            break;
                        }
                    }
                }
            })?;

        Ok(WriterThreadPool {
            num_buckets,
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
        batch: AppendEventsBatch,
    ) -> oneshot::Receiver<io::Result<()>> {
        let bucket_id = extract_event_id_bucket(batch.partition_key, self.num_buckets);
        let target_thread = bucket_to_thread(bucket_id, self.num_buckets, self.num_threads);

        let sender = self
            .senders
            .get(target_thread as usize)
            .expect("sender should be present");

        let (reply_tx, reply_rx) = oneshot::channel();
        let send_res = sender
            .send(WriteRequest::AppendEvents(Box::new(AppendEventsRequest {
                bucket_id,
                batch,
                reply_tx,
            })))
            .await;
        match send_res {
            Ok(()) => reply_rx,
            Err(err) => match err.0 {
                WriteRequest::AppendEvents(req) => {
                    req.reply_tx
                        .send(Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "writer thread is no longer running",
                        )))
                        .unwrap();
                    reply_rx
                }
                WriteRequest::FlushPoll => unreachable!("we never send a flush poll here"),
            },
        }
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
    events: SmallVec<[WriteRequestEvent; 4]>,
}

impl AppendEventsBatch {
    pub fn single(event: WriteRequestEvent) -> io::Result<Self> {
        if !validate_event_id(event.event_id, &event.stream_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid event id: expected 8th and 9th bit of event id to be a hash of the partition key",
            ));
        }

        Ok(AppendEventsBatch {
            partition_key: event.partition_key,
            transaction_id: Uuid::nil(),
            events: smallvec![event],
        })
    }

    pub fn transaction(
        events: SmallVec<[WriteRequestEvent; 4]>,
        transaction_id: Uuid,
    ) -> io::Result<Self> {
        let Some(partition_key) = events
            .iter()
            .try_fold(None, |ret, event| match ret {
                ret @ Some(partition_key) => {
                    if event.partition_key != partition_key {
                        return Err(io::Error::new(io::ErrorKind::InvalidInput, "all events in a transaction must share the same partition key"));
                    }
                    if !validate_event_id(event.event_id, &event.stream_id) {
                        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid event id: expected 8th and 9th bit of event id to be a hash of the partition key"));
                    }

                    Ok(ret)
                },
                None => Ok(Some(
                    event.partition_key,
                )),
            })? else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "transaction has no events"));
        };

        Ok(AppendEventsBatch {
            partition_key,
            transaction_id,
            events,
        })
    }
}

enum WriteRequest {
    AppendEvents(Box<AppendEventsRequest>),
    FlushPoll,
}

struct AppendEventsRequest {
    bucket_id: BucketId,
    batch: AppendEventsBatch,
    reply_tx: oneshot::Sender<io::Result<()>>,
}

pub struct WriteRequestEvent {
    pub event_id: Uuid,
    pub partition_key: Uuid,
    pub stream_version: u64,
    pub timestamp: u64,
    pub stream_id: Arc<str>,
    pub event_name: String,
    pub metadata: Vec<u8>,
    pub payload: Vec<u8>,
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
    flush_interval: Duration,
    thread_pool: Arc<ThreadPool>,
}

impl WriterSet {
    fn flush(&mut self) -> io::Result<()> {
        self.last_flushed = Instant::now();
        self.writer.flush()?;
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

        Ok(())
    }

    fn flush_if_necessary(&mut self) {
        if self.last_flushed.elapsed() >= self.flush_interval {
            if let Err(err) = self.flush() {
                error!("failed to flush writer: {err}");
            }
        }
    }

    fn rollover(&mut self) -> io::Result<()> {
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
}

struct Worker {
    segment_size: usize,
    thread_id: u16,
    writers: HashMap<BucketId, WriterSet>,
}

impl Worker {
    fn new(
        dir: PathBuf,
        segment_size: usize,
        thread_id: u16,
        num_buckets: u16,
        num_threads: u16,
        flush_interval: Duration,
        reader_pool: &ReaderThreadPool,
        thread_pool: &Arc<ThreadPool>,
    ) -> io::Result<Self> {
        let mut writers = HashMap::new();
        let now = Instant::now();
        for bucket_id in 0..num_buckets {
            if bucket_to_thread(bucket_id, num_buckets, num_threads) == thread_id {
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
                    pending_indexes: Vec::with_capacity(256),
                    last_flushed: now,
                    flush_interval,
                    thread_pool: Arc::clone(thread_pool),
                };

                writers.insert(bucket_id, writer_set);
            }
        }

        Ok(Worker {
            segment_size,
            thread_id,
            writers,
        })
    }

    fn run(mut self, mut rx: Receiver) {
        // Process incoming write requests.
        'outer: while let Some(req) = rx.blocking_recv() {
            match req {
                WriteRequest::AppendEvents(req) => {
                    let AppendEventsRequest {
                        bucket_id,
                        batch:
                            AppendEventsBatch {
                                transaction_id,
                                events,
                                ..
                            },
                        reply_tx,
                    } = *req;

                    let Some(writer_set) = self.writers.get_mut(&bucket_id) else {
                        error!(
                            "thread {} received a request for bucket {} that isn't assigned here.",
                            self.thread_id, bucket_id
                        );
                        continue;
                    };

                    writer_set.flush_if_necessary();

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
                        + if transaction_id.is_nil() {
                            0
                        } else {
                            COMMIT_SIZE
                        };

                    macro_rules! tri {
                        ($($tt:tt)*) => {
                            match { $($tt)* } {
                                Ok(val) => val,
                                Err(err) => {
                                    if let Err(err) = writer_set.writer.set_len(file_size) {
                                        error!("failed to set segment file length after write error: {err}");
                                    }

                                    if let Err(Err(err)) = reply_tx.send(Err(err)) {
                                        error!("failed to append events: {err}");
                                    }

                                    continue 'outer;
                                }
                            }
                        };
                    }

                    if file_size as usize + writer_set.writer.buf_len() + events_size
                        > self.segment_size
                    {
                        tri!(writer_set.rollover());
                    }

                    let mut new_pending_indexes = Vec::with_capacity(events.len());

                    let event_count = events.len();
                    for event in events {
                        let body = AppendEventBody::new(
                            &event.stream_id,
                            &event.event_name,
                            &event.metadata,
                            &event.payload,
                        );
                        let header = tri!(AppendEventHeader::new(
                            &event.event_id,
                            &event.partition_key,
                            &transaction_id,
                            event.stream_version,
                            event.timestamp,
                            body
                        ));
                        let append = AppendEvent::new(header, body);
                        let (offset, _) = tri!(writer_set.writer.append_event(append));
                        new_pending_indexes.push(PendingIndex {
                            event_id: event.event_id,
                            partition_key: event.partition_key,
                            stream_id: event.stream_id,
                            stream_version: event.stream_version,
                            offset,
                        });
                    }

                    if !transaction_id.is_nil() {
                        let timestamp = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("time went backwards")
                            .as_nanos() as u64;
                        tri!(writer_set.writer.append_commit(
                            &transaction_id,
                            timestamp,
                            event_count as u32
                        ));
                    }

                    writer_set
                        .pending_indexes
                        .extend(new_pending_indexes.into_iter());

                    let _ = reply_tx.send(Ok(()));
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

struct PendingIndex {
    event_id: Uuid,
    partition_key: Uuid,
    stream_id: Arc<str>,
    stream_version: u64,
    offset: u64,
}

fn bucket_to_thread(bucket_id: BucketId, num_buckets: u16, num_threads: u16) -> u16 {
    assert!(
        num_buckets >= num_threads,
        "number of buckets cannot be less than number of threads"
    );

    if num_threads == 1 {
        return 0;
    }

    let buckets_per_thread = num_buckets / num_threads;
    let extra = num_buckets % num_threads;

    if bucket_id < (buckets_per_thread + 1) * extra {
        bucket_id / (buckets_per_thread + 1)
    } else {
        extra + (bucket_id - (buckets_per_thread + 1) * extra) / buckets_per_thread
    }
}
