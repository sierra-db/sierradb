use std::{
    collections::HashMap,
    io,
    path::Path,
    sync::Arc,
    thread::{self},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use smallvec::{SmallVec, smallvec};
use thread_priority::ThreadBuilderExt;
use tokio::sync::{
    RwLock,
    mpsc::{self, error::TrySendError},
    oneshot,
};
use tracing::{error, trace};
use uuid::Uuid;

use crate::id::{stream_id_hash, validate_event_id};

use super::{
    SegmentKind,
    event_index::OpenEventIndex,
    file_name,
    flusher::FlushSender,
    reader_thread_pool::ReaderThreadPool,
    segment::{
        AppendEvent, AppendEventBody, AppendEventHeader, BucketSegmentReader, BucketSegmentWriter,
    },
    stream_index::OpenStreamIndex,
};

const TOTAL_BUFFERED_WRITES: usize = 1_000; // At most, there can be 1,000 writes buffered across all threads
const CHANNEL_BUFFER_MIN: usize = 16;

type Sender = mpsc::Sender<WriteRequest>;
type Receiver = mpsc::Receiver<WriteRequest>;

#[derive(Clone)]
pub struct WriterThreadPool {
    num_buckets: u16,
    num_threads: u16,
    senders: Arc<[Sender]>,
    indexes: Arc<HashMap<u16, (u32, Arc<RwLock<(OpenEventIndex, OpenStreamIndex)>>)>>,
}

impl WriterThreadPool {
    pub fn new(
        dir: impl AsRef<Path>,
        segment_size: usize,
        num_buckets: u16,
        num_threads: u16,
        flush_interval: Duration,
        reader_pool: &ReaderThreadPool,
    ) -> io::Result<Self> {
        assert!(num_threads > 0);
        assert!(
            num_buckets >= num_threads,
            "number of buckets cannot be less than number of threads"
        );

        let mut senders = Vec::with_capacity(num_threads as usize);
        let mut indexes = HashMap::new();

        for thread_id in 0..num_threads {
            let worker = Worker::new(
                &dir,
                segment_size,
                thread_id,
                num_buckets,
                num_threads,
                flush_interval,
                reader_pool,
            )?;
            for (bucket_id, writer_set) in &worker.writers {
                indexes.insert(
                    *bucket_id,
                    (writer_set.segment_id, Arc::clone(&writer_set.indexes)),
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

    pub fn indexes(
        &self,
    ) -> &Arc<HashMap<u16, (u32, Arc<RwLock<(OpenEventIndex, OpenStreamIndex)>>)>> {
        &self.indexes
    }

    pub async fn append_events(
        &self,
        batch: AppendEventsBatch,
    ) -> oneshot::Receiver<io::Result<()>> {
        let target_thread = bucket_to_thread(batch.bucket_id, self.num_buckets, self.num_threads);

        let sender = self
            .senders
            .get(target_thread as usize)
            .expect("sender should be present");

        let (reply_tx, reply_rx) = oneshot::channel();
        let send_res = sender
            .send(WriteRequest::AppendEvents(Box::new(AppendEventsRequest {
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

    pub async fn with_event_index<F, R>(&self, bucket_id: u16, f: F) -> Option<R>
    where
        F: FnOnce(u32, &OpenEventIndex) -> R,
    {
        let (segment_id, index) = self.indexes.get(&bucket_id)?;
        let lock = index.read().await;

        Some(f(*segment_id, &lock.0))
    }

    pub async fn with_stream_index<F, R>(&self, bucket_id: u16, f: F) -> Option<R>
    where
        F: FnOnce(u32, &OpenStreamIndex) -> R,
    {
        let (segment_id, index) = self.indexes.get(&bucket_id)?;
        let lock = index.read().await;

        Some(f(*segment_id, &lock.1))
    }
}

pub struct AppendEventsBatch {
    bucket_id: u16,
    transaction_id: Uuid,
    events: SmallVec<[WriteRequestEvent; 4]>,
}

impl AppendEventsBatch {
    pub fn single(num_buckets: u16, event: WriteRequestEvent) -> io::Result<Self> {
        if !validate_event_id(event.event_id, &event.stream_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid event id: expected 8th and 9th bit of event id to be a hash of the correlation id",
            ));
        }

        let bucket_id = stream_id_hash(&event.stream_id) % num_buckets;

        Ok(AppendEventsBatch {
            bucket_id,
            transaction_id: Uuid::nil(),
            events: smallvec![event],
        })
    }

    pub fn transaction(
        num_buckets: u16,
        events: SmallVec<[WriteRequestEvent; 4]>,
        transaction_id: Uuid,
    ) -> io::Result<Self> {
        let Some(bucket_id) = events
            .iter()
            .try_fold(None, |ret, event| match ret {
                ret @ Some((_, correlation_id)) => {
                    if event.correlation_id != correlation_id {
                        return Err(io::Error::new(io::ErrorKind::InvalidInput, "all events in a transaction must share the same correlation id"));
                    }
                    if !validate_event_id(event.event_id, &event.stream_id) {
                        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid event id: expected 8th and 9th bit of event id to be a hash of the correlation id"));
                    }

                    Ok(ret)
                },
                None => Ok(Some((
                    stream_id_hash(&event.stream_id) % num_buckets,
                    event.correlation_id,
                ))),
            })
            .map(|opt| opt.map(|(bucket_id, _)| bucket_id))? else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "transaction has no events"));
        };

        Ok(AppendEventsBatch {
            bucket_id,
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
    batch: AppendEventsBatch,
    reply_tx: oneshot::Sender<io::Result<()>>,
}

pub struct WriteRequestEvent {
    pub event_id: Uuid,
    pub correlation_id: Uuid,
    pub stream_version: u64,
    pub timestamp: u64,
    pub stream_id: Arc<str>,
    pub event_name: String,
    pub metadata: Vec<u8>,
    pub payload: Vec<u8>,
}

struct WriterSet {
    segment_id: u32,
    writer: BucketSegmentWriter,
    indexes: Arc<RwLock<(OpenEventIndex, OpenStreamIndex)>>,
    pending_indexes: Vec<PendingIndex>,
    last_flushed: Instant,
}

struct Worker {
    thread_id: u16,
    flush_interval: Duration,
    writers: HashMap<u16, WriterSet>,
}

impl Worker {
    fn new(
        dir: impl AsRef<Path>,
        segment_size: usize,
        thread_id: u16,
        num_buckets: u16,
        num_threads: u16,
        flush_interval: Duration,
        reader_pool: &ReaderThreadPool,
    ) -> io::Result<Self> {
        let dir = dir.as_ref();

        let mut writers = HashMap::new();
        let now = Instant::now();
        for bucket_id in 0..num_buckets {
            if bucket_to_thread(bucket_id, num_buckets, num_threads) == thread_id {
                let (bucket_segment_id, writer) =
                    BucketSegmentWriter::latest(bucket_id, dir, FlushSender::local())?;
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
                    segment_id: bucket_segment_id.segment_id,
                    writer,
                    indexes,
                    pending_indexes: Vec::with_capacity(256),
                    last_flushed: now,
                };

                writers.insert(bucket_id, writer_set);
            }
        }

        Ok(Worker {
            thread_id,
            flush_interval,
            writers,
        })
    }

    fn run(mut self, mut rx: Receiver) {
        let flush_if_necessary = |writer_set: &mut WriterSet| {
            if writer_set.last_flushed.elapsed() >= self.flush_interval {
                writer_set.last_flushed = Instant::now();
                match writer_set.writer.flush() {
                    Ok(()) => {
                        let mut indexes = writer_set.indexes.blocking_write();
                        for PendingIndex {
                            event_id,
                            correlation_id,
                            stream_id,
                            stream_version,
                            offset,
                        } in writer_set.pending_indexes.drain(..)
                        {
                            indexes.0.insert(event_id, offset);
                            if let Err(err) =
                                indexes
                                    .1
                                    .insert(stream_id, stream_version, correlation_id, offset)
                            {
                                error!("failed to insert index: {err}");
                            }
                        }
                    }
                    Err(err) => {
                        error!("failed to flush writer: {err}");
                    }
                }
            }
        };

        // Process incoming write requests.
        'outer: while let Some(req) = rx.blocking_recv() {
            match req {
                WriteRequest::AppendEvents(req) => {
                    let AppendEventsRequest {
                        batch:
                            AppendEventsBatch {
                                bucket_id,
                                transaction_id,
                                events,
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

                    flush_if_necessary(writer_set);

                    let file_size = writer_set.writer.file_size();

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
                            &event.correlation_id,
                            &transaction_id,
                            event.stream_version,
                            event.timestamp,
                            body
                        ));
                        let append = AppendEvent::new(header, body);
                        let (offset, _) = tri!(writer_set.writer.append_event(append));
                        new_pending_indexes.push(PendingIndex {
                            event_id: event.event_id,
                            correlation_id: event.correlation_id,
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
                        flush_if_necessary(writer_set);
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
    correlation_id: Uuid,
    stream_id: Arc<str>,
    stream_version: u64,
    offset: u64,
}

fn bucket_to_thread(bucket_id: u16, num_buckets: u16, num_threads: u16) -> u16 {
    assert!(
        num_buckets >= num_threads,
        "number of buckets cannot be less than number of threads"
    );

    let buckets_per_thread = num_buckets / num_threads;
    let extra = num_buckets % num_threads;

    if bucket_id < (buckets_per_thread + 1) * extra {
        bucket_id / (buckets_per_thread + 1)
    } else {
        extra + (bucket_id - (buckets_per_thread + 1) * extra) / buckets_per_thread
    }
}
