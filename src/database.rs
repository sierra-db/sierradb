use std::{
    collections::{BTreeMap, HashMap},
    fs, io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::sync::oneshot;
use tracing::{error, warn};
use uuid::Uuid;

use crate::{
    bucket::{
        BucketId, BucketSegmentId, SegmentId, SegmentKind,
        event_index::ClosedEventIndex,
        reader_thread_pool::ReaderThreadPool,
        segment::{BucketSegmentReader, CommittedEvents, EventRecord, FlushedOffset},
        stream_index::{ClosedStreamIndex, EventStreamIter},
        writer_thread_pool::{AppendEventsBatch, WriterThreadPool},
    },
    id::{extract_event_id_bucket, stream_id_bucket},
    pool::create_thread_pool,
};

pub struct Database {
    reader_pool: ReaderThreadPool,
    writer_pool: WriterThreadPool,
    num_buckets: u16,
}

impl Database {
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        DatabaseBuilder::new(dir).open()
    }

    pub async fn append_events(&self, events: AppendEventsBatch) -> io::Result<()> {
        self.writer_pool
            .append_events(events)
            .await
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "writer thread panicked"))?
    }

    pub async fn read_event(&self, event_id: Uuid) -> io::Result<Option<EventRecord<'static>>> {
        let bucket_id = extract_event_id_bucket(event_id, self.num_buckets);
        let segment_id_offset = self
            .writer_pool
            .with_event_index(bucket_id, |segment_id, event_index| {
                event_index
                    .get(&event_id)
                    .map(|offset| (segment_id.load(Ordering::Acquire), offset))
            })
            .await
            .flatten();

        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| match segment_id_offset {
                Some((segment_id, offset)) => {
                    let Some(reader_set) = readers
                        .get_mut(&bucket_id)
                        .and_then(|segments| segments.get_mut(&segment_id))
                    else {
                        warn!(%bucket_id, %segment_id, "bucket or segment doesn't exist in reader pool");
                        let _ = reply_tx.send(Ok(None));
                        return;
                    };

                    let res = reader_set
                        .reader
                        .read_committed_events(offset, false)
                        .map(|events| events.map(CommittedEvents::into_owned));
                    let _ = reply_tx.send(res);
                }
                None => {
                    let Some(segments) = readers.get_mut(&bucket_id) else {
                        let _ = reply_tx.send(Ok(None));
                        return;
                    };

                    for reader_set in segments.values_mut().rev() {
                        if let Some(event_index) = &mut reader_set.event_index {
                            match event_index.get(&event_id) {
                                Ok(Some(offset)) => {
                                    let res = reader_set
                                        .reader
                                        .read_committed_events(offset, false)
                                        .map(|events| events.map(CommittedEvents::into_owned));
                                    let _ = reply_tx.send(res);
                                    return;
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    let _ = reply_tx.send(Err(err));
                                    return;
                                }
                            }
                        }
                    }

                    let _ = reply_tx.send(Ok(None));
                }
            })
        });

        match reply_rx.await {
            Ok(Ok(Some(events))) => Ok(events.into_iter().next()),
            Ok(Ok(None)) => Ok(None),
            Ok(Err(err)) => Err(err),
            Err(_) => {
                error!("no reply from reader pool");
                Ok(None)
            }
        }
    }

    pub async fn read_stream(&self, stream_id: impl Into<Arc<str>>) -> io::Result<EventStreamIter> {
        let stream_id = stream_id.into();
        let bucket_id = stream_id_bucket(&stream_id, self.num_buckets);
        EventStreamIter::new(
            stream_id,
            bucket_id,
            self.reader_pool.clone(),
            self.writer_pool.indexes(),
        )
        .await
    }
}

pub struct DatabaseBuilder {
    dir: PathBuf,
    segment_size: usize,
    num_buckets: u16,
    reader_pool_size: u16,
    writer_pool_size: u16,
    flush_interval: Duration,
}

impl DatabaseBuilder {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let cores = num_cpus::get_physical() as u16;
        let reader_pool_size = cores.clamp(4, 32);
        let writer_pool_size = (cores * 2).clamp(4, 64);

        DatabaseBuilder {
            dir: dir.into(),
            segment_size: 256_000_000,
            num_buckets: 64,
            reader_pool_size,
            writer_pool_size,
            flush_interval: Duration::from_millis(100),
        }
    }

    pub fn open(&self) -> io::Result<Database> {
        let _ = fs::create_dir_all(&self.dir);
        let thread_pool = Arc::new(
            create_thread_pool().map_err(|err| io::Error::new(io::ErrorKind::Other, err))?,
        );
        let reader_pool = ReaderThreadPool::new(self.reader_pool_size as usize);
        let writer_pool = WriterThreadPool::new(
            &self.dir,
            self.segment_size,
            self.num_buckets,
            self.writer_pool_size,
            self.flush_interval,
            &reader_pool,
            &thread_pool,
        )?;

        // Scan all previous segments and add to reader pool
        let events_suffix = format!(".{}.dat", SegmentKind::Events);
        let event_index_suffix = format!(".{}.dat", SegmentKind::EventIndex);
        let stream_index_suffix = format!(".{}.dat", SegmentKind::StreamIndex);
        let mut segments: BTreeMap<BucketSegmentId, UnopenedFileSet> = BTreeMap::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            if file_name_str.len() < "00000-0000000000.dat".len() {
                continue;
            }

            let Ok(bucket_id) = file_name_str[0..5].parse::<u16>() else {
                continue;
            };
            let Ok(segment_id) = file_name_str[6..16].parse::<u32>() else {
                continue;
            };
            let bucket_segment_id = BucketSegmentId::new(bucket_id, segment_id);

            if file_name_str.ends_with(&events_suffix) {
                segments.entry(bucket_segment_id).or_default().events = Some(entry.path());
                // println!("opening events: {bucket_segment_id}");
            } else if file_name_str.ends_with(&event_index_suffix) {
                segments.entry(bucket_segment_id).or_default().event_index = Some(entry.path());
                // println!("opening event index: {bucket_segment_id}");
                // let event_index = ClosedEventIndex::open(bucket_segment_id, entry.path())?;
                // segments.entry(bucket_segment_id).or_default().1 = Some(event_index);
            } else if file_name_str.ends_with(&stream_index_suffix) {
                segments.entry(bucket_segment_id).or_default().stream_index = Some(entry.path());
                // println!("opening stream index: {bucket_segment_id}");
                // let stream_index =
                //     ClosedStreamIndex::open(bucket_segment_id, entry.path(), self.segment_size)?;
                // segments.entry(bucket_segment_id).or_default().2 = Some(stream_index);
            }
        }

        let latest_segments: HashMap<BucketId, SegmentId> =
            segments
                .iter()
                .fold(HashMap::new(), |mut latest, (bucket_segment_id, _)| {
                    latest
                        .entry(bucket_segment_id.bucket_id)
                        .and_modify(|segment_id| {
                            *segment_id = (*segment_id).max(bucket_segment_id.segment_id);
                        })
                        .or_insert(bucket_segment_id.segment_id);
                    latest
                });

        // Remove latest segments
        segments.retain(|bucket_segment_id, _| {
            latest_segments
                .get(&bucket_segment_id.bucket_id)
                .map(|latest_segment_id| *latest_segment_id != bucket_segment_id.segment_id)
                .unwrap_or(true)
        });

        for (
            bucket_segment_id,
            UnopenedFileSet {
                events,
                event_index,
                stream_index,
            },
        ) in segments
        {
            let Some(events) = events else {
                continue;
            };

            let reader = BucketSegmentReader::open(
                events,
                FlushedOffset::new(Arc::new(AtomicU64::new(u64::MAX))),
            )?;

            let event_index = event_index
                .map(|path| ClosedEventIndex::open(bucket_segment_id, path))
                .transpose()?;
            let stream_index = stream_index
                .map(|path| ClosedStreamIndex::open(bucket_segment_id, path, self.segment_size))
                .transpose()?;

            reader_pool.add_bucket_segment(
                bucket_segment_id,
                &reader,
                event_index.as_ref(),
                stream_index.as_ref(),
            );
        }

        Ok(Database {
            reader_pool,
            writer_pool,
            num_buckets: self.num_buckets,
        })
    }

    pub fn segment_size(&mut self, n: usize) -> &mut Self {
        self.segment_size = n;
        self
    }

    pub fn num_buckets(&mut self, n: u16) -> &mut Self {
        self.num_buckets = n;
        self
    }

    pub fn reader_pool_size(&mut self, n: u16) -> &mut Self {
        self.reader_pool_size = n;
        self
    }

    pub fn writer_pool_size(&mut self, n: u16) -> &mut Self {
        self.writer_pool_size = n;
        self
    }

    pub fn flush_interval(&mut self, interval: Duration) -> &mut Self {
        self.flush_interval = interval;
        self
    }
}

#[derive(Debug, Default)]
struct UnopenedFileSet {
    events: Option<PathBuf>,
    event_index: Option<PathBuf>,
    stream_index: Option<PathBuf>,
}
