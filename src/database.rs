use std::{
    collections::{BTreeMap, HashMap},
    fmt, fs, ops,
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
        stream_index::{ClosedStreamIndex, EventStreamIter, StreamIndexRecord, StreamOffsets},
        writer_thread_pool::{AppendEventsBatch, WriterThreadPool},
    },
    error::{DatabaseError, ReadError, StreamIndexError, WriteError},
    id::{extract_event_id_bucket, partition_id_to_bucket},
    pool::create_thread_pool,
};

#[derive(Clone)]
pub struct Database {
    reader_pool: ReaderThreadPool,
    writer_pool: WriterThreadPool,
    num_buckets: u16,
}

impl Database {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, DatabaseError> {
        DatabaseBuilder::new(dir).open()
    }

    pub async fn append_events(&self, events: AppendEventsBatch) -> Result<(), WriteError> {
        self.writer_pool.append_events(events).await.await.unwrap()
    }

    pub async fn read_event(
        &self,
        event_id: Uuid,
    ) -> Result<Option<EventRecord<'static>>, ReadError> {
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
                                    let _ = reply_tx.send(Err(Box::new(err).into()));
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

    pub async fn read_stream(
        &self,
        stream_id: impl Into<Arc<str>>,
        partition_key: Uuid,
    ) -> Result<EventStreamIter, StreamIndexError> {
        EventStreamIter::new(
            stream_id.into(),
            extract_event_id_bucket(partition_key, self.num_buckets),
            self.reader_pool.clone(),
            self.writer_pool.indexes(),
        )
        .await
    }

    pub async fn read_stream_latest_version(
        &self,
        stream_id: &Arc<str>,
        partition_id: u16,
    ) -> Result<Option<StreamLatestVersion>, StreamIndexError> {
        let root_bucket_id = partition_id_to_bucket(partition_id, self.num_buckets);
        let latest = self
            .writer_pool
            .with_stream_index(root_bucket_id, |_, stream_index| {
                stream_index.get(stream_id).map(
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
                )
            })
            .await
            .flatten();
        if let Some(latest) = latest {
            return Ok(Some(latest));
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader_pool.spawn({
            let stream_id = Arc::clone(stream_id);
            move |with_readers| {
                with_readers(move |readers| {
                    let res = readers
                        .get(&root_bucket_id)
                        .and_then(|segments| {
                            segments.iter().rev().find_map(|(_, reader_set)| {
                                let stream_index = reader_set.stream_index.as_ref()?;
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
                        .transpose();
                    let _ = reply_tx.send(res);
                });
            }
        });

        Ok(reply_rx.await.unwrap()?.map(
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

pub struct DatabaseBuilder {
    dir: PathBuf,
    segment_size: usize,
    num_buckets: u16,
    reader_pool_num_threads: u16,
    writer_pool_num_threads: u16,
    flush_interval_duration: Duration,
    flush_interval_events: u32,
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
            reader_pool_num_threads: reader_pool_size,
            writer_pool_num_threads: writer_pool_size,
            flush_interval_duration: Duration::from_millis(100),
            flush_interval_events: 1_000,
        }
    }

    pub fn open(&self) -> Result<Database, DatabaseError> {
        let _ = fs::create_dir_all(&self.dir);
        let thread_pool = Arc::new(create_thread_pool()?);
        let reader_pool = ReaderThreadPool::new(self.reader_pool_num_threads as usize);
        let writer_pool = WriterThreadPool::new(
            &self.dir,
            self.segment_size,
            self.num_buckets,
            self.writer_pool_num_threads,
            self.flush_interval_duration,
            self.flush_interval_events,
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
            } else if file_name_str.ends_with(&event_index_suffix) {
                segments.entry(bucket_segment_id).or_default().event_index = Some(entry.path());
            } else if file_name_str.ends_with(&stream_index_suffix) {
                segments.entry(bucket_segment_id).or_default().stream_index = Some(entry.path());
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

    pub fn reader_pool_num_threads(&mut self, n: u16) -> &mut Self {
        self.reader_pool_num_threads = n;
        self
    }

    pub fn writer_pool_num_threads(&mut self, n: u16) -> &mut Self {
        self.writer_pool_num_threads = n;
        self
    }

    pub fn flush_interval_duration(&mut self, interval: Duration) -> &mut Self {
        self.flush_interval_duration = interval;
        self
    }

    pub fn flush_interval_events(&mut self, events: u32) -> &mut Self {
        self.flush_interval_events = events;
        self
    }
}

#[derive(Debug, Default)]
struct UnopenedFileSet {
    events: Option<PathBuf>,
    event_index: Option<PathBuf>,
    stream_index: Option<PathBuf>,
}

/// The expected version **before** the event is inserted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    pub fn from_next_version(version: u64) -> Self {
        if version == 0 {
            ExpectedVersion::NoStream
        } else {
            ExpectedVersion::Exact(version - 1)
        }
    }

    pub fn into_next_version(self) -> Option<u64> {
        match self {
            ExpectedVersion::NoStream => Some(0),
            ExpectedVersion::Exact(version) => version.checked_add(1),
            _ => panic!("expected no stream or exact version"),
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

impl ops::AddAssign<u64> for CurrentVersion {
    fn add_assign(&mut self, rhs: u64) {
        match self {
            CurrentVersion::Current(current) => *current += rhs,
            CurrentVersion::NoStream => {
                if rhs > 0 {
                    *self = CurrentVersion::Current(rhs - 1)
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamLatestVersion {
    LatestVersion { partition_key: Uuid, version: u64 },
    ExternalBucket { partition_key: Uuid },
}
