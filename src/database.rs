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

use arrayvec::ArrayVec;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use tokio::sync::oneshot;
use tracing::warn;
use uuid::Uuid;

use crate::{
    MAX_REDUNDANCY,
    bucket::{
        BucketId, BucketSegmentId, SegmentId, SegmentKind,
        event_index::ClosedEventIndex,
        reader_thread_pool::ReaderThreadPool,
        segment::{BucketSegmentReader, CommittedEvents, EventRecord, FlushedOffset},
        stream_index::{ClosedStreamIndex, EventStreamIter, StreamIndexRecord, StreamOffsets},
        writer_thread_pool::{AppendEventsBatch, WriterThreadPool},
    },
    error::{DatabaseError, QuorumError, ReadError, StreamIndexError, WriteError},
    id::{
        extract_event_id_bucket, extract_stream_hash, get_redundant_buckets, partition_id_to_bucket,
    },
    pool::create_thread_pool,
};

#[derive(Clone)]
pub struct Database {
    reader_pool: ReaderThreadPool,
    writer_pool: WriterThreadPool,
    num_buckets: u16,
    replication_factor: u8,
}

impl Database {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, DatabaseError> {
        DatabaseBuilder::new(dir).open()
    }

    pub async fn append_events(
        &self,
        events: Arc<AppendEventsBatch>,
        quorum: Quorum,
    ) -> QuorumResult<(), WriteError> {
        let bucket_ids = get_redundant_buckets(
            extract_stream_hash(events.partition_key()),
            self.num_buckets,
            self.replication_factor,
        );
        let mut replies: FuturesUnordered<_> = bucket_ids
            .into_iter()
            .map(|bucket_id| {
                let events = Arc::clone(&events);
                async move {
                    let result = self.writer_pool.append_events(bucket_id, events).await;
                    (bucket_id, result)
                }
            })
            .collect();

        let mut results = ArrayVec::new();
        let required_buckets = quorum.required_buckets(self.replication_factor) as usize;
        let mut successes = 0;
        while let Some((bucket_id, result)) = replies.next().await {
            if result.is_ok() {
                successes += 1;
            }
            results.push((bucket_id, result));
            if successes >= required_buckets {
                // Try collecting the remaining results without awaiting
                while let Some((bucket_id, result)) = replies.next().now_or_never().flatten() {
                    results.push((bucket_id, result));
                }
                return QuorumResult {
                    results,
                    quorum,
                    replication_factor: self.replication_factor,
                };
            }
        }

        QuorumResult {
            results,
            quorum,
            replication_factor: self.replication_factor,
        }
    }

    pub async fn read_event(
        &self,
        event_id: Uuid,
        quorum: Quorum,
    ) -> QuorumResult<Option<EventRecord<'static>>, ReadError> {
        let bucket_ids = get_redundant_buckets(
            extract_stream_hash(event_id),
            self.num_buckets,
            self.replication_factor,
        );
        let mut replies: FuturesUnordered<_> = bucket_ids.into_iter().map(|bucket_id| async move {
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

            (bucket_id, match reply_rx.await {
                Ok(res) => res.map(|events| events.and_then(|events| events.into_iter().next())),
                Err(_) => Err(ReadError::NoThreadReply),
            })
        }).collect();

        let mut results = ArrayVec::new();
        let required_buckets = quorum.required_buckets(self.replication_factor) as usize;
        let mut successes = 0;
        while let Some((bucket_id, result)) = replies.next().await {
            if result.is_ok() {
                successes += 1;
            }
            results.push((bucket_id, result));
            if successes >= required_buckets {
                // Try collecting the remaining results without awaiting
                while let Some((bucket_id, result)) = replies.next().now_or_never().flatten() {
                    results.push((bucket_id, result));
                }
                return QuorumResult {
                    results,
                    quorum,
                    replication_factor: self.replication_factor,
                };
            }
        }

        QuorumResult {
            results,
            quorum,
            replication_factor: self.replication_factor,
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
                        .get_mut(&root_bucket_id)
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
    replication_factor: u8,
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
            replication_factor: 3,
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
            replication_factor: self.replication_factor,
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

    pub fn replication_factor(&mut self, n: u8) -> &mut Self {
        assert!(n > 0, "replication factor must be greater than 0");
        assert!(
            n <= MAX_REDUNDANCY as u8,
            "replication factor cannot be not be greater than 12",
        );
        self.replication_factor = n;
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
    pub fn next_version(&self) -> u64 {
        match self {
            CurrentVersion::Current(version) => version + 1,
            CurrentVersion::NoStream => 0,
        }
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quorum {
    /// Only one replica needs to acknowledge the write.
    One,
    /// A majority of replicas (more than half) must acknowledge the write.
    Majority,
    /// Every replica must acknowledge the write.
    All,
}

impl Quorum {
    /// Given the total number of nodes, returns how many nodes are required to
    /// satisfy the quorum.
    pub fn required_buckets(self, replication_factor: u8) -> u8 {
        match self {
            Quorum::One => 1,
            Quorum::Majority => (replication_factor / 2) + 1,
            Quorum::All => replication_factor,
        }
    }
}

pub struct QuorumResult<T, E> {
    results: ArrayVec<(BucketId, Result<T, E>), MAX_REDUNDANCY>,
    quorum: Quorum,
    replication_factor: u8,
}

impl<T, E> QuorumResult<T, E> {
    // / Converts into a quorum result by checking if the number of successful responses
    // / meets the quorum. If so, returns an `Ok` containing all the successful responses.
    // / Otherwise, returns an `Err` with the original `QuorumResult`.
    // pub fn into_quorum_result(self) -> Result<ArrayVec<(BucketId, T), MAX_REDUNDANCY>, Self> {
    //     // Determine how many successful results are required.
    //     let required = self.quorum.required_buckets(self.replication_factor) as usize;
    //     let success_count = self.results.iter().filter(|(_, res)| res.is_ok()).count();

    //     if success_count >= required {
    //         // Collect the successful outcomes.
    //         let successes = self
    //             .results
    //             .into_iter()
    //             .filter_map(|(bucket_id, res)| res.ok().map(|value| (bucket_id, value)))
    //             .collect();
    //         Ok(successes)
    //     } else {
    //         Err(self)
    //     }
    // }

    pub fn into_quorum_result(self) -> Result<T, QuorumError<E>> {
        let required = self.quorum.required_buckets(self.replication_factor) as usize;
        let mut successes = ArrayVec::<(BucketId, T), MAX_REDUNDANCY>::new();
        let mut errors = ArrayVec::<(BucketId, E), MAX_REDUNDANCY>::new();

        for (bucket_id, result) in self.results {
            match result {
                Ok(val) => successes.push((bucket_id, val)),
                Err(e) => errors.push((bucket_id, e)),
            }
        }

        if successes.len() >= required {
            // Assuming that all successful writes return a consistent T,
            // we simply return the first one.
            Ok(successes.into_iter().next().unwrap().1)
        } else {
            Err(QuorumError::InsufficientSuccesses {
                successes: successes.len() as u8,
                required: required as u8,
                errors,
            })
        }
    }

    pub fn into_quorum_result_compare(self) -> Result<T, QuorumError<E>>
    where
        T: PartialEq,
    {
        let required = self.quorum.required_buckets(self.replication_factor) as usize;
        let mut successes = ArrayVec::<(BucketId, T), MAX_REDUNDANCY>::new();
        let mut errors = ArrayVec::<(BucketId, E), MAX_REDUNDANCY>::new();

        for (bucket_id, result) in self.results {
            match result {
                Ok(val) => successes.push((bucket_id, val)),
                Err(e) => errors.push((bucket_id, e)),
            }
        }

        if successes.len() >= required {
            // Assuming that all successful writes return a consistent T,
            // we simply return the first one.
            Ok(successes.into_iter().next().unwrap().1)
        } else {
            Err(QuorumError::InsufficientSuccesses {
                successes: successes.len() as u8,
                required: required as u8,
                errors,
            })
        }
    }
}
