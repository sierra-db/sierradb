use std::collections::{BTreeMap, HashMap};
use std::ops::{self, RangeBounds};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{fmt, fs, process};

use rayon::{ThreadPool, ThreadPoolBuildError, ThreadPoolBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::StreamId;
use crate::bucket::event_index::ClosedEventIndex;
use crate::bucket::partition_index::{
    ClosedPartitionIndex, PartitionEventIter, PartitionIndexRecord, PartitionOffsets,
};
use crate::bucket::segment::{BucketSegmentReader, EventRecord, FlushedOffset};
use crate::bucket::stream_index::{
    ClosedStreamIndex, EventStreamIter, StreamIndexRecord, StreamOffsets,
};
use crate::bucket::{BucketId, BucketSegmentId, PartitionId, SegmentId, SegmentKind};
use crate::error::{
    DatabaseError, PartitionIndexError, ReadError, StreamIndexError, ThreadPoolError, WriteError,
};
use crate::reader_thread_pool::ReaderThreadPool;
use crate::writer_thread_pool::{AppendEventsBatch, WriterThreadPool};

#[derive(Clone)]
pub struct Database {
    dir: PathBuf,
    reader_pool: ReaderThreadPool,
    writer_pool: WriterThreadPool,
    total_buckets: u16,
}

impl Database {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, DatabaseError> {
        DatabaseBuilder::new(dir).open()
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub async fn append_events(
        &self,
        partition_id: PartitionId,
        events: AppendEventsBatch,
    ) -> Result<HashMap<StreamId, CurrentVersion>, WriteError> {
        let bucket_id = partition_id % self.total_buckets;
        self.writer_pool.append_events(bucket_id, events).await
    }

    pub async fn read_event(
        &self,
        partition_id: PartitionId,
        event_id: Uuid,
    ) -> Result<Option<EventRecord>, ReadError> {
        let bucket_id = partition_id % self.total_buckets;
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
                            .read_committed_events(offset, false);
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
                                            .read_committed_events(offset, false);
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
            Ok(res) => res.map(|events| events.and_then(|events| events.into_iter().next())),
            Err(_) => Err(ReadError::NoThreadReply),
        }
    }

    pub async fn read_partition(
        &self,
        partition_id: PartitionId,
    ) -> Result<PartitionEventIter, PartitionIndexError> {
        let bucket_id = partition_id % self.total_buckets;
        PartitionEventIter::new(
            partition_id,
            bucket_id,
            self.reader_pool.clone(),
            self.writer_pool.indexes(),
        )
        .await
    }

    pub async fn get_partition_sequence(
        &self,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionLatestSequence>, PartitionIndexError> {
        let bucket_id = partition_id % self.total_buckets;
        let latest = self
            .writer_pool
            .with_partition_index(bucket_id, |_, partition_index| {
                partition_index.get(partition_id).map(
                    |PartitionIndexRecord {
                         sequence_max,
                         offsets,
                         ..
                     }| {
                        match offsets {
                            PartitionOffsets::Offsets(_) => {
                                PartitionLatestSequence::LatestSequence {
                                    partition_id,
                                    sequence: *sequence_max,
                                }
                            }
                            &PartitionOffsets::ExternalBucket => {
                                PartitionLatestSequence::ExternalBucket { partition_id }
                            }
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
            move |with_readers| {
                with_readers(move |readers| {
                    let res = readers
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
                        .transpose();
                    let _ = reply_tx.send(res);
                });
            }
        });

        Ok(reply_rx
            .await
            .unwrap()?
            .map(
                |(PartitionIndexRecord { sequence_max, .. }, offsets)| match offsets {
                    PartitionOffsets::Offsets(_) => PartitionLatestSequence::LatestSequence {
                        partition_id,
                        sequence: sequence_max,
                    },
                    PartitionOffsets::ExternalBucket => {
                        PartitionLatestSequence::ExternalBucket { partition_id }
                    }
                },
            ))
    }

    pub async fn read_stream(
        &self,
        partition_id: PartitionId,
        stream_id: StreamId,
    ) -> Result<EventStreamIter, StreamIndexError> {
        let bucket_id = partition_id % self.total_buckets;
        EventStreamIter::new(
            stream_id,
            bucket_id,
            self.reader_pool.clone(),
            self.writer_pool.indexes(),
        )
        .await
    }

    pub async fn get_stream_version(
        &self,
        partition_id: PartitionId,
        stream_id: &Arc<str>,
    ) -> Result<Option<StreamLatestVersion>, StreamIndexError> {
        let bucket_id = partition_id % self.total_buckets;
        let latest = self
            .writer_pool
            .with_stream_index(bucket_id, |_, stream_index| {
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
    bucket_ids: Arc<[BucketId]>,
    total_buckets: u16,
    reader_pool_num_threads: u16,
    writer_pool_num_threads: u16,
    flush_interval_duration: Duration,
    flush_interval_events: u32,
}

impl DatabaseBuilder {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        let total_buckets = 64;
        let cores = num_cpus::get_physical() as u16;
        let reader_pool_size = cores.clamp(4, 32);
        let writer_pool_size = (cores * 2).clamp(4, 64);

        DatabaseBuilder {
            dir: dir.into(),
            segment_size: 256_000_000,
            bucket_ids: Arc::from((0..total_buckets).collect::<Vec<_>>()),
            total_buckets,
            reader_pool_num_threads: reader_pool_size,
            writer_pool_num_threads: writer_pool_size,
            flush_interval_duration: Duration::from_millis(100),
            flush_interval_events: 1_000,
        }
    }

    pub fn open(&self) -> Result<Database, DatabaseError> {
        assert!(
            self.bucket_ids.len() % self.writer_pool_num_threads as usize == 0,
            "number of writer threads ({}) must be a divisor of the number of buckets ({})",
            self.writer_pool_num_threads,
            self.bucket_ids.len()
        );

        let _ = fs::create_dir_all(&self.dir);
        let thread_pool = Arc::new(create_thread_pool()?);
        let reader_pool = ReaderThreadPool::new(self.reader_pool_num_threads as usize);
        let writer_pool = WriterThreadPool::new(
            &self.dir,
            self.segment_size,
            self.bucket_ids.clone(),
            self.writer_pool_num_threads,
            self.flush_interval_duration,
            self.flush_interval_events,
            &reader_pool,
            &thread_pool,
        )?;

        // Scan all previous segments and add to reader pool
        let mut segments: BTreeMap<BucketSegmentId, UnopenedFileSet> = BTreeMap::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name_str) = file_name.to_str() else {
                continue;
            };

            let Some((bucket_segment_id, segment_kind)) =
                SegmentKind::parse_file_name(file_name_str)
            else {
                continue;
            };

            if !self.bucket_ids.contains(&bucket_segment_id.bucket_id) {
                debug!("ignoring unowned bucket {}", bucket_segment_id.bucket_id);
                continue;
            }

            let segment = segments.entry(bucket_segment_id).or_default();

            match segment_kind {
                SegmentKind::Events => {
                    segment.events = Some(entry.path());
                }
                SegmentKind::EventIndex => {
                    segment.event_index = Some(entry.path());
                }
                SegmentKind::PartitionIndex => {
                    segment.partition_index = Some(entry.path());
                }
                SegmentKind::StreamIndex => {
                    segment.stream_index = Some(entry.path());
                }
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
                partition_index,
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
            let partition_index = partition_index
                .map(|path| ClosedPartitionIndex::open(bucket_segment_id, path))
                .transpose()?;
            let stream_index = stream_index
                .map(|path| ClosedStreamIndex::open(bucket_segment_id, path, self.segment_size))
                .transpose()?;

            reader_pool.add_bucket_segment(
                bucket_segment_id,
                &reader,
                event_index.as_ref(),
                partition_index.as_ref(),
                stream_index.as_ref(),
            );
        }

        Ok(Database {
            dir: self.dir.clone(),
            reader_pool,
            writer_pool,
            total_buckets: self.total_buckets,
        })
    }

    pub fn segment_size(&mut self, n: usize) -> &mut Self {
        self.segment_size = n;
        self
    }

    pub fn total_buckets(&mut self, total_buckets: u16) -> &mut Self {
        self.total_buckets = total_buckets;
        self
    }

    pub fn bucket_ids(&mut self, bucket_ids: impl Into<Arc<[BucketId]>>) -> &mut Self {
        let bucket_ids: Arc<[BucketId]> = bucket_ids.into();
        assert!(
            bucket_ids.len() <= self.total_buckets as usize,
            "bucket ids length cannot exceed total number of buckets ({})",
            self.total_buckets
        );
        self.bucket_ids = bucket_ids;
        self
    }

    pub fn bucket_ids_from_range(&mut self, range: impl RangeBounds<BucketId>) -> &mut Self {
        let min = match range.start_bound() {
            ops::Bound::Included(min) => *min,
            ops::Bound::Excluded(min) => min.checked_sub(1).unwrap(),
            ops::Bound::Unbounded => panic!("unbounded range not supported"),
        };
        let max = match range.end_bound() {
            ops::Bound::Included(max) => *max,
            ops::Bound::Excluded(max) => max.checked_sub(1).unwrap(),
            ops::Bound::Unbounded => panic!("unbounded range not supported"),
        };
        let bucket_ids = Arc::from((min..=max).collect::<Vec<_>>());
        self.bucket_ids(bucket_ids)
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
    partition_index: Option<PathBuf>,
    stream_index: Option<PathBuf>,
}

/// The expected version **before** the event is inserted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Actual position of a stream.
pub enum CurrentVersion {
    /// The stream doesn't exist.
    NoStream,
    /// The last event's number.
    Current(u64),
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
pub enum PartitionLatestSequence {
    LatestSequence {
        partition_id: PartitionId,
        sequence: u64,
    },
    ExternalBucket {
        partition_id: PartitionId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamLatestVersion {
    LatestVersion { partition_key: Uuid, version: u64 },
    ExternalBucket { partition_key: Uuid },
}

fn create_thread_pool() -> Result<ThreadPool, ThreadPoolBuildError> {
    ThreadPoolBuilder::new()
        .panic_handler(|err| match err.downcast::<ThreadPoolError>() {
            Ok(err) => {
                error!("{err}");
                match *err {
                    ThreadPoolError::FlushEventIndex { .. } => {
                        // What to do when flushing an event index fails?
                        // For now, just exit.
                        process::exit(1);
                    }
                    ThreadPoolError::FlushPartitionIndex { .. } => {
                        // What to do when flushing an partition index fails?
                        // For now, just exit.
                        process::exit(1);
                    }
                    ThreadPoolError::FlushStreamIndex { .. } => {
                        // What to do when flushing an stream index fails?
                        // For now, just exit.
                        process::exit(1);
                    }
                }
            }
            Err(err) => {
                if let Some(err) = err.downcast_ref::<&str>() {
                    error!("fatal error: {err}");
                    process::exit(1);
                }
                if let Some(err) = err.downcast_ref::<String>() {
                    error!("fatal error: {err}");
                    process::exit(1);
                }
                error!("unknown fatal error");
                process::exit(1);
            }
        })
        .build()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use smallvec::smallvec;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::writer_thread_pool::WriteEventRequest;

    // Helper functions for test setup
    async fn create_temp_db() -> (tempfile::TempDir, Database) {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let db = DatabaseBuilder::new(temp_dir.path())
            .flush_interval_events(1)
            .open()
            .expect("Failed to open database");
        (temp_dir, db)
    }

    fn create_test_event(
        event_id: Uuid,
        partition_key: Uuid,
        partition_id: PartitionId,
        stream_id_str: &str,
        version: ExpectedVersion,
    ) -> WriteEventRequest {
        WriteEventRequest {
            event_id,
            partition_key,
            partition_id,
            stream_id: StreamId::new(stream_id_str).expect("Invalid stream ID"),
            stream_version: version,
            event_name: "test_event".to_string(),
            timestamp: 12345678,     // Fixed timestamp for testing
            metadata: vec![1, 2, 3], // Some test metadata
            payload: b"test payload".to_vec(),
        }
    }

    // Database creation and initialization tests
    #[tokio::test]
    async fn test_database_open() {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let db = Database::open(temp_dir.path());
        assert!(db.is_ok(), "Failed to open database: {:?}", db.err());

        let db = db.unwrap();
        assert_eq!(
            db.dir(),
            &temp_dir.path().to_path_buf(),
            "Database directory mismatch"
        );
    }

    #[tokio::test]
    async fn test_database_builder_options() {
        let temp_dir = tempdir().expect("Failed to create temp directory");

        let db = DatabaseBuilder::new(temp_dir.path())
            .segment_size(1_000_000)
            .total_buckets(32)
            .reader_pool_num_threads(4)
            .writer_pool_num_threads(4)
            .flush_interval_duration(Duration::from_millis(50))
            .flush_interval_events(500)
            .open();

        assert!(db.is_ok(), "Failed to open database with custom options");
    }

    // Event writing tests
    #[tokio::test]
    async fn test_append_single_event() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let stream_id_str = "test-stream";

        let event = create_test_event(
            event_id,
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::NoStream,
        );

        let batch = AppendEventsBatch::single(event).expect("Failed to create batch");
        let result = db.append_events(partition_id, batch).await;

        assert!(result.is_ok(), "Failed to append event: {:?}", result.err());
        let versions = result.unwrap();

        let stream_id = StreamId::new(stream_id_str).unwrap();
        assert!(versions.contains_key(&stream_id), "Stream ID not in result");
        assert_eq!(
            versions[&stream_id],
            CurrentVersion::Current(0),
            "Expected version 0"
        );
    }

    #[tokio::test]
    async fn test_append_multiple_events_same_stream() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let stream_id_str = "test-stream";

        // Append 3 events to the same stream with sequential versions
        for i in 0..3 {
            let event = create_test_event(
                Uuid::new_v4(),
                partition_key,
                partition_id,
                stream_id_str,
                if i == 0 {
                    ExpectedVersion::NoStream
                } else {
                    ExpectedVersion::Exact(i - 1)
                },
            );

            let batch = AppendEventsBatch::single(event).unwrap();
            let result = db.append_events(partition_id, batch).await;

            assert!(
                result.is_ok(),
                "Failed to append event {}: {:?}",
                i,
                result.err()
            );
            let versions = result.unwrap();

            let stream_id = StreamId::new(stream_id_str).unwrap();
            assert_eq!(
                versions[&stream_id],
                CurrentVersion::Current(i),
                "Expected version {}",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_append_transaction() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let transaction_id = Uuid::new_v4();

        // Create events for two different streams in the same transaction
        let event1 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            "stream-1",
            ExpectedVersion::NoStream,
        );

        let event2 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            "stream-2",
            ExpectedVersion::NoStream,
        );

        let events = smallvec![event1, event2];
        let batch = AppendEventsBatch::transaction(events, transaction_id).unwrap();

        let result = db.append_events(partition_id, batch).await;
        assert!(
            result.is_ok(),
            "Failed to append transaction: {:?}",
            result.err()
        );

        let versions = result.unwrap();
        assert_eq!(versions.len(), 2, "Expected two stream versions");

        let stream1_id = StreamId::new("stream-1").unwrap();
        let stream2_id = StreamId::new("stream-2").unwrap();

        assert_eq!(
            versions[&stream1_id],
            CurrentVersion::Current(0),
            "Stream1 should be at version 0"
        );
        assert_eq!(
            versions[&stream2_id],
            CurrentVersion::Current(0),
            "Stream2 should be at version 0"
        );
    }

    // Event reading tests
    #[tokio::test]
    async fn test_read_event() {
        let (_temp_dir, db) = create_temp_db().await;

        // Append an event first
        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let stream_id_str = "test-stream";

        let event = create_test_event(
            event_id,
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::NoStream,
        );

        let batch = AppendEventsBatch::single(event).unwrap();
        let _ = db
            .append_events(partition_id, batch)
            .await
            .expect("Failed to append event");

        // Read the event back
        let result = db.read_event(partition_id, event_id).await;
        assert!(result.is_ok(), "Failed to read event: {:?}", result.err());

        let event_opt = result.unwrap();
        assert!(event_opt.is_some(), "Event should be found");

        let event_record = event_opt.unwrap();
        assert_eq!(event_record.event_id, event_id, "Event ID mismatch");
        assert_eq!(
            event_record.partition_key, partition_key,
            "Partition key mismatch"
        );
        assert_eq!(
            event_record.partition_id, partition_id,
            "Partition ID mismatch"
        );
        assert_eq!(event_record.event_name, "test_event", "Event name mismatch");
        assert_eq!(event_record.payload, b"test payload", "Payload mismatch");
    }

    #[tokio::test]
    async fn test_read_nonexistent_event() {
        let (_temp_dir, db) = create_temp_db().await;

        // Try to read an event that doesn't exist
        let partition_id = 1;
        let nonexistent_event_id = Uuid::new_v4();

        let result = db.read_event(partition_id, nonexistent_event_id).await;
        assert!(
            result.is_ok(),
            "Read should succeed even for nonexistent events"
        );

        let event_opt = result.unwrap();
        assert!(event_opt.is_none(), "Nonexistent event should return None");
    }

    // Partition reading tests
    #[tokio::test]
    async fn test_read_partition() {
        let (_temp_dir, db) = create_temp_db().await;

        // Append multiple events to the same partition
        let partition_id = 1;
        let partition_key = Uuid::new_v4();

        // Create and append 3 events
        for i in 0..3 {
            let event = create_test_event(
                Uuid::new_v4(),
                partition_key,
                partition_id,
                &format!("stream-{}", i),
                ExpectedVersion::NoStream,
            );

            let batch = AppendEventsBatch::single(event).unwrap();
            let _ = db
                .append_events(partition_id, batch)
                .await
                .expect("Failed to append event");
        }

        // Read the partition
        let mut partition_iter = db
            .read_partition(partition_id)
            .await
            .expect("Failed to read partition");

        // Collect all events
        let mut events = Vec::new();
        while let Some(event) = partition_iter.next().await.unwrap() {
            events.push(event);
        }

        assert_eq!(events.len(), 3, "Expected 3 events in the partition");

        // Check that events are ordered by sequence
        for i in 0..2 {
            assert!(
                events[i].partition_sequence <= events[i + 1].partition_sequence,
                "Events not ordered by sequence"
            );
        }
    }

    #[tokio::test]
    async fn test_get_partition_sequence() {
        let (_temp_dir, db) = create_temp_db().await;

        // Append events to get a sequence
        let partition_id = 1;
        let partition_key = Uuid::new_v4();

        // Create and append 3 events
        for i in 0..3 {
            let event = create_test_event(
                Uuid::new_v4(),
                partition_key,
                partition_id,
                "test-stream",
                if i == 0 {
                    ExpectedVersion::NoStream
                } else {
                    ExpectedVersion::Exact(i - 1)
                },
            );

            let batch = AppendEventsBatch::single(event).unwrap();
            let _ = db
                .append_events(partition_id, batch)
                .await
                .expect("Failed to append event");
        }

        // Get the partition sequence
        let seq_opt = db
            .get_partition_sequence(partition_id)
            .await
            .expect("Failed to get partition sequence");
        assert!(seq_opt.is_some(), "Expected a sequence to be returned");

        match seq_opt.unwrap() {
            PartitionLatestSequence::LatestSequence {
                partition_id: pid,
                sequence,
            } => {
                assert_eq!(pid, partition_id, "Partition ID mismatch");
                assert_eq!(
                    sequence, 2,
                    "Expected sequence to be 2 (0-based for 3 events)"
                );
            }
            PartitionLatestSequence::ExternalBucket { .. } => {
                panic!("Expected LatestSequence, got ExternalBucket");
            }
        }
    }

    // Stream reading tests
    #[tokio::test]
    async fn test_read_stream() {
        let (_temp_dir, db) = create_temp_db().await;

        // Append multiple events to the same stream
        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let stream_id_str = "test-stream";

        // Create and append 3 events
        for i in 0..3 {
            let event = create_test_event(
                Uuid::new_v4(),
                partition_key,
                partition_id,
                stream_id_str,
                if i == 0 {
                    ExpectedVersion::NoStream
                } else {
                    ExpectedVersion::Exact(i - 1)
                },
            );

            let batch = AppendEventsBatch::single(event).unwrap();
            let _ = db
                .append_events(partition_id, batch)
                .await
                .expect("Failed to append event");
        }

        // Read the stream
        let stream_id = StreamId::new(stream_id_str).unwrap();
        let mut stream_iter = db
            .read_stream(partition_id, stream_id)
            .await
            .expect("Failed to read stream");

        // Collect all events
        let mut events = Vec::new();
        while let Some(event) = stream_iter.next().await.unwrap() {
            events.push(event);
        }

        assert_eq!(events.len(), 3, "Expected 3 events in the stream");

        // Check that events are ordered by version
        for i in 0..2 {
            assert_eq!(events[i].stream_version, i as u64, "Event version mismatch");
            assert!(
                events[i].stream_version < events[i + 1].stream_version,
                "Events not ordered by version"
            );
        }
    }

    #[tokio::test]
    async fn test_get_stream_version() {
        let (_temp_dir, db) = create_temp_db().await;

        // Append events to get a version
        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let stream_id_str = "test-stream";

        // Create and append 3 events
        for i in 0..3 {
            let event = create_test_event(
                Uuid::new_v4(),
                partition_key,
                partition_id,
                stream_id_str,
                if i == 0 {
                    ExpectedVersion::NoStream
                } else {
                    ExpectedVersion::Exact(i - 1)
                },
            );

            let batch = AppendEventsBatch::single(event).unwrap();
            let _ = db
                .append_events(partition_id, batch)
                .await
                .expect("Failed to append event");
        }

        // Get the stream version
        let stream_id = Arc::from(stream_id_str);
        let version_opt = db
            .get_stream_version(partition_id, &stream_id)
            .await
            .expect("Failed to get stream version");
        assert!(version_opt.is_some(), "Expected a version to be returned");

        match version_opt.unwrap() {
            StreamLatestVersion::LatestVersion {
                partition_key: pk,
                version,
            } => {
                assert_eq!(pk, partition_key, "Partition key mismatch");
                assert_eq!(
                    version, 2,
                    "Expected version to be 2 (0-based for 3 events)"
                );
            }
            StreamLatestVersion::ExternalBucket { .. } => {
                panic!("Expected LatestVersion, got ExternalBucket");
            }
        }
    }

    // Concurrency and version conflict tests
    #[tokio::test]
    async fn test_optimistic_concurrency_control() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let stream_id_str = "test-stream";

        // First event - create stream
        let event1 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::NoStream,
        );

        let batch1 = AppendEventsBatch::single(event1).unwrap();
        let _ = db
            .append_events(partition_id, batch1)
            .await
            .expect("Failed to append first event");

        // Second event - tries to create stream again (should fail)
        let event2 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::NoStream, // This should fail as stream now exists
        );

        let batch2 = AppendEventsBatch::single(event2).unwrap();
        let result = db.append_events(partition_id, batch2).await;

        assert!(result.is_err(), "Expected version conflict error");
        // Specific error type would depend on your error definitions
    }

    #[tokio::test]
    async fn test_exact_version_concurrency() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let stream_id_str = "test-stream";

        // Create stream
        let event1 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::NoStream,
        );

        let batch1 = AppendEventsBatch::single(event1).unwrap();
        let _ = db
            .append_events(partition_id, batch1)
            .await
            .expect("Failed to append first event");

        // Try to append with wrong version
        let event2 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::Exact(1), // Wrong, should be 0
        );

        let batch2 = AppendEventsBatch::single(event2).unwrap();
        let result = db.append_events(partition_id, batch2).await;

        assert!(result.is_err(), "Expected version conflict error");
    }

    // Edge case tests
    #[tokio::test]
    async fn test_empty_stream() {
        let (_temp_dir, db) = create_temp_db().await;

        // Try to read a stream that doesn't exist
        let partition_id = 1;
        let nonexistent_stream = StreamId::new("nonexistent-stream").unwrap();

        let mut stream_iter = db
            .read_stream(partition_id, nonexistent_stream)
            .await
            .expect("Failed to read empty stream");
        let event = stream_iter.next().await.unwrap();
        assert!(event.is_none(), "Expected no events in empty stream");
    }

    #[tokio::test]
    async fn test_empty_partition() {
        let (_temp_dir, db) = create_temp_db().await;

        // Try to read a partition that hasn't been used
        let unused_partition_id = 999;

        let mut partition_iter = db
            .read_partition(unused_partition_id)
            .await
            .expect("Failed to read empty partition");
        let event = partition_iter.next().await.unwrap();
        assert!(event.is_none(), "Expected no events in empty partition");
    }

    #[tokio::test]
    async fn test_stream_exists_expectation() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let stream_id_str = "test-stream";

        // First create the stream
        let event1 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::NoStream,
        );

        let batch1 = AppendEventsBatch::single(event1).unwrap();
        let _ = db
            .append_events(partition_id, batch1)
            .await
            .expect("Failed to append first event");

        // Append with StreamExists expectation (should succeed)
        let event2 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::StreamExists,
        );

        let batch2 = AppendEventsBatch::single(event2).unwrap();
        let result = db.append_events(partition_id, batch2).await;

        assert!(
            result.is_ok(),
            "StreamExists expectation should have succeeded"
        );
    }

    #[tokio::test]
    async fn test_any_version_expectation() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let stream_id_str = "test-stream";

        // For a new stream, Any should work
        let event1 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::Any,
        );

        let batch1 = AppendEventsBatch::single(event1).unwrap();
        let result1 = db.append_events(partition_id, batch1).await;
        assert!(
            result1.is_ok(),
            "Any expectation should work for new stream"
        );

        // For an existing stream, Any should also work
        let event2 = create_test_event(
            Uuid::new_v4(),
            partition_key,
            partition_id,
            stream_id_str,
            ExpectedVersion::Any,
        );

        let batch2 = AppendEventsBatch::single(event2).unwrap();
        let result2 = db.append_events(partition_id, batch2).await;
        assert!(
            result2.is_ok(),
            "Any expectation should work for existing stream"
        );
    }

    // Multi-bucket tests
    #[tokio::test]
    async fn test_multiple_buckets() {
        let temp_dir = tempdir().expect("Failed to create temp directory");

        // Create a database with 4 buckets
        let db = DatabaseBuilder::new(temp_dir.path())
            .total_buckets(4)
            .flush_interval_events(1)
            .open()
            .expect("Failed to open database");

        // Events for different partitions should go to different buckets
        let partition_ids = [0, 1, 2, 3]; // Should map to different buckets

        for &partition_id in &partition_ids {
            let partition_key = Uuid::new_v4();
            let event = create_test_event(
                Uuid::new_v4(),
                partition_key,
                partition_id,
                &format!("stream-{}", partition_id),
                ExpectedVersion::NoStream,
            );

            let batch = AppendEventsBatch::single(event).unwrap();
            let result = db.append_events(partition_id, batch).await;
            assert!(
                result.is_ok(),
                "Failed to append to partition {}: {:?}",
                partition_id,
                result.err()
            );
        }

        // Read back events from each partition
        for &partition_id in &partition_ids {
            let seq_opt = db
                .get_partition_sequence(partition_id)
                .await
                .expect("Failed to get partition sequence");
            assert!(
                seq_opt.is_some(),
                "Expected sequence for partition {}",
                partition_id
            );

            match seq_opt.unwrap() {
                PartitionLatestSequence::LatestSequence { sequence, .. } => {
                    assert_eq!(
                        sequence, 0,
                        "Expected sequence 0 for partition {}",
                        partition_id
                    );
                }
                _ => panic!("Expected LatestSequence"),
            }
        }
    }

    // Test ExpectedVersion and CurrentVersion implementations
    #[test]
    fn test_expected_version_from_next() {
        assert_eq!(
            ExpectedVersion::from_next_version(0),
            ExpectedVersion::NoStream
        );
        assert_eq!(
            ExpectedVersion::from_next_version(1),
            ExpectedVersion::Exact(0)
        );
        assert_eq!(
            ExpectedVersion::from_next_version(42),
            ExpectedVersion::Exact(41)
        );
    }

    #[test]
    fn test_expected_version_into_next() {
        assert_eq!(ExpectedVersion::NoStream.into_next_version(), Some(0));
        assert_eq!(ExpectedVersion::Exact(0).into_next_version(), Some(1));
        assert_eq!(ExpectedVersion::Exact(41).into_next_version(), Some(42));
    }

    #[test]
    fn test_current_version_next() {
        assert_eq!(CurrentVersion::NoStream.next_version(), 0);
        assert_eq!(CurrentVersion::Current(0).next_version(), 1);
        assert_eq!(CurrentVersion::Current(41).next_version(), 42);
    }

    #[test]
    fn test_current_version_as_expected() {
        assert_eq!(
            CurrentVersion::NoStream.as_expected_version(),
            ExpectedVersion::NoStream
        );
        assert_eq!(
            CurrentVersion::Current(0).as_expected_version(),
            ExpectedVersion::Exact(0)
        );
        assert_eq!(
            CurrentVersion::Current(41).as_expected_version(),
            ExpectedVersion::Exact(41)
        );
    }

    #[test]
    fn test_current_version_add_assign() {
        let mut version = CurrentVersion::NoStream;
        version += 1;
        assert_eq!(version, CurrentVersion::Current(0));

        version += 2;
        assert_eq!(version, CurrentVersion::Current(2));
    }
}
