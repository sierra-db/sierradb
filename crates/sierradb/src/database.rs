use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, BufWriter};
use std::ops::{self, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, process};

use libc::{RLIMIT_NOFILE, getrlimit, rlimit, setrlimit};
use rayon::{ThreadPool, ThreadPoolBuildError, ThreadPoolBuilder};
use seglog::read::ReadHint;
use serde::{Deserialize, Serialize};
pub use sierradb_protocol::{CurrentVersion, ExpectedVersion, VersionGap};
use smallvec::SmallVec;
use tokio::sync::oneshot;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::bucket::event_index::ClosedEventIndex;
use crate::bucket::iter::{PartitionIter, PartitionIterConfig, StreamIter, StreamIterConfig};
use crate::bucket::partition_index::{ClosedPartitionIndex, PartitionIndexRecord};
use crate::bucket::segment::{BucketSegmentReader, CommittedEvents, EventRecord};
use crate::bucket::stream_index::{ClosedStreamIndex, StreamIndexRecord};
use crate::bucket::{BucketId, BucketSegmentId, PartitionId, SegmentId};
use crate::cache::BLOCK_SIZE;
use crate::error::{
    DatabaseError, EventValidationError, MetadataError, PartitionIndexError, ReadError,
    StreamIndexError, ThreadPoolError, WriteError,
};
use crate::id::{set_uuid_flag, uuid_to_partition_hash, validate_event_id};
use crate::reader_thread_pool::ReaderThreadPool;
use crate::writer_thread_pool::{AppendResult, WriterThreadPool};
use crate::{IterDirection, StreamId};

#[derive(Clone)]
pub struct Database {
    dir: PathBuf,
    reader_pool: ReaderThreadPool,
    writer_pool: WriterThreadPool,
    total_buckets: u16,
}

impl Database {
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, DatabaseError> {
        DatabaseBuilder::new().open(dir)
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    pub fn total_buckets(&self) -> u16 {
        self.total_buckets
    }

    pub fn reader_pool(&self) -> &ReaderThreadPool {
        &self.reader_pool
    }

    pub async fn shutdown(&self) {
        self.writer_pool.shutdown().await;
    }

    pub async fn append_events(&self, events: Transaction) -> Result<AppendResult, WriteError> {
        let bucket_id = events.partition_id % self.total_buckets;
        self.writer_pool.append_events(bucket_id, events).await
    }

    pub async fn set_confirmations(
        &self,
        partition_id: PartitionId,
        mut offsets: SmallVec<[u64; 4]>,
        transaction_id: Uuid,
        confirmation_count: u8,
    ) -> Result<(), ReadError> {
        let bucket_id = partition_id % self.total_buckets;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| {
                let Some(segments) = readers.get_mut(&bucket_id) else {
                    let _ = reply_tx.send(Err(ReadError::BucketIdNotFound { bucket_id }));
                    return;
                };

                let mut errors = Vec::new();

                for reader_set in segments.values_mut().rev() {
                    offsets.retain(|offset| {
                        match reader_set.reader.set_confirmations(
                            *offset,
                            &transaction_id,
                            confirmation_count,
                        ) {
                            Ok(false) => true,
                            Ok(true) => false,
                            Err(err) => {
                                errors.push(err);
                                false
                            }
                        }
                    });
                    if offsets.is_empty() {
                        break;
                    }
                }

                for offset in offsets {
                    errors.push(ReadError::TransactionIdNotFoundAtOffset {
                        transaction_id,
                        offset,
                    });
                }

                if !errors.is_empty() {
                    let _ = reply_tx.send(Err(ReadError::SetConfirmations { errors }));
                } else {
                    let _ = reply_tx.send(Ok(()));
                }
            })
        });

        reply_rx.await?
    }

    pub async fn read_event(
        &self,
        partition_id: PartitionId,
        event_id: Uuid,
    ) -> Result<Option<EventRecord>, ReadError> {
        Ok(self
            .read_transaction(partition_id, event_id)
            .await?
            .and_then(|events| events.into_iter().next()))
    }

    pub async fn read_transaction(
        &self,
        partition_id: PartitionId,
        first_event_id: Uuid,
    ) -> Result<Option<CommittedEvents>, ReadError> {
        let bucket_id = partition_id % self.total_buckets;
        let segment_id_offset = self
            .writer_pool
            .with_event_index(bucket_id, |segment_id, event_index| {
                event_index
                    .get(&first_event_id)
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
                            .read_committed_events(offset, ReadHint::Random)
                            .map(|(events, _)| events);
                        let _ = reply_tx.send(res);
                    }
                    None => {
                        let Some(segments) = readers.get_mut(&bucket_id) else {
                            let _ = reply_tx.send(Ok(None));
                            return;
                        };

                        for reader_set in segments.values_mut().rev() {
                            if let Some(event_index) = &mut reader_set.event_index {
                                match event_index.get(&first_event_id) {
                                    Ok(Some(offset)) => {
                                        let res = reader_set
                                            .reader
                                            .read_committed_events(offset, ReadHint::Random)
                                            .map(|(events, _)| events);
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
            Ok(res) => res,
            Err(_) => Err(ReadError::NoThreadReply),
        }
    }

    pub async fn read_partition(
        &self,
        partition_id: PartitionId,
        from_sequence: u64,
        dir: IterDirection,
    ) -> Result<PartitionIter, PartitionIndexError> {
        let bucket_id = partition_id % self.total_buckets;
        PartitionIter::new(
            PartitionIterConfig::new(partition_id),
            bucket_id,
            self.reader_pool.clone(),
            self.writer_pool.indexes().clone(),
            from_sequence,
            dir,
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
                    |PartitionIndexRecord { sequence_max, .. }| PartitionLatestSequence {
                        partition_id,
                        sequence: *sequence_max,
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
                                match partition_index.get_key(partition_id).transpose()? {
                                    Ok(record) => Some(Ok(record.sequence_max)),
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
            .map(|sequence| PartitionLatestSequence {
                partition_id,
                sequence,
            }))
    }

    pub async fn read_stream(
        &self,
        partition_id: PartitionId,
        stream_id: StreamId,
        from_version: u64,
        dir: IterDirection,
    ) -> Result<StreamIter, StreamIndexError> {
        let bucket_id = partition_id % self.total_buckets;
        StreamIter::new(
            StreamIterConfig::new(stream_id),
            bucket_id,
            self.reader_pool.clone(),
            self.writer_pool.indexes().clone(),
            from_version,
            dir,
        )
        .await
    }

    pub async fn get_stream_version(
        &self,
        partition_id: PartitionId,
        stream_id: &StreamId,
    ) -> Result<Option<StreamLatestVersion>, StreamIndexError> {
        let bucket_id = partition_id % self.total_buckets;
        let latest = self
            .writer_pool
            .with_stream_index(bucket_id, |_, stream_index| {
                stream_index.get(stream_id).map(
                    |StreamIndexRecord {
                         partition_key,
                         version_max,
                         ..
                     }| {
                        StreamLatestVersion {
                            partition_key: *partition_key,
                            version: *version_max,
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
            let stream_id = stream_id.clone();
            move |with_readers| {
                with_readers(move |readers| {
                    let res = readers
                        .get_mut(&bucket_id)
                        .and_then(|segments| {
                            segments.iter_mut().rev().find_map(|(_, reader_set)| {
                                let stream_index = reader_set.stream_index.as_mut()?;
                                match stream_index.get_key(&stream_id).transpose()? {
                                    Ok(record) => {
                                        Some(Ok((record.partition_key, record.version_max)))
                                    }
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
            .map(|(partition_key, version_max)| StreamLatestVersion {
                partition_key,
                version: version_max,
            }))
    }
}

const META_FILENAME: &str = "meta.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatabaseMeta {
    version: u16,
    created_at: u64,
    total_buckets: u16,
    segment_size_bytes: usize,
}

impl DatabaseMeta {
    const CURRENT_VERSION: u16 = 1;

    fn new(total_buckets: u16, segment_size_bytes: usize) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);

        DatabaseMeta {
            version: Self::CURRENT_VERSION,
            created_at,
            total_buckets,
            segment_size_bytes,
        }
    }

    fn load(path: &Path) -> Result<Self, MetadataError> {
        let file = fs::File::open(path).map_err(MetadataError::Io)?;
        let reader = BufReader::new(file);
        let meta: DatabaseMeta = serde_json::from_reader(reader)?;
        Ok(meta)
    }

    fn save(&self, path: &Path) -> Result<(), MetadataError> {
        let file = fs::File::create(path).map_err(MetadataError::Io)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, self)?;
        Ok(())
    }

    fn validate(&self, builder: &DatabaseBuilder) -> Result<(), MetadataError> {
        if self.version > Self::CURRENT_VERSION {
            return Err(MetadataError::UnsupportedVersion {
                actual: self.version,
            });
        }

        if self.total_buckets != builder.total_buckets {
            return Err(MetadataError::TotalBucketsMismatch {
                expected: self.total_buckets,
                actual: builder.total_buckets,
            });
        }

        if self.segment_size_bytes != builder.segment_size_bytes {
            return Err(MetadataError::SegmentSizeMismatch {
                expected: self.segment_size_bytes,
                actual: builder.segment_size_bytes,
            });
        }

        Ok(())
    }
}

pub struct DatabaseBuilder {
    segment_size_bytes: usize,
    bucket_ids: Arc<[BucketId]>,
    total_buckets: u16,
    reader_threads: u16,
    writer_threads: Option<u16>,
    sync_interval: Duration,
    sync_idle_interval: Duration,
    max_batch_size: usize,
    min_sync_bytes: usize,
    cache_capacity_bytes: usize,
    compression: bool,
}

impl DatabaseBuilder {
    pub fn new() -> Self {
        let cores = num_cpus::get_physical() as u16;
        let total_buckets = (cores * 2).clamp(2, 16);
        let reader_threads = (cores * 2).clamp(4, 32);

        DatabaseBuilder {
            segment_size_bytes: 256 * 1024 * 1024,
            bucket_ids: Arc::from((0..total_buckets).collect::<Vec<_>>()),
            total_buckets,
            reader_threads,
            writer_threads: None,
            sync_interval: Duration::from_millis(5),
            sync_idle_interval: Duration::from_millis(50),
            max_batch_size: 50,
            min_sync_bytes: 4096,
            cache_capacity_bytes: 256 * 1024 * 1024,
            compression: true,
        }
    }

    pub fn open(&self, dir: impl Into<PathBuf>) -> Result<Database, DatabaseError> {
        assert!(
            self.bucket_ids.len() <= self.total_buckets as usize,
            "bucket ids length cannot exceed total number of buckets ({})",
            self.total_buckets
        );

        let writer_threads = self
            .writer_threads
            .unwrap_or_else(|| self.bucket_ids.len() as u16);

        assert!(
            self.bucket_ids
                .len()
                .is_multiple_of(writer_threads as usize),
            "number of writer threads ({writer_threads}) must be a divisor of the number of buckets ({})",
            self.bucket_ids.len()
        );
        assert!(
            writer_threads as usize <= self.bucket_ids.len(),
            "number of writer threads ({writer_threads}) cannot be more than the number of buckets ({})",
            self.bucket_ids.len()
        );

        let dir = dir.into();

        let buckets_dir = dir.join("buckets");
        if !buckets_dir.exists() {
            fs::create_dir_all(&buckets_dir)?;
        }

        // Load or create metadata file
        let meta_path = dir.join(META_FILENAME);
        if meta_path.exists() {
            let meta = DatabaseMeta::load(&meta_path)?;
            meta.validate(self)?;
        } else {
            let meta = DatabaseMeta::new(self.total_buckets, self.segment_size_bytes);
            meta.save(&meta_path)?;
        }

        // Update rlimit, so we don't get an error about too many open files
        unsafe {
            let mut rlimit = rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };

            if getrlimit(RLIMIT_NOFILE, &mut rlimit) == 0 {
                let desired_rlimit = self.bucket_ids.len() as u64 * 4 + 8; // 8 added for safety

                if rlimit.rlim_cur < desired_rlimit {
                    rlimit.rlim_cur = std::cmp::min(desired_rlimit, rlimit.rlim_max);
                    if setrlimit(RLIMIT_NOFILE, &rlimit) == 0 {
                        debug!("successfully updated rlimit to {}", rlimit.rlim_cur);
                    } else {
                        error!("failed to update rlimit to {}", rlimit.rlim_cur);
                    }
                }
            } else {
                error!("failed to query rlimit");
            }
        }

        let thread_pool = Arc::new(create_thread_pool()?);
        let reader_pool = ReaderThreadPool::new(
            self.reader_threads as usize,
            &self.bucket_ids,
            self.cache_capacity_bytes,
        );
        let writer_pool = WriterThreadPool::new(
            &dir,
            self.segment_size_bytes,
            self.bucket_ids.clone(),
            writer_threads,
            self.sync_interval,
            self.sync_idle_interval,
            self.max_batch_size,
            self.min_sync_bytes,
            self.compression,
            &reader_pool,
            &thread_pool,
        )?;

        // Scan all previous segments and add to reader pool
        let mut segments: BTreeMap<BucketSegmentId, UnopenedFileSet> = BTreeMap::new();

        for bucket_entry in fs::read_dir(&buckets_dir)? {
            let bucket_entry = bucket_entry?;
            if !bucket_entry.file_type()?.is_dir() {
                continue;
            }

            // Extract bucket ID from directory name
            let bucket_name = bucket_entry.file_name();
            let Some(bucket_name_str) = bucket_name.to_str() else {
                continue;
            };
            let Ok(bucket_id) = bucket_name_str.parse::<BucketId>() else {
                continue;
            };

            if !self.bucket_ids.contains(&bucket_id) {
                debug!("ignoring unowned bucket {}", bucket_id);
                continue;
            }

            // Access the 'segments' subdirectory within this bucket
            let segments_dir = bucket_entry.path().join("segments");
            let _ = fs::create_dir_all(&segments_dir);
            if !segments_dir.exists() {
                continue;
            }

            // Iterate through each segment directory
            for segment_entry in fs::read_dir(&segments_dir)? {
                let segment_entry = segment_entry?;
                if !segment_entry.file_type()?.is_dir() {
                    continue;
                }

                // Extract segment ID from directory name
                let segment_name = segment_entry.file_name();
                let Some(segment_name_str) = segment_name.to_str() else {
                    continue;
                };
                let Ok(segment_id) = segment_name_str.parse::<SegmentId>() else {
                    continue;
                };

                let bucket_segment_id = BucketSegmentId::new(bucket_id, segment_id);
                let segment = segments.entry(bucket_segment_id).or_default();

                // Check for each file type within this segment directory
                for file_entry in fs::read_dir(segment_entry.path())? {
                    let file_entry = file_entry?;
                    let file_name = file_entry.file_name();
                    let Some(file_name_str) = file_name.to_str() else {
                        continue;
                    };

                    // Match the file to its type
                    match file_name_str {
                        "data.evts" => {
                            segment.events = Some(file_entry.path());
                        }
                        "index.eidx" => {
                            segment.event_index = Some(file_entry.path());
                        }
                        "partition.pidx" => {
                            segment.partition_index = Some(file_entry.path());
                        }
                        "stream.sidx" => {
                            segment.stream_index = Some(file_entry.path());
                        }
                        _ => continue,
                    }
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

            let reader = BucketSegmentReader::open(events, None)?;

            let event_index = event_index
                .map(|path| ClosedEventIndex::open(bucket_segment_id, path))
                .transpose()?;
            let partition_index = partition_index
                .map(|path| ClosedPartitionIndex::open(bucket_segment_id, path))
                .transpose()?;
            let stream_index = stream_index
                .map(|path| {
                    ClosedStreamIndex::open(bucket_segment_id, path, self.segment_size_bytes)
                })
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
            dir,
            reader_pool,
            writer_pool,
            total_buckets: self.total_buckets,
        })
    }

    pub fn segment_size_bytes(&mut self, n: usize) -> &mut Self {
        // This minimum is somewhat arbitrary, but it ensures our cache is actually used
        // and segment sizes shouldn't be this small anyway
        assert!(
            n >= BLOCK_SIZE * 2,
            "segment size must be at least {} bytes",
            BLOCK_SIZE * 2
        );
        self.segment_size_bytes = n;
        self
    }

    pub fn total_buckets(&mut self, total_buckets: u16) -> &mut Self {
        self.total_buckets = total_buckets;
        self
    }

    pub fn bucket_ids(&mut self, bucket_ids: impl Into<Arc<[BucketId]>>) -> &mut Self {
        self.bucket_ids = bucket_ids.into();
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

    pub fn reader_threads(&mut self, n: u16) -> &mut Self {
        self.reader_threads = n;
        self
    }

    pub fn writer_threads(&mut self, n: u16) -> &mut Self {
        self.writer_threads = Some(n);
        self
    }

    pub fn sync_interval(&mut self, interval: Duration) -> &mut Self {
        self.sync_interval = interval;
        self
    }

    pub fn sync_idle_interval(&mut self, interval: Duration) -> &mut Self {
        self.sync_idle_interval = interval;
        self
    }

    pub fn max_batch_size(&mut self, events: usize) -> &mut Self {
        self.max_batch_size = events;
        self
    }

    pub fn min_sync_bytes(&mut self, bytes: usize) -> &mut Self {
        self.min_sync_bytes = bytes;
        self
    }

    pub fn cache_capacity_bytes(&mut self, bytes: usize) -> &mut Self {
        self.cache_capacity_bytes = bytes;
        self
    }

    pub fn compression(&mut self, enabled: bool) -> &mut Self {
        self.compression = enabled;
        self
    }
}

impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
struct UnopenedFileSet {
    events: Option<PathBuf>,
    event_index: Option<PathBuf>,
    partition_index: Option<PathBuf>,
    stream_index: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionLatestSequence {
    pub partition_id: PartitionId,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamLatestVersion {
    pub partition_key: Uuid,
    pub version: u64,
}

fn create_thread_pool() -> Result<ThreadPool, ThreadPoolBuildError> {
    ThreadPoolBuilder::new()
        .thread_name(|i| format!("flusher-{i}"))
        .num_threads(4)
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transaction {
    pub(crate) partition_key: Uuid,
    pub(crate) partition_id: PartitionId,
    pub(crate) transaction_id: Uuid,
    pub(crate) events: SmallVec<[NewEvent; 4]>,
    pub(crate) expected_partition_sequence: ExpectedVersion,
    pub(crate) confirmation_count: u8,
}

impl Transaction {
    pub fn new(
        partition_key: Uuid,
        partition_id: PartitionId,
        events: SmallVec<[NewEvent; 4]>,
    ) -> Result<Self, EventValidationError> {
        if events.is_empty() {
            return Err(EventValidationError::EmptyTransaction);
        }

        let partition_hash = uuid_to_partition_hash(partition_key);

        events.iter().try_fold((), |_, event| {
            if !validate_event_id(event.event_id, partition_hash) {
                return Err(EventValidationError::InvalidEventId);
            }

            Ok(())
        })?;

        let transaction_id = set_uuid_flag(Uuid::new_v4(), events.len() == 1);

        Ok(Transaction {
            partition_key,
            partition_id,
            transaction_id,
            events,
            expected_partition_sequence: ExpectedVersion::Any,
            confirmation_count: 0,
        })
    }

    pub fn expected_partition_sequence(mut self, sequence: ExpectedVersion) -> Self {
        self.expected_partition_sequence = sequence;
        self
    }

    pub fn get_expected_partition_sequence(&self) -> ExpectedVersion {
        self.expected_partition_sequence
    }

    pub fn with_confirmation_count(mut self, confirmation_count: u8) -> Self {
        self.confirmation_count = confirmation_count;
        self
    }

    pub fn with_transaction_id(mut self, transaction_id: Uuid) -> Self {
        self.transaction_id = transaction_id;
        self
    }

    pub fn partition_key(&self) -> Uuid {
        self.partition_key
    }

    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    pub fn transaction_id(&self) -> Uuid {
        self.transaction_id
    }

    pub fn confirmation_count(&self) -> u8 {
        self.confirmation_count
    }

    pub fn events(&self) -> &SmallVec<[NewEvent; 4]> {
        &self.events
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewEvent {
    pub event_id: Uuid,
    pub stream_id: StreamId,
    pub stream_version: ExpectedVersion,
    pub event_name: String,
    pub timestamp: u64,
    pub metadata: Vec<u8>,
    pub payload: Vec<u8>,
}

#[cfg(test)]
mod tests {

    use smallvec::smallvec;
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::*;
    use crate::bucket::PartitionHash;
    use crate::id::uuid_v7_with_partition_hash;

    // Helper functions for test setup
    async fn create_temp_db() -> (tempfile::TempDir, Database) {
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let db = DatabaseBuilder::new()
            .segment_size_bytes(1024 * 128)
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .open(temp_dir.path())
            .expect("Failed to open database");
        (temp_dir, db)
    }

    fn create_test_event(
        partition_hash: PartitionHash,
        stream_id_str: &str,
        version: ExpectedVersion,
    ) -> NewEvent {
        NewEvent {
            event_id: uuid_v7_with_partition_hash(partition_hash),
            stream_id: StreamId::new(stream_id_str).expect("Invalid stream ID"),
            stream_version: version,
            event_name: "test_event".to_string(),
            timestamp: 12345678,     // Fixed timestamp for testing
            metadata: vec![1, 2, 3], // Some test metadata
            payload: b"test payload".to_vec(),
        }
    }

    #[test]
    fn test_version_gaps() {
        // Any always satisfies
        assert_eq!(
            ExpectedVersion::Any.gap_from(CurrentVersion::Empty),
            VersionGap::None
        );
        assert_eq!(
            ExpectedVersion::Any.gap_from(CurrentVersion::Current(5)),
            VersionGap::None
        );

        // Exists requirements
        assert_eq!(
            ExpectedVersion::Exists.gap_from(CurrentVersion::Empty),
            VersionGap::Incompatible
        );
        assert_eq!(
            ExpectedVersion::Exists.gap_from(CurrentVersion::Current(0)),
            VersionGap::None
        );
        assert_eq!(
            ExpectedVersion::Exists.gap_from(CurrentVersion::Current(10)),
            VersionGap::None
        );

        // Empty requirements
        assert_eq!(
            ExpectedVersion::Empty.gap_from(CurrentVersion::Empty),
            VersionGap::None
        );
        assert_eq!(
            ExpectedVersion::Empty.gap_from(CurrentVersion::Current(0)),
            VersionGap::Ahead(1)
        );
        assert_eq!(
            ExpectedVersion::Empty.gap_from(CurrentVersion::Current(5)),
            VersionGap::Ahead(6)
        );

        // Exact version requirements
        assert_eq!(
            ExpectedVersion::Exact(5).gap_from(CurrentVersion::Current(5)),
            VersionGap::None
        );
        assert_eq!(
            ExpectedVersion::Exact(5).gap_from(CurrentVersion::Current(3)),
            VersionGap::Behind(2)
        );
        assert_eq!(
            ExpectedVersion::Exact(5).gap_from(CurrentVersion::Current(8)),
            VersionGap::Ahead(3)
        );
        assert_eq!(
            ExpectedVersion::Exact(0).gap_from(CurrentVersion::Empty),
            VersionGap::Behind(1)
        );
        assert_eq!(
            ExpectedVersion::Exact(3).gap_from(CurrentVersion::Empty),
            VersionGap::Behind(4)
        );
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

        let db = DatabaseBuilder::new()
            .segment_size_bytes(1_000_000)
            .total_buckets(32)
            .bucket_ids_from_range(0..32)
            .reader_threads(4)
            .writer_threads(4)
            .open(temp_dir.path());

        assert!(db.is_ok(), "Failed to open database with custom options");
    }

    // Event writing tests
    #[tokio::test]
    async fn test_append_single_event() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let partition_hash = uuid_to_partition_hash(partition_key);
        let stream_id_str = "test-stream";

        let event = create_test_event(partition_hash, stream_id_str, ExpectedVersion::Empty);

        let batch = Transaction::new(partition_key, partition_id, smallvec![event])
            .expect("Failed to create batch");
        let result = db.append_events(batch).await;

        assert!(result.is_ok(), "Failed to append event: {:?}", result.err());
        let versions = result.unwrap().stream_versions;

        let stream_id = StreamId::new(stream_id_str).unwrap();
        assert!(versions.contains_key(&stream_id), "Stream ID not in result");
        assert_eq!(versions[&stream_id], 0, "Expected version 0");
    }

    #[tokio::test]
    async fn test_append_multiple_events_same_stream() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let partition_hash = uuid_to_partition_hash(partition_key);
        let stream_id_str = "test-stream";

        // Append 3 events to the same stream with sequential versions
        for i in 0..3 {
            let event = create_test_event(
                partition_hash,
                stream_id_str,
                if i == 0 {
                    ExpectedVersion::Empty
                } else {
                    ExpectedVersion::Exact(i - 1)
                },
            );

            let batch = Transaction::new(partition_key, partition_id, smallvec![event]).unwrap();
            let result = db.append_events(batch).await;

            assert!(
                result.is_ok(),
                "Failed to append event {i}: {:?}",
                result.err()
            );
            let versions = result.unwrap().stream_versions;

            let stream_id = StreamId::new(stream_id_str).unwrap();
            assert_eq!(versions[&stream_id], i, "Expected version {i}");
        }
    }

    #[tokio::test]
    async fn test_append_transaction() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let partition_hash = uuid_to_partition_hash(partition_key);

        // Create events for two different streams in the same transaction
        let event1 = create_test_event(partition_hash, "stream-1", ExpectedVersion::Empty);
        let event2 = create_test_event(partition_hash, "stream-2", ExpectedVersion::Empty);

        let events = smallvec![event1, event2];
        let batch = Transaction::new(partition_key, partition_id, events).unwrap();

        let result = db.append_events(batch).await;
        assert!(
            result.is_ok(),
            "Failed to append transaction: {:?}",
            result.err()
        );

        let versions = result.unwrap().stream_versions;
        assert_eq!(versions.len(), 2, "Expected two stream versions");

        let stream1_id = StreamId::new("stream-1").unwrap();
        let stream2_id = StreamId::new("stream-2").unwrap();

        assert_eq!(versions[&stream1_id], 0, "Stream1 should be at version 0");
        assert_eq!(versions[&stream2_id], 0, "Stream2 should be at version 0");
    }

    // Event reading tests
    #[tokio::test]
    async fn test_read_event() {
        let (_temp_dir, db) = create_temp_db().await;

        // Append an event first
        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let partition_hash = uuid_to_partition_hash(partition_key);
        let stream_id_str = "test-stream";

        let event = create_test_event(partition_hash, stream_id_str, ExpectedVersion::Empty);
        let event_id = event.event_id;

        let batch = Transaction::new(partition_key, partition_id, smallvec![event]).unwrap();
        let _ = db
            .append_events(batch)
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
        let partition_hash = uuid_to_partition_hash(partition_key);

        // Create and append 3 events
        for i in 0..3 {
            let event = create_test_event(
                partition_hash,
                &format!("stream-{i}"),
                ExpectedVersion::Empty,
            );

            let batch = Transaction::new(partition_key, partition_id, smallvec![event]).unwrap();
            let _ = db
                .append_events(batch)
                .await
                .expect("Failed to append event");
        }

        // Read the partition
        let mut partition_iter = db
            .read_partition(partition_id, 0, IterDirection::Forward)
            .await
            .expect("Failed to read partition");

        // Collect all events
        let mut events = Vec::new();
        while let Some(event) = partition_iter.next().await.unwrap() {
            events.push(event);
        }

        assert_eq!(events.len(), 3, "Expected 3 events in the partition");

        // Read the partition
        let mut partition_iter = db
            .read_partition(partition_id, 1, IterDirection::Forward)
            .await
            .expect("Failed to read partition");

        // Collect all events
        let mut events = Vec::new();
        while let Some(event) = partition_iter.next().await.unwrap() {
            events.push(event);
        }

        assert_eq!(events.len(), 2, "Expected 2 events in the partition");
        assert_eq!(
            events.first().unwrap().first_partition_sequence().unwrap(),
            1,
            "Expected first event to have partition sequence of 1"
        );

        // Check that events are ordered by sequence
        for i in 0..1 {
            assert!(
                events[i].first_partition_sequence().unwrap()
                    <= events[i + 1].first_partition_sequence().unwrap(),
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
        let partition_hash = uuid_to_partition_hash(partition_key);

        // Create and append 3 events
        for i in 0..3 {
            let event = create_test_event(
                partition_hash,
                "test-stream",
                if i == 0 {
                    ExpectedVersion::Empty
                } else {
                    ExpectedVersion::Exact(i - 1)
                },
            );

            let batch = Transaction::new(partition_key, partition_id, smallvec![event]).unwrap();
            let _ = db
                .append_events(batch)
                .await
                .expect("Failed to append event");
        }

        // Get the partition sequence
        let seq_opt = db
            .get_partition_sequence(partition_id)
            .await
            .expect("Failed to get partition sequence");
        assert!(seq_opt.is_some(), "Expected a sequence to be returned");

        let PartitionLatestSequence {
            partition_id: pid,
            sequence,
        } = seq_opt.unwrap();
        assert_eq!(pid, partition_id, "Partition ID mismatch");
        assert_eq!(
            sequence, 2,
            "Expected sequence to be 2 (0-based for 3 events)"
        );
    }

    // Stream reading tests
    #[tokio::test]
    async fn test_read_stream() {
        let (_temp_dir, db) = create_temp_db().await;

        // Append multiple events to the same stream
        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let partition_hash = uuid_to_partition_hash(partition_key);
        let stream_id_str = "test-stream";

        // Create and append 3 events
        for i in 0..3 {
            let event = create_test_event(
                partition_hash,
                stream_id_str,
                if i == 0 {
                    ExpectedVersion::Empty
                } else {
                    ExpectedVersion::Exact(i - 1)
                },
            );

            let batch = Transaction::new(partition_key, partition_id, smallvec![event]).unwrap();
            let _ = db
                .append_events(batch)
                .await
                .expect("Failed to append event");
        }

        // Read the stream
        let stream_id = StreamId::new(stream_id_str).unwrap();
        let mut stream_iter = db
            .read_stream(partition_id, stream_id, 0, IterDirection::Forward)
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
            assert_eq!(
                events[i].first().unwrap().stream_version,
                i as u64,
                "Event version mismatch"
            );
            assert!(
                events[i].first().unwrap().stream_version
                    < events[i + 1].first().unwrap().stream_version,
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
        let partition_hash = uuid_to_partition_hash(partition_key);
        let stream_id_str = "test-stream";

        // Create and append 3 events
        for i in 0..3 {
            let event = create_test_event(
                partition_hash,
                stream_id_str,
                if i == 0 {
                    ExpectedVersion::Empty
                } else {
                    ExpectedVersion::Exact(i - 1)
                },
            );

            let batch = Transaction::new(partition_key, partition_id, smallvec![event]).unwrap();
            let _ = db
                .append_events(batch)
                .await
                .expect("Failed to append event");
        }

        // Get the stream version
        let stream_id = StreamId::new(stream_id_str).unwrap();
        let version_opt = db
            .get_stream_version(partition_id, &stream_id)
            .await
            .expect("Failed to get stream version");
        assert!(version_opt.is_some(), "Expected a version to be returned");

        let StreamLatestVersion {
            partition_key: pk,
            version,
        } = version_opt.unwrap();
        assert_eq!(pk, partition_key, "Partition key mismatch");
        assert_eq!(
            version, 2,
            "Expected version to be 2 (0-based for 3 events)"
        );
    }

    // Concurrency and version conflict tests
    #[tokio::test]
    async fn test_optimistic_concurrency_control() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let partition_hash = uuid_to_partition_hash(partition_key);
        let stream_id_str = "test-stream";

        // First event - create stream
        let event1 = create_test_event(partition_hash, stream_id_str, ExpectedVersion::Empty);

        let batch1 = Transaction::new(partition_key, partition_id, smallvec![event1]).unwrap();
        let _ = db
            .append_events(batch1)
            .await
            .expect("Failed to append first event");

        // Second event - tries to create stream again (should fail)
        let event2 = create_test_event(
            partition_hash,
            stream_id_str,
            ExpectedVersion::Empty, // This should fail as stream now exists
        );

        let batch2 = Transaction::new(partition_key, partition_id, smallvec![event2]).unwrap();
        let result = db.append_events(batch2).await;

        assert!(result.is_err(), "Expected version conflict error");
        // Specific error type would depend on your error definitions
    }

    #[tokio::test]
    async fn test_exact_version_concurrency() {
        let (_temp_dir, db) = create_temp_db().await;

        let partition_id = 1;
        let partition_key = Uuid::new_v4();
        let partition_hash = uuid_to_partition_hash(partition_key);
        let stream_id_str = "test-stream";

        // Create stream
        let event1 = create_test_event(partition_hash, stream_id_str, ExpectedVersion::Empty);

        let batch1 = Transaction::new(partition_key, partition_id, smallvec![event1]).unwrap();
        let _ = db
            .append_events(batch1)
            .await
            .expect("Failed to append first event");

        // Try to append with wrong version
        let event2 = create_test_event(
            partition_hash,
            stream_id_str,
            ExpectedVersion::Exact(1), // Wrong, should be 0
        );

        let batch2 = Transaction::new(partition_key, partition_id, smallvec![event2]).unwrap();
        let result = db.append_events(batch2).await;

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
            .read_stream(partition_id, nonexistent_stream, 0, IterDirection::Forward)
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
            .read_partition(unused_partition_id, 0, IterDirection::Forward)
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
        let partition_hash = uuid_to_partition_hash(partition_key);
        let stream_id_str = "test-stream";

        // First create the stream
        let event1 = create_test_event(partition_hash, stream_id_str, ExpectedVersion::Empty);

        let batch1 = Transaction::new(partition_key, partition_id, smallvec![event1]).unwrap();
        let _ = db
            .append_events(batch1)
            .await
            .expect("Failed to append first event");

        // Append with StreamExists expectation (should succeed)
        let event2 = create_test_event(partition_hash, stream_id_str, ExpectedVersion::Exists);

        let batch2 = Transaction::new(partition_key, partition_id, smallvec![event2]).unwrap();
        let result = db.append_events(batch2).await;

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
        let partition_hash = uuid_to_partition_hash(partition_key);
        let stream_id_str = "test-stream";

        // For a new stream, Any should work
        let event1 = create_test_event(partition_hash, stream_id_str, ExpectedVersion::Any);

        let batch1 = Transaction::new(partition_key, partition_id, smallvec![event1]).unwrap();
        let result1 = db.append_events(batch1).await;
        assert!(
            result1.is_ok(),
            "Any expectation should work for new stream"
        );

        // For an existing stream, Any should also work
        let event2 = create_test_event(partition_hash, stream_id_str, ExpectedVersion::Any);

        let batch2 = Transaction::new(partition_key, partition_id, smallvec![event2]).unwrap();
        let result2 = db.append_events(batch2).await;
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
        let db = DatabaseBuilder::new()
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .open(temp_dir.path())
            .expect("Failed to open database");

        // Events for different partitions should go to different buckets
        let partition_ids = [0, 1, 2, 3]; // Should map to different buckets

        for &partition_id in &partition_ids {
            let partition_key = Uuid::new_v4();
            let partition_hash = uuid_to_partition_hash(partition_key);
            let event = create_test_event(
                partition_hash,
                &format!("stream-{partition_id}"),
                ExpectedVersion::Empty,
            );

            let batch = Transaction::new(partition_key, partition_id, smallvec![event]).unwrap();
            let result = db.append_events(batch).await;
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
                "Expected sequence for partition {partition_id}",
            );

            assert_eq!(
                seq_opt.unwrap().sequence,
                0,
                "Expected sequence 0 for partition {partition_id}",
            );
        }
    }

    // Test ExpectedVersion and CurrentVersion implementations
    #[test]
    fn test_expected_version_from_next() {
        assert_eq!(
            ExpectedVersion::from_next_version(0),
            ExpectedVersion::Empty
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
        assert_eq!(ExpectedVersion::Empty.into_next_version(), Some(0));
        assert_eq!(ExpectedVersion::Exact(0).into_next_version(), Some(1));
        assert_eq!(ExpectedVersion::Exact(41).into_next_version(), Some(42));
    }

    #[test]
    fn test_current_version_next() {
        assert_eq!(CurrentVersion::Empty.next(), 0);
        assert_eq!(CurrentVersion::Current(0).next(), 1);
        assert_eq!(CurrentVersion::Current(41).next(), 42);
    }

    #[test]
    fn test_current_version_as_expected() {
        assert_eq!(
            CurrentVersion::Empty.as_expected_version(),
            ExpectedVersion::Empty
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
        let mut version = CurrentVersion::Empty;
        version += 1;
        assert_eq!(version, CurrentVersion::Current(0));

        version += 2;
        assert_eq!(version, CurrentVersion::Current(2));
    }

    #[test]
    fn test_metadata_created_on_new_database() {
        let temp_dir = tempdir().expect("failed to create temp directory");

        let _db = DatabaseBuilder::new()
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .segment_size_bytes(256 * 1024 * 1024)
            .open(temp_dir.path())
            .expect("failed to open database");

        let meta_path = temp_dir.path().join("meta.json");
        assert!(meta_path.exists(), "meta.json should be created");

        let meta = DatabaseMeta::load(&meta_path).expect("failed to load meta.json");
        assert_eq!(meta.version, 1);
        assert_eq!(meta.total_buckets, 4);
        assert_eq!(meta.segment_size_bytes, 256 * 1024 * 1024);
    }

    #[test]
    fn test_metadata_validated_on_reopen() {
        let temp_dir = tempdir().expect("failed to create temp directory");

        // Create database with specific settings
        let _db = DatabaseBuilder::new()
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .open(temp_dir.path())
            .expect("failed to open database");

        drop(_db);

        // Reopen with same settings - should work
        let _db = DatabaseBuilder::new()
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .open(temp_dir.path())
            .expect("failed to reopen database with same settings");

        drop(_db);

        // Reopen with different total_buckets - should fail
        let result = DatabaseBuilder::new()
            .total_buckets(8)
            .bucket_ids_from_range(0..8)
            .open(temp_dir.path());

        assert!(
            matches!(
                result,
                Err(DatabaseError::Metadata(
                    MetadataError::TotalBucketsMismatch { .. }
                ))
            ),
            "expected TotalBucketsMismatch error"
        );
    }

    #[test]
    fn test_metadata_segment_size_mismatch() {
        let temp_dir = tempdir().expect("failed to create temp directory");

        // Create database with specific segment size
        let _db = DatabaseBuilder::new()
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .segment_size_bytes(256 * 1024 * 1024)
            .open(temp_dir.path())
            .expect("failed to open database");

        drop(_db);

        // Reopen with different segment_size_bytes - should fail
        let result = DatabaseBuilder::new()
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .segment_size_bytes(512 * 1024 * 1024)
            .open(temp_dir.path());

        assert!(
            matches!(
                result,
                Err(DatabaseError::Metadata(
                    MetadataError::SegmentSizeMismatch { .. }
                ))
            ),
            "expected SegmentSizeMismatch error"
        );
    }
}
