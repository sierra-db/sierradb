use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::{fmt, mem};

use tokio::sync::oneshot;
use tracing::warn;

use crate::bucket::partition_index::PartitionOffsets;
use crate::bucket::segment::{CommittedEvents, SegmentIter};
use crate::bucket::stream_index::StreamOffsets;
use crate::bucket::{BucketId, BucketSegmentId, PartitionId, SegmentId};
use crate::error::{PartitionIndexError, ReadError, StreamIndexError};
use crate::reader_thread_pool::{ReaderSet, ReaderThreadPool};
use crate::writer_thread_pool::LiveIndexes;
use crate::{IterDirection, StreamId};

/// Configuration trait that defines the behavior differences between
/// partition and stream iteration.
pub trait IterConfig: Clone + Send + 'static {
    type Id: fmt::Display + Clone + Send;
    type Error: From<oneshot::error::RecvError> + From<ReadError> + Send;

    /// Get the ID being iterated over
    fn id(&self) -> &Self::Id;

    /// Try to get offsets from live indexes
    /// Returns: (file_offsets, offsets_index)
    fn try_get_from_live_indexes(
        &self,
        live_indexes: &LiveIndexes,
        from_position: u64,
        dir: IterDirection,
    ) -> impl Future<Output = Option<(Vec<u64>, usize)>>;

    /// Try to get offsets from a reader set
    /// Returns: (file_offsets, offsets_index)
    fn try_get_from_reader_set(
        &self,
        reader_set: &mut ReaderSet,
        from_position: u64,
        dir: IterDirection,
        segment_index: usize,
        segments_len: usize,
    ) -> Result<Option<(Vec<u64>, usize)>, Self::Error>;

    /// Filter a commit (no-op for partitions, filters by stream for streams)
    fn filter_commit(&self, commit: CommittedEvents) -> Option<CommittedEvents>;

    /// Extract the last position from a commit (sequence for partition, version for stream)
    fn extract_last_position(&self, commit: &CommittedEvents) -> Option<u64>;
}

/// Configuration for partition iteration
#[derive(Clone)]
pub struct PartitionIterConfig {
    partition_id: PartitionId,
}

impl PartitionIterConfig {
    pub fn new(partition_id: PartitionId) -> Self {
        Self { partition_id }
    }
}

impl IterConfig for PartitionIterConfig {
    type Id = PartitionId;
    type Error = PartitionIndexError;

    fn id(&self) -> &Self::Id {
        &self.partition_id
    }

    async fn try_get_from_live_indexes(
        &self,
        live_indexes: &LiveIndexes,
        from_position: u64,
        dir: IterDirection,
    ) -> Option<(Vec<u64>, usize)> {
        let (sequence_min, offsets) = {
            let live_indexes_guard = live_indexes.read().await;
            let partition_index = live_indexes_guard.partition_index.get(self.partition_id)?;

            if partition_index.sequence_min > from_position {
                return None;
            }

            let PartitionOffsets::Offsets(offsets) = partition_index.offsets.clone() else {
                return None;
            };

            (partition_index.sequence_min, offsets)
        };

        let offsets_index = if matches!(dir, IterDirection::Reverse) && from_position == u64::MAX {
            offsets.len()
        } else {
            (from_position.saturating_sub(sequence_min) as usize).min(offsets.len())
        };

        let file_offsets = offsets.into_iter().map(|o| o.offset).collect();

        Some((file_offsets, offsets_index))
    }

    fn try_get_from_reader_set(
        &self,
        reader_set: &mut ReaderSet,
        from_position: u64,
        dir: IterDirection,
        segment_index: usize,
        segments_len: usize,
    ) -> Result<Option<(Vec<u64>, usize)>, Self::Error> {
        let partition_index = match reader_set.partition_index.as_mut() {
            Some(index) => index,
            None => return Ok(None),
        };

        let key = match partition_index.get_key(self.partition_id)? {
            Some(key) if key.sequence_min <= from_position || segment_index == 0 => key,
            Some(key)
                if matches!(dir, IterDirection::Reverse) && segment_index == segments_len - 1 =>
            {
                key
            }
            _ => return Ok(None),
        };

        let sequence_min = key.sequence_min;
        let offsets = match partition_index.get_from_key(key)? {
            PartitionOffsets::Offsets(offsets) => offsets,
            PartitionOffsets::ExternalBucket => unimplemented!(),
        };

        let offsets_index = if matches!(dir, IterDirection::Reverse) && from_position == u64::MAX {
            offsets.len()
        } else {
            (from_position.saturating_sub(sequence_min) as usize).min(offsets.len())
        };

        let file_offsets = offsets.into_iter().map(|o| o.offset).collect();

        Ok(Some((file_offsets, offsets_index)))
    }

    fn filter_commit(&self, commit: CommittedEvents) -> Option<CommittedEvents> {
        Some(commit)
    }

    fn extract_last_position(&self, commit: &CommittedEvents) -> Option<u64> {
        commit.last_partition_sequence()
    }
}

/// Configuration for stream iteration
#[derive(Clone)]
pub struct StreamIterConfig {
    stream_id: StreamId,
}

impl StreamIterConfig {
    pub fn new(stream_id: StreamId) -> Self {
        Self { stream_id }
    }
}

impl IterConfig for StreamIterConfig {
    type Id = StreamId;
    type Error = StreamIndexError;

    fn id(&self) -> &Self::Id {
        &self.stream_id
    }

    async fn try_get_from_live_indexes(
        &self,
        live_indexes: &LiveIndexes,
        from_position: u64,
        dir: IterDirection,
    ) -> Option<(Vec<u64>, usize)> {
        let (version_min, offsets) = {
            let live_indexes_guard = live_indexes.read().await;
            let stream_index = live_indexes_guard.stream_index.get(&self.stream_id)?;

            let StreamOffsets::Offsets(offsets) = stream_index.offsets.clone() else {
                return None;
            };

            if stream_index.version_min > from_position {
                return None;
            }

            (stream_index.version_min, offsets)
        };

        let offsets_index = if matches!(dir, IterDirection::Reverse) && from_position == u64::MAX {
            offsets.len()
        } else {
            (from_position.saturating_sub(version_min) as usize).min(offsets.len())
        };

        Some((offsets, offsets_index))
    }

    fn try_get_from_reader_set(
        &self,
        reader_set: &mut ReaderSet,
        from_position: u64,
        dir: IterDirection,
        segment_index: usize,
        segments_len: usize,
    ) -> Result<Option<(Vec<u64>, usize)>, Self::Error> {
        let stream_index = match reader_set.stream_index.as_mut() {
            Some(index) => index,
            None => return Ok(None),
        };

        let key = match stream_index.get_key(&self.stream_id)? {
            Some(key) if key.version_min <= from_position || segment_index == 0 => key,
            Some(key)
                if matches!(dir, IterDirection::Reverse) && segment_index == segments_len - 1 =>
            {
                key
            }
            _ => return Ok(None),
        };

        let version_min = key.version_min;
        let offsets = match stream_index.get_from_key(key)? {
            StreamOffsets::Offsets(offsets) => offsets,
            StreamOffsets::ExternalBucket => unimplemented!(),
        };

        let offsets_index = if matches!(dir, IterDirection::Reverse) && from_position == u64::MAX {
            offsets.len()
        } else {
            (from_position.saturating_sub(version_min) as usize).min(offsets.len())
        };

        Ok(Some((offsets, offsets_index)))
    }

    fn filter_commit(&self, commit: CommittedEvents) -> Option<CommittedEvents> {
        match commit {
            CommittedEvents::Transaction { mut events, commit } => {
                events.retain(|event| event.stream_id == self.stream_id);
                if events.is_empty() {
                    None
                } else {
                    Some(CommittedEvents::Transaction { events, commit })
                }
            }
            CommittedEvents::Single(event) => {
                if event.stream_id == self.stream_id {
                    Some(CommittedEvents::Single(event))
                } else {
                    None
                }
            }
        }
    }

    fn extract_last_position(&self, commit: &CommittedEvents) -> Option<u64> {
        commit.last_stream_version()
    }
}

/// Generic iterator over buckets that can be configured for either
/// partition or stream iteration.
#[derive(Debug)]
pub struct BucketIter<C: IterConfig> {
    config: C,
    segment_iter: Option<SegmentIter>,
    live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
    last_position: u64,
    is_live: bool,
    has_next_segment: bool,
    dir: IterDirection,
    batch: VecDeque<CommittedEvents>,
}

impl<C: IterConfig> BucketIter<C> {
    pub(crate) async fn new(
        config: C,
        bucket_id: BucketId,
        reader_pool: ReaderThreadPool,
        live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
        from_position: u64,
        dir: IterDirection,
    ) -> Result<Self, C::Error> {
        Self::new_inner(
            config,
            bucket_id,
            reader_pool,
            live_indexes,
            from_position,
            dir,
            match dir {
                IterDirection::Forward => 0,
                IterDirection::Reverse => u32::MAX,
            },
            true,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn new_inner(
        config: C,
        bucket_id: BucketId,
        reader_pool: ReaderThreadPool,
        live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
        from_position: u64,
        dir: IterDirection,
        next_segment_id: SegmentId,
        check_closed_segments: bool,
    ) -> Result<Self, C::Error> {
        // Check live indexes first
        if let Some((segment_id, index)) = live_indexes.get(&bucket_id) {
            let segment_id = segment_id.load(Ordering::Acquire);
            let matches = match dir {
                IterDirection::Forward => segment_id >= next_segment_id,
                IterDirection::Reverse => segment_id <= next_segment_id,
            };

            if matches
                && let Some((file_offsets, offsets_index)) = config
                    .try_get_from_live_indexes(index, from_position, dir)
                    .await
            {
                let segment_iter = SegmentIter::new(
                    reader_pool,
                    BucketSegmentId::new(bucket_id, segment_id),
                    file_offsets,
                    offsets_index,
                    dir,
                );

                let has_next_segment = match dir {
                    IterDirection::Forward => false,
                    IterDirection::Reverse => segment_id > 0,
                };

                return Ok(BucketIter {
                    config,
                    segment_iter: Some(segment_iter),
                    live_indexes,
                    last_position: from_position,
                    is_live: true,
                    has_next_segment,
                    dir,
                    batch: VecDeque::new(),
                });
            }
        }

        if !check_closed_segments {
            return Ok(BucketIter {
                config,
                segment_iter: None,
                live_indexes,
                last_position: from_position,
                is_live: false,
                has_next_segment: false,
                dir,
                batch: VecDeque::new(),
            });
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        let config_clone = config.clone();
        reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| {
                let Some(segments) = readers.get_mut(&bucket_id) else {
                    let _ = reply_tx.send(None);
                    return;
                };

                let segments_len = segments.len();
                let res = match dir {
                    IterDirection::Forward => segments.iter_mut().enumerate().rev().find_map(
                        |(i, (segment_id, reader_set)): (usize, (&u32, &mut ReaderSet))| {
                            if *segment_id < next_segment_id {
                                return None;
                            }

                            match config_clone.try_get_from_reader_set(
                                reader_set,
                                from_position,
                                dir,
                                i,
                                segments_len,
                            ) {
                                Ok(Some((file_offsets, offsets_index))) => {
                                    debug_assert!(segments_len > 0);
                                    let has_next_segment = segments_len - 1 > i;
                                    Some(Ok((
                                        *segment_id,
                                        file_offsets,
                                        offsets_index,
                                        has_next_segment,
                                    )))
                                }
                                Ok(None) => None,
                                Err(err) => Some(Err(err)),
                            }
                        },
                    ),
                    IterDirection::Reverse => segments.iter_mut().enumerate().rev().find_map(
                        |(i, (segment_id, reader_set)): (usize, (&u32, &mut ReaderSet))| {
                            if *segment_id > next_segment_id {
                                return None;
                            }

                            match config_clone.try_get_from_reader_set(
                                reader_set,
                                from_position,
                                dir,
                                i,
                                segments_len,
                            ) {
                                Ok(Some((file_offsets, offsets_index))) => {
                                    debug_assert!(segments_len > 0);
                                    let has_next_segment = i > 0;
                                    Some(Ok((
                                        *segment_id,
                                        file_offsets,
                                        offsets_index,
                                        has_next_segment,
                                    )))
                                }
                                Ok(None) => None,
                                Err(err) => Some(Err(err)),
                            }
                        },
                    ),
                };

                let _ = reply_tx.send(res);
            })
        });

        match reply_rx.await?.transpose()? {
            Some((segment_id, file_offsets, offsets_index, has_next_segment)) => {
                let segment_iter = Some(SegmentIter::new(
                    reader_pool,
                    BucketSegmentId::new(bucket_id, segment_id),
                    file_offsets,
                    offsets_index,
                    dir,
                ));
                Ok(BucketIter {
                    config,
                    segment_iter,
                    live_indexes,
                    last_position: from_position,
                    is_live: false,
                    has_next_segment,
                    dir,
                    batch: VecDeque::new(),
                })
            }
            None => {
                if let Some((segment_id, index)) = live_indexes.get(&bucket_id) {
                    let segment_id = segment_id.load(Ordering::Acquire);
                    if let Some((file_offsets, offsets_index)) = config
                        .try_get_from_live_indexes(index, from_position, dir)
                        .await
                    {
                        let segment_iter = SegmentIter::new(
                            reader_pool,
                            BucketSegmentId::new(bucket_id, segment_id),
                            file_offsets,
                            offsets_index,
                            dir,
                        );
                        return Ok(BucketIter {
                            config,
                            segment_iter: Some(segment_iter),
                            live_indexes,
                            last_position: from_position,
                            is_live: true,
                            has_next_segment: false,
                            dir,
                            batch: VecDeque::new(),
                        });
                    }
                }

                Ok(BucketIter {
                    config,
                    segment_iter: None,
                    live_indexes,
                    last_position: from_position,
                    is_live: false,
                    has_next_segment: false,
                    dir,
                    batch: VecDeque::new(),
                })
            }
        }
    }

    pub async fn next_batch(
        &mut self,
        limit: usize,
    ) -> Result<Option<Vec<CommittedEvents>>, C::Error> {
        if limit == 0 {
            return Ok(None);
        }

        if let Some(batch_back) = self.batch.back() {
            self.last_position = self
                .config
                .extract_last_position(batch_back)
                .map(|v| match self.dir {
                    IterDirection::Forward => v + 1,
                    IterDirection::Reverse => v.saturating_sub(1),
                })
                .unwrap_or(self.last_position);
            return Ok(Some(mem::take(&mut self.batch).into()));
        }

        loop {
            let Some(segment_iter) = &mut self.segment_iter else {
                return Ok(None);
            };

            match segment_iter.next(limit).await? {
                Some(commits) => {
                    self.last_position = self
                        .config
                        .extract_last_position(
                            commits
                                .last()
                                .expect("commits should not be empty if Some is returned"),
                        )
                        .map(|v| match self.dir {
                            IterDirection::Forward => v + 1,
                            IterDirection::Reverse => v.saturating_sub(1),
                        })
                        .unwrap_or(self.last_position);

                    // Apply filtering
                    let commits: Vec<_> = commits
                        .into_iter()
                        .filter_map(|commit| self.config.filter_commit(commit))
                        .collect();

                    if commits.is_empty() {
                        warn!("transaction had no events for {}", self.config.id());
                        continue;
                    }

                    return Ok(Some(commits));
                }
                None => {
                    if segment_iter.is_finished() && (!self.is_live || self.has_next_segment) {
                        if !self.rollover().await? {
                            return Ok(None);
                        }
                        continue;
                    }

                    return Ok(None);
                }
            }
        }
    }

    /// Convenience method that maintains backward compatibility.
    /// Reads a single event by calling next_batch(50).
    pub async fn next(&mut self) -> Result<Option<CommittedEvents>, C::Error> {
        if self.batch.is_empty() {
            let Some(batch) = self.next_batch(50).await? else {
                return Ok(None);
            };
            self.batch = batch.into_iter().collect();
        }

        Ok(self.batch.pop_front())
    }

    async fn rollover(&mut self) -> Result<bool, C::Error> {
        let segment_iter = self.segment_iter.take().unwrap();
        let current_segment_id = segment_iter.bucket_segment_id.segment_id;

        // For reverse iteration, stop if we've reached segment 0
        if matches!(self.dir, IterDirection::Reverse) && current_segment_id == 0 {
            return Ok(false);
        }

        let next_segment_id = match self.dir {
            IterDirection::Forward => current_segment_id + 1,
            IterDirection::Reverse => current_segment_id.saturating_sub(1),
        };

        let from_position = self.last_position;

        *self = Self::new_inner(
            self.config.clone(),
            segment_iter.bucket_segment_id.bucket_id,
            segment_iter.reader_pool,
            self.live_indexes.clone(),
            from_position,
            self.dir,
            next_segment_id,
            self.has_next_segment,
        )
        .await?;

        Ok(true)
    }
}

// Type aliases for clean public API
pub type PartitionIter = BucketIter<PartitionIterConfig>;
pub type StreamIter = BucketIter<StreamIterConfig>;

// Convenience constructors
impl PartitionIter {
    pub async fn new_partition(
        partition_id: PartitionId,
        bucket_id: BucketId,
        reader_pool: ReaderThreadPool,
        live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
        from_sequence: u64,
        dir: IterDirection,
    ) -> Result<Self, PartitionIndexError> {
        BucketIter::new(
            PartitionIterConfig::new(partition_id),
            bucket_id,
            reader_pool,
            live_indexes,
            from_sequence,
            dir,
        )
        .await
    }
}

impl StreamIter {
    pub async fn new_stream(
        stream_id: StreamId,
        bucket_id: BucketId,
        reader_pool: ReaderThreadPool,
        live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
        from_version: u64,
        dir: IterDirection,
    ) -> Result<Self, StreamIndexError> {
        BucketIter::new(
            StreamIterConfig::new(stream_id),
            bucket_id,
            reader_pool,
            live_indexes,
            from_version,
            dir,
        )
        .await
    }
}
