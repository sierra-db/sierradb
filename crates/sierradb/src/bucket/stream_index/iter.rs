use std::collections::{HashMap, VecDeque};
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::oneshot;
use tracing::warn;

use crate::StreamId;
use crate::bucket::segment::{CommittedEvents, SegmentIter};
use crate::bucket::stream_index::StreamOffsets;
use crate::bucket::{BucketId, BucketSegmentId, SegmentId};
use crate::error::StreamIndexError;
use crate::reader_thread_pool::{ReaderSet, ReaderThreadPool};
use crate::writer_thread_pool::LiveIndexes;

#[derive(Debug)]
pub struct StreamIter {
    stream_id: StreamId,
    segment_iter: Option<SegmentIter>,
    live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
    last_version: u64,
    is_live: bool,
    has_next_segment: bool,
    reverse: bool,
    batch: VecDeque<CommittedEvents>,
}

impl StreamIter {
    pub(crate) async fn new(
        stream_id: StreamId,
        bucket_id: BucketId,
        reader_pool: ReaderThreadPool,
        live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
        from_version: u64,
        reverse: bool,
    ) -> Result<Self, StreamIndexError> {
        Self::new_inner(
            stream_id,
            bucket_id,
            reader_pool,
            live_indexes,
            from_version,
            reverse,
            if reverse { u32::MAX } else { 0 },
            true,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn new_inner(
        stream_id: StreamId,
        bucket_id: BucketId,
        reader_pool: ReaderThreadPool,
        live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
        from_version: u64,
        reverse: bool,
        next_segment_id: SegmentId,
        check_closed_segments: bool,
    ) -> Result<Self, StreamIndexError> {
        // Check live indexes first
        if let Some((segment_id, index)) = live_indexes.get(&bucket_id) {
            let segment_id = segment_id.load(Ordering::Acquire);
            let matches = if reverse {
                segment_id <= next_segment_id
            } else {
                segment_id >= next_segment_id
            };
            if matches {
                let index_guard = index.read().await;
                if let Some(stream_index) = index_guard.stream_index.get(&stream_id)
                    && let StreamOffsets::Offsets(offsets) = stream_index.offsets.clone()
                    && (if reverse {
                        // For reverse iteration, check if segment contains versions <= from_version
                        stream_index.version_min <= from_version
                    } else {
                        // For forward iteration, use original logic
                        stream_index.version_min <= from_version
                    })
                {
                    let offsets_index = if reverse && from_version == u64::MAX {
                        // For reverse iteration from max, start from the end
                        offsets.len()
                    } else {
                        (from_version.saturating_sub(stream_index.version_min) as usize)
                            .min(offsets.len())
                    };

                    drop(index_guard);

                    let segment_iter = SegmentIter::new(
                        reader_pool,
                        BucketSegmentId::new(bucket_id, segment_id),
                        offsets,
                        offsets_index,
                        reverse,
                    );

                    // For reverse iteration, check if there are segments with lower IDs
                    let has_next_segment = if reverse {
                        segment_id > 0 // If segment_id > 0, there are lower numbered segments
                    } else {
                        false
                    };

                    return Ok(StreamIter {
                        stream_id,
                        segment_iter: Some(segment_iter),
                        live_indexes,
                        last_version: from_version,
                        is_live: true,
                        has_next_segment,
                        reverse,
                        batch: VecDeque::new(),
                    });
                }
            }
        }

        if !check_closed_segments {
            return Ok(StreamIter {
                stream_id,
                segment_iter: None,
                live_indexes,
                last_version: from_version,
                is_live: false,
                has_next_segment: false,
                reverse,
                batch: VecDeque::new(),
            });
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        reader_pool.spawn({
            let stream_id = stream_id.clone();
            move |with_readers| {
                with_readers(move |readers| {
                    let Some(segments) = readers.get_mut(&bucket_id) else {
                        let _ = reply_tx.send(None);
                        return;
                    };

                    let segments_len = segments.len();
                    let res = if reverse {
                        // For reverse iteration, start from the last (newest) segment and work
                        // backwards
                        segments.iter_mut().enumerate().rev().find_map(
                            |(i, (segment_id, reader_set)): (usize, (&u32, &mut ReaderSet))| {
                                if *segment_id > next_segment_id {
                                    return None;
                                }

                                let stream_index = reader_set.stream_index.as_mut()?;

                                match stream_index.get_key(&stream_id) {
                                    Ok(Some(key))
                                        if key.version_min <= from_version
                                            || i == segments_len - 1 =>
                                    {
                                        let version_min = key.version_min;
                                        match stream_index.get_from_key(key) {
                                            Ok(StreamOffsets::Offsets(offsets)) => {
                                                let offsets_index =
                                                    if reverse && from_version == u64::MAX {
                                                        // For reverse iteration from max, start
                                                        // from the end
                                                        offsets.len()
                                                    } else {
                                                        (from_version.saturating_sub(version_min)
                                                            as usize)
                                                            .min(offsets.len())
                                                    };

                                                debug_assert!(segments_len > 0);
                                                let has_next_segment = i > 0;
                                                Some(Ok((
                                                    *segment_id,
                                                    offsets,
                                                    offsets_index,
                                                    has_next_segment,
                                                )))
                                            }
                                            Ok(StreamOffsets::ExternalBucket) => unimplemented!(),
                                            Err(err) => Some(Err(err)),
                                        }
                                    }
                                    Ok(_) => None,
                                    Err(err) => Some(Err(err)),
                                }
                            },
                        )
                    } else {
                        // For forward iteration, start from the last (newest) segment
                        segments.iter_mut().enumerate().rev().find_map(
                            |(i, (segment_id, reader_set)): (usize, (&u32, &mut ReaderSet))| {
                                if *segment_id < next_segment_id {
                                    return None;
                                }

                                let stream_index = reader_set.stream_index.as_mut()?;

                                match stream_index.get_key(&stream_id) {
                                    Ok(Some(key)) if key.version_min <= from_version || i == 0 => {
                                        let version_min = key.version_min;
                                        match stream_index.get_from_key(key) {
                                            Ok(StreamOffsets::Offsets(offsets)) => {
                                                let offsets_index = (from_version
                                                    .saturating_sub(version_min)
                                                    as usize)
                                                    .min(offsets.len());

                                                debug_assert!(segments_len > 0);
                                                let has_next_segment = segments_len - 1 > i;
                                                Some(Ok((
                                                    *segment_id,
                                                    offsets,
                                                    offsets_index,
                                                    has_next_segment,
                                                )))
                                            }
                                            Ok(StreamOffsets::ExternalBucket) => unimplemented!(),
                                            Err(err) => Some(Err(err)),
                                        }
                                    }
                                    Ok(_) => None,
                                    Err(err) => Some(Err(err)),
                                }
                            },
                        )
                    };

                    let _ = reply_tx.send(res);
                })
            }
        });

        match reply_rx.await?.transpose()? {
            Some((segment_id, offsets, offsets_index, has_next_segment)) => {
                let segment_iter = Some(SegmentIter::new(
                    reader_pool,
                    BucketSegmentId::new(bucket_id, segment_id),
                    offsets,
                    offsets_index,
                    reverse,
                ));
                Ok(StreamIter {
                    stream_id,
                    segment_iter,
                    live_indexes,
                    last_version: from_version,
                    is_live: false,
                    has_next_segment,
                    reverse,
                    batch: VecDeque::new(),
                })
            }
            None => {
                if let Some((segment_id, index)) = live_indexes.get(&bucket_id) {
                    let segment_id = segment_id.load(Ordering::Acquire);
                    let index_guard = index.read().await;
                    if let Some(stream_index) = index_guard.stream_index.get(&stream_id)
                        && let StreamOffsets::Offsets(offsets) = stream_index.offsets.clone()
                        && stream_index.version_min <= from_version
                    {
                        let offsets_index = (from_version.saturating_sub(stream_index.version_min)
                            as usize)
                            .min(offsets.len());
                        drop(index_guard);

                        let segment_iter = SegmentIter::new(
                            reader_pool,
                            BucketSegmentId::new(bucket_id, segment_id),
                            offsets,
                            offsets_index,
                            reverse,
                        );
                        return Ok(StreamIter {
                            stream_id,
                            segment_iter: Some(segment_iter),
                            live_indexes,
                            last_version: from_version,
                            is_live: true,
                            has_next_segment: false,
                            reverse,
                            batch: VecDeque::new(),
                        });
                    }
                }

                Ok(StreamIter {
                    stream_id,
                    segment_iter: None,
                    live_indexes,
                    last_version: from_version,
                    is_live: false,
                    has_next_segment: false,
                    reverse,
                    batch: VecDeque::new(),
                })
            }
        }
    }

    pub fn next_batch(
        &mut self,
        limit: usize,
    ) -> BoxFuture<'_, Result<Option<Vec<CommittedEvents>>, StreamIndexError>> {
        async move {
            if limit == 0 {
                return Ok(None);
            }

            if let Some(batch_back) = self.batch.back() {
                self.last_version = batch_back
                    .last_stream_version()
                    .map(|v| {
                        if self.reverse {
                            v.saturating_sub(1)
                        } else {
                            v + 1
                        }
                    })
                    .unwrap_or(self.last_version);
                return Ok(Some(mem::take(&mut self.batch).into()));
            }

            let Some(segment_iter) = &mut self.segment_iter else {
                return Ok(None);
            };

            match segment_iter.next(limit).await? {
                Some(commits) => {
                    self.last_version = commits
                        .last()
                        .expect("commits should not be empty if Some is returned")
                        .last_stream_version()
                        .map(|v| {
                            if self.reverse {
                                v.saturating_sub(1)
                            } else {
                                v + 1
                            }
                        })
                        .unwrap_or(self.last_version);

                    let commits: Vec<_> = commits
                        .into_iter()
                        .filter_map(|commit| filter_commit_for_stream(&self.stream_id, commit))
                        .collect();
                    if commits.is_empty() {
                        // This shouldn't happen
                        warn!("transaction had no events for stream id {}", self.stream_id);
                        return self.next_batch(limit).await;
                    }

                    Ok(Some(commits))
                }
                None => {
                    if segment_iter.is_finished() && (!self.is_live || self.has_next_segment) {
                        if !self.rollover().await? {
                            return Ok(None);
                        }

                        return self.next_batch(limit).await;
                    }

                    Ok(None)
                }
            }
        }
        .boxed()
    }

    /// Convenience method that maintains backward compatibility.
    /// Reads a single event by calling next_batch(1).
    pub async fn next(&mut self) -> Result<Option<CommittedEvents>, StreamIndexError> {
        if self.batch.is_empty() {
            let Some(batch) = self.next_batch(50).await? else {
                return Ok(None);
            };
            self.batch = batch.into_iter().collect();
        }

        Ok(self.batch.pop_front())
    }

    async fn rollover(&mut self) -> Result<bool, StreamIndexError> {
        let segment_iter = self.segment_iter.take().unwrap();
        let current_segment_id = segment_iter.bucket_segment_id.segment_id;

        // For reverse iteration, stop if we've reached segment 0
        if self.reverse && current_segment_id == 0 {
            return Ok(false);
        }

        let next_segment_id = if self.reverse {
            // For reverse iteration, go to previous (lower numbered) segment
            current_segment_id.saturating_sub(1)
        } else {
            // For forward iteration, go to next (higher numbered) segment
            current_segment_id + 1
        };

        let from_version = self.last_version;

        let min_segment_id = next_segment_id;

        *self = Self::new_inner(
            self.stream_id.clone(),
            segment_iter.bucket_segment_id.bucket_id,
            segment_iter.reader_pool,
            self.live_indexes.clone(),
            from_version,
            self.reverse,
            min_segment_id,
            self.has_next_segment,
        )
        .await?;

        Ok(true)
    }
}

fn filter_commit_for_stream(
    stream_id: &StreamId,
    commit: CommittedEvents,
) -> Option<CommittedEvents> {
    match commit {
        CommittedEvents::Transaction { mut events, commit } => {
            events.retain(|event| &event.stream_id == stream_id);
            if events.is_empty() {
                None
            } else {
                Some(CommittedEvents::Transaction { events, commit })
            }
        }
        CommittedEvents::Single(event) => {
            if &event.stream_id == stream_id {
                Some(CommittedEvents::Single(event))
            } else {
                None
            }
        }
    }
}
