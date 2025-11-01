use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::{io, mem};

use futures::FutureExt;
use futures::future::BoxFuture;
use tokio::sync::oneshot;
use tracing::warn;

use crate::StreamId;
use crate::bucket::segment::{CommittedEvents, ReadHint, SegmentBlock};
use crate::bucket::stream_index::StreamOffsets;
use crate::bucket::{BucketId, BucketSegmentId, SegmentId};
use crate::cache::SegmentBlockCache;
use crate::error::{ReadError, StreamIndexError};
use crate::reader_thread_pool::{ReaderSet, ReaderThreadPool};
use crate::writer_thread_pool::LiveIndexes;

#[derive(Debug)]
pub struct StreamIter {
    segment_iter: Option<SegmentStreamIter>,
    live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
    last_version: u64,
    is_live: bool,
    has_next_segment: bool,
}

impl StreamIter {
    pub(crate) async fn new(
        stream_id: StreamId,
        bucket_id: BucketId,
        from_version: u64,
        reader_pool: ReaderThreadPool,
        live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
    ) -> Result<Self, StreamIndexError> {
        Self::new_inner(
            stream_id,
            bucket_id,
            from_version,
            reader_pool,
            live_indexes,
            0,
            true,
        )
        .await
    }

    async fn new_inner(
        stream_id: StreamId,
        bucket_id: BucketId,
        from_version: u64,
        reader_pool: ReaderThreadPool,
        live_indexes: Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>>,
        min_segment_id: SegmentId,
        check_closed_segments: bool,
    ) -> Result<Self, StreamIndexError> {
        // Check live indexes first
        if let Some((segment_id, index)) = live_indexes.get(&bucket_id) {
            let segment_id = segment_id.load(Ordering::Acquire);
            if segment_id >= min_segment_id {
                let index_guard = index.read().await;
                if let Some(stream_index) = index_guard.stream_index.get(&stream_id)
                    && let StreamOffsets::Offsets(offsets) = stream_index.offsets.clone()
                    && stream_index.version_min <= from_version
                {
                    let offsets_index = (from_version.saturating_sub(stream_index.version_min)
                        as usize)
                        .min(offsets.len());
                    drop(index_guard);

                    let segment_iter = SegmentStreamIter::new(
                        stream_id,
                        reader_pool,
                        BucketSegmentId::new(bucket_id, segment_id),
                        offsets,
                        offsets_index,
                    );
                    return Ok(StreamIter {
                        segment_iter: Some(segment_iter),
                        live_indexes,
                        last_version: from_version,
                        is_live: true,
                        has_next_segment: false,
                    });
                }
            }
        }

        if !check_closed_segments {
            return Ok(StreamIter {
                segment_iter: None,
                live_indexes,
                last_version: from_version,
                is_live: false,
                has_next_segment: false,
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
                    let res = segments.iter_mut().enumerate().rev().find_map(
                        |(i, (segment_id, reader_set)): (usize, (&u32, &mut ReaderSet))| {
                            if *segment_id < min_segment_id {
                                return None;
                            }

                            let stream_index = reader_set.stream_index.as_mut()?;

                            match stream_index.get_key(&stream_id) {
                                Ok(Some(key)) if key.version_min >= from_version || i == 0 => {
                                    let version_min = key.version_min;
                                    match stream_index.get_from_key(key) {
                                        Ok(StreamOffsets::Offsets(offsets)) => {
                                            let offsets_index =
                                                (from_version.saturating_sub(version_min) as usize)
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
                    );

                    let _ = reply_tx.send(res);
                })
            }
        });

        let (segment_iter, has_next_segment) = reply_rx
            .await?
            .transpose()?
            .map(|(segment_id, offsets, offsets_index, has_next_segment)| {
                (
                    SegmentStreamIter::new(
                        stream_id,
                        reader_pool,
                        BucketSegmentId::new(bucket_id, segment_id),
                        offsets,
                        offsets_index,
                    ),
                    has_next_segment,
                )
            })
            .unzip();

        Ok(StreamIter {
            segment_iter,
            live_indexes,
            last_version: from_version,
            is_live: false,
            has_next_segment: has_next_segment.unwrap_or(false),
        })
    }

    pub fn next_batch(
        &mut self,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<CommittedEvents>, StreamIndexError>> {
        async move {
            let Some(segment_iter) = &mut self.segment_iter else {
                return Ok(Vec::new());
            };

            let commits = segment_iter.next(limit).await?;
            if commits.is_empty()
                && segment_iter.is_finished()
                && (!self.is_live || self.has_next_segment)
            {
                // Rollover to next segment
                let segment_iter = self.segment_iter.take().unwrap();
                *self = Self::new_inner(
                    segment_iter.stream_id,
                    segment_iter.bucket_segment_id.bucket_id,
                    self.last_version,
                    segment_iter.reader_pool,
                    self.live_indexes.clone(),
                    segment_iter.bucket_segment_id.segment_id + 1,
                    self.has_next_segment,
                )
                .await?;
                return self.next_batch(limit).await;
            }

            Ok(commits)
        }
        .boxed()
    }

    /// Convenience method that maintains backward compatibility.
    /// Reads a single event by calling next_batch(1).
    pub async fn next(&mut self) -> Result<Option<CommittedEvents>, StreamIndexError> {
        let mut batch = self.next_batch(1).await?;
        Ok(batch.pop())
    }
}

/// An event stream iterator over a single segment file
#[derive(Debug)]
struct SegmentStreamIter {
    stream_id: StreamId,
    reader_pool: ReaderThreadPool,
    bucket_segment_id: BucketSegmentId,
    block: Option<Arc<SegmentBlock>>,
    last_block_offset_attempt: u64,
    offsets: Vec<u64>,
    offsets_index: usize,
}

impl SegmentStreamIter {
    fn new(
        stream_id: StreamId,
        reader_pool: ReaderThreadPool,
        bucket_segment_id: BucketSegmentId,
        offsets: Vec<u64>,
        offsets_index: usize,
    ) -> Self {
        debug_assert!(
            offsets.windows(2).all(|w| w[0] <= w[1]),
            "offsets must be sorted in ascending order"
        );

        SegmentStreamIter {
            stream_id,
            reader_pool,
            bucket_segment_id,
            block: None,
            last_block_offset_attempt: u64::MAX,
            offsets,
            offsets_index,
        }
    }

    fn is_finished(&self) -> bool {
        self.offsets_index >= self.offsets.len()
    }

    async fn next(&mut self, limit: usize) -> Result<Vec<CommittedEvents>, StreamIndexError> {
        let mut offsets_index = self.offsets_index;
        // Arbitrary max capacity of 10 - can we use something better, or should we just
        // not assume capacity?
        let mut commits = Vec::with_capacity(limit.min(10));
        self.read_from_cache(&mut offsets_index, &mut commits, limit)
            .await?;
        if offsets_index >= self.offsets.len() {
            self.offsets_index = offsets_index;
            return Ok(commits);
        }

        let stream_id = self.stream_id.clone();
        let bucket_segment_id = self.bucket_segment_id;
        let offsets = mem::take(&mut self.offsets);

        let (reply_tx, reply_rx) = oneshot::channel();
        struct Reply {
            res: Result<Vec<CommittedEvents>, ReadError>,
            offsets: Vec<u64>,
            offsets_index: usize,
        }

        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| {
                let res = Self::read_from_pool(
                    readers,
                    bucket_segment_id,
                    &stream_id,
                    &offsets,
                    &mut offsets_index,
                    &mut commits,
                    limit,
                )
                .map(|_| commits);
                let _ = reply_tx.send(Reply {
                    res,
                    offsets,
                    offsets_index,
                });
            });
        });

        let Reply {
            res,
            offsets,
            offsets_index,
        } = reply_rx.await?;
        self.offsets = offsets;

        let commits = res?;
        self.offsets_index = offsets_index;
        Ok(commits)
    }

    async fn hydrate_block(&mut self, offset: u64) -> Result<bool, ReadError> {
        let block_offset = SegmentBlockCache::get_block_offset(offset);
        if self.last_block_offset_attempt == block_offset {
            return Ok(false);
        }

        self.block = match self.reader_pool.get_cache(self.bucket_segment_id.bucket_id) {
            Some(cache) => {
                cache
                    .get_or_load(self.bucket_segment_id.segment_id, offset)
                    .await?
            }
            None => None,
        };
        self.last_block_offset_attempt = block_offset;

        Ok(true)
    }

    fn read_from_cache<'a>(
        &'a mut self,
        offsets_index: &'a mut usize,
        commits: &'a mut Vec<CommittedEvents>,
        limit: usize,
    ) -> BoxFuture<'a, Result<(), ReadError>> {
        async move {
            if let Some(block) = &self.block {
                while *offsets_index < self.offsets.len() {
                    let offset = self.offsets[*offsets_index];
                    let commit = match block.read_committed_events(offset) {
                        Ok((Some(commit), _)) => commit,
                        Ok((None, _)) => {
                            warn!("stream index points to event which doesn't exist");
                            break;
                        }
                        Err(ReadError::OutOfBounds) => {
                            if offset >= block.next_segment_block_offset() {
                                // Preload the next cache block and read from cache again
                                self.hydrate_block(block.next_segment_block_offset())
                                    .await?;
                                return self.read_from_cache(offsets_index, commits, limit).await;
                            } else {
                                self.block = None;
                            }

                            break;
                        }
                        Err(err) => return Err(err),
                    };

                    if let Some(filtered_commit) = filter_and_deduplicate_events(
                        &self.stream_id,
                        &self.offsets,
                        offsets_index,
                        commit,
                    ) {
                        commits.push(filtered_commit);

                        if commits.len() >= limit {
                            return Ok(());
                        }
                    }
                }
            } else if let Some(next_offset) = self.offsets.get(*offsets_index) {
                self.hydrate_block(*next_offset).await?;
                if self.block.is_some() {
                    // We got a new block, lets try from cache again
                    return self.read_from_cache(offsets_index, commits, limit).await;
                }
            }

            Ok(())
        }
        .boxed()
    }

    fn read_from_pool(
        readers: &mut HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>,
        bucket_segment_id: BucketSegmentId,
        stream_id: &StreamId,
        offsets: &[u64],
        offsets_index: &mut usize,
        commits: &mut Vec<CommittedEvents>,
        limit: usize,
    ) -> Result<(), ReadError> {
        let Some(readers) = readers.get_mut(&bucket_segment_id.bucket_id) else {
            return Ok(());
        };

        let Some(reader_set) = readers.get_mut(&bucket_segment_id.segment_id) else {
            return Ok(());
        };

        while *offsets_index < offsets.len() {
            let offset = offsets[*offsets_index];
            match reader_set
                .reader
                .read_committed_events(offset, ReadHint::Random)?
            {
                (Some(commit), _) => {
                    if let Some(filtered_commit) =
                        filter_and_deduplicate_events(stream_id, offsets, offsets_index, commit)
                    {
                        commits.push(filtered_commit);

                        if commits.len() >= limit {
                            return Ok(());
                        }
                    }
                }
                (None, _) => {
                    return Err(ReadError::Io(io::Error::other(format!(
                        "event not found at offset {offset} in {bucket_segment_id}"
                    ))));
                }
            }
        }

        Ok(())
    }
}

/// Filter events from a transaction and handle deduplication.
/// Advances offsets_index past all offsets in the same transaction.
/// Returns None if no events from our stream are in this transaction.
fn filter_and_deduplicate_events(
    stream_id: &StreamId,
    offsets: &[u64],
    offsets_index: &mut usize,
    mut commit: CommittedEvents,
) -> Option<CommittedEvents> {
    let max_returned_version = match &mut commit {
        CommittedEvents::Transaction { events, .. } => {
            events.retain(|event| &event.stream_id == stream_id);
            events.iter().map(|e| e.stream_version).max()
        }
        CommittedEvents::Single(event) => {
            if &event.stream_id == stream_id {
                Some(event.stream_version)
            } else {
                None
            }
        }
    };

    if max_returned_version.is_some() {
        // Skip over any remaining offsets that point to events in the same transaction
        if let CommittedEvents::Transaction {
            events: tx_events, ..
        } = &commit
        {
            let min_event_offset = tx_events.iter().map(|e| e.offset).min().unwrap_or(0);
            let max_event_offset = tx_events.iter().map(|e| e.offset).max().unwrap_or(0);

            // Count how many offsets (starting from current position) are in this
            // transaction
            let offsets_in_transaction = offsets[*offsets_index + 1..]
                .iter()
                .take_while(|&&offset| offset <= max_event_offset)
                .filter(|&&offset| offset >= min_event_offset)
                .count();

            // Advance index past all offsets in this transaction
            *offsets_index += 1 + offsets_in_transaction;
        } else {
            // Single event - just advance by 1
            *offsets_index += 1;
        }

        Some(commit)
    } else {
        // No matching events, but still advance past this offset
        *offsets_index += 1;
        None
    }
}

#[cfg(test)]
mod filter_and_deduplicate_events_tests {
    use smallvec::smallvec;
    use uuid::Uuid;

    use super::*;
    use crate::bucket::segment::{CommitRecord, EventRecord};

    fn make_event(stream_id: &str, stream_version: u64, offset: u64) -> EventRecord {
        EventRecord {
            offset,
            event_id: Uuid::new_v4(),
            partition_key: Uuid::new_v4(),
            partition_id: 0,
            transaction_id: Uuid::new_v4(),
            partition_sequence: 0,
            stream_version,
            timestamp: 0,
            confirmation_count: 1,
            stream_id: StreamId::new(stream_id).unwrap(),
            event_name: "MyEvent".to_string(),
            metadata: vec![],
            payload: vec![],
            size: 64,
        }
    }

    fn make_commit() -> CommitRecord {
        CommitRecord {
            offset: 0,
            transaction_id: Uuid::new_v4(),
            timestamp: 0,
            confirmation_count: 1,
            event_count: 5,
        }
    }

    #[test]
    fn test_single_event_matching_stream() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 30];
        let mut offsets_index = 1;

        let commit = CommittedEvents::Single(make_event("stream-1", 5, 20));

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        assert_eq!(offsets, vec![10, 20, 30]); // Offsets unchanged
        assert_eq!(offsets_index, 2); // Advanced by 1
    }

    #[test]
    fn test_single_event_not_matching_stream() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 30];
        let mut offsets_index = 1;

        let commit = CommittedEvents::Single(make_event("stream-2", 5, 20));

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_none());
        assert_eq!(offsets, vec![10, 20, 30]); // Offsets unchanged
        assert_eq!(offsets_index, 2); // Still advanced by 1
    }

    #[test]
    fn test_transaction_with_matching_events() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 30];
        let mut offsets_index = 1;

        let events = smallvec![
            make_event("stream-1", 5, 20),
            make_event("stream-2", 3, 21),
            make_event("stream-1", 6, 22),
        ];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        if let Some(CommittedEvents::Transaction { events, .. }) = result {
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].stream_version, 5);
            assert_eq!(events[1].stream_version, 6);
        } else {
            panic!("Expected Transaction variant");
        }
        assert_eq!(offsets, vec![10, 20, 30]); // Offsets unchanged
    }

    #[test]
    fn test_transaction_with_no_matching_events() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 30];
        let mut offsets_index = 1;

        let events = smallvec![make_event("stream-2", 5, 20), make_event("stream-3", 3, 21),];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_none());
        assert_eq!(offsets, vec![10, 20, 30]); // Offsets unchanged
        assert_eq!(offsets_index, 2); // Still advanced by 1
    }

    #[test]
    fn test_transaction_skips_all_offsets_in_range() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 21, 22, 30, 40];
        let mut offsets_index = 1; // Starting at offset 20

        // Transaction spans offsets 20-22
        let events = smallvec![
            make_event("stream-1", 5, 20),
            make_event("stream-1", 6, 21),
            make_event("stream-1", 7, 22),
        ];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        assert_eq!(offsets, vec![10, 20, 21, 22, 30, 40]); // Offsets unchanged
        // Should skip past offsets 20, 21, 22 (3 offsets in transaction)
        assert_eq!(offsets_index, 4); // Now pointing at offset 30
    }

    #[test]
    fn test_transaction_with_gaps_in_offsets() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 25, 30];
        let mut offsets_index = 1; // Starting at offset 20

        // Transaction spans offsets 20-22, but offset 21, 22 aren't in our vec
        let events = smallvec![
            make_event("stream-1", 5, 20),
            make_event("stream-1", 6, 21),
            make_event("stream-1", 7, 22),
        ];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        // Only offset 20 is in the transaction range from our offsets vec
        assert_eq!(offsets_index, 2); // Advanced by 1 (only consumed offset 20)
    }

    #[test]
    fn test_transaction_consumes_multiple_sequential_offsets() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![5, 20, 21, 22, 23, 24, 40];
        let mut offsets_index = 1; // Starting at offset 20

        // Transaction spans offsets 20-24
        let events = smallvec![
            make_event("stream-1", 5, 20),
            make_event("stream-1", 6, 21),
            make_event("stream-1", 7, 22),
            make_event("stream-1", 8, 23),
            make_event("stream-1", 9, 24),
        ];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        // Should consume offsets 20, 21, 22, 23, 24 (5 offsets)
        assert_eq!(offsets_index, 6); // Now pointing at offset 40
    }

    #[test]
    fn test_transaction_at_end_of_offsets() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 21, 22];
        let mut offsets_index = 1; // Starting at offset 20

        // Transaction spans offsets 20-22
        let events = smallvec![
            make_event("stream-1", 5, 20),
            make_event("stream-1", 6, 21),
            make_event("stream-1", 7, 22),
        ];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        // Should consume all remaining offsets (20, 21, 22)
        assert_eq!(offsets_index, 4); // Past the end of offsets vec
    }

    #[test]
    fn test_transaction_beyond_remaining_offsets() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 30];
        let mut offsets_index = 1; // Starting at offset 20

        // Transaction spans offsets 20-25, but we only have 20, 30 left
        let events = smallvec![
            make_event("stream-1", 5, 20),
            make_event("stream-1", 6, 21),
            make_event("stream-1", 7, 22),
            make_event("stream-1", 8, 25),
        ];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        // Should consume only offset 20 (offset 30 > max_event_offset of 25)
        assert_eq!(offsets_index, 2); // Now pointing at offset 30
    }

    #[test]
    fn test_at_beginning_of_offsets() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![20, 21, 22, 30];
        let mut offsets_index = 0;

        let events = smallvec![
            make_event("stream-1", 5, 20),
            make_event("stream-1", 6, 21),
            make_event("stream-1", 7, 22),
        ];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        // Should consume offsets 20, 21, 22
        assert_eq!(offsets_index, 3); // Now pointing at offset 30
    }

    #[test]
    fn test_transaction_no_offsets_in_range() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 30, 40];
        let mut offsets_index = 1; // Starting at offset 30

        // Transaction spans offsets 20-22, none of which are in our vec
        let events = smallvec![
            make_event("stream-1", 5, 20),
            make_event("stream-1", 6, 21),
            make_event("stream-1", 7, 22),
        ];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        assert_eq!(offsets_index, 2); // Advanced by 1
    }

    #[test]
    fn test_empty_transaction_after_filtering() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 30];
        let mut offsets_index = 1;

        // Transaction with events, but none match our stream
        let events = smallvec![make_event("stream-2", 5, 20),];

        let commit = CommittedEvents::Transaction {
            events: Box::new(events),
            commit: make_commit(),
        };

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_none());
        assert_eq!(offsets_index, 2); // Still advanced
    }

    #[test]
    fn test_single_event_at_end() {
        let stream_id = StreamId::new("stream-1").unwrap();
        let offsets = vec![10, 20, 30];
        let mut offsets_index = 2; // Last offset

        let commit = CommittedEvents::Single(make_event("stream-1", 5, 30));

        let result =
            filter_and_deduplicate_events(&stream_id, &offsets, &mut offsets_index, commit);

        assert!(result.is_some());
        assert_eq!(offsets_index, 3); // Past the end
    }
}
