use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::{io, mem};

use seglog::read::ReadHint;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::IterDirection;
use crate::bucket::segment::{CommittedEvents, SegmentBlock};
use crate::bucket::{BucketId, BucketSegmentId, SegmentId};
use crate::cache::SegmentBlockCache;
use crate::error::ReadError;
use crate::reader_thread_pool::{ReaderSet, ReaderThreadPool};

#[derive(Debug)]
pub struct SegmentIter {
    pub(crate) reader_pool: ReaderThreadPool,
    pub(crate) bucket_segment_id: BucketSegmentId,
    pub(crate) block: Option<Arc<SegmentBlock>>,
    pub(crate) last_block_offset_attempt: u64,
    pub(crate) offsets: Vec<u64>,
    pub(crate) offsets_index: usize,
}

impl SegmentIter {
    pub fn new(
        reader_pool: ReaderThreadPool,
        bucket_segment_id: BucketSegmentId,
        mut offsets: Vec<u64>,
        offsets_index: usize,
        dir: IterDirection,
    ) -> Self {
        debug_assert!(
            offsets.windows(2).all(|w| w[0] <= w[1]),
            "offsets must be sorted in ascending order"
        );

        // For reverse iteration, reverse the offsets and adjust the index
        let offsets_index = match dir {
            IterDirection::Forward => offsets_index,
            IterDirection::Reverse => {
                offsets.reverse();
                if offsets_index == 0 && !offsets.is_empty() {
                    0 // Start from first index after reversal (which is the last event)
                } else if offsets_index < offsets.len() {
                    offsets.len() - 1 - offsets_index
                } else {
                    0
                }
            }
        };

        SegmentIter {
            reader_pool,
            bucket_segment_id,
            block: None,
            last_block_offset_attempt: u64::MAX,
            offsets,
            offsets_index,
        }
    }

    pub fn is_finished(&self) -> bool {
        self.offsets_index >= self.offsets.len()
    }

    pub fn remaining_offsets(&self) -> &[u64] {
        &self.offsets[self.offsets_index..]
    }

    pub fn skip(&mut self, count: usize) {
        self.offsets_index = (self.offsets_index + count).min(self.offsets.len());
    }

    pub async fn next(&mut self, limit: usize) -> Result<Option<Vec<CommittedEvents>>, ReadError> {
        let mut offsets_index = self.offsets_index;

        if limit == 0 || offsets_index >= self.offsets.len() {
            return Ok(None);
        }

        // Arbitrary max capacity of 10 - can we use something better, or should we just
        // not assume capacity?
        let mut commits = Vec::with_capacity(limit.min(10));
        let commits_from_cache = self
            .read_from_cache(&mut offsets_index, &mut commits, limit)
            .await?;
        if offsets_index >= self.offsets.len() {
            assert!(
                commits_from_cache > 0,
                "if offsets_index has advanced, then we should've read at least one commit from cache"
            );
            self.offsets_index = offsets_index;
            return Ok(Some(commits));
        }

        let bucket_segment_id = self.bucket_segment_id;
        let offsets = mem::take(&mut self.offsets);

        let (reply_tx, reply_rx) = oneshot::channel();
        struct Reply {
            res: Result<(Vec<CommittedEvents>, usize), ReadError>,
            offsets: Vec<u64>,
            offsets_index: usize,
        }

        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| {
                let res = Self::read_from_pool(
                    readers,
                    bucket_segment_id,
                    &offsets,
                    &mut offsets_index,
                    &mut commits,
                    limit,
                )
                .map(|commits_from_disk| (commits, commits_from_disk));
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

        let (commits, commits_from_disk) = res?;
        self.offsets_index = offsets_index;

        if commits_from_cache + commits_from_disk == 0 {
            assert!(
                commits.is_empty(),
                "if we read nothing from cache nor disk, then commits should be empty"
            );
            return Ok(None);
        }

        Ok(Some(commits))
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
            None => {
                debug!(
                    "cache doesn't exist for bucket id {}",
                    self.bucket_segment_id.bucket_id
                );
                None
            }
        };
        self.last_block_offset_attempt = block_offset;

        Ok(true)
    }

    async fn read_from_cache<'a>(
        &'a mut self,
        offsets_index: &'a mut usize,
        commits: &'a mut Vec<CommittedEvents>,
        limit: usize,
    ) -> Result<usize, ReadError> {
        let mut appended = 0;

        // Use iteration instead of recursion to avoid stack overflow when
        // reading across many cache blocks (256MB segments / 64KB blocks = ~4096 blocks)
        loop {
            let mut should_hydrate: Option<u64> = None;
            let mut clear_block = false;

            if let Some(block) = &self.block {
                debug!(
                    "offsets_index = {offsets_index}, offsets_len = {}, next_offset = {:?}",
                    self.offsets.len(),
                    self.offsets.get(*offsets_index)
                );
                while *offsets_index < self.offsets.len() {
                    let offset = self.offsets[*offsets_index];
                    let commit = match block.read_committed_events(offset) {
                        Ok((Some(commit), _)) => commit,
                        Ok((None, _)) => {
                            warn!("offset points to event which doesn't exist");
                            break;
                        }
                        Err(ReadError::Reader(seglog::read::ReadError::OutOfBounds { .. })) => {
                            let next_segment_block_offset = block.next_segment_block_offset();
                            if offset >= next_segment_block_offset {
                                // Preload the next cache block and continue iterating
                                should_hydrate = Some(next_segment_block_offset);
                            } else {
                                debug!(
                                    "offset {offset} is before the next segment block offset {next_segment_block_offset}"
                                );
                                clear_block = true;
                            }
                            break;
                        }
                        Err(err) => return Err(err),
                    };

                    advance_offsets_index(&commit, &self.offsets, offsets_index);
                    commits.push(commit);
                    appended += 1;

                    if commits.len() >= limit {
                        return Ok(appended);
                    }
                }
            } else if let Some(next_offset) = self.offsets.get(*offsets_index) {
                should_hydrate = Some(*next_offset);
            }

            // Borrow of block has ended, we can now mutate self
            if clear_block {
                self.block = None;
            }

            if let Some(offset) = should_hydrate {
                self.hydrate_block(offset).await?;
                if self.block.is_some() {
                    // Successfully hydrated a new block, continue reading
                    continue;
                }
            }

            // No more work to do
            break;
        }

        Ok(appended)
    }

    fn read_from_pool(
        readers: &mut HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>,
        bucket_segment_id: BucketSegmentId,
        offsets: &[u64],
        offsets_index: &mut usize,
        commits: &mut Vec<CommittedEvents>,
        limit: usize,
    ) -> Result<usize, ReadError> {
        let Some(readers) = readers.get_mut(&bucket_segment_id.bucket_id) else {
            return Ok(0);
        };

        let Some(reader_set) = readers.get_mut(&bucket_segment_id.segment_id) else {
            return Ok(0);
        };

        let mut appended = 0;
        while *offsets_index < offsets.len() {
            let offset = offsets[*offsets_index];
            match reader_set
                .reader
                .read_committed_events(offset, ReadHint::Random)?
            {
                (Some(commit), _) => {
                    advance_offsets_index(&commit, offsets, offsets_index);
                    commits.push(commit);
                    appended += 1;

                    if commits.len() >= limit {
                        return Ok(appended);
                    }
                }
                (None, _) => {
                    return Err(ReadError::Reader(seglog::read::ReadError::Io(
                        io::Error::other(format!(
                            "event not found at offset {offset} in {bucket_segment_id}"
                        )),
                    )));
                }
            }
        }

        Ok(appended)
    }
}

/// Filter events from a transaction and handle deduplication.
/// Advances offsets_index past all offsets in the same transaction.
/// Returns false if no events are in this transaction.
fn advance_offsets_index(commit: &CommittedEvents, offsets: &[u64], offsets_index: &mut usize) {
    // Skip over any remaining offsets that point to events in the same transaction
    if let CommittedEvents::Transaction { events, .. } = &commit
        && !events.is_empty()
    {
        let min_event_offset = events.iter().map(|e| e.offset).min().unwrap_or(0);
        let max_event_offset = events.iter().map(|e| e.offset).max().unwrap_or(0);

        // Count how many offsets (after current position) are in this transaction
        let offsets_in_transaction = offsets[*offsets_index + 1..]
            .iter()
            .take_while(|&&offset| offset >= min_event_offset && offset <= max_event_offset)
            .count();

        // Advance index past all offsets in this transaction
        *offsets_index += 1 + offsets_in_transaction;
    } else {
        // Single event - just advance by 1
        *offsets_index += 1;
    }
}
