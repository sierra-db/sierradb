use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use moka::sync::Cache;
use tokio::sync::oneshot;
use tracing::trace;

use crate::bucket::segment::SegmentBlock;
use crate::bucket::{BucketId, SegmentId};
use crate::error::ReadError;
use crate::reader_thread_pool::ReaderThreadPool;

const KB: usize = 1024;
pub const BLOCK_SIZE: usize = KB * 64;

static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static BLOCKS_EVICTED: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct SegmentBlockCache {
    bucket_id: BucketId,
    // Key: (segment_id, block_offset aligned to boundary)
    // Value: block of raw bytes
    cache: Cache<(SegmentId, u64), Arc<SegmentBlock>>,
    reader_pool: ReaderThreadPool,
}

impl SegmentBlockCache {
    #[inline]
    pub fn cache_hits() -> u64 {
        CACHE_HITS.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn cache_misses() -> u64 {
        CACHE_MISSES.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn blocks_evicted() -> u64 {
        BLOCKS_EVICTED.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn cache(&self) -> &Cache<(SegmentId, u64), Arc<SegmentBlock>> {
        &self.cache
    }

    pub fn new(bucket_id: BucketId, capacity: u64, reader_pool: ReaderThreadPool) -> Self {
        Self {
            bucket_id,
            cache: Cache::builder()
                .max_capacity(capacity)
                .weigher(|_, _| BLOCK_SIZE as u32)
                .eviction_listener(|_key, _val, _cause| {
                    BLOCKS_EVICTED.fetch_add(1, Ordering::Relaxed);
                })
                .build(),
            reader_pool,
        }
    }

    #[inline]
    pub fn get_block_offset(offset: u64) -> u64 {
        offset & !0xFFFF // Round down to boundary
    }

    pub fn get(&self, segment_id: SegmentId, block_offset: u64) -> Option<Arc<SegmentBlock>> {
        let key = (segment_id, block_offset);
        self.cache.get(&key)
    }

    pub fn insert(&self, segment_id: SegmentId, block_offset: u64, block: Arc<SegmentBlock>) {
        let key = (segment_id, block_offset);
        self.cache.insert(key, block);
    }

    pub async fn get_or_load(
        &self,
        segment_id: SegmentId,
        offset: u64,
    ) -> Result<Option<Arc<SegmentBlock>>, ReadError> {
        let block_offset = Self::get_block_offset(offset);
        let key = (segment_id, block_offset);

        if let Some(block) = self.cache.get(&key) {
            CACHE_HITS.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(block));
        }

        // Cache miss - read from disk
        CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
        let bucket_id = self.bucket_id;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| {
                let Some(segments) = readers.get(&bucket_id) else {
                    trace!("bucket id doesn't exist in readers");
                    let _ = reply_tx.send(Ok(None));
                    return;
                };

                let Some(reader_set) = segments.get(&segment_id) else {
                    trace!("segment id doesn't exist in bucket segments");
                    let _ = reply_tx.send(Ok(None));
                    return;
                };

                match reader_set.reader.read_block(block_offset) {
                    Ok(Some(block)) => {
                        // Block successfully read and is fully flushed
                        let block = Arc::new(block);
                        let _ = reply_tx.send(Ok(Some(block)));
                    }
                    Ok(None) => {
                        // Block not fully flushed yet
                        trace!("could not read block as it was not fully flushed yet");
                        let _ = reply_tx.send(Ok(None));
                    }
                    Err(err) => {
                        let _ = reply_tx.send(Err(err));
                    }
                }
            })
        });

        let Some(block) = reply_rx.await.map_err(|_| ReadError::NoThreadReply)?? else {
            return Ok(None);
        };

        self.cache.insert(key, Arc::clone(&block));
        Ok(Some(block))
    }
}
