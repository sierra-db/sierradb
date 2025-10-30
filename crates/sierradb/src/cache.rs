use std::sync::Arc;

use moka::sync::Cache;
use tokio::sync::oneshot;

use crate::bucket::segment::SegmentBlock;
use crate::bucket::{BucketId, SegmentId};
use crate::error::ReadError;
use crate::reader_thread_pool::ReaderThreadPool;

const KB: usize = 1024;
pub const BLOCK_SIZE: usize = KB * 64;

#[derive(Clone, Debug)]
pub struct SegmentBlockCache {
    bucket_id: BucketId,
    // Key: (segment_id, block_offset aligned to boundary)
    // Value: block of raw bytes
    cache: Cache<(SegmentId, u64), Arc<SegmentBlock>>,
    reader_pool: ReaderThreadPool,
}

impl SegmentBlockCache {
    pub fn new(bucket_id: BucketId, capacity: usize, reader_pool: ReaderThreadPool) -> Self {
        Self {
            bucket_id,
            cache: Cache::new(capacity.try_into().unwrap()),
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
            return Ok(Some(block));
        }

        // Cache miss - read from disk
        let bucket_id = self.bucket_id;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| {
                let Some(segments) = readers.get(&bucket_id) else {
                    let _ = reply_tx.send(Ok(None));
                    return;
                };

                let Some(reader_set) = segments.get(&segment_id) else {
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
