use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use rayon::{ThreadPool, ThreadPoolBuilder};
use tracing::warn;

use crate::bucket::event_index::ClosedEventIndex;
use crate::bucket::partition_index::ClosedPartitionIndex;
use crate::bucket::segment::BucketSegmentReader;
use crate::bucket::stream_index::ClosedStreamIndex;
use crate::bucket::{BucketId, BucketSegmentId, SegmentId};
use crate::cache::SegmentBlockCache;

thread_local! {
    static READERS: RefCell<HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>> = RefCell::new(HashMap::new());
}

pub struct ReaderSet {
    pub reader: BucketSegmentReader,
    pub event_index: Option<ClosedEventIndex>,
    pub partition_index: Option<ClosedPartitionIndex>,
    pub stream_index: Option<ClosedStreamIndex>,
    pub cache: Arc<SegmentBlockCache>,
}

#[derive(Clone, Debug)]
pub struct ReaderThreadPool {
    pool: Arc<ThreadPool>,
    caches: Arc<HashMap<BucketId, Arc<SegmentBlockCache>>>,
}

impl ReaderThreadPool {
    /// Spawns threads to process read requests in a thread pool.
    pub fn new(num_workers: usize, bucket_ids: &[BucketId], total_cache_size: usize) -> Self {
        assert!(!bucket_ids.is_empty());

        let pool = ThreadPoolBuilder::new()
            .num_threads(num_workers)
            .build()
            .unwrap();

        // Calculate blocks per bucket
        let num_buckets = bucket_ids.len();
        let cache_size_per_bucket = (total_cache_size / num_buckets) as u64;
        if cache_size_per_bucket == 0 {
            warn!(
                "cache size per bucket too small: {cache_size_per_bucket}B per bucket results in 0 blocks: consider increasing total_cache_size or reducing number of buckets"
            );
        }

        let mut reader_pool = ReaderThreadPool {
            pool: Arc::new(pool),
            caches: Arc::new(HashMap::new()),
        };

        reader_pool.caches = Arc::new(
            bucket_ids
                .iter()
                .map(|bucket_id| {
                    (
                        *bucket_id,
                        Arc::new(SegmentBlockCache::new(
                            *bucket_id,
                            cache_size_per_bucket,
                            reader_pool.clone(),
                        )),
                    )
                })
                .collect(),
        );

        reader_pool
    }

    pub fn caches(&self) -> &Arc<HashMap<BucketId, Arc<SegmentBlockCache>>> {
        &self.caches
    }

    pub fn get_cache(&self, bucket_id: BucketId) -> Option<&Arc<SegmentBlockCache>> {
        self.caches.get(&bucket_id)
    }

    /// Spawns an asynchronous task in this thread pool. This task will
    /// run in the implicit, global scope, which means that it may outlast
    /// the current stack frame -- therefore, it cannot capture any references
    /// onto the stack (you will likely need a `move` closure).
    pub fn spawn<OP, IN, R>(&self, op: OP)
    where
        OP: FnOnce(fn(IN) -> R) + Send + 'static,
        IN: FnOnce(&mut HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>) -> R,
    {
        self.pool.spawn(|| {
            let with_reader = |op: IN| READERS.with_borrow_mut(op);
            op(with_reader)
        })
    }

    // pub fn install<OP, IN, OPR, INR>(&self, op: OP) -> OPR
    // where
    //     OP: FnOnce(fn(IN)) -> OPR + Send,
    //     IN: FnOnce(&mut HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>) ->
    // INR,     OPR: Send,
    // {
    //     self.pool.install(|| {
    //         let with_reader = |op: IN| READERS.with_borrow_mut(op);
    //         op(with_reader as fn(_))
    //     })
    // }

    /// Executes `op` within the thread pool. Any attempts to use
    /// `join`, `scope`, or parallel iterators will then operate
    /// within that thread pool.
    pub fn install<OP, R, IN, RR>(&self, op: OP) -> R
    where
        OP: FnOnce(fn(IN) -> RR) -> R + Send,
        IN: FnOnce(&mut HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>) -> RR,
        R: Send,
    {
        self.pool.install(|| {
            let with_reader = |op: IN| READERS.with_borrow_mut(op);
            op(with_reader as fn(_) -> _)
        })
    }

    /// Adds a bucket segment reader to all workers in the thread pool.
    pub fn add_bucket_segment(
        &self,
        bucket_segment_id: BucketSegmentId,
        reader: &BucketSegmentReader,
        event_index: Option<&ClosedEventIndex>,
        partition_index: Option<&ClosedPartitionIndex>,
        stream_index: Option<&ClosedStreamIndex>,
    ) {
        let cache = self
            .caches
            .get(&bucket_segment_id.bucket_id)
            .unwrap()
            .clone();
        self.pool.broadcast(|_| {
            let reader_set = ReaderSet {
                reader: reader.try_clone().unwrap(),
                event_index: event_index.map(|index| index.try_clone().unwrap()),
                partition_index: partition_index.map(|index| index.try_clone().unwrap()),
                stream_index: stream_index.map(|index| index.try_clone().unwrap()),
                cache: cache.clone(),
            };
            READERS.with_borrow_mut(|readers| {
                readers
                    .entry(bucket_segment_id.bucket_id)
                    .or_default()
                    .insert(bucket_segment_id.segment_id, reader_set)
            });
        });
    }

    // pub fn read_confirmed_events(
    //     &self,
    //     bucket_segment_id: BucketSegmentId,
    //     offset: u64,
    // ) -> io::Result<Option<CommittedEvents<'static>>> {
    //     self.pool.install(move || {
    //         READERS.with_borrow_mut(|readers| match
    // readers.get_mut(&bucket_segment_id) {             Some(reader) => reader
    //                 .read_committed_events(offset, false)
    //                 .map(|record| record.map(CommittedEvents::into_owned)),
    //             None => Ok(None),
    //         })
    //     })
    // }

    // /// Reads a record from the specified bucket and segment at the given offset.
    // /// This method executes synchronously using the thread pool and returns an
    // owned record. ///
    // /// # Arguments
    // /// - `bucket_segment_id`: The bucket segment id.
    // /// - `offset`: The offset within the segment.
    // ///
    // /// # Returns
    // /// - `Ok(Some(Record<'static>))` if the record is found.
    // /// - `Ok(None)` if the record does not exist.
    // /// - `Err(io::Error)` if an error occurs during reading.
    // pub fn read_record(
    //     &self,
    //     bucket_segment_id: BucketSegmentId,
    //     offset: u64,
    // ) -> io::Result<Option<Record<'static>>> {
    //     self.pool.install(move || {
    //         READERS.with_borrow_mut(|readers| match
    // readers.get_mut(&bucket_segment_id) {             Some(reader) => reader
    //                 .read_record(offset, false)
    //                 .map(|record| record.map(Record::into_owned)),
    //             None => Ok(None),
    //         })
    //     })
    // }

    // /// Reads a record and passes the result to the provided handler function.
    // /// This method executes synchronously using the thread pool.
    // ///
    // /// # Arguments
    // /// - `bucket_segment_id`: The bucket segment id.
    // /// - `offset`: The offset within the segment.
    // /// - `handler`: A closure that processes the read result.
    // ///
    // /// # Returns
    // /// - The return value of the `handler` function, wrapped in `Some`.
    // /// - `None` if the segment is unknown or if the handler panics.
    // pub fn read_record_with<R>(
    //     &self,
    //     bucket_segment_id: BucketSegmentId,
    //     offset: u64,
    //     handler: impl for<'a> FnOnce(io::Result<Option<Record<'a>>>) -> R + Send
    // + 'static, ) -> Option<R>
    // where
    //     R: Send,
    // {
    //     self.pool
    //         .install(move || {
    //             READERS.with_borrow_mut(|readers| match
    // readers.get_mut(&bucket_segment_id) {                 Some(reader) => {
    //                     let res = reader.read_record(offset, false);
    //                     catch_unwind(AssertUnwindSafe(move ||
    // Some(handler(res))))                 }
    //                 None => Ok(None),
    //             })
    //         })
    //         .unwrap()
    // }

    // /// Spawns an asynchronous task to read a record and process it using the
    // given handler function. /// This method does not block and executes the
    // handler in a separate thread. ///
    // /// # Arguments
    // /// - `bucket_segment_id`: The bucket segment id.
    // /// - `offset`: The offset within the segment.
    // /// - `handler`: A closure that processes the read result.
    // ///
    // /// # Notes
    // /// - If the handler panics, the error is logged.
    // /// - If the segment is unknown, an error is logged.
    // pub fn spawn_read_record(
    //     &self,
    //     bucket_segment_id: BucketSegmentId,
    //     offset: u64,
    //     handler: impl for<'a> FnOnce(io::Result<Option<Record<'a>>>) + Send +
    // 'static, ) {
    //     self.pool.spawn(move || {
    //         READERS.with_borrow_mut(|readers| match
    // readers.get_mut(&bucket_segment_id) {             Some(reader) => {
    //                 let res = reader.read_record(offset, false);
    //                 let panic_res = catch_unwind(AssertUnwindSafe(move ||
    // handler(res)));                 if let Err(err) = panic_res {
    //                     error!("reader panicked: {err:?}");
    //                 }
    //             }
    //             None => {
    //                 error!("spawn_read_record: unknown bucket segment
    // {bucket_segment_id}");             }
    //         });
    //     });
    // }
}
