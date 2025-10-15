use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::bucket::event_index::ClosedEventIndex;
use crate::bucket::partition_index::ClosedPartitionIndex;
use crate::bucket::segment::BucketSegmentReader;
use crate::bucket::stream_index::ClosedStreamIndex;
use crate::bucket::{BucketId, BucketSegmentId, SegmentId};

thread_local! {
    static READERS: RefCell<HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>> = RefCell::new(HashMap::new());
}

pub struct ReaderSet {
    pub reader: BucketSegmentReader,
    pub event_index: Option<ClosedEventIndex>,
    pub partition_index: Option<ClosedPartitionIndex>,
    pub stream_index: Option<ClosedStreamIndex>,
}

#[derive(Clone, Debug)]
pub struct ReaderThreadPool {
    pool: Arc<ThreadPool>,
}

impl ReaderThreadPool {
    /// Spawns threads to process read requests in a thread pool.
    pub fn new(num_workers: usize) -> Self {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_workers)
            .build()
            .unwrap();

        ReaderThreadPool {
            pool: Arc::new(pool),
        }
    }

    pub fn spawn<OP, IN>(&self, op: OP)
    where
        OP: FnOnce(fn(IN)) + Send + 'static,
        IN: FnOnce(&mut HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>),
    {
        self.pool.spawn(|| {
            let with_reader = |op: IN| READERS.with_borrow_mut(op);
            op(with_reader as fn(_))
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
        self.pool.broadcast(|_| {
            let reader_set = ReaderSet {
                reader: reader.try_clone().unwrap(),
                event_index: event_index.map(|index| index.try_clone().unwrap()),
                partition_index: partition_index.map(|index| index.try_clone().unwrap()),
                stream_index: stream_index.map(|index| index.try_clone().unwrap()),
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

pub struct SequentialReaderContext {
    readers: HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>,
}

impl SequentialReaderContext {
    /// Creates a snapshot of current readers for sequential access
    pub fn from_pool(pool: &ReaderThreadPool) -> Self {
        // Get current state from one worker in the pool
        pool.install(|with_reader| {
            let snapshot = with_reader(|readers| {
                // Clone all readers for sequential access
                readers
                    .iter()
                    .map(|(bucket_id, segments)| {
                        let cloned_segments = segments
                            .iter()
                            .map(|(seg_id, reader_set)| {
                                (
                                    *seg_id,
                                    ReaderSet {
                                        reader: reader_set.reader.try_clone().unwrap(),
                                        event_index: reader_set
                                            .event_index
                                            .as_ref()
                                            .map(|i| i.try_clone().unwrap()),
                                        partition_index: reader_set
                                            .partition_index
                                            .as_ref()
                                            .map(|i| i.try_clone().unwrap()),
                                        stream_index: reader_set
                                            .stream_index
                                            .as_ref()
                                            .map(|i| i.try_clone().unwrap()),
                                    },
                                )
                            })
                            .collect();
                        (*bucket_id, cloned_segments)
                    })
                    .collect()
            });

            SequentialReaderContext { readers: snapshot }
        })
    }

    pub fn get_reader(&mut self, bucket_segment_id: BucketSegmentId) -> Option<&mut ReaderSet> {
        self.readers
            .get_mut(&bucket_segment_id.bucket_id)?
            .get_mut(&bucket_segment_id.segment_id)
    }
}
