use std::{
    cell::RefCell,
    collections::HashMap,
    io,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Arc,
};

use rayon::{ThreadPool, ThreadPoolBuilder};
use tracing::error;

use super::{BucketSegmentReader, Record};

thread_local! {
    static READERS: RefCell<HashMap<(u16, u32), BucketSegmentReader>> = RefCell::new(HashMap::new());
}

#[derive(Clone, Debug)]
pub struct ReaderThreadPool {
    pool: Arc<ThreadPool>,
}

impl ReaderThreadPool {
    /// Spawns threads to process read requests in a thread pool.
    pub fn spawn(num_workers: usize) -> Self {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_workers)
            .build()
            .unwrap();

        ReaderThreadPool {
            pool: Arc::new(pool),
        }
    }

    /// Adds a bucket segment reader to all workers in the thread pool.
    pub fn add_bucket_segment(
        &self,
        bucket_id: u16,
        segment: u32,
        reader: &BucketSegmentReader,
    ) -> io::Result<()> {
        self.pool.broadcast(|_| {
            READERS.with_borrow_mut(|readers| {
                readers.insert((bucket_id, segment), reader.try_clone().unwrap())
            });
        });

        Ok(())
    }

    /// Reads a record from the specified bucket and segment at the given offset.
    /// This method executes synchronously using the thread pool and returns an owned record.
    ///
    /// # Arguments
    /// - `bucket_id`: The ID of the bucket.
    /// - `segment`: The segment number.
    /// - `offset`: The offset within the segment.
    ///
    /// # Returns
    /// - `Ok(Some(Record<'static>))` if the record is found.
    /// - `Ok(None)` if the record does not exist.
    /// - `Err(io::Error)` if an error occurs during reading.
    pub fn read_record(
        &self,
        bucket_id: u16,
        segment: u32,
        offset: u64,
    ) -> io::Result<Option<Record<'static>>> {
        self.pool.install(move || {
            READERS.with_borrow_mut(|readers| match readers.get_mut(&(bucket_id, segment)) {
                Some(reader) => reader
                    .read_record(offset, false)
                    .map(|record| record.map(Record::into_owned)),
                None => Ok(None),
            })
        })
    }

    /// Reads a record and passes the result to the provided handler function.
    /// This method executes synchronously using the thread pool.
    ///
    /// # Arguments
    /// - `bucket_id`: The ID of the bucket.
    /// - `segment`: The segment number.
    /// - `offset`: The offset within the segment.
    /// - `handler`: A closure that processes the read result.
    ///
    /// # Returns
    /// - The return value of the `handler` function, wrapped in `Some`.
    /// - `None` if the segment is unknown or if the handler panics.
    pub fn read_record_with<R>(
        &self,
        bucket_id: u16,
        segment: u32,
        offset: u64,
        handler: impl FnOnce(io::Result<Option<Record>>) -> R + Send + 'static,
    ) -> Option<R>
    where
        R: Send,
    {
        self.pool
            .install(move || {
                READERS.with_borrow_mut(|readers| match readers.get_mut(&(bucket_id, segment)) {
                    Some(reader) => {
                        let res = reader.read_record(offset, false);
                        catch_unwind(AssertUnwindSafe(move || Some(handler(res))))
                    }
                    None => Ok(None),
                })
            })
            .unwrap()
    }

    /// Spawns an asynchronous task to read a record and process it using the given handler function.
    /// This method does not block and executes the handler in a separate thread.
    ///
    /// # Arguments
    /// - `bucket_id`: The ID of the bucket.
    /// - `segment`: The segment number.
    /// - `offset`: The offset within the segment.
    /// - `handler`: A closure that processes the read result.
    ///
    /// # Notes
    /// - If the handler panics, the error is logged.
    /// - If the segment is unknown, an error is logged.
    pub fn spawn_read_record(
        &self,
        bucket_id: u16,
        segment: u32,
        offset: u64,
        handler: impl FnOnce(io::Result<Option<Record>>) + Send + 'static,
    ) {
        self.pool.spawn(move || {
            READERS.with_borrow_mut(|readers| match readers.get_mut(&(bucket_id, segment)) {
                Some(reader) => {
                    let res = reader.read_record(offset, false);
                    let panic_res = catch_unwind(AssertUnwindSafe(move || handler(res)));
                    if let Err(err) = panic_res {
                        error!("reader panicked: {err:?}");
                    }
                }
                None => {
                    error!(
                        "spawn_read_record: unknown (bucket_id: {}, segment: {})",
                        bucket_id, segment
                    );
                }
            });
        });
    }
}
