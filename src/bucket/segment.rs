mod reader;
mod writer;

use std::{
    mem,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use uuid::Uuid;

pub use self::reader::{
    BucketSegmentIter, BucketSegmentReader, CommitRecord, CommittedEvents, CommittedEventsIntoIter,
    EventRecord, Record,
};
pub use self::writer::{AppendEvent, AppendEventBody, AppendEventHeader, BucketSegmentWriter};

use super::{BucketId, flusher::FlushSender};

// Segment header constants
const MAGIC_BYTES: u32 = 0x53545645; // EVTS
const MAGIC_BYTES_SIZE: usize = mem::size_of::<u32>();
const VERSION_SIZE: usize = mem::size_of::<u16>();
const BUCKET_ID_SIZE: usize = mem::size_of::<u16>();
const CREATED_AT_SIZE: usize = mem::size_of::<u64>();
const PADDING_SIZE: usize = 32; // For adding headers in future versions
pub const SEGMENT_HEADER_SIZE: usize =
    MAGIC_BYTES_SIZE + VERSION_SIZE + BUCKET_ID_SIZE + CREATED_AT_SIZE + PADDING_SIZE;

// Record sizes
pub const RECORD_HEADER_SIZE: usize = mem::size_of::<Uuid>() // Event ID
        + mem::size_of::<Uuid>() // Transaction ID
        + mem::size_of::<u64>() // Timestamp nanoseconds
        + mem::size_of::<u32>(); // CRC32C hash
pub const EVENT_HEADER_SIZE: usize = RECORD_HEADER_SIZE
    + mem::size_of::<u64>() // Stream version
    + mem::size_of::<Uuid>() // Partition key
    + mem::size_of::<u16>() // Stream ID length
    + mem::size_of::<u16>() // Event name length
    + mem::size_of::<u32>() // Metadata length
    + mem::size_of::<u32>(); // Payload length
pub const COMMIT_SIZE: usize = RECORD_HEADER_SIZE + mem::size_of::<u32>(); // Event count

pub type WriteFn = Box<dyn FnOnce(&mut BucketSegmentWriter) + Send>;

// /// The latest segment, with write access.
// #[derive(Debug)]
// pub struct OpenBucketSegment {
//     id: BucketSegmentId,
//     reader: BucketSegmentReader,
//     pub reader_pool: ReaderThreadPool,
//     // writer_tx: Sender<WriteFn>,
//     writer: BucketSegmentWriter,
// }

// impl OpenBucketSegment {
//     pub fn new(
//         id: BucketSegmentId,
//         reader: BucketSegmentReader,
//         reader_pool: ReaderThreadPool,
//         writer: BucketSegmentWriter,
//     ) -> Self {
//         // Register the bucket segment in the reader thread pool
//         reader_pool.add_bucket_segment(id, &reader);

//         // // Spawn background thread for writer
//         // let (writer_tx, writer_rx) = bounded::<WriteFn>(1024);
//         // thread::spawn(move || {
//         //     while let Ok(handler) = writer_rx.recv() {
//         //         handler(&mut writer);
//         //     }
//         // });

//         OpenBucketSegment {
//             id,
//             reader,
//             reader_pool,
//             // writer_tx,
//             writer,
//         }
//     }

//     /// Closes the open bucket segment, dropping the writer.
//     pub fn close(self) -> ClosedBucketSegment {
//         ClosedBucketSegment {
//             id: self.id,
//             reader: self.reader,
//             reader_pool: self.reader_pool,
//         }
//     }

//     /// Reads a record from the specified bucket and segment at the given offset.
//     /// This method executes synchronously using the thread pool and returns an owned record.
//     ///
//     /// # Arguments
//     /// - `offset`: The offset within the segment.
//     ///
//     /// # Returns
//     /// - `Ok(Some(Record<'static>))` if the record is found.
//     /// - `Ok(None)` if the record does not exist.
//     /// - `Err(io::Error)` if an error occurs during reading.
//     pub fn read_record(&self, offset: u64) -> io::Result<Option<Record<'static>>> {
//         self.reader_pool.read_record(self.id, offset)
//     }

//     /// Reads a record and passes the result to the provided handler function.
//     /// This method executes synchronously using the thread pool.
//     ///
//     /// # Arguments
//     /// - `offset`: The offset within the segment.
//     /// - `handler`: A closure that processes the read result.
//     ///
//     /// # Returns
//     /// - The return value of the `handler` function, wrapped in `Some`.
//     /// - `None` if the segment is unknown or if the handler panics.
//     pub fn read_record_with<R>(
//         &self,
//         offset: u64,
//         handler: impl for<'a> FnOnce(io::Result<Option<Record<'a>>>) -> R + Send + 'static,
//     ) -> Option<R>
//     where
//         R: Send,
//     {
//         self.reader_pool.read_record_with(self.id, offset, handler)
//     }

//     /// Spawns an asynchronous task to read a record and process it using the given handler function.
//     /// This method does not block and executes the handler in a separate thread.
//     ///
//     /// # Arguments
//     /// - `offset`: The offset within the segment.
//     /// - `handler`: A closure that processes the read result.
//     ///
//     /// # Notes
//     /// - If the handler panics, the error is logged.
//     /// - If the segment is unknown, an error is logged.
//     pub fn spawn_read_record(
//         &self,
//         offset: u64,
//         handler: impl for<'a> FnOnce(io::Result<Option<Record<'a>>>) + Send + 'static,
//     ) {
//         self.reader_pool.spawn_read_record(self.id, offset, handler)
//     }

//     /// Returns a dedicated reader useful for sequential reads.
//     #[inline]
//     pub fn sequential_reader(&self) -> io::Result<BucketSegmentReader> {
//         self.reader.try_clone()
//     }

//     pub fn writer(&mut self) -> &mut BucketSegmentWriter {
//         &mut self.writer
//     }

//     // /// Submits work to the writer thread.
//     // pub fn with_writer<F>(&self, f: F) -> Result<(), SendError<WriteFn>>
//     // where
//     //     F: FnOnce(&mut BucketSegmentWriter) + Send + 'static,
//     // {
//     //     self.writer_tx.send(Box::new(f))
//     // }
// }

// /// An immutable segment which is closed and cannot be written to.
// #[derive(Debug)]
// pub struct ClosedBucketSegment {
//     id: BucketSegmentId,
//     reader: BucketSegmentReader,
//     reader_pool: ReaderThreadPool,
// }

// impl ClosedBucketSegment {
//     pub fn new(
//         id: BucketSegmentId,
//         reader: BucketSegmentReader,
//         reader_pool: ReaderThreadPool,
//     ) -> Self {
//         // Register the bucket segment in the reader thread pool
//         reader_pool.add_bucket_segment(id, &reader);

//         ClosedBucketSegment {
//             id,
//             reader,
//             reader_pool,
//         }
//     }

//     /// Reads a record from the specified bucket and segment at the given offset.
//     /// This method executes synchronously using the thread pool and returns an owned record.
//     ///
//     /// # Arguments
//     /// - `offset`: The offset within the segment.
//     ///
//     /// # Returns
//     /// - `Ok(Some(Record<'static>))` if the record is found.
//     /// - `Ok(None)` if the record does not exist.
//     /// - `Err(io::Error)` if an error occurs during reading.
//     pub fn read_record(&self, offset: u64) -> io::Result<Option<Record<'static>>> {
//         self.reader_pool.read_record(self.id, offset)
//     }

//     /// Reads a record and passes the result to the provided handler function.
//     /// This method executes synchronously using the thread pool.
//     ///
//     /// # Arguments
//     /// - `offset`: The offset within the segment.
//     /// - `handler`: A closure that processes the read result.
//     ///
//     /// # Returns
//     /// - The return value of the `handler` function, wrapped in `Some`.
//     /// - `None` if the segment is unknown or if the handler panics.
//     pub fn read_record_with<R>(
//         &self,
//         offset: u64,
//         handler: impl for<'a> FnOnce(io::Result<Option<Record<'a>>>) -> R + Send + 'static,
//     ) -> Option<R>
//     where
//         R: Send,
//     {
//         self.reader_pool.read_record_with(self.id, offset, handler)
//     }

//     /// Spawns an asynchronous task to read a record and process it using the given handler function.
//     /// This method does not block and executes the handler in a separate thread.
//     ///
//     /// # Arguments
//     /// - `offset`: The offset within the segment.
//     /// - `handler`: A closure that processes the read result.
//     ///
//     /// # Notes
//     /// - If the handler panics, the error is logged.
//     /// - If the segment is unknown, an error is logged.
//     pub fn spawn_read_record(
//         &self,
//         offset: u64,
//         handler: impl for<'a> FnOnce(io::Result<Option<Record<'a>>>) + Send + 'static,
//     ) {
//         self.reader_pool.spawn_read_record(self.id, offset, handler)
//     }

//     /// Returns a dedicated reader useful for sequential reads.
//     #[inline]
//     pub fn sequential_reader(&self) -> io::Result<BucketSegmentReader> {
//         self.reader.try_clone()
//     }
// }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BucketSegmentHeader {
    version: u16,
    bucket_id: BucketId,
    created_at: u64,
}

#[derive(Clone, Debug)]
pub struct FlushedOffset(Arc<AtomicU64>);

impl FlushedOffset {
    pub fn new(offset: Arc<AtomicU64>) -> Self {
        FlushedOffset(offset)
    }

    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

#[must_use]
#[allow(clippy::too_many_arguments)]
fn calculate_event_crc32c(
    event_id: &Uuid,
    transaction_id: &Uuid,
    timestamp: u64,
    stream_version: u64,
    partition_key: &Uuid,
    stream_id: &[u8],
    event_name: &[u8],
    metadata: &[u8],
    payload: &[u8],
) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(event_id.as_bytes());
    hasher.update(transaction_id.as_bytes());
    hasher.update(&(timestamp & !0b1).to_le_bytes());
    hasher.update(&stream_version.to_le_bytes());
    hasher.update(partition_key.as_bytes());
    hasher.update(stream_id);
    hasher.update(event_name);
    hasher.update(metadata);
    hasher.update(payload);
    hasher.finalize()
}

#[must_use]
fn calculate_commit_crc32c(transaction_id: &Uuid, timestamp: u64, event_count: u32) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(transaction_id.as_bytes());
    hasher.update(&(timestamp | 0b1).to_le_bytes());
    hasher.update(&event_count.to_le_bytes());
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rand::rng;
    use rand::seq::SliceRandom;
    use uuid::Uuid;

    use super::*;

    fn temp_file_path() -> PathBuf {
        tempfile::Builder::new()
            .make(|path| Ok(path.to_path_buf()))
            .unwrap()
            .path()
            .to_path_buf()
    }

    #[test]
    fn test_write_and_read_segment_header() {
        let path = temp_file_path();

        let header = BucketSegmentHeader {
            version: 1,
            bucket_id: 42,
            created_at: 1700000000,
        };

        {
            let mut writer = BucketSegmentWriter::create(&path, 0, FlushSender::local()).unwrap();
            writer.write_segment_header(&header).unwrap();
            writer.flush().unwrap();
        }

        let mut reader = BucketSegmentReader::open(
            &path,
            FlushedOffset::new(Arc::new(AtomicU64::new(u64::MAX))),
        )
        .unwrap();
        let read_header = reader.read_segment_header().unwrap();

        assert_eq!(header, read_header);
    }

    #[test]
    fn test_validate_magic_bytes() {
        let path = temp_file_path();

        let header = BucketSegmentHeader {
            version: 1,
            bucket_id: 42,
            created_at: 1700000000,
        };

        {
            let mut writer = BucketSegmentWriter::create(&path, 0, FlushSender::local()).unwrap();
            writer.write_segment_header(&header).unwrap();
            writer.flush().unwrap();
        }

        let mut reader = BucketSegmentReader::open(
            &path,
            FlushedOffset::new(Arc::new(AtomicU64::new(u64::MAX))),
        )
        .unwrap();
        assert!(reader.validate_magic_bytes().unwrap());
    }

    #[test]
    fn test_write_and_read_record() {
        let path = temp_file_path();

        let event_id = Uuid::new_v4();
        let transaction_id = Uuid::new_v4();
        let partition_key = Uuid::new_v4();
        let timestamp = 1700000000;
        let stream_version = 1;
        let stream_id = "test_stream";
        let event_name = "TestEvent";
        let metadata = b"{}";
        let payload = b"payload_data";

        let mut offsets = Vec::with_capacity(10_000);
        let flushed_offset;

        {
            let mut writer = BucketSegmentWriter::create(&path, 0, FlushSender::local()).unwrap();

            for i in 0..10_000 {
                if i % 5 == 0 {
                    let (offset, _) = writer.append_commit(&transaction_id, timestamp, 1).unwrap();
                    offsets.push((1u8, offset));
                } else {
                    let body = AppendEventBody::new(stream_id, event_name, metadata, payload);
                    let header = AppendEventHeader::new(
                        &event_id,
                        &partition_key,
                        &transaction_id,
                        stream_version,
                        timestamp,
                        body,
                    )
                    .unwrap();
                    let (offset, _) = writer.append_event(AppendEvent::new(header, body)).unwrap();
                    offsets.push((0u8, offset));
                }
            }

            writer.flush().unwrap();
            flushed_offset = writer.flushed_offset();
        }

        offsets.shuffle(&mut rng());

        let mut reader = BucketSegmentReader::open(&path, flushed_offset.clone()).unwrap();

        for (i, (kind, offset)) in offsets.into_iter().enumerate() {
            let record = reader.read_record(offset, i % 2 == 0).unwrap().unwrap();
            match record {
                Record::Event(EventRecord {
                    offset: _,
                    event_id: rid,
                    partition_key: rpk,
                    transaction_id: rtid,
                    stream_version: rsv,
                    timestamp: rts,
                    stream_id: rsid,
                    event_name: ren,
                    metadata: rm,
                    payload: rp,
                }) => {
                    assert_eq!(kind, 0);
                    assert_eq!(rid, event_id);
                    assert_eq!(rpk, partition_key);
                    assert_eq!(rtid, transaction_id);
                    assert_eq!(rsv, stream_version);
                    assert_eq!(rts, timestamp);
                    assert_eq!(rsid, stream_id);
                    assert_eq!(ren, event_name);
                    assert_eq!(rm.as_ref(), metadata);
                    assert_eq!(rp.as_ref(), payload);
                }
                Record::Commit(CommitRecord {
                    offset: _,
                    transaction_id: rtid,
                    timestamp: rts,
                    event_count,
                }) => {
                    assert_eq!(kind, 1);
                    assert_eq!(rtid, transaction_id);
                    assert_eq!(rts & !0b1, timestamp);
                    assert_eq!(event_count, 1);
                }
            }
        }
    }
}
