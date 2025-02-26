use std::{
    collections::{BTreeMap, HashMap, VecDeque, btree_map::Entry},
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
    os::unix::fs::FileExt,
    panic::panic_any,
    path::Path,
    sync::{
        Arc, Weak,
        atomic::{AtomicU32, Ordering},
    },
};

use bloomfilter::Bloom;
use rayon::ThreadPool;
use tokio::sync::oneshot;
use tracing::{error, warn};
use uuid::Uuid;

use crate::{
    BLOOM_SEED, RANDOM_STATE, bucket::segment::CommittedEvents, copy_bytes, error::ThreadPoolError,
    from_bytes,
};

use super::{
    BucketId, BucketSegmentId, SegmentId,
    reader_thread_pool::ReaderThreadPool,
    segment::{BucketSegmentReader, EventRecord, Record},
    writer_thread_pool::LiveIndexes,
};

const STREAM_ID_SIZE: usize = 64;
const VERSION_SIZE: usize = mem::size_of::<u64>();
const CORRELATION_ID_SIZE: usize = mem::size_of::<Uuid>();
const OFFSET_SIZE: usize = mem::size_of::<u64>();
const LEN_SIZE: usize = mem::size_of::<u32>();
// Stream ID, version min, version max, correlation id, offset, len
const RECORD_SIZE: usize =
    STREAM_ID_SIZE + VERSION_SIZE + VERSION_SIZE + CORRELATION_ID_SIZE + OFFSET_SIZE + LEN_SIZE;

const AVG_EVENT_SIZE: usize = 350;
const AVG_EVENTS_PER_STREAM: usize = 10;
const FALSE_POSITIVE_PROBABILITY: f64 = 0.001;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamIndexRecord<T> {
    pub version_min: u64,
    pub version_max: u64,
    pub correlation_id: Uuid,
    offsets: T,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OffsetKind {
    Pointer(u64, u32), // Its in the file at this location
    Cached(Vec<u64>),  // Its cached
}

pub struct OpenStreamIndex {
    id: BucketSegmentId,
    file: File,
    index: BTreeMap<Arc<str>, StreamIndexRecord<Vec<u64>>>,
    bloom: Bloom<str>,
}

impl OpenStreamIndex {
    pub fn create(
        id: BucketSegmentId,
        path: impl AsRef<Path>,
        segment_size: usize,
    ) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let index = BTreeMap::new();
        let bloom = Bloom::new_for_fp_rate_with_seed(
            (segment_size / AVG_EVENT_SIZE / AVG_EVENTS_PER_STREAM).max(1),
            FALSE_POSITIVE_PROBABILITY,
            &BLOOM_SEED,
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

        Ok(OpenStreamIndex {
            id,
            file,
            index,
            bloom,
        })
    }

    pub fn open(
        id: BucketSegmentId,
        path: impl AsRef<Path>,
        segment_size: usize,
    ) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let (index, bloom) = load_index_from_file(&mut file, segment_size)?;

        Ok(OpenStreamIndex {
            id,
            file,
            index,
            bloom,
        })
    }

    /// Closes the stream index, flushing the index in a background thread.
    pub fn close(self, pool: &ThreadPool) -> io::Result<ClosedStreamIndex> {
        let id = self.id;
        let mut file_clone = self.file.try_clone()?;
        let num_slots = self.num_slots();
        let strong_index = Arc::new(self.index);
        let weak_index = Arc::downgrade(&strong_index);
        let bloom = Arc::new(self.bloom);

        pool.spawn({
            let bloom = Arc::clone(&bloom);
            move || {
                if let Err(err) =
                    Self::flush_inner(&mut file_clone, &strong_index, &bloom, num_slots, |_, _| {
                        true
                    })
                {
                    panic_any(ThreadPoolError::FlushStreamIndex {
                        id,
                        file: file_clone,
                        index: strong_index,
                        num_slots: num_slots as u64,
                        err,
                    });
                }
            }
        });

        Ok(ClosedStreamIndex {
            id,
            file: self.file,
            num_slots: num_slots as u64,
            index: Arc::new(weak_index),
            bloom,
        })
    }

    // /// Closes the stream index, flushing the index in a background thread.
    // pub fn close(
    //     self,
    //     pool: &ThreadPool,
    //     committed_event_offsets: Arc<HashSet<u64>>,
    // ) -> io::Result<ClosedStreamIndex> {
    //     let id = self.id;
    //     let mut file_clone = self.file.try_clone()?;
    //     let num_slots = self.num_slots();
    //     let strong_index = Arc::new(self.index);
    //     let weak_index = Arc::downgrade(&strong_index);
    //     let bloom = Arc::new(self.bloom);

    //     pool.spawn({
    //         let bloom = Arc::clone(&bloom);
    //         move || {
    //             if let Err(err) = Self::flush_inner(
    //                 &mut file_clone,
    //                 &strong_index,
    //                 &bloom,
    //                 num_slots,
    //                 |_, offset| committed_event_offsets.contains(offset),
    //             ) {
    //                 panic_any(ThreadPoolError::FlushStreamIndex {
    //                     id,
    //                     file: file_clone,
    //                     index: strong_index,
    //                     num_slots: num_slots as u64,
    //                     err,
    //                 });
    //             }
    //         }
    //     });

    //     Ok(ClosedStreamIndex {
    //         id,
    //         file: self.file,
    //         num_slots: num_slots as u64,
    //         index: Arc::new(weak_index),
    //         bloom,
    //     })
    // }

    pub fn get(&self, stream_id: &str) -> Option<&StreamIndexRecord<Vec<u64>>> {
        self.index.get(stream_id)
    }

    pub fn insert(
        &mut self,
        stream_id: impl Into<Arc<str>>,
        stream_version: u64,
        correlation_id: Uuid,
        offset: u64,
    ) -> io::Result<()> {
        let stream_id = stream_id.into();
        if stream_id.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream id is empty",
            ));
        }
        if stream_id.len() > STREAM_ID_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream id exceeds max len",
            ));
        }

        match self.index.entry(stream_id) {
            Entry::Vacant(entry) => {
                self.bloom.set(entry.key());
                entry.insert(StreamIndexRecord {
                    version_min: stream_version,
                    version_max: stream_version,
                    correlation_id,
                    offsets: vec![offset],
                });
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                if entry.correlation_id != correlation_id {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "correlation id mismatch in stream index",
                    ));
                }
                entry.version_min = entry.version_min.min(stream_version);
                entry.version_max = entry.version_max.max(stream_version);
                entry.offsets.push(offset);
            }
        }

        Ok(())
    }

    // pub fn retain<F>(&mut self, f: F)
    // where
    //     F: FnMut(&Arc<str>, &mut StreamIndexEntry) -> bool,
    // {
    //     self.index.retain(f);
    // }

    // pub fn retain_offsets<F>(&mut self, f: F)
    // where
    //     F: FnMut(&u64) -> bool + Copy,
    // {
    //     for offsets in self.index.values_mut() {
    //         offsets.retain(f)
    //     }
    // }

    pub fn flush(&mut self) -> io::Result<()> {
        let num_slots = self.num_slots();
        Self::flush_inner(
            &mut self.file,
            &self.index,
            &self.bloom,
            num_slots,
            |_, _| true,
        )
    }

    /// Hydrates the index from a reader.
    pub fn hydrate(&mut self, reader: &mut BucketSegmentReader) -> io::Result<()> {
        let mut reader_iter = reader.iter();
        while let Some(record) = reader_iter.next_record()? {
            match record {
                Record::Event(EventRecord {
                    offset,
                    stream_id,
                    stream_version,
                    correlation_id,
                    ..
                }) => {
                    self.insert(
                        stream_id.into_owned(),
                        stream_version,
                        correlation_id,
                        offset,
                    )?;
                }
                Record::Commit(_) => {}
            }
        }

        Ok(())
    }

    fn flush_inner<F>(
        file: &mut File,
        index: &BTreeMap<Arc<str>, StreamIndexRecord<Vec<u64>>>,
        bloom: &Bloom<str>,
        num_slots: usize,
        mut filter: F,
    ) -> io::Result<()>
    where
        F: FnMut(&Arc<str>, &u64) -> bool,
    {
        let bloom_bytes = bloom.to_bytes();

        let file_size = (mem::size_of::<u64>() + mem::size_of::<u64>())
            .checked_add(bloom_bytes.len())
            .unwrap()
            .checked_add(num_slots * RECORD_SIZE)
            .unwrap();
        let mut file_data = vec![0u8; file_size];
        let mut offset = file_size as u64;

        let mut value_data = Vec::new();

        copy_bytes!(file_data, [
            &(num_slots as u64).to_le_bytes(),
            &(bloom_bytes.len() as u64).to_le_bytes(),
            &bloom_bytes,
        ]);

        for (stream_id, record) in index {
            let offsets_bytes: Vec<_> = record
                .offsets
                .iter()
                .filter(|offset| filter(stream_id, offset))
                .flat_map(|v| v.to_le_bytes())
                .collect();
            if offsets_bytes.is_empty() {
                continue;
            }

            let mut slot = RANDOM_STATE.hash_one(stream_id) % num_slots as u64;
            let offsets_len = record.offsets.len() as u32;

            loop {
                let mut pos = mem::size_of::<u64>()
                    + mem::size_of::<u64>()
                    + bloom_bytes.len()
                    + (slot * RECORD_SIZE as u64) as usize;
                let existing_key = &file_data[pos..pos + STREAM_ID_SIZE];

                if existing_key.iter().all(|&b| b == 0) {
                    copy_bytes!(
                        file_data,
                        pos,
                        [
                            stream_id.as_bytes() => stream_id.len(); + STREAM_ID_SIZE,
                            &record.version_min.to_le_bytes() => VERSION_SIZE,
                            &record.version_max.to_le_bytes() => VERSION_SIZE,
                            record.correlation_id.as_bytes() => CORRELATION_ID_SIZE,
                            &offset.to_le_bytes() => OFFSET_SIZE,
                            &offsets_len.to_le_bytes() => LEN_SIZE,
                        ]
                    );

                    break;
                }

                slot = (slot + 1) % num_slots as u64;
            }

            value_data.extend(offsets_bytes);
            offset += (record.offsets.len() * 8) as u64;
        }

        file.write_all_at(&file_data, 0)?;
        file.write_all_at(&value_data, file_size as u64)?;
        file.flush()
    }

    #[inline]
    fn num_slots(&self) -> usize {
        self.index.len() * 2
    }
}

pub struct ClosedStreamIndex {
    #[allow(unused)] // TODO: is this ID needed?
    id: BucketSegmentId,
    file: File,
    num_slots: u64,
    #[allow(clippy::type_complexity)]
    index: Arc<Weak<BTreeMap<Arc<str>, StreamIndexRecord<Vec<u64>>>>>,
    bloom: Arc<Bloom<str>>,
}

impl ClosedStreamIndex {
    pub fn open(
        id: BucketSegmentId,
        path: impl AsRef<Path>,
        segment_size: usize,
    ) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut count_buf = [0u8; 8];
        file.read_exact(&mut count_buf)?;
        let num_slots = u64::from_le_bytes(count_buf);

        let bloom = load_bloom_from_file(&mut file, segment_size)?;

        Ok(ClosedStreamIndex {
            id,
            file,
            num_slots,
            index: Arc::new(Weak::new()),
            bloom: Arc::new(bloom),
        })
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(ClosedStreamIndex {
            id: self.id,
            file: self.file.try_clone()?,
            num_slots: self.num_slots,
            index: Arc::clone(&self.index),
            bloom: Arc::clone(&self.bloom),
        })
    }

    pub fn get_key(&self, stream_id: &str) -> io::Result<Option<StreamIndexRecord<OffsetKind>>> {
        // Read from memory.
        // This code should only run when the segment is being closed and is being flushed in the background.
        if let Some(index) = self.index.upgrade() {
            return Ok(index
                .get(stream_id)
                .cloned()
                .map(|record| StreamIndexRecord {
                    version_min: record.version_min,
                    version_max: record.version_max,
                    correlation_id: record.correlation_id,
                    offsets: OffsetKind::Cached(record.offsets),
                }));
        }

        if self.num_slots == 0 {
            return Ok(None);
        }

        if !self.bloom.check(stream_id) {
            return Ok(None);
        }

        // Compute slot index
        let mut slot = RANDOM_STATE.hash_one(stream_id) % self.num_slots;

        let mut read_buf = [0u8; RECORD_SIZE * 2];
        let mut buf: &[u8] = &[];

        // Try to find the key using linear probing
        for _ in 0..self.num_slots {
            if buf.len() < RECORD_SIZE {
                let mut pos =
                    8 + 8 + bloom_bytes_len(&self.bloom) as u64 + slot * RECORD_SIZE as u64;
                let mut read = 0;

                while read < RECORD_SIZE * 2 {
                    let n = self.file.read_at(&mut read_buf[read..], pos)?;
                    pos += n as u64;
                    read += n;
                    if n == 0 {
                        break;
                    }
                }

                if read == 0 {
                    return Ok(None);
                }

                buf = &read_buf[..read];
            }

            let mut pos = 0;

            let stored_stream_id = std::str::from_utf8(&buf[pos..pos + STREAM_ID_SIZE])
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF8 stream id"))?
                .trim_end_matches('\0');
            pos += STREAM_ID_SIZE;

            if stored_stream_id.is_empty() {
                return Ok(None);
            }

            if stored_stream_id == stream_id {
                let (version_min, version_max, correlation_id, offset, len) =
                    from_bytes!(buf, pos, [u64, u64, Uuid, u64, u32]);

                return Ok(Some(StreamIndexRecord {
                    version_min,
                    version_max,
                    correlation_id,
                    offsets: OffsetKind::Pointer(offset, len),
                }));
            }

            // Collision: check next slot
            slot = (slot + 1) % self.num_slots;

            // Slide the buf across if there's length
            if buf.len() >= RECORD_SIZE * 2 {
                buf = &buf[RECORD_SIZE..];
            } else {
                buf = &[];
            }
        }

        Ok(None)
    }

    pub fn get_from_key(&self, key: StreamIndexRecord<OffsetKind>) -> io::Result<Vec<u64>> {
        match key.offsets {
            OffsetKind::Pointer(offset, len) => {
                let mut values_buf = vec![0u8; len as usize * 8];
                self.file.read_exact_at(&mut values_buf, offset)?;
                let offsets = values_buf
                    .chunks_exact(8)
                    .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                    .collect();
                Ok(offsets)
            }
            OffsetKind::Cached(offsets) => Ok(offsets),
        }
    }

    pub fn get(&self, stream_id: &str) -> io::Result<Vec<u64>> {
        Ok(self
            .get_key(stream_id)
            .and_then(|key| key.map(|key| self.get_from_key(key)).transpose())?
            .unwrap_or_default())
    }
}

#[derive(Debug)]
pub struct EventStreamIter {
    stream_id: Arc<str>,
    bucket_id: BucketId,
    reader_pool: ReaderThreadPool,
    segment_id: SegmentId,
    segment_offsets: VecDeque<u64>,
    live_segment_id: SegmentId,
    live_segment_offsets: VecDeque<u64>,
    next_offset: Option<NextOffset>,
    next_live_offset: Option<NextOffset>,
}

#[derive(Clone, Copy, Debug)]
struct NextOffset {
    offset: u64,
    segment_id: SegmentId,
}

impl EventStreamIter {
    #[allow(clippy::type_complexity)]
    pub(crate) async fn new(
        stream_id: Arc<str>,
        bucket_id: BucketId,
        reader_pool: ReaderThreadPool,
        live_indexes: &HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>,
    ) -> io::Result<Self> {
        let mut live_segment_id = 0;
        let mut live_segment_offsets: VecDeque<_> = match live_indexes.get(&bucket_id) {
            Some((current_live_segment_id, live_indexes)) => {
                let current_live_segment_id = current_live_segment_id.load(Ordering::Acquire);
                live_segment_id = current_live_segment_id;
                match live_indexes.read().await.1.get(&stream_id).cloned() {
                    Some(index) if index.version_min == 0 => {
                        let mut live_segment_offsets: VecDeque<_> = index.offsets.into();
                        let next_live_offset =
                            live_segment_offsets.pop_front().map(|offset| NextOffset {
                                offset,
                                segment_id: current_live_segment_id,
                            });
                        return Ok(EventStreamIter {
                            stream_id,
                            bucket_id,
                            reader_pool,
                            segment_id: current_live_segment_id,
                            segment_offsets: VecDeque::new(),
                            live_segment_id: current_live_segment_id,
                            live_segment_offsets,
                            next_offset: None,
                            next_live_offset,
                        });
                    }
                    Some(index) => Some(index.offsets.into()),
                    None => None,
                }
            }
            None => {
                warn!("live index doesn't contain this bucket");
                None
            }
        }
        .unwrap_or_default();

        let (reply_tx, reply_rx) = oneshot::channel();
        reader_pool.spawn({
            let stream_id = Arc::clone(&stream_id);
            move |with_readers| {
                with_readers(move |readers| {
                    let res = readers
                        .get_mut(&bucket_id)
                        .and_then(|segments| {
                            segments.iter().enumerate().rev().find_map(
                                |(i, (segment_id, reader_set))| {
                                    let Some(stream_index) = &reader_set.stream_index else {
                                        return None;
                                    };

                                    match stream_index.get_key(&stream_id) {
                                        Ok(Some(key)) if key.version_min == 0 || i == 0 => {
                                            match stream_index.get_from_key(key) {
                                                Ok(offsets) => Some(Ok((*segment_id, offsets))),
                                                Err(err) => Some(Err(err)),
                                            }
                                        }
                                        Ok(_) => None,
                                        Err(err) => Some(Err(err)),
                                    }
                                },
                            )
                        })
                        .transpose();
                    let _ = reply_tx.send(res);
                });
            }
        });

        match reply_rx.await {
            Ok(Ok(Some((segment_id, segment_offsets)))) => {
                let mut segment_offsets: VecDeque<_> = segment_offsets.into();
                let next_offset = segment_offsets
                    .pop_front()
                    .map(|offset| NextOffset { offset, segment_id });
                let next_live_offset = live_segment_offsets.pop_front().map(|offset| NextOffset {
                    offset,
                    segment_id: live_segment_id,
                });

                Ok(EventStreamIter {
                    stream_id,
                    bucket_id,
                    reader_pool,
                    segment_id,
                    segment_offsets,
                    live_segment_id,
                    live_segment_offsets,
                    next_offset,
                    next_live_offset,
                })
            }
            Ok(Ok(None)) | Err(_) => {
                let next_live_offset = live_segment_offsets.pop_front().map(|offset| NextOffset {
                    offset,
                    segment_id: live_segment_id,
                });

                Ok(EventStreamIter {
                    stream_id,
                    bucket_id,
                    reader_pool,
                    segment_id: 0,
                    segment_offsets: VecDeque::new(),
                    live_segment_id,
                    live_segment_offsets,
                    next_offset: None,
                    next_live_offset,
                })
            }
            Ok(Err(err)) => Err(err),
        }
    }

    pub async fn next(&mut self) -> io::Result<Option<EventRecord<'static>>> {
        struct ReadResult {
            events: Option<CommittedEvents<'static>>,
            new_offsets: Option<(SegmentId, VecDeque<u64>)>,
            is_live: bool,
        }

        let stream_id = Arc::clone(&self.stream_id);
        let bucket_id = self.bucket_id;
        let segment_id = self.segment_id;
        let live_segment_id = self.live_segment_id;
        let next_offset = self.next_offset;
        let next_live_offset = self.next_live_offset;

        // if next_offset.is_none() && next_live_offset.is_none() && segment_id == live_segment_id {
        //     return Ok(None);
        // }

        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| {
                let res = readers
                    .get_mut(&bucket_id)
                    .map(|segments| {
                        match next_offset {
                            Some(NextOffset { offset, segment_id }) => {
                                // We have an offset from the last batch
                                match segments.get_mut(&segment_id) {
                                    Some(reader_set) => Ok(ReadResult {
                                        events: reader_set
                                            .reader
                                            .read_committed_events(offset, false)?
                                            .map(CommittedEvents::into_owned),
                                        new_offsets: None,
                                        is_live: false,
                                    }),
                                    None => Err(io::Error::new(
                                        io::ErrorKind::NotFound,
                                        "segment not found",
                                    )),
                                }
                            }
                            None => {
                                // There's no more offsets in this batch, progress forwards finding the next batch
                                for i in segment_id.saturating_add(1)
                                    ..(segments.len() as SegmentId).min(live_segment_id)
                                {
                                    let Some(reader_set) = segments.get_mut(&i) else {
                                        continue;
                                    };

                                    let Some(stream_index) = &reader_set.stream_index else {
                                        continue;
                                    };

                                    let mut new_offsets: VecDeque<_> =
                                        stream_index.get(&stream_id)?.into();
                                    let Some(next_offset) = new_offsets.pop_front() else {
                                        continue;
                                    };

                                    return Ok(ReadResult {
                                        events: reader_set
                                            .reader
                                            .read_committed_events(next_offset, false)?
                                            .map(CommittedEvents::into_owned),
                                        new_offsets: Some((i, new_offsets)),
                                        is_live: false,
                                    });
                                }

                                // No more batches found, we'll process the live offsets
                                match next_live_offset {
                                    Some(NextOffset { offset, segment_id }) => {
                                        let Some(reader_set) = segments.get_mut(&segment_id) else {
                                            return Ok(ReadResult {
                                                events: None,
                                                new_offsets: None,
                                                is_live: true,
                                            });
                                        };

                                        Ok(ReadResult {
                                            events: reader_set
                                                .reader
                                                .read_committed_events(offset, false)?
                                                .map(CommittedEvents::into_owned),
                                            new_offsets: None,
                                            is_live: true,
                                        })
                                    }
                                    None => Ok(ReadResult {
                                        events: None,
                                        new_offsets: None,
                                        is_live: true,
                                    }),
                                }
                            }
                        }
                    })
                    .transpose();
                let _ = reply_tx.send(res);
            });
        });

        match reply_rx.await {
            Ok(Ok(Some(ReadResult {
                events,
                new_offsets,
                is_live,
            }))) => {
                if is_live {
                    self.segment_id = self.live_segment_id;
                    self.next_live_offset =
                        self.live_segment_offsets
                            .pop_front()
                            .map(|offset| NextOffset {
                                offset,
                                segment_id: self.live_segment_id,
                            });
                    return Ok(events.and_then(|events| events.into_iter().next()));
                }

                if let Some((new_segment, new_offsets)) = new_offsets {
                    self.segment_id = new_segment;
                    self.segment_offsets = new_offsets;
                    self.next_offset = self.segment_offsets.pop_front().map(|offset| NextOffset {
                        offset,
                        segment_id: self.segment_id,
                    });
                } else {
                    self.next_offset = self.segment_offsets.pop_front().map(|offset| NextOffset {
                        offset,
                        segment_id: self.segment_id,
                    });
                    // if self.segment_offsets.is_empty() {
                    //     self.segment_id = self.segment_id.saturating_add(1);
                    //     println!(
                    //         "segment offsets is empty, incrementing to {}",
                    //         self.segment_id
                    //     );
                    // }
                }

                Ok(events.and_then(|events| events.into_iter().next()))
            }
            Ok(Ok(None)) => Ok(None),
            Ok(Err(err)) => Err(err),
            Err(_) => {
                error!("no reply from reader pool");
                Ok(None)
            }
        }
    }

    // pub async fn next(&mut self) -> io::Result<Option<EventRecord<'static>>> {
    //     let stream_id = Arc::clone(&self.stream_id);
    //     let bucket_id = self.bucket_id;
    //     let segment_id = self.segment_id;
    //     let next_offset = self
    //         .segment_offsets
    //         .pop_front()
    //         .map(|offset| (self.segment_id, offset))
    //         .or_else(|| {
    //             if self.live_segment_id == Some(self.segment_id) {
    //                 self.live_segment_offsets
    //                     .pop_front()
    //                     .map(|offset| (self.live_segment_id.unwrap(), offset))
    //             } else {
    //                 None
    //             }
    //         });

    //     struct ReadResult {
    //         events: Option<CommittedEvents<'static>>,
    //         new_offsets: Option<VecDeque<u64>>,
    //         last_segment_id: u32,
    //     }

    //     let (reply_tx, reply_rx) = oneshot::channel();
    //     self.reader_pool.spawn(move |with_readers| {
    //         with_readers(move |readers| {
    //             let res = readers
    //                 .get_mut(&bucket_id)
    //                 .map(|segments| {
    //                     let last_segment_id =
    //                         segments.last_entry().map(|last| *last.key()).unwrap_or(0);
    //                     match next_offset {
    //                         Some((live_segment_id, next_offset)) => {
    //                             match segments.get_mut(&live_segment_id) {
    //                                 Some(reader_set) => Ok(ReadResult {
    //                                     events: reader_set
    //                                         .reader
    //                                         .read_committed_events(next_offset, false)?
    //                                         .map(CommittedEvents::into_owned),
    //                                     new_offsets: None,
    //                                     last_segment_id,
    //                                 }),
    //                                 None => {}
    //                             }
    //                         }
    //                         None => {
    //                             for i in segment_id..segments.len() as u32 {
    //                                 let Some(reader_set) = segments.get_mut(&i) else {
    //                                     continue;
    //                                 };

    //                                 let Some(stream_index) = &reader_set.stream_index else {
    //                                     continue;
    //                                 };

    //                                 let mut new_offsets: VecDeque<_> =
    //                                     stream_index.get(&stream_id)?.into();
    //                                 let Some(next_offset) = new_offsets.pop_front() else {
    //                                     continue;
    //                                 };

    //                                 return Ok(ReadResult {
    //                                     events: reader_set
    //                                         .reader
    //                                         .read_committed_events(next_offset, false)?
    //                                         .map(CommittedEvents::into_owned),
    //                                     new_offsets: Some(new_offsets),
    //                                     last_segment_id,
    //                                 });
    //                             }

    //                             Ok(ReadResult {
    //                                 events: None,
    //                                 new_offsets: None,
    //                                 last_segment_id,
    //                             })
    //                         }
    //                     }
    //                 })
    //                 .transpose();
    //             let _ = reply_tx.send(res);
    //         })
    //     });

    //     match reply_rx.await {
    //         Ok(Ok(Some(ReadResult {
    //             events,
    //             new_offsets,
    //             last_segment_id,
    //         }))) => {
    //             if let Some(new_offsets) = new_offsets {
    //                 // Only move onto the next segment if:
    //                 //  - self.segment_offsets is empty
    //                 //  - we didn't just read the last segment
    //                 if self.segment_offsets.is_empty() && self.segment_id < last_segment_id {
    //                     self.segment_id = self.segment_id.saturating_add(1);
    //                 }

    //                 self.segment_offsets = new_offsets;
    //             } else if self.segment_offsets.is_empty() {
    //                 self.segment_id = self.segment_id.saturating_add(1);
    //             }

    //             Ok(events.and_then(|events| events.into_iter().next()))
    //         }
    //         Ok(Ok(None)) => Ok(None),
    //         Ok(Err(err)) => Err(err),
    //         Err(_) => {
    //             error!("no reply from reader pool");
    //             Ok(None)
    //         }
    //     }
    // }
}

#[allow(clippy::type_complexity)]
fn load_index_from_file(
    file: &mut File,
    segment_size: usize,
) -> io::Result<(BTreeMap<Arc<str>, StreamIndexRecord<Vec<u64>>>, Bloom<str>)> {
    let mut file_data = Vec::with_capacity(file.metadata()?.len() as usize);
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut file_data)?;

    if file_data.is_empty() {
        let bloom = Bloom::new_for_fp_rate_with_seed(
            (segment_size / AVG_EVENT_SIZE / AVG_EVENTS_PER_STREAM).max(1),
            FALSE_POSITIVE_PROBABILITY,
            &BLOOM_SEED,
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        return Ok((BTreeMap::new(), bloom));
    }

    if file_data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "invalid stream index file",
        ));
    }

    let (num_slots, bloom_bytes) = {
        let mut pos = 0;
        let (num_slots, bloom_len) = from_bytes!(&file_data, pos, [u64, u64]);
        let bloom_bytes = from_bytes!(&file_data, pos, &[u8], bloom_len as usize);
        (num_slots as usize, bloom_bytes)
    };
    let bloom =
        Bloom::from_slice(bloom_bytes).map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

    let slot_section_size = num_slots * RECORD_SIZE;

    if file_data.len() < 8 + slot_section_size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "corrupted stream index file",
        ));
    }

    let mut index = BTreeMap::new();

    for i in 0..num_slots {
        let pos = 8 + 8 + bloom_bytes.len() + (i * RECORD_SIZE);

        if file_data.len() < pos + RECORD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "invalid stream index file",
            ));
        }

        let record_bytes = &file_data[pos..pos + RECORD_SIZE];
        let mut record_pos = 0;

        let stream_id = from_bytes!(record_bytes, record_pos, str, STREAM_ID_SIZE)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF8 stream id"))?
            .trim_end_matches('\0');

        if stream_id.is_empty() {
            continue; // Skip empty slots
        }

        let (version_min, version_max, correlation_id, offset, len) =
            from_bytes!(record_bytes, record_pos, [u64, u64, Uuid, u64, u32]);

        let value_start = offset as usize;
        let value_end = value_start + (len as usize * 8);
        if value_end > file_data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "invalid value offset or length",
            ));
        }

        let offsets = file_data[value_start..value_end]
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        index.insert(Arc::from(stream_id), StreamIndexRecord {
            version_min,
            version_max,
            correlation_id,
            offsets,
        });
    }

    Ok((index, bloom))
}

fn load_bloom_from_file(file: &mut File, segment_size: usize) -> io::Result<Bloom<str>> {
    let mut file_data = Vec::with_capacity(file.metadata()?.len() as usize);
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut file_data)?;

    if file_data.is_empty() {
        let bloom = Bloom::new_for_fp_rate_with_seed(
            segment_size / AVG_EVENT_SIZE / AVG_EVENTS_PER_STREAM,
            FALSE_POSITIVE_PROBABILITY,
            &BLOOM_SEED,
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        return Ok(bloom);
    }

    if file_data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "invalid stream index file",
        ));
    }

    let bloom_bytes = {
        let mut pos = 0;
        let (_num_slots, bloom_len) = from_bytes!(&file_data, pos, [u64, u64]);
        from_bytes!(&file_data, pos, &[u8], bloom_len as usize)
    };
    let bloom =
        Bloom::from_slice(bloom_bytes).map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

    Ok(bloom)
}

fn bloom_bytes_len<T: ?Sized>(bloom: &Bloom<T>) -> usize {
    ((bloom.len() / 8) + 45) as usize
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    const SEGMENT_SIZE: usize = 256_000_000; // 64 MB

    fn temp_file_path() -> PathBuf {
        tempfile::Builder::new()
            .make(|path| Ok(path.to_path_buf()))
            .unwrap()
            .path()
            .to_path_buf()
    }

    #[test]
    fn test_open_event_index_insert_and_get() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), path, SEGMENT_SIZE).unwrap();

        let stream_id = "stream-a";
        let correlation_id = Uuid::new_v4();
        let offsets = vec![42, 105];
        for (i, offset) in offsets.iter().enumerate() {
            index
                .insert(stream_id, i as u64, correlation_id, *offset)
                .unwrap();
        }

        assert_eq!(
            index.get(stream_id),
            Some(&StreamIndexRecord {
                version_min: 0,
                version_max: 1,
                correlation_id,
                offsets
            })
        );
        assert_eq!(index.get("unknown"), None);
    }

    #[test]
    fn test_open_event_index_flush_and_reopen() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        let stream_id1 = "stream-a";
        let stream_id2 = "stream-b";
        let correlation_id1 = Uuid::new_v4();
        let correlation_id2 = Uuid::new_v4();
        let offsets1 = vec![1111, 2222];
        let offsets2 = vec![3333];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(stream_id1, i as u64, correlation_id1, *offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(stream_id2, i as u64, correlation_id2, *offset)
                .unwrap();
        }
        index.flush().unwrap();

        // Reopen and verify data is still present
        let reopened_index =
            OpenStreamIndex::open(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        assert_eq!(
            reopened_index.get(stream_id1),
            Some(&StreamIndexRecord {
                version_min: 0,
                version_max: 1,
                correlation_id: correlation_id1,
                offsets: offsets1,
            })
        );
        assert_eq!(
            reopened_index.get(stream_id2),
            Some(&StreamIndexRecord {
                version_min: 0,
                version_max: 0,
                correlation_id: correlation_id2,
                offsets: offsets2,
            })
        );
        assert_eq!(reopened_index.get("unknown"), None);
    }

    #[test]
    fn test_closed_event_index_lookup() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        let stream_id1 = "stream-a";
        let stream_id2 = "stream-b";
        let correlation_id1 = Uuid::new_v4();
        let correlation_id2 = Uuid::new_v4();
        let offsets1 = vec![1111, 2222];
        let offsets2 = vec![3333];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(stream_id1, i as u64, correlation_id1, *offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(stream_id2, i as u64, correlation_id2, *offset)
                .unwrap();
        }
        index.flush().unwrap();

        let closed_index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        assert_eq!(
            closed_index.get_key(stream_id1).unwrap(),
            Some(StreamIndexRecord {
                version_min: 0,
                version_max: 1,
                correlation_id: correlation_id1,
                offsets: OffsetKind::Pointer(131944, 2),
            })
        );
        assert_eq!(closed_index.get(stream_id1).unwrap(), offsets1);
        assert_eq!(
            closed_index.get_key(stream_id2).unwrap(),
            Some(StreamIndexRecord {
                version_min: 0,
                version_max: 0,
                correlation_id: correlation_id2,
                offsets: OffsetKind::Pointer(131960, 1),
            })
        );
        assert_eq!(closed_index.get(stream_id2).unwrap(), offsets2);
    }

    #[test]
    fn test_collision_handling_in_direct_mapping() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        let stream_id1 = "stream-a";
        let stream_id2 = "stream-b";
        assert_ne!(
            RANDOM_STATE.hash_one(stream_id1) % 8,
            RANDOM_STATE.hash_one(stream_id2) % 8
        );

        let stream_id3 = "stream-k";
        let stream_id4 = "stream-l";
        assert_eq!(
            RANDOM_STATE.hash_one(stream_id3) % 8,
            RANDOM_STATE.hash_one(stream_id4) % 8
        );
        assert_ne!(
            RANDOM_STATE.hash_one(stream_id1) % 8,
            RANDOM_STATE.hash_one(stream_id3) % 8
        );
        assert_ne!(
            RANDOM_STATE.hash_one(stream_id1) % 8,
            RANDOM_STATE.hash_one(stream_id4) % 8
        );
        assert_ne!(
            RANDOM_STATE.hash_one(stream_id2) % 8,
            RANDOM_STATE.hash_one(stream_id3) % 8
        );
        assert_ne!(
            RANDOM_STATE.hash_one(stream_id2) % 8,
            RANDOM_STATE.hash_one(stream_id4) % 8
        );

        let correlation_id1 = Uuid::new_v4();
        let correlation_id2 = Uuid::new_v4();
        let correlation_id3 = Uuid::new_v4();
        let correlation_id4 = Uuid::new_v4();

        let offsets1 = vec![883, 44];
        let offsets2 = vec![39, 1, 429];
        let offsets3 = vec![1111, 2222];
        let offsets4 = vec![3333];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(stream_id1, i as u64, correlation_id1, *offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(stream_id2, i as u64, correlation_id2, *offset)
                .unwrap();
        }
        for (i, offset) in offsets3.iter().enumerate() {
            index
                .insert(stream_id3, i as u64, correlation_id3, *offset)
                .unwrap();
        }
        for (i, offset) in offsets4.iter().enumerate() {
            index
                .insert(stream_id4, i as u64, correlation_id4, *offset)
                .unwrap();
        }
        index.flush().unwrap();

        let closed_index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        assert_eq!(
            closed_index.get_key(stream_id1).unwrap(),
            Some(StreamIndexRecord {
                version_min: 0,
                version_max: 1,
                correlation_id: correlation_id1,
                offsets: OffsetKind::Pointer(132376, 2),
            })
        );
        assert_eq!(closed_index.get(stream_id1).unwrap(), offsets1);

        assert_eq!(
            closed_index.get_key(stream_id2).unwrap(),
            Some(StreamIndexRecord {
                version_min: 0,
                version_max: 2,
                correlation_id: correlation_id2,
                offsets: OffsetKind::Pointer(132392, 3),
            })
        );
        assert_eq!(closed_index.get(stream_id2).unwrap(), offsets2);

        assert_eq!(
            closed_index.get_key(stream_id3).unwrap(),
            Some(StreamIndexRecord {
                version_min: 0,
                version_max: 1,
                correlation_id: correlation_id3,
                offsets: OffsetKind::Pointer(132416, 2),
            })
        );
        assert_eq!(closed_index.get(stream_id3).unwrap(), offsets3);

        assert_eq!(
            closed_index.get_key(stream_id4).unwrap(),
            Some(StreamIndexRecord {
                version_min: 0,
                version_max: 0,
                correlation_id: correlation_id4,
                offsets: OffsetKind::Pointer(132432, 1),
            })
        );
        assert_eq!(closed_index.get(stream_id4).unwrap(), offsets4);

        assert_eq!(closed_index.get("unknown").unwrap(), Vec::<u64>::new());
    }

    #[test]
    fn test_non_existent_event_lookup() {
        let path = temp_file_path();

        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        index.flush().unwrap();

        let index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), path, SEGMENT_SIZE).unwrap();
        assert_eq!(index.get("unknown").unwrap(), Vec::<u64>::new());
    }

    #[test]
    fn test_insert_empty_stream_id() {
        let path = temp_file_path();

        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        assert!(index.insert("", 0, Uuid::new_v4(), 0).is_err());
        index.flush().unwrap();

        let index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), path, SEGMENT_SIZE).unwrap();
        assert_eq!(index.get("").unwrap(), Vec::<u64>::new());
    }

    #[test]
    fn test_insert_large_stream_id() {
        let path = temp_file_path();

        let stream_id = "THIS STREAM ID IS TOO LONG! THIS STREAM ID IS TOO LONG! THIS STREAM ID IS TOO LONG! THIS STREAM ID IS TOO LONG!";

        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        assert!(index.insert(stream_id, 0, Uuid::new_v4(), 0).is_err());
        index.flush().unwrap();

        let index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), path, SEGMENT_SIZE).unwrap();
        assert_eq!(index.get(stream_id).unwrap(), Vec::<u64>::new());
    }
}
