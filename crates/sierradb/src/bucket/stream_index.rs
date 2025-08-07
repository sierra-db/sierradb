//! The file format for an MPHF-based stream index is defined as follows:
//! - `[0..4]`     : magic marker: `b"SIDX"`
//! - `[4..12]`    : number of keys (n) as a `u64`
//! - `[12..20]`   : length of serialized MPHF (L) as a `u64`
//! - `[20..20+L]` : serialized MPHF bytes (using bincode)
//! - `[20+L..]`   : records array, exactly n records of RECORD_SIZE bytes each.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::mem;
use std::ops::ControlFlow;
use std::os::unix::fs::FileExt;
use std::panic::panic_any;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arc_swap::{ArcSwap, Cache};
use bincode::config;
use bloomfilter::Bloom;
use boomphf::Mphf;
use rayon::ThreadPool;
use tokio::sync::oneshot;
use tracing::{error, warn};
use uuid::Uuid;

use super::segment::{BucketSegmentReader, EventRecord, Record};
use super::{BucketId, BucketSegmentId, SegmentId};
use crate::bucket::segment::{CommittedEvents, ReadHint};
use crate::error::{EventValidationError, StreamIndexError, ThreadPoolError};
use crate::reader_thread_pool::{ReaderSet, ReaderThreadPool};
use crate::writer_thread_pool::LiveIndexes;
use crate::{BLOOM_SEED, STREAM_ID_SIZE, StreamId, from_bytes};

const VERSION_SIZE: usize = mem::size_of::<u64>();
const PARTITION_KEY_SIZE: usize = mem::size_of::<Uuid>();
const OFFSET_SIZE: usize = mem::size_of::<u64>();
const LEN_SIZE: usize = mem::size_of::<u32>();
// Stream ID, version min, version max, partition key, offset, len
const RECORD_SIZE: usize =
    STREAM_ID_SIZE + VERSION_SIZE + VERSION_SIZE + PARTITION_KEY_SIZE + OFFSET_SIZE + LEN_SIZE;

const AVG_EVENT_SIZE: usize = 350;
const AVG_EVENTS_PER_STREAM: usize = 10;
const FALSE_POSITIVE_PROBABILITY: f64 = 0.001;
const MPHF_GAMMA: f64 = 1.4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamIndexRecord<T> {
    pub partition_key: Uuid,
    pub version_min: u64,
    pub version_max: u64,
    pub offsets: T,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamOffsets {
    Offsets(Vec<u64>), // Its cached
    ExternalBucket,    // This stream lives in a different bucket
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClosedOffsetKind {
    Pointer(u64, u32), // Its in the file at this location
    Cached(Vec<u64>),  // Its cached
    ExternalBucket,    // This stream lives in a different bucket
}

impl From<StreamOffsets> for ClosedOffsetKind {
    fn from(offsets: StreamOffsets) -> Self {
        match offsets {
            StreamOffsets::Offsets(offsets) => ClosedOffsetKind::Cached(offsets),
            StreamOffsets::ExternalBucket => ClosedOffsetKind::ExternalBucket,
        }
    }
}

#[derive(Debug)]
pub struct OpenStreamIndex {
    id: BucketSegmentId,
    file: File,
    index: BTreeMap<StreamId, StreamIndexRecord<StreamOffsets>>,
    bloom: Bloom<str>,
}

impl OpenStreamIndex {
    pub fn create(
        id: BucketSegmentId,
        path: impl AsRef<Path>,
        segment_size: usize,
    ) -> Result<Self, StreamIndexError> {
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
        .map_err(|err| StreamIndexError::Bloom { err })?;

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
    ) -> Result<Self, StreamIndexError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let index = BTreeMap::new();
        let bloom = Bloom::new_for_fp_rate_with_seed(
            (segment_size / AVG_EVENT_SIZE / AVG_EVENTS_PER_STREAM).max(1),
            FALSE_POSITIVE_PROBABILITY,
            &BLOOM_SEED,
        )
        .map_err(|err| StreamIndexError::Bloom { err })?;

        Ok(OpenStreamIndex {
            id,
            file,
            index,
            bloom,
        })
    }

    /// Closes the stream index, flushing the index in a background thread.
    pub fn close(self, pool: &ThreadPool) -> Result<ClosedStreamIndex, StreamIndexError> {
        let id = self.id;
        let mut file_clone = self.file.try_clone()?;
        let index = Arc::new(ArcSwap::new(Arc::new(ClosedIndex::Cache(self.index))));
        let bloom = Arc::new(self.bloom);

        pool.spawn({
            let index = Arc::clone(&index);
            let bloom = Arc::clone(&bloom);
            move || match &**index.load() {
                ClosedIndex::Cache(map) => match Self::flush_inner(&mut file_clone, map, &bloom) {
                    Ok((mphf, records_offset)) => {
                        index.store(Arc::new(ClosedIndex::Mphf {
                            mphf,
                            records_offset,
                        }));
                    }
                    Err(err) => {
                        panic_any(ThreadPoolError::FlushStreamIndex {
                            id,
                            file: file_clone,
                            index,
                            err,
                        });
                    }
                },
                ClosedIndex::Mphf { .. } => unreachable!("no other threads write to this arc swap"),
            }
        });

        Ok(ClosedStreamIndex {
            id,
            file: self.file,
            index: Cache::new(index),
            bloom,
        })
    }

    pub fn get(&self, stream_id: &str) -> Option<&StreamIndexRecord<StreamOffsets>> {
        self.index.get(stream_id)
    }

    pub fn insert(
        &mut self,
        stream_id: StreamId,
        partition_key: Uuid,
        stream_version: u64,
        offset: u64,
    ) -> Result<(), StreamIndexError> {
        match self.index.entry(stream_id) {
            Entry::Vacant(entry) => {
                self.bloom.set(entry.key());
                entry.insert(StreamIndexRecord {
                    partition_key,
                    version_min: stream_version,
                    version_max: stream_version,
                    offsets: StreamOffsets::Offsets(vec![offset]),
                });
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                if entry.partition_key != partition_key {
                    return Err(StreamIndexError::Validation(
                        EventValidationError::PartitionKeyMismatch,
                    ));
                }
                entry.version_min = entry.version_min.min(stream_version);
                entry.version_max = entry.version_max.max(stream_version);
                match &mut entry.offsets {
                    StreamOffsets::Offsets(offsets) => {
                        offsets.push(offset);
                    }
                    StreamOffsets::ExternalBucket => {
                        return Err(StreamIndexError::StreamIdMappedToExternalBucket);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn insert_external_bucket(
        &mut self,
        stream_id: StreamId,
        partition_key: Uuid,
    ) -> Result<(), StreamIndexError> {
        match self.index.entry(stream_id) {
            Entry::Vacant(entry) => {
                self.bloom.set(entry.key());
                entry.insert(StreamIndexRecord {
                    partition_key,
                    version_min: 0,
                    version_max: 0,
                    offsets: StreamOffsets::ExternalBucket,
                });
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                if entry.partition_key != partition_key {
                    return Err(StreamIndexError::Validation(
                        EventValidationError::PartitionKeyMismatch,
                    ));
                }
                match &mut entry.offsets {
                    StreamOffsets::Offsets(_) => {
                        return Err(StreamIndexError::StreamIdOffsetExists);
                    }
                    StreamOffsets::ExternalBucket => {}
                }
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(Mphf<StreamId>, u64), StreamIndexError> {
        Self::flush_inner(&mut self.file, &self.index, &self.bloom)
    }

    /// Hydrates the index from a reader.
    pub fn hydrate(&mut self, reader: &mut BucketSegmentReader) -> Result<(), StreamIndexError> {
        let mut reader_iter = reader.iter();
        while let Some(record) = reader_iter.next_record()? {
            match record {
                Record::Event(EventRecord {
                    offset,
                    partition_key,
                    stream_id,
                    stream_version,
                    ..
                }) => {
                    self.insert(stream_id, partition_key, stream_version, offset)?;
                }
                Record::Commit(_) => {}
            }
        }

        Ok(())
    }

    fn flush_inner(
        file: &mut File,
        index: &BTreeMap<StreamId, StreamIndexRecord<StreamOffsets>>,
        bloom: &Bloom<str>,
    ) -> Result<(Mphf<StreamId>, u64), StreamIndexError> {
        // Collect all keys from the index as strings
        let keys: Vec<_> = index.keys().cloned().collect();
        let n = keys.len() as u64;

        // Build the MPHF over the keys
        let mphf = Mphf::new(MPHF_GAMMA, &keys);

        // Serialize the MPHF structure
        let mphf_bytes = bincode::serde::encode_to_vec(&mphf, config::standard())
            .map_err(StreamIndexError::SerializeMphf)?;
        let mphf_bytes_len = mphf_bytes.len() as u64;

        // Get the bloom filter bytes
        let bloom_bytes = bloom.to_bytes();

        // Allocate a records array for exactly n records
        let mut records = vec![0u8; index.len() * RECORD_SIZE];

        // Truncate the file before writing
        file.set_len(0)?;

        // Value data for all stream records
        let mut value_data = Vec::new();

        // Calculate the base offset for values
        let header_size = 4 + 8 + 8 + mphf_bytes.len() as u64 + 8 + bloom_bytes.len() as u64;
        let records_size = (index.len() * RECORD_SIZE) as u64;
        let values_base_offset = header_size + records_size;

        // Place each record in its slot according to the MPHF
        for (stream_id, record) in index {
            let slot = mphf.hash(stream_id) as usize;
            let pos = slot * RECORD_SIZE;

            match &record.offsets {
                StreamOffsets::Offsets(offsets) => {
                    let offsets_bytes: Vec<_> =
                        offsets.iter().flat_map(|v| v.to_le_bytes()).collect();

                    if offsets_bytes.is_empty() {
                        continue;
                    }

                    let offsets_len = offsets.len() as u32;

                    // Record entry: stream_id, partition_key, version_min, version_max, offset, len
                    let mut pos_in_record = 0;

                    // Stream ID with padding to STREAM_ID_SIZE
                    let stream_id_bytes = stream_id.as_bytes();
                    records[pos + pos_in_record..pos + pos_in_record + stream_id_bytes.len()]
                        .copy_from_slice(stream_id_bytes);
                    pos_in_record += STREAM_ID_SIZE;

                    // Partition key
                    records[pos + pos_in_record..pos + pos_in_record + PARTITION_KEY_SIZE]
                        .copy_from_slice(record.partition_key.as_bytes());
                    pos_in_record += PARTITION_KEY_SIZE;

                    // Version min
                    records[pos + pos_in_record..pos + pos_in_record + VERSION_SIZE]
                        .copy_from_slice(&record.version_min.to_le_bytes());
                    pos_in_record += VERSION_SIZE;

                    // Version max
                    records[pos + pos_in_record..pos + pos_in_record + VERSION_SIZE]
                        .copy_from_slice(&record.version_max.to_le_bytes());
                    pos_in_record += VERSION_SIZE;

                    // Current offset in values section
                    let current_value_offset = values_base_offset + value_data.len() as u64;

                    // Offset to values - use the actual file offset
                    records[pos + pos_in_record..pos + pos_in_record + OFFSET_SIZE]
                        .copy_from_slice(&current_value_offset.to_le_bytes());
                    pos_in_record += OFFSET_SIZE;

                    // Length of values
                    records[pos + pos_in_record..pos + pos_in_record + LEN_SIZE]
                        .copy_from_slice(&offsets_len.to_le_bytes());

                    // Add to value data
                    value_data.extend(offsets_bytes);
                }
                StreamOffsets::ExternalBucket => {
                    // Record entry for external bucket
                    let mut pos_in_record = 0;

                    // Stream ID with padding to STREAM_ID_SIZE
                    let stream_id_bytes = stream_id.as_bytes();
                    records[pos + pos_in_record..pos + pos_in_record + stream_id_bytes.len()]
                        .copy_from_slice(stream_id_bytes);
                    pos_in_record += STREAM_ID_SIZE;

                    // Partition key
                    records[pos + pos_in_record..pos + pos_in_record + PARTITION_KEY_SIZE]
                        .copy_from_slice(record.partition_key.as_bytes());
                    pos_in_record += PARTITION_KEY_SIZE;

                    // Version min
                    records[pos + pos_in_record..pos + pos_in_record + VERSION_SIZE]
                        .copy_from_slice(&record.version_min.to_le_bytes());
                    pos_in_record += VERSION_SIZE;

                    // Version max
                    records[pos + pos_in_record..pos + pos_in_record + VERSION_SIZE]
                        .copy_from_slice(&record.version_max.to_le_bytes());
                    pos_in_record += VERSION_SIZE;

                    // Special value for external bucket (u64::MAX)
                    records[pos + pos_in_record..pos + pos_in_record + OFFSET_SIZE]
                        .copy_from_slice(&u64::MAX.to_le_bytes());
                    pos_in_record += OFFSET_SIZE;

                    // Special value for external bucket (u32::MAX)
                    records[pos + pos_in_record..pos + pos_in_record + LEN_SIZE]
                        .copy_from_slice(&u32::MAX.to_le_bytes());
                }
            }
        }

        // Build the file header
        // Magic marker ("SIDX"), number of keys, length of mph_bytes, then the
        // mph_bytes and bloom_bytes
        let mut file_data = Vec::with_capacity(
            4 + 8 + 8 + mphf_bytes.len() + 8 + bloom_bytes.len() + records.len(),
        );
        file_data.extend_from_slice(b"SIDX"); // magic: 4 bytes
        file_data.extend_from_slice(&n.to_le_bytes()); // number of keys: 8 bytes
        file_data.extend_from_slice(&mphf_bytes_len.to_le_bytes()); // length of mph_bytes: 8 bytes
        file_data.extend_from_slice(&mphf_bytes); // serialized MPHF
        file_data.extend_from_slice(&(bloom_bytes.len() as u64).to_le_bytes()); // length of bloom_bytes: 8 bytes
        file_data.extend_from_slice(&bloom_bytes); // bloom filter bytes
        file_data.extend_from_slice(&records); // records array

        // Calculate records offset
        let records_offset = 4 + 8 + 8 + mphf_bytes.len() as u64 + 8 + bloom_bytes.len() as u64;

        // Write the file data and values data
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&file_data)?;
        file.write_all(&value_data)?;
        file.flush()?;

        Ok((mphf, records_offset))
    }
}

#[derive(Debug)]
pub enum ClosedIndex {
    Cache(BTreeMap<StreamId, StreamIndexRecord<StreamOffsets>>),
    Mphf {
        mphf: Mphf<StreamId>,
        records_offset: u64,
    },
}

pub struct ClosedStreamIndex {
    id: BucketSegmentId,
    file: File,
    index: Cache<Arc<ArcSwap<ClosedIndex>>, Arc<ClosedIndex>>,
    bloom: Arc<Bloom<str>>,
}

impl ClosedStreamIndex {
    pub fn open(
        id: BucketSegmentId,
        path: impl AsRef<Path>,
        _segment_size: usize, // Keep parameter for API compatibility
    ) -> Result<Self, StreamIndexError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        // Read the magic marker
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;

        if &magic != b"SIDX" {
            return Err(StreamIndexError::CorruptHeader);
        }

        // File is positioned after magic bytes
        let (mphf, _, records_offset, bloom) = load_index_from_file(&mut file)?;

        Ok(ClosedStreamIndex {
            id,
            file,
            index: Cache::new(Arc::new(ArcSwap::new(Arc::new(ClosedIndex::Mphf {
                mphf,
                records_offset,
            })))),
            bloom: Arc::new(bloom),
        })
    }

    pub fn try_clone(&self) -> Result<Self, StreamIndexError> {
        Ok(ClosedStreamIndex {
            id: self.id,
            file: self.file.try_clone()?,
            index: self.index.clone(),
            bloom: Arc::clone(&self.bloom),
        })
    }

    pub fn get_key(
        &mut self,
        stream_id: &str,
    ) -> Result<Option<StreamIndexRecord<ClosedOffsetKind>>, StreamIndexError> {
        // First check if we should use the bloom filter
        if !self.bloom.check(stream_id) {
            return Ok(None);
        }

        match self.index.load().as_ref() {
            // Cache mode - this is used during transitions or legacy format
            ClosedIndex::Cache(cache) => {
                Ok(cache
                    .get(stream_id)
                    .cloned()
                    .map(|record| StreamIndexRecord {
                        version_min: record.version_min,
                        version_max: record.version_max,
                        partition_key: record.partition_key,
                        offsets: record.offsets.into(),
                    }))
            }
            // New MPHF-based lookup
            ClosedIndex::Mphf {
                mphf,
                records_offset,
            } => {
                // Try to compute the slot using the MPHF
                let Some(slot) = mphf.try_hash(stream_id) else {
                    return Ok(None);
                };

                // Calculate the position in the file
                let pos = records_offset + (slot * RECORD_SIZE as u64);

                // Read the record
                let mut buf = vec![0u8; RECORD_SIZE];
                self.file.read_exact_at(&mut buf, pos)?;

                // Extract the stream ID and verify it matches
                let mut pos = 0;
                let stored_stream_id = std::str::from_utf8(&buf[pos..pos + STREAM_ID_SIZE])
                    .map_err(StreamIndexError::InvalidStreamIdUtf8)?
                    .trim_end_matches('\0');
                pos += STREAM_ID_SIZE;

                // Double-check the stream ID - this is an important safety check
                // because the MPHF is defined only for keys that existed when it was built
                if stored_stream_id != stream_id {
                    return Ok(None);
                }

                // Extract the record fields
                let (partition_key, version_min, version_max, offset, len) =
                    from_bytes!(&buf, pos, [Uuid, u64, u64, u64, u32]);

                // Determine the type of offset
                let offsets = if offset == u64::MAX && len == u32::MAX {
                    ClosedOffsetKind::ExternalBucket
                } else {
                    ClosedOffsetKind::Pointer(offset, len)
                };

                // Return the record
                Ok(Some(StreamIndexRecord {
                    version_min,
                    version_max,
                    partition_key,
                    offsets,
                }))
            }
        }
    }

    pub fn get_from_key(
        &self,
        key: StreamIndexRecord<ClosedOffsetKind>,
    ) -> Result<StreamOffsets, StreamIndexError> {
        match key.offsets {
            ClosedOffsetKind::Pointer(offset, len) => {
                // Read values from the file
                let mut values_buf = vec![0u8; len as usize * 8];
                match self.file.read_exact_at(&mut values_buf, offset) {
                    Ok(_) => {
                        // Successfully read the values
                        let offsets = values_buf
                            .chunks_exact(8)
                            .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                            .collect();
                        Ok(StreamOffsets::Offsets(offsets))
                    }
                    Err(e) => {
                        // If we can't read the values, use the offset and len for diagnostic info
                        Err(StreamIndexError::Io(e))
                    }
                }
            }
            ClosedOffsetKind::Cached(offsets) => Ok(StreamOffsets::Offsets(offsets)),
            ClosedOffsetKind::ExternalBucket => Ok(StreamOffsets::ExternalBucket),
        }
    }

    pub fn get(&mut self, stream_id: &str) -> Result<Option<StreamOffsets>, StreamIndexError> {
        self.get_key(stream_id)
            .and_then(|key| key.map(|key| self.get_from_key(key)).transpose())
    }
}

#[derive(Debug)]
pub struct EventStreamIter {
    stream_id: StreamId,
    bucket_id: BucketId,
    reverse: bool,
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
        stream_id: StreamId,
        bucket_id: BucketId,
        from_version: u64,
        reverse: bool,
        reader_pool: ReaderThreadPool,
        live_indexes: &HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>,
    ) -> Result<Self, StreamIndexError> {
        let mut live_segment_id = 0;
        let mut live_segment_offsets: VecDeque<_> = match live_indexes.get(&bucket_id) {
            Some((current_live_segment_id, live_indexes)) => {
                let current_live_segment_id = current_live_segment_id.load(Ordering::Acquire);
                live_segment_id = current_live_segment_id;
                match live_indexes
                    .read()
                    .await
                    .stream_index
                    .get(&stream_id)
                    .cloned()
                {
                    Some(StreamIndexRecord {
                        version_min,
                        offsets: StreamOffsets::Offsets(mut offsets),
                        ..
                    }) => {
                        if version_min <= from_version {
                            let to_remove = (from_version - version_min).min(offsets.len() as u64);
                            offsets.drain(0..to_remove as usize);

                            let mut live_segment_offsets: VecDeque<_> = offsets.into();

                            let next_live_offset = if !reverse {
                                live_segment_offsets.pop_front()
                            } else {
                                live_segment_offsets.pop_back()
                            }
                            .map(|offset| NextOffset {
                                offset,
                                segment_id: current_live_segment_id,
                            });

                            return Ok(EventStreamIter {
                                stream_id,
                                bucket_id,
                                reverse,
                                reader_pool,
                                segment_id: current_live_segment_id,
                                segment_offsets: VecDeque::new(),
                                live_segment_id: current_live_segment_id,
                                live_segment_offsets,
                                next_offset: None,
                                next_live_offset,
                            });
                        }

                        Some(offsets.into())
                    }
                    Some(StreamIndexRecord {
                        offsets: StreamOffsets::ExternalBucket,
                        ..
                    }) => None,
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
            let stream_id = stream_id.clone();
            move |with_readers| {
                with_readers(move |readers| {
                    let res = readers
                        .get_mut(&bucket_id)
                        .and_then(|segments| {
                            let find_map_fn =
                                |(i, (segment_id, reader_set)): (usize, (&u32, &mut ReaderSet))| {
                                    let stream_index = reader_set.stream_index.as_mut()?;

                                    match stream_index.get_key(&stream_id) {
                                        Ok(Some(key))
                                            if key.version_min >= from_version || i == 0 =>
                                        {
                                            let version_min = key.version_min;
                                            match stream_index.get_from_key(key) {
                                                Ok(mut offsets) => {
                                                    if let StreamOffsets::Offsets(offsets) =
                                                        &mut offsets
                                                    {
                                                        let to_remove = (from_version
                                                            - version_min)
                                                            .min(offsets.len() as u64);
                                                        offsets.drain(0..to_remove as usize);
                                                    }

                                                    if let StreamOffsets::Offsets(offsets) =
                                                        &offsets
                                                        && let Some(offset) = offsets.first()
                                                    {
                                                        reader_set.reader.prefetch(*offset);
                                                    }

                                                    Some(Ok((*segment_id, offsets)))
                                                }
                                                Err(err) => Some(Err(err)),
                                            }
                                        }
                                        Ok(_) => None,
                                        Err(err) => Some(Err(err)),
                                    }
                                };

                            if !reverse {
                                segments.iter_mut().enumerate().rev().find_map(find_map_fn)
                            } else {
                                segments.iter_mut().enumerate().find_map(find_map_fn)
                            }
                        })
                        .transpose();
                    let _ = reply_tx.send(res);
                });
            }
        });

        match reply_rx.await {
            Ok(Ok(Some((segment_id, StreamOffsets::Offsets(segment_offsets))))) => {
                let mut segment_offsets: VecDeque<_> = segment_offsets.into();

                let (next_offset, next_live_offset) = if !reverse {
                    let next_offset = segment_offsets
                        .pop_front()
                        .map(|offset| NextOffset { offset, segment_id });
                    let next_live_offset =
                        live_segment_offsets.pop_front().map(|offset| NextOffset {
                            offset,
                            segment_id: live_segment_id,
                        });

                    (next_offset, next_live_offset)
                } else {
                    let next_offset = segment_offsets
                        .pop_back()
                        .map(|offset| NextOffset { offset, segment_id });
                    let next_live_offset =
                        live_segment_offsets.pop_back().map(|offset| NextOffset {
                            offset,
                            segment_id: live_segment_id,
                        });

                    (next_offset, next_live_offset)
                };

                Ok(EventStreamIter {
                    stream_id,
                    bucket_id,
                    reverse,
                    reader_pool,
                    segment_id,
                    segment_offsets,
                    live_segment_id,
                    live_segment_offsets,
                    next_offset,
                    next_live_offset,
                })
            }
            Ok(Ok(Some((_, StreamOffsets::ExternalBucket)))) | Ok(Ok(None)) | Err(_) => {
                let next_live_offset = if !reverse {
                    live_segment_offsets.pop_front()
                } else {
                    live_segment_offsets.pop_back()
                }
                .map(|offset| NextOffset {
                    offset,
                    segment_id: live_segment_id,
                });

                Ok(EventStreamIter {
                    stream_id,
                    bucket_id,
                    reverse,
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

    pub async fn next(&mut self) -> Result<Option<CommittedEvents>, StreamIndexError> {
        struct ReadResult {
            events: Option<CommittedEvents>,
            new_offsets: Option<(SegmentId, VecDeque<u64>)>,
            is_live: bool,
        }

        let stream_id = self.stream_id.clone();
        let bucket_id = self.bucket_id;
        let reverse = self.reverse;
        let segment_id = self.segment_id;
        let live_segment_id = self.live_segment_id;
        let next_offset = self.next_offset;
        let next_live_offset = self.next_live_offset;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| {
                let res = readers
                    .get_mut(&bucket_id)
                    .map(|segments| -> Result<ReadResult, StreamIndexError> {
                        match next_offset {
                            Some(NextOffset { offset, segment_id }) => {
                                // We have an offset from the last batch
                                let reader_set = segments.get_mut(&segment_id).ok_or(
                                    StreamIndexError::SegmentNotFound {
                                        bucket_segment_id: BucketSegmentId::new(
                                            bucket_id, segment_id,
                                        ),
                                    },
                                )?;

                                let (events, _) = reader_set
                                    .reader
                                    .read_committed_events(offset, ReadHint::Random)?;

                                Ok(ReadResult {
                                    events,
                                    new_offsets: None,
                                    is_live: false,
                                })
                            }
                            None => {
                                // There's no more offsets in this batch, progress forwards finding
                                // the next batch
                                let process_segment = |i: SegmentId,
                                                       segments: &mut BTreeMap<u32, ReaderSet>|
                                 -> Result<
                                    ControlFlow<ReadResult>,
                                    StreamIndexError,
                                > {
                                    let Some(reader_set) = segments.get_mut(&i) else {
                                        return Ok(ControlFlow::Continue(()));
                                    };

                                    let Some(stream_index) = &mut reader_set.stream_index else {
                                        return Ok(ControlFlow::Continue(()));
                                    };

                                    let mut new_offsets: VecDeque<_> =
                                        match stream_index.get(&stream_id)? {
                                            Some(StreamOffsets::Offsets(offsets)) => offsets.into(),
                                            Some(StreamOffsets::ExternalBucket) => {
                                                return Ok(ControlFlow::Break(ReadResult {
                                                    events: None,
                                                    new_offsets: None,
                                                    is_live: false,
                                                }));
                                            }
                                            None => {
                                                return Ok(ControlFlow::Continue(()));
                                            }
                                        };
                                    let Some(next_offset) = new_offsets.pop_front() else {
                                        return Ok(ControlFlow::Continue(()));
                                    };

                                    let (events, _) = reader_set
                                        .reader
                                        .read_committed_events(next_offset, ReadHint::Random)?;

                                    Ok(ControlFlow::Break(ReadResult {
                                        events,
                                        new_offsets: Some((i, new_offsets)),
                                        is_live: false,
                                    }))
                                };
                                let process_live_offsets = |segments: &mut BTreeMap<
                                    u32,
                                    ReaderSet,
                                >|
                                 -> Result<
                                    Option<ReadResult>,
                                    StreamIndexError,
                                > {
                                    match next_live_offset {
                                        Some(NextOffset { offset, segment_id }) => {
                                            let Some(reader_set) = segments.get_mut(&segment_id)
                                            else {
                                                return Ok(None);
                                            };

                                            let (events, _) = reader_set
                                                .reader
                                                .read_committed_events(offset, ReadHint::Random)?;

                                            Ok(Some(ReadResult {
                                                events,
                                                new_offsets: None,
                                                is_live: true,
                                            }))
                                        }
                                        None => Ok(None),
                                    }
                                };

                                if !reverse {
                                    for i in segment_id.saturating_add(1)
                                        ..(segments.len() as SegmentId).min(live_segment_id)
                                    {
                                        match process_segment(i, segments)? {
                                            ControlFlow::Continue(()) => {}
                                            ControlFlow::Break(res) => return Ok(res),
                                        }
                                    }

                                    Ok(process_live_offsets(segments)?.unwrap_or(ReadResult {
                                        events: None,
                                        new_offsets: None,
                                        is_live: true,
                                    }))
                                } else {
                                    if let Some(res) = process_live_offsets(segments)? {
                                        return Ok(res);
                                    }

                                    for i in (segments.len() as SegmentId).min(live_segment_id)
                                        ..segment_id.saturating_add(1)
                                    {
                                        match process_segment(i, segments)? {
                                            ControlFlow::Continue(()) => {}
                                            ControlFlow::Break(res) => return Ok(res),
                                        }
                                    }

                                    Ok(ReadResult {
                                        events: None,
                                        new_offsets: None,
                                        is_live: false,
                                    })
                                }

                                // No more batches found, we'll process the live
                                // offsets
                            }
                        }
                    })
                    .transpose();
                let _ = reply_tx.send(res);
            });
        });

        match reply_rx.await {
            Ok(Ok(Some(ReadResult {
                mut events,
                new_offsets,
                is_live,
            }))) => {
                // Filter events and handle skipping
                if let Some(ref mut committed_events) = events {
                    let max_returned_version = match committed_events {
                        CommittedEvents::Transaction { events, .. } => {
                            // Filter to only keep events from our stream
                            events.retain(|event| event.stream_id == self.stream_id);

                            // Get the max stream version from remaining events
                            events.iter().map(|e| e.stream_version).max()
                        }
                        CommittedEvents::Single(event) => {
                            // If this single event is not from our stream, return None
                            if event.stream_id == self.stream_id {
                                Some(event.stream_version)
                            } else {
                                None
                            }
                        }
                    };

                    if let Some(_max_version) = max_returned_version {
                        // Skip all offsets that might contain events with
                        // stream_version <= max_version
                        // Note: We can't know which offsets correspond to which
                        // versions without reading, but
                        // we can still benefit from skipping some redundant
                        // reads This is a best-effort
                        // optimization
                    } else {
                        // No events from our stream in this transaction
                        events = None;
                    }
                }

                // Update segment state
                if is_live {
                    self.segment_id = self.live_segment_id;
                    if !self.reverse {
                        self.next_live_offset =
                            self.live_segment_offsets
                                .pop_front()
                                .map(|offset| NextOffset {
                                    offset,
                                    segment_id: self.live_segment_id,
                                });
                    } else {
                        self.next_live_offset =
                            self.live_segment_offsets
                                .pop_back()
                                .map(|offset| NextOffset {
                                    offset,
                                    segment_id: self.live_segment_id,
                                });
                    }
                } else {
                    if let Some((new_segment, new_offsets)) = new_offsets {
                        self.segment_id = new_segment;
                        self.segment_offsets = new_offsets;
                    }
                    if !self.reverse {
                        self.next_offset =
                            self.segment_offsets.pop_front().map(|offset| NextOffset {
                                offset,
                                segment_id: self.segment_id,
                            });
                    } else {
                        self.next_offset =
                            self.segment_offsets.pop_back().map(|offset| NextOffset {
                                offset,
                                segment_id: self.segment_id,
                            });
                    }
                }

                Ok(events)
            }
            Ok(Ok(None)) => Ok(None),
            Ok(Err(err)) => Err(err),
            Err(_) => {
                error!("no reply from reader pool");
                Ok(None)
            }
        }
    }
}

#[allow(clippy::type_complexity)]
/// Loads the MPHF-based index from a file format
///
/// The file format is:
///   [0..4]   : magic marker: b"SIDX"
///   [4..12]  : number of keys (n) as a u64
///   [12..20] : length of serialized MPHF (L) as a u64
///   [20..20+L] : serialized MPHF bytes (using bincode)
///   [20+L..20+L+8] : length of bloom bytes (B) as a u64
///   [20+L+8..20+L+8+B] : bloom bytes
///   [20+L+8+B..] : records array, exactly n records of RECORD_SIZE bytes each.
fn load_index_from_file(
    file: &mut File,
) -> Result<(Mphf<StreamId>, u64, u64, Bloom<str>), StreamIndexError> {
    // File should already be positioned after the magic bytes

    // Read number of keys
    let mut n_buf = [0u8; 8];
    file.read_exact(&mut n_buf)?;
    let n = u64::from_le_bytes(n_buf);

    // Read length of serialized MPHF
    let mut mph_len_buf = [0u8; 8];
    file.read_exact(&mut mph_len_buf)?;
    let mph_bytes_len = u64::from_le_bytes(mph_len_buf) as usize;

    // Read the MPHF bytes and deserialize
    let mut mph_bytes = vec![0u8; mph_bytes_len];
    file.read_exact(&mut mph_bytes)?;
    let (mph, _): (Mphf<StreamId>, _) =
        bincode::serde::decode_from_slice(&mph_bytes, bincode::config::standard())
            .map_err(StreamIndexError::DeserializeMphf)?;

    // Read bloom filter length
    let mut bloom_len_buf = [0u8; 8];
    file.read_exact(&mut bloom_len_buf)?;
    let bloom_bytes_len = u64::from_le_bytes(bloom_len_buf) as usize;

    // Read bloom filter bytes
    let mut bloom_bytes = vec![0u8; bloom_bytes_len];
    file.read_exact(&mut bloom_bytes)?;
    let bloom = Bloom::from_slice(&bloom_bytes).map_err(|err| StreamIndexError::Bloom { err })?;

    // The records array immediately follows
    let records_offset = 4 + 8 + 8 + mph_bytes_len as u64 + 8 + bloom_bytes_len as u64;

    Ok((mph, n, records_offset, bloom))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    const SEGMENT_SIZE: usize = 256_000_000; // 64 MB

    fn temp_file_path() -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Create a unique filename for each test to avoid conflicts
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        tempfile::Builder::new()
            .prefix(&format!("test_{timestamp}_"))
            .make(|path| Ok(path.to_path_buf()))
            .unwrap()
            .path()
            .to_path_buf()
    }

    #[test]
    fn test_open_stream_index_insert_and_get() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), path, SEGMENT_SIZE).unwrap();

        let stream_id = StreamId::new("stream-a").unwrap();
        let partition_key = Uuid::new_v4();
        let offsets = vec![42, 105];
        for (i, offset) in offsets.iter().enumerate() {
            index
                .insert(stream_id.clone(), partition_key, i as u64, *offset)
                .unwrap();
        }

        assert_eq!(
            index.get(&stream_id),
            Some(&StreamIndexRecord {
                version_min: 0,
                version_max: 1,
                partition_key,
                offsets: StreamOffsets::Offsets(offsets),
            })
        );
        assert_eq!(index.get("unknown"), None);
    }

    #[test]
    fn test_closed_stream_index_lookup() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        let stream_id1 = StreamId::new("stream-a").unwrap();
        let stream_id2 = StreamId::new("stream-b").unwrap();
        let partition_key1 = Uuid::new_v4();
        let partition_key2 = Uuid::new_v4();
        let offsets1 = vec![1111, 2222];
        let offsets2 = vec![3333];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(stream_id1.clone(), partition_key1, i as u64, *offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(stream_id2.clone(), partition_key2, i as u64, *offset)
                .unwrap();
        }

        // Flush with the new MPHF format
        let (mphf, _) = index.flush().unwrap();
        assert!(mphf.try_hash(&stream_id1).is_some());
        assert!(mphf.try_hash(&stream_id2).is_some());
        assert!(mphf.try_hash(&StreamId::new("unknown").unwrap()).is_none());

        // Open with the closed index
        let mut closed_index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        // Test get_key with MPHF implementation
        let key1 = closed_index.get_key(&stream_id1).unwrap().unwrap();
        assert_eq!(key1.version_min, 0);
        assert_eq!(key1.version_max, 1);
        assert_eq!(key1.partition_key, partition_key1);

        // Test get with MPHF implementation
        assert_eq!(
            closed_index.get(&stream_id1).unwrap(),
            Some(StreamOffsets::Offsets(offsets1)),
        );

        // Test get_key for second stream ID
        let key2 = closed_index.get_key(&stream_id2).unwrap().unwrap();
        assert_eq!(key2.version_min, 0);
        assert_eq!(key2.version_max, 0);
        assert_eq!(key2.partition_key, partition_key2);

        // Test get for second stream ID
        assert_eq!(
            closed_index.get(&stream_id2).unwrap(),
            Some(StreamOffsets::Offsets(offsets2)),
        );

        // Test unknown stream ID
        assert_eq!(closed_index.get_key("unknown").unwrap(), None);
        assert_eq!(closed_index.get("unknown").unwrap(), None);
    }

    #[test]
    fn test_mphf_collision_handling() {
        let path = temp_file_path();
        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        // These two IDs might have collisions in a regular hash table,
        // but MPHF should handle them perfectly
        let stream_id1 = StreamId::new("stream-a").unwrap();
        let stream_id2 = StreamId::new("stream-b").unwrap();
        let stream_id3 = StreamId::new("stream-k").unwrap();
        let stream_id4 = StreamId::new("stream-l").unwrap();

        let partition_key1 = Uuid::new_v4();
        let partition_key2 = Uuid::new_v4();
        let partition_key3 = Uuid::new_v4();
        let partition_key4 = Uuid::new_v4();

        let offsets1 = vec![883, 44];
        let offsets2 = vec![39, 1, 429];
        let offsets3 = vec![1111, 2222];
        let offsets4 = vec![3333];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(stream_id1.clone(), partition_key1, i as u64, *offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(stream_id2.clone(), partition_key2, i as u64, *offset)
                .unwrap();
        }
        for (i, offset) in offsets3.iter().enumerate() {
            index
                .insert(stream_id3.clone(), partition_key3, i as u64, *offset)
                .unwrap();
        }
        for (i, offset) in offsets4.iter().enumerate() {
            index
                .insert(stream_id4.clone(), partition_key4, i as u64, *offset)
                .unwrap();
        }

        // Flush with the new MPHF format
        let (mphf, _) = index.flush().unwrap();

        // Verify MPHF correctness - all streams should have different hash values
        let hash1 = mphf.hash(&stream_id1);
        let hash2 = mphf.hash(&stream_id2);
        let hash3 = mphf.hash(&stream_id3);
        let hash4 = mphf.hash(&stream_id4);

        assert_ne!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_ne!(hash1, hash4);
        assert_ne!(hash2, hash3);
        assert_ne!(hash2, hash4);
        assert_ne!(hash3, hash4);

        // Test with closed index
        let mut closed_index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();

        // For this test, we'll just check that we can retrieve all the stream IDs
        // without checking the exact offsets, since the file format has been updated
        assert_eq!(
            closed_index.get(&stream_id1).unwrap(),
            Some(StreamOffsets::Offsets(offsets1))
        );
        assert_eq!(
            closed_index.get(&stream_id2).unwrap(),
            Some(StreamOffsets::Offsets(offsets2))
        );
        assert_eq!(
            closed_index.get(&stream_id3).unwrap(),
            Some(StreamOffsets::Offsets(offsets3))
        );
        assert_eq!(
            closed_index.get(&stream_id4).unwrap(),
            Some(StreamOffsets::Offsets(offsets4))
        );

        // Unknown stream ID should not be found
        assert_eq!(closed_index.get("unknown").unwrap(), None);
    }

    #[test]
    fn test_non_existent_stream_lookup() {
        let path = temp_file_path();

        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        index.flush().unwrap();

        let mut index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), path, SEGMENT_SIZE).unwrap();
        assert_eq!(index.get("unknown").unwrap(), None);
    }

    #[test]
    fn test_insert_external_bucket() {
        let path = temp_file_path();

        let stream_id = StreamId::new("my-stream").unwrap();
        let partition_key = Uuid::new_v4();

        let mut index =
            OpenStreamIndex::create(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        index
            .insert_external_bucket(stream_id.clone(), partition_key)
            .unwrap();
        assert_eq!(
            index.get(&stream_id),
            Some(&StreamIndexRecord {
                partition_key,
                version_min: 0,
                version_max: 0,
                offsets: StreamOffsets::ExternalBucket
            })
        );
        index.flush().unwrap();

        let mut index =
            ClosedStreamIndex::open(BucketSegmentId::new(0, 0), &path, SEGMENT_SIZE).unwrap();
        assert_eq!(
            index.get(&stream_id).unwrap(),
            Some(StreamOffsets::ExternalBucket)
        );
    }
}
