//! The file format for an MPHF-based partition index is defined as follows:
//! - `[0..4]`     : magic marker: `b"PIDX"`
//! - `[4..12]`    : number of keys (n) as a `u64`
//! - `[12..20]`   : length of serialized MPHF (L) as a `u64`
//! - `[20..20+L]` : serialized MPHF bytes (using bincode)
//! - `[20+L..]`   : records array, exactly n records of RECORD_SIZE bytes each.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::mem;
use std::os::unix::fs::FileExt;
use std::panic::panic_any;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arc_swap::{ArcSwap, Cache};
use bincode::config;
use boomphf::Mphf;
use rayon::ThreadPool;
use tokio::sync::oneshot;
use tracing::{error, warn};

use super::segment::{BucketSegmentReader, EventRecord, Record};
use super::{BucketId, BucketSegmentId, PartitionId, SegmentId};
use crate::bucket::segment::CommittedEvents;
use crate::error::{PartitionIndexError, ThreadPoolError};
use crate::reader_thread_pool::ReaderThreadPool;
use crate::writer_thread_pool::LiveIndexes;

// Partition ID, sequence min, sequence max, event count, events offset, events
// length
const PARTITION_ID_SIZE: usize = mem::size_of::<PartitionId>();
const SEQUENCE_SIZE: usize = mem::size_of::<u64>();
const EVENTS_OFFSET_SIZE: usize = mem::size_of::<u64>();
const EVENTS_LEN_SIZE: usize = mem::size_of::<u32>();
const RECORD_SIZE: usize = PARTITION_ID_SIZE
    + SEQUENCE_SIZE
    + SEQUENCE_SIZE
    + SEQUENCE_SIZE
    + EVENTS_OFFSET_SIZE
    + EVENTS_LEN_SIZE;

const MPHF_GAMMA: f64 = 1.4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionIndexRecord<T> {
    pub sequence_min: u64,
    pub sequence_max: u64,
    pub sequence: u64,
    pub offsets: T,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionOffsets {
    Offsets(Vec<PartitionSequenceOffset>), // Its cached
    ExternalBucket,                        // This partition lives in a different bucket
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionSequenceOffset {
    pub sequence: u64,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClosedOffsetKind {
    Pointer(u64, u32), // Its in the file at this location (offset, length)
    Cached(Vec<PartitionSequenceOffset>), // Its cached
    ExternalBucket,    // This partition lives in a different bucket
}

impl From<PartitionOffsets> for ClosedOffsetKind {
    fn from(offsets: PartitionOffsets) -> Self {
        match offsets {
            PartitionOffsets::Offsets(offsets) => ClosedOffsetKind::Cached(offsets),
            PartitionOffsets::ExternalBucket => ClosedOffsetKind::ExternalBucket,
        }
    }
}

#[derive(Debug)]
pub struct OpenPartitionIndex {
    id: BucketSegmentId,
    file: File,
    index: BTreeMap<PartitionId, PartitionIndexRecord<PartitionOffsets>>,
}

impl OpenPartitionIndex {
    pub fn create(
        id: BucketSegmentId,
        path: impl AsRef<Path>,
    ) -> Result<Self, PartitionIndexError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let index = BTreeMap::new();

        Ok(OpenPartitionIndex { id, file, index })
    }

    pub fn open(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, PartitionIndexError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let index = BTreeMap::new();

        Ok(OpenPartitionIndex { id, file, index })
    }

    /// Closes the partition index, flushing the index in a background thread.
    pub fn close(self, pool: &ThreadPool) -> Result<ClosedPartitionIndex, PartitionIndexError> {
        let id = self.id;
        let mut file_clone = self.file.try_clone()?;
        let index = Arc::new(ArcSwap::new(Arc::new(ClosedIndex::Cache(self.index))));

        pool.spawn({
            let index = Arc::clone(&index);
            move || match &**index.load() {
                ClosedIndex::Cache(map) => match Self::flush_inner(&mut file_clone, map) {
                    Ok((mphf, records_offset)) => {
                        index.store(Arc::new(ClosedIndex::Mphf {
                            mphf,
                            records_offset,
                        }));
                    }
                    Err(err) => {
                        panic_any(ThreadPoolError::FlushPartitionIndex {
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

        Ok(ClosedPartitionIndex {
            id,
            file: self.file,
            index: Cache::new(index),
        })
    }

    pub fn get(
        &self,
        partition_id: PartitionId,
    ) -> Option<&PartitionIndexRecord<PartitionOffsets>> {
        self.index.get(&partition_id)
    }

    pub fn insert(
        &mut self,
        partition_id: PartitionId,
        sequence: u64,
        offset: u64,
    ) -> Result<(), PartitionIndexError> {
        match self.index.entry(partition_id) {
            Entry::Vacant(entry) => {
                entry.insert(PartitionIndexRecord {
                    sequence_min: sequence,
                    sequence_max: sequence,
                    sequence: 1,
                    offsets: PartitionOffsets::Offsets(vec![PartitionSequenceOffset {
                        sequence,
                        offset,
                    }]),
                });
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                entry.sequence_min = entry.sequence_min.min(sequence);
                entry.sequence_max = entry.sequence_max.max(sequence);
                match &mut entry.offsets {
                    PartitionOffsets::Offsets(offsets) => {
                        offsets.push(PartitionSequenceOffset { sequence, offset });
                        entry.sequence = entry
                            .sequence
                            .checked_add(1)
                            .ok_or(PartitionIndexError::EventCountOverflow)?;
                    }
                    PartitionOffsets::ExternalBucket => {
                        return Err(PartitionIndexError::PartitionIdMappedToExternalBucket);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn insert_external_bucket(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<(), PartitionIndexError> {
        match self.index.entry(partition_id) {
            Entry::Vacant(entry) => {
                entry.insert(PartitionIndexRecord {
                    sequence_min: 0,
                    sequence_max: 0,
                    sequence: 0,
                    offsets: PartitionOffsets::ExternalBucket,
                });
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                match &mut entry.offsets {
                    PartitionOffsets::Offsets(_) => {
                        return Err(PartitionIndexError::PartitionIdOffsetExists);
                    }
                    PartitionOffsets::ExternalBucket => {}
                }
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<(Mphf<PartitionId>, u64), PartitionIndexError> {
        Self::flush_inner(&mut self.file, &self.index)
    }

    /// Hydrates the index from a reader.
    pub fn hydrate(&mut self, reader: &mut BucketSegmentReader) -> Result<(), PartitionIndexError> {
        let mut reader_iter = reader.iter();
        while let Some(record) = reader_iter.next_record(true)? {
            match record {
                Record::Event(EventRecord {
                    offset,
                    partition_id,
                    partition_sequence,
                    ..
                }) => {
                    self.insert(partition_id, partition_sequence, offset)?;
                }
                Record::Commit(_) => {}
            }
        }

        Ok(())
    }

    fn flush_inner(
        file: &mut File,
        index: &BTreeMap<PartitionId, PartitionIndexRecord<PartitionOffsets>>,
    ) -> Result<(Mphf<PartitionId>, u64), PartitionIndexError> {
        // Collect all keys from the index
        let keys: Vec<PartitionId> = index.keys().copied().collect();
        let n = keys.len() as u64;

        // Build the MPHF over the keys
        let mphf = Mphf::new(MPHF_GAMMA, &keys);

        // Serialize the MPHF structure
        let mphf_bytes = bincode::serde::encode_to_vec(&mphf, config::standard())
            .map_err(PartitionIndexError::SerializeMphf)?;
        let mphf_bytes_len = mphf_bytes.len() as u64;

        // Allocate a records array for exactly n records
        let mut records = vec![0u8; index.len() * RECORD_SIZE];

        // Value data for all partition records
        let mut value_data = Vec::new();

        // Calculate the base offset for values
        let header_size = 4 + 8 + 8 + mphf_bytes.len() as u64;
        let records_size = (index.len() * RECORD_SIZE) as u64;
        let values_base_offset = header_size + records_size;

        // Place each record in its slot according to the MPHF
        for (&partition_id, record) in index {
            let slot = mphf.hash(&partition_id) as usize;
            let pos = slot * RECORD_SIZE;

            match &record.offsets {
                PartitionOffsets::Offsets(offsets) => {
                    let mut offsets_bytes: Vec<u8> =
                        vec![0; (SEQUENCE_SIZE + EVENTS_OFFSET_SIZE) * offsets.len()];
                    for (i, PartitionSequenceOffset { sequence, offset }) in
                        offsets.iter().enumerate()
                    {
                        let mut i = (SEQUENCE_SIZE + EVENTS_OFFSET_SIZE) * i;
                        offsets_bytes[i..i + SEQUENCE_SIZE]
                            .copy_from_slice(&sequence.to_le_bytes());
                        i += SEQUENCE_SIZE;
                        offsets_bytes[i..i + EVENTS_OFFSET_SIZE]
                            .copy_from_slice(&offset.to_le_bytes());
                    }

                    if offsets_bytes.is_empty() {
                        continue;
                    }

                    // Record entry: partition_id, sequence_min, sequence_max, event_count,
                    // events_offset, events_len
                    let mut pos_in_record = 0;

                    // Partition ID
                    records[pos + pos_in_record..pos + pos_in_record + PARTITION_ID_SIZE]
                        .copy_from_slice(&partition_id.to_le_bytes());
                    pos_in_record += PARTITION_ID_SIZE;

                    // Sequence min
                    records[pos + pos_in_record..pos + pos_in_record + SEQUENCE_SIZE]
                        .copy_from_slice(&record.sequence_min.to_le_bytes());
                    pos_in_record += SEQUENCE_SIZE;

                    // Sequence max
                    records[pos + pos_in_record..pos + pos_in_record + SEQUENCE_SIZE]
                        .copy_from_slice(&record.sequence_max.to_le_bytes());
                    pos_in_record += SEQUENCE_SIZE;

                    // Event count
                    records[pos + pos_in_record..pos + pos_in_record + SEQUENCE_SIZE]
                        .copy_from_slice(&record.sequence.to_le_bytes());
                    pos_in_record += SEQUENCE_SIZE;

                    // Current offset in values section
                    let current_value_offset = values_base_offset + value_data.len() as u64;

                    // Events offset - use the actual file offset
                    records[pos + pos_in_record..pos + pos_in_record + EVENTS_OFFSET_SIZE]
                        .copy_from_slice(&current_value_offset.to_le_bytes());
                    pos_in_record += EVENTS_OFFSET_SIZE;

                    // Events length
                    records[pos + pos_in_record..pos + pos_in_record + EVENTS_LEN_SIZE]
                        .copy_from_slice(&(offsets.len() as u32).to_le_bytes());

                    // Add to value data
                    value_data.extend(offsets_bytes);
                }
                PartitionOffsets::ExternalBucket => {
                    // Record entry for external bucket
                    let mut pos_in_record = 0;

                    // Partition ID
                    records[pos + pos_in_record..pos + pos_in_record + PARTITION_ID_SIZE]
                        .copy_from_slice(&partition_id.to_le_bytes());
                    pos_in_record += PARTITION_ID_SIZE;

                    // Sequence min
                    records[pos + pos_in_record..pos + pos_in_record + SEQUENCE_SIZE]
                        .copy_from_slice(&record.sequence_min.to_le_bytes());
                    pos_in_record += SEQUENCE_SIZE;

                    // Sequence max
                    records[pos + pos_in_record..pos + pos_in_record + SEQUENCE_SIZE]
                        .copy_from_slice(&record.sequence_max.to_le_bytes());
                    pos_in_record += SEQUENCE_SIZE;

                    // Event count
                    records[pos + pos_in_record..pos + pos_in_record + SEQUENCE_SIZE]
                        .copy_from_slice(&record.sequence.to_le_bytes());
                    pos_in_record += SEQUENCE_SIZE;

                    // Special value for external bucket (u64::MAX)
                    records[pos + pos_in_record..pos + pos_in_record + EVENTS_OFFSET_SIZE]
                        .copy_from_slice(&u64::MAX.to_le_bytes());
                    pos_in_record += EVENTS_OFFSET_SIZE;

                    // Special value for external bucket (u32::MAX)
                    records[pos + pos_in_record..pos + pos_in_record + EVENTS_LEN_SIZE]
                        .copy_from_slice(&u32::MAX.to_le_bytes());
                }
            }
        }

        // Build the file header
        // Magic marker ("PIDX"), number of keys, length of mph_bytes, then the
        // mph_bytes
        let mut file_data = Vec::with_capacity(4 + 8 + 8 + mphf_bytes.len() + records.len());
        file_data.extend_from_slice(b"PIDX"); // magic: 4 bytes
        file_data.extend_from_slice(&n.to_le_bytes()); // number of keys: 8 bytes
        file_data.extend_from_slice(&mphf_bytes_len.to_le_bytes()); // length of mph_bytes: 8 bytes
        file_data.extend_from_slice(&mphf_bytes); // serialized MPHF
        file_data.extend_from_slice(&records); // records array

        // Calculate records offset
        let records_offset = 4 + 8 + 8 + mphf_bytes.len() as u64;

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
    Cache(BTreeMap<PartitionId, PartitionIndexRecord<PartitionOffsets>>),
    Mphf {
        mphf: Mphf<PartitionId>,
        records_offset: u64,
    },
}

pub struct ClosedPartitionIndex {
    id: BucketSegmentId,
    file: File,
    index: Cache<Arc<ArcSwap<ClosedIndex>>, Arc<ClosedIndex>>,
}

impl ClosedPartitionIndex {
    pub fn open(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, PartitionIndexError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        // Read the magic marker
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;

        if &magic != b"PIDX" {
            return Err(PartitionIndexError::CorruptHeader);
        }

        // File is positioned after magic bytes
        let (mphf, _, records_offset) = load_index_from_file(&mut file)?;

        Ok(ClosedPartitionIndex {
            id,
            file,
            index: Cache::new(Arc::new(ArcSwap::new(Arc::new(ClosedIndex::Mphf {
                mphf,
                records_offset,
            })))),
        })
    }

    pub fn try_clone(&self) -> Result<Self, PartitionIndexError> {
        Ok(ClosedPartitionIndex {
            id: self.id,
            file: self.file.try_clone()?,
            index: self.index.clone(),
        })
    }

    pub fn get_key(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionIndexRecord<ClosedOffsetKind>>, PartitionIndexError> {
        match self.index.load().as_ref() {
            // Cache mode - this is used during transitions or legacy format
            ClosedIndex::Cache(cache) => {
                Ok(cache
                    .get(&partition_id)
                    .cloned()
                    .map(|record| PartitionIndexRecord {
                        sequence_min: record.sequence_min,
                        sequence_max: record.sequence_max,
                        sequence: record.sequence,
                        offsets: record.offsets.into(),
                    }))
            }
            // New MPHF-based lookup
            ClosedIndex::Mphf {
                mphf,
                records_offset,
            } => {
                // Try to compute the slot using the MPHF
                let Some(slot) = mphf.try_hash(&partition_id) else {
                    return Ok(None);
                };

                // Calculate the position in the file
                let pos = records_offset + (slot * RECORD_SIZE as u64);

                let file_size = self.file.metadata()?.len();
                if pos + RECORD_SIZE as u64 > file_size {
                    return Ok(None); // Position is out of bounds, treat as "not found"
                }

                // Read the record
                let mut buf = vec![0u8; RECORD_SIZE];
                self.file.read_exact_at(&mut buf, pos)?;

                // Extract the partition ID and verify it matches
                let stored_partition_id = PartitionId::from_le_bytes([buf[0], buf[1]]);

                // Double-check the partition ID - this is an important safety check
                // because the MPHF is defined only for keys that existed when it was built
                if stored_partition_id != partition_id {
                    return Ok(None);
                }

                // Extract the record fields
                let sequence_min = u64::from_le_bytes(buf[2..10].try_into().unwrap());
                let sequence_max = u64::from_le_bytes(buf[10..18].try_into().unwrap());
                let sequence = u64::from_le_bytes(buf[18..26].try_into().unwrap());
                let events_offset = u64::from_le_bytes(buf[26..34].try_into().unwrap());
                let events_len = u32::from_le_bytes(buf[34..38].try_into().unwrap());

                // Determine the type of offset
                let offsets = if events_offset == u64::MAX && events_len == u32::MAX {
                    ClosedOffsetKind::ExternalBucket
                } else {
                    ClosedOffsetKind::Pointer(events_offset, events_len)
                };

                // Return the record
                Ok(Some(PartitionIndexRecord {
                    sequence_min,
                    sequence_max,
                    sequence,
                    offsets,
                }))
            }
        }
    }

    pub fn get_from_key(
        &self,
        key: PartitionIndexRecord<ClosedOffsetKind>,
    ) -> Result<PartitionOffsets, PartitionIndexError> {
        match key.offsets {
            ClosedOffsetKind::Pointer(offset, len) => {
                // Read values from the file
                let mut values_buf = vec![0u8; len as usize * (SEQUENCE_SIZE + EVENTS_OFFSET_SIZE)];
                match self.file.read_exact_at(&mut values_buf, offset) {
                    Ok(_) => {
                        // Successfully read the values
                        let offsets = values_buf
                            .chunks_exact(SEQUENCE_SIZE + EVENTS_OFFSET_SIZE)
                            .map(|b| {
                                let sequence =
                                    u64::from_le_bytes(b[0..SEQUENCE_SIZE].try_into().unwrap());
                                let offset = u64::from_le_bytes(
                                    b[SEQUENCE_SIZE..SEQUENCE_SIZE + EVENTS_OFFSET_SIZE]
                                        .try_into()
                                        .unwrap(),
                                );
                                PartitionSequenceOffset { sequence, offset }
                            })
                            .collect();
                        Ok(PartitionOffsets::Offsets(offsets))
                    }
                    Err(err) => {
                        // If we can't read the values, use the offset and len for diagnostic info
                        Err(PartitionIndexError::Io(err))
                    }
                }
            }
            ClosedOffsetKind::Cached(offsets) => Ok(PartitionOffsets::Offsets(offsets)),
            ClosedOffsetKind::ExternalBucket => Ok(PartitionOffsets::ExternalBucket),
        }
    }

    pub fn get(
        &mut self,
        partition_id: PartitionId,
    ) -> Result<Option<PartitionOffsets>, PartitionIndexError> {
        self.get_key(partition_id)
            .and_then(|key| key.map(|key| self.get_from_key(key)).transpose())
    }
}

#[derive(Debug)]
pub struct PartitionEventIter {
    partition_id: PartitionId,
    bucket_id: BucketId,
    reader_pool: ReaderThreadPool,
    from_sequence: u64,
    segment_id: SegmentId,
    segment_offsets: VecDeque<PartitionSequenceOffset>,
    live_segment_id: SegmentId,
    live_segment_offsets: VecDeque<PartitionSequenceOffset>,
    next_offset: Option<NextOffset>,
    next_live_offset: Option<NextOffset>,
}

#[derive(Clone, Copy, Debug)]
struct NextOffset {
    offset: u64,
    segment_id: SegmentId,
}

impl PartitionEventIter {
    #[allow(clippy::type_complexity)]
    pub(crate) async fn new(
        partition_id: PartitionId,
        bucket_id: BucketId,
        reader_pool: ReaderThreadPool,
        live_indexes: &HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>,
        from_sequence: u64,
    ) -> Result<Self, PartitionIndexError> {
        let mut live_segment_id = 0;
        let mut live_segment_offsets: Vec<_> = match live_indexes.get(&bucket_id) {
            Some((current_live_segment_id, live_indexes)) => {
                let current_live_segment_id = current_live_segment_id.load(Ordering::Acquire);
                live_segment_id = current_live_segment_id;
                match live_indexes
                    .read()
                    .await
                    .partition_index
                    .get(partition_id)
                    .cloned()
                {
                    Some(PartitionIndexRecord {
                        sequence_min,
                        offsets: PartitionOffsets::Offsets(mut offsets),
                        ..
                    }) if sequence_min <= from_sequence => {
                        if from_sequence > 0 {
                            let split_point =
                                offsets.partition_point(|x| x.sequence < from_sequence);
                            offsets.drain(0..split_point); // Remove items before the from_sequence
                        }

                        let mut live_segment_offsets: VecDeque<_> = offsets.into();
                        let next_live_offset = live_segment_offsets.pop_front().map(
                            |PartitionSequenceOffset { offset, .. }| NextOffset {
                                offset,
                                segment_id: current_live_segment_id,
                            },
                        );
                        return Ok(PartitionEventIter {
                            partition_id,
                            bucket_id,
                            reader_pool,
                            from_sequence,
                            segment_id: current_live_segment_id,
                            segment_offsets: VecDeque::new(),
                            live_segment_id: current_live_segment_id,
                            live_segment_offsets,
                            next_offset: None,
                            next_live_offset,
                        });
                    }
                    Some(PartitionIndexRecord {
                        offsets: PartitionOffsets::Offsets(offsets),
                        ..
                    }) => Some(offsets),
                    Some(PartitionIndexRecord {
                        offsets: PartitionOffsets::ExternalBucket,
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

        if from_sequence > 0 {
            let split_point = live_segment_offsets.partition_point(|x| x.sequence < from_sequence);
            live_segment_offsets.drain(0..split_point); // Remove items before the from_sequence
        }

        let mut live_segment_offsets: VecDeque<_> = live_segment_offsets.into();

        // Find the earlierst segment and offsets
        let (reply_tx, reply_rx) = oneshot::channel();
        reader_pool.spawn({
            move |with_readers| {
                with_readers(move |readers| {
                    let res = readers
                        .get_mut(&bucket_id)
                        .and_then(|segments| {
                            segments.iter_mut().enumerate().rev().find_map(
                                |(i, (segment_id, reader_set))| {
                                    let Some(partition_index) = &mut reader_set.partition_index
                                    else {
                                        return None;
                                    };

                                    match partition_index.get_key(partition_id) {
                                        Ok(Some(key))
                                            if key.sequence_min <= from_sequence || i == 0 =>
                                        {
                                            match partition_index.get_from_key(key) {
                                                Ok(mut offsets) => {
                                                    if let PartitionOffsets::Offsets(offsets) =
                                                        &mut offsets
                                                    {
                                                        if from_sequence > 0 {
                                                            let split_point = offsets
                                                                .partition_point(|x| {
                                                                    x.sequence < from_sequence
                                                                });
                                                            offsets.drain(0..split_point); // Remove items before the from_sequence
                                                        }
                                                        if let Some(PartitionSequenceOffset {
                                                            offset,
                                                            ..
                                                        }) = offsets.first()
                                                        {
                                                            reader_set.reader.prefetch(*offset);
                                                        }
                                                    }
                                                    Some(Ok((*segment_id, offsets)))
                                                }
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
            Ok(Ok(Some((segment_id, PartitionOffsets::Offsets(segment_offsets))))) => {
                let mut segment_offsets: VecDeque<_> = segment_offsets.into();
                let next_offset = segment_offsets.pop_front().map(
                    |PartitionSequenceOffset { offset, .. }| NextOffset { offset, segment_id },
                );
                let next_live_offset = live_segment_offsets.pop_front().map(
                    |PartitionSequenceOffset { offset, .. }| NextOffset {
                        offset,
                        segment_id: live_segment_id,
                    },
                );

                Ok(PartitionEventIter {
                    partition_id,
                    bucket_id,
                    reader_pool,
                    from_sequence,
                    segment_id,
                    segment_offsets,
                    live_segment_id,
                    live_segment_offsets,
                    next_offset,
                    next_live_offset,
                })
            }
            Ok(Ok(Some((_, PartitionOffsets::ExternalBucket)))) | Ok(Ok(None)) | Err(_) => {
                let next_live_offset = live_segment_offsets.pop_front().map(
                    |PartitionSequenceOffset { offset, .. }| NextOffset {
                        offset,
                        segment_id: live_segment_id,
                    },
                );

                Ok(PartitionEventIter {
                    partition_id,
                    bucket_id,
                    reader_pool,
                    from_sequence,
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

    pub async fn next(
        &mut self,
        header_only: bool,
    ) -> Result<Option<CommittedEvents>, PartitionIndexError> {
        struct ReadResult {
            events: Option<CommittedEvents>,
            new_offsets: Option<(SegmentId, VecDeque<PartitionSequenceOffset>)>,
            is_live: bool,
        }

        let partition_id = self.partition_id;
        let bucket_id = self.bucket_id;
        let segment_id = self.segment_id;
        let from_sequence = self.from_sequence;
        let live_segment_id = self.live_segment_id;
        let next_offset = self.next_offset;
        let next_live_offset = self.next_live_offset;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.reader_pool.spawn(move |with_readers| {
            with_readers(move |readers| {
                let res = readers
                    .get_mut(&bucket_id)
                    .map(|segments| {
                        match next_offset {
                            Some(NextOffset {
                                offset, segment_id, ..
                            }) => {
                                // We have an offset from the last batch
                                match segments.get_mut(&segment_id) {
                                    Some(reader_set) => {
                                        let (events, _) = reader_set.reader.read_committed_events(
                                            offset,
                                            false,
                                            header_only,
                                        )?;

                                        Ok(ReadResult {
                                            events,
                                            new_offsets: None,
                                            is_live: false,
                                        })
                                    }
                                    None => Err(PartitionIndexError::SegmentNotFound {
                                        bucket_segment_id: BucketSegmentId::new(
                                            bucket_id, segment_id,
                                        ),
                                    }),
                                }
                            }
                            None => {
                                // There's no more offsets in this batch, progress forwards finding
                                // the next batch
                                for i in segment_id.saturating_add(1)
                                    ..(segments.len() as SegmentId).min(live_segment_id)
                                {
                                    let Some(reader_set) = segments.get_mut(&i) else {
                                        continue;
                                    };

                                    let Some(partition_index) = &mut reader_set.partition_index
                                    else {
                                        continue;
                                    };

                                    let mut new_offsets: VecDeque<_> = match partition_index
                                        .get(partition_id)?
                                    {
                                        Some(PartitionOffsets::Offsets(mut offsets)) => {
                                            if from_sequence > 0 {
                                                let split_point = offsets.partition_point(|x| {
                                                    x.sequence < from_sequence
                                                });
                                                offsets.drain(0..split_point); // Remove items before the from_sequence
                                            }

                                            offsets.into()
                                        }
                                        Some(PartitionOffsets::ExternalBucket) => {
                                            return Ok(ReadResult {
                                                events: None,
                                                new_offsets: None,
                                                is_live: false,
                                            });
                                        }
                                        None => {
                                            continue;
                                        }
                                    };

                                    let Some(next_offset) = new_offsets.pop_front() else {
                                        continue;
                                    };

                                    let (events, _) = reader_set.reader.read_committed_events(
                                        next_offset.offset,
                                        false,
                                        header_only,
                                    )?;

                                    return Ok(ReadResult {
                                        events,
                                        new_offsets: Some((i, new_offsets)),
                                        is_live: false,
                                    });
                                }

                                // No more batches found, we'll process the live offsets
                                match next_live_offset {
                                    Some(NextOffset {
                                        offset, segment_id, ..
                                    }) => {
                                        let Some(reader_set) = segments.get_mut(&segment_id) else {
                                            return Ok(ReadResult {
                                                events: None,
                                                new_offsets: None,
                                                is_live: true,
                                            });
                                        };

                                        let (events, _) = reader_set.reader.read_committed_events(
                                            offset,
                                            false,
                                            header_only,
                                        )?;

                                        Ok(ReadResult {
                                            events,
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
                    self.next_live_offset = self.live_segment_offsets.pop_front().map(
                        |PartitionSequenceOffset { offset, .. }| NextOffset {
                            offset,
                            segment_id: self.live_segment_id,
                        },
                    );
                    return Ok(events);
                }

                if let Some((new_segment, new_offsets)) = new_offsets {
                    self.segment_id = new_segment;
                    self.segment_offsets = new_offsets;
                    self.next_offset = self.segment_offsets.pop_front().map(
                        |PartitionSequenceOffset { offset, .. }| NextOffset {
                            offset,
                            segment_id: self.segment_id,
                        },
                    );
                } else {
                    self.next_offset = self.segment_offsets.pop_front().map(
                        |PartitionSequenceOffset { offset, .. }| NextOffset {
                            offset,
                            segment_id: self.segment_id,
                        },
                    );
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

/// Loads the MPHF-based index from a file format
///
/// The file format is:
///   [0..4]   : magic marker: b"PIDX"
///   [4..12]  : number of keys (n) as a u64
///   [12..20] : length of serialized MPHF (L) as a u64
///   [20..20+L] : serialized MPHF bytes (using bincode)
///   [20+L..] : records array, exactly n records of RECORD_SIZE bytes each.
fn load_index_from_file(
    file: &mut File,
) -> Result<(Mphf<PartitionId>, u64, u64), PartitionIndexError> {
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
    let (mph, _): (Mphf<PartitionId>, _) =
        bincode::serde::decode_from_slice(&mph_bytes, bincode::config::standard())
            .map_err(PartitionIndexError::DeserializeMphf)?;

    // The records array immediately follows
    let records_offset = 4 + 8 + 8 + mph_bytes_len as u64;

    Ok((mph, n, records_offset))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn temp_file_path() -> PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Create a unique filename for each test to avoid conflicts
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        tempfile::Builder::new()
            .prefix(&format!("test_{timestamp}_"))
            .suffix(".pidx")
            .tempfile()
            .unwrap()
            .into_temp_path()
            .to_path_buf()
    }

    #[test]
    fn test_open_partition_index_insert_and_get() {
        let path = temp_file_path();
        let mut index = OpenPartitionIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();

        let partition_id = 42;
        let offsets = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 1000,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 2000,
            },
            PartitionSequenceOffset {
                sequence: 2,
                offset: 3000,
            },
        ];

        for (i, offset) in offsets.iter().enumerate() {
            index.insert(partition_id, i as u64, offset.offset).unwrap();
        }

        let record = index.get(partition_id).unwrap();
        assert_eq!(
            record,
            &PartitionIndexRecord {
                sequence_min: 0,
                sequence_max: 2,
                sequence: 3,
                offsets: PartitionOffsets::Offsets(offsets),
            }
        );
        assert_eq!(index.get(99), None);
    }

    #[test]
    fn test_closed_partition_index_lookup() {
        let path = temp_file_path();
        let mut index = OpenPartitionIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();

        let partition_id1 = 1;
        let partition_id2 = 2;
        let offsets1 = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 100,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 200,
            },
        ];
        let offsets2 = vec![PartitionSequenceOffset {
            sequence: 0,
            offset: 300,
        }];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(partition_id1, i as u64, offset.offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(partition_id2, i as u64, offset.offset)
                .unwrap();
        }

        // Flush with the new MPHF format
        let (mphf, _) = index.flush().unwrap();
        assert!(mphf.try_hash(&partition_id1).is_some());
        assert!(mphf.try_hash(&partition_id2).is_some());
        assert!(mphf.try_hash(&999).is_none());

        // Open with the closed index
        let mut closed_index =
            ClosedPartitionIndex::open(BucketSegmentId::new(0, 0), &path).unwrap();

        // Test get_key with MPHF implementation
        let key1 = closed_index.get_key(partition_id1).unwrap().unwrap();
        assert_eq!(key1.sequence_min, 0);
        assert_eq!(key1.sequence_max, 1);
        assert_eq!(key1.sequence, 2);

        // Test get with MPHF implementation
        assert_eq!(
            closed_index.get(partition_id1).unwrap(),
            Some(PartitionOffsets::Offsets(offsets1)),
        );

        // Test get_key for second partition ID
        let key2 = closed_index.get_key(partition_id2).unwrap().unwrap();
        assert_eq!(key2.sequence_min, 0);
        assert_eq!(key2.sequence_max, 0);
        assert_eq!(key2.sequence, 1);

        // Test get for second partition ID
        assert_eq!(
            closed_index.get(partition_id2).unwrap(),
            Some(PartitionOffsets::Offsets(offsets2)),
        );

        // Test unknown partition ID
        assert_eq!(closed_index.get_key(999).unwrap(), None);
        assert_eq!(closed_index.get(999).unwrap(), None);
    }

    #[test]
    fn test_mphf_collision_handling() {
        let path = temp_file_path();
        let mut index = OpenPartitionIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();

        // Add multiple partitions to test MPHF collision handling
        let partition_id1 = 1;
        let partition_id2 = 2;
        let partition_id3 = 3;
        let partition_id4 = 4;

        let offsets1 = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 1001,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 1002,
            },
        ];
        let offsets2 = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 2001,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 2002,
            },
            PartitionSequenceOffset {
                sequence: 2,
                offset: 2003,
            },
        ];
        let offsets3 = vec![
            PartitionSequenceOffset {
                sequence: 0,
                offset: 3001,
            },
            PartitionSequenceOffset {
                sequence: 1,
                offset: 3002,
            },
        ];
        let offsets4 = vec![PartitionSequenceOffset {
            sequence: 0,
            offset: 4001,
        }];

        for (i, offset) in offsets1.iter().enumerate() {
            index
                .insert(partition_id1, i as u64, offset.offset)
                .unwrap();
        }
        for (i, offset) in offsets2.iter().enumerate() {
            index
                .insert(partition_id2, i as u64, offset.offset)
                .unwrap();
        }
        for (i, offset) in offsets3.iter().enumerate() {
            index
                .insert(partition_id3, i as u64, offset.offset)
                .unwrap();
        }
        for (i, offset) in offsets4.iter().enumerate() {
            index
                .insert(partition_id4, i as u64, offset.offset)
                .unwrap();
        }

        // Flush with the new MPHF format
        let (mphf, _) = index.flush().unwrap();

        // Verify MPHF correctness - all partitions should have different hash values
        let hash1 = mphf.hash(&partition_id1);
        let hash2 = mphf.hash(&partition_id2);
        let hash3 = mphf.hash(&partition_id3);
        let hash4 = mphf.hash(&partition_id4);

        assert_ne!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_ne!(hash1, hash4);
        assert_ne!(hash2, hash3);
        assert_ne!(hash2, hash4);
        assert_ne!(hash3, hash4);

        // Test with closed index
        let mut closed_index =
            ClosedPartitionIndex::open(BucketSegmentId::new(0, 0), &path).unwrap();

        // Verify all partitions can be retrieved correctly
        assert_eq!(
            closed_index.get(partition_id1).unwrap(),
            Some(PartitionOffsets::Offsets(offsets1))
        );
        assert_eq!(
            closed_index.get(partition_id2).unwrap(),
            Some(PartitionOffsets::Offsets(offsets2))
        );
        assert_eq!(
            closed_index.get(partition_id3).unwrap(),
            Some(PartitionOffsets::Offsets(offsets3))
        );
        assert_eq!(
            closed_index.get(partition_id4).unwrap(),
            Some(PartitionOffsets::Offsets(offsets4))
        );

        // Unknown partition ID should not be found
        assert_eq!(closed_index.get(999).unwrap(), None);
    }

    #[test]
    fn test_insert_external_bucket() {
        let path = temp_file_path();

        let partition_id = 123;

        let mut index = OpenPartitionIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();
        index.insert_external_bucket(partition_id).unwrap();
        assert_eq!(
            index.get(partition_id),
            Some(&PartitionIndexRecord {
                sequence_min: 0,
                sequence_max: 0,
                sequence: 0,
                offsets: PartitionOffsets::ExternalBucket
            })
        );
        index.flush().unwrap();

        let mut index = ClosedPartitionIndex::open(BucketSegmentId::new(0, 0), &path).unwrap();
        assert_eq!(
            index.get(partition_id).unwrap(),
            Some(PartitionOffsets::ExternalBucket)
        );
    }
}
