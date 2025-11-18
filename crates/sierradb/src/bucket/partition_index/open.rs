use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::panic::panic_any;
use std::path::Path;
use std::sync::Arc;

use arc_swap::{ArcSwap, Cache};
use bincode::config;
use boomphf::Mphf;
use rayon::ThreadPool;

use super::{
    ClosedIndex, ClosedPartitionIndex, EVENTS_LEN_SIZE, EVENTS_OFFSET_SIZE, MPHF_GAMMA,
    PARTITION_ID_SIZE, PartitionIndexRecord, PartitionOffsets, PartitionSequenceOffset,
    RECORD_SIZE, SEQUENCE_SIZE,
};
use crate::bucket::segment::{BucketSegmentReader, EventRecord, Record};
use crate::bucket::{BucketSegmentId, PartitionId};
use crate::error::{PartitionIndexError, ThreadPoolError};

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

        Ok(ClosedPartitionIndex::new(id, self.file, Cache::new(index)))
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
        while let Some(record) = reader_iter.next_record()? {
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
