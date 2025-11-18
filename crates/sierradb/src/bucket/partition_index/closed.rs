use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use arc_swap::{ArcSwap, Cache};
use boomphf::Mphf;

use super::{
    EVENTS_OFFSET_SIZE, PartitionIndexRecord, PartitionOffsets, PartitionSequenceOffset,
    RECORD_SIZE, SEQUENCE_SIZE,
};
use crate::bucket::{BucketSegmentId, PartitionId};
use crate::error::PartitionIndexError;

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
    pub(super) fn new(
        id: BucketSegmentId,
        file: File,
        index: Cache<Arc<ArcSwap<ClosedIndex>>, Arc<ClosedIndex>>,
    ) -> Self {
        ClosedPartitionIndex { id, file, index }
    }

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
