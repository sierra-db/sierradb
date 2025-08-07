//! The file format for an MPHF-based index is defined as follows:
//! - `[0..4]`     : magic marker: `b"EIDX"`
//! - `[4..12]`    : number of keys (n) as a `u64`
//! - `[12..20]`   : length of serialized MPHF (L) as a `u64`
//! - `[20..20+L]` : serialized MPHF bytes (using bincode)
//! - `[20+L..]`   : records array, exactly n records of RECORD_SIZE bytes each.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::mem;
use std::os::unix::fs::FileExt;
use std::panic::panic_any;
use std::path::Path;
use std::sync::Arc;

use arc_swap::{ArcSwap, Cache};
use boomphf::Mphf;
use rayon::ThreadPool;
use uuid::Uuid;

use super::BucketSegmentId;
use super::segment::{BucketSegmentReader, EventRecord, Record};
use crate::error::{EventIndexError, ThreadPoolError};

// Each record is 16 bytes for the Uuid and 8 bytes for the offset.
const RECORD_SIZE: usize = mem::size_of::<Uuid>() + mem::size_of::<u64>();

const MPHF_GAMMA: f64 = 1.4;

#[derive(Debug)]
pub struct OpenEventIndex {
    id: BucketSegmentId,
    file: File,
    index: BTreeMap<Uuid, u64>,
}

impl OpenEventIndex {
    pub fn create(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, EventIndexError> {
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(path)?;
        let index = BTreeMap::new();

        Ok(OpenEventIndex { id, file, index })
    }

    pub fn open(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, EventIndexError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let index = BTreeMap::new();

        Ok(OpenEventIndex { id, file, index })
    }

    /// Closes the event index, flushing the index in a background thread.
    pub fn close(self, pool: &ThreadPool) -> Result<ClosedEventIndex, EventIndexError> {
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
                        panic_any(ThreadPoolError::FlushEventIndex {
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

        Ok(ClosedEventIndex {
            id,
            file: self.file,
            index: Cache::new(index),
        })
    }

    pub fn get(&self, event_id: &Uuid) -> Option<u64> {
        self.index.get(event_id).copied()
    }

    pub fn insert(&mut self, event_id: Uuid, offset: u64) -> Option<u64> {
        self.index.insert(event_id, offset)
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&Uuid, &mut u64) -> bool,
    {
        self.index.retain(f);
    }

    pub fn flush(&mut self) -> Result<(Mphf<Uuid>, u64), EventIndexError> {
        Self::flush_inner(&mut self.file, &self.index)
    }

    /// Hydrates the index from a reader.
    pub fn hydrate(&mut self, reader: &mut BucketSegmentReader) -> Result<(), EventIndexError> {
        let mut reader_iter = reader.iter();
        while let Some(record) = reader_iter.next_record()? {
            match record {
                Record::Event(EventRecord {
                    offset, event_id, ..
                }) => {
                    self.insert(event_id, offset);
                }
                Record::Commit(_) => {}
            }
        }

        Ok(())
    }

    fn flush_inner(
        file: &mut File,
        index: &BTreeMap<Uuid, u64>,
    ) -> Result<(Mphf<Uuid>, u64), EventIndexError> {
        // Collect all keys from the index.
        let keys: Vec<Uuid> = index.keys().cloned().collect();
        let n = keys.len() as u64;

        // Build the MPHF over the keys. The parameter (gamma) controls the
        // space/performance trade-off.
        let mphf = Mphf::new(MPHF_GAMMA, &keys);

        // Serialize the MPHF structure.
        let mphf_bytes = bincode::serde::encode_to_vec(&mphf, bincode::config::standard())
            .map_err(EventIndexError::SerializeMphf)?;
        let mphf_bytes_len = mphf_bytes.len() as u64;

        // Allocate a records array for exactly n records.
        let mut records = vec![0u8; index.len() * RECORD_SIZE];

        // Place each record in its slot according to the MPHF.
        // Since the MPHF is perfect, each key maps to a unique slot.
        for (&event_id, &offset) in index {
            let slot = mphf.hash(&event_id) as usize;
            let pos = slot * RECORD_SIZE;
            records[pos..pos + 16].copy_from_slice(event_id.as_bytes());
            records[pos + 16..pos + 24].copy_from_slice(&offset.to_le_bytes());
        }

        // Build the file header.
        // Magic marker ("MPHF"), number of keys, length of mph_bytes, then the
        // mph_bytes.
        let mut file_data = Vec::with_capacity(20 + mphf_bytes.len() + records.len());
        file_data.extend_from_slice(b"EIDX"); // magic: 4 bytes
        file_data.extend_from_slice(&n.to_le_bytes()); // number of keys: 8 bytes
        file_data.extend_from_slice(&mphf_bytes_len.to_le_bytes()); // length of mph_bytes: 8 bytes
        file_data.extend_from_slice(&mphf_bytes); // serialized MPHF
        file_data.extend_from_slice(&records); // records array

        file.write_all_at(&file_data, 0)?;
        file.flush()?;

        Ok((mphf, 4 + 8 + 8 + mphf_bytes_len))
    }
}

pub struct ClosedEventIndex {
    id: BucketSegmentId,
    file: File,
    index: Cache<Arc<ArcSwap<ClosedIndex>>, Arc<ClosedIndex>>,
}

#[derive(Debug)]
pub enum ClosedIndex {
    Cache(BTreeMap<Uuid, u64>),
    Mphf {
        mphf: Mphf<Uuid>,
        records_offset: u64,
    },
}

impl ClosedEventIndex {
    pub fn open(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, EventIndexError> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let (mphf, _, records_offset) = load_index_from_file(&mut file)?;

        Ok(ClosedEventIndex {
            id,
            file,
            index: Cache::new(Arc::new(ArcSwap::new(Arc::new(ClosedIndex::Mphf {
                mphf,
                records_offset,
            })))),
        })
    }

    pub fn try_clone(&self) -> Result<Self, EventIndexError> {
        Ok(ClosedEventIndex {
            id: self.id,
            file: self.file.try_clone()?,
            index: self.index.clone(),
        })
    }

    pub fn get(&mut self, event_id: &Uuid) -> Result<Option<u64>, EventIndexError> {
        match self.index.load().as_ref() {
            ClosedIndex::Cache(index) => Ok(index.get(event_id).copied()),
            ClosedIndex::Mphf {
                mphf,
                records_offset,
            } => {
                // Compute the slot using the MPHF.
                let Some(slot) = mphf.try_hash(event_id) else {
                    return Ok(None);
                };
                let pos = records_offset + slot * RECORD_SIZE as u64;
                let mut buf = [0u8; RECORD_SIZE];
                self.file.read_exact_at(&mut buf, pos)?;
                let stored_uuid = Uuid::from_bytes(buf[..16].try_into().unwrap());

                // Because the MPHF is defined only on the keys present at build time,
                // if the key is not in the set you might read a record that doesn’t match.
                if stored_uuid.is_nil() || &stored_uuid != event_id {
                    return Ok(None);
                }
                let offset = u64::from_le_bytes(buf[16..24].try_into().unwrap());

                Ok(Some(offset))
            }
        }
    }
}

/// Loads the index from a direct-mapped file format
fn load_index_from_file(file: &mut File) -> Result<(Mphf<Uuid>, u64, u64), EventIndexError> {
    // Read magic marker.
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;
    if &magic != b"EIDX" {
        return Err(EventIndexError::CorruptHeader);
    }

    // Read number of keys.
    let mut n_buf = [0u8; 8];
    file.read_exact(&mut n_buf)?;
    let n = u64::from_le_bytes(n_buf);

    // Read length of serialized MPHF.
    let mut mph_len_buf = [0u8; 8];
    file.read_exact(&mut mph_len_buf)?;
    let mph_bytes_len = u64::from_le_bytes(mph_len_buf) as usize;

    // Read the MPHF bytes and deserialize.
    let mut mph_bytes = vec![0u8; mph_bytes_len];
    file.read_exact(&mut mph_bytes)?;
    let (mph, _): (Mphf<Uuid>, _) =
        bincode::serde::decode_from_slice(&mph_bytes, bincode::config::standard())
            .map_err(EventIndexError::DeserializeMphf)?;

    // The records array immediately follows.
    let records_offset = 4 + 8 + 8 + mph_bytes_len as u64;

    Ok((mph, n, records_offset))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

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
        let mut index = OpenEventIndex::create(BucketSegmentId::new(0, 0), path).unwrap();

        let event_id = Uuid::new_v4();
        let offset = 12345;
        index.insert(event_id, offset);

        assert_eq!(index.get(&event_id), Some(offset));
        assert_eq!(index.get(&Uuid::new_v4()), None);
    }

    #[test]
    fn test_closed_event_index_lookup() {
        let path = temp_file_path();
        let mut index = OpenEventIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();

        let event_id1 = Uuid::new_v4();
        let event_id2 = Uuid::new_v4();
        let offset1 = 54321;
        let offset2 = 98765;

        index.insert(event_id1, offset1);
        index.insert(event_id2, offset2);
        index.flush().unwrap();

        let mut closed_index = ClosedEventIndex::open(BucketSegmentId::new(0, 0), &path).unwrap();
        assert_eq!(closed_index.get(&event_id1).unwrap(), Some(offset1));
        assert_eq!(closed_index.get(&event_id2).unwrap(), Some(offset2));
        assert_eq!(closed_index.get(&Uuid::new_v4()).unwrap(), None);
    }

    #[test]
    fn test_collision_handling_in_direct_mapping() {
        let path = temp_file_path();
        let mut index = OpenEventIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();

        let event_id1 = Uuid::from_bytes([1, 4, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let event_id2 = Uuid::from_bytes([1, 4, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let offset1 = 10101;
        let offset2 = 20202;

        // Insert two items that may hash to the same slot
        index.insert(event_id1, offset1);
        index.insert(event_id2, offset2);
        index.flush().unwrap();

        let mut closed_index = ClosedEventIndex::open(BucketSegmentId::new(0, 0), &path).unwrap();
        assert_eq!(closed_index.get(&event_id1).unwrap(), Some(offset1));
        assert_eq!(closed_index.get(&event_id2).unwrap(), Some(offset2));
    }

    #[test]
    fn test_non_existent_event_lookup() {
        let path = temp_file_path();

        let mut index = OpenEventIndex::create(BucketSegmentId::new(0, 0), &path).unwrap();
        index.flush().unwrap();

        let mut index = ClosedEventIndex::open(BucketSegmentId::new(0, 0), path).unwrap();

        let unknown_event_id = Uuid::new_v4();
        assert_eq!(index.get(&unknown_event_id).unwrap(), None);
    }
}
