use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use arc_swap::{ArcSwap, Cache};
use bloomfilter::Bloom;
use boomphf::Mphf;
use uuid::Uuid;

use crate::bucket::BucketSegmentId;
use crate::bucket::stream_index::{RECORD_SIZE, StreamIndexRecord};
use crate::error::StreamIndexError;
use crate::{STREAM_ID_SIZE, StreamId, from_bytes};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClosedOffsetKind {
    Pointer(u64, u32), // Its in the file at this location
    Cached(Vec<u64>),  // Its cached
}

#[derive(Debug)]
pub enum ClosedIndex {
    Cache(BTreeMap<StreamId, StreamIndexRecord<Vec<u64>>>),
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
    pub(crate) fn new(
        id: BucketSegmentId,
        file: File,
        index: Cache<Arc<ArcSwap<ClosedIndex>>, Arc<ClosedIndex>>,
        bloom: Arc<Bloom<str>>,
    ) -> Self {
        ClosedStreamIndex {
            id,
            file,
            index,
            bloom,
        }
    }

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
                        offsets: ClosedOffsetKind::Cached(record.offsets),
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

                let offsets = ClosedOffsetKind::Pointer(offset, len);

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
    ) -> Result<Vec<u64>, StreamIndexError> {
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
                        Ok(offsets)
                    }
                    Err(err) => {
                        // If we can't read the values, use the offset and len for diagnostic info
                        Err(StreamIndexError::Io(err))
                    }
                }
            }
            ClosedOffsetKind::Cached(offsets) => Ok(offsets),
        }
    }

    pub fn get(&mut self, stream_id: &str) -> Result<Option<Vec<u64>>, StreamIndexError> {
        self.get_key(stream_id)
            .and_then(|key| key.map(|key| self.get_from_key(key)).transpose())
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
