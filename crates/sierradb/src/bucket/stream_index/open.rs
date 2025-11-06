use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::panic::panic_any;
use std::path::Path;
use std::sync::Arc;

use arc_swap::{ArcSwap, Cache};
use bincode::config;
use bloomfilter::Bloom;
use boomphf::Mphf;
use rayon::ThreadPool;
use uuid::Uuid;

use crate::bucket::BucketSegmentId;
use crate::bucket::segment::{BucketSegmentReader, EventRecord, Record};
use crate::bucket::stream_index::closed::{ClosedIndex, ClosedStreamIndex};
use crate::bucket::stream_index::{
    LEN_SIZE, OFFSET_SIZE, PARTITION_KEY_SIZE, RECORD_SIZE, StreamIndexRecord, StreamOffsets,
    VERSION_SIZE,
};
use crate::error::{EventValidationError, StreamIndexError, ThreadPoolError};
use crate::{BLOOM_SEED, STREAM_ID_SIZE, StreamId};

const AVG_EVENT_SIZE: usize = 350;
const AVG_EVENTS_PER_STREAM: usize = 10;
const FALSE_POSITIVE_PROBABILITY: f64 = 0.001;
const MPHF_GAMMA: f64 = 1.4;

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

        Ok(ClosedStreamIndex::new(
            id,
            self.file,
            Cache::new(index),
            bloom,
        ))
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
                        EventValidationError::PartitionKeyMismatch {
                            existing_partition_key: entry.partition_key,
                            new_partition_key: partition_key,
                        },
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
                        EventValidationError::PartitionKeyMismatch {
                            existing_partition_key: entry.partition_key,
                            new_partition_key: partition_key,
                        },
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
