use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
    os::unix::fs::FileExt,
    path::Path,
};

use uuid::Uuid;

use crate::RANDOM_STATE;

use super::segment::{BucketSegmentReader, Record};

const RECORD_SIZE: usize = mem::size_of::<Uuid>() + mem::size_of::<u64>();

pub struct OpenEventIndex {
    file: File,
    index: BTreeMap<Uuid, u64>,
}

impl OpenEventIndex {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .create_new(true)
            .open(path)?;
        let index = BTreeMap::new();

        Ok(OpenEventIndex { file, index })
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let index = load_index_from_file(&mut file)?;

        Ok(OpenEventIndex { file, index })
    }

    pub fn get(&self, event_id: &Uuid) -> Option<u64> {
        self.index.get(event_id).copied()
    }

    pub fn insert(&mut self, event_id: Uuid, offset: u64) -> Option<u64> {
        self.index.insert(event_id, offset)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let num_slots = self.index.len() * 2;
        let mut file_data = vec![0u8; 8 + num_slots * RECORD_SIZE];

        // Write number of slots at the start
        file_data[..8].copy_from_slice(&(num_slots as u64).to_le_bytes());

        for (&event_id, &offset) in &self.index {
            let mut slot = RANDOM_STATE.hash_one(event_id) % num_slots as u64;

            // Linear probing to find an empty slot
            loop {
                let pos = 8 + (slot * RECORD_SIZE as u64) as usize;
                let existing_uuid = Uuid::from_bytes(file_data[pos..pos + 16].try_into().unwrap());

                if existing_uuid.is_nil() {
                    // Write UUID and offset
                    file_data[pos..pos + 16].copy_from_slice(event_id.as_bytes());
                    file_data[pos + 16..pos + 24].copy_from_slice(&offset.to_le_bytes());
                    break;
                }

                slot = (slot + 1) % num_slots as u64;
            }
        }

        // Flush the whole file at once
        self.file.write_all_at(&file_data, 0)?;
        self.file.flush()
    }

    /// Hydrates the index from a reader
    pub fn hydrate(&mut self, reader: &mut BucketSegmentReader) -> io::Result<()> {
        let mut reader_iter = reader.iter();
        while let Some((offset, record)) = reader_iter.next_record()? {
            match record {
                Record::Event { event_id, .. } => {
                    self.insert(event_id, offset);
                }
                Record::Commit { .. } => {}
            }
        }

        Ok(())
    }
}

pub struct ClosedEventIndex {
    file: File,
    num_slots: u64,
}

impl ClosedEventIndex {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;

        // Read the first 8 bytes to get the total number of slots
        let mut count_buf = [0u8; 8];
        file.read_exact(&mut count_buf)?;
        let num_slots = u64::from_le_bytes(count_buf);

        Ok(ClosedEventIndex { file, num_slots })
    }

    pub fn get(&self, event_id: &Uuid) -> io::Result<Option<u64>> {
        if self.num_slots == 0 {
            return Ok(None);
        }

        // Compute slot index
        let mut slot = RANDOM_STATE.hash_one(event_id) % self.num_slots;

        let mut read_buf = [0u8; RECORD_SIZE * 2];
        let mut buf: &[u8] = &[];

        // Try to find the key using linear probing
        for _ in 0..self.num_slots {
            if buf.len() < RECORD_SIZE {
                let mut pos = 8 + slot * RECORD_SIZE as u64;
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

            let stored_uuid = Uuid::from_bytes(buf[..16].try_into().unwrap());
            let offset = u64::from_le_bytes(buf[16..16 + 8].try_into().unwrap());

            if stored_uuid.is_nil() {
                return Ok(None);
            }
            if &stored_uuid == event_id {
                return Ok(Some(offset));
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
}

/// Loads the index from a direct-mapped file format
fn load_index_from_file(file: &mut File) -> io::Result<BTreeMap<Uuid, u64>> {
    let mut file_data = Vec::with_capacity(file.metadata()?.len() as usize);
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut file_data)?;

    if file_data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "invalid event index file",
        ));
    }

    let num_slots = u64::from_le_bytes(file_data[..8].try_into().unwrap()) as usize;

    let mut index = BTreeMap::new();

    for i in 0..num_slots {
        let pos = 8 + RECORD_SIZE * i;

        if file_data.len() < pos + RECORD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "invalid event index file",
            ));
        }

        let uuid = Uuid::from_bytes(file_data[pos..pos + 16].try_into().unwrap());
        let offset = u64::from_le_bytes(file_data[pos + 16..pos + 16 + 8].try_into().unwrap());

        if uuid.is_nil() {
            continue;
        }

        index.insert(uuid, offset);
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Seek, SeekFrom},
        path::PathBuf,
    };

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
        let mut index = OpenEventIndex::create(path).unwrap();

        let event_id = Uuid::new_v4();
        let offset = 12345;
        index.insert(event_id, offset);

        assert_eq!(index.get(&event_id), Some(offset));
        assert_eq!(index.get(&Uuid::new_v4()), None);
    }

    #[test]
    fn test_open_event_index_flush_and_reopen() {
        let path = temp_file_path();
        let mut index = OpenEventIndex::create(&path).unwrap();

        let event_id1 = Uuid::new_v4();
        let event_id2 = Uuid::new_v4();
        let offset1 = 11111;
        let offset2 = 22222;

        index.insert(event_id1, offset1);
        index.insert(event_id2, offset2);
        index.flush().unwrap();

        // Ensure the file is written by seeking back to the beginning
        index.file.seek(SeekFrom::Start(0)).unwrap();

        // Reopen and verify data is still present
        let reopened_index = OpenEventIndex::open(&path).unwrap();
        assert_eq!(reopened_index.get(&event_id1), Some(offset1));
        assert_eq!(reopened_index.get(&event_id2), Some(offset2));
        assert_eq!(reopened_index.get(&Uuid::new_v4()), None);
    }

    #[test]
    fn test_closed_event_index_lookup() {
        let path = temp_file_path();
        let mut index = OpenEventIndex::create(&path).unwrap();

        let event_id1 = Uuid::new_v4();
        let event_id2 = Uuid::new_v4();
        let offset1 = 54321;
        let offset2 = 98765;

        index.insert(event_id1, offset1);
        index.insert(event_id2, offset2);
        index.flush().unwrap();

        let closed_index = ClosedEventIndex::open(&path).unwrap();
        assert_eq!(closed_index.get(&event_id1).unwrap(), Some(offset1));
        assert_eq!(closed_index.get(&event_id2).unwrap(), Some(offset2));
        assert_eq!(closed_index.get(&Uuid::new_v4()).unwrap(), None);
    }

    #[test]
    fn test_collision_handling_in_direct_mapping() {
        let path = temp_file_path();
        let mut index = OpenEventIndex::create(&path).unwrap();

        let event_id1 = Uuid::from_bytes([1, 4, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let event_id2 = Uuid::from_bytes([1, 4, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let offset1 = 10101;
        let offset2 = 20202;

        // Insert two items that may hash to the same slot
        index.insert(event_id1, offset1);
        index.insert(event_id2, offset2);
        index.flush().unwrap();

        let closed_index = ClosedEventIndex::open(&path).unwrap();
        assert_eq!(closed_index.get(&event_id1).unwrap(), Some(offset1));
        assert_eq!(closed_index.get(&event_id2).unwrap(), Some(offset2));
    }

    #[test]
    fn test_non_existent_event_lookup() {
        let path = temp_file_path();

        let mut index = OpenEventIndex::create(&path).unwrap();
        index.flush().unwrap();

        let index = ClosedEventIndex::open(path).unwrap();

        let unknown_event_id = Uuid::new_v4();
        assert_eq!(index.get(&unknown_event_id).unwrap(), None);
    }
}
