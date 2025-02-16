use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    mem,
    os::unix::fs::FileExt,
    path::Path,
    sync::Arc,
};

use crate::RANDOM_STATE;

const KEY_SIZE: usize = 64;
const RECORD_SIZE: usize = KEY_SIZE + mem::size_of::<u64>() + mem::size_of::<u32>();

pub struct OpenStreamIndex {
    file: File,
    index: BTreeMap<Arc<str>, Vec<u64>>,
}

impl OpenStreamIndex {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let index = BTreeMap::new();

        Ok(OpenStreamIndex { file, index })
    }

    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let index = load_index_from_file(&mut file)?;

        Ok(OpenStreamIndex { file, index })
    }

    pub fn get(&self, stream_id: &str) -> Option<&Vec<u64>> {
        self.index.get(stream_id)
    }

    pub fn insert(&mut self, stream_id: impl Into<Arc<str>>, offset: u64) {
        self.index.entry(stream_id.into()).or_default().push(offset);
    }

    pub fn flush(&mut self) -> io::Result<()> {
        let num_slots = self.index.len() * 2;
        let mut file_data = vec![0u8; 8 + num_slots * RECORD_SIZE];
        let mut offset = 8 + num_slots as u64 * RECORD_SIZE as u64;

        let mut value_data = Vec::new();

        file_data[..8].copy_from_slice(&(num_slots as u64).to_le_bytes());

        for (stream_id, values) in &self.index {
            let mut slot = RANDOM_STATE.hash_one(stream_id) % num_slots as u64;
            dbg!(stream_id, slot);
            let values_len = values.len() as u32;

            loop {
                let pos = 8 + (slot * RECORD_SIZE as u64) as usize;
                let existing_key = &file_data[pos..pos + KEY_SIZE];

                if existing_key.iter().all(|&b| b == 0) {
                    let mut key_bytes = [0u8; KEY_SIZE];
                    let bytes = stream_id.as_bytes();
                    key_bytes[..bytes.len()].copy_from_slice(bytes);

                    file_data[pos..pos + KEY_SIZE].copy_from_slice(&key_bytes);
                    file_data[pos + KEY_SIZE..pos + KEY_SIZE + 8]
                        .copy_from_slice(&offset.to_le_bytes());
                    file_data[pos + KEY_SIZE + 8..pos + KEY_SIZE + 12]
                        .copy_from_slice(&values_len.to_le_bytes());
                    break;
                }

                slot = (slot + 1) % num_slots as u64;
            }

            value_data.extend(values.iter().flat_map(|v| v.to_le_bytes()));
            offset += (values.len() * 8) as u64;
        }

        self.file.write_all_at(&file_data, 0)?;
        self.file
            .write_all_at(&value_data, 8 + num_slots as u64 * RECORD_SIZE as u64)?;
        self.file.flush()
    }
}

pub struct ClosedStreamIndex {
    file: File,
    num_slots: u64,
}

impl ClosedStreamIndex {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut count_buf = [0u8; 8];
        file.read_exact(&mut count_buf)?;
        let num_slots = u64::from_le_bytes(count_buf);

        Ok(ClosedStreamIndex { file, num_slots })
    }

    pub fn get(&self, stream_id: &str) -> io::Result<Option<Vec<u64>>> {
        if self.num_slots == 0 {
            return Ok(None);
        }

        // Compute slot index
        let mut slot = RANDOM_STATE.hash_one(stream_id) % self.num_slots;

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

            let stored_stream_id = std::str::from_utf8(&buf[0..KEY_SIZE])
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF8 stream id"))?
                .trim_end_matches('\0');
            let offset = u64::from_le_bytes(buf[KEY_SIZE..KEY_SIZE + 8].try_into().unwrap());
            let len = u32::from_le_bytes(buf[KEY_SIZE + 8..KEY_SIZE + 8 + 4].try_into().unwrap())
                as usize;

            if stored_stream_id.is_empty() {
                return Ok(None);
            }
            if stored_stream_id == stream_id {
                let mut values_buf = vec![0u8; len * 8];
                self.file.read_exact_at(&mut values_buf, offset)?;
                let values = values_buf
                    .chunks_exact(8)
                    .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                    .collect();
                return Ok(Some(values));
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

fn load_index_from_file(file: &mut File) -> io::Result<BTreeMap<Arc<str>, Vec<u64>>> {
    let mut file_data = Vec::with_capacity(file.metadata()?.len() as usize);
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut file_data)?;

    if file_data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "invalid stream index file",
        ));
    }

    let num_slots = u64::from_le_bytes(file_data[..8].try_into().unwrap()) as usize;
    let slot_section_size = num_slots * RECORD_SIZE;

    if file_data.len() < 8 + slot_section_size {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "corrupted stream index file",
        ));
    }

    let mut index = BTreeMap::new();

    for i in 0..num_slots {
        let pos = 8 + (i * RECORD_SIZE);

        if file_data.len() < pos + RECORD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "invalid stream index file",
            ));
        }

        let key_bytes = &file_data[pos..pos + KEY_SIZE];

        if key_bytes.iter().all(|&b| b == 0) {
            continue; // Skip empty slots
        }

        let stream_id = String::from_utf8(
            key_bytes
                .iter()
                .take_while(|&&b| b != 0)
                .copied()
                .collect::<Vec<_>>(),
        )
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF8 stream id"))?;

        let offset = u64::from_le_bytes(
            file_data[pos + KEY_SIZE..pos + KEY_SIZE + 8]
                .try_into()
                .unwrap(),
        );
        let length = u32::from_le_bytes(
            file_data[pos + KEY_SIZE + 8..pos + KEY_SIZE + 8 + 4]
                .try_into()
                .unwrap(),
        ) as usize;

        let value_start = offset as usize;
        let value_end = value_start + (length * 8);
        if value_end > file_data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "invalid value offset or length",
            ));
        }

        let values = file_data[value_start..value_end]
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        index.insert(Arc::from(stream_id), values);
    }

    Ok(index)
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
        let mut index = OpenStreamIndex::create(path).unwrap();

        let stream_id = "stream-a";
        let offsets = vec![42, 105];
        for offset in &offsets {
            index.insert(stream_id, *offset);
        }

        assert_eq!(index.get(stream_id), Some(&offsets));
        assert_eq!(index.get("unknown"), None);
    }

    #[test]
    fn test_open_event_index_flush_and_reopen() {
        let path = temp_file_path();
        let mut index = OpenStreamIndex::create(&path).unwrap();

        let stream_id1 = "stream-a";
        let stream_id2 = "stream-b";
        let offsets1 = vec![1111, 2222];
        let offsets2 = vec![3333];

        for offset in &offsets1 {
            index.insert(stream_id1, *offset);
        }
        for offset in &offsets2 {
            index.insert(stream_id2, *offset);
        }
        index.flush().unwrap();

        // Reopen and verify data is still present
        let reopened_index = OpenStreamIndex::open(&path).unwrap();
        assert_eq!(reopened_index.get(stream_id1), Some(&offsets1));
        assert_eq!(reopened_index.get(stream_id2), Some(&offsets2));
        assert_eq!(reopened_index.get("unknown"), None);
    }

    #[test]
    fn test_closed_event_index_lookup() {
        let path = temp_file_path();
        let mut index = OpenStreamIndex::create(&path).unwrap();

        let stream_id1 = "stream-a";
        let stream_id2 = "stream-b";
        let offsets1 = vec![1111, 2222];
        let offsets2 = vec![3333];

        for offset in &offsets1 {
            index.insert(stream_id1, *offset);
        }
        for offset in &offsets2 {
            index.insert(stream_id2, *offset);
        }
        index.flush().unwrap();

        let closed_index = ClosedStreamIndex::open(&path).unwrap();
        assert_eq!(closed_index.get(stream_id1).unwrap(), Some(offsets1));
        assert_eq!(closed_index.get(stream_id2).unwrap(), Some(offsets2));
        assert_eq!(closed_index.get("unknown").unwrap(), None);
    }

    #[test]
    fn test_collision_handling_in_direct_mapping() {
        let path = temp_file_path();
        let mut index = OpenStreamIndex::create(&path).unwrap();

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

        let offsets1 = vec![883, 44];
        let offsets2 = vec![39, 1, 429];
        let offsets3 = vec![1111, 2222];
        let offsets4 = vec![3333];

        for offset in &offsets1 {
            index.insert(stream_id1, *offset);
        }
        for offset in &offsets2 {
            index.insert(stream_id2, *offset);
        }
        for offset in &offsets3 {
            index.insert(stream_id3, *offset);
        }
        for offset in &offsets4 {
            index.insert(stream_id4, *offset);
        }
        index.flush().unwrap();

        let closed_index = ClosedStreamIndex::open(&path).unwrap();
        assert_eq!(closed_index.get(stream_id1).unwrap(), Some(offsets1));
        assert_eq!(closed_index.get(stream_id2).unwrap(), Some(offsets2));
        assert_eq!(closed_index.get(stream_id3).unwrap(), Some(offsets3));
        assert_eq!(closed_index.get(stream_id4).unwrap(), Some(offsets4));
        assert_eq!(closed_index.get("unknown").unwrap(), None);
    }

    #[test]
    fn test_non_existent_event_lookup() {
        let path = temp_file_path();

        let mut index = OpenStreamIndex::create(&path).unwrap();
        index.flush().unwrap();

        let index = ClosedStreamIndex::open(path).unwrap();

        assert_eq!(index.get("unknown").unwrap(), None);
    }
}
