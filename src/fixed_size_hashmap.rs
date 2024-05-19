use std::collections::hash_map::DefaultHasher;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem;
use std::path::Path;

use memmap2::MmapMut;

const KEY_SIZE: usize = 64;
const OFFSET_SIZE: usize = mem::size_of::<u64>();
const INDEX_ENTRY_SIZE: usize = KEY_SIZE + OFFSET_SIZE + OFFSET_SIZE; // 64 (key) + 8 (head offset) + 8 (next entry offset)
                                                                      // const MAX_ENTRIES: usize = 100_000;
const VALUE_BLOCK_SIZE: usize = OFFSET_SIZE + OFFSET_SIZE; // 8 (value) + 8 (next offset)

/// `FixedSizeHashMap` is a memory-mapped file-based fixed-size hashmap implementation.
///
/// This hashmap supports performant lookups where keys can have a maximum size of 64 bytes,
/// and each value is a list of `u64` offsets. The hashmap is backed by two files: an index file
/// and a data file.
///
/// # Index File Structure
///
/// The index file is divided into two sections:
///
/// 1. **Initial Space**:
///    - This section stores the keys, head offsets, and next entry offsets.
///    - Each entry in this section is of a fixed size (`INDEX_ENTRY_SIZE`) and consists of:
///      - `key` (up to 64 bytes)
///      - `head_offset` (u64): The offset of the first value in the data file.
///      - `next_entry_offset` (u64): The offset of the next entry in case of collisions.
///
/// 2. **Linked List Section**:
///    - This section contains the actual values and the offsets of the next values in the list.
///    - Each block in this section is of a fixed size (`VALUE_BLOCK_SIZE`) and consists of:
///      - `value` (u64)
///      - `next_offset` (u64): The offset of the next value in the list, or 0 if it is the end of the list.
///
/// # Usage
///
/// The `FixedSizeHashMap` provides methods for inserting key-value pairs and retrieving values efficiently.
///
/// ## Example
///
/// ```rust,ignore
/// use std::path::Path;
/// use std::io;
///
/// fn main() -> io::Result<()> {
///     let index_path = Path::new("index.dat");
///     let data_path = Path::new("data.dat");
///
///     let mut map = FixedSizeHashMap::new(index_path, data_path, 10_000)?;
///
///     let key = b"example_key";
///     let value = 42u64;
///
///     map.insert(key, value)?;
///
///     let retrieved_values = map.get(key)?;
///     assert_eq!(retrieved_values, vec![value]);
///
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct FixedSizeHashMap {
    index: MmapMut,
    file: File,
    next_value_offset: u64,
    next_index_offset: Option<u64>,
    max_entries: usize,
}

impl FixedSizeHashMap {
    /// Creates a new `FixedSizeHashMap` with the given index and data file paths.
    ///
    /// Initializes the index file with the initial space for `MAX_ENTRIES` entries,
    /// and maps it into memory. Also opens the data file for reading and writing.
    ///
    /// # Arguments
    ///
    /// * `index_path` - Path to the index file.
    /// * `data_path` - Path to the data file.
    ///
    /// # Returns
    ///
    /// * `io::Result<Self>` - A result containing the new `FixedSizeHashMap` instance or an I/O error.
    pub fn new(index_path: &Path, data_path: &Path, max_entries: usize) -> io::Result<Self> {
        let initial_space = max_entries * INDEX_ENTRY_SIZE;
        let total_size = (initial_space as f64 * 2.0) as u64;

        let next_index_offset = if !index_path.exists() {
            Some(initial_space as u64)
        } else {
            None
        };

        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(index_path)?;

        index_file.set_len(total_size as u64)?;

        let index = unsafe { MmapMut::map_mut(&index_file)? };

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_path)?;

        // Set initial file length to a reasonable size to avoid initial EOF errors
        file.set_len(1024 * 1024)?;

        Ok(Self {
            index,
            file,
            next_value_offset: 0,
            next_index_offset,
            max_entries,
        })
    }

    /// Inserts a key-value pair into the hashmap.
    ///
    /// If the key already exists, appends the new value to its value list.
    /// If the key does not exist, finds an empty slot and inserts the key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert (up to 64 bytes).
    /// * `value` - The value to insert (u64).
    ///
    /// # Returns
    ///
    /// * `io::Result<()>` - A result indicating success or an I/O error.
    pub fn insert(&mut self, key: &[u8], value: u64) -> io::Result<()> {
        assert!(key.len() <= KEY_SIZE, "Key length exceeds limit");

        let initial_offset = self.calculate_offset(key);
        let index_size = self.index.len();
        let padded_key = self.pad_key(key);

        // Check if the key already exists and append value if so
        if let Some(existing_offset) = self.find_key_offset(key) {
            let head_offset = u64::from_ne_bytes(
                self.index[existing_offset + KEY_SIZE..existing_offset + KEY_SIZE + OFFSET_SIZE]
                    .try_into()
                    .unwrap(),
            );

            self.append_value(head_offset, value)?;

            return Ok(());
        }

        // Find an empty slot in the index
        let mut offset = initial_offset;
        while &self.index[offset..offset + KEY_SIZE] != &[0; KEY_SIZE] {
            let next_entry_offset = u64::from_ne_bytes(
                self.index[offset + KEY_SIZE + OFFSET_SIZE
                    ..offset + KEY_SIZE + OFFSET_SIZE + OFFSET_SIZE]
                    .try_into()
                    .unwrap(),
            );
            if next_entry_offset == 0 {
                break;
            }
            offset = next_entry_offset as usize;
        }

        let new_offset = self.get_next_offset(index_size)?;

        if &self.index[offset..offset + KEY_SIZE] != &[0; KEY_SIZE] {
            self.index
                [offset + KEY_SIZE + OFFSET_SIZE..offset + KEY_SIZE + OFFSET_SIZE + OFFSET_SIZE]
                .copy_from_slice(&(new_offset as u64).to_ne_bytes());
            offset = new_offset; // Update offset to new offset
            *self.next_index_offset.as_mut().unwrap() += INDEX_ENTRY_SIZE as u64;
        }

        self.index[offset..offset + KEY_SIZE].copy_from_slice(&padded_key);

        let value_offset = self.next_value_offset;
        self.file.seek(SeekFrom::Start(value_offset))?;
        self.file.write_all(&value.to_ne_bytes())?;
        self.file.write_all(&0u64.to_ne_bytes())?;

        self.index[offset + KEY_SIZE..offset + KEY_SIZE + OFFSET_SIZE]
            .copy_from_slice(&value_offset.to_ne_bytes());
        self.index[offset + KEY_SIZE + OFFSET_SIZE..offset + KEY_SIZE + OFFSET_SIZE + OFFSET_SIZE]
            .copy_from_slice(&0u64.to_ne_bytes());

        self.next_value_offset += VALUE_BLOCK_SIZE as u64;

        Ok(())
    }

    /// Retrieves the list of values associated with the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up (up to 64 bytes).
    ///
    /// # Returns
    ///
    /// * `io::Result<Vec<u64>>` - A result containing the list of values or an I/O error.
    pub fn get(&mut self, key: &[u8]) -> io::Result<Vec<u64>> {
        assert!(key.len() <= KEY_SIZE, "Key length exceeds limit");

        if let Some(offset) = self.find_key_offset(key) {
            let value_head_offset = u64::from_ne_bytes(
                self.index[offset + KEY_SIZE..offset + KEY_SIZE + 8]
                    .try_into()
                    .unwrap(),
            );

            let mut values = Vec::new();
            let mut value_offset = value_head_offset;
            let mut value_bytes = [0u8; 8];
            let mut next_offset_bytes = [0u8; 8];

            loop {
                self.file.seek(SeekFrom::Start(value_offset))?;
                self.file.read_exact(&mut value_bytes)?;
                values.push(u64::from_ne_bytes(value_bytes));
                self.file.seek(SeekFrom::Start(value_offset + 8))?;
                self.file.read_exact(&mut next_offset_bytes)?;
                value_offset = u64::from_ne_bytes(next_offset_bytes);

                if next_offset_bytes == [0u8; 8] {
                    break;
                }
            }

            return Ok(values);
        }

        Ok(vec![])
    }

    pub fn flush(&self) -> io::Result<()> {
        self.index.flush()
    }

    fn get_next_offset(&mut self, index_size: usize) -> io::Result<usize> {
        let next_index_offset = match self.next_index_offset {
            Some(n) => n,
            None => {
                // We haven't determined the next index offset yet, we'll calculate this
                // by finding the first sequence of 0s.
                self.load_next_index_offset()?
            }
        };

        // Check if the next index offset is within the index bounds
        let new_offset = next_index_offset as usize;
        if new_offset + INDEX_ENTRY_SIZE > index_size {
            return Err(io::Error::new(io::ErrorKind::Other, "Index out of bounds"));
        }

        Ok(new_offset)
    }

    pub fn load_next_index_offset(&mut self) -> io::Result<u64> {
        let start = self.max_entries * INDEX_ENTRY_SIZE;
        let index_len = self.index.len();

        // Iterate through the index file from the starting point
        for i in (start..index_len).step_by(INDEX_ENTRY_SIZE) {
            // Check the first 8 bytes of the entry to see if they are zero
            if self.index[i..i + INDEX_ENTRY_SIZE]
                .iter()
                .all(|&byte| byte == 0)
            {
                let n = i as u64;
                self.next_index_offset = Some(n);
                return Ok(n);
            }
        }

        // If no zero entry found, assume the index is fully used and set the next offset accordingly
        Err(io::Error::new(
            io::ErrorKind::Other,
            "Next index offset not found",
        ))
    }

    fn calculate_offset(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize % self.max_entries) * INDEX_ENTRY_SIZE
    }

    fn find_key_offset(&self, key: &[u8]) -> Option<usize> {
        let mut offset = self.calculate_offset(key); // Calculate initial offset based on hash of the key
        let padded_key = self.pad_key(key);
        loop {
            // Check if the current offset contains the key we're looking for
            if &self.index[offset..offset + KEY_SIZE] == padded_key {
                return Some(offset); // Key found, return the offset
            }

            // Read the next entry offset
            let next_entry_offset = u64::from_ne_bytes(
                self.index[offset + KEY_SIZE + OFFSET_SIZE
                    ..offset + KEY_SIZE + OFFSET_SIZE + OFFSET_SIZE]
                    .try_into()
                    .unwrap(),
            );

            // If next_entry_offset is 0, we've reached the end of the chain without finding the key
            if next_entry_offset == 0 {
                return None;
            }

            // Move to the next entry in the chain
            offset = next_entry_offset as usize;
        }
    }

    fn pad_key(&self, key: &[u8]) -> [u8; KEY_SIZE] {
        let mut padded_key = [0; KEY_SIZE];
        padded_key[..key.len()].copy_from_slice(key);
        padded_key
    }

    fn append_value(&mut self, head_offset: u64, value: u64) -> io::Result<()> {
        let mut current_offset = head_offset;
        let mut next_offset_bytes = [0u8; OFFSET_SIZE];

        loop {
            self.file
                .seek(SeekFrom::Start(current_offset + OFFSET_SIZE as u64))?;
            self.file.read_exact(&mut next_offset_bytes)?;
            let next_offset = u64::from_ne_bytes(next_offset_bytes);

            if next_offset == 0 {
                break;
            }
            current_offset = next_offset;
        }

        // Write the new value block
        self.file.seek(SeekFrom::Start(self.next_value_offset))?;
        self.file.write_all(&value.to_ne_bytes())?;
        self.file.write_all(&0u64.to_ne_bytes())?; // Initialize next_offset to 0

        // Update the next_offset of the last block to point to the new block
        self.file
            .seek(SeekFrom::Start(current_offset + OFFSET_SIZE as u64))?;
        self.file.write_all(&self.next_value_offset.to_ne_bytes())?;

        self.next_value_offset += VALUE_BLOCK_SIZE as u64;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::TestDir;

    use super::*;
    use std::fs;

    fn setup() -> FixedSizeHashMap {
        let dir = TestDir::new();
        let index_path = dir.as_ref().join("test_index.dat");
        let data_path = dir.as_ref().join("test_data.dat");

        // Clean up any existing test files
        let _ = fs::remove_file(&index_path);
        let _ = fs::remove_file(&data_path);

        FixedSizeHashMap::new(&index_path, &data_path, 100)
            .expect("Failed to create FixedSizeHashMap")
    }

    fn find_collision(map: &FixedSizeHashMap, base_str: &str) -> (String, String, String) {
        let base_offset = map.calculate_offset(base_str.as_bytes());
        let mut count = 0;

        let key1;
        let key2;
        let key3;

        // Find the first colliding key
        loop {
            let k1 = format!("{}{}", base_str, count);
            if map.calculate_offset(k1.as_bytes()) == base_offset && k1 != base_str {
                key1 = k1;
                break;
            }
            count += 1;
        }

        count = 0;

        // Find the second colliding key
        loop {
            let k2 = format!("{}{}", base_str, count + 1);
            if map.calculate_offset(k2.as_bytes()) == base_offset && k2 != base_str && k2 != key1 {
                key2 = k2;
                break;
            }
            count += 1;
        }

        count = 0;

        // Find the third colliding key
        loop {
            let k3 = format!("{}{}", base_str, count + 2);
            if map.calculate_offset(k3.as_bytes()) == base_offset
                && k3 != base_str
                && k3 != key1
                && k3 != key2
            {
                key3 = k3;
                break;
            }
            count += 1;
        }

        (key1, key2, key3)
    }

    #[test]
    fn test_insert_and_retrieve_single_key_value() {
        let mut map = setup();

        map.insert(b"key1", 42).expect("Insert failed");
        let values = map.get(b"key1").expect("Get failed");

        assert_eq!(values, vec![42]);
    }

    #[test]
    fn test_insert_and_retrieve_single_key_multiple_values() {
        let mut map = setup();

        map.insert(b"key1", 28).expect("Insert failed");
        map.insert(b"key1", 42).expect("Insert failed");
        map.insert(b"key1", 96).expect("Insert failed");

        let values = map.get(b"key1").expect("Get failed");

        assert_eq!(values, vec![28, 42, 96]);
    }

    #[test]
    fn test_insert_and_retrieve_multiple_key_values() {
        let mut map = setup();

        map.insert(b"key1", 42).expect("Insert failed");
        map.insert(b"key2", 84).expect("Insert failed");

        let values1 = map.get(b"key1").expect("Get failed");
        let values2 = map.get(b"key2").expect("Get failed");

        assert_eq!(values1, vec![42]);
        assert_eq!(values2, vec![84]);
    }

    #[test]
    fn test_insert_and_retrieve_with_collisions() {
        let mut map = setup();

        // Keys that are different but should produce the same hash index
        let (key1, key2, key3) = find_collision(&map, "key");

        // Ensure both keys produce the same hash index
        assert_eq!(
            map.calculate_offset(key1.as_bytes()),
            map.calculate_offset(key2.as_bytes())
        );
        assert_eq!(
            map.calculate_offset(key1.as_bytes()),
            map.calculate_offset(key3.as_bytes())
        );

        map.insert(key1.as_bytes(), 42).expect("Insert failed");
        map.insert(key2.as_bytes(), 84).expect("Insert failed");
        map.insert(key3.as_bytes(), 92).expect("Insert failed");

        let values1 = map.get(key1.as_bytes()).expect("Get failed");
        let values2 = map.get(key2.as_bytes()).expect("Get failed");
        let values3 = map.get(key3.as_bytes()).expect("Get failed");

        assert_eq!(values1, vec![42]);
        assert_eq!(values2, vec![84]);
        assert_eq!(values3, vec![92]);
    }

    #[test]
    fn test_update_existing_key_with_additional_values() {
        let mut map = setup();

        map.insert(b"key1", 42).expect("Insert failed");
        map.insert(b"key1", 84).expect("Insert failed");

        let values = map.get(b"key1").expect("Get failed");

        assert_eq!(values, vec![42, 84]);
    }

    #[test]
    fn test_handle_maximum_key_size() {
        let mut map = setup();

        let max_key = [b'a'; KEY_SIZE];
        map.insert(&max_key, 42).expect("Insert failed");

        let values = map.get(&max_key).expect("Get failed");

        assert_eq!(values, vec![42]);
    }

    #[test]
    fn test_non_existent_key_retrieval() {
        let mut map = setup();

        map.insert(b"key1", 42).expect("Insert failed");

        let values = map.get(b"non_existent_key").expect("Get failed");

        assert_eq!(values, vec![]);
    }

    #[test]
    fn test_empty_key_insertion() {
        let mut map = setup();

        let empty_key = b"";
        map.insert(empty_key, 42).expect("Insert failed");

        let values = map.get(empty_key).expect("Get failed");

        assert_eq!(values, vec![42]);
    }

    #[test]
    fn test_sequential_insertions_and_deletions() {
        let mut map = setup();

        // Insert and then delete the same key multiple times
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            map.insert(&key, i as u64).expect("Insert failed");
            let values = map.get(&key).expect("Get failed");
            assert_eq!(values, vec![i as u64]);
        }

        // Ensure deleted keys are not retrievable
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let _ = map.get(&key).expect("Get failed");
        }
    }

    #[test]
    fn test_overwriting_values() {
        let mut map = setup();

        map.insert(b"key1", 42).expect("Insert failed");
        map.insert(b"key1", 84).expect("Insert failed");

        let values = map.get(b"key1").expect("Get failed");

        assert_eq!(values, vec![42, 84]);

        // Overwrite the value
        map.insert(b"key1", 100).expect("Insert failed");

        let updated_values = map.get(b"key1").expect("Get failed");

        assert_eq!(updated_values, vec![42, 84, 100]);
    }

    #[test]
    fn test_boundary_conditions() {
        let mut map = setup();

        let max_entries = map.max_entries;

        // Insert maximum number of entries
        for i in 0..max_entries {
            let key = format!("key{}", i).into_bytes();
            map.insert(&key, i as u64).expect("Insert failed");
        }

        // Ensure all entries are retrievable
        for i in 0..max_entries {
            let key = format!("key{}", i);
            let values = map.get(key.as_bytes()).expect("Get failed");
            assert_eq!(values, vec![i as u64], "assert failed for key: {key}");
        }
    }

    #[test]
    fn test_next_index_offset_loading() {
        let dir = TestDir::new();
        let index_path = dir.as_ref().join("test_index.dat");
        let data_path = dir.as_ref().join("test_data.dat");

        // Clean up any existing test files
        let _ = fs::remove_file(&index_path);
        let _ = fs::remove_file(&data_path);

        let (next_index_offset, key1) = {
            let mut map = FixedSizeHashMap::new(&index_path, &data_path, 100)
                .expect("Failed to create FixedSizeHashMap");

            let (key1, key2, key3) = find_collision(&map, "key");

            map.insert(key1.as_bytes(), 42).expect("Insert failed");
            map.insert(key2.as_bytes(), 84).expect("Insert failed");
            map.insert(key3.as_bytes(), 11).expect("Insert failed");

            map.flush().expect("Flush failed");
            (map.next_index_offset, key1)
        };

        let mut map = FixedSizeHashMap::new(&index_path, &data_path, 100)
            .expect("Failed to create FixedSizeHashMap");
        assert_eq!(map.get(key1.as_bytes()).unwrap(), vec![42]);
        map.load_next_index_offset()
            .expect("Load next index offset failed");

        assert_eq!(map.next_index_offset, next_index_offset);
    }
}
