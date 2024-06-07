use std::hash::{Hash, Hasher};
use std::mem;
use std::path::{Path, PathBuf};

use log::info;
use memmap2::MmapMut;
use tokio::fs::{self, OpenOptions};
use tokio::io;
use twox_hash::Xxh3Hash64;

use crate::Offset;

pub static STREAM_INDEX_FILE_NAME_EXTENSION: &str = "streams";
pub static SEGMENT_FILE_NAME_LEN: usize = 20;

const KEY_SIZE: usize = 64;
const OFFSET_SIZE: usize = mem::size_of::<u64>();
const INDEX_ENTRY_SIZE: usize = KEY_SIZE + OFFSET_SIZE + OFFSET_SIZE + OFFSET_SIZE; // 64 (key) + 8 (head offset) + 8 (tail offset) + 8 (next entry offset)
const VALUE_BLOCK_SIZE: usize = OFFSET_SIZE + OFFSET_SIZE; // 8 (value) + 8 (next offset)
const HEAD_OFFSET: usize = KEY_SIZE;
const HEAD_SIZE: usize = OFFSET_SIZE;
const TAIL_OFFSET: usize = HEAD_OFFSET + HEAD_SIZE;
const TAIL_SIZE: usize = OFFSET_SIZE;
const NEXT_ENTRY_OFFSET: usize = TAIL_OFFSET + TAIL_SIZE;
const NEXT_ENTRY_SIZE: usize = OFFSET_SIZE;

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
pub struct StreamIndex {
    data: MmapMut,
    path: PathBuf,
    next_index_offset: usize,
    next_value_offset: usize,
    max_entries: u64,
    base_offset: u64,
}

impl StreamIndex {
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
    pub async fn new(log_dir: &Path, base_offset: u64, max_entries: u64) -> io::Result<Self> {
        let initial_space = max_entries as usize * INDEX_ENTRY_SIZE + OFFSET_SIZE;

        let path = {
            // the log is of the form BASE_OFFSET.log
            let mut path_buf = PathBuf::new();
            path_buf.push(&log_dir);
            path_buf.push(format!("{:020}", base_offset));
            path_buf.set_extension(STREAM_INDEX_FILE_NAME_EXTENSION);
            path_buf
        };

        let filename = path.file_name().unwrap().to_str().unwrap();
        let base_offset = match (&filename[0..SEGMENT_FILE_NAME_LEN]).parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Segment file name does not parse as u64",
                ));
            }
        };

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.set_len(Self::total_size(max_entries) as u64).await?;

        let mut data = unsafe { MmapMut::map_mut(&file)? };
        data[0..OFFSET_SIZE].copy_from_slice(&max_entries.to_ne_bytes());
        let (res, data) = tokio::task::spawn_blocking(move || (data.flush(), data))
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        res?;

        Ok(Self {
            data,
            path,
            next_index_offset: initial_space,
            next_value_offset: Self::values_start_offset(max_entries),
            max_entries,
            base_offset,
        })
    }

    pub async fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let filename = path.file_name().unwrap().to_str().unwrap();
        let base_offset = match (&filename[0..SEGMENT_FILE_NAME_LEN]).parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Segment file name does not parse as u64",
                ));
            }
        };

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await?;

        let data = unsafe { MmapMut::map_mut(&file)? };
        let max_entries = u64::from_ne_bytes(data[0..OFFSET_SIZE].try_into().unwrap());

        let next_index_offset = Self::load_next_index_offset(max_entries, &data)?;
        let next_value_offset = Self::load_next_value_offset(max_entries, &data)?;

        Ok(Self {
            data,
            path,
            next_index_offset,
            next_value_offset,
            max_entries,
            base_offset,
        })
    }

    #[inline]
    pub fn starting_offset(&self) -> u64 {
        self.base_offset
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
    pub fn insert(&mut self, key: &str, value: u64) -> io::Result<()> {
        assert!(key.len() <= KEY_SIZE, "Key length exceeds limit");
        assert!(!key.is_empty(), "Empty keys are not supported");

        let initial_offset = self.calculate_offset(key);
        let index_size = self.data.len();
        let padded_key = self.pad_key(key);

        // Check if the key already exists and append value if so
        if let Some(existing_offset) = self.find_key_offset(key) {
            let tail_offset = u64::from_ne_bytes(
                self.data[existing_offset + TAIL_OFFSET..existing_offset + TAIL_OFFSET + TAIL_SIZE]
                    .try_into()
                    .unwrap(),
            );

            self.append_value(existing_offset, tail_offset as usize, value + 1)?;

            return Ok(());
        }

        // Find an empty slot in the index
        let mut offset = initial_offset;
        while &self.data[offset..offset + KEY_SIZE] != &[0; KEY_SIZE] {
            let next_entry_offset = u64::from_ne_bytes(
                self.data[offset + NEXT_ENTRY_OFFSET..offset + NEXT_ENTRY_OFFSET + NEXT_ENTRY_SIZE]
                    .try_into()
                    .unwrap(),
            );
            if next_entry_offset == 0 {
                break;
            }
            offset = next_entry_offset as usize;
        }

        let new_offset = self.get_next_offset(index_size)?;

        if &self.data[offset..offset + KEY_SIZE] != &[0; KEY_SIZE] {
            self.data[offset + NEXT_ENTRY_OFFSET..offset + NEXT_ENTRY_OFFSET + NEXT_ENTRY_SIZE]
                .copy_from_slice(&(new_offset as u64).to_ne_bytes());
            offset = new_offset; // Update offset to new offset
            self.next_index_offset += INDEX_ENTRY_SIZE;
        }

        self.data[offset..offset + KEY_SIZE].copy_from_slice(&padded_key);

        let value_offset = self.next_value_offset;
        self.data[value_offset..value_offset + OFFSET_SIZE]
            .copy_from_slice(&(value + 1).to_ne_bytes());
        self.data[value_offset + OFFSET_SIZE..value_offset + OFFSET_SIZE + OFFSET_SIZE]
            .copy_from_slice(&0u64.to_ne_bytes());

        self.data[offset + HEAD_OFFSET..offset + HEAD_OFFSET + HEAD_SIZE]
            .copy_from_slice(&value_offset.to_ne_bytes());
        self.data[offset + TAIL_OFFSET..offset + TAIL_OFFSET + TAIL_SIZE]
            .copy_from_slice(&value_offset.to_ne_bytes());
        self.data[offset + NEXT_ENTRY_OFFSET..offset + NEXT_ENTRY_OFFSET + NEXT_ENTRY_SIZE]
            .copy_from_slice(&0u64.to_ne_bytes());

        self.next_value_offset += VALUE_BLOCK_SIZE;

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
    pub fn get(&self, key: &str) -> io::Result<Vec<u64>> {
        assert!(key.len() <= KEY_SIZE, "Key length exceeds limit");
        assert!(!key.is_empty(), "Empty keys are not supported");

        if let Some(offset) = self.find_key_offset(key) {
            let value_head_offset = u64::from_ne_bytes(
                self.data[offset + HEAD_OFFSET..offset + HEAD_OFFSET + HEAD_SIZE]
                    .try_into()
                    .unwrap(),
            );

            let mut values = Vec::new();
            let mut value_offset = value_head_offset as usize;

            loop {
                let stored_value = u64::from_ne_bytes(
                    self.data[value_offset..value_offset + OFFSET_SIZE]
                        .try_into()
                        .unwrap(),
                );
                if stored_value == 0 {
                    break; // Skip invalid or cleared entries
                }
                values.push(stored_value - 1);
                value_offset = u64::from_ne_bytes(
                    self.data[value_offset + OFFSET_SIZE..value_offset + OFFSET_SIZE + OFFSET_SIZE]
                        .try_into()
                        .unwrap(),
                ) as usize;

                if value_offset == 0 {
                    break;
                }
            }

            return Ok(values);
        }

        Ok(vec![])
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.data.flush()?;
        Ok(())
    }

    /// Removes the stream index file.
    pub async fn remove(self) -> io::Result<()> {
        info!("Removing stream index file {}", self.path.display());
        fs::remove_file(self.path).await
    }

    /// Truncates to an offset, inclusive. The file length of the
    /// segment for truncation is returned.
    // pub fn truncate(&mut self, offset: u64) -> io::Result<()> {
    //     let mut found_free = false;

    //     // Iterate over each index entry
    //     for i in 0..self.max_entries {
    //         let entry_offset = OFFSET_SIZE + i as usize * INDEX_ENTRY_SIZE;
    //         let head_offset = u64::from_ne_bytes(
    //             self.data[entry_offset + HEAD_OFFSET..entry_offset + HEAD_OFFSET + HEAD_SIZE]
    //                 .try_into()
    //                 .unwrap(),
    //         ) as usize;

    //         // Skip empty entries
    //         if head_offset == 0 {
    //             continue;
    //         }

    //         // Traverse the linked list of values
    //         let mut current_offset = head_offset;
    //         let mut prev_offset: Option<usize> = None;

    //         while current_offset != 0 {
    //             let value_block = &mut self.data[current_offset..current_offset + VALUE_BLOCK_SIZE];
    //             let value = u64::from_ne_bytes(value_block[0..OFFSET_SIZE].try_into().unwrap());
    //             let next_offset = u64::from_ne_bytes(
    //                 value_block[OFFSET_SIZE..OFFSET_SIZE + OFFSET_SIZE]
    //                     .try_into()
    //                     .unwrap(),
    //             ) as usize;

    //             if value > offset {
    //                 // Clear the value block and mark the offset as free
    //                 value_block.fill(0);

    //                 if let Some(prev) = prev_offset {
    //                     // Update the previous block's next offset
    //                     let prev_block = &mut self.data[prev..prev + VALUE_BLOCK_SIZE];
    //                     prev_block[OFFSET_SIZE..OFFSET_SIZE + OFFSET_SIZE]
    //                         .copy_from_slice(&(next_offset as u64).to_ne_bytes());
    //                 } else {
    //                     // Update the head offset of the entry
    //                     self.data
    //                         [entry_offset + HEAD_OFFSET..entry_offset + HEAD_OFFSET + HEAD_SIZE]
    //                         .copy_from_slice(&(next_offset as u64).to_ne_bytes());
    //                 }

    //                 if next_offset == 0 {
    //                     // Update the tail offset if this was the tail
    //                     self.data
    //                         [entry_offset + TAIL_OFFSET..entry_offset + TAIL_OFFSET + TAIL_SIZE]
    //                         .copy_from_slice(&(prev_offset.unwrap_or(0) as u64).to_ne_bytes());
    //                 }

    //                 // Update the next_value_offset if a free block is found earlier
    //                 if current_offset < self.next_value_offset {
    //                     self.next_value_offset = current_offset;
    //                     found_free = true;
    //                 }
    //             } else {
    //                 // Stop truncating when we reach a value <= truncate_value
    //                 break;
    //             }

    //             prev_offset = Some(current_offset);
    //             current_offset = next_offset;
    //         }

    //         // If head_offset is 0 after truncation, reset the entry
    //         let new_head_offset = u64::from_ne_bytes(
    //             self.data[entry_offset + HEAD_OFFSET..entry_offset + HEAD_OFFSET + HEAD_SIZE]
    //                 .try_into()
    //                 .unwrap(),
    //         );
    //         if new_head_offset == 0 {
    //             self.data[entry_offset..entry_offset + INDEX_ENTRY_SIZE].fill(0);
    //         }
    //     }

    //     // Reset next_value_offset to the original end if no free block is found
    //     if !found_free {
    //         self.next_value_offset = Self::values_start_offset(self.max_entries);
    //     }

    //     Ok(())
    // }
    pub fn truncate(&mut self, offset: Offset) {
        let values_start_offset = Self::values_start_offset(self.max_entries);

        // Step 1: Clear values that are greater than the truncate offset
        for i in (0..((self.next_value_offset - values_start_offset) / VALUE_BLOCK_SIZE)).rev() {
            let current = values_start_offset + (VALUE_BLOCK_SIZE * i);
            let stored_value = u64::from_ne_bytes(
                self.data[current..current + OFFSET_SIZE]
                    .try_into()
                    .unwrap(),
            );
            if stored_value == 0 {
                continue; // Skip invalid or cleared entries
            }
            let value = stored_value - 1;
            if value <= offset {
                // Reset next offset to zeros
                self.data[current + OFFSET_SIZE..current + OFFSET_SIZE + OFFSET_SIZE]
                    .copy_from_slice(&[0u8; OFFSET_SIZE]);

                // Update the next value offset to the current position
                self.next_value_offset = current + VALUE_BLOCK_SIZE;
                break;
            }

            // Clear value block entirely
            self.data[current..current + VALUE_BLOCK_SIZE]
                .copy_from_slice(&[0u8; VALUE_BLOCK_SIZE]);
        }

        // Step 2: Iterate entries to reset any head and tail offsets
        for i in 0..self.max_entries as usize {
            let entry_offset = OFFSET_SIZE + i * INDEX_ENTRY_SIZE;
            let head_offset = u64::from_ne_bytes(
                self.data[entry_offset + HEAD_OFFSET..entry_offset + HEAD_OFFSET + HEAD_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize;

            if head_offset == 0 {
                continue; // Skip empty entries
            }

            let mut current_offset = head_offset;
            let mut prev_offset: Option<usize> = None;
            let mut new_tail_offset: Option<u64> = None;
            let mut new_head_offset: Option<u64> = None;

            while current_offset != 0 {
                let value_block = &mut self.data[current_offset..current_offset + VALUE_BLOCK_SIZE];
                let stored_value =
                    u64::from_ne_bytes(value_block[0..OFFSET_SIZE].try_into().unwrap());
                if stored_value == 0 {
                    break; // Skip invalid or cleared entries
                }
                let value = stored_value - 1;
                let next_offset = u64::from_ne_bytes(
                    value_block[OFFSET_SIZE..OFFSET_SIZE + OFFSET_SIZE]
                        .try_into()
                        .unwrap(),
                ) as usize;

                if value > offset {
                    // Clear the value block and mark the offset as free
                    value_block.fill(0);

                    if let Some(prev) = prev_offset {
                        // Update the previous block's next offset
                        let prev_block = &mut self.data[prev..prev + VALUE_BLOCK_SIZE];
                        prev_block[OFFSET_SIZE..OFFSET_SIZE + OFFSET_SIZE]
                            .copy_from_slice(&(next_offset as u64).to_ne_bytes());
                    } else {
                        // Update the new head offset
                        new_head_offset = Some(next_offset as u64);
                    }

                    // Update the next_value_offset if a free block is found earlier
                    if current_offset < self.next_value_offset {
                        self.next_value_offset = current_offset;
                    }
                } else {
                    if new_head_offset.is_none() {
                        new_head_offset = Some(current_offset as u64);
                    }
                    new_tail_offset = Some(current_offset as u64);
                    prev_offset = Some(current_offset);
                }

                current_offset = next_offset;
            }

            // Update the head and tail offsets of the entry
            if let Some(new_head) = new_head_offset {
                self.data[entry_offset + HEAD_OFFSET..entry_offset + HEAD_OFFSET + HEAD_SIZE]
                    .copy_from_slice(&new_head.to_ne_bytes());
            } else {
                // If all values are truncated, clear the entire entry
                self.data[entry_offset..entry_offset + INDEX_ENTRY_SIZE].fill(0);
                continue;
            }

            if let Some(new_tail) = new_tail_offset {
                self.data[entry_offset + TAIL_OFFSET..entry_offset + TAIL_OFFSET + TAIL_SIZE]
                    .copy_from_slice(&new_tail.to_ne_bytes());

                // Ensure head offset is set correctly if it was cleared initially
                if self.data[entry_offset + HEAD_OFFSET..entry_offset + HEAD_OFFSET + HEAD_SIZE]
                    == [0; HEAD_SIZE]
                {
                    self.data[entry_offset + HEAD_OFFSET..entry_offset + HEAD_OFFSET + HEAD_SIZE]
                        .copy_from_slice(&new_tail.to_ne_bytes());
                }
            } else {
                // If no valid tail is found, clear the tail offset
                self.data[entry_offset + TAIL_OFFSET..entry_offset + TAIL_OFFSET + TAIL_SIZE]
                    .copy_from_slice(&[0u8; TAIL_SIZE]);
            }
        }
    }

    fn total_size(max_entries: u64) -> usize {
        Self::values_start_offset(max_entries) + (VALUE_BLOCK_SIZE * max_entries as usize)
    }

    fn values_start_offset(max_entries: u64) -> usize {
        OFFSET_SIZE + (max_entries as usize * INDEX_ENTRY_SIZE) * 2
    }

    fn get_next_offset(&mut self, index_size: usize) -> io::Result<usize> {
        // Check if the next index offset is within the index bounds
        let new_offset = self.next_index_offset as usize;
        if new_offset + INDEX_ENTRY_SIZE > index_size {
            return Err(io::Error::new(io::ErrorKind::Other, "Index out of bounds"));
        }

        Ok(new_offset)
    }

    fn load_next_index_offset(max_entries: u64, index: &MmapMut) -> io::Result<usize> {
        Self::find_next_zeros(
            index,
            max_entries as usize * INDEX_ENTRY_SIZE + OFFSET_SIZE,
            INDEX_ENTRY_SIZE,
        )
    }

    fn load_next_value_offset(max_entries: u64, index: &MmapMut) -> io::Result<usize> {
        Self::find_next_zeros(
            index,
            max_entries as usize * INDEX_ENTRY_SIZE * 2 + OFFSET_SIZE,
            VALUE_BLOCK_SIZE,
        )
    }

    fn find_next_zeros(index: &MmapMut, start: usize, step_by: usize) -> io::Result<usize> {
        // TODO: This could perhaps be done with a binary search
        let index_len = index.len();

        // Iterate through the index file from the starting point
        for i in (start..index_len).step_by(step_by) {
            // Check the first 8 bytes of the entry to see if they are zero
            if index[i..i + step_by].iter().all(|&byte| byte == 0) {
                return Ok(i);
            }
        }

        // If no zero entry found, assume the index is fully used and set the next offset accordingly
        Err(io::Error::new(
            io::ErrorKind::Other,
            "Next offset not found",
        ))
    }

    fn calculate_offset(&self, key: &str) -> usize {
        let mut hasher = Xxh3Hash64::with_seed(0);
        key.hash(&mut hasher);
        (hasher.finish() as usize % self.max_entries as usize) * INDEX_ENTRY_SIZE + OFFSET_SIZE
    }

    fn find_key_offset(&self, key: &str) -> Option<usize> {
        let mut offset = self.calculate_offset(key); // Calculate initial offset based on hash of the key
        let padded_key = self.pad_key(key);
        loop {
            // Check if the current offset contains the key we're looking for
            if &self.data[offset..offset + KEY_SIZE] == padded_key {
                return Some(offset); // Key found, return the offset
            }

            // Read the next entry offset
            let next_entry_offset = u64::from_ne_bytes(
                self.data[offset + NEXT_ENTRY_OFFSET..offset + NEXT_ENTRY_OFFSET + NEXT_ENTRY_SIZE]
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

    fn pad_key(&self, key: &str) -> [u8; KEY_SIZE] {
        let mut padded_key = [0; KEY_SIZE];
        padded_key[..key.len()].copy_from_slice(key.as_bytes());
        padded_key
    }

    fn append_value(
        &mut self,
        entry_offset: usize,
        tail_offset: usize,
        value: u64,
    ) -> io::Result<()> {
        // Write the new value block
        self.data[self.next_value_offset..self.next_value_offset + OFFSET_SIZE]
            .copy_from_slice(&value.to_ne_bytes());
        self.data[self.next_value_offset + OFFSET_SIZE
            ..self.next_value_offset + OFFSET_SIZE + OFFSET_SIZE]
            .copy_from_slice(&0u64.to_ne_bytes());

        // Update the next_offset of the last block to point to the new block
        self.data[tail_offset + OFFSET_SIZE..tail_offset + OFFSET_SIZE + OFFSET_SIZE]
            .copy_from_slice(&(self.next_value_offset as u64).to_ne_bytes());

        // Update the tail offset of the entry to the new block
        self.data[entry_offset + TAIL_OFFSET..entry_offset + TAIL_OFFSET + TAIL_SIZE]
            .copy_from_slice(&self.next_value_offset.to_ne_bytes());

        self.next_value_offset += VALUE_BLOCK_SIZE;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::TestDir;

    use super::*;
    use std::iter;

    async fn setup() -> StreamIndex {
        let dir = TestDir::new();

        StreamIndex::new(dir.as_ref(), 0, 100)
            .await
            .expect("Failed to create FixedSizeHashMap")
    }

    fn find_collision(map: &StreamIndex, base_str: &str) -> (String, String, String) {
        let base_offset = map.calculate_offset(base_str);
        let mut count = 0;

        let key1;
        let key2;
        let key3;

        // Find the first colliding key
        loop {
            let k1 = format!("{}{}", base_str, count);
            if map.calculate_offset(&k1) == base_offset && k1 != base_str {
                key1 = k1;
                break;
            }
            count += 1;
        }

        count = 0;

        // Find the second colliding key
        loop {
            let k2 = format!("{}{}", base_str, count + 1);
            if map.calculate_offset(&k2) == base_offset && k2 != base_str && k2 != key1 {
                key2 = k2;
                break;
            }
            count += 1;
        }

        count = 0;

        // Find the third colliding key
        loop {
            let k3 = format!("{}{}", base_str, count + 2);
            if map.calculate_offset(&k3) == base_offset
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

    #[tokio::test]
    async fn test_insert_and_retrieve_single_key_value() {
        let mut map = setup().await;

        map.insert("key1", 42).expect("Insert failed");
        let values = map.get("key1").expect("Get failed");

        assert_eq!(values, vec![42]);
    }

    #[tokio::test]
    async fn test_insert_and_retrieve_single_key_multiple_values() {
        let mut map = setup().await;

        map.insert("key1", 28).expect("Insert failed");
        map.insert("key1", 42).expect("Insert failed");
        map.insert("key1", 96).expect("Insert failed");

        let values = map.get("key1").expect("Get failed");

        assert_eq!(values, vec![28, 42, 96]);
    }

    #[tokio::test]
    async fn test_insert_and_retrieve_multiple_key_values() {
        let mut map = setup().await;

        map.insert("key1", 42).expect("Insert failed");
        map.insert("key2", 84).expect("Insert failed");

        let values1 = map.get("key1").expect("Get failed");
        let values2 = map.get("key2").expect("Get failed");

        assert_eq!(values1, vec![42]);
        assert_eq!(values2, vec![84]);
    }

    #[tokio::test]
    async fn test_insert_and_retrieve_with_collisions() {
        let mut map = setup().await;

        // Keys that are different but should produce the same hash index
        let (key1, key2, key3) = find_collision(&map, "key");

        // Ensure both keys produce the same hash index
        assert_eq!(map.calculate_offset(&key1), map.calculate_offset(&key2));
        assert_eq!(map.calculate_offset(&key1), map.calculate_offset(&key3));

        map.insert(&key1, 42).expect("Insert failed");
        map.insert(&key2, 84).expect("Insert failed");
        map.insert(&key3, 92).expect("Insert failed");

        let values1 = map.get(&key1).expect("Get failed");
        let values2 = map.get(&key2).expect("Get failed");
        let values3 = map.get(&key3).expect("Get failed");

        assert_eq!(values1, vec![42]);
        assert_eq!(values2, vec![84]);
        assert_eq!(values3, vec![92]);
    }

    #[tokio::test]
    async fn test_update_existing_key_with_additional_values() {
        let mut map = setup().await;

        map.insert("key1", 42).expect("Insert failed");
        map.insert("key1", 84).expect("Insert failed");

        let values = map.get("key1").expect("Get failed");

        assert_eq!(values, vec![42, 84]);
    }

    #[tokio::test]
    async fn test_handle_maximum_key_size() {
        let mut map = setup().await;

        let max_key: String = iter::repeat('a').take(KEY_SIZE).collect();
        map.insert(&max_key, 42).expect("Insert failed");

        let values = map.get(&max_key).expect("Get failed");

        assert_eq!(values, vec![42]);
    }

    #[tokio::test]
    async fn test_non_existent_key_retrieval() {
        let mut map = setup().await;

        map.insert("key1", 42).expect("Insert failed");

        let values = map.get("non_existent_key").expect("Get failed");

        assert_eq!(values, vec![]);
    }

    #[tokio::test]
    async fn test_sequential_insertions_and_deletions() {
        let mut map = setup().await;

        // Insert and then delete the same key multiple times
        for i in 0..10 {
            let key = format!("key{}", i);
            map.insert(&key, i as u64).expect("Insert failed");
            let values = map.get(&key).expect("Get failed");
            assert_eq!(values, vec![i as u64]);
        }

        // Ensure deleted keys are not retrievable
        for i in 0..10 {
            let key = format!("key{}", i);
            let _ = map.get(&key).expect("Get failed");
        }
    }

    #[tokio::test]
    async fn test_overwriting_values() {
        let mut map = setup().await;

        map.insert("key1", 42).expect("Insert failed");
        map.insert("key1", 84).expect("Insert failed");

        let values = map.get("key1").expect("Get failed");

        assert_eq!(values, vec![42, 84]);

        // Overwrite the value
        map.insert("key1", 100).expect("Insert failed");

        let updated_values = map.get("key1").expect("Get failed");

        assert_eq!(updated_values, vec![42, 84, 100]);
    }

    #[tokio::test]
    async fn test_boundary_conditions() {
        let mut map = setup().await;

        let max_entries = map.max_entries;

        // Insert maximum number of entries
        for i in 0..max_entries {
            let key = format!("key{}", i);
            map.insert(&key, i as u64).expect("Insert failed");
        }

        // Ensure all entries are retrievable
        for i in 0..max_entries {
            let key = format!("key{}", i);
            let values = map.get(&key).expect("Get failed");
            assert_eq!(values, vec![i as u64], "assert failed for key: {key}");
        }
    }

    #[tokio::test]
    async fn test_next_index_offset_loading() {
        let dir = TestDir::new();

        let (next_index_offset, next_value_offset, key1) = {
            let mut map = StreamIndex::new(dir.as_ref(), 0, 100)
                .await
                .expect("Failed to create FixedSizeHashMap");

            let (key1, key2, key3) = find_collision(&map, "key");

            map.insert(&key1, 42).expect("Insert failed");
            map.insert(&key2, 84).expect("Insert failed");
            map.insert(&key3, 11).expect("Insert failed");

            map.flush().expect("Flush failed");
            (map.next_index_offset, map.next_value_offset, key1)
        };

        let map = StreamIndex::open(
            &dir.as_ref()
                .join(format!("{:020}.{}", 0, STREAM_INDEX_FILE_NAME_EXTENSION)),
        )
        .await
        .expect("Failed to create FixedSizeHashMap");
        assert_eq!(map.get(&key1).unwrap(), vec![42]);

        assert_eq!(map.next_index_offset, next_index_offset);
        assert_eq!(map.next_value_offset, next_value_offset);
    }

    #[tokio::test]
    async fn test_max_entries_loading() {
        let dir = TestDir::new();
        {
            let map = StreamIndex::new(dir.as_ref(), 0, 24).await.unwrap();
            assert_eq!(map.max_entries, 24);
        }
        {
            let map = StreamIndex::open(
                &dir.as_ref()
                    .join(format!("{:020}.{}", 0, STREAM_INDEX_FILE_NAME_EXTENSION)),
            )
            .await
            .unwrap();
            assert_eq!(map.max_entries, 24);
        }
    }

    #[tokio::test]
    async fn test_truncate_simple_case() {
        let mut map = setup().await;

        // Insert values
        map.insert("key1", 10).expect("Insert failed");
        map.insert("key1", 20).expect("Insert failed");
        map.insert("key1", 30).expect("Insert failed");

        // Truncate at value 20
        map.truncate(20);

        let values = map.get("key1").expect("Get failed");
        assert_eq!(
            values,
            vec![10, 20],
            "Values after truncation are not as expected"
        );
    }

    #[tokio::test]
    async fn test_truncate_with_multiple_keys() {
        let mut map = setup().await;

        // Insert values for multiple keys
        map.insert("key1", 10).expect("Insert failed");
        map.insert("key1", 20).expect("Insert failed");
        map.insert("key2", 15).expect("Insert failed");
        map.insert("key2", 25).expect("Insert failed");
        map.insert("key2", 35).expect("Insert failed");

        // Truncate at value 20
        map.truncate(20);

        let values1 = map.get("key1").expect("Get failed");
        let values2 = map.get("key2").expect("Get failed");

        assert_eq!(
            values1,
            vec![10, 20],
            "Values for key1 after truncation are not as expected"
        );
        assert_eq!(
            values2,
            vec![15],
            "Values for key2 after truncation are not as expected"
        );
    }

    #[tokio::test]
    async fn test_truncate_with_collisions() {
        let mut map = setup().await;

        // Keys that are different but should produce the same hash index
        let (key1, key2, key3) = find_collision(&map, "key");

        // Insert colliding keys
        map.insert(&key1, 10).expect("Insert failed");
        map.insert(&key2, 20).expect("Insert failed");
        map.insert(&key3, 30).expect("Insert failed");

        // Truncate at value 20
        map.truncate(20);

        let values1 = map.get(&key1).expect("Get failed");
        let values2 = map.get(&key2).expect("Get failed");
        let values3 = map.get(&key3).expect("Get failed");

        assert_eq!(
            values1,
            vec![10],
            "Values for key1 after truncation are not as expected"
        );
        assert_eq!(
            values2,
            vec![20],
            "Values for key2 after truncation are not as expected"
        );
        assert_eq!(
            values3,
            vec![],
            "Values for key3 after truncation are not as expected"
        );
    }

    #[tokio::test]
    async fn test_truncate_with_fragmented_index() {
        let mut map = setup().await;

        // Insert values
        map.insert("key1", 10).expect("Insert failed");
        map.insert("key1", 20).expect("Insert failed");
        map.insert("key2", 30).expect("Insert failed");

        // Truncate at value 20
        map.truncate(20);

        let values1_after_truncate = map.get("key1").expect("Get failed");
        let values2_after_truncate = map.get("key2").expect("Get failed");

        // Validate values after truncation
        assert_eq!(
            values1_after_truncate,
            vec![10, 20],
            "Values for key1 after truncation are not as expected"
        );
        assert_eq!(
            values2_after_truncate,
            vec![],
            "Values for key2 after truncation are not as expected"
        );

        // Insert new values
        map.insert("key1", 25).expect("Insert failed");
        map.insert("key2", 35).expect("Insert failed");

        let values1_after_insert = map.get("key1").expect("Get failed");
        let values2_after_insert = map.get("key2").expect("Get failed");

        // Validate values after re-insertion
        assert_eq!(
            values1_after_insert,
            vec![10, 20, 25],
            "Values for key1 after re-insertion are not as expected"
        );
        assert_eq!(
            values2_after_insert,
            vec![35],
            "Values for key2 after re-insertion are not as expected"
        );
    }

    #[tokio::test]
    async fn test_truncate_boundary_case() {
        let mut map = setup().await;

        // Insert values
        map.insert("key1", 10).expect("Insert failed");
        map.insert("key1", 20).expect("Insert failed");
        map.insert("key1", 30).expect("Insert failed");

        // Truncate at value 30 (boundary case)
        map.truncate(30);

        let values = map.get("key1").expect("Get failed");
        assert_eq!(
            values,
            vec![10, 20, 30],
            "Values after boundary truncation are not as expected"
        );
    }

    #[tokio::test]
    async fn test_truncate_no_effect() {
        let mut map = setup().await;

        // Insert values
        map.insert("key1", 10).expect("Insert failed");
        map.insert("key1", 20).expect("Insert failed");

        // Truncate at value 30 (no values should be removed)
        map.truncate(30);

        let values = map.get("key1").expect("Get failed");
        assert_eq!(
            values,
            vec![10, 20],
            "Values after no-effect truncation are not as expected"
        );
    }
}
