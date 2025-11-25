use std::{
    io, mem,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use thiserror::Error;

pub mod read;
pub mod write;

const LEN_SIZE: usize = mem::size_of::<u32>();
const CRC32C_SIZE: usize = mem::size_of::<u32>();

/// Size of the record header in bytes, consisting of length and CRC32C checksum.
pub const RECORD_HEAD_SIZE: usize = LEN_SIZE + CRC32C_SIZE;

/// Thread-safe atomic offset tracking for flushed data.
///
/// Represents the byte offset in a segment file up to which all data has been safely
/// flushed to disk and can be read concurrently.
#[derive(Clone, Debug)]
pub struct FlushedOffset(Arc<AtomicU64>);

impl FlushedOffset {
    pub(crate) fn new(offset: u64) -> Self {
        FlushedOffset(Arc::new(AtomicU64::new(offset)))
    }

    pub(crate) fn set(&self, offset: u64) {
        self.0.store(offset, Ordering::Release)
    }

    /// Returns the current flushed offset value.
    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

/// Errors that can occur during segment reading operations.
#[derive(Debug, Error)]
pub enum ReadError {
    #[error("crc32c hash mismatch")]
    Crc32cMismatch,
    #[error("read offset and length is out of bounds")]
    OutOfBounds,
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Errors that can occur during segment writing operations.
#[derive(Debug, Error)]
pub enum WriteError {
    #[error("segment full: attempted to write {attempted} bytes, only {available} bytes remaining")]
    SegmentFull { attempted: u64, available: u64 },
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[cfg(target_os = "linux")]
    #[error(transparent)]
    Nix(#[from] nix::errno::Errno),
}

fn calculate_crc32c(len_bytes: &[u8], data: &[u8]) -> u32 {
    let mut crc_hasher = crc32fast::Hasher::new();
    crc_hasher.update(len_bytes);
    crc_hasher.update(data);
    crc_hasher.finalize()
}
