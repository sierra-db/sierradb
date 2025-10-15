//! Custom log reading.
use std::{fs::File, os::unix::fs::FileExt};

use crate::message::HEADER_SIZE;

use super::message::{MessageBuf, MessageError};

/// Trait that allows reading from a slice of the log.
pub trait LogSliceReader {
    /// Result type of this reader.
    type Result: 'static;

    /// Reads the slice of the file containing the message set.
    ///
    /// * `file` - The segment file that contains the slice of the log.
    /// * `file_position` - The offset within the file that starts the slice.
    /// * `bytes` - Total number of bytes, from the offset, that contains the
    ///   message set slice.
    fn read_from(
        &mut self,
        file: &File,
        file_position: u32,
        bytes: usize,
    ) -> Result<Self::Result, MessageError>;

    fn read_from_partial(
        &mut self,
        file: &File,
        file_position: u32,
        bytes: usize,
    ) -> Result<Self::Result, MessageError>;
}

pub struct MessageHeaderReader;

impl LogSliceReader for MessageHeaderReader {
    type Result = MessageBuf;

    fn read_from(
        &mut self,
        file: &File,
        file_pos: u32,
        _bytes: usize, // We ignore this because we know we only need HEADER_SIZE bytes
    ) -> Result<Self::Result, MessageError> {
        let mut buf = vec![0u8; HEADER_SIZE];
        file.read_at(&mut buf, file_pos as u64)?;
        Ok(MessageBuf::from_bytes(buf)?)
    }

    fn read_from_partial(
        &mut self,
        file: &File,
        file_pos: u32,
        _bytes: usize,
    ) -> Result<Self::Result, MessageError> {
        let mut buf = vec![0u8; HEADER_SIZE];
        file.read_at(&mut buf, file_pos as u64)?;
        Ok(MessageBuf::from_bytes_partial(buf)?)
    }
}

#[cfg(unix)]
#[derive(Default, Copy, Clone)]
/// Reader of the file segment into memory.
pub struct MessageBufReader;

impl LogSliceReader for MessageBufReader {
    type Result = MessageBuf;

    fn read_from(
        &mut self,
        file: &File,
        file_position: u32,
        bytes: usize,
    ) -> Result<Self::Result, MessageError> {
        let mut vec = vec![0; bytes];
        file.read_at(&mut vec, u64::from(file_position))?;
        MessageBuf::from_bytes(vec)
    }

    fn read_from_partial(
        &mut self,
        file: &File,
        file_position: u32,
        bytes: usize,
    ) -> Result<Self::Result, MessageError> {
        let mut vec = vec![0; bytes];
        file.read_at(&mut vec, u64::from(file_position))?;
        MessageBuf::from_bytes_partial(vec)
    }
}
