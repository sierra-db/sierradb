//! Custom log reading.
use std::io::SeekFrom;

use futures::Future;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, BufWriter},
};

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
        file: &mut BufWriter<File>,
        file_position: u32,
        bytes: usize,
    ) -> impl Future<Output = Result<Self::Result, MessageError>> + Send;
}

#[cfg(unix)]
#[derive(Default, Copy, Clone)]
/// Reader of the file segment into memory.
pub struct MessageBufReader;

impl LogSliceReader for MessageBufReader {
    type Result = MessageBuf;

    async fn read_from(
        &mut self,
        file: &mut BufWriter<File>,
        file_position: u32,
        bytes: usize,
    ) -> Result<Self::Result, MessageError> {
        let mut vec = vec![0; bytes];
        file.seek(SeekFrom::Start(file_position as u64)).await?;
        file.read_exact(&mut vec).await?;
        MessageBuf::from_bytes(vec)
    }
}
