//! Message encoding used for the on-disk format for the log.
use super::Offset;
use byteorder::{ByteOrder, LittleEndian};
use bytes::BufMut;
use crc32c::{crc32c, crc32c_append};
use log::trace;
use std::{
    io::{self, Read},
    iter::{FromIterator, IntoIterator},
    u16,
};

/// Error for the message encoding or decoding.
#[derive(Debug)]
pub enum MessageError {
    /// `std::io` Error
    IoError(io::Error),
    /// Invalid crc32c hash encountered
    InvalidHash,
    /// Message payload is mismatched by the size field.
    InvalidPayloadLength,
}

#[derive(Debug, Copy, Clone)]
pub enum MessageSerializationError {
    /// The Metdata is too large to serialize
    MetadataExceedsLimit,
    /// The payload and metadata exceed the buffer size
    TotalSizeExceedsBuffer,
}

impl From<io::Error> for MessageError {
    fn from(e: io::Error) -> MessageError {
        MessageError::IoError(e)
    }
}

macro_rules! read_n {
    ($reader:expr, $buf:expr, $size:expr, $err_msg:expr) => {{
        match $reader.read(&mut $buf) {
            Ok(s) if s == $size => (),
            Ok(_) => {
                return Err(MessageError::IoError(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    $err_msg,
                )));
            }
            Err(e) => return Err(MessageError::IoError(e)),
        }
    }};
    ($reader:expr, $buf:expr, $size:expr) => {{
        match $reader.read(&mut $buf) {
            Ok(s) if s == $size => (),
            Ok(_) => return Err(MessageError::InvalidPayloadLength),
            Err(e) => return Err(MessageError::IoError(e)),
        }
    }};
}

pub const HEADER_SIZE: usize = 20;

macro_rules! read_header {
    (offset, $buf:expr) => {
        LittleEndian::read_u64(&$buf[0..8])
    };
    (size, $buf:expr) => {
        LittleEndian::read_u32(&$buf[8..12])
    };
    (hash, $buf:expr) => {
        LittleEndian::read_u32(&$buf[12..16])
    };
    (meta_size, $buf:expr) => {
        LittleEndian::read_u16(&$buf[18..20])
    };
}

macro_rules! set_header {
    (offset, $buf:expr, $v:expr) => {
        LittleEndian::write_u64(&mut $buf[0..8], $v)
    };
    (size, $buf:expr, $v:expr) => {
        LittleEndian::write_u32(&mut $buf[8..12], $v)
    };
    (hash, $buf:expr, $v:expr) => {
        LittleEndian::write_u32(&mut $buf[12..16], $v)
    };
    (meta_size, $buf:expr, $v:expr) => {
        LittleEndian::write_u16(&mut $buf[18..20], $v)
    };
}

/// Serializes a new message into a buffer
pub fn serialize<B: BufMut, M: AsRef<[u8]>, P: AsRef<[u8]>>(
    mut bytes: B,
    offset: u64,
    meta: M,
    payload: P,
) -> Result<(), MessageSerializationError> {
    let payload_slice = payload.as_ref();
    let meta_slice = meta.as_ref();
    if meta_slice.len() > (u16::MAX) as usize {
        return Err(MessageSerializationError::MetadataExceedsLimit);
    }

    let append_size = HEADER_SIZE + meta_slice.len() + payload_slice.len();
    if bytes.remaining_mut() < append_size {
        return Err(MessageSerializationError::TotalSizeExceedsBuffer);
    }

    let mut buf = [0; HEADER_SIZE];
    set_header!(offset, buf, offset);
    set_header!(size, buf, (meta_slice.len() + payload_slice.len()) as u32);
    set_header!(hash, buf, crc32c_append(crc32c(meta_slice), payload_slice));
    set_header!(meta_size, buf, meta_slice.len() as u16);

    // add the header
    bytes.put_slice(&buf);

    // metadata
    bytes.put_slice(meta_slice);

    // payload
    bytes.put_slice(payload_slice);

    Ok(())
}

/// Messages contain finite-sized binary values with an offset from
/// the beginning of the log.
///
/// | Bytes       | Encoding          | Value                          |
/// | ---------   | ----------------- | ------------------------------ |
/// | 0-7         | Little Endian u64 | Offset                         |
/// | 8-11        | Little Endian u32 | Payload and Metadata Size      |
/// | 12-15       | Little Endian u32 | CRC32C of payload and metadata |
/// | 16-17       |                   | Reserved                       |
/// | m: 18-19    | Little Endian u16 | Size of metadata               |
/// | 20-(20+m-1) |                   | Metadata                       |
/// | (20+m)      |                   | Payload                        |
#[derive(Debug)]
pub struct Message<'a> {
    bytes: &'a [u8],
}

impl<'a> Message<'a> {
    /// crc32c of the payload.
    #[inline]
    pub fn hash(&self) -> u32 {
        read_header!(hash, self.bytes)
    }

    /// Size of the payload.
    #[inline]
    pub fn size(&self) -> u32 {
        read_header!(size, self.bytes)
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.bytes.len()
    }

    /// Offset of the message in the log.
    #[inline]
    pub fn offset(&self) -> Offset {
        read_header!(offset, self.bytes)
    }

    /// Payload of the message.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.bytes[(HEADER_SIZE + self.metadata_size() as usize)..]
    }

    /// Size of the metadata bytes.
    #[inline]
    pub fn metadata_size(&self) -> u16 {
        read_header!(meta_size, self.bytes)
    }

    /// Metadata bytes of hte message.
    #[inline]
    pub fn metadata(&self) -> &[u8] {
        &self.bytes[HEADER_SIZE..(HEADER_SIZE + self.metadata_size() as usize)]
    }

    /// Check that the hash matches the hash of the payload.
    #[inline]
    pub fn verify_hash(&self) -> bool {
        self.hash() == crc32c(&self.bytes[HEADER_SIZE..])
    }
}

/// Iterator for Messages allowing mutation of offsets
#[derive(Debug)]
pub struct MessageMut<'a> {
    bytes: &'a mut [u8],
}

impl<'a> MessageMut<'a> {
    /// crc32c of the payload.
    #[inline]
    pub fn hash(&self) -> u32 {
        read_header!(hash, self.bytes)
    }

    /// Size of the payload.
    #[inline]
    pub fn size(&self) -> u32 {
        read_header!(size, self.bytes)
    }

    /// Offset of the message in the log.
    #[inline]
    pub fn offset(&self) -> Offset {
        read_header!(offset, self.bytes)
    }

    /// Payload of the message.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.bytes[(HEADER_SIZE + self.metadata_size() as usize)..]
    }

    /// Size of the metadata bytes.
    #[inline]
    pub fn metadata_size(&self) -> u16 {
        read_header!(meta_size, self.bytes)
    }

    /// Metadata bytes of hte message.
    #[inline]
    pub fn metadata(&self) -> &[u8] {
        &self.bytes[HEADER_SIZE..(HEADER_SIZE + self.metadata_size() as usize)]
    }

    /// Check that the hash matches the hash of the payload.
    #[inline]
    pub fn verify_hash(&self) -> bool {
        self.hash() == crc32c(&self.bytes[HEADER_SIZE..])
    }

    /// Sets the offset of the message
    #[inline]
    pub fn set_offset(&mut self, offset: u64) {
        set_header!(offset, self.bytes, offset);
    }
}

/// Iterator for `Message` within a `MessageSet`.
pub struct MessageIter<'a> {
    bytes: &'a [u8],
}

impl<'a> Iterator for MessageIter<'a> {
    type Item = Message<'a>;

    fn next(&mut self) -> Option<Message<'a>> {
        if self.bytes.len() < HEADER_SIZE {
            return None;
        }

        let size = read_header!(size, self.bytes) as usize;

        trace!("message iterator: size {} bytes", size);
        assert!(self.bytes.len() >= HEADER_SIZE + size);

        let message_slice = &self.bytes[0..HEADER_SIZE + size];
        self.bytes = &self.bytes[HEADER_SIZE + size..];
        Some(Message { bytes: message_slice })
    }
}

/// Iterator for `Message` within a `MessageSet`.
pub struct MessageMutIter<'a> {
    bytes: &'a mut [u8],
}

impl<'a> Iterator for MessageMutIter<'a> {
    type Item = MessageMut<'a>;

    fn next(&mut self) -> Option<MessageMut<'a>> {
        let slice = std::mem::take(&mut self.bytes);
        if slice.len() < HEADER_SIZE {
            return None;
        }

        let size = read_header!(size, slice) as usize;

        trace!("message iterator: size {} bytes", size);
        assert!(slice.len() >= HEADER_SIZE + size);

        let (message_slice, rest) = slice.split_at_mut(HEADER_SIZE + size);
        self.bytes = rest;
        Some(MessageMut { bytes: message_slice })
    }
}

/// Serialized log message set.
///
/// The bytes must be serialized in the format defined by `Message`.
pub trait MessageSet {
    /// Bytes that make up the serialized message set.
    fn bytes(&self) -> &[u8];

    /// Iterator on the messages in the message set.
    fn iter(&self) -> MessageIter {
        MessageIter { bytes: self.bytes() }
    }

    /// Number of messages in the message set.
    fn len(&self) -> usize {
        self.iter().count()
    }

    /// Indicator of whether there are messages within the `MessageSet`.
    fn is_empty(&self) -> bool {
        self.bytes().is_empty()
    }

    /// Verifies the hashes of all the messages, returning the
    /// index of a corrupt message when found.
    fn verify_hashes(&self) -> Result<(), usize> {
        for (i, msg) in self.iter().enumerate() {
            if !msg.verify_hash() {
                return Err(i);
            }
        }
        Ok(())
    }
}

/// Message set that can be mutated.
///
/// The mutation occurs once the `MessageSet` has been appended to the log. The
/// messages will contain the absolute offsets after the append opperation.
pub trait MessageSetMut: MessageSet {
    /// Bytes of the buffer for mutation.
    fn bytes_mut(&mut self) -> &mut [u8];

    /// Mutable iterator
    fn iter_mut(&mut self) -> MessageMutIter {
        MessageMutIter { bytes: self.bytes_mut() }
    }
}

/// Mutable message buffer.
///
/// The buffer will handle the serialization of the message into the proper
/// format expected by the `CommitLog`.
#[derive(Default)]
pub struct MessageBuf {
    bytes: Vec<u8>,
    len: usize,
}

impl MessageSet for MessageBuf {
    /// Bytes that make up the serialized message set.
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Number of messages in the message set.
    fn len(&self) -> usize {
        self.len
    }
}

impl MessageSetMut for MessageBuf {
    fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl<R: AsRef<[u8]>> FromIterator<R> for MessageBuf {
    fn from_iter<T>(iter: T) -> MessageBuf
    where
        T: IntoIterator<Item = R>,
    {
        let mut buf = MessageBuf::default();
        for v in iter.into_iter() {
            buf.push(v).expect("Total size of messages exceeds usize::MAX");
        }
        buf
    }
}

impl MessageBuf {
    /// Creates a message buffer from a previously serialized vector of bytes.
    /// Integrity checking is performed on the vector to ensure that it was
    /// properly serialized.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<MessageBuf, MessageError> {
        let mut msgs = 0usize;

        // iterate over the bytes to initialize size and ensure we have
        // a properly formed message set
        {
            let mut bytes = bytes.as_slice();
            while !bytes.is_empty() {
                // check that the offset, size and hash are present
                if bytes.len() < HEADER_SIZE {
                    return Err(MessageError::InvalidPayloadLength);
                }

                let size = read_header!(size, bytes) as usize;
                let hash = read_header!(hash, bytes);

                let next_msg_offset = HEADER_SIZE + size;
                if bytes.len() < next_msg_offset {
                    return Err(MessageError::InvalidPayloadLength);
                }

                // check the hash
                let payload_hash = crc32c(&bytes[HEADER_SIZE..next_msg_offset]);
                if payload_hash != hash {
                    return Err(MessageError::InvalidHash);
                }

                // update metadata
                msgs += 1;

                // move the slice along
                bytes = &bytes[next_msg_offset..];
            }
        }
        Ok(MessageBuf { bytes, len: msgs })
    }

    /// Clears the message buffer.
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.len = 0;
    }

    /// Clears the message buffer without dropping the contents.
    ///
    /// # Safety
    /// The bytes within the message buffer will remain. Implementations that
    /// wish to clear the buffer for security reasons should use `clear()`.
    pub unsafe fn unsafe_clear(&mut self) {
        self.bytes.set_len(0);
        self.len = 0;
    }

    /// Moves the underlying serialized bytes into a vector.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Adds a new message to the buffer.
    pub fn push<B: AsRef<[u8]>>(&mut self, payload: B) -> Result<(), MessageSerializationError> {
        // blank offset, expect the log to set the offsets
        // empty metadata
        let meta = [0u8; 0];
        serialize(&mut self.bytes, 0u64, &meta, payload)?;
        self.len += 1;
        Ok(())
    }

    /// Adds a new message with metadata.
    pub fn push_with_metadata<M: AsRef<[u8]>, B: AsRef<[u8]>>(
        &mut self,
        metadata: M,
        payload: B,
    ) -> Result<(), MessageSerializationError> {
        // blank offset, expect the log to set the offsets
        // empty metadata
        serialize(&mut self.bytes, 0u64, metadata, payload)?;
        self.len += 1;
        Ok(())
    }

    /// Reads a single message. The reader is expected to have a full message
    /// serialized.
    pub fn read<R: Read>(&mut self, reader: &mut R) -> Result<(), MessageError> {
        let mut buf = [0; HEADER_SIZE];
        read_n!(reader, buf, HEADER_SIZE, "Unable to read header");

        let size = read_header!(size, buf) as usize;
        let hash = read_header!(hash, buf);

        let mut bytes = vec![0; size];
        read_n!(reader, bytes, size);

        let payload_hash = crc32c(&bytes);
        if payload_hash != hash {
            return Err(MessageError::InvalidHash);
        }

        self.bytes.extend_from_slice(&buf);
        self.bytes.extend(bytes);

        self.len += 1;

        Ok(())
    }
}

/// Mutates the buffer with starting offset
pub fn set_offsets<S: MessageSetMut>(msg_set: &mut S, starting_offset: u64) {
    let mut offset = starting_offset;

    for mut msg in msg_set.iter_mut() {
        msg.set_offset(offset);
        offset += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;
    use std::io;

    #[test]
    fn message_construction() {
        env_logger::try_init().unwrap_or(());
        let mut msg_buf = MessageBuf::default();
        msg_buf.push("123456789").unwrap();
        msg_buf.push("000000000").unwrap();

        set_offsets(&mut msg_buf, 100);

        let mut msg_it = msg_buf.iter();
        {
            let msg = msg_it.next().unwrap();
            assert_eq!(msg.payload(), b"123456789");
            assert_eq!(msg.hash(), 3808858755);
            assert_eq!(msg.size(), 9u32);
            assert_eq!(msg.offset(), 100);
            assert!(msg.verify_hash());
        }
        {
            let msg = msg_it.next().unwrap();
            assert_eq!(msg.payload(), b"000000000");
            assert_eq!(msg.hash(), 49759193);
            assert_eq!(msg.size(), 9u32);
            assert_eq!(msg.offset(), 101);
            assert!(msg.verify_hash());
        }

        assert!(msg_it.next().is_none());
    }

    #[test]
    fn message_read() {
        let mut buf = Vec::new();
        serialize(&mut buf, 120, b"", b"123456789").unwrap();
        let mut buf_reader = io::BufReader::new(buf.as_slice());

        let mut reader = MessageBuf::default();
        let read_msg_result = reader.read(&mut buf_reader);
        assert!(read_msg_result.is_ok(), "result = {:?}", read_msg_result);

        let read_msg = reader.iter().next().unwrap();
        assert_eq!(read_msg.payload(), b"123456789");
        assert_eq!(read_msg.size(), 9u32);
        assert_eq!(read_msg.offset(), 120);
    }

    #[test]
    fn message_construction_with_metadata() {
        let mut buf = Vec::new();
        serialize(&mut buf, 120, b"123", b"456789").unwrap();
        let res = MessageBuf::from_bytes(buf).unwrap();
        let msg = res.iter().next().unwrap();
        assert_eq!(b"123", msg.metadata());
        assert_eq!(b"456789", msg.payload());
    }

    #[test]
    fn message_buf_push_with_meta() {
        let mut buf = MessageBuf::default();
        buf.push_with_metadata(b"123", b"456789").unwrap();
        let msg = buf.iter().next().unwrap();
        assert_eq!(b"123", msg.metadata());
        assert_eq!(b"456789", msg.payload());
    }

    #[test]
    fn message_read_invalid_hash() {
        let mut buf = Vec::new();
        serialize(&mut buf, 120, b"", b"123456789").unwrap();
        // mess with the payload such that the hash does not match
        let last_ind = buf.len() - 1;
        buf[last_ind] ^= buf[last_ind] + 1;
        let mut buf_reader = io::BufReader::new(buf.as_slice());

        let mut reader = MessageBuf::default();
        let read_msg_result = reader.read(&mut buf_reader);
        let matches_invalid_hash = match read_msg_result {
            Err(MessageError::InvalidHash) => true,
            _ => false,
        };
        assert!(
            matches_invalid_hash,
            "Invalid result, not Hash error. Result = {:?}",
            read_msg_result
        );
    }

    #[test]
    fn message_read_invalid_payload_length() {
        let mut buf = Vec::new();
        serialize(&mut buf, 120, b"", b"123456789").unwrap();
        // pop the last byte
        buf.pop();

        let mut buf_reader = io::BufReader::new(buf.as_slice());
        let mut msg_reader = MessageBuf::default();
        let read_msg_result = msg_reader.read(&mut buf_reader);
        let matches_invalid_hash = match read_msg_result {
            Err(MessageError::InvalidPayloadLength) => true,
            _ => false,
        };
        assert!(
            matches_invalid_hash,
            "Invalid result, not Hasherror. Result = {:?}",
            read_msg_result
        );
    }

    #[test]
    pub fn messagebuf_fromiterator() {
        let buf = vec!["test", "123"].iter().collect::<MessageBuf>();
        assert_eq!(2, buf.len());
    }

    #[test]
    pub fn messageset_deserialize() {
        let bytes = {
            let mut buf = MessageBuf::default();
            buf.push("foo").unwrap();
            buf.push("bar").unwrap();
            buf.push("baz").unwrap();
            set_offsets(&mut buf, 10);
            buf.into_bytes()
        };

        let bytes_copy = bytes;

        // deserialize it
        let res = MessageBuf::from_bytes(bytes_copy);
        assert!(res.is_ok());
        let res = res.unwrap();
        assert_eq!(3, res.len());

        let mut it = res.iter();

        {
            let m0 = it.next().unwrap();
            assert_eq!(10, m0.offset());
            assert_eq!(b"foo", m0.payload());
        }

        {
            let m1 = it.next().unwrap();
            assert_eq!(11, m1.offset());
            assert_eq!(b"bar", m1.payload());
        }

        {
            let m2 = it.next().unwrap();
            assert_eq!(12, m2.offset());
            assert_eq!(b"baz", m2.payload());
        }

        let n = it.next();
        assert!(n.is_none());
    }
}
