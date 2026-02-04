use std::ops::Deref;

use bincode::{
    Decode, Encode,
    de::{Decoder, read::Reader},
    enc::{Encoder, write::Writer},
    error::{DecodeError, EncodeError},
    impl_borrow_decode,
};
use thiserror::Error;
use uuid::Uuid;

use crate::{MAX_REPLICATION_FACTOR, StreamId, bucket::PartitionId, error::ConfirmationCountError};

#[derive(Debug, Encode, Decode)]
pub struct RecordKindTimestamp(u64);

#[derive(Clone, Copy, Debug)]
pub enum RecordKind {
    Event,
    Commit,
}

#[derive(Clone, Copy, Debug, Error)]
#[error("timestamp must be less than 2^63")]
pub struct InvalidTimestamp;

impl RecordKindTimestamp {
    pub fn new(kind: RecordKind, timestamp: u64) -> Result<Self, InvalidTimestamp> {
        if (timestamp >> 63) == 1 {
            return Err(InvalidTimestamp);
        }

        let value = match kind {
            RecordKind::Event => timestamp,                 // High bit already 0
            RecordKind::Commit => timestamp | (1u64 << 63), // Set high bit
        };

        Ok(RecordKindTimestamp(value))
    }

    pub fn record_kind(&self) -> RecordKind {
        match (self.0 >> 63) & 1 {
            0 => RecordKind::Event,
            _ => RecordKind::Commit,
        }
    }

    pub fn timestamp(&self) -> u64 {
        self.0 & !(1u64 << 63)
    }
}

#[derive(Debug)]
pub struct ConfirmationCount(u8);

impl ConfirmationCount {
    pub fn new(confirmation_count: u8) -> Result<Self, ConfirmationCountError> {
        if confirmation_count > MAX_REPLICATION_FACTOR as u8 {
            return Err(ConfirmationCountError::ExceedsMaxReplicationFactor);
        }

        Ok(ConfirmationCount(confirmation_count))
    }

    pub fn get(&self) -> u8 {
        self.0
    }

    pub fn to_byte(&self) -> u8 {
        (self.0 << 4) | self.0 // Duplicates the lower 4 bits to the upper 4 bits for redundancy
    }

    pub fn from_byte(byte: u8) -> Result<Self, ConfirmationCountError> {
        let upper = byte >> 4;
        let lower = byte & 0x0F;

        if upper != lower {
            return Err(ConfirmationCountError::RedundancyCheck { upper, lower });
        }

        ConfirmationCount::new(upper)
    }
}

impl Encode for ConfirmationCount {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        debug_assert!(self.0 <= MAX_REPLICATION_FACTOR as u8);
        self.to_byte().encode(encoder) // Duplicates the lower 4 bits to the upper 4 bits for redundancy
    }
}

impl<C> Decode<C> for ConfirmationCount {
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let byte = u8::decode(decoder)?;
        ConfirmationCount::from_byte(byte).map_err(|err| DecodeError::OtherString(err.to_string()))
    }
}

#[derive(Debug, Encode, Decode)]
pub struct RecordHeader {
    pub timestamp: RecordKindTimestamp,
    pub transaction_id: [u8; 16],
}

impl RecordHeader {
    pub fn new(timestamp: RecordKindTimestamp, transaction_id: Uuid) -> Self {
        RecordHeader {
            timestamp,
            transaction_id: transaction_id.into_bytes(),
        }
    }

    pub fn new_event(timestamp: u64, transaction_id: Uuid) -> Result<Self, InvalidTimestamp> {
        Ok(Self::new(
            RecordKindTimestamp::new(RecordKind::Event, timestamp)?,
            transaction_id,
        ))
    }

    pub fn new_commit(timestamp: u64, transaction_id: Uuid) -> Result<Self, InvalidTimestamp> {
        Ok(Self::new(
            RecordKindTimestamp::new(RecordKind::Commit, timestamp)?,
            transaction_id,
        ))
    }
}

#[derive(Encode, Decode)]
pub struct RawEvent {
    pub header: RecordHeader,
    pub event_id: [u8; 16],
    pub partition_key: [u8; 16],
    pub partition_id: PartitionId,
    pub partition_sequence: u64,
    pub stream_version: u64,
    pub stream_id: StreamId,
    pub event_name: ShortString,
    pub metadata: LongBytes,
    pub payload: LongBytes,
}

#[derive(Encode, Decode)]
pub struct RawCommit {
    pub header: RecordHeader,
    pub event_count: u32,
}

/// A string with a max length of 255
pub struct ShortString(pub String);

impl ShortString {
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl Deref for ShortString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Encode for ShortString {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        u8::try_from(self.0.len())
            .map_err(|_| {
                EncodeError::OtherString(format!(
                    "string too long: length is {}, maximum allowed is {}",
                    self.0.len(),
                    u8::MAX
                ))
            })?
            .encode(encoder)?;

        encoder.writer().write(self.0.as_bytes())
    }
}

impl<C> Decode<C> for ShortString {
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let len = u8::decode(decoder)? as usize;
        decoder.claim_container_read::<u8>(len)?;
        let mut vec = vec![0; len];
        decoder.reader().read(&mut vec)?;
        Ok(ShortString(String::from_utf8(vec).map_err(|e| {
            DecodeError::Utf8 {
                inner: e.utf8_error(),
            }
        })?))
    }
}

impl_borrow_decode!(ShortString);

/// Bytes with a max length of u32::MAX
pub struct LongBytes(pub Vec<u8>);

impl LongBytes {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl Deref for LongBytes {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Encode for LongBytes {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        u32::try_from(self.0.len())
            .map_err(|_| {
                EncodeError::OtherString(format!(
                    "bytes too long: length is {}, maximum allowed is {}",
                    self.0.len(),
                    u32::MAX
                ))
            })?
            .encode(encoder)?;

        encoder.writer().write(&self.0)
    }
}

impl<C> Decode<C> for LongBytes {
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let len = u32::decode(decoder)? as usize;
        decoder.claim_container_read::<u8>(len)?;
        let mut vec = vec![0; len];
        decoder.reader().read(&mut vec)?;
        Ok(LongBytes(vec))
    }
}

impl_borrow_decode!(LongBytes);

#[cfg(test)]
mod tests {
    use crate::bucket::segment::BINCODE_CONFIG;

    use super::*;

    #[test]
    fn test_confirmation_count_encode() {
        let tests = [
            (0b0000_1100, 0b1100_1100),
            (0b0000_1010, 0b1010_1010),
            (0b0000_1001, 0b1001_1001),
            (0b0000_1000, 0b1000_1000),
        ];
        for (raw, expected) in tests {
            let confirmation_count = ConfirmationCount::new(raw).unwrap();
            assert_eq!(confirmation_count.to_byte(), expected);

            let bytes = bincode::encode_to_vec(&confirmation_count, BINCODE_CONFIG).unwrap();
            assert_eq!(bytes, [expected]);
        }
    }

    #[test]
    fn test_confirmation_count_decode() {
        let tests = [
            (0b1100_1100, 0b0000_1100),
            (0b1010_1010, 0b0000_1010),
            (0b1001_1001, 0b0000_1001),
            (0b1000_1000, 0b0000_1000),
        ];
        for (encoded, expected) in tests {
            let confirmation_count = ConfirmationCount::from_byte(encoded).unwrap();
            assert_eq!(confirmation_count.get(), expected);

            let (confirmation_count, _) =
                bincode::decode_from_slice::<ConfirmationCount, _>(&[encoded], BINCODE_CONFIG)
                    .unwrap();
            assert_eq!(confirmation_count.get(), expected);
        }
    }
}
