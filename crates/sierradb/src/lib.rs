use core::fmt;
use std::sync::Arc;
use std::{borrow, ops};

use bincode::de::Decoder;
use bincode::de::read::Reader;
use bincode::enc::Encoder;
use bincode::enc::write::Writer;
use bincode::error::{DecodeError, EncodeError};
use bincode::{Decode, Encode, impl_borrow_decode};
use error::StreamIdError;
use serde::{Deserialize, Serialize};

pub mod bucket;
pub mod cache;
pub mod database;
pub mod error;
pub mod id;
pub mod reader_thread_pool;
pub mod writer_thread_pool;

const BLOOM_SEED: [u8; 32] = [
    242, 218, 55, 84, 243, 117, 63, 59, 8, 112, 190, 73, 105, 98, 165, 58, 214, 159, 14, 184, 159,
    111, 33, 192, 108, 225, 81, 138, 231, 213, 234, 217,
];
pub const STREAM_ID_SIZE: usize = 64;
pub const MAX_REPLICATION_FACTOR: usize = 12;

#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamId {
    inner: Arc<str>,
}

impl StreamId {
    pub fn new(s: impl Into<Arc<str>>) -> Result<Self, StreamIdError> {
        let inner = s.into();
        if !(1..=STREAM_ID_SIZE).contains(&inner.len()) {
            return Err(StreamIdError::InvalidLength {
                input: inner.to_string(),
                len: inner.len(),
            });
        }

        if inner.contains('\0') {
            return Err(StreamIdError::ContainsNullByte);
        }

        Ok(StreamId { inner })
    }

    /// # Safety
    ///
    /// This function is safe in Rust, however can be unsafe if used with sierra
    /// when the string is invalid.
    ///
    /// Calling this function, you must ensure:
    /// - The string length is between 1 and 64
    /// - The string contains no null bytes
    pub unsafe fn new_unchecked(s: impl Into<Arc<str>>) -> Self {
        StreamId { inner: s.into() }
    }

    pub fn into_inner(self) -> Arc<str> {
        self.inner
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl AsRef<str> for StreamId {
    fn as_ref(&self) -> &str {
        self.inner.as_ref()
    }
}

impl ops::Deref for StreamId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl borrow::Borrow<str> for StreamId {
    fn borrow(&self) -> &str {
        self.inner.borrow()
    }
}

impl PartialEq<&str> for StreamId {
    fn eq(&self, other: &&str) -> bool {
        self.inner.as_ref() == *other
    }
}

impl PartialEq<StreamId> for &str {
    fn eq(&self, other: &StreamId) -> bool {
        *self == other.inner.as_ref()
    }
}

impl Encode for StreamId {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        (self.len() as u8).encode(encoder)?;
        encoder.writer().write(self.as_bytes())
    }
}

impl<C> Decode<C> for StreamId {
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let len = u8::decode(decoder)? as usize;
        decoder.claim_container_read::<u8>(len)?;
        let mut vec = vec![0; len];
        decoder.reader().read(&mut vec)?;
        let s = String::from_utf8(vec).map_err(|e| DecodeError::Utf8 {
            inner: e.utf8_error(),
        })?;
        StreamId::new(s).map_err(|err| DecodeError::OtherString(err.to_string()))
    }
}

impl_borrow_decode!(StreamId);

#[derive(Clone, Copy, Debug)]
pub enum IterDirection {
    Forward,
    Reverse,
}

#[macro_export]
macro_rules! from_bytes {
    ($buf:expr, $pos:expr, [ $( $t:tt ),* ]) => {
        ($(
            from_bytes!($buf, $pos, $t)
        ),*)
    };
    ($buf:expr, $pos:expr, str, $len:expr) => {
        from_bytes!($buf, $pos, $len, |buf| std::str::from_utf8(buf))
    };
    ($buf:expr, $pos:expr, &[u8], $len:expr) => {
        from_bytes!($buf, $pos, $len, |buf| buf)
    };
    ($buf:expr, $pos:expr, Uuid) => {
        from_bytes!($buf, $pos, std::mem::size_of::<Uuid>(), |buf| uuid::Uuid::from_bytes(buf.try_into().unwrap()))
    };
    ($buf:expr, $pos:expr, u16) => {
        from_bytes!($buf, $pos, u16=>from_le_bytes)
    };
    ($buf:expr, $pos:expr, u32) => {
        from_bytes!($buf, $pos, u32=>from_le_bytes)
    };
    ($buf:expr, $pos:expr, u64) => {
        from_bytes!($buf, $pos, u64=>from_le_bytes)
    };
    ($buf:expr, $pos:expr, $t:ty=>from_le_bytes) => {
        from_bytes!($buf, $pos, std::mem::size_of::<$t>(), |buf| <$t>::from_le_bytes(buf.try_into().unwrap()))
    };
    ($buf:expr, $pos:expr, $len:expr, |$slice:ident| $( $tt:tt )* ) => {{
        let buf = $buf;
        let pos = &mut $pos;
        let len = $len;
        let $slice = &buf[*pos..*pos + len];
        let res = {
            $( $tt )*
        };
        *pos += len;
        res
    }};
    ($buf:expr, $( $tt:tt )*) => {{
        let mut pos = 0;
        from_bytes!($buf, pos, $( $tt )*)
    }}
}
