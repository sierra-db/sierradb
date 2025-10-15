use core::fmt;
use std::sync::Arc;
use std::{borrow, ops};

use ahash::RandomState;
use error::StreamIdError;
use serde::{Deserialize, Serialize};

pub mod bucket;
pub mod database;
pub mod error;
pub mod id;
pub mod reader_thread_pool;
pub mod writer_thread_pool;

const SEED_K0: u64 = 0x53C8FF368077E723;
const SEED_K1: u64 = 0x586C670340740E26;
const SEED_K2: u64 = 0x34C309D85840FAF5;
const SEED_K3: u64 = 0x392F381A75A0BE2B;
const RANDOM_STATE: RandomState = RandomState::with_seeds(SEED_K0, SEED_K1, SEED_K2, SEED_K3);
const BLOOM_SEED: [u8; 32] = [
    242, 218, 55, 84, 243, 117, 63, 59, 8, 112, 190, 73, 105, 98, 165, 58, 214, 159, 14, 184, 159,
    111, 33, 192, 108, 225, 81, 138, 231, 213, 234, 217,
];
pub const STREAM_ID_SIZE: usize = 64;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamId {
    inner: Arc<str>,
}

impl StreamId {
    pub fn new(s: impl Into<Arc<str>>) -> Result<Self, StreamIdError> {
        let inner = s.into();
        if !(1..=STREAM_ID_SIZE).contains(&inner.len()) {
            return Err(StreamIdError::InvalidLength);
        }

        if inner.contains('\0') {
            return Err(StreamIdError::ContainsNullByte);
        }

        Ok(StreamId { inner })
    }

    /// # Safety
    ///
    /// This function is safe in Rust, however can be unsafe if used with klio
    /// when the string is invalid.
    ///
    /// Calling this function, you must ensure:
    /// - The string length is between 1 and 64
    /// - The string contains no null bytes
    pub unsafe fn new_unchecked(s: impl Into<Arc<str>>) -> Self {
        StreamId { inner: s.into() }
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
