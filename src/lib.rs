#![deny(elided_lifetimes_in_paths)]

use ahash::RandomState;

pub mod bucket;
pub mod database;
pub mod error;
pub mod id;
pub mod pool;
// pub mod swarm;

const SEED_K0: u64 = 0x53c8ff368077e723;
const SEED_K1: u64 = 0x586c670340740e26;
const SEED_K2: u64 = 0x34c309d85840faf5;
const SEED_K3: u64 = 0x392f381a75a0be2b;
const RANDOM_STATE: RandomState = RandomState::with_seeds(SEED_K0, SEED_K1, SEED_K2, SEED_K3);
const BLOOM_SEED: [u8; 32] = [
    242, 218, 55, 84, 243, 117, 63, 59, 8, 112, 190, 73, 105, 98, 165, 58, 214, 159, 14, 184, 159,
    111, 33, 192, 108, 225, 81, 138, 231, 213, 234, 217,
];
const MAX_REDUNDANCY: usize = 12;

#[macro_export]
macro_rules! copy_bytes {
    ($dest:expr, [ $( $src:expr $( => $len:expr $( ; + $add:expr )? )? $(,)? )* ]) => {{
        let mut pos = 0;
       $(
           copy_bytes!($dest, pos, $src $(, $len $( ; + $add )? )?);
       )*
    }};
    ($dest:expr, $pos:expr, [ $( $src:expr $( => $len:expr $( ; + $add:expr )? )? $(,)? )* ]) => {{
       $(
           copy_bytes!($dest, $pos, $src $(, $len $( ; + $add )? )?);
       )*
    }};
    ($dest:expr, $src:expr) => {{
        let len = $src.len();
        copy_bytes!($dest, 0, $src, len);
    }};
    ($dest:expr, $pos:expr, $src:expr) => {{
        let len = $src.len();
        copy_bytes!($dest, $pos, $src, len);
    }};
    ($dest:expr, $pos:expr, $src:expr, $len:expr) => {{
        let len = $len;
        copy_bytes!($dest, $pos, $src, len; + len);
    }};
    ($dest:expr, $pos:expr, $src:expr, $len:expr; + $add:expr) => {{
        let dest = &mut $dest;
        let pos = &mut $pos;
        let src = $src;
        let len = $len;
        let add = $add;
        dest[*pos..*pos + len].copy_from_slice(src);
        *pos += add;
    }};
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
