#![allow(unused_parens, deprecated)]

use uuid::{Uuid, uuid};

#[macro_use]
mod macros;
mod commands;
mod error;
mod options;
mod subscription;
mod types;

// Re-export public types
pub use commands::*;
// pub use connection::*;
pub use error::*;
pub use options::*;
pub use subscription::*;
pub use types::*;

// Re-export protocol types for convenience
pub use sierradb_protocol::{CurrentVersion, ErrorCode, ExpectedVersion, VersionGap};

// Uuid::new_v5(&Uuid::NAMESPACE_DNS, b"sierradb.tqwewe.com")
pub const NAMESPACE_PARTITION_KEY: Uuid = uuid!("219bd637-e279-53e9-9e2b-eabe5d9120cc");

pub fn stream_partition_key(stream_id: &str) -> Uuid {
    Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes())
}
