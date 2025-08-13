use combine::{Parser, optional};
use redis_protocol::resp3::types::BytesFrame;
use sierradb::StreamId;
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash};
use sierradb_cluster::read::GetStreamVersion;
use uuid::Uuid;

use crate::error::MapRedisError;
use crate::parser::{FrameStream, keyword, partition_key, stream_id};
use crate::request::{HandleRequest, number};
use crate::server::Conn;

/// Get the current version number for a stream.
///
/// # Syntax
/// ```text
/// ESVER <stream_id> [PARTITION_KEY <partition_key>]
/// ```
///
/// # Parameters
/// - `stream_id`: Stream identifier to get version for
/// - `partition_key` (optional): UUID to check specific partition
///
/// # Examples
/// ```text
/// ESVER my-stream
/// ESVER my-stream PARTITION_KEY 550e8400-e29b-41d4-a716-446655440000
/// ```
pub struct ESVer {
    pub stream_id: StreamId,
    pub partition_key: Option<Uuid>,
}

impl ESVer {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = ESVer> + 'a {
        (
            stream_id(),
            optional(keyword("PARTITION_KEY").with(partition_key())),
        )
            .map(|(stream_id, partition_key)| ESVer {
                stream_id,
                partition_key,
            })
    }
}

impl HandleRequest for ESVer {
    type Error = String;
    type Ok = ESVerResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let partition_key = self
            .partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, self.stream_id.as_bytes()));
        let partition_id = uuid_to_partition_hash(partition_key) % conn.num_partitions;

        let version = conn
            .cluster_ref
            .ask(GetStreamVersion {
                partition_id,
                stream_id: self.stream_id,
            })
            .await
            .map_redis_err()?;

        Ok(Some(ESVerResp { version }))
    }
}

pub struct ESVerResp {
    version: Option<u64>,
}

impl From<ESVerResp> for BytesFrame {
    fn from(resp: ESVerResp) -> Self {
        match resp.version {
            Some(n) => number(n as i64),
            None => BytesFrame::Null,
        }
    }
}
