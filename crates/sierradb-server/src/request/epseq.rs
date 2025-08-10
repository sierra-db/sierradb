use redis_protocol::resp3::types::BytesFrame;
use sierradb_cluster::read::GetPartitionSequence;

use crate::error::MapRedisError;
use crate::impl_command;
use crate::request::{HandleRequest, PartitionSelector, number};
use crate::server::Conn;

/// Get the current sequence number for a partition.
///
/// # Syntax
/// ```text
/// EPSEQ <partition>
/// ```
///
/// # Parameters
/// - `partition`: Partition selector (partition ID 0-65535 or UUID key)
///
/// # Examples
/// ```text
/// EPSEQ 42
/// EPSEQ 550e8400-e29b-41d4-a716-446655440000
/// ```
pub struct EPSeq {
    pub partition: PartitionSelector,
}

impl_command!(EPSeq, [partition], []);

impl HandleRequest for EPSeq {
    type Error = String;
    type Ok = EPSeqResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let partition_id = self.partition.into_partition_id(conn.num_partitions);

        let sequence = conn
            .cluster_ref
            .ask(GetPartitionSequence { partition_id })
            .await
            .map_redis_err()?;

        Ok(Some(EPSeqResp { sequence }))
    }
}

pub struct EPSeqResp {
    sequence: Option<u64>,
}

impl From<EPSeqResp> for BytesFrame {
    fn from(resp: EPSeqResp) -> Self {
        match resp.sequence {
            Some(n) => number(n as i64),
            None => BytesFrame::Null,
        }
    }
}
