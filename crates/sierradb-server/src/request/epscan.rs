use std::collections::HashMap;

use redis_protocol::resp3::types::BytesFrame;
use sierradb::bucket::segment::EventRecord;
use sierradb_cluster::read::ReadPartition;
use sierradb_protocol::ErrorCode;

use crate::error::MapRedisError;
use crate::impl_command;
use crate::request::{
    HandleRequest, PartitionSelector, RangeValue, array, encode_event, map, simple_str,
};
use crate::server::Conn;

/// Scan events in a partition by sequence number range.
///
/// # Syntax
/// ```text
/// EPSCAN <partition> <start_sequence> <end_sequence> [COUNT <count>]
/// ```
///
/// # Parameters
/// - `partition`: Partition selector (partition ID 0-65535 or UUID key)
/// - `start_sequence`: Starting sequence number (use "-" for beginning)
/// - `end_sequence`: Ending sequence number (use "+" for end, or specific
///   number)
/// - `count` (optional): Maximum number of events to return
///
/// # Examples
/// ```text
/// EPSCAN 42 100 200 COUNT 50
/// EPSCAN 550e8400-e29b-41d4-a716-446655440000 - + COUNT 100
/// ```
pub struct EPScan {
    pub partition: PartitionSelector,
    pub start_sequence: u64,
    pub end_sequence: RangeValue,
    pub count: Option<u64>,
}

impl_command!(EPScan, [partition, start_sequence, end_sequence], [count]);

impl HandleRequest for EPScan {
    type Error = String;
    type Ok = EPScanResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let end_sequence = match self.end_sequence {
            RangeValue::Start => {
                return Err(ErrorCode::Syntax.with_message("end_sequence cannot be '-'"));
            }
            RangeValue::End => None,
            RangeValue::Value(n) => Some(n),
        };

        let records = conn
            .cluster_ref
            .ask(ReadPartition {
                partition_id: self.partition.into_partition_id(conn.num_partitions),
                start_sequence: self.start_sequence,
                end_sequence,
                count: self.count.unwrap_or(100),
            })
            .await
            .map_redis_err()?;

        Ok(Some(EPScanResp {
            has_more: records.has_more,
            events: records.events,
        }))
    }
}

pub struct EPScanResp {
    has_more: bool,
    events: Vec<EventRecord>,
}

impl From<EPScanResp> for BytesFrame {
    fn from(resp: EPScanResp) -> Self {
        map(HashMap::from_iter([
            (simple_str("has_more"), resp.has_more.into()),
            (
                simple_str("events"),
                array(resp.events.into_iter().map(encode_event).collect()),
            ),
        ]))
    }
}
