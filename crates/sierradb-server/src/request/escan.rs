use std::collections::HashMap;

use redis_protocol::resp3::types::BytesFrame;
use sierradb::StreamId;
use sierradb::bucket::segment::EventRecord;
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash};
use sierradb_cluster::read::ReadStream;
use sierradb_protocol::ErrorCode;
use uuid::Uuid;

use crate::error::MapRedisError;
use crate::impl_command;
use crate::request::{HandleRequest, RangeValue, array, encode_event, map, simple_str};
use crate::server::Conn;

/// Scan events in a stream by version range.
///
/// # Syntax
/// ```text
/// ESCAN <stream_id> <start_version> <end_version> [PARTITION_KEY <partition_key>] [COUNT <count>]
/// ```
///
/// # Parameters
/// - `stream_id`: Stream identifier to scan
/// - `start_version`: Starting version number (use "-" for beginning)
/// - `end_version`: Ending version number (use "+" for end, or specific number)
/// - `partition_key` (optional): UUID to scan specific partition
/// - `count` (optional): Maximum number of events to return
///
/// # Examples
/// ```text
/// ESCAN my-stream 0 100 COUNT 50
/// ESCAN my-stream - + PARTITION_KEY 550e8400-e29b-41d4-a716-446655440000
/// ```
pub struct EScan {
    pub stream_id: StreamId,
    pub start_version: u64,
    pub end_version: RangeValue,
    pub partition_key: Option<Uuid>,
    pub count: Option<u64>,
}

impl_command!(
    EScan,
    [stream_id, start_version, end_version],
    [partition_key, count]
);

impl HandleRequest for EScan {
    type Error = String;
    type Ok = EScanResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let partition_key = self
            .partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, self.stream_id.as_bytes()));
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % conn.num_partitions;

        let end_version = match self.end_version {
            RangeValue::Start => {
                return Err(ErrorCode::Syntax.with_message("end_version cannot be '-'"));
            }
            RangeValue::End => None,
            RangeValue::Value(n) => Some(n),
        };

        let records = conn
            .cluster_ref
            .ask(ReadStream {
                stream_id: self.stream_id,
                partition_id,
                start_version: self.start_version,
                end_version,
                count: self.count.unwrap_or(100),
            })
            .await
            .map_redis_err()?;

        Ok(Some(EScanResp {
            has_more: records.has_more,
            events: records.events,
        }))
    }
}

pub struct EScanResp {
    has_more: bool,
    events: Vec<EventRecord>,
}

impl From<EScanResp> for BytesFrame {
    fn from(resp: EScanResp) -> Self {
        map(HashMap::from_iter([
            (simple_str("has_more"), resp.has_more.into()),
            (
                simple_str("events"),
                array(resp.events.into_iter().map(encode_event).collect()),
            ),
        ]))
    }
}
