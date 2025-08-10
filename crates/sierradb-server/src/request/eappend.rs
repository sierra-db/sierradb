use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_protocol::resp3::types::BytesFrame;
use sierradb::StreamId;
use sierradb::bucket::PartitionId;
use sierradb::database::{NewEvent, Transaction};
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash, uuid_v7_with_partition_hash};
use sierradb_cluster::write::execute::ExecuteTransaction;
use sierradb_protocol::{ErrorCode, ExpectedVersion};
use smallvec::smallvec;
use uuid::Uuid;

use crate::error::MapRedisError;
use crate::impl_command;
use crate::request::{HandleRequest, map, number, simple_str};
use crate::server::Conn;

/// Append an event to a stream.
///
/// # Syntax
/// ```text
/// EAPPEND <stream_id> <event_name> [EVENT_ID <event_id>] [PARTITION_KEY <partition_key>] [EXPECTED_VERSION <version>] [PAYLOAD <payload>] [METADATA <metadata>]
/// ```
///
/// # Parameters
/// - `stream_id`: Stream identifier to append the event to
/// - `event_name`: Name/type of the event
/// - `event_id` (optional): UUID for the event (auto-generated if not provided)
/// - `partition_key` (optional): UUID to determine event partitioning
/// - `expected_version` (optional): Expected stream version (number, "any",
///   "exists", "empty")
/// - `payload` (optional): Event payload data
/// - `metadata` (optional): Event metadata
///
/// # Example
/// ```text
/// EAPPEND my-stream UserCreated PAYLOAD '{"name":"john"}' METADATA '{"source":"api"}'
/// ```
pub struct EAppend {
    pub stream_id: StreamId,
    pub event_name: String,
    pub event_id: Option<Uuid>,
    pub partition_key: Option<Uuid>,
    pub expected_version: ExpectedVersion,
    pub timestamp: Option<u64>,
    pub payload: Vec<u8>,
    pub metadata: Vec<u8>,
}

impl_command!(
    EAppend,
    [stream_id, event_name],
    [
        event_id,
        partition_key,
        expected_version,
        timestamp,
        payload,
        metadata
    ]
);

impl HandleRequest for EAppend {
    type Error = String;
    type Ok = EAppendResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let partition_key = self
            .partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, self.stream_id.as_bytes()));
        let partition_hash = uuid_to_partition_hash(partition_key);
        let event_id = self
            .event_id
            .unwrap_or_else(|| uuid_v7_with_partition_hash(partition_hash));

        let partition_id = partition_hash % conn.num_partitions;
        let timestamp = self
            .timestamp
            .map(|timestamp| {
                timestamp.checked_mul(1_000_000).ok_or(
                    ErrorCode::InvalidArg
                        .with_message(
                            "invalid timestamp format: expected milliseconds, got nanoseconds",
                        )
                        .to_string(),
                )
            })
            .unwrap_or_else(|| {
                Ok(SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_redis_err()?
                    .as_nanos() as u64)
            })?;

        let transaction = match Transaction::new(
            partition_key,
            partition_id,
            smallvec![NewEvent {
                event_id,
                stream_id: self.stream_id,
                stream_version: self.expected_version,
                event_name: self.event_name,
                timestamp,
                metadata: self.metadata,
                payload: self.payload,
            }],
        ) {
            Ok(transaction) => transaction,
            Err(err) => return Err(err.to_string()),
        };

        let append = conn
            .cluster_ref
            .ask(ExecuteTransaction::new(transaction))
            .await
            .map_redis_err()?;

        let mut stream_versions = append.stream_versions.into_iter();
        let (_, stream_version) = stream_versions.next().unwrap();
        debug_assert_eq!(stream_versions.next(), None);
        debug_assert_eq!(
            append.first_partition_sequence,
            append.last_partition_sequence
        );

        Ok(Some(EAppendResp {
            event_id,
            partition_key,
            partition_id,
            partition_sequence: append.first_partition_sequence,
            stream_version,
            timestamp,
        }))
    }
}

pub struct EAppendResp {
    event_id: Uuid,
    partition_key: Uuid,
    partition_id: PartitionId,
    partition_sequence: u64,
    stream_version: u64,
    timestamp: u64,
}

impl From<EAppendResp> for BytesFrame {
    fn from(resp: EAppendResp) -> Self {
        map(HashMap::from_iter([
            (
                simple_str("event_id"),
                simple_str(resp.event_id.to_string()),
            ),
            (
                simple_str("partition_key"),
                simple_str(resp.partition_key.to_string()),
            ),
            (simple_str("partition_id"), number(resp.partition_id as i64)),
            (
                simple_str("partition_sequence"),
                number(resp.partition_sequence as i64),
            ),
            (
                simple_str("stream_version"),
                number(resp.stream_version as i64),
            ),
            (
                simple_str("timestamp"),
                number((resp.timestamp / 1_000_000) as i64),
            ),
        ]))
    }
}
