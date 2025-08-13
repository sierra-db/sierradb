use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use combine::error::StreamError;
use combine::{Parser, attempt, choice, easy, many};
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
use crate::parser::{
    FrameStream, data, event_id, expected_version, keyword, number_u64, partition_key, stream_id,
    string,
};
use crate::request::{HandleRequest, map, number, simple_str};
use crate::server::Conn;

/// Append an event to a stream.
///
/// # Syntax
/// ```text
/// EAPPEND <stream_id> <event_name> [EVENT_ID <event_id>] [PARTITION_KEY <partition_key>] [EXPECTED_VERSION <version>] [TIMESTAMP <timestamp>] [PAYLOAD <payload>] [METADATA <metadata>]
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
#[derive(Clone, Debug, Default)]
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

#[derive(Debug, Clone, PartialEq)]
enum OptionalArg<'a> {
    EventId(Uuid),
    PartitionKey(Uuid),
    ExpectedVersion(ExpectedVersion),
    Timestamp(u64),
    Payload(&'a [u8]),
    Metadata(&'a [u8]),
}

impl<'a> OptionalArg<'a> {
    fn parser() -> impl Parser<FrameStream<'a>, Output = OptionalArg<'a>> + 'a {
        let event_id = keyword("EVENT_ID")
            .with(event_id())
            .map(OptionalArg::EventId);
        let partition_key = keyword("PARTITION_KEY")
            .with(partition_key())
            .map(OptionalArg::PartitionKey);
        let expected_version = keyword("EXPECTED_VERSION")
            .with(expected_version())
            .map(OptionalArg::ExpectedVersion);
        let timestamp = keyword("TIMESTAMP")
            .with(number_u64())
            .map(OptionalArg::Timestamp);
        let payload = keyword("PAYLOAD").with(data()).map(OptionalArg::Payload);
        let metadata = keyword("METADATA").with(data()).map(OptionalArg::Metadata);

        choice!(
            attempt(event_id),
            attempt(partition_key),
            attempt(expected_version),
            attempt(timestamp),
            attempt(payload),
            attempt(metadata)
        )
    }
}

impl EAppend {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = EAppend> + 'a {
        (
            stream_id(),
            string().expected("event name"),
            many::<Vec<_>, _, _>(OptionalArg::parser()),
        )
            .and_then(|(stream_id, event_name, args)| {
                let mut cmd = EAppend {
                    stream_id,
                    event_name: event_name.to_string(),
                    ..Default::default()
                };

                for arg in args {
                    match arg {
                        OptionalArg::EventId(event_id) => {
                            if cmd.event_id.is_some() {
                                return Err(easy::Error::message_format(
                                    "event id already specified",
                                ));
                            }

                            cmd.event_id = Some(event_id);
                        }
                        OptionalArg::PartitionKey(partition_key) => {
                            if cmd.partition_key.is_some() {
                                return Err(easy::Error::message_format(
                                    "partition key already specified",
                                ));
                            }

                            cmd.partition_key = Some(partition_key);
                        }
                        OptionalArg::ExpectedVersion(expected_version) => {
                            if !matches!(cmd.expected_version, ExpectedVersion::Any) {
                                return Err(easy::Error::message_format(
                                    "expected version already specified",
                                ));
                            }

                            cmd.expected_version = expected_version;
                        }
                        OptionalArg::Timestamp(timestamp) => {
                            if cmd.timestamp.is_some() {
                                return Err(easy::Error::message_format(
                                    "timestamp already specified",
                                ));
                            }

                            cmd.timestamp = Some(timestamp);
                        }
                        OptionalArg::Payload(payload) => {
                            if !cmd.payload.is_empty() {
                                return Err(easy::Error::message_format(
                                    "payload already specified",
                                ));
                            }

                            cmd.payload = payload.to_vec();
                        }
                        OptionalArg::Metadata(metadata) => {
                            if !cmd.metadata.is_empty() {
                                return Err(easy::Error::message_format(
                                    "metadata already specified",
                                ));
                            }

                            cmd.metadata = metadata.to_vec();
                        }
                    }
                }

                Ok(cmd)
            })
    }
}

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
