use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use redis_protocol::resp3::types::BytesFrame;
use sierradb::StreamId;
use sierradb::bucket::PartitionId;
use sierradb::database::{NewEvent, Transaction};
use sierradb::id::{uuid_to_partition_hash, uuid_v7_with_partition_hash};
use sierradb_cluster::write::execute::ExecuteTransaction;
use sierradb_protocol::{ErrorCode, ExpectedVersion};
use smallvec::SmallVec;
use uuid::Uuid;

use crate::error::MapRedisError;
use crate::request::{FromArgs, FromBytesFrame, HandleRequest, array, map, number, simple_str};
use crate::server::Conn;

/// Append multiple events to streams in a single transaction.
///
/// # Syntax
/// ```text
/// EMAPPEND <partition_key> <stream_id1> <event_name1> [EVENT_ID <event_id1>] [EXPECTED_VERSION <version1>] [PAYLOAD <payload1>] [METADATA <metadata1>] [<stream_id2> <event_name2> ...]
/// ```
///
/// # Parameters
/// - `partition_key`: UUID that determines which partition all events will be
///   written to
/// - For each event:
///   - `stream_id`: Stream identifier to append the event to
///   - `event_name`: Name/type of the event
///   - `event_id` (optional): UUID for the event (auto-generated if not
///     provided)
///   - `expected_version` (optional): Expected stream version (number, "any",
///     "exists", "empty")
///   - `timestamp` (optional): Event timestamp in milliseconds
///   - `payload` (optional): Event payload data
///   - `metadata` (optional): Event metadata
///
/// # Example
/// ```text
/// EMAPPEND 550e8400-e29b-41d4-a716-446655440000 stream1 EventA PAYLOAD '{"data":"value1"}' stream2 EventB PAYLOAD '{"data":"value2"}'
/// ```
///
/// **Note:** All events are appended atomically in a single transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EMAppend {
    pub partition_key: Uuid,
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Event {
    pub stream_id: StreamId,
    pub event_name: String,
    pub event_id: Option<Uuid>,
    pub expected_version: ExpectedVersion,
    pub timestamp: Option<u64>,
    pub payload: Vec<u8>,
    pub metadata: Vec<u8>,
}

impl FromArgs for EMAppend {
    fn from_args(args: &[BytesFrame]) -> Result<Self, String> {
        if args.is_empty() {
            return Err(ErrorCode::InvalidArg.with_message("missing partition_key argument"));
        }

        let mut i = 0;

        // Parse required partition_key (first positional argument)
        let partition_key = Uuid::from_bytes_frame(&args[i]).map_err(|err| {
            ErrorCode::InvalidArg
                .with_message(format!("failed to parse partition_key argument: {err}"))
        })?;
        i += 1;

        let mut events = Vec::new();

        while i < args.len() {
            // Parse stream_id (required for each event)
            let stream_id = StreamId::from_bytes_frame(&args[i]).map_err(|err| {
                ErrorCode::InvalidArg
                    .with_message(format!("failed to parse stream_id argument: {err}"))
            })?;
            i += 1;

            if i >= args.len() {
                return Err(ErrorCode::InvalidArg.with_message("missing event_name argument"));
            }

            // Parse event_name (required for each event)
            let event_name = String::from_bytes_frame(&args[i])?;
            i += 1;

            // Initialize optional fields
            let mut event_id = None;
            let mut expected_version = ExpectedVersion::default();
            let mut timestamp = None;
            let mut payload = Vec::new();
            let mut metadata = Vec::new();

            // Parse optional fields for this event
            while i < args.len() {
                let field_name_str = match <&str>::from_bytes_frame(&args[i]) {
                    Ok(s) => s.to_lowercase(),
                    Err(_) => break, // Not a string, probably next event's stream_id
                };

                // Check if this looks like a field name
                if ![
                    "event_id",
                    "expected_version",
                    "timestamp",
                    "payload",
                    "metadata",
                ]
                .contains(&field_name_str.as_str())
                {
                    // This is likely a new event's stream_id, don't consume it
                    break;
                }

                i += 1; // consume field name
                if i >= args.len() {
                    return Err(ErrorCode::InvalidArg
                        .with_message(format!("expected value after {field_name_str}")));
                }

                let value = &args[i];
                i += 1; // consume field value

                match field_name_str.as_str() {
                    "event_id" => {
                        event_id = FromBytesFrame::from_bytes_frame(value).map_err(|err| {
                            ErrorCode::InvalidArg
                                .with_message(format!("failed to parse event_id arg: {err}"))
                        })?;
                    }
                    "expected_version" => {
                        expected_version =
                            FromBytesFrame::from_bytes_frame(value).map_err(|err| {
                                ErrorCode::InvalidArg.with_message(format!(
                                    "failed to parse expected_version arg: {err}"
                                ))
                            })?;
                    }
                    "timestamp" => {
                        timestamp = FromBytesFrame::from_bytes_frame(value).map_err(|err| {
                            ErrorCode::InvalidArg
                                .with_message(format!("failed to parse timestamp arg: {err}"))
                        })?;
                    }
                    "payload" => {
                        payload = FromBytesFrame::from_bytes_frame(value).map_err(|err| {
                            ErrorCode::InvalidArg
                                .with_message(format!("failed to parse payload arg: {err}"))
                        })?;
                    }
                    "metadata" => {
                        metadata = FromBytesFrame::from_bytes_frame(value).map_err(|err| {
                            ErrorCode::InvalidArg
                                .with_message(format!("failed to parse metadata arg: {err}"))
                        })?;
                    }
                    _ => {
                        return Err(ErrorCode::InvalidArg
                            .with_message(format!("unknown field '{field_name_str}'")));
                    }
                }
            }

            events.push(Event {
                stream_id,
                event_name,
                event_id,
                expected_version,
                timestamp,
                payload,
                metadata,
            });
        }

        if events.is_empty() {
            return Err(ErrorCode::InvalidArg.with_message("at least one event required"));
        }

        Ok(EMAppend {
            partition_key,
            events,
        })
    }
}

impl HandleRequest for EMAppend {
    type Error = String;
    type Ok = EMAppendResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let partition_hash = uuid_to_partition_hash(self.partition_key);
        let partition_id = partition_hash % conn.num_partitions;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_redis_err()?
            .as_nanos() as u64;

        let events: SmallVec<[_; 4]> = self.events
            .into_iter()
            .map(|event| {
                let event_id = event
                    .event_id
                    .unwrap_or_else(|| uuid_v7_with_partition_hash(partition_hash));
                let timestamp = event
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
                    .transpose()?
                    .unwrap_or(now);
                Ok(NewEvent {
                    event_id,
                    stream_id: event.stream_id,
                    stream_version: event.expected_version,
                    event_name: event.event_name,
                    timestamp,
                    metadata: event.metadata,
                    payload: event.payload,
                })
            })
            .collect::<Result<_, String>>()?;
        let event_ids_timestamps: SmallVec<[_; 4]> = events
            .iter()
            .map(|event| (event.event_id, event.timestamp))
            .collect();

        let transaction = Transaction::new(self.partition_key, partition_id, events)
            .map_err(|err| ErrorCode::InvalidArg.with_message(err))?;

        let result = conn
            .cluster_ref
            .ask(ExecuteTransaction::new(transaction))
            .await
            .map_redis_err()?;

        let stream_versions = result.stream_versions.into_iter();
        let events = event_ids_timestamps
            .into_iter()
            .zip(stream_versions)
            .map(
                |((event_id, timestamp), (stream_id, stream_version))| EventInfo {
                    event_id,
                    stream_id,
                    stream_version,
                    timestamp,
                },
            )
            .collect();

        Ok(Some(EMAppendResp {
            partition_key: self.partition_key,
            partition_id,
            first_partition_sequence: result.first_partition_sequence,
            last_partition_sequence: result.last_partition_sequence,
            events,
        }))
    }
}

pub struct EMAppendResp {
    partition_key: Uuid,
    partition_id: PartitionId,
    first_partition_sequence: u64,
    last_partition_sequence: u64,
    events: Vec<EventInfo>,
}

struct EventInfo {
    event_id: Uuid,
    stream_id: StreamId,
    stream_version: u64,
    timestamp: u64,
}

impl From<EMAppendResp> for BytesFrame {
    fn from(resp: EMAppendResp) -> Self {
        map(HashMap::from_iter([
            (
                simple_str("partition_key"),
                simple_str(resp.partition_key.to_string()),
            ),
            (simple_str("partition_id"), number(resp.partition_id as i64)),
            (
                simple_str("first_partition_sequence"),
                number(resp.first_partition_sequence as i64),
            ),
            (
                simple_str("last_partition_sequence"),
                number(resp.last_partition_sequence as i64),
            ),
            (
                simple_str("events"),
                array(resp.events.into_iter().map(BytesFrame::from).collect()),
            ),
        ]))
    }
}

impl From<EventInfo> for BytesFrame {
    fn from(info: EventInfo) -> Self {
        map(HashMap::from_iter([
            (
                simple_str("event_id"),
                simple_str(info.event_id.to_string()),
            ),
            (
                simple_str("stream_id"),
                simple_str(info.stream_id.to_string()),
            ),
            (
                simple_str("stream_version"),
                number(info.stream_version as i64),
            ),
            (
                simple_str("timestamp"),
                number((info.timestamp / 1_000_000) as i64),
            ),
        ]))
    }
}
