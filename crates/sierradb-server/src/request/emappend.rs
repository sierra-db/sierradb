use std::time::{SystemTime, UNIX_EPOCH};

use combine::error::StreamError;
use combine::{Parser, attempt, choice, easy, many, many1};
use indexmap::indexmap;
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
use crate::parser::{
    FrameStream, data, event_id, expected_version, keyword, number_u64, partition_key, stream_id,
    string,
};
use crate::request::{HandleRequest, array, map, number, simple_str};
use crate::server::Conn;

/// Append multiple events to streams in a single transaction.
///
/// # Syntax
/// ```text
/// EMAPPEND <partition_key> <stream_id1> <event_name1> [EVENT_ID <event_id1>] [EXPECTED_VERSION <version1>] [TIMESTAMP <timestamp>] [PAYLOAD <payload1>] [METADATA <metadata1>] [<stream_id2> <event_name2> ...]
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

impl Event {
    fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = Event> + 'a {
        (
            stream_id(),
            string().expected("event name"),
            many::<Vec<_>, _, _>(OptionalArg::parser()),
        )
            .and_then(|(stream_id, event_name, args)| {
                let mut cmd = Event {
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

#[derive(Debug, Clone, PartialEq)]
enum OptionalArg<'a> {
    EventId(Uuid),
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
            attempt(expected_version),
            attempt(timestamp),
            attempt(payload),
            attempt(metadata)
        )
    }
}

impl EMAppend {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = EMAppend> + 'a {
        (partition_key(), many1::<Vec<_>, _, _>(Event::parser())).map(|(partition_key, events)| {
            EMAppend {
                partition_key,
                events,
            }
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
        map(indexmap! {
            simple_str("partition_key") => simple_str(resp.partition_key.to_string()),
            simple_str("partition_id") => number(resp.partition_id as i64),
            simple_str("first_partition_sequence") => number(resp.first_partition_sequence as i64),
            simple_str("last_partition_sequence") => number(resp.last_partition_sequence as i64),
            simple_str("events") => array(resp.events.into_iter().map(BytesFrame::from).collect()),
        })
    }
}

impl From<EventInfo> for BytesFrame {
    fn from(info: EventInfo) -> Self {
        map(indexmap! {
            simple_str("event_id") => simple_str(info.event_id.to_string()),
            simple_str("stream_id") => simple_str(info.stream_id.to_string()),
            simple_str("stream_version") => number(info.stream_version as i64),
            simple_str("timestamp") => number((info.timestamp / 1_000_000) as i64),
        })
    }
}
