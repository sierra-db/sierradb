use combine::error::StreamError;
use combine::{Parser, attempt, choice, easy, many};
use indexmap::indexmap;
use redis_protocol::resp3::types::BytesFrame;
use sierradb::StreamId;
use sierradb::bucket::segment::EventRecord;
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash};
use sierradb_cluster::read::ReadStream;
use sierradb_protocol::ErrorCode;
use uuid::Uuid;

use crate::error::MapRedisError;
use crate::parser::{FrameStream, keyword, number_u64, partition_key, range_value, stream_id};
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
    pub start_version: RangeValue,
    pub end_version: RangeValue,
    pub partition_key: Option<Uuid>,
    pub count: Option<u64>,
}

impl EScan {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = EScan> + 'a {
        (
            stream_id(),
            range_value(),
            range_value(),
            many::<Vec<_>, _, _>(OptionalArg::parser()),
        )
            .and_then(|(stream_id, start_version, end_version, args)| {
                let mut cmd = EScan {
                    stream_id,
                    start_version,
                    end_version,
                    partition_key: None,
                    count: None,
                };

                for arg in args {
                    match arg {
                        OptionalArg::PartitionKey(partition_key) => {
                            if cmd.partition_key.is_some() {
                                return Err(easy::Error::message_format(
                                    "partition key already specified",
                                ));
                            }

                            cmd.partition_key = Some(partition_key);
                        }
                        OptionalArg::Count(count) => {
                            if cmd.count.is_some() {
                                return Err(easy::Error::message_format("count already specified"));
                            }

                            cmd.count = Some(count);
                        }
                    }
                }

                Ok(cmd)
            })
    }
}

#[derive(Debug, Clone, PartialEq)]
enum OptionalArg {
    PartitionKey(Uuid),
    Count(u64),
}

impl OptionalArg {
    fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = OptionalArg> + 'a {
        let partition_key = keyword("PARTITION_KEY")
            .with(partition_key())
            .map(OptionalArg::PartitionKey);
        let count = keyword("COUNT").with(number_u64()).map(OptionalArg::Count);

        choice!(attempt(partition_key), attempt(count))
    }
}

impl HandleRequest for EScan {
    type Error = String;
    type Ok = EScanResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let partition_key = self
            .partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, self.stream_id.as_bytes()));
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % conn.num_partitions;

        let start_version = match self.start_version {
            RangeValue::Start => 0,
            RangeValue::End => {
                return Err(ErrorCode::InvalidArg.with_message("start version cannot be '+'"));
            }
            RangeValue::Value(n) => n,
        };

        let end_version = match self.end_version {
            RangeValue::Start => {
                return Err(ErrorCode::InvalidArg.with_message("end version cannot be '-'"));
            }
            RangeValue::End => None,
            RangeValue::Value(n) => Some(n),
        };

        let records = conn
            .cluster_ref
            .ask(ReadStream {
                stream_id: self.stream_id,
                partition_id,
                start_version,
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
        map(indexmap! {
            simple_str("has_more") => resp.has_more.into(),
            simple_str("events") => array(resp.events.into_iter().map(encode_event).collect()),
        })
    }
}
