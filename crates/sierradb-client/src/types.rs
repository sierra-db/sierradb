use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::{FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! parse_value {
    (String, $value:ident, $field:literal) => {
        match $value {
            Value::SimpleString(s) => Ok(s.clone()),
            Value::BulkString(s) => String::from_utf8(s.clone()).map_err(|_| {
                RedisError::from((
                    redis::ErrorKind::TypeError,
                    concat!("Invalid string for ", $field),
                ))
            }),
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                concat!($field, " must be a string"),
            ))),
        }
    };
    (Uuid, $value:ident, $field:literal) => {
        match $value {
            Value::SimpleString(s) => Uuid::parse_str(s).map_err(|_| {
                RedisError::from((
                    redis::ErrorKind::TypeError,
                    concat!("Invalid UUID format for ", $field),
                ))
            }),
            Value::BulkString(s) => str::from_utf8(s)
                .ok()
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        concat!("Invalid UUID format for ", $field),
                    ))
                }),
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                concat!($field, " must be a string"),
            ))),
        }
    };
}

/// Represents messages received from SierraDB subscriptions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SierraMessage {
    /// An event received from a subscription with cursor information.
    Event { event: Event, cursor: u64 },
    /// Confirmation that a subscription was successfully created.
    SubscriptionConfirmed { subscription_count: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendInfo {
    pub event_id: Uuid,
    pub partition_key: Uuid,
    pub partition_id: u16,
    pub partition_sequence: u64,
    pub stream_version: u64,
    pub timestamp: SystemTime,
}

impl FromRedisValue for AppendInfo {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Map(fields) => {
                let mut event_id = None;
                let mut partition_key = None;
                let mut partition_id = None;
                let mut partition_sequence = None;
                let mut stream_version = None;
                let mut timestamp = None;

                // Extract fields from map
                for (key, val) in fields {
                    let field_name = match key {
                        Value::SimpleString(s) => s.as_str(),
                        _ => continue,
                    };

                    match field_name {
                        "event_id" => {
                            event_id = Some(parse_value!(Uuid, val, "event_id")?);
                        }
                        "partition_key" => {
                            partition_key = Some(parse_value!(Uuid, val, "partition_key")?);
                        }
                        "partition_id" => {
                            partition_id = Some(match val {
                                Value::Int(n) => *n as u16,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "partition_id must be an integer",
                                    )));
                                }
                            });
                        }
                        "partition_sequence" => {
                            partition_sequence = Some(match val {
                                Value::Int(n) => *n as u64,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "partition_sequence must be an integer",
                                    )));
                                }
                            });
                        }
                        "stream_version" => {
                            stream_version = Some(match val {
                                Value::Int(n) => *n as u64,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "stream_version must be an integer",
                                    )));
                                }
                            });
                        }
                        "timestamp" => {
                            timestamp = Some(match val {
                                Value::Int(ms) => {
                                    let duration = Duration::from_millis(*ms as u64);
                                    UNIX_EPOCH + duration
                                }
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "timestamp must be an integer",
                                    )));
                                }
                            });
                        }
                        _ => {} // Ignore unknown fields
                    }
                }

                // Ensure all required fields are present
                let event_id = event_id.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: event_id",
                    ))
                })?;
                let partition_key = partition_key.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: partition_key",
                    ))
                })?;
                let partition_id = partition_id.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: partition_id",
                    ))
                })?;
                let partition_sequence = partition_sequence.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: partition_sequence",
                    ))
                })?;
                let stream_version = stream_version.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: stream_version",
                    ))
                })?;
                let timestamp = timestamp.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: timestamp",
                    ))
                })?;

                Ok(AppendInfo {
                    event_id,
                    partition_key,
                    partition_id,
                    partition_sequence,
                    stream_version,
                    timestamp,
                })
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Append info must be a Redis map",
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventBatch {
    pub events: Vec<Event>,
    pub has_more: bool,
}

impl FromRedisValue for EventBatch {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Map(fields) => {
                let mut has_more = None;
                let mut events = None;

                // Extract fields from map
                for (key, val) in fields {
                    let field_name = match key {
                        Value::SimpleString(s) => s.as_str(),
                        _ => continue,
                    };

                    match field_name {
                        "has_more" => {
                            has_more = Some(match val {
                                Value::Boolean(b) => *b,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "has_more must be a boolean",
                                    )));
                                }
                            });
                        }
                        "events" => {
                            events = Some(match val {
                                Value::Array(event_values) => event_values
                                    .iter()
                                    .map(Event::from_redis_value)
                                    .collect::<Result<Vec<_>, _>>()?,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "events must be an array",
                                    )));
                                }
                            });
                        }
                        _ => {} // Ignore unknown fields
                    }
                }

                // Ensure all required fields are present
                let has_more = has_more.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: has_more",
                    ))
                })?;
                let events = events.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: events",
                    ))
                })?;

                Ok(EventBatch { events, has_more })
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Event batch must be a Redis map",
            ))),
        }
    }
}

/// Represents a single event record in the event store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Globally unique identifier for this specific event.
    /// Generated once when the event is created and never changes.
    pub event_id: Uuid,

    /// Determines which partition this event belongs to.
    ///
    /// This is typically a domain-significant identifier (like customer ID,
    /// tenant ID) that groups related events together. All events for the
    /// same stream must share the same partition key.
    pub partition_key: Uuid,

    /// The numeric partition identifier (0-1023) derived from the
    /// partition_key.
    ///
    /// Events with the same partition_id have a guaranteed total ordering
    /// defined by their partition_sequence, regardless of which stream they
    /// belong to.
    pub partition_id: u16,

    /// Identifier for multi-event transactions.
    ///
    /// When multiple events are saved as part of a single transaction, they
    /// share this identifier. For events not part of a transaction, this
    /// may be a null UUID.
    pub transaction_id: Uuid,

    /// The monotonic, gapless sequence number within the partition.
    ///
    /// This defines the total ordering of events within a partition. Each new
    /// event in a partition receives a sequence number exactly one higher
    /// than the previous event.
    pub partition_sequence: u64,

    /// The version number of the entity/aggregate after this event is applied.
    ///
    /// This is a monotonic, gapless counter specific to the stream. It starts
    /// at 0 and increments by 1 for each event in the stream. Used for
    /// optimistic concurrency control and to determine the current state
    /// version of an entity.
    pub stream_version: u64,

    /// Unix timestamp (in milliseconds) when the event was created.
    ///
    /// Useful for time-based queries and analysis, though not used for event
    /// ordering.
    pub timestamp: SystemTime,

    /// Identifier for the stream (entity/aggregate) this event belongs to.
    ///
    /// Typically corresponds to a domain entity ID, like "account-123" or
    /// "order-456". All events for the same entity share the same
    /// stream_id.
    pub stream_id: String,

    /// Name of the event type, used for deserialization and event handling.
    ///
    /// Examples: "AccountCreated", "OrderShipped", "PaymentRefunded".
    /// Should be meaningful in the domain context.
    pub event_name: String,

    /// Additional system or application metadata about the event.
    ///
    /// May include information like user ID, correlation IDs, causation IDs,
    /// or other contextual data not part of the event payload itself.
    pub metadata: Vec<u8>,

    /// The actual event data serialized as bytes.
    ///
    /// Contains the domain-specific information that constitutes the event.
    /// Must be deserializable based on the event_name.
    pub payload: Vec<u8>,
}

impl Event {
    /// Get the sequence/version number to use for acknowledgment.
    ///
    /// For stream subscriptions, this returns the stream_version.
    /// For partition subscriptions, this returns the partition_sequence.
    /// Use this value with `acknowledge_events()` to acknowledge this event.
    pub fn sequence_or_version_for_stream(&self) -> u64 {
        self.stream_version
    }

    /// Get the sequence number to use for partition subscription
    /// acknowledgment.
    ///
    /// Use this value with `acknowledge_events()` when acknowledging
    /// events from a partition subscription.
    pub fn sequence_for_partition(&self) -> u64 {
        self.partition_sequence
    }

    /// Get the version number to use for stream subscription acknowledgment.
    ///
    /// Use this value with `acknowledge_events()` when acknowledging
    /// events from a stream subscription.
    pub fn version_for_stream(&self) -> u64 {
        self.stream_version
    }
}

impl FromRedisValue for Event {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Map(fields) => {
                let mut event_id = None;
                let mut partition_key = None;
                let mut partition_id = None;
                let mut transaction_id = None;
                let mut partition_sequence = None;
                let mut stream_version = None;
                let mut timestamp = None;
                let mut stream_id = None;
                let mut event_name = None;
                let mut metadata = None;
                let mut payload = None;

                // Extract fields from map
                for (key, val) in fields {
                    let field_name = match key {
                        Value::SimpleString(s) => s.as_str(),
                        _ => continue,
                    };

                    match field_name {
                        "event_id" => {
                            event_id = Some(parse_value!(Uuid, val, "event_id")?);
                        }
                        "partition_key" => {
                            partition_key = Some(parse_value!(Uuid, val, "partition_key")?);
                        }
                        "partition_id" => {
                            partition_id = Some(match val {
                                Value::Int(n) => *n as u16,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "partition_id must be an integer",
                                    )));
                                }
                            });
                        }
                        "transaction_id" => {
                            transaction_id = Some(parse_value!(Uuid, val, "transaction_id")?);
                        }
                        "partition_sequence" => {
                            partition_sequence = Some(match val {
                                Value::Int(n) => *n as u64,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "partition_sequence must be an integer",
                                    )));
                                }
                            });
                        }
                        "stream_version" => {
                            stream_version = Some(match val {
                                Value::Int(n) => *n as u64,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "stream_version must be an integer",
                                    )));
                                }
                            });
                        }
                        "timestamp" => {
                            timestamp = Some(match val {
                                Value::Int(ms) => {
                                    let duration = Duration::from_millis(*ms as u64);
                                    UNIX_EPOCH + duration
                                }
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "timestamp must be an integer",
                                    )));
                                }
                            });
                        }
                        "stream_id" => {
                            stream_id = Some(parse_value!(String, val, "stream_id")?);
                        }
                        "event_name" => {
                            event_name = Some(parse_value!(String, val, "event_name")?);
                        }
                        "metadata" => {
                            metadata = Some(match val {
                                Value::BulkString(data) => data.clone(),
                                Value::Nil => Vec::new(), // Handle null metadata
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "metadata must be bulk data",
                                    )));
                                }
                            });
                        }
                        "payload" => {
                            payload = Some(match val {
                                Value::BulkString(data) => data.clone(),
                                Value::Nil => Vec::new(),
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "payload must be bulk data",
                                    )));
                                }
                            });
                        }
                        _ => {} // Ignore unknown fields
                    }
                }

                // Ensure all required fields are present
                let event_id = event_id.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: event_id",
                    ))
                })?;
                let partition_key = partition_key.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: partition_key",
                    ))
                })?;
                let partition_id = partition_id.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: partition_id",
                    ))
                })?;
                let transaction_id = transaction_id.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: transaction_id",
                    ))
                })?;
                let partition_sequence = partition_sequence.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: partition_sequence",
                    ))
                })?;
                let stream_version = stream_version.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: stream_version",
                    ))
                })?;
                let timestamp = timestamp.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: timestamp",
                    ))
                })?;
                let stream_id = stream_id.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: stream_id",
                    ))
                })?;
                let event_name = event_name.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: event_name",
                    ))
                })?;
                let metadata = metadata.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: metadata",
                    ))
                })?;
                let payload = payload.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: payload",
                    ))
                })?;

                Ok(Event {
                    event_id,
                    partition_key,
                    partition_id,
                    transaction_id,
                    partition_sequence,
                    stream_version,
                    timestamp,
                    stream_id,
                    event_name,
                    metadata,
                    payload,
                })
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Event must be a Redis map",
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RangeValue {
    Start,      // "-"
    Value(u64), // specific number
    End,        // "+"
}

impl From<u64> for RangeValue {
    fn from(value: u64) -> Self {
        RangeValue::Value(value)
    }
}

impl ToRedisArgs for RangeValue {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            RangeValue::Start => out.write_arg(b"-"),
            RangeValue::End => out.write_arg(b"+"),
            RangeValue::Value(v) => out.write_arg(v.to_string().as_bytes()),
        }
    }
}

/// Information about a multi-event append transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiAppendInfo {
    pub partition_key: Uuid,
    pub partition_id: u16,
    pub first_partition_sequence: u64,
    pub last_partition_sequence: u64,
    pub events: Vec<EventInfo>,
}

/// Information about a single event within a multi-append transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventInfo {
    pub event_id: Uuid,
    pub partition_id: u16,
    pub partition_sequence: u64,
    pub stream_id: String,
    pub stream_version: u64,
    pub timestamp: SystemTime,
}

impl EventInfo {
    fn from_redis_value_inner(value: &Value, is_multi: bool) -> RedisResult<Self> {
        match value {
            Value::Map(fields) => {
                let mut event_id = None;
                let mut partition_id = None;
                let mut partition_sequence = None;
                let mut stream_id = None;
                let mut stream_version = None;
                let mut timestamp = None;

                // Extract fields from map
                for (key, val) in fields {
                    let field_name = match key {
                        Value::SimpleString(s) => s.as_str(),
                        _ => continue,
                    };

                    match field_name {
                        "event_id" => {
                            event_id = Some(parse_value!(Uuid, val, "event_id")?);
                        }
                        "partition_id" => {
                            partition_id = Some(match val {
                                Value::Int(n) => *n as u16,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "partition_id must be an integer",
                                    )));
                                }
                            });
                        }
                        "partition_sequence" => {
                            partition_sequence = Some(match val {
                                Value::Int(n) => *n as u64,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "partition_sequence must be an integer",
                                    )));
                                }
                            });
                        }
                        "stream_id" => {
                            stream_id = Some(parse_value!(String, val, "stream_id")?);
                        }
                        "stream_version" => {
                            stream_version = Some(match val {
                                Value::Int(n) => *n as u64,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "stream_version must be an integer",
                                    )));
                                }
                            });
                        }
                        "timestamp" => {
                            timestamp = Some(match val {
                                Value::Int(ms) => {
                                    let duration = Duration::from_millis(*ms as u64);
                                    UNIX_EPOCH + duration
                                }
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "timestamp must be an integer",
                                    )));
                                }
                            });
                        }
                        _ => {} // Ignore unknown fields
                    }
                }

                // Ensure all required fields are present
                let event_id = event_id.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: event_id",
                    ))
                })?;
                let partition_id = partition_id.or(is_multi.then_some(0)).ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: partition_id",
                    ))
                })?;
                let partition_sequence =
                    partition_sequence
                        .or(is_multi.then_some(0))
                        .ok_or_else(|| {
                            RedisError::from((
                                redis::ErrorKind::TypeError,
                                "Missing required field: partition_sequence",
                            ))
                        })?;
                let stream_id = stream_id.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: stream_id",
                    ))
                })?;
                let stream_version = stream_version.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: stream_version",
                    ))
                })?;
                let timestamp = timestamp.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: timestamp",
                    ))
                })?;

                Ok(EventInfo {
                    event_id,
                    partition_id,
                    partition_sequence,
                    stream_id,
                    stream_version,
                    timestamp,
                })
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Event info must be a Redis map",
            ))),
        }
    }
}

impl FromRedisValue for EventInfo {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        Self::from_redis_value_inner(value, false)
    }
}

impl FromRedisValue for MultiAppendInfo {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Map(fields) => {
                let mut partition_key = None;
                let mut partition_id = None;
                let mut first_partition_sequence = None;
                let mut last_partition_sequence = None;
                let mut events = None;

                // Extract fields from map
                for (key, val) in fields {
                    let field_name = match key {
                        Value::SimpleString(s) => s.as_str(),
                        _ => continue,
                    };

                    match field_name {
                        "partition_key" => {
                            partition_key = Some(parse_value!(Uuid, val, "partition_key")?);
                        }
                        "partition_id" => {
                            partition_id = Some(match val {
                                Value::Int(n) => *n as u16,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "partition_id must be an integer",
                                    )));
                                }
                            });
                        }
                        "first_partition_sequence" => {
                            first_partition_sequence = Some(match val {
                                Value::Int(n) => *n as u64,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "first_partition_sequence must be an integer",
                                    )));
                                }
                            });
                        }
                        "last_partition_sequence" => {
                            last_partition_sequence = Some(match val {
                                Value::Int(n) => *n as u64,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "last_partition_sequence must be an integer",
                                    )));
                                }
                            });
                        }
                        "events" => {
                            events = Some(match val {
                                Value::Array(event_values) => event_values
                                    .iter()
                                    .map(|value| EventInfo::from_redis_value_inner(value, true))
                                    .collect::<Result<Vec<_>, _>>()?,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "events must be an array",
                                    )));
                                }
                            });
                        }
                        _ => {} // Ignore unknown fields
                    }
                }

                // Ensure all required fields are present
                let partition_key = partition_key.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: partition_key",
                    ))
                })?;
                let partition_id = partition_id.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: partition_id",
                    ))
                })?;
                let first_partition_sequence = first_partition_sequence.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: first_partition_sequence",
                    ))
                })?;
                let last_partition_sequence = last_partition_sequence.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: last_partition_sequence",
                    ))
                })?;
                let events = events
                    .ok_or_else(|| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Missing required field: events",
                        ))
                    })?
                    .into_iter()
                    .enumerate()
                    .map(|(i, mut event)| {
                        event.partition_id = partition_id;
                        event.partition_sequence = first_partition_sequence + i as u64;
                        event
                    })
                    .collect();

                Ok(MultiAppendInfo {
                    partition_key,
                    partition_id,
                    first_partition_sequence,
                    last_partition_sequence,
                    events,
                })
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Multi append info must be a Redis map",
            ))),
        }
    }
}

/// Information about an event subscription.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionInfo {
    /// The type of subscription ("subscribe")
    pub subscription_type: String,
    /// The channel/topic name being subscribed to
    pub channel: String,
    /// Current number of active subscriptions on this connection
    pub active_subscriptions: i64,
}

impl FromRedisValue for SubscriptionInfo {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Map(fields) => {
                let mut subscription_type = None;
                let mut channel = None;
                let mut active_subscriptions = None;

                // Extract fields from map
                for (key, val) in fields {
                    let field_name = match key {
                        Value::SimpleString(s) => s.as_str(),
                        _ => continue,
                    };

                    match field_name {
                        "subscription_type" => {
                            subscription_type =
                                Some(parse_value!(String, val, "subscription_type")?);
                        }
                        "channel" => {
                            channel = Some(parse_value!(String, val, "channel")?);
                        }
                        "active_subscriptions" => {
                            active_subscriptions = Some(match val {
                                Value::Int(n) => *n,
                                _ => {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "active_subscriptions must be an integer",
                                    )));
                                }
                            });
                        }
                        _ => {} // Ignore unknown fields
                    }
                }

                // Ensure all required fields are present
                let subscription_type = subscription_type.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: subscription_type",
                    ))
                })?;
                let channel = channel.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: channel",
                    ))
                })?;
                let active_subscriptions = active_subscriptions.ok_or_else(|| {
                    RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Missing required field: active_subscriptions",
                    ))
                })?;

                Ok(SubscriptionInfo {
                    subscription_type,
                    channel,
                    active_subscriptions,
                })
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Subscription info must be a Redis map",
            ))),
        }
    }
}
