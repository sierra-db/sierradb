use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::{FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
            Value::Array(values) => {
                // Ensure we have the expected number of fields
                if values.len() != 6 {
                    return Err(RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Append info array must have exactly 6 elements",
                    )));
                }

                // Extract and parse each field
                let event_id = match &values[0] {
                    Value::SimpleString(s) => Uuid::parse_str(s).map_err(|_| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Invalid UUID format for event_id",
                        ))
                    })?,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "event_id must be a string",
                        )));
                    }
                };

                let partition_key = match &values[1] {
                    Value::SimpleString(s) => Uuid::parse_str(s).map_err(|_| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Invalid UUID format for partition_key",
                        ))
                    })?,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "partition_key must be a string",
                        )));
                    }
                };

                let partition_id = match &values[2] {
                    Value::Int(n) => *n as u16,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "partition_id must be an integer",
                        )));
                    }
                };

                let partition_sequence = match &values[3] {
                    Value::Int(n) => *n as u64,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "partition_sequence must be an integer",
                        )));
                    }
                };

                let stream_version = match &values[4] {
                    Value::Int(n) => *n as u64,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "stream_version must be an integer",
                        )));
                    }
                };

                let timestamp = match &values[5] {
                    Value::Int(nanos) => {
                        let duration = Duration::from_nanos(*nanos as u64);
                        UNIX_EPOCH + duration
                    }
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "timestamp must be an integer",
                        )));
                    }
                };

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
                "Append info must be a Redis array",
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
            Value::Array(values) => {
                // Ensure we have the expected number of fields
                if values.len() != 2 {
                    return Err(RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Event batch array must have exactly 2 elements",
                    )));
                }

                // Extract and parse each field
                let has_more = match &values[0] {
                    Value::Boolean(b) => *b,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "has_more must be a boolean",
                        )));
                    }
                };

                let events = match &values[1] {
                    Value::Array(events) => events
                        .iter()
                        .map(Event::from_redis_value)
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "events must be an array",
                        )));
                    }
                };

                Ok(EventBatch { events, has_more })
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Append info must be a Redis array",
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

    /// Unix timestamp (in nanoseconds) when the event was created.
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

impl FromRedisValue for Event {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Array(values) => {
                // Ensure we have the expected number of fields
                if values.len() != 11 {
                    return Err(RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Event array must have exactly 11 elements",
                    )));
                }

                // Extract and parse each field
                let event_id = match &values[0] {
                    Value::SimpleString(s) => Uuid::parse_str(s).map_err(|_| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Invalid UUID format for event_id",
                        ))
                    })?,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "event_id must be a string",
                        )));
                    }
                };

                let partition_key = match &values[1] {
                    Value::SimpleString(s) => Uuid::parse_str(s).map_err(|_| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Invalid UUID format for partition_key",
                        ))
                    })?,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "partition_key must be a string",
                        )));
                    }
                };

                let partition_id = match &values[2] {
                    Value::Int(n) => *n as u16,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "partition_id must be an integer",
                        )));
                    }
                };

                let transaction_id = match &values[3] {
                    Value::SimpleString(s) => Uuid::parse_str(s).map_err(|_| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Invalid UUID format for transaction_id",
                        ))
                    })?,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "transaction_id must be a string",
                        )));
                    }
                };

                let partition_sequence = match &values[4] {
                    Value::Int(n) => *n as u64,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "partition_sequence must be an integer",
                        )));
                    }
                };

                let stream_version = match &values[5] {
                    Value::Int(n) => *n as u64,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "stream_version must be an integer",
                        )));
                    }
                };

                let timestamp = match &values[6] {
                    Value::Int(nanos) => {
                        let duration = Duration::from_nanos(*nanos as u64);
                        UNIX_EPOCH + duration
                    }
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "timestamp must be an integer",
                        )));
                    }
                };

                let stream_id = match &values[7] {
                    Value::SimpleString(s) => s.clone(),
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "stream_id must be a string",
                        )));
                    }
                };

                let event_name = match &values[8] {
                    Value::SimpleString(s) => s.clone(),
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "event_name must be a string",
                        )));
                    }
                };

                let metadata = match &values[9] {
                    Value::BulkString(data) => data.clone(),
                    Value::Nil => Vec::new(), // Handle null metadata
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "metadata must be bulk data",
                        )));
                    }
                };

                let payload = match &values[10] {
                    Value::BulkString(data) => data.clone(),
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "payload must be bulk data",
                        )));
                    }
                };

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
                "Event must be a Redis array",
            ))),
        }
    }
}

/// The expected version **before** the event is inserted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedVersion {
    /// Accept any version, whether the stream/partition exists or not.
    #[default]
    Any,
    /// The stream/partition must exist (have at least one event).
    Exists,
    /// The stream/partition must be empty (have no events yet).
    Empty,
    /// The stream/partition must be exactly at this version.
    Exact(u64),
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
    pub timestamp: SystemTime,
    pub events: Vec<EventInfo>,
}

/// Information about a single event within a multi-append transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventInfo {
    pub event_id: Uuid,
    pub stream_id: String,
    pub stream_version: u64,
}

impl FromRedisValue for MultiAppendInfo {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::Array(values) => {
                if values.len() != 6 {
                    return Err(RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Multi append info array must have exactly 6 elements",
                    )));
                }

                let partition_key = match &values[0] {
                    Value::SimpleString(s) => Uuid::parse_str(s).map_err(|_| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Invalid UUID format for partition_key",
                        ))
                    })?,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "partition_key must be a string",
                        )));
                    }
                };

                let partition_id = match &values[1] {
                    Value::Int(n) => *n as u16,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "partition_id must be an integer",
                        )));
                    }
                };

                let first_partition_sequence = match &values[2] {
                    Value::Int(n) => *n as u64,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "first_partition_sequence must be an integer",
                        )));
                    }
                };

                let last_partition_sequence = match &values[3] {
                    Value::Int(n) => *n as u64,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "last_partition_sequence must be an integer",
                        )));
                    }
                };

                let timestamp = match &values[4] {
                    Value::Int(nanos) => {
                        let duration = Duration::from_nanos(*nanos as u64);
                        UNIX_EPOCH + duration
                    }
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "timestamp must be an integer",
                        )));
                    }
                };

                let events = match &values[5] {
                    Value::Array(event_arrays) => event_arrays
                        .iter()
                        .map(|event_array| match event_array {
                            Value::Array(event_values) => {
                                if event_values.len() != 3 {
                                    return Err(RedisError::from((
                                        redis::ErrorKind::TypeError,
                                        "Event info array must have exactly 3 elements",
                                    )));
                                }

                                let event_id = match &event_values[0] {
                                    Value::SimpleString(s) => Uuid::parse_str(s).map_err(|_| {
                                        RedisError::from((
                                            redis::ErrorKind::TypeError,
                                            "Invalid UUID format for event_id",
                                        ))
                                    })?,
                                    _ => {
                                        return Err(RedisError::from((
                                            redis::ErrorKind::TypeError,
                                            "event_id must be a string",
                                        )));
                                    }
                                };

                                let stream_id = match &event_values[1] {
                                    Value::SimpleString(s) => s.clone(),
                                    _ => {
                                        return Err(RedisError::from((
                                            redis::ErrorKind::TypeError,
                                            "stream_id must be a string",
                                        )));
                                    }
                                };

                                let stream_version = match &event_values[2] {
                                    Value::Int(n) => *n as u64,
                                    _ => {
                                        return Err(RedisError::from((
                                            redis::ErrorKind::TypeError,
                                            "stream_version must be an integer",
                                        )));
                                    }
                                };

                                Ok(EventInfo {
                                    event_id,
                                    stream_id,
                                    stream_version,
                                })
                            }
                            _ => Err(RedisError::from((
                                redis::ErrorKind::TypeError,
                                "Event info must be an array",
                            ))),
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "events must be an array",
                        )));
                    }
                };

                Ok(MultiAppendInfo {
                    partition_key,
                    partition_id,
                    first_partition_sequence,
                    last_partition_sequence,
                    timestamp,
                    events,
                })
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Multi append info must be a Redis array",
            ))),
        }
    }
}

/// Information about an event subscription.
/// For now, this is just a placeholder since ESUB is not yet implemented.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionInfo {
    pub subscription_id: String,
}

impl FromRedisValue for SubscriptionInfo {
    fn from_redis_value(value: &Value) -> RedisResult<Self> {
        match value {
            Value::SimpleString(s) => Ok(SubscriptionInfo {
                subscription_id: s.clone(),
            }),
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Subscription info must be a string",
            ))),
        }
    }
}
