#![allow(unused_parens, deprecated)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use redis::{
    Cmd, ConnectionLike, FromRedisValue, Pipeline, RedisError, RedisResult, RedisWrite,
    ToRedisArgs, Value, cmd,
};
use serde::{Deserialize, Serialize};
use uuid::{Uuid, uuid};

const NAMESPACE_PARTITION_KEY: Uuid = uuid!("bd804425-2e2e-499f-b61d-e045d035ca86");

pub fn stream_partition_key(stream_id: &str) -> Uuid {
    Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes())
}

#[macro_use]
mod macros;

implement_commands! {
    'a

    /// Append an event
    fn eappend<S: ToRedisArgs, E: ToRedisArgs>(stream_id: S, event_name: E, options: EAppendOptions) -> (AppendInfo) {
        cmd("EAPPEND").arg(stream_id).arg(event_name).arg(options)
    }

    /// Append multiple events in a transaction
    fn emappend<>(partition_key: Uuid, events: &[EMAppendEvent]) -> (Value) {
        cmd("EMAPPEND").arg(partition_key.to_string()).arg(events)
    }

    /// Get an event by ID
    fn eget<>(event_id: Uuid) -> (Option<Event>) {
        cmd("EGET").arg(event_id.to_string())
    }
}

impl<T> Commands for T where T: ConnectionLike {}

#[cfg(feature = "aio")]
impl<T> AsyncCommands for T where T: redis::aio::ConnectionLike + Send + Sync + Sized {}

impl<T> TypedCommands for T where T: ConnectionLike {}

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

/// Options for the EAPPEND command
#[derive(Clone, Default)]
pub struct EAppendOptions {
    event_id: Option<Uuid>,
    partition_key: Option<Uuid>,
    expected_version: ExpectedVersion,
    payload: Vec<u8>,
    metadata: Vec<u8>,
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

impl EAppendOptions {
    pub fn event_id(mut self, event_id: Uuid) -> Self {
        self.event_id = Some(event_id);
        self
    }

    pub fn partition_key(mut self, partition_key: Uuid) -> Self {
        self.partition_key = Some(partition_key);
        self
    }

    pub fn expected_version(mut self, expected_version: ExpectedVersion) -> Self {
        self.expected_version = expected_version;
        self
    }

    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    pub fn metadata(mut self, metadata: Vec<u8>) -> Self {
        self.metadata = metadata;
        self
    }
}

impl ToRedisArgs for EAppendOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(event_id) = &self.event_id {
            out.write_arg(b"EVENT_ID");
            out.write_arg(event_id.as_bytes());
        }
        if let Some(partition_key) = &self.partition_key {
            out.write_arg(b"PARTITION_KEY");
            out.write_arg(partition_key.as_bytes());
        }
        match self.expected_version {
            ExpectedVersion::Any => {}
            ExpectedVersion::Exists => {
                out.write_arg(b"EXPECTED_VERSION");
                out.write_arg(b"EXISTS");
            }
            ExpectedVersion::Empty => {
                out.write_arg(b"EXPECTED_VERSION");
                out.write_arg(b"EMPTY");
            }
            ExpectedVersion::Exact(version) => {
                out.write_arg(b"EXPECTED_VERSION");
                out.write_arg(version.to_string().as_bytes());
            }
        }
        if !self.payload.is_empty() {
            out.write_arg(b"PAYLOAD");
            out.write_arg(&self.payload);
        }
        if !self.metadata.is_empty() {
            out.write_arg(b"METADATA");
            out.write_arg(&self.metadata);
        }
    }
}
/// Options for the EAPPEND command
#[derive(Clone, Default)]
pub struct EMAppendEvent<'a> {
    stream_id: &'a str,
    event_name: &'a str,
    event_id: Option<Uuid>,
    expected_version: ExpectedVersion,
    payload: &'a [u8],
    metadata: &'a [u8],
}

impl<'a> EMAppendEvent<'a> {
    pub fn new(stream_id: &'a str, event_name: &'a str) -> EMAppendEvent<'a> {
        EMAppendEvent {
            stream_id,
            event_name,
            event_id: None,
            expected_version: ExpectedVersion::Any,
            payload: &[],
            metadata: &[],
        }
    }

    pub fn event_id(mut self, event_id: Uuid) -> Self {
        self.event_id = Some(event_id);
        self
    }

    pub fn expected_version(mut self, expected_version: ExpectedVersion) -> Self {
        self.expected_version = expected_version;
        self
    }

    pub fn payload(mut self, payload: &'a [u8]) -> Self {
        self.payload = payload;
        self
    }

    pub fn metadata(mut self, metadata: &'a [u8]) -> Self {
        self.metadata = metadata;
        self
    }
}

impl<'a> ToRedisArgs for EMAppendEvent<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        out.write_arg(self.stream_id.as_bytes());
        out.write_arg(self.event_name.as_bytes());
        if let Some(event_id) = &self.event_id {
            out.write_arg(b"EVENT_ID");
            out.write_arg(event_id.as_bytes());
        }
        match self.expected_version {
            ExpectedVersion::Any => {}
            ExpectedVersion::Exists => {
                out.write_arg(b"EXPECTED_VERSION");
                out.write_arg(b"EXISTS");
            }
            ExpectedVersion::Empty => {
                out.write_arg(b"EXPECTED_VERSION");
                out.write_arg(b"EMPTY");
            }
            ExpectedVersion::Exact(version) => {
                out.write_arg(b"EXPECTED_VERSION");
                out.write_arg(version.to_string().as_bytes());
            }
        }
        if !self.payload.is_empty() {
            out.write_arg(b"PAYLOAD");
            out.write_arg(&self.payload);
        }
        if !self.metadata.is_empty() {
            out.write_arg(b"METADATA");
            out.write_arg(&self.metadata);
        }
    }
}
