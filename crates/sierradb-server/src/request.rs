use std::io;

use sierradb::StreamId;
use sierradb::bucket::PartitionId;
use sierradb::database::ExpectedVersion;
use sierradb::id::uuid_to_partition_hash;
use uuid::Uuid;

use crate::impl_command;
use crate::value::Value;

// EAPPEND
pub struct EAppend {
    pub stream_id: StreamId,
    pub event_name: String,
    pub event_id: Option<Uuid>,
    pub partition_key: Option<Uuid>,
    pub expected_version: ExpectedVersion,
    pub payload: Vec<u8>,
    pub metadata: Vec<u8>,
}

impl_command!(
    EAppend,
    [stream_id, event_name],
    [event_id, partition_key, expected_version, payload, metadata]
);

pub struct Event {
    pub stream_id: StreamId,
    pub event_name: String,
    pub event_id: Option<Uuid>,
    pub expected_version: ExpectedVersion,
    pub payload: Vec<u8>,
    pub metadata: Vec<u8>,
}

pub struct EMAppend {
    pub partition_key: Uuid,
    pub events: Vec<Event>,
}

impl FromArgs for EMAppend {
    fn from_args(args: &[Value]) -> Result<Self, Value> {
        if args.is_empty() {
            return Err(Value::Error("Missing partition_key argument".to_string()));
        }

        let mut i = 0;

        // Parse required partition_key (first positional argument)
        let partition_key =
            TryFrom::try_from(&args[i]).map_err(|err: io::Error| Value::Error(err.to_string()))?;
        i += 1;

        let mut events = Vec::new();

        while i < args.len() {
            // Parse stream_id (required for each event)
            let stream_id = TryFrom::try_from(&args[i])
                .map_err(|err: io::Error| Value::Error(err.to_string()))?;
            i += 1;

            if i >= args.len() {
                return Err(Value::Error(
                    "Missing event_name after stream_id".to_string(),
                ));
            }

            // Parse event_name (required for each event)
            let event_name = args[i]
                .as_str()
                .map_err(|_| Value::Error("Expected event name".to_string()))?
                .to_string();
            i += 1;

            // Initialize optional fields
            let mut event_id = None;
            let mut expected_version = ExpectedVersion::default();
            let mut payload = Vec::new();
            let mut metadata = Vec::new();

            // Parse optional fields for this event
            while i < args.len() {
                let field_name_str = match args[i].as_str() {
                    Ok(s) => s.to_lowercase(),
                    Err(_) => break, // Not a string, probably next event's stream_id
                };

                // Check if this looks like a field name
                if !["event_id", "expected_version", "payload", "metadata"]
                    .contains(&field_name_str.as_str())
                {
                    // This is likely a new event's stream_id, don't consume it
                    break;
                }

                i += 1; // consume field name
                if i >= args.len() {
                    return Err(Value::Error("Missing value for field".to_string()));
                }

                let value = &args[i];
                i += 1; // consume field value

                match field_name_str.as_str() {
                    "event_id" => {
                        event_id = Some(
                            TryFrom::try_from(value)
                                .map_err(|err: io::Error| Value::Error(err.to_string()))?,
                        );
                    }
                    "expected_version" => {
                        expected_version = TryFrom::try_from(value)
                            .map_err(|err: io::Error| Value::Error(err.to_string()))?;
                    }
                    "payload" => {
                        payload = TryFrom::try_from(value)
                            .map_err(|err: io::Error| Value::Error(err.to_string()))?;
                    }
                    "metadata" => {
                        metadata = TryFrom::try_from(value)
                            .map_err(|err: io::Error| Value::Error(err.to_string()))?;
                    }
                    _ => return Err(Value::Error(format!("Unknown field {field_name_str}"))),
                }
            }

            events.push(Event {
                stream_id,
                event_name,
                event_id,
                expected_version,
                payload,
                metadata,
            });
        }

        if events.is_empty() {
            return Err(Value::Error("At least one event required".to_string()));
        }

        Ok(EMAppend {
            partition_key,
            events,
        })
    }
}

// EGET
pub struct EGet {
    pub event_id: Uuid,
}

impl_command!(EGet, [event_id], []);

// EPSCAN
pub struct EPScan {
    pub partition: PartitionSelector,
    pub start_sequence: u64,
    pub end_sequence: RangeValue,
    pub count: Option<u64>,
}

impl_command!(EPScan, [partition, start_sequence, end_sequence], [count]);

// ESCAN
pub struct EScan {
    pub stream_id: StreamId,
    pub partition_key: Option<Uuid>,
    pub start_version: u64,
    pub end_version: RangeValue,
    pub count: Option<u64>,
}

impl_command!(
    EScan,
    [stream_id, start_version, end_version],
    [partition_key, count]
);

// EPSEQ
pub struct EPSeq {
    pub partition: PartitionSelector,
}

impl_command!(EPSeq, [partition], []);

// ESVER
pub struct ESVer {
    pub stream_id: StreamId,
    pub partition_key: Option<Uuid>,
}

impl_command!(ESVer, [stream_id], [partition_key]);

// ESUB
pub struct ESub {}

impl_command!(ESub, [], []);

pub trait FromArgs: Sized {
    fn from_args(args: &[Value]) -> Result<Self, Value>;
}

impl TryFrom<&Value> for Option<Uuid> {
    type Error = io::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        Ok(Some(value.try_into()?))
    }
}

impl TryFrom<&Value> for StreamId {
    type Error = io::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        StreamId::new(value.as_str()?).map_err(io::Error::other)
    }
}

impl TryFrom<&Value> for Uuid {
    type Error = io::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value.as_str()?.parse().map_err(io::Error::other)
    }
}

impl TryFrom<&Value> for ExpectedVersion {
    type Error = io::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_integer() {
            Ok(v) => Ok(ExpectedVersion::Exact(
                u64::try_from(v).map_err(io::Error::other)?,
            )),
            Err(_) => match value.as_str()?.to_lowercase().as_str() {
                "any" => Ok(ExpectedVersion::Any),
                "exists" => Ok(ExpectedVersion::Exists),
                "empty" => Ok(ExpectedVersion::Empty),
                _ => Err(io::Error::other(
                    "Invalid version format, expected number or: any/exists/empty",
                )),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RangeValue {
    Start,      // "-"
    End,        // "+"
    Value(u64), // specific number
}

impl TryFrom<&Value> for RangeValue {
    type Error = io::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_str() {
            Ok(s) => match s {
                "-" => Ok(RangeValue::Start),
                "+" => Ok(RangeValue::End),
                _ => {
                    // Try to parse as u64
                    s.parse::<u64>().map(RangeValue::Value).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Invalid range value: expected number, '-', or '+', got '{s}'"),
                        )
                    })
                }
            },
            Err(_) => {
                // Try to parse as u64 directly if it's not a string
                let num = u64::try_from(value).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Range value must be a number, '-', or '+'",
                    )
                })?;
                Ok(RangeValue::Value(num))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionSelector {
    ById(PartitionId), // 0-65535
    ByKey(Uuid),       // 550e8400-e29b-41d4-a716-446655440000
}

impl PartitionSelector {
    pub fn into_partition_id(self, num_partitions: u16) -> PartitionId {
        match self {
            PartitionSelector::ById(id) => id,
            PartitionSelector::ByKey(key) => uuid_to_partition_hash(key) % num_partitions,
        }
    }
}

impl TryFrom<&Value> for PartitionSelector {
    type Error = io::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value.as_str() {
            Ok(s) => {
                // Try UUID first (has dashes)
                if let Ok(uuid) = Uuid::parse_str(s) {
                    Ok(PartitionSelector::ByKey(uuid))
                } else if let Ok(id) = s.parse::<PartitionId>() {
                    Ok(PartitionSelector::ById(id))
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Invalid partition selector: expected UUID or partition ID (0-65535)",
                    ))
                }
            }
            Err(_) => {
                // Try as integer
                let id = u16::try_from(value)?;
                Ok(PartitionSelector::ById(id))
            }
        }
    }
}
