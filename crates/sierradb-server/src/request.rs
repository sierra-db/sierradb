use std::io;

use sierradb::StreamId;
use sierradb::database::ExpectedVersion;
use uuid::Uuid;

use crate::value::Value;

pub struct Append {
    pub stream_id: StreamId,
    pub event_name: String,
    pub event_id: Option<Uuid>,
    pub partition_key: Option<Uuid>,
    pub expected_version: ExpectedVersion,
    pub payload: Vec<u8>,
    pub metadata: Vec<u8>,
}

pub struct Get {
    pub event_id: Uuid,
}

pub struct PRead {}

pub struct SRead {
    pub stream_id: StreamId,
    pub partition_key: Option<Uuid>,
}

pub struct PInfo {}

pub struct SInfo {}

pub struct Subscribe {}

pub trait FromArgs: Sized {
    fn from_args(args: &[Value]) -> Result<Self, Value>;
}

macro_rules! impl_command {
    ($cmd:ident, [ $( $req_field:ident ),* ], [ $( $opt_field:ident ),* ]) => {
        impl FromArgs for $cmd {
            fn from_args(args: &[Value]) -> Result<Self, Value> {
                let mut iter = args.into_iter();

                $(
                    let $req_field = TryFrom::try_from(iter.next().ok_or_else(|| Value::Error(
                        concat!(
                            "Missing ",
                            stringify!($req_field),
                            " argument"
                        ).to_string()
                    ))?).map_err(|err: io::Error| Value::Error(err.to_string()))?;
                )*

                $(
                    let mut $opt_field = None;
                )*

                loop {
                    let Some(name) = iter.next() else {
                        break;
                    };
                    let name = name.as_str().map_err(|_| Value::Error("Expected field name".to_string()))?;
                    #[allow(unused_variables)]
                    let value = iter.next().ok_or_else(|| Value::Error("Unexpected value".to_string()))?;

                    match name.to_lowercase().as_str() {
                        $(
                            stringify!($opt_field) => {
                                $opt_field = Some(TryFrom::try_from(value).map_err(|err: io::Error| Value::Error(err.to_string()))?);
                            }
                        )*
                        _ => return Err(Value::Error(format!("Unknown field {name}")))
                    }
                }

                Ok($cmd {
                    $( $req_field, )*
                    $( $opt_field: $opt_field.unwrap_or_default(), )*
                })
            }
        }
    };
}

impl_command!(
    Append,
    [stream_id, event_name],
    [event_id, partition_key, expected_version, payload, metadata]
);

impl_command!(Get, [event_id], []);

impl_command!(SRead, [stream_id], [partition_key]);

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
