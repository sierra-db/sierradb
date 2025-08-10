pub mod eappend;
pub mod eget;
pub mod emappend;
pub mod epscan;
pub mod epseq;
pub mod epsub;
pub mod escan;
pub mod esub;
pub mod esver;
pub mod hello;
pub mod ping;

use std::collections::HashMap;
use std::num::{ParseIntError, TryFromIntError};

use bytes::Bytes;
use redis_protocol::resp3::types::{BytesFrame, VerbatimStringFormat};
use sierradb::StreamId;
use sierradb::bucket::PartitionId;
use sierradb::bucket::segment::EventRecord;
use sierradb::database::ExpectedVersion;
use sierradb::id::uuid_to_partition_hash;
use sierradb_protocol::ErrorCode;
use tokio::io;
use tracing::warn;
use uuid::Uuid;

use crate::request::eappend::EAppend;
use crate::request::eget::EGet;
use crate::request::emappend::EMAppend;
use crate::request::epscan::EPScan;
use crate::request::epseq::EPSeq;
use crate::request::epsub::EPSub;
use crate::request::escan::EScan;
use crate::request::esub::ESub;
use crate::request::esver::ESVer;
use crate::request::hello::Hello;
use crate::request::ping::Ping;
use crate::server::Conn;

pub enum Command {
    EAppend,
    EGet,
    EMAppend,
    EPScan,
    EPSeq,
    EPSub,
    ESVer,
    EScan,
    ESub,
    Hello,
    Ping,
}

impl Command {
    pub async fn handle(
        &self,
        args: &[BytesFrame],
        conn: &mut Conn,
    ) -> Result<Option<BytesFrame>, io::Error> {
        macro_rules! handle_commands {
            ( $( $name:ident ),* $(,)? ) => {
                match self {
                    $( Command::$name => {
                        match $name::from_args(args) {
                            Ok(cmd) => cmd.handle_request_failable(conn).await,
                            Err(err) => {
                                Ok(Some(BytesFrame::SimpleError {
                                    data: err.into(),
                                    attributes: None,
                                }))
                            }
                        }
                    } )*
                }
            };
        }

        handle_commands![
            EAppend, EGet, EMAppend, EPScan, EPSeq, EPSub, EScan, ESVer, ESub, Hello, Ping
        ]
    }
}

impl TryFrom<&BytesFrame> for Command {
    type Error = String;

    fn try_from(frame: &BytesFrame) -> Result<Self, Self::Error> {
        match frame {
            BytesFrame::BlobString { data, .. }
            | BytesFrame::SimpleString { data, .. }
            | BytesFrame::BigNumber { data, .. }
            | BytesFrame::VerbatimString {
                data,
                format: VerbatimStringFormat::Text,
                ..
            } => {
                match str::from_utf8(data)
                    .map_err(|_| "invalid command".to_string())?
                    .to_ascii_uppercase()
                    .as_str()
                {
                    "EAPPEND" => Ok(Command::EAppend),
                    "EGET" => Ok(Command::EGet),
                    "EMAPPEND" => Ok(Command::EMAppend),
                    "EPSCAN" => Ok(Command::EPScan),
                    "EPSEQ" => Ok(Command::EPSeq),
                    "EPSUB" => Ok(Command::EPSub),
                    "ESVER" => Ok(Command::ESVer),
                    "ESCAN" => Ok(Command::EScan),
                    "ESUB" => Ok(Command::ESub),
                    "HELLO" => Ok(Command::Hello),
                    "PING" => Ok(Command::Ping),
                    cmd => {
                        warn!("received unknown command {cmd}");
                        Err(ErrorCode::InvalidArg.with_message(format!("unknown command '{cmd}'")))
                    }
                }
            }
            _ => Err(ErrorCode::InvalidArg.with_message("invalid type for command name")),
        }
    }
}

pub trait HandleRequest: Sized + Send {
    type Ok: Into<BytesFrame>;
    type Error: ToString;

    fn handle_request(
        self,
        conn: &mut Conn,
    ) -> impl Future<Output = Result<Option<Self::Ok>, Self::Error>> + Send;

    fn handle_request_failable(
        self,
        conn: &mut Conn,
    ) -> impl Future<Output = Result<Option<BytesFrame>, io::Error>> + Send {
        async move {
            match self.handle_request(conn).await {
                Ok(Some(resp)) => Ok(Some(resp.into())),
                Ok(None) => Ok(None),
                Err(err) => Ok(Some(BytesFrame::SimpleError {
                    data: err.to_string().into(),
                    attributes: None,
                })),
            }
        }
    }
}

pub trait FromArgs: Sized {
    fn from_args(args: &[BytesFrame]) -> Result<Self, String>;
}

pub trait FromBytesFrame<'a>: Sized {
    fn from_bytes_frame(frame: &'a BytesFrame) -> Result<Self, String>;
}

impl<'a, T> FromBytesFrame<'a> for Option<T>
where
    T: FromBytesFrame<'a>,
{
    fn from_bytes_frame(frame: &'a BytesFrame) -> Result<Self, String> {
        match frame {
            BytesFrame::Null => Ok(None),
            _ => Ok(Some(T::from_bytes_frame(frame)?)),
        }
    }
}

impl FromBytesFrame<'_> for i64 {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        match frame {
            BytesFrame::BlobString { data, .. }
            | BytesFrame::SimpleString { data, .. }
            | BytesFrame::BigNumber { data, .. }
            | BytesFrame::VerbatimString {
                data,
                format: VerbatimStringFormat::Text,
                ..
            } => str::from_utf8(data)
                .map_err(|err| err.to_string())?
                .parse()
                .map_err(|err: ParseIntError| err.to_string()),
            BytesFrame::Number { data, .. } => Ok(*data),
            _ => Err("unsupported type, expecting i64".to_string()),
        }
    }
}

impl FromBytesFrame<'_> for u64 {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        match frame {
            BytesFrame::BlobString { data, .. }
            | BytesFrame::SimpleString { data, .. }
            | BytesFrame::BigNumber { data, .. }
            | BytesFrame::VerbatimString {
                data,
                format: VerbatimStringFormat::Text,
                ..
            } => str::from_utf8(data)
                .map_err(|err| err.to_string())?
                .parse()
                .map_err(|err: ParseIntError| err.to_string()),
            BytesFrame::Number { data, .. } => (*data)
                .try_into()
                .map_err(|err: TryFromIntError| err.to_string()),
            _ => Err("unsupported type, expecting i64".to_string()),
        }
    }
}

impl FromBytesFrame<'_> for u16 {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        match frame {
            BytesFrame::BlobString { data, .. }
            | BytesFrame::SimpleString { data, .. }
            | BytesFrame::BigNumber { data, .. }
            | BytesFrame::VerbatimString {
                data,
                format: VerbatimStringFormat::Text,
                ..
            } => str::from_utf8(data)
                .map_err(|err| err.to_string())?
                .parse()
                .map_err(|err: ParseIntError| err.to_string()),
            BytesFrame::Number { data, .. } => (*data)
                .try_into()
                .map_err(|err: TryFromIntError| err.to_string()),
            _ => Err("unsupported type, expecting i64".to_string()),
        }
    }
}

impl<'a> FromBytesFrame<'a> for &'a str {
    fn from_bytes_frame(frame: &'a BytesFrame) -> Result<Self, String> {
        match frame {
            BytesFrame::BlobString { data, .. }
            | BytesFrame::SimpleString { data, .. }
            | BytesFrame::BigNumber { data, .. }
            | BytesFrame::VerbatimString {
                data,
                format: VerbatimStringFormat::Text,
                ..
            } => str::from_utf8(data).map_err(|err| err.to_string()),
            _ => Err("unsupported type, expecting string".to_string()),
        }
    }
}

impl FromBytesFrame<'_> for String {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        <&str>::from_bytes_frame(frame).map(ToOwned::to_owned)
    }
}

impl<'a> FromBytesFrame<'a> for &'a [u8] {
    fn from_bytes_frame(frame: &'a BytesFrame) -> Result<Self, String> {
        match frame {
            BytesFrame::BlobString { data, .. }
            | BytesFrame::SimpleString { data, .. }
            | BytesFrame::BigNumber { data, .. }
            | BytesFrame::VerbatimString {
                data,
                format: VerbatimStringFormat::Text,
                ..
            } => Ok(data),
            _ => Err("unsupported type, expecting bytes".to_string()),
        }
    }
}

impl FromBytesFrame<'_> for Vec<u8> {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        <&[u8]>::from_bytes_frame(frame).map(ToOwned::to_owned)
    }
}

impl FromBytesFrame<'_> for StreamId {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        StreamId::new(<String>::from_bytes_frame(frame)?).map_err(|err| err.to_string())
    }
}

impl FromBytesFrame<'_> for Uuid {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        <&str>::from_bytes_frame(frame)?
            .parse()
            .map_err(|err: uuid::Error| err.to_string())
    }
}

impl FromBytesFrame<'_> for ExpectedVersion {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        <u64>::from_bytes_frame(frame)
            .map(ExpectedVersion::Exact)
            .or_else(|_| {
                <&str>::from_bytes_frame(frame).and_then(|s| match s {
                    "any" | "ANY" => Ok(ExpectedVersion::Any),
                    "exists" | "EXISTS" => Ok(ExpectedVersion::Exists),
                    "empty" | "EMPTY" => Ok(ExpectedVersion::Empty),
                    _ => Err("unknown expected version value".to_string()),
                })
            })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RangeValue {
    Start,      // "-"
    End,        // "+"
    Value(u64), // specific number
}

impl FromBytesFrame<'_> for RangeValue {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        <&str>::from_bytes_frame(frame)
            .and_then(|s| match s {
                "-" => Ok(RangeValue::Start),
                "+" => Ok(RangeValue::End),
                _ => Err(String::default()),
            })
            .or_else(|_| <u64>::from_bytes_frame(frame).map(RangeValue::Value))
            .map_err(|_| "unknown range value, expected '-', '+', or number".to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
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

impl FromBytesFrame<'_> for PartitionSelector {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        <Uuid>::from_bytes_frame(frame)
            .map(PartitionSelector::ByKey)
            .or_else(|_| <PartitionId>::from_bytes_frame(frame).map(PartitionSelector::ById))
    }
}

#[inline(always)]
pub fn simple_str(s: impl Into<Bytes>) -> BytesFrame {
    BytesFrame::SimpleString {
        data: s.into(),
        attributes: None,
    }
}

#[inline(always)]
pub fn blob_str(s: impl Into<Bytes>) -> BytesFrame {
    BytesFrame::BlobString {
        data: s.into(),
        attributes: None,
    }
}

#[inline(always)]
pub fn number(n: i64) -> BytesFrame {
    BytesFrame::Number {
        data: n,
        attributes: None,
    }
}

#[inline(always)]
pub fn map(items: HashMap<BytesFrame, BytesFrame>) -> BytesFrame {
    BytesFrame::Map {
        data: items,
        attributes: None,
    }
}

#[inline(always)]
pub fn array(items: Vec<BytesFrame>) -> BytesFrame {
    BytesFrame::Array {
        data: items,
        attributes: None,
    }
}

#[inline(always)]
pub fn encode_event(record: EventRecord) -> BytesFrame {
    map(HashMap::from_iter([
        (
            simple_str("event_id"),
            blob_str(record.event_id.to_string()),
        ),
        (
            simple_str("partition_key"),
            simple_str(record.partition_key.to_string()),
        ),
        (
            simple_str("partition_id"),
            number(record.partition_id as i64),
        ),
        (
            simple_str("transaction_id"),
            simple_str(record.transaction_id.to_string()),
        ),
        (
            simple_str("partition_sequence"),
            number(record.partition_sequence as i64),
        ),
        (
            simple_str("stream_version"),
            number(record.stream_version as i64),
        ),
        (
            simple_str("timestamp"),
            number((record.timestamp / 1_000_000) as i64),
        ),
        (
            simple_str("stream_id"),
            blob_str(record.stream_id.to_string()),
        ),
        (simple_str("event_name"), blob_str(record.event_name)),
        (simple_str("metadata"), blob_str(record.metadata)),
        (simple_str("payload"), blob_str(record.payload)),
    ]))
}
