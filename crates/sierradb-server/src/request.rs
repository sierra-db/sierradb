pub mod eack;
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
use combine::{Parser, eof};
use redis_protocol::resp3::types::{BytesFrame, VerbatimStringFormat};
use sierradb::StreamId;
use sierradb::bucket::PartitionId;
use sierradb::bucket::segment::EventRecord;
use sierradb::database::ExpectedVersion;
use sierradb::id::uuid_to_partition_hash;
use sierradb_cluster::subscription::{FromSequences, SubscriptionMatcher};
use sierradb_protocol::ErrorCode;
use tokio::io;
use tracing::warn;
use uuid::Uuid;

use crate::parser::frame_stream;
use crate::request::eack::EAck;
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
    EAck,
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
                        let stream = frame_stream(args);
                        match $name::parser().skip(eof()).parse(stream) {
                            Ok((cmd, _)) => cmd.handle_request_failable(conn).await,
                            Err(err) => {
                                Ok(Some(BytesFrame::SimpleError {
                                    data: err.to_string().into(),
                                    attributes: None,
                                }))
                            }
                        }
                    } )*
                }
            };
        }

        handle_commands![
            EAck, EAppend, EGet, EMAppend, EPScan, EPSeq, EPSub, EScan, ESVer, ESub, Hello, Ping
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
                    "EACK" => Ok(Command::EAck),
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

impl FromArgs for SubscriptionMatcher {
    fn from_args(args: &[BytesFrame]) -> Result<Self, String> {
        let mut i = 0;
        let kind = <&str>::from_bytes_frame(
            args.get(i)
                .ok_or_else(|| ErrorCode::InvalidArg.with_message("missing subscription type"))?,
        )
        .map_err(|err| {
            ErrorCode::InvalidArg.with_message(format!("invalid subscription type: {err}"))
        })?;
        i += 1;
        match kind {
            "ALL_PARTITIONS" | "all_partitions" => {
                let from_sequences_kind =
                    <&str>::from_bytes_frame(args.get(i).ok_or_else(|| {
                        ErrorCode::InvalidArg.with_message("missing start filter")
                    })?)
                    .map_err(|err| {
                        ErrorCode::InvalidArg.with_message(format!("invalid start filter: {err}"))
                    })?;
                i += 1;
                match from_sequences_kind {
                    "LATEST" | "latest" => Ok(SubscriptionMatcher::AllPartitions {
                        from_sequences: FromSequences::Latest,
                    }),
                    "ALL" | "all" => {
                        let from_sequence =
                            u64::from_bytes_frame(args.get(i).ok_or_else(|| {
                                ErrorCode::InvalidArg.with_message("missing start sequence")
                            })?)
                            .map_err(|err| {
                                ErrorCode::InvalidArg
                                    .with_message(format!("invalid start sequence: {err}"))
                            })?;

                        Ok(SubscriptionMatcher::AllPartitions {
                            from_sequences: FromSequences::AllPartitions(from_sequence),
                        })
                    }
                    "PARTITIONS" | "partitions" => {
                        let mut from_sequences = HashMap::new();

                        loop {
                            let Some(arg) = args.get(i) else {
                                break;
                            };

                            i += 1;

                            match PartitionId::from_bytes_frame(arg) {
                                Ok(partition_id) => {
                                    let from_sequence =
                                        u64::from_bytes_frame(args.get(i).ok_or_else(|| {
                                            ErrorCode::InvalidArg
                                                .with_message("missing from sequence")
                                        })?)
                                        .map_err(
                                            |err| {
                                                ErrorCode::InvalidArg.with_message(format!(
                                                    "invalid from sequence: {err}"
                                                ))
                                            },
                                        )?;

                                    i += 1;

                                    from_sequences.insert(partition_id, from_sequence);
                                }
                                Err(err) => {
                                    let Ok(fallback_keyword) = <&str>::from_bytes_frame(arg) else {
                                        return Err(err);
                                    };
                                    if fallback_keyword != "FALLBACK"
                                        && fallback_keyword != "fallback"
                                    {
                                        return Err(err);
                                    }

                                    let fallback =
                                        u64::from_bytes_frame(args.get(i).ok_or_else(|| {
                                            ErrorCode::InvalidArg
                                                .with_message("missing fallback sequence")
                                        })?)
                                        .map_err(
                                            |err| {
                                                ErrorCode::InvalidArg.with_message(format!(
                                                    "invalid fallback sequence: {err}"
                                                ))
                                            },
                                        )?;

                                    return Ok(SubscriptionMatcher::AllPartitions {
                                        from_sequences: FromSequences::Partitions {
                                            from_sequences,
                                            fallback: Some(fallback),
                                        },
                                    });
                                }
                            }
                        }

                        Ok(SubscriptionMatcher::AllPartitions {
                            from_sequences: FromSequences::Partitions {
                                from_sequences,
                                fallback: None,
                            },
                        })
                    }
                    _ => Err(ErrorCode::InvalidArg
                        .with_message(format!("unknown start filter '{from_sequences_kind}'"))),
                }
            }
            "PARTITIONS" | "partitions" => {
                todo!()
            }
            "STREAMS" | "streams" => {
                todo!()
            }
            _ => Err(ErrorCode::InvalidArg.with_message("unknown subscription type '{kind}'")),
        }
    }
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

impl FromBytesFrame<'_> for u32 {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        match frame {
            BytesFrame::BlobString { data, .. }
            | BytesFrame::SimpleString { data, .. }
            | BytesFrame::BigNumber { data, .. }
            | BytesFrame::VerbatimString {
                data,
                format: VerbatimStringFormat::Text,
                ..
            } => {
                let s = std::str::from_utf8(data).map_err(|_| "invalid string".to_string())?;
                s.parse::<u32>().map_err(|_| "invalid u32".to_string())
            }
            BytesFrame::Number { data, .. } => {
                if *data < 0 {
                    Err("negative number for u32".to_string())
                } else if *data > u32::MAX as i64 {
                    Err("number too large for u32".to_string())
                } else {
                    Ok(*data as u32)
                }
            }
            _ => Err("invalid type for u32".to_string()),
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

/// Represents a range of partitions for multi-partition subscriptions
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionRange {
    /// Single partition (backwards compatibility)
    Single(PartitionSelector),
    /// Range of partition IDs (inclusive): "0-127", "50-99"
    Range(u16, u16),
    /// Explicit list of partitions: "0,1,5,42"
    List(Vec<PartitionSelector>),
    /// All partitions: "*"
    All,
}

impl PartitionRange {
    /// Expand the range into a vector of concrete partition IDs
    pub fn expand(&self, num_partitions: u16) -> Vec<PartitionId> {
        match self {
            PartitionRange::Single(selector) => {
                vec![selector.into_partition_id(num_partitions)]
            }
            PartitionRange::Range(start, end) => {
                let start = (*start).min(num_partitions.saturating_sub(1));
                let end = (*end).min(num_partitions.saturating_sub(1));
                if start <= end {
                    (start..=end).collect()
                } else {
                    vec![]
                }
            }
            PartitionRange::List(selectors) => selectors
                .iter()
                .map(|s| s.into_partition_id(num_partitions))
                .collect(),
            PartitionRange::All => (0..num_partitions).collect(),
        }
    }
}

/// Specification for FROM_SEQUENCE parameter in multi-partition subscriptions  
#[derive(Debug, Clone)]
pub enum FromSequenceSpec {
    /// Same sequence for all partitions
    Single(u64),
    /// Individual sequences per partition: {partition_id: sequence}
    PerPartition(HashMap<u16, u64>),
}

impl FromSequenceSpec {
    /// Get the FROM_SEQUENCE value for a specific partition
    pub fn get_sequence_for_partition(&self, partition_id: u16) -> Option<u64> {
        match self {
            FromSequenceSpec::Single(seq) => Some(*seq),
            FromSequenceSpec::PerPartition(map) => map.get(&partition_id).copied(),
        }
    }
}

impl FromBytesFrame<'_> for PartitionRange {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        // Try to parse as a string first for ranges, lists, and "*"
        if let Ok(s) = <&str>::from_bytes_frame(frame) {
            // Check for wildcard "*"
            if s == "*" {
                return Ok(PartitionRange::All);
            }

            // Check for range format "start-end"
            if let Some(dash_pos) = s.find('-') {
                let start_str = &s[..dash_pos];
                let end_str = &s[dash_pos + 1..];

                let start: u16 = start_str
                    .parse()
                    .map_err(|_| format!("invalid start partition ID in range: '{start_str}'"))?;
                let end: u16 = end_str
                    .parse()
                    .map_err(|_| format!("invalid end partition ID in range: '{end_str}'"))?;

                return Ok(PartitionRange::Range(start, end));
            }

            // Check for comma-separated list "0,1,5,42"
            if s.contains(',') {
                let mut selectors = Vec::new();
                for part in s.split(',') {
                    let part = part.trim();
                    // Try UUID first, then partition ID
                    let selector = if let Ok(uuid) = part.parse::<Uuid>() {
                        PartitionSelector::ByKey(uuid)
                    } else if let Ok(id) = part.parse::<u16>() {
                        PartitionSelector::ById(id)
                    } else {
                        return Err(format!("invalid partition selector in list: '{part}'"));
                    };
                    selectors.push(selector);
                }
                return Ok(PartitionRange::List(selectors));
            }
        }

        // Fall back to single partition selector (backwards compatibility)
        PartitionSelector::from_bytes_frame(frame).map(PartitionRange::Single)
    }
}

impl FromBytesFrame<'_> for FromSequenceSpec {
    fn from_bytes_frame(frame: &BytesFrame) -> Result<Self, String> {
        // Try parsing as a single number first
        if let Ok(sequence) = u64::from_bytes_frame(frame) {
            return Ok(FromSequenceSpec::Single(sequence));
        }

        // Try parsing as string (could be comma-separated pairs or single number)
        if let Ok(s) = <&str>::from_bytes_frame(frame) {
            // Try parsing as single number first
            if let Ok(sequence) = s.parse::<u64>() {
                return Ok(FromSequenceSpec::Single(sequence));
            }

            // Parse as "partition:sequence,partition:sequence" format
            if s.contains(':') {
                let mut partition_sequences = HashMap::new();

                for pair in s.split(',') {
                    let parts: Vec<&str> = pair.split(':').collect();
                    if parts.len() != 2 {
                        return Err(format!("invalid partition:sequence pair: '{pair}'"));
                    }

                    let partition_id: u16 = parts[0]
                        .parse()
                        .map_err(|_| format!("invalid partition ID: '{}'", parts[0]))?;
                    let sequence: u64 = parts[1]
                        .parse()
                        .map_err(|_| format!("invalid sequence number: '{}'", parts[1]))?;

                    partition_sequences.insert(partition_id, sequence);
                }

                if partition_sequences.is_empty() {
                    return Err("no valid partition:sequence pairs found".to_string());
                }

                return Ok(FromSequenceSpec::PerPartition(partition_sequences));
            }
        }

        match frame {
            // RESP3 Map format: {"0": 501, "1": 1230}
            BytesFrame::Map { data, .. } => {
                let mut partition_sequences = HashMap::new();

                for (key_frame, value_frame) in data {
                    // Parse partition ID from key
                    let partition_id = match key_frame {
                        BytesFrame::SimpleString { data, .. }
                        | BytesFrame::BlobString { data, .. } => std::str::from_utf8(data)
                            .map_err(|_| "invalid UTF-8 in partition ID key")?
                            .parse::<u16>()
                            .map_err(|_| "invalid partition ID in map key")?,
                        BytesFrame::Number { data, .. } => {
                            if *data < 0 || *data > u16::MAX as i64 {
                                return Err("partition ID out of range".to_string());
                            }
                            *data as u16
                        }
                        _ => return Err("invalid type for partition ID key in map".to_string()),
                    };

                    // Parse sequence from value
                    let sequence = u64::from_bytes_frame(value_frame)
                        .map_err(|_| "invalid sequence value in map")?;

                    partition_sequences.insert(partition_id, sequence);
                }

                Ok(FromSequenceSpec::PerPartition(partition_sequences))
            }

            // RESP3 Array format: [["0", "501"], ["1", "1230"]]
            BytesFrame::Array { data, .. } => {
                let mut partition_sequences = HashMap::new();

                for item in data {
                    if let BytesFrame::Array { data: pair, .. } = item {
                        if pair.len() != 2 {
                            return Err(
                                "expected [partition_id, sequence] pairs in array".to_string()
                            );
                        }

                        // Parse partition ID from first element
                        let partition_id = match &pair[0] {
                            BytesFrame::SimpleString { data, .. }
                            | BytesFrame::BlobString { data, .. } => std::str::from_utf8(data)
                                .map_err(|_| "invalid UTF-8 in partition ID")?
                                .parse::<u16>()
                                .map_err(|_| "invalid partition ID in array pair")?,
                            BytesFrame::Number { data, .. } => {
                                if *data < 0 || *data > u16::MAX as i64 {
                                    return Err(
                                        "partition ID out of range in array pair".to_string()
                                    );
                                }
                                *data as u16
                            }
                            _ => {
                                return Err(
                                    "invalid type for partition ID in array pair".to_string()
                                );
                            }
                        };

                        // Parse sequence from second element
                        let sequence = u64::from_bytes_frame(&pair[1])
                            .map_err(|_| "invalid sequence in array pair")?;

                        partition_sequences.insert(partition_id, sequence);
                    } else {
                        return Err(
                            "expected array pairs in FROM_SEQUENCE array format".to_string()
                        );
                    }
                }

                Ok(FromSequenceSpec::PerPartition(partition_sequences))
            }

            _ => Err("expected number, map, or array for FROM_SEQUENCE".to_string()),
        }
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
