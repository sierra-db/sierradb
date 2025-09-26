use std::collections::HashSet;
use std::fmt;

use combine::error::{StreamError, Tracked};
use combine::parser::choice::or;
use combine::stream::{ResetStream, StreamErrorFor, StreamOnce};
use combine::{ParseError, Parser, Positioned, attempt, choice, easy, satisfy_map};
use redis_protocol::resp3::types::{BytesFrame, VerbatimStringFormat};
use sierradb::StreamId;
use sierradb::bucket::PartitionId;
use sierradb_protocol::{ErrorCode, ExpectedVersion};
use uuid::Uuid;

use crate::request::{PartitionSelector, RangeValue};

#[derive(Debug, PartialEq)]
pub struct FrameStreamErrors<'a> {
    errors: easy::Errors<&'a BytesFrame, &'a [BytesFrame], usize>,
}

impl<'a> ParseError<&'a BytesFrame, &'a [BytesFrame], usize> for FrameStreamErrors<'a> {
    type StreamError = easy::Error<&'a BytesFrame, &'a [BytesFrame]>;

    fn empty(position: usize) -> Self {
        FrameStreamErrors {
            errors: easy::Errors::empty(position),
        }
    }

    fn set_position(&mut self, position: usize) {
        self.errors.set_position(position);
    }

    fn add(&mut self, err: Self::StreamError) {
        self.errors.add(err);
    }

    fn set_expected<F>(self_: &mut Tracked<Self>, info: Self::StreamError, f: F)
    where
        F: FnOnce(&mut Tracked<Self>),
    {
        let start = self_.error.errors.errors.len();
        f(self_);
        // Replace all expected errors that were added from the previous add_error
        // with this expected error
        let mut i = 0;
        self_.error.errors.errors.retain(|e| {
            if i < start {
                i += 1;
                true
            } else {
                !matches!(*e, easy::Error::Expected(_))
            }
        });
        self_.error.errors.add(info);
    }

    fn is_unexpected_end_of_input(&self) -> bool {
        self.errors.is_unexpected_end_of_input()
    }

    fn into_other<T>(self) -> T
    where
        T: ParseError<&'a BytesFrame, &'a [BytesFrame], usize>,
    {
        self.errors.into_other()
    }
}

fn frame_kind(frame: &BytesFrame) -> &'static str {
    match frame {
        BytesFrame::BlobString { .. } => "string",
        BytesFrame::BlobError { .. } => "error",
        BytesFrame::SimpleString { .. } => "string",
        BytesFrame::SimpleError { .. } => "error",
        BytesFrame::Boolean { .. } => "boolean",
        BytesFrame::Null => "null",
        BytesFrame::Number { .. } => "number",
        BytesFrame::Double { .. } => "double",
        BytesFrame::BigNumber { .. } => "number",
        BytesFrame::VerbatimString { .. } => "string",
        BytesFrame::Array { .. } => "array",
        BytesFrame::Map { .. } => "map",
        BytesFrame::Set { .. } => "set",
        BytesFrame::Push { .. } => "push",
        BytesFrame::Hello { .. } => "hello",
        BytesFrame::ChunkedString(_) => "chunk",
    }
}

fn display_error<'a>(
    f: &mut fmt::Formatter<'_>,
    err: &easy::Error<&'a BytesFrame, &'a [BytesFrame]>,
) -> fmt::Result {
    match err {
        easy::Error::Unexpected(info) => display_info(f, info),
        easy::Error::Expected(info) => display_info(f, info),
        easy::Error::Message(info) => display_info(f, info),
        easy::Error::Other(error) => write!(f, "{error}"),
    }
}

fn display_info<'a>(
    f: &mut fmt::Formatter<'_>,
    info: &easy::Info<&'a BytesFrame, &'a [BytesFrame]>,
) -> fmt::Result {
    match info {
        easy::Info::Token(token) => write!(f, "{}", frame_kind(token)),
        easy::Info::Range(range) => {
            for (i, frame) in range.iter().enumerate() {
                if i < range.len() - 1 {
                    write!(f, "{}, ", frame_kind(frame))?;
                } else {
                    write!(f, "{}", frame_kind(frame))?;
                }
            }
            Ok(())
        }
        easy::Info::Owned(msg) => write!(f, "{msg}"),
        easy::Info::Static(msg) => write!(f, "{msg}"),
    }
}

impl<'a> fmt::Display for FrameStreamErrors<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", ErrorCode::InvalidArg)?;

        // First print the token that we did not expect
        // There should really just be one unexpected message at this point though we
        // print them all to be safe
        let unexpected = self
            .errors
            .errors
            .iter()
            .filter(|e| matches!(**e, easy::Error::Unexpected(_)));
        let mut has_unexpected = false;
        for err in unexpected {
            if !has_unexpected {
                write!(f, "unexpected ")?;
            }
            has_unexpected = true;
            display_error(f, err)?;
        }

        // Then we print out all the things that were expected in a comma separated list
        // 'Expected 'a', 'expression' or 'let'
        let iter = || {
            self.errors.errors.iter().filter_map(|err| match *err {
                easy::Error::Expected(ref err) => Some(err),
                _ => None,
            })
        };
        let expected_count = iter().count();
        for (i, message) in iter().enumerate() {
            if has_unexpected {
                write!(f, ": ")?;
                has_unexpected = false;
            }
            let s = match i {
                0 => "expected",
                _ if i < expected_count - 1 => ",",
                // Last expected message to be written
                _ => " or",
            };
            write!(f, "{s} ")?;
            display_info(f, message)?;
        }
        // If there are any generic messages we print them out last
        let messages = self
            .errors
            .errors
            .iter()
            .filter(|e| matches!(**e, easy::Error::Message(_) | easy::Error::Other(_)));
        for (i, err) in messages.enumerate() {
            if i == 0 && expected_count != 0 {
                write!(f, ": ")?;
            }
            display_error(f, err)?;
        }
        Ok(())
    }
}

// Implement the Stream trait for &[BytesFrame]
#[derive(Clone, Debug, PartialEq)]
pub struct FrameStream<'a> {
    frames: &'a [BytesFrame],
    position: usize,
}

impl<'a> StreamOnce for FrameStream<'a> {
    type Error = FrameStreamErrors<'a>;
    type Position = usize;
    type Range = &'a [BytesFrame];
    type Token = &'a BytesFrame;

    fn uncons(&mut self) -> Result<Self::Token, StreamErrorFor<Self>> {
        match self.frames.split_first() {
            Some((first, rest)) => {
                self.frames = rest;
                self.position += 1;
                Ok(first)
            }
            None => Err(easy::Error::end_of_input()),
        }
    }

    fn is_partial(&self) -> bool {
        false
    }
}

impl<'a> ResetStream for FrameStream<'a> {
    type Checkpoint = (usize, &'a [BytesFrame]);

    fn checkpoint(&self) -> Self::Checkpoint {
        (self.position, self.frames)
    }

    fn reset(&mut self, checkpoint: Self::Checkpoint) -> Result<(), Self::Error> {
        self.position = checkpoint.0;
        self.frames = checkpoint.1;
        Ok(())
    }
}

impl<'a> Positioned for FrameStream<'a> {
    fn position(&self) -> Self::Position {
        self.position
    }
}

// Helper function to create a FrameStream
pub fn frame_stream(frames: &'_ [BytesFrame]) -> FrameStream<'_> {
    FrameStream {
        frames,
        position: 0,
    }
}

// Basic frame parsers using regular functions instead of the parser! macro
pub fn string<'a>() -> impl Parser<FrameStream<'a>, Output = &'a str> + 'a {
    satisfy_map(|frame: &'a BytesFrame| match frame {
        BytesFrame::BlobString { data, .. }
        | BytesFrame::SimpleString { data, .. }
        | BytesFrame::VerbatimString {
            data,
            format: VerbatimStringFormat::Text,
            ..
        } => str::from_utf8(data).ok(),
        _ => None,
    })
    .expected("string")
}

pub fn data<'a>() -> impl Parser<FrameStream<'a>, Output = &'a [u8]> + 'a {
    satisfy_map(|frame: &'a BytesFrame| match frame {
        BytesFrame::BlobString { data, .. }
        | BytesFrame::SimpleString { data, .. }
        | BytesFrame::VerbatimString {
            data,
            format: VerbatimStringFormat::Text,
            ..
        } => Some(&**data),
        _ => None,
    })
    .expected("string or bytes")
}

pub fn data_owned<'a>() -> impl Parser<FrameStream<'a>, Output = Vec<u8>> + 'a {
    data().map(ToOwned::to_owned)
}

pub fn keyword<'a>(kw: &'static str) -> impl Parser<FrameStream<'a>, Output = &'a str> + 'a {
    debug_assert_eq!(kw, kw.to_uppercase(), "keywords should be uppercase");
    satisfy_map(move |frame: &'a BytesFrame| match frame {
        BytesFrame::BlobString { data, .. }
        | BytesFrame::SimpleString { data, .. }
        | BytesFrame::VerbatimString {
            data,
            format: VerbatimStringFormat::Text,
            ..
        } => str::from_utf8(data).ok().and_then(|s| {
            if s.to_uppercase() == kw {
                Some(s)
            } else {
                None
            }
        }),
        _ => None,
    })
    .expected(kw)
}

pub fn number_u64<'a>() -> impl Parser<FrameStream<'a>, Output = u64> + 'a {
    satisfy_map(|frame: &'a BytesFrame| match frame {
        BytesFrame::BlobString { data, .. }
        | BytesFrame::SimpleString { data, .. }
        | BytesFrame::BigNumber { data, .. }
        | BytesFrame::VerbatimString {
            data,
            format: VerbatimStringFormat::Text,
            ..
        } => str::from_utf8(data).ok().and_then(|s| s.parse().ok()),
        BytesFrame::Number { data, .. } => (*data).try_into().ok(),
        _ => None,
    })
    .expected("number")
}

pub fn number_u64_min<'a>(min: u64) -> impl Parser<FrameStream<'a>, Output = u64> + 'a {
    number_u64().and_then(move |n| {
        if n < min {
            Err(easy::Error::message_format(format!(
                "number {n} must not be less than {min}"
            )))
        } else {
            Ok(n)
        }
    })
}

pub fn number_i64<'a>() -> impl Parser<FrameStream<'a>, Output = i64> + 'a {
    satisfy_map(|frame: &'a BytesFrame| match frame {
        BytesFrame::BlobString { data, .. }
        | BytesFrame::SimpleString { data, .. }
        | BytesFrame::BigNumber { data, .. }
        | BytesFrame::VerbatimString {
            data,
            format: VerbatimStringFormat::Text,
            ..
        } => str::from_utf8(data).ok().and_then(|s| s.parse().ok()),
        BytesFrame::Number { data, .. } => Some(*data),
        _ => None,
    })
    .expected("number")
}

pub fn number_u32<'a>() -> impl Parser<FrameStream<'a>, Output = u32> + 'a {
    satisfy_map(|frame: &'a BytesFrame| match frame {
        BytesFrame::BlobString { data, .. }
        | BytesFrame::SimpleString { data, .. }
        | BytesFrame::BigNumber { data, .. }
        | BytesFrame::VerbatimString {
            data,
            format: VerbatimStringFormat::Text,
            ..
        } => str::from_utf8(data).ok().and_then(|s| s.parse().ok()),
        BytesFrame::Number { data, .. } => (*data).try_into().ok(),
        _ => None,
    })
    .expected("number")
}

pub fn partition_id<'a>() -> impl Parser<FrameStream<'a>, Output = PartitionId> + 'a {
    satisfy_map(|frame: &'a BytesFrame| match frame {
        BytesFrame::BlobString { data, .. }
        | BytesFrame::SimpleString { data, .. }
        | BytesFrame::BigNumber { data, .. }
        | BytesFrame::VerbatimString {
            data,
            format: VerbatimStringFormat::Text,
            ..
        } => str::from_utf8(data).ok().and_then(|s| s.parse().ok()),
        BytesFrame::Number { data, .. } => (*data).try_into().ok(),
        _ => None,
    })
    .expected("partition id")
}

fn uuid<'a>(expected: &'static str) -> impl Parser<FrameStream<'a>, Output = Uuid> + 'a {
    string()
        .and_then(move |s| {
            Uuid::parse_str(s.trim())
                .map_err(|_| easy::Error::message_format(format!("invalid {expected}")))
        })
        .expected(expected)
}

pub fn event_id<'a>() -> impl Parser<FrameStream<'a>, Output = Uuid> + 'a {
    uuid("event id")
}

pub fn partition_key<'a>() -> impl Parser<FrameStream<'a>, Output = Uuid> + 'a {
    uuid("partition key")
}

pub fn subscription_id<'a>() -> impl Parser<FrameStream<'a>, Output = Uuid> + 'a {
    uuid("subscription id")
}

pub fn expected_version<'a>() -> impl Parser<FrameStream<'a>, Output = ExpectedVersion> + 'a {
    let exact = number_u64().map(ExpectedVersion::Exact);

    let keyword = choice((
        keyword("ANY").map(|_| ExpectedVersion::Any),
        keyword("EXISTS").map(|_| ExpectedVersion::Exists),
        keyword("EMPTY").map(|_| ExpectedVersion::Empty),
    ));

    exact
        .or(keyword)
        .message("expected version number or 'any', 'exists', 'empty'")
}

pub fn range_value<'a>() -> impl Parser<FrameStream<'a>, Output = RangeValue> + 'a {
    choice!(
        keyword("-").map(|_| RangeValue::Start),
        keyword("+").map(|_| RangeValue::End),
        number_u64().map(RangeValue::Value)
    )
    .expected("range value (-, +, or number)")
}

pub fn partition_selector<'a>() -> impl Parser<FrameStream<'a>, Output = PartitionSelector> + 'a {
    or(
        attempt(partition_key().map(PartitionSelector::ByKey)),
        partition_id().map(PartitionSelector::ById),
    )
    .expected("partition id or key")
}

pub fn all_selector<'a>() -> impl Parser<FrameStream<'a>, Output = char> + 'a {
    keyword("*").map(|_| '*')
}

pub fn partition_ids<'a>() -> impl Parser<FrameStream<'a>, Output = HashSet<PartitionId>> + 'a {
    satisfy_map(|frame: &'a BytesFrame| {
        match frame {
            BytesFrame::BlobString { data, .. }
            | BytesFrame::SimpleString { data, .. }
            | BytesFrame::VerbatimString {
                data,
                format: VerbatimStringFormat::Text,
                ..
            } => {
                let data = str::from_utf8(data).ok()?;

                // Otherwise try comma-separated list
                data.split(',')
                    .map(|part| part.trim().parse::<PartitionId>())
                    .collect::<Result<_, _>>()
                    .ok()
            }
            BytesFrame::BigNumber { data, .. } => {
                // Handle Number frames directly
                let id: PartitionId = str::from_utf8(data).ok()?.parse().ok()?;
                Some(HashSet::from_iter([id]))
            }
            BytesFrame::Number { data, .. } => {
                // Handle Number frames directly
                let id: PartitionId = (*data).try_into().ok()?;
                Some(HashSet::from_iter([id]))
            }
            _ => None,
        }
    })
    .expected("comma-separated partition ids")
}

// <p1>=<s1>
pub fn partition_id_sequence<'a>() -> impl Parser<FrameStream<'a>, Output = (PartitionId, u64)> + 'a
{
    satisfy_map(|frame: &'a BytesFrame| match frame {
        BytesFrame::BlobString { data, .. }
        | BytesFrame::SimpleString { data, .. }
        | BytesFrame::VerbatimString {
            data,
            format: VerbatimStringFormat::Text,
            ..
        } => {
            let (partition_id, sequence) = str::from_utf8(data).ok()?.split_once('=')?;
            let partition_id: PartitionId = partition_id.parse().ok()?;
            let sequence: u64 = sequence.parse().ok()?;
            Some((partition_id, sequence))
        }
        _ => None,
    })
    .expected("partition id sequence value")
}

// <s1>=<v1>
pub fn stream_id_version<'a>() -> impl Parser<FrameStream<'a>, Output = (StreamId, u64)> + 'a {
    string()
        .and_then(|s| {
            let (stream_id, version) = s
                .split_once('=')
                .ok_or_else(|| easy::Error::message_format("missing `=` in stream id version"))?;
            let stream_id = StreamId::new(stream_id).map_err(easy::Error::message_format)?;
            let version: u64 = version
                .parse()
                .map_err(|_| easy::Error::message_format("invalid stream id version number"))?;
            Ok::<_, easy::Error<_, _>>((stream_id, version))
        })
        .expected("stream id version value")
}

pub fn stream_id<'a>() -> impl Parser<FrameStream<'a>, Output = StreamId> + 'a {
    string()
        .and_then(|s| StreamId::new(s).map_err(easy::Error::message_format))
        .expected("stream id")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_parser() {
        let frames = vec![BytesFrame::SimpleString {
            data: b"GET".to_vec().into(),
            attributes: None,
        }];

        let stream = frame_stream(&frames);
        let result = string().parse(stream);
        assert!(result.is_ok());
        let (parsed, _) = result.unwrap();
        assert_eq!(parsed, "GET");
    }

    #[test]
    fn test_keyword_parser() {
        let frames = vec![BytesFrame::SimpleString {
            data: b"PARTITION_KEY".to_vec().into(),
            attributes: None,
        }];

        let stream = frame_stream(&frames);
        let (parsed, _) = keyword("PARTITION_KEY").parse(stream).unwrap();
        assert_eq!(parsed, "PARTITION_KEY");
    }

    #[test]
    fn test_partition_id_parser() {
        let frames = vec![BytesFrame::Number {
            data: 10,
            attributes: None,
        }];

        let stream = frame_stream(&frames);
        let (parsed, _) = partition_id().parse(stream).unwrap();
        assert_eq!(parsed, 10);

        let frames = vec![BytesFrame::SimpleString {
            data: b"10".to_vec().into(),
            attributes: None,
        }];

        let stream = frame_stream(&frames);
        let (parsed, _) = partition_id().parse(stream).unwrap();
        assert_eq!(parsed, 10);
    }

    #[test]
    fn test_partition_ids_parser() {
        let frames = vec![BytesFrame::Number {
            data: 10,
            attributes: None,
        }];

        let stream = frame_stream(&frames);
        let (parsed, _) = partition_ids().parse(stream).unwrap();
        assert_eq!(parsed, HashSet::from_iter([10]));

        let frames = vec![BytesFrame::SimpleString {
            data: b"10".to_vec().into(),
            attributes: None,
        }];

        let stream = frame_stream(&frames);
        let (parsed, _) = partition_ids().parse(stream).unwrap();
        assert_eq!(parsed, HashSet::from_iter([10]));

        let frames = vec![BytesFrame::SimpleString {
            data: b"10,59,24".to_vec().into(),
            attributes: None,
        }];

        let stream = frame_stream(&frames);
        let (parsed, _) = partition_ids().parse(stream).unwrap();
        assert_eq!(parsed, HashSet::from_iter([10, 59, 24]));
    }
}
