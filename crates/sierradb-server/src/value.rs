use std::convert::{From, TryFrom};
use std::{io, str};

use bytes::{Buf, BufMut, BytesMut};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Value {
    String(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<Value>),
    StaticError(&'static str),
    StaticString(&'static str),
}

impl Value {
    pub(crate) fn encode(&self, buf: &mut BytesMut) {
        ValueEncoder::encode(buf, self);
    }

    pub fn as_integer(&self) -> io::Result<i64> {
        match self {
            Value::Integer(i) => Ok(*i),
            Value::String(s) => s.parse().map_err(to_error),
            Value::Bulk(v) => str::from_utf8(v)
                .map_err(to_error)
                .and_then(|s| s.parse().map_err(to_error)),
            _ => inconvertible(self, "Integer"),
        }
    }

    pub fn to_integer(self) -> io::Result<i64> {
        self.as_integer()
    }

    pub fn as_float(&self) -> io::Result<f64> {
        match self {
            Value::Integer(i) => Ok((*i) as f64),
            Value::String(s) => s.parse().map_err(to_error),
            Value::Bulk(v) => str::from_utf8(v)
                .map_err(to_error)
                .and_then(|s| s.parse().map_err(to_error)),
            _ => inconvertible(self, "Float"),
        }
    }

    pub fn to_float(self) -> io::Result<f64> {
        self.as_float()
    }

    pub fn as_str(&self) -> io::Result<&str> {
        match self {
            Value::Bulk(v) => str::from_utf8(v.as_slice()).map_err(to_error),
            Value::String(s) => Ok(s.as_str()),
            _ => inconvertible(self, "&str"),
        }
    }

    pub fn to_string(self) -> io::Result<String> {
        match self {
            Value::Bulk(b) => Ok(String::from_utf8(b).map_err(to_error)?),
            Value::Integer(i) => Ok(i.to_string()),
            Value::String(s) => Ok(s),
            Value::Null => Ok(String::new()),
            _ => inconvertible(&self, "String"),
        }
    }

    pub fn as_slice(&self) -> io::Result<&[u8]> {
        match self {
            Value::Bulk(v) => Ok(v.as_slice()),
            Value::String(s) => Ok(s.as_bytes()),
            _ => inconvertible(self, "&[u8]"),
        }
    }

    pub fn to_bytes(self) -> io::Result<Vec<u8>> {
        match self {
            Value::Bulk(b) => Ok(b),
            Value::String(s) => Ok(s.into_bytes()),
            Value::Integer(i) => Ok(i.to_string().into_bytes()),
            Value::Null => Ok(Vec::new()),
            _ => inconvertible(&self, "Vec<u8>"),
        }
    }
}

pub fn inconvertible<A>(from: &Value, target: &str) -> io::Result<A> {
    invalid_data(format!("'{from:?}' is not convertible to '{target}'"))
}

impl TryFrom<&Value> for String {
    type Error = io::Error;

    fn try_from(val: &Value) -> io::Result<Self> {
        val.as_str().map(ToOwned::to_owned)
    }
}

impl TryFrom<Value> for String {
    type Error = io::Error;

    fn try_from(val: Value) -> io::Result<Self> {
        val.to_string()
    }
}

impl TryFrom<&Value> for Vec<u8> {
    type Error = io::Error;

    fn try_from(val: &Value) -> io::Result<Self> {
        val.as_slice().map(ToOwned::to_owned)
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = io::Error;

    fn try_from(val: Value) -> io::Result<Self> {
        val.to_bytes()
    }
}

impl TryFrom<&Value> for Vec<String> {
    type Error = io::Error;

    fn try_from(val: &Value) -> io::Result<Self> {
        if let Value::Array(array) = val {
            array.iter().map(TryInto::try_into).collect()
        } else {
            inconvertible(val, "Vec<String>")
        }
    }
}

impl TryFrom<Value> for Vec<String> {
    type Error = io::Error;

    fn try_from(val: Value) -> io::Result<Self> {
        if let Value::Array(array) = val {
            array.into_iter().map(Value::to_string).collect()
        } else {
            inconvertible(&val, "Vec<String>")
        }
    }
}

impl From<Value> for Vec<Value> {
    fn from(val: Value) -> Self {
        if let Value::Array(array) = val {
            array
        } else {
            Vec::from(val)
        }
    }
}

impl From<&Value> for Vec<Value> {
    fn from(val: &Value) -> Self {
        if let Value::Array(array) = val {
            array.iter().map(Clone::clone).collect()
        } else {
            Vec::from(val)
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = io::Error;

    fn try_from(val: Value) -> io::Result<Self> {
        val.as_integer()
    }
}

impl TryFrom<&Value> for i64 {
    type Error = io::Error;

    fn try_from(val: &Value) -> io::Result<Self> {
        val.as_integer()
    }
}

impl TryFrom<Value> for f64 {
    type Error = io::Error;

    fn try_from(val: Value) -> io::Result<Self> {
        val.as_float()
    }
}

impl TryFrom<&Value> for f64 {
    type Error = io::Error;

    fn try_from(val: &Value) -> io::Result<Self> {
        val.as_float()
    }
}

impl TryFrom<Value> for u64 {
    type Error = io::Error;

    fn try_from(val: Value) -> io::Result<Self> {
        Ok(val.as_integer()? as u64)
    }
}

impl TryFrom<&Value> for u64 {
    type Error = io::Error;

    fn try_from(val: &Value) -> io::Result<Self> {
        Ok(val.as_integer()? as u64)
    }
}

impl TryFrom<&Value> for Option<u64> {
    type Error = io::Error;

    fn try_from(val: &Value) -> io::Result<Self> {
        match val.as_integer() {
            Ok(n) => Ok(Some(n as u64)),
            Err(err) => match val {
                Value::Null => Ok(None),
                _ => Err(err),
            },
        }
    }
}

impl TryFrom<Value> for u16 {
    type Error = io::Error;

    fn try_from(val: Value) -> io::Result<Self> {
        Ok(val.as_integer()? as u16)
    }
}

impl TryFrom<&Value> for u16 {
    type Error = io::Error;

    fn try_from(val: &Value) -> io::Result<Self> {
        Ok(val.as_integer()? as u16)
    }
}

impl TryFrom<Value> for Option<Vec<u8>> {
    type Error = io::Error;

    fn try_from(val: Value) -> io::Result<Self> {
        if let Value::Null = val {
            Ok(None)
        } else {
            val.to_bytes().map(Some)
        }
    }
}

impl TryFrom<&Value> for Option<Vec<u8>> {
    type Error = io::Error;

    fn try_from(val: &Value) -> io::Result<Self> {
        if let Value::Null = val {
            Ok(None)
        } else {
            val.as_slice().map(ToOwned::to_owned).map(Some)
        }
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Integer(i64::from(i))
    }
}

impl From<usize> for Value {
    fn from(i: usize) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<u32> for Value {
    fn from(i: u32) -> Self {
        Value::Integer(i64::from(i))
    }
}

impl From<u64> for Value {
    fn from(i: u64) -> Self {
        Value::Integer(i as i64)
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bulk(b)
    }
}

impl From<Option<Vec<u8>>> for Value {
    fn from(b: Option<Vec<u8>>) -> Self {
        match b {
            Some(b) => Value::Bulk(b),
            None => Value::Null,
        }
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value>,
{
    fn from(a: Vec<T>) -> Self {
        Value::Array(a.into_iter().map(Into::into).collect())
    }
}

pub struct ValueEncoder;

impl ValueEncoder {
    #[inline]
    fn ensure_capacity(buf: &mut BytesMut, size: usize) {
        if buf.remaining_mut() < size {
            buf.reserve(size)
        }
    }

    #[inline]
    fn write_crlf(buf: &mut BytesMut) {
        buf.put_slice(b"\r\n");
    }

    fn write_header_format(buf: &mut [u8], ty: u8, number: i64) -> &[u8] {
        let s = format!("{}{}\r\n", char::from(ty), number);
        let s = s.as_bytes();
        let buf = &mut buf[0..s.len()];
        buf.copy_from_slice(s);
        buf
    }

    fn write_header_fast(buf: &mut [u8], ty: u8, number: i64) -> &[u8] {
        let len = buf.len();
        buf[len - 1] = b'\n';
        buf[len - 2] = b'\r';
        let mut i = len - 3;
        let mut number = number;
        loop {
            buf[i] = b'0' + (number % 10) as u8;
            i -= 1;
            number /= 10;
            if number == 0 {
                break;
            }
        }
        buf[i] = ty;
        &buf[i..]
    }

    fn write_header(buf: &mut BytesMut, ty: u8, number: i64) {
        let mut hdr = [0u8; 32];
        let hdr = if number < 0 {
            Self::write_header_format(&mut hdr, ty, number)
        } else {
            Self::write_header_fast(&mut hdr, ty, number)
        };
        Self::ensure_capacity(buf, hdr.len());
        buf.put(hdr)
    }

    fn write_bulk(buf: &mut BytesMut, ty: u8, bytes: &[u8]) {
        Self::ensure_capacity(buf, bytes.len() + 32);
        Self::write_header(buf, ty, bytes.len() as i64);
        buf.put(bytes);
        Self::write_crlf(buf);
    }

    fn write_string(buf: &mut BytesMut, ty: u8, str: &str) {
        Self::ensure_capacity(buf, str.len() + 3);
        buf.put_u8(ty);
        buf.put(str.as_bytes());
        Self::write_crlf(buf);
    }

    pub fn encode(buf: &mut BytesMut, value: &Value) {
        match value {
            Value::Null => Self::write_header(buf, b'$', -1),
            Value::Array(a) => {
                Self::write_header(buf, b'*', a.len() as i64);
                for e in a {
                    Self::encode(buf, e);
                }
            }
            Value::Integer(i) => Self::write_header(buf, b':', *i),
            Value::Bulk(b) => Self::write_bulk(buf, b'$', b),
            Value::String(s) => Self::write_string(buf, b'+', s),
            Value::Error(e) => Self::write_string(buf, b'-', e),
            Value::StaticError(e) => Self::write_string(buf, b'-', e),
            Value::StaticString(e) => Self::write_string(buf, b'+', e),
        }
    }
}

const RESP_TYPE_BULK_STRING: u8 = b'$';
const RESP_TYPE_ARRAY: u8 = b'*';
const RESP_TYPE_INTEGER: u8 = b':';
const RESP_TYPE_SIMPLE_STRING: u8 = b'+';
const RESP_TYPE_ERROR: u8 = b'-';

fn split_input(input: &mut BytesMut, at: usize) -> BytesMut {
    let mut buf = input.split_to(at);
    let len = buf.len();
    buf.truncate(len - 2);
    buf
}

#[derive(Debug, Default)]
struct StringDecoder {
    expect_lf: bool,
    inspect: usize,
}

impl StringDecoder {
    fn decode(&mut self, input: &mut BytesMut) -> io::Result<String> {
        Ok(str::from_utf8(&split_input(input, self.inspect))
            .map_err(to_error)?
            .into())
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> io::Result<Option<String>> {
        let length = input.len();
        loop {
            if length <= self.inspect {
                return Ok(None);
            }
            let inspect = self.inspect;
            self.inspect += 1;
            match (self.expect_lf, input[inspect]) {
                (false, b'\r') => self.expect_lf = true,
                (false, _) => (),
                (true, b'\n') => return self.decode(input).map(Some),
                (true, b) => {
                    return invalid_data(format!("Invalid last tailing line feed: '{b}'"));
                }
            }
        }
    }
}

#[derive(Debug)]
struct BulkDecoder {
    length_decoder: Option<IntegerDecoder>,
    length: i64,
}

impl BulkDecoder {
    fn try_decode_length(&mut self, input: &mut BytesMut) -> io::Result<bool> {
        if let Some(length) = self.length_decoder.as_mut().unwrap().try_decode(input)? {
            self.length_decoder = None;
            self.length = length;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn try_decode_bulk(&mut self, input: &mut BytesMut) -> io::Result<Option<Value>> {
        if self.length < 0 {
            return if self.length == -1 {
                Ok(Some(Value::Null))
            } else {
                invalid_data(format!("Invalid bulk length: '{}'", self.length))
            };
        }
        let length = self.length as usize;
        let len = input.len();
        if len < length + 2 {
            return Ok(None);
        }
        if input[length] != b'\r' || input[length + 1] != b'\n' {
            return invalid_data(format!(
                "Invalid bulk tailing bytes: '[{}, {}]'",
                input[length],
                input[length + 1]
            ));
        }
        let mut bulk = input.split_to(length + 2).to_vec();
        bulk.truncate(length);
        Ok(Some(Value::Bulk(bulk)))
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> io::Result<Option<Value>> {
        if self.length_decoder.is_some() && !self.try_decode_length(input)? {
            return Ok(None);
        }
        self.try_decode_bulk(input)
    }
}

impl Default for BulkDecoder {
    fn default() -> Self {
        BulkDecoder {
            length_decoder: Some(IntegerDecoder::default()),
            length: 0,
        }
    }
}

#[derive(Debug)]
struct ArrayDecoder {
    size_decoder: Option<IntegerDecoder>,
    value_decoder: Box<ValueDecoder>,
    array: Option<Vec<Value>>,
    size: usize,
}

impl ArrayDecoder {
    fn try_decode_size(&mut self, input: &mut BytesMut) -> io::Result<bool> {
        if let Some(size) = self.size_decoder.as_mut().unwrap().try_decode(input)? {
            self.size_decoder = None;
            if size < 0 {
                return invalid_data(format!("Invalid array size '{size}'"));
            }
            self.size = size as usize;
            self.array = Some(Vec::with_capacity(self.size));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn try_decode_element(&mut self, input: &mut BytesMut) -> io::Result<Option<Value>> {
        if self.size == 0 {
            return Ok(Some(Value::Array(Vec::new())));
        }
        while !input.is_empty() {
            if let Some(value) = self.value_decoder.try_decode(input)? {
                self.array.as_mut().unwrap().push(value);
                if self.array.as_ref().unwrap().len() == self.size {
                    return Ok(self.array.take().map(Value::Array));
                }
            } else {
                break;
            }
        }
        Ok(None)
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> io::Result<Option<Value>> {
        if self.size_decoder.is_some() && !self.try_decode_size(input)? {
            return Ok(None);
        }
        self.try_decode_element(input)
    }
}

impl Default for ArrayDecoder {
    fn default() -> Self {
        ArrayDecoder {
            size_decoder: Some(IntegerDecoder::default()),
            array: None,
            value_decoder: Default::default(),
            size: 0,
        }
    }
}

#[derive(Debug, Default)]
struct IntegerDecoder {
    expect_lf: bool,
    inspect: usize,
}

impl IntegerDecoder {
    fn decode(&mut self, input: &mut BytesMut) -> io::Result<i64> {
        str::from_utf8(&split_input(input, self.inspect))
            .map_err(to_error)?
            .parse()
            .map_err(to_error)
    }

    fn try_decode(&mut self, input: &mut BytesMut) -> io::Result<Option<i64>> {
        let length = input.len();
        loop {
            if length <= self.inspect {
                return Ok(None);
            }
            let inspect = self.inspect;
            self.inspect += 1;
            match (self.expect_lf, input[inspect]) {
                (false, b'0'..=b'9') => (),
                (false, b'-') => (),
                (false, b'\r') => self.expect_lf = true,
                (true, b'\n') => return self.decode(input).map(Some),
                (_, b) => {
                    return invalid_data(format!(
                        "Invalid byte '{b}' when decoding integer {input:?}"
                    ));
                }
            }
        }
    }
}

#[derive(Debug)]
enum TypedDecoder {
    String(StringDecoder),
    Error(StringDecoder),
    Integer(IntegerDecoder),
    Bulk(BulkDecoder),
    Array(ArrayDecoder),
}

impl TypedDecoder {
    fn try_decode(&mut self, input: &mut BytesMut) -> io::Result<Option<Value>> {
        match self {
            TypedDecoder::String(decoder) => {
                decoder.try_decode(input).map(|x| x.map(Value::String))
            }
            TypedDecoder::Error(decoder) => decoder.try_decode(input).map(|x| x.map(Value::Error)),
            TypedDecoder::Integer(decoder) => {
                decoder.try_decode(input).map(|x| x.map(Value::Integer))
            }
            TypedDecoder::Bulk(decoder) => decoder.try_decode(input),
            TypedDecoder::Array(decoder) => decoder.try_decode(input),
        }
    }
}

#[derive(Debug, Default)]
pub struct ValueDecoder {
    decoder: Option<TypedDecoder>,
}

impl ValueDecoder {
    pub fn try_decode(&mut self, input: &mut BytesMut) -> io::Result<Option<Value>> {
        if input.is_empty() {
            return Ok(None);
        }
        if self.decoder.is_none() {
            let decoder = match input[0] {
                RESP_TYPE_BULK_STRING => TypedDecoder::Bulk(BulkDecoder::default()),
                RESP_TYPE_ARRAY => TypedDecoder::Array(ArrayDecoder::default()),
                RESP_TYPE_INTEGER => TypedDecoder::Integer(IntegerDecoder::default()),
                RESP_TYPE_SIMPLE_STRING => TypedDecoder::String(StringDecoder::default()),
                RESP_TYPE_ERROR => TypedDecoder::Error(StringDecoder::default()),
                t => return invalid_data(format!("Invalid value type '{t}'")),
            };
            input.advance(1);
            self.decoder = Some(decoder);
        }
        let result = self.decoder.as_mut().unwrap().try_decode(input)?;
        if result.is_some() {
            self.decoder = None;
        }
        Ok(result)
    }
}

fn to_error<E>(err: E) -> io::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    io::Error::new(io::ErrorKind::InvalidData, err)
}

fn invalid_data<A, T>(msg: T) -> io::Result<A>
where
    T: Into<Option<String>>,
{
    Err(if let Some(msg) = msg.into() {
        io::Error::new(io::ErrorKind::InvalidData, msg)
    } else {
        io::ErrorKind::InvalidData.into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_decode_partially(input: &BytesMut) {
        let len = input.len();
        for i in 1..len {
            let mut s = input[0..i].into();
            let v = ValueDecoder::default().try_decode(&mut s);
            assert!(v.is_ok());
            let v = v.unwrap();
            assert!(v.is_none());
        }
    }

    fn test_decode(mut input: BytesMut, expect: Value) {
        test_decode_partially(&input);
        let mut decoder = ValueDecoder::default();
        if let Ok(Some(v)) = decoder.try_decode(&mut input) {
            assert_eq!(v, expect);
        } else {
            panic!("failed to decode");
        }
    }

    fn test_encode(expect: &BytesMut, value: &Value) {
        let mut buf = BytesMut::with_capacity(128);
        ValueEncoder::encode(&mut buf, value);
        assert_eq!(&buf, expect);
    }

    fn test_codec(serialized: BytesMut, value: Value) {
        test_encode(&serialized, &value);
        test_decode(serialized, value);
    }

    #[test]
    fn test_simple_string() {
        test_codec(b"+OK\r\n"[..].into(), Value::String("OK".into()));
    }

    #[test]
    fn test_error() {
        test_codec(
            b"-Error message\r\n"[..].into(),
            Value::Error("Error message".into()),
        );
    }

    #[test]
    fn test_integer() {
        test_codec(b":1000\r\n"[..].into(), Value::Integer(1000));
    }

    #[test]
    fn test_bulk_string() {
        test_codec(
            b"$6\r\nfoobar\r\n"[..].into(),
            Value::Bulk(b"foobar"[..].into()),
        );
        test_codec(b"$0\r\n\r\n"[..].into(), Value::Bulk(b""[..].into()));
        test_codec(b"$-1\r\n"[..].into(), Value::Null);
    }

    #[test]
    fn test_array() {
        test_codec(b"*0\r\n"[..].into(), Value::Array(vec![]));
        test_codec(
            b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..].into(),
            Value::Array(vec![
                Value::Bulk(b"foo"[..].into()),
                Value::Bulk(b"bar"[..].into()),
            ]),
        );
        test_codec(
            b"*3\r\n:1\r\n:2\r\n:3\r\n"[..].into(),
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
        );
        test_codec(
            b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n"[..].into(),
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4),
                Value::Bulk(b"foobar"[..].into()),
            ]),
        );
    }
}
