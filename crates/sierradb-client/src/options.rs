use std::{
    borrow::Cow,
    time::{SystemTime, UNIX_EPOCH},
};

use redis::{RedisWrite, ToRedisArgs};
use sierradb_protocol::ExpectedVersion;
use uuid::Uuid;

/// Options for the EAPPEND command
#[derive(Clone, Default)]
pub struct EAppendOptions<'a> {
    event_id: Option<Uuid>,
    partition_key: Option<Uuid>,
    expected_version: ExpectedVersion,
    timestamp: Option<u64>,
    payload: Cow<'a, [u8]>,
    metadata: Cow<'a, [u8]>,
}

impl<'a> EAppendOptions<'a> {
    pub fn new() -> Self {
        Self {
            event_id: None,
            partition_key: None,
            expected_version: ExpectedVersion::Any,
            timestamp: None,
            payload: Cow::Borrowed(&[]),
            metadata: Cow::Borrowed(&[]),
        }
    }

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

    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(
            timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .try_into()
                .unwrap(),
        );
        self
    }

    pub fn payload(mut self, payload: impl Into<Cow<'a, [u8]>>) -> Self {
        self.payload = payload.into();
        self
    }

    pub fn metadata(mut self, metadata: impl Into<Cow<'a, [u8]>>) -> Self {
        self.metadata = metadata.into();
        self
    }
}

impl<'a> ToRedisArgs for EAppendOptions<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(event_id) = &self.event_id {
            out.write_arg(b"EVENT_ID");
            out.write_arg(event_id.to_string().as_bytes());
        }
        if let Some(partition_key) = &self.partition_key {
            out.write_arg(b"PARTITION_KEY");
            out.write_arg(partition_key.to_string().as_bytes());
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
        if let Some(timestamp) = self.timestamp {
            out.write_arg(b"TIMESTAMP");
            out.write_arg(timestamp.to_string().as_bytes());
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

/// Event configuration for the EMAPPEND command
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EMAppendEvent<'a> {
    stream_id: Cow<'a, str>,
    event_name: Cow<'a, str>,
    event_id: Option<Uuid>,
    expected_version: ExpectedVersion,
    timestamp: Option<u64>,
    payload: Cow<'a, [u8]>,
    metadata: Cow<'a, [u8]>,
}

impl<'a> EMAppendEvent<'a> {
    pub fn new(
        stream_id: impl Into<Cow<'a, str>>,
        event_name: impl Into<Cow<'a, str>>,
    ) -> EMAppendEvent<'a> {
        EMAppendEvent {
            stream_id: stream_id.into(),
            event_name: event_name.into(),
            event_id: None,
            expected_version: ExpectedVersion::Any,
            timestamp: None,
            payload: Cow::Borrowed(&[]),
            metadata: Cow::Borrowed(&[]),
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

    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(
            timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                .try_into()
                .unwrap(),
        );
        self
    }

    pub fn payload(mut self, payload: impl Into<Cow<'a, [u8]>>) -> Self {
        self.payload = payload.into();
        self
    }

    pub fn metadata(mut self, metadata: impl Into<Cow<'a, [u8]>>) -> Self {
        self.metadata = metadata.into();
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
            out.write_arg(event_id.to_string().as_bytes());
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
        if let Some(timestamp) = self.timestamp {
            out.write_arg(b"TIMESTAMP");
            out.write_arg(timestamp.to_string().as_bytes());
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
