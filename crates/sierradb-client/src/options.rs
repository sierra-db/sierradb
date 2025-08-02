use redis::{RedisWrite, ToRedisArgs};
use uuid::Uuid;

use crate::types::ExpectedVersion;

/// Options for the EAPPEND command
#[derive(Clone, Copy, Default)]
pub struct EAppendOptions<'a> {
    event_id: Option<Uuid>,
    partition_key: Option<Uuid>,
    expected_version: ExpectedVersion,
    payload: &'a [u8],
    metadata: &'a [u8],
}

impl<'a> EAppendOptions<'a> {
    pub fn new() -> Self {
        Self {
            event_id: None,
            partition_key: None,
            expected_version: ExpectedVersion::Any,
            payload: &[],
            metadata: &[],
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

    pub fn payload(mut self, payload: &'a [u8]) -> Self {
        self.payload = payload;
        self
    }

    pub fn metadata(mut self, metadata: &'a [u8]) -> Self {
        self.metadata = metadata;
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
        if !self.payload.is_empty() {
            out.write_arg(b"PAYLOAD");
            out.write_arg(self.payload);
        }
        if !self.metadata.is_empty() {
            out.write_arg(b"METADATA");
            out.write_arg(self.metadata);
        }
    }
}

/// Event configuration for the EMAPPEND command
#[derive(Clone, Copy, Default)]
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
        if !self.payload.is_empty() {
            out.write_arg(b"PAYLOAD");
            out.write_arg(self.payload);
        }
        if !self.metadata.is_empty() {
            out.write_arg(b"METADATA");
            out.write_arg(self.metadata);
        }
    }
}
