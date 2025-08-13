use combine::Parser;
use redis_protocol::resp3::types::BytesFrame;
use sierradb::bucket::segment::EventRecord;
use sierradb_cluster::read::ReadEvent;
use uuid::Uuid;

use crate::error::MapRedisError;
use crate::parser::{FrameStream, event_id};
use crate::request::{HandleRequest, encode_event};
use crate::server::Conn;

/// Get an event by its unique identifier.
///
/// # Syntax
/// ```text
/// EGET <event_id>
/// ```
///
/// # Parameters
/// - `event_id`: UUID of the event to retrieve
///
/// # Example
/// ```text
/// EGET 550e8400-e29b-41d4-a716-446655440000
/// ```
pub struct EGet {
    pub event_id: Uuid,
}

impl EGet {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = EGet> + 'a {
        event_id().map(|event_id| EGet { event_id })
    }
}

impl HandleRequest for EGet {
    type Error = String;
    type Ok = EGetResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        match conn
            .cluster_ref
            .ask(ReadEvent::new(self.event_id))
            .await
            .map_redis_err()?
        {
            Some(record) => Ok(Some(EGetResp::Exists(record))),
            None => Ok(Some(EGetResp::NotFound)),
        }
    }
}

pub enum EGetResp {
    Exists(EventRecord),
    NotFound,
}

impl From<EGetResp> for BytesFrame {
    fn from(resp: EGetResp) -> Self {
        match resp {
            EGetResp::Exists(record) => encode_event(record),
            EGetResp::NotFound => BytesFrame::Null,
        }
    }
}
