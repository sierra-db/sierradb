use std::convert::Infallible;

use combine::{Parser, value};
use redis_protocol::resp3::types::BytesFrame;

use crate::parser::FrameStream;
use crate::request::{HandleRequest, simple_str};
use crate::server::Conn;

#[derive(Clone, Copy)]
pub struct Ping {}

impl Ping {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = Ping> + 'a {
        value(Ping {})
    }
}

impl HandleRequest for Ping {
    type Error = Infallible;
    type Ok = PingResp;

    async fn handle_request(self, _conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        Ok(Some(PingResp))
    }
}

pub struct PingResp;

impl From<PingResp> for BytesFrame {
    fn from(_: PingResp) -> Self {
        simple_str("PONG")
    }
}
