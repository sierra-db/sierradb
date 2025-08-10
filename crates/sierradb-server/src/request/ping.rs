use std::convert::Infallible;

use redis_protocol::resp3::types::BytesFrame;

use crate::impl_command;
use crate::request::{HandleRequest, simple_str};
use crate::server::Conn;

pub struct Ping {}

impl_command!(Ping, [], []);

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
