use std::collections::HashMap;

use combine::Parser;
use libp2p::PeerId;
use redis_protocol::resp3::types::BytesFrame;

use crate::parser::{FrameStream, number_i64};
use crate::request::{HandleRequest, blob_str, map, number};
use crate::server::Conn;

pub struct Hello {
    pub version: i64,
}

impl Hello {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = Hello> + 'a {
        number_i64().map(|version| Hello { version })
    }
}

impl HandleRequest for Hello {
    type Error = String;
    type Ok = HelloResp;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        if self.version != 3 {
            return Err(format!(
                "unsupported version {}, only 3 is supported",
                self.version
            ));
        }

        Ok(Some(HelloResp {
            server: "sierradb",
            version: env!("CARGO_PKG_VERSION"),
            peer_id: *conn.cluster_ref.id().peer_id().unwrap(),
            num_partitions: conn.num_partitions,
        }))
    }
}

pub struct HelloResp {
    server: &'static str,
    version: &'static str,
    peer_id: PeerId,
    num_partitions: u16,
}

impl From<HelloResp> for BytesFrame {
    fn from(resp: HelloResp) -> Self {
        map(HashMap::from_iter([
            (blob_str("server"), blob_str(resp.server)),
            (blob_str("version"), blob_str(resp.version)),
            (blob_str("peer_id"), blob_str(resp.peer_id.to_base58())),
            (
                blob_str("num_partitions"),
                number(resp.num_partitions.into()),
            ),
        ]))
    }
}
