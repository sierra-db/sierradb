use combine::Parser;
use redis_protocol::resp3::types::BytesFrame;
use sierradb_protocol::ErrorCode;
use uuid::Uuid;

use crate::parser::{FrameStream, number_u64, subscription_id};
use crate::request::{HandleRequest, simple_str};
use crate::server::Conn;

/// Acknowledge events up to a cursor for a subscription.
///
/// # Syntax
/// ```text
/// EACK <subscription_id> <sequence_or_version>
/// ```
///
/// # Examples
/// ```text
/// EACK 550e8400-e29b-41d4-a716-446655440000 1000
/// ```
pub struct EAck {
    pub subscription_id: Uuid,
    pub cursor: u64,
}

impl EAck {
    pub fn parser<'a>() -> impl Parser<FrameStream<'a>, Output = EAck> + 'a {
        (subscription_id(), number_u64()).map(|(subscription_id, cursor)| EAck {
            subscription_id,
            cursor,
        })
    }
}

impl HandleRequest for EAck {
    type Error = String;
    type Ok = BytesFrame;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let cursor_tx = conn
            .subscriptions
            .get(&self.subscription_id)
            .ok_or_else(|| ErrorCode::NotFound.with_message("subscription not found"))?;

        let result = cursor_tx.send(Some(self.cursor));

        match result {
            Ok(_) => Ok(Some(simple_str("OK"))),
            Err(_) => Err("failed to acknowledge events".to_string()),
        }
    }
}
