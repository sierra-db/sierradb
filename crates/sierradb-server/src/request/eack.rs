use redis_protocol::resp3::types::BytesFrame;
use sierradb_cluster::subscription::AcknowledgeEvents;
use sierradb_protocol::ErrorCode;
use uuid::Uuid;

use crate::impl_command;
use crate::request::{HandleRequest, simple_str};
use crate::server::Conn;

/// Acknowledge events up to a specific sequence/version for a subscription.
///
/// # Syntax
/// ```text
/// EACK <subscription_id> <sequence_or_version>
/// ```
///
/// # Parameters
/// - `subscription_id`: UUID of the subscription to acknowledge
/// - `sequence_or_version`: Sequence number (for partition subscriptions) or
///   version number (for stream subscriptions) up to which all events are
///   acknowledged
///
/// # Examples
/// ```text
/// EACK 550e8400-e29b-41d4-a716-446655440000 1000
/// ```
///
/// **Note:** Acknowledges all events up to and including the specified
/// sequence/version, allowing the subscription window to advance.
pub struct EAck {
    pub subscription_id: Uuid,
    pub sequence_or_version: u64,
}

impl_command!(EAck, [subscription_id, sequence_or_version], []);

impl HandleRequest for EAck {
    type Error = String;
    type Ok = BytesFrame;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        if !conn.subscriptions.contains(&self.subscription_id) {
            return Err(ErrorCode::InvalidArg.with_message("subscription not found"));
        }

        let result = conn
            .cluster_ref
            .tell(AcknowledgeEvents {
                subscription_id: self.subscription_id,
                up_to_sequence: self.sequence_or_version,
            })
            .send()
            .await;

        match result {
            Ok(_) => Ok(Some(simple_str("OK"))),
            Err(_) => Err("failed to acknowledge events".to_string()),
        }
    }
}
