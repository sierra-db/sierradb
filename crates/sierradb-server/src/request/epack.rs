use redis_protocol::resp3::types::BytesFrame;
use sierradb_cluster::subscription::AcknowledgeAllEvents;
use sierradb_protocol::ErrorCode;
use uuid::Uuid;

use crate::impl_command;
use crate::request::{HandleRequest, simple_str};
use crate::server::Conn;

/// Acknowledge all pending events for a subscription.
///
/// # Syntax
/// ```text
/// EPACK <subscription_id>
/// ```
///
/// # Parameters
/// - `subscription_id`: UUID of the subscription to acknowledge all events for
///
/// # Examples
/// ```text
/// EPACK 550e8400-e29b-41d4-a716-446655440000
/// ```
///
/// **Note:** Acknowledges all currently pending unacknowledged events for the 
/// subscription, effectively resetting the window to allow maximum throughput.
pub struct EPack {
    pub subscription_id: Uuid,
}

impl_command!(EPack, [subscription_id], []);

impl HandleRequest for EPack {
    type Error = String;
    type Ok = BytesFrame;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        if !conn.subscriptions.contains(&self.subscription_id) {
            return Err(ErrorCode::InvalidArg.with_message("subscription not found"));
        }

        let result = conn
            .cluster_ref
            .tell(AcknowledgeAllEvents {
                subscription_id: self.subscription_id,
            })
            .send()
            .await;

        match result {
            Ok(_) => Ok(Some(simple_str("OK"))),
            Err(_) => Err("failed to acknowledge all events".to_string()),
        }
    }
}