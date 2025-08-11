use kameo::error::{Infallible, SendError};
use redis_protocol::resp3;
use redis_protocol::resp3::types::BytesFrame;
use sierradb_cluster::subscription::CreatePartitionSubscription;
use sierradb_protocol::ErrorCode;
use tokio::io::{self, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::debug;

use crate::error::AsRedisError;
use crate::impl_command;
use crate::request::{HandleRequest, PartitionSelector, number, simple_str};
use crate::server::Conn;

/// Subscribe to events from a partition.
///
/// # Syntax
/// ```text
/// EPSUB <partition> [FROM_SEQUENCE <sequence>] [WINDOW_SIZE <size>]
/// ```
///
/// # Parameters
/// - `partition`: Partition selector (partition ID 0-65535 or UUID key)
/// - `from_sequence` (optional): Start streaming from this sequence number
///   (defaults to current end)
/// - `window_size` (optional): Maximum unacknowledged events (defaults to unlimited)
///
/// # Examples
/// ```text
/// EPSUB 42
/// EPSUB 550e8400-e29b-41d4-a716-446655440000 FROM_SEQUENCE 1000
/// EPSUB 42 FROM_SEQUENCE 500 WINDOW_SIZE 1000
/// ```
///
/// **Note:** Establishes a persistent connection to receive real-time partition
/// events.
pub struct EPSub {
    pub partition: PartitionSelector,
    pub from_sequence: Option<u64>,
    pub window_size: Option<u32>,
}

impl_command!(EPSub, [partition], [from_sequence, window_size]);

impl HandleRequest for EPSub {
    type Error = String;
    type Ok = BytesFrame;

    async fn handle_request(self, conn: &mut Conn) -> Result<Option<Self::Ok>, Self::Error> {
        let sender = match conn
            .subscription_channel
            .as_ref()
            .and_then(|(weak_sender, _)| weak_sender.upgrade())
        {
            Some(sender) => sender,
            None => {
                let (sender, receiver) = mpsc::unbounded_channel();
                conn.subscription_channel = Some((sender.downgrade(), receiver));
                sender
            }
        };

        let partition_id = self.partition.into_partition_id(conn.num_partitions);

        let subscription_id = conn
            .cluster_ref
            .ask(CreatePartitionSubscription {
                partition_id,
                from_sequence: self.from_sequence,
                window_size: self.window_size,
                sender,
            })
            .await
            .map_err(|err| match err {
                SendError::HandlerError(replicas) => ErrorCode::Redirect.with_message(
                    replicas
                        .into_iter()
                        .map(|(replica, _)| replica.id().peer_id().unwrap().to_base58())
                        .collect::<Vec<_>>()
                        .join(" "),
                ),
                err => err
                    .map_err::<Infallible, _>(|_| unreachable!())
                    .as_redis_error(),
            })?;

        conn.subscriptions.insert(subscription_id);

        debug!(
            subscription_id = %subscription_id,
            partition_id = partition_id,
            from_sequence = ?self.from_sequence,
            "created partition subscription"
        );

        Ok(Some(simple_str(subscription_id.to_string())))
    }

    async fn handle_request_failable(
        self,
        conn: &mut Conn,
    ) -> Result<Option<BytesFrame>, io::Error> {
        let subscription_id = match self.handle_request(conn).await {
            Ok(Some(subscription_id)) => subscription_id,
            Ok(None) => unreachable!("always returns some"),
            Err(err) => {
                return Ok(Some(BytesFrame::SimpleError {
                    data: err.into(),
                    attributes: None,
                }));
            }
        };

        resp3::encode::complete::extend_encode(&mut conn.write, &subscription_id, false)
            .map_err(io::Error::other)?;

        resp3::encode::complete::extend_encode(
            &mut conn.write,
            &BytesFrame::Push {
                data: vec![
                    simple_str("subscribe"),
                    subscription_id.clone(),
                    number(conn.subscriptions.len() as i64),
                ],
                attributes: None,
            },
            false,
        )
        .map_err(io::Error::other)?;

        conn.socket.write_all(&conn.write).await?;
        conn.socket.flush().await?;
        conn.write.clear();

        Ok(None)
    }
}
