use kameo::error::{Infallible, SendError};
use redis_protocol::resp3;
use redis_protocol::resp3::types::BytesFrame;
use sierradb::StreamId;
use sierradb_cluster::subscription::CreateStreamSubscription;
use sierradb_protocol::ErrorCode;
use tokio::io::{self, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::debug;
use uuid::Uuid;

use crate::error::AsRedisError;
use crate::impl_command;
use crate::request::{HandleRequest, number, simple_str};
use crate::server::Conn;

/// Subscribe to events from a stream.
///
/// # Syntax
/// ```text
/// ESUB <stream_id> [PARTITION_KEY <partition_key>] [FROM_VERSION <version>]
/// ```
///
/// # Parameters
/// - `stream_id`: Stream identifier to subscribe to
/// - `partition_key` (optional): UUID to subscribe to specific partition
/// - `from_version` (optional): Start streaming from this version number
///   (defaults to current end)
///
/// # Examples
/// ```text
/// ESUB my-stream
/// ESUB my-stream PARTITION_KEY 550e8400-e29b-41d4-a716-446655440000 FROM_VERSION 100
/// ```
///
/// **Note:** Establishes a persistent connection to receive real-time stream
/// events.
pub struct ESub {
    pub stream_id: StreamId,
    pub partition_key: Option<Uuid>,
    pub from_version: Option<u64>,
}

impl_command!(ESub, [stream_id], [partition_key, from_version]);

impl HandleRequest for ESub {
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

        let subscription_id = conn
            .cluster_ref
            .ask(CreateStreamSubscription {
                stream_id: self.stream_id.clone(),
                partition_key: self.partition_key,
                from_version: self.from_version,
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
            stream_id = %self.stream_id,
            partition_key = ?self.partition_key,
            from_version = ?self.from_version,
            "created stream subscription"
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
                    subscription_id,
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
