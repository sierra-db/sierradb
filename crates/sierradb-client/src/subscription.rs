use std::collections::HashMap;
use std::sync::Arc;

use futures_util::stream::Stream;
use redis::{
    AsyncConnectionConfig, Client, FromRedisValue, PushInfo, PushKind, RedisError, RedisResult,
    Value, cmd,
};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tracing::error;
use uuid::Uuid;

use crate::types::{Event, SierraMessage};

/// A manager for SierraDB subscriptions that handles a shared push channel
/// and demultiplexes messages to individual subscriptions.
pub struct SubscriptionManager {
    inner: Arc<Mutex<SubscriptionManagerInner>>,
    _background_task: JoinHandle<()>,
}

struct SubscriptionManagerInner {
    connection: redis::aio::MultiplexedConnection,
    subscriptions: HashMap<Uuid, mpsc::UnboundedSender<SierraMessage>>,
}

impl SubscriptionManager {
    /// Create a new SubscriptionManager with the given Redis client.
    ///
    /// This establishes a multiplexed connection and starts a background task
    /// to process push messages.
    pub async fn new(client: &Client) -> RedisResult<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel::<PushInfo>();

        let connection = client
            .get_multiplexed_async_connection_with_config(
                &AsyncConnectionConfig::new().set_push_sender(tx),
            )
            .await?;

        let inner = Arc::new(Mutex::new(SubscriptionManagerInner {
            connection,
            subscriptions: HashMap::new(),
        }));

        // Spawn background task to handle push messages
        let inner_clone = inner.clone();
        let background_task = tokio::spawn(async move {
            while let Some(push_info) = rx.recv().await {
                let mut inner_guard = inner_clone.lock().await;
                if let Err(err) = inner_guard.handle_push_message(push_info).await {
                    error!("error handling push message: {err}");
                }
            }
        });

        Ok(SubscriptionManager {
            inner,
            _background_task: background_task,
        })
    }

    /// Subscribe to events from a stream.
    pub async fn subscribe_to_stream<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let response: Value = cmd("ESUB")
            .arg(stream_id)
            .query_async(&mut inner.connection)
            .await?;

        let subscription_id = match response {
            Value::SimpleString(id_str) => Uuid::parse_str(&id_str).map_err(|_| {
                RedisError::from((redis::ErrorKind::TypeError, "Invalid UUID in response"))
            })?,
            _ => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected subscription ID",
                )));
            }
        };

        let (sender, receiver) = mpsc::unbounded_channel();
        inner.subscriptions.insert(subscription_id, sender);

        Ok(EventSubscription {
            subscription_id,
            receiver,
            manager: self.inner.clone(),
        })
    }

    /// Subscribe to events from a stream starting from a specific version.
    pub async fn subscribe_to_stream_from_version<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        from_version: u64,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let response: Value = cmd("ESUB")
            .arg(stream_id)
            .arg("FROM_VERSION")
            .arg(from_version)
            .query_async(&mut inner.connection)
            .await?;

        let subscription_id = match response {
            Value::SimpleString(id_str) => Uuid::parse_str(&id_str).map_err(|_| {
                RedisError::from((redis::ErrorKind::TypeError, "Invalid UUID in response"))
            })?,
            _ => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected subscription ID",
                )));
            }
        };

        let (sender, receiver) = mpsc::unbounded_channel();
        inner.subscriptions.insert(subscription_id, sender);

        Ok(EventSubscription {
            subscription_id,
            receiver,
            manager: self.inner.clone(),
        })
    }

    /// Subscribe to events from a specific partition.
    pub async fn subscribe_to_partition(
        &mut self,
        partition: u16,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let response: Value = cmd("EPSUB")
            .arg(partition)
            .query_async(&mut inner.connection)
            .await?;

        let subscription_id = match response {
            Value::SimpleString(id_str) => Uuid::parse_str(&id_str).map_err(|_| {
                RedisError::from((redis::ErrorKind::TypeError, "Invalid UUID in response"))
            })?,
            _ => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected subscription ID",
                )));
            }
        };

        let (sender, receiver) = mpsc::unbounded_channel();
        inner.subscriptions.insert(subscription_id, sender);

        Ok(EventSubscription {
            subscription_id,
            receiver,
            manager: self.inner.clone(),
        })
    }

    /// Subscribe to events from a partition identified by a UUID key.
    pub async fn subscribe_to_partition_key(
        &mut self,
        key: Uuid,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let response: Value = cmd("EPSUB")
            .arg(key.to_string())
            .query_async(&mut inner.connection)
            .await?;

        let subscription_id = match response {
            Value::SimpleString(id_str) => Uuid::parse_str(&id_str).map_err(|_| {
                RedisError::from((redis::ErrorKind::TypeError, "Invalid UUID in response"))
            })?,
            _ => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected subscription ID",
                )));
            }
        };

        let (sender, receiver) = mpsc::unbounded_channel();
        inner.subscriptions.insert(subscription_id, sender);

        Ok(EventSubscription {
            subscription_id,
            receiver,
            manager: self.inner.clone(),
        })
    }

    /// Subscribe to events from a stream with a partition key.
    pub async fn subscribe_to_stream_with_partition_key<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        partition_key: Uuid,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let response: Value = cmd("ESUB")
            .arg(stream_id)
            .arg("PARTITION_KEY")
            .arg(partition_key.to_string())
            .query_async(&mut inner.connection)
            .await?;

        let subscription_id = match response {
            Value::SimpleString(id_str) => Uuid::parse_str(&id_str).map_err(|_| {
                RedisError::from((redis::ErrorKind::TypeError, "Invalid UUID in response"))
            })?,
            _ => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected subscription ID",
                )));
            }
        };

        let (sender, receiver) = mpsc::unbounded_channel();
        inner.subscriptions.insert(subscription_id, sender);

        Ok(EventSubscription {
            subscription_id,
            receiver,
            manager: self.inner.clone(),
        })
    }

    /// Subscribe to events from a stream with both partition key and starting
    /// version.
    pub async fn subscribe_to_stream_with_partition_and_version<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        partition_key: Uuid,
        from_version: u64,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let response: Value = cmd("ESUB")
            .arg(stream_id)
            .arg("PARTITION_KEY")
            .arg(partition_key.to_string())
            .arg("FROM_VERSION")
            .arg(from_version)
            .query_async(&mut inner.connection)
            .await?;

        let subscription_id = match response {
            Value::SimpleString(id_str) => Uuid::parse_str(&id_str).map_err(|_| {
                RedisError::from((redis::ErrorKind::TypeError, "Invalid UUID in response"))
            })?,
            _ => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected subscription ID",
                )));
            }
        };

        let (sender, receiver) = mpsc::unbounded_channel();
        inner.subscriptions.insert(subscription_id, sender);

        Ok(EventSubscription {
            subscription_id,
            receiver,
            manager: self.inner.clone(),
        })
    }

    /// Subscribe to events from a partition starting from a specific sequence.
    pub async fn subscribe_to_partition_from_sequence(
        &mut self,
        partition: u16,
        from_sequence: u64,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let response: Value = cmd("EPSUB")
            .arg(partition)
            .arg("FROM_SEQUENCE")
            .arg(from_sequence)
            .query_async(&mut inner.connection)
            .await?;

        let subscription_id = match response {
            Value::SimpleString(id_str) => Uuid::parse_str(&id_str).map_err(|_| {
                RedisError::from((redis::ErrorKind::TypeError, "Invalid UUID in response"))
            })?,
            _ => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected subscription ID",
                )));
            }
        };

        let (sender, receiver) = mpsc::unbounded_channel();
        inner.subscriptions.insert(subscription_id, sender);

        Ok(EventSubscription {
            subscription_id,
            receiver,
            manager: self.inner.clone(),
        })
    }

    /// Subscribe to events from a partition (by key) starting from a specific
    /// sequence.
    pub async fn subscribe_to_partition_key_from_sequence(
        &mut self,
        key: Uuid,
        from_sequence: u64,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let response: Value = cmd("EPSUB")
            .arg(key.to_string())
            .arg("FROM_SEQUENCE")
            .arg(from_sequence)
            .query_async(&mut inner.connection)
            .await?;

        let subscription_id = match response {
            Value::SimpleString(id_str) => Uuid::parse_str(&id_str).map_err(|_| {
                RedisError::from((redis::ErrorKind::TypeError, "Invalid UUID in response"))
            })?,
            _ => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Expected subscription ID",
                )));
            }
        };

        let (sender, receiver) = mpsc::unbounded_channel();
        inner.subscriptions.insert(subscription_id, sender);

        Ok(EventSubscription {
            subscription_id,
            receiver,
            manager: self.inner.clone(),
        })
    }
}

impl SubscriptionManagerInner {
    async fn handle_push_message(&mut self, push_info: PushInfo) -> Result<(), RedisError> {
        let PushInfo { kind, data } = push_info;

        match data.as_slice() {
            [Value::SimpleString(subscription_id_str), third] => {
                let subscription_id = Uuid::parse_str(subscription_id_str).map_err(|_| {
                    RedisError::from((redis::ErrorKind::TypeError, "Invalid subscription ID"))
                })?;

                if let Some(sender) = self.subscriptions.get(&subscription_id) {
                    let message = match kind {
                        PushKind::Subscribe => {
                            if let Value::Int(count) = third {
                                SierraMessage::SubscriptionConfirmed {
                                    subscription_count: *count,
                                }
                            } else {
                                return Err(RedisError::from((
                                    redis::ErrorKind::TypeError,
                                    "Invalid subscription count",
                                )));
                            }
                        }
                        PushKind::Message => {
                            let event = Event::from_redis_value(third)?;
                            SierraMessage::Event(event)
                        }
                        _ => {
                            return Err(RedisError::from((
                                redis::ErrorKind::TypeError,
                                "Unknown message type",
                            )));
                        }
                    };

                    if sender.send(message).is_err() {
                        // Subscription was dropped, remove it
                        self.subscriptions.remove(&subscription_id);
                    }
                }
            }
            _ => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Unexpected push message format",
                )));
            }
        }

        Ok(())
    }
}

/// A handle to an individual SierraDB subscription.
///
/// Provides a typed interface for receiving subscription messages.
pub struct EventSubscription {
    subscription_id: Uuid,
    receiver: mpsc::UnboundedReceiver<SierraMessage>,
    manager: Arc<Mutex<SubscriptionManagerInner>>,
}

impl EventSubscription {
    /// Get the unique identifier for this subscription.
    pub fn subscription_id(&self) -> Uuid {
        self.subscription_id
    }

    /// Receive the next message from this subscription.
    ///
    /// Returns `None` if the subscription has been closed.
    pub async fn next_message(&mut self) -> Option<SierraMessage> {
        self.receiver.recv().await
    }

    /// Convert this subscription into a stream of messages.
    ///
    /// This consumes the subscription and returns a stream that can be used
    /// with stream combinators like `while let Some(msg) =
    /// stream.next().await`.
    pub fn into_stream(self) -> impl Stream<Item = SierraMessage> {
        futures_util::stream::unfold(self, |mut subscription| async move {
            subscription
                .next_message()
                .await
                .map(|msg| (msg, subscription))
        })
    }

    /// Unsubscribe and close this subscription.
    ///
    /// After calling this, no more messages will be received.
    pub async fn unsubscribe(mut self) -> RedisResult<()> {
        // Close the receiver
        self.receiver.close();

        // Remove from the manager's subscriptions map
        let mut manager = self.manager.lock().await;
        manager.subscriptions.remove(&self.subscription_id);

        // TODO: Send UNSUBSCRIBE command to server when that's implemented
        Ok(())
    }
}

impl Drop for EventSubscription {
    fn drop(&mut self) {
        // Remove from subscriptions map when dropped
        let manager = self.manager.clone();
        let subscription_id = self.subscription_id;

        tokio::spawn(async move {
            let mut inner = manager.lock().await;
            inner.subscriptions.remove(&subscription_id);
        });
    }
}
