use std::collections::HashMap;
use std::sync::Arc;

use futures_util::stream::Stream;
use redis::{
    AsyncConnectionConfig, Client, FromRedisValue, PushInfo, PushKind, RedisError, RedisResult,
    Value, cmd,
};
use tokio::sync::{Mutex, mpsc};
use tracing::error;
use uuid::Uuid;

use crate::types::{Event, SierraMessage};

/// Selector for partition subscriptions
#[derive(Debug, Clone)]
enum PartitionSelector {
    Id(u16),
    Key(Uuid),
}

/// A manager for SierraDB subscriptions that handles a shared push channel
/// and demultiplexes messages to individual subscriptions.
#[derive(Clone)]
pub struct SubscriptionManager {
    inner: Arc<Mutex<SubscriptionManagerInner>>,
    // _background_task: JoinHandle<()>,
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
        let _background_task = tokio::spawn(async move {
            while let Some(push_info) = rx.recv().await {
                let mut inner_guard = inner_clone.lock().await;
                if let Err(err) = inner_guard.handle_push_message(push_info).await {
                    error!("error handling push message: {err}");
                }
            }
        });

        Ok(SubscriptionManager {
            inner,
            // _background_task: background_task,
        })
    }

    /// Subscribe to events from a stream.
    pub async fn subscribe_to_stream<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_stream_with_options(stream_id, None, None, None)
            .await
    }

    /// Subscribe to events from a stream with optional windowing.
    pub async fn subscribe_to_stream_with_window<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        window_size: u32,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_stream_with_options(stream_id, None, None, Some(window_size))
            .await
    }

    /// Subscribe to events from a stream starting from a specific version.
    pub async fn subscribe_to_stream_from_version<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        from_version: u64,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_stream_with_options(stream_id, None, Some(from_version), None)
            .await
    }

    /// Subscribe to events from a stream starting from a specific version with
    /// windowing.
    pub async fn subscribe_to_stream_from_version_with_window<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        from_version: u64,
        window_size: u32,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_stream_with_options(
            stream_id,
            None,
            Some(from_version),
            Some(window_size),
        )
        .await
    }

    /// Core method to subscribe to events from a stream with all options.
    async fn subscribe_to_stream_with_options<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        partition_key: Option<Uuid>,
        from_version: Option<u64>,
        window_size: Option<u32>,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let mut cmd = cmd("ESUB");
        cmd.arg(stream_id);

        if let Some(key) = partition_key {
            cmd.arg("PARTITION_KEY").arg(key.to_string());
        }

        if let Some(version) = from_version {
            cmd.arg("FROM").arg(version);
        }

        if let Some(size) = window_size {
            cmd.arg("WINDOW").arg(size);
        }

        let response: Value = cmd.query_async(&mut inner.connection).await?;

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
        self.subscribe_to_partition_with_options(PartitionSelector::Id(partition), None, None)
            .await
    }

    /// Subscribe to events from a specific partition with windowing.
    pub async fn subscribe_to_partition_with_window(
        &mut self,
        partition: u16,
        window_size: u32,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_partition_with_options(
            PartitionSelector::Id(partition),
            None,
            Some(window_size),
        )
        .await
    }

    /// Subscribe to events from a partition identified by a UUID key.
    pub async fn subscribe_to_partition_key(
        &mut self,
        key: Uuid,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_partition_with_options(PartitionSelector::Key(key), None, None)
            .await
    }

    /// Subscribe to events from a partition identified by a UUID key with
    /// windowing.
    pub async fn subscribe_to_partition_key_with_window(
        &mut self,
        key: Uuid,
        window_size: u32,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_partition_with_options(
            PartitionSelector::Key(key),
            None,
            Some(window_size),
        )
        .await
    }

    /// Subscribe to events from a stream with a partition key.
    pub async fn subscribe_to_stream_with_partition_key<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        partition_key: Uuid,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_stream_with_options(stream_id, Some(partition_key), None, None)
            .await
    }

    /// Subscribe to events from a stream with a partition key and windowing.
    pub async fn subscribe_to_stream_with_partition_key_and_window<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        partition_key: Uuid,
        window_size: u32,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_stream_with_options(
            stream_id,
            Some(partition_key),
            None,
            Some(window_size),
        )
        .await
    }

    /// Subscribe to events from a stream with both partition key and starting
    /// version.
    pub async fn subscribe_to_stream_with_partition_and_version<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
        partition_key: Uuid,
        from_version: u64,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_stream_with_options(
            stream_id,
            Some(partition_key),
            Some(from_version),
            None,
        )
        .await
    }

    /// Subscribe to events from a stream with partition key, starting version,
    /// and windowing.
    pub async fn subscribe_to_stream_with_partition_and_version_and_window<
        S: redis::ToRedisArgs,
    >(
        &mut self,
        stream_id: S,
        partition_key: Uuid,
        from_version: u64,
        window_size: u32,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_stream_with_options(
            stream_id,
            Some(partition_key),
            Some(from_version),
            Some(window_size),
        )
        .await
    }

    /// Subscribe to events from a partition starting from a specific sequence.
    pub async fn subscribe_to_partition_from_sequence(
        &mut self,
        partition: u16,
        from_sequence: u64,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_partition_with_options(
            PartitionSelector::Id(partition),
            Some(from_sequence),
            None,
        )
        .await
    }

    /// Subscribe to events from a partition starting from a specific sequence
    /// with windowing.
    pub async fn subscribe_to_partition_from_sequence_with_window(
        &mut self,
        partition: u16,
        from_sequence: u64,
        window_size: u32,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_partition_with_options(
            PartitionSelector::Id(partition),
            Some(from_sequence),
            Some(window_size),
        )
        .await
    }

    /// Subscribe to events from a partition (by key) starting from a specific
    /// sequence.
    pub async fn subscribe_to_partition_key_from_sequence(
        &mut self,
        key: Uuid,
        from_sequence: u64,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_partition_with_options(
            PartitionSelector::Key(key),
            Some(from_sequence),
            None,
        )
        .await
    }

    /// Subscribe to events from a partition (by key) starting from a specific
    /// sequence with windowing.
    pub async fn subscribe_to_partition_key_from_sequence_with_window(
        &mut self,
        key: Uuid,
        from_sequence: u64,
        window_size: u32,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_partition_with_options(
            PartitionSelector::Key(key),
            Some(from_sequence),
            Some(window_size),
        )
        .await
    }

    /// Core method to subscribe to events from a partition with all options.
    async fn subscribe_to_partition_with_options(
        &mut self,
        partition: PartitionSelector,
        from_sequence: Option<u64>,
        window_size: Option<u32>,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let mut cmd = cmd("EPSUB");

        match partition {
            PartitionSelector::Id(id) => cmd.arg(id),
            PartitionSelector::Key(key) => cmd.arg(key.to_string()),
        };

        if let Some(sequence) = from_sequence {
            cmd.arg("FROM").arg(sequence);
        }

        if let Some(size) = window_size {
            cmd.arg("WINDOW").arg(size);
        }

        let response: Value = cmd.query_async(&mut inner.connection).await?;

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

    /// Acknowledge events up to a specific cursor for a subscription.
    ///
    /// The cursor value is provided with each event in the subscription stream.
    /// Acknowledging up to a cursor allows the subscription window to advance.
    pub async fn acknowledge_up_to_cursor(
        &mut self,
        subscription_id: Uuid,
        cursor: u64,
    ) -> RedisResult<()> {
        let mut inner = self.inner.lock().await;

        let _response: Value = cmd("EACK")
            .arg(subscription_id.to_string())
            .arg(cursor)
            .query_async(&mut inner.connection)
            .await?;

        Ok(())
    }

    /// Subscribe to events from multiple partitions with the same starting
    /// sequence.
    ///
    /// # Arguments
    /// * `partition_range` - String specifying partitions: "*", "0-127",
    ///   "0,1,5", or "42"
    /// * `from_sequence` - Starting sequence number for all partitions
    /// * `window_size` - Optional window size for flow control
    ///
    /// # Examples
    /// ```rust,ignore
    /// let sub = manager.subscribe_to_partitions("*", 1000, Some(100)).await?;
    /// let sub = manager.subscribe_to_partitions("0-127", 500, None).await?;
    /// let sub = manager.subscribe_to_partitions("0,1,5", 0, Some(50)).await?;
    /// ```
    pub async fn subscribe_to_partitions(
        &mut self,
        partition_range: &str,
        from_sequence: u64,
        window_size: Option<u32>,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let mut cmd = cmd("EPSUB");
        cmd.arg(partition_range);
        cmd.arg("FROM").arg(from_sequence);

        if let Some(size) = window_size {
            cmd.arg("WINDOW").arg(size);
        }

        let response: Value = cmd.query_async(&mut inner.connection).await?;

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

    /// Subscribe to events from multiple partitions with per-partition
    /// sequences.
    ///
    /// # Arguments
    /// * `partition_sequences` - Map of partition_id -> from_sequence
    /// * `window_size` - Optional window size for flow control
    ///
    /// # Example
    /// ```rust,ignore
    /// let mut sequences = HashMap::new();
    /// sequences.insert(0, 500);
    /// sequences.insert(1, 1200);
    /// sequences.insert(127, 999);
    /// let sub = manager.subscribe_to_partitions_with_sequences(sequences, Some(100)).await?;
    /// ```
    pub async fn subscribe_to_partitions_with_sequences(
        &mut self,
        partition_sequences: HashMap<u16, u64>,
        window_size: Option<u32>,
    ) -> RedisResult<EventSubscription> {
        if partition_sequences.is_empty() {
            return Err(RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "At least one partition must be specified",
            )));
        }

        let mut inner = self.inner.lock().await;

        let mut cmd = cmd("EPSUB");

        // Create partition list from the keys
        let partition_list: Vec<String> =
            partition_sequences.keys().map(|&p| p.to_string()).collect();
        cmd.arg(partition_list.join(","));

        cmd.arg("FROM");
        cmd.arg("MAP");

        // Send as space-separated "partition=sequence partition=sequence" format
        for (partition_id, sequence) in &partition_sequences {
            cmd.arg(format!("{partition_id}={sequence}"));
        }

        if let Some(size) = window_size {
            cmd.arg("WINDOW").arg(size);
        }

        let response: Value = cmd.query_async(&mut inner.connection).await?;

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

    /// Subscribe to all partitions starting from the same sequence.
    ///
    /// This is a convenience method equivalent to `subscribe_to_partitions("*",
    /// from_sequence, window_size)`.
    pub async fn subscribe_to_all_partitions(
        &mut self,
        from_sequence: u64,
        window_size: Option<u32>,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_partitions("*", from_sequence, window_size)
            .await
    }

    /// Subscribe to a range of partitions starting from the same sequence.
    ///
    /// # Arguments
    /// * `start_partition` - Starting partition ID (inclusive)
    /// * `end_partition` - Ending partition ID (inclusive)
    /// * `from_sequence` - Starting sequence number for all partitions in range
    /// * `window_size` - Optional window size for flow control
    ///
    /// # Example
    /// ```rust,ignore
    /// // Subscribe to partitions 0-127 starting from sequence 1000
    /// let sub = manager.subscribe_to_partition_range(0, 127, 1000, Some(50)).await?;
    /// ```
    pub async fn subscribe_to_partition_range(
        &mut self,
        start_partition: u16,
        end_partition: u16,
        from_sequence: u64,
        window_size: Option<u32>,
    ) -> RedisResult<EventSubscription> {
        let range = format!("{start_partition}-{end_partition}");
        self.subscribe_to_partitions(&range, from_sequence, window_size)
            .await
    }

    /// Subscribe to events from a stream starting from latest.
    pub async fn subscribe_to_stream_from_latest<S: redis::ToRedisArgs>(
        &mut self,
        stream_id: S,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let mut cmd = cmd("ESUB");
        cmd.arg(stream_id);
        cmd.arg("FROM");
        cmd.arg("LATEST");

        let response: Value = cmd.query_async(&mut inner.connection).await?;

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

    /// Subscribe to events from all partitions starting from latest.
    pub async fn subscribe_to_all_partitions_from_latest(
        &mut self,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let mut cmd = cmd("EPSUB");
        cmd.arg("*");
        cmd.arg("FROM");
        cmd.arg("LATEST");

        let response: Value = cmd.query_async(&mut inner.connection).await?;

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

    /// Subscribe to events from all partitions with custom starting sequences
    /// for specific partitions and a fallback.
    ///
    /// # Arguments
    /// * `partition_sequences` - Map of partition_id -> starting_sequence for
    ///   specific partitions
    /// * `fallback_sequence` - Starting sequence for all other partitions not
    ///   specified in the map
    /// * `window_size` - Optional window size for flow control
    ///
    /// # Example
    /// ```rust,ignore
    /// let mut sequences = HashMap::new();
    /// sequences.insert(0, 1000);    // Partition 0 starts from sequence 1000
    /// sequences.insert(5, 2500);    // Partition 5 starts from sequence 2500
    /// // All other partitions start from sequence 100 (fallback)
    /// let sub = manager.subscribe_to_all_partitions_with_fallback(sequences, 100, Some(50)).await?;
    /// ```
    pub async fn subscribe_to_all_partitions_with_fallback(
        &mut self,
        partition_sequences: HashMap<u16, u64>,
        fallback_sequence: u64,
        window_size: Option<u32>,
    ) -> RedisResult<EventSubscription> {
        self.subscribe_to_all_partitions_flexible(
            partition_sequences,
            Some(fallback_sequence),
            window_size,
        )
        .await
    }

    /// Subscribe to events from all partitions with flexible starting position
    /// options.
    ///
    /// # Arguments
    /// * `from_map` - Map of partition_id -> starting_sequence for specific
    ///   partitions (empty map means no overrides)
    /// * `fallback_sequence` - Optional fallback sequence for partitions not in
    ///   the map
    /// * `window_size` - Optional window size for flow control
    ///
    /// # Examples
    /// ```rust,ignore
    /// // Subscribe from latest on all partitions
    /// let sub = manager.subscribe_to_all_partitions_flexible(HashMap::new(), None, Some(50)).await?;
    ///
    /// // Subscribe with specific sequences for some partitions, latest for others
    /// let mut sequences = HashMap::new();
    /// sequences.insert(0, 1000);
    /// sequences.insert(5, 2500);
    /// let sub = manager.subscribe_to_all_partitions_flexible(sequences, None, Some(50)).await?;
    ///
    /// // Subscribe with fallback for all partitions not specified
    /// let sub = manager.subscribe_to_all_partitions_flexible(sequences, Some(100), Some(50)).await?;
    /// ```
    pub async fn subscribe_to_all_partitions_flexible(
        &mut self,
        from_map: HashMap<u16, u64>,
        fallback_sequence: Option<u64>,
        window_size: Option<u32>,
    ) -> RedisResult<EventSubscription> {
        let mut inner = self.inner.lock().await;

        let mut cmd = cmd("EPSUB");
        cmd.arg("*");

        // Determine FROM clause based on parameters
        if from_map.is_empty() {
            match fallback_sequence {
                None => {
                    // Default to FROM LATEST
                    cmd.arg("FROM");
                    cmd.arg("LATEST");
                }
                Some(fallback) => {
                    // FROM <fallback> for all partitions
                    cmd.arg("FROM");
                    cmd.arg(fallback);
                }
            }
        } else {
            // FROM MAP with optional DEFAULT
            cmd.arg("FROM");
            cmd.arg("MAP");

            // Send partition=sequence pairs
            for (partition_id, sequence) in &from_map {
                cmd.arg(format!("{partition_id}={sequence}"));
            }

            // Add fallback sequence if provided
            if let Some(fallback) = fallback_sequence {
                cmd.arg("DEFAULT");
                cmd.arg(fallback);
            }
        }

        if let Some(size) = window_size {
            cmd.arg("WINDOW").arg(size);
        }

        let response: Value = cmd.query_async(&mut inner.connection).await?;

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

        match kind {
            PushKind::Message => {
                // Message format: data = [subscription_id, cursor, event]
                match data.as_slice() {
                    [
                        Value::SimpleString(subscription_id_str),
                        Value::Int(cursor),
                        event_value,
                    ] => {
                        let subscription_id =
                            Uuid::parse_str(subscription_id_str).map_err(|_| {
                                RedisError::from((
                                    redis::ErrorKind::TypeError,
                                    "Invalid subscription ID",
                                ))
                            })?;

                        if let Some(sender) = self.subscriptions.get(&subscription_id) {
                            let event = Event::from_redis_value(event_value)?;
                            let cursor = *cursor as u64;
                            let message = SierraMessage::Event { event, cursor };

                            if sender.send(message).is_err() {
                                // Subscription was dropped, remove it
                                self.subscriptions.remove(&subscription_id);
                            }
                        }
                    }
                    _ => {
                        return Err(RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Unexpected message format",
                        )));
                    }
                }
            }
            PushKind::Subscribe => {
                // Subscribe format: data = [subscription_id, count]
                match data.as_slice() {
                    [Value::SimpleString(subscription_id_str), Value::Int(count)] => {
                        let subscription_id =
                            Uuid::parse_str(subscription_id_str).map_err(|_| {
                                RedisError::from((
                                    redis::ErrorKind::TypeError,
                                    "Invalid subscription ID",
                                ))
                            })?;

                        if let Some(sender) = self.subscriptions.get(&subscription_id) {
                            let message = SierraMessage::SubscriptionConfirmed {
                                subscription_count: *count,
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
                            "Unexpected subscribe format",
                        )));
                    }
                }
            }
            PushKind::Disconnection => {}
            kind => {
                return Err(RedisError::from((
                    redis::ErrorKind::TypeError,
                    "Unknown push kind",
                    kind.to_string(),
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

    /// Acknowledge events up to a specific cursor for this subscription.
    ///
    /// The cursor value is provided with each event in the subscription stream.
    /// Acknowledging up to a cursor allows the subscription window to advance.
    pub async fn acknowledge_up_to_cursor(&self, cursor: u64) -> RedisResult<()> {
        let mut manager = self.manager.lock().await;

        let _response: Value = cmd("EACK")
            .arg(self.subscription_id.to_string())
            .arg(cursor)
            .query_async(&mut manager.connection)
            .await?;

        Ok(())
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
