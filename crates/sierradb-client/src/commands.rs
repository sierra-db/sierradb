use redis::{ConnectionLike, ToRedisArgs, cmd};
use uuid::Uuid;

use crate::options::{EAppendOptions, EMAppendEvent};
use crate::types::{AppendInfo, Event, EventBatch, MultiAppendInfo, RangeValue, SubscriptionInfo};

implement_commands! {
    'a

    /// Append an event to a stream.
    ///
    /// # Parameters
    /// - `stream_id`: Stream identifier to append the event to
    /// - `event_name`: Name/type of the event
    /// - `options`: Configuration for optional parameters (event_id, partition_key, expected_version, payload, metadata)
    ///
    /// # Example
    /// ```ignore
    /// let options = EAppendOptions::new()
    ///     .payload(br#"{"name":"john"}"#)
    ///     .metadata(br#"{"source":"api"}"#);
    /// conn.eappend("my-stream", "UserCreated", options)?;
    /// ```
    ///
    /// # Returns
    /// Returns an `AppendInfo` containing event metadata.
    fn eappend<S: ToRedisArgs, E: ToRedisArgs>(stream_id: S, event_name: E, options: EAppendOptions<'a>) -> (AppendInfo) {
        cmd("EAPPEND").arg(stream_id).arg(event_name).arg(options)
    }

    /// Append multiple events to streams in a single transaction.
    ///
    /// # Parameters
    /// - `partition_key`: UUID that determines which partition all events will be written to
    /// - `events`: Array of events to append, each with their own configuration
    ///
    /// # Example
    /// ```ignore
    /// let events = [
    ///     EMAppendEvent::new("stream1", "EventA").payload(br#"{"data":"value1"}"#),
    ///     EMAppendEvent::new("stream2", "EventB").payload(br#"{"data":"value2"}"#),
    /// ];
    /// conn.emappend(partition_key, &events)?;
    /// ```
    ///
    /// **Note:** All events are appended atomically in a single transaction.
    ///
    /// # Returns
    /// Returns transaction result information.
    fn emappend<>(partition_key: Uuid, events: &'a [EMAppendEvent<'a>]) -> (MultiAppendInfo) {
        cmd("EMAPPEND").arg(partition_key.to_string()).arg(events)
    }

    /// Get an event by its unique identifier.
    ///
    /// # Parameters
    /// - `event_id`: UUID of the event to retrieve
    ///
    /// # Example
    /// ```ignore
    /// let event_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    /// let event = conn.eget(event_id)?;
    /// ```
    ///
    /// # Returns
    /// Returns `Some(Event)` if found, `None` if the event doesn't exist.
    fn eget<>(event_id: Uuid) -> (Option<Event>) {
        cmd("EGET").arg(event_id.to_string())
    }

    /// Scan events in a partition by partition key.
    ///
    /// # Parameters
    /// - `partition_key`: UUID key to determine which partition to scan
    /// - `start_sequence`: Starting sequence number
    /// - `end_sequence`: Ending sequence number (`None` means scan to end)
    /// - `count`: Maximum number of events to return (defaults to 100)
    ///
    /// # Example
    /// ```ignore
    /// let partition_key = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    /// let batch = conn.epscan_by_key(partition_key, 0, Some(100), Some(50))?;
    /// ```
    ///
    /// # Returns
    /// Returns an `EventBatch` containing events and pagination info.
    fn epscan_by_key<>(partition_key: Uuid, start_sequence: u64, end_sequence: Option<u64>, count: Option<u64>) -> (EventBatch) {
        cmd("EPSCAN").arg(partition_key.to_string()).arg(start_sequence).arg(match end_sequence {
            Some(seq) => RangeValue::Value(seq),
            None => RangeValue::End,
        }).arg("COUNT").arg(count.unwrap_or(100))
    }

    /// Scan events in a partition by partition ID.
    ///
    /// # Parameters
    /// - `partition_id`: Numeric partition identifier (0-65535)
    /// - `start_sequence`: Starting sequence number
    /// - `end_sequence`: Ending sequence number (`None` means scan to end)
    /// - `count`: Maximum number of events to return (defaults to 100)
    ///
    /// # Example
    /// ```ignore
    /// let batch = conn.epscan_by_id(42, 100, Some(200), Some(50))?;
    /// ```
    ///
    /// # Returns
    /// Returns an `EventBatch` containing events and pagination info.
    fn epscan_by_id<>(partition_id: u16, start_sequence: u64, end_sequence: Option<u64>, count: Option<u64>) -> (EventBatch) {
        cmd("EPSCAN").arg(partition_id).arg(start_sequence).arg(match end_sequence {
            Some(seq) => RangeValue::Value(seq),
            None => RangeValue::End,
        }).arg("COUNT").arg(count.unwrap_or(100))
    }

    /// Scan events in a stream by stream ID.
    ///
    /// # Parameters
    /// - `stream_id`: Stream identifier to scan
    /// - `start_version`: Starting version number
    /// - `end_version`: Ending version number (`None` means scan to end)
    /// - `count`: Maximum number of events to return (defaults to 100)
    ///
    /// # Example
    /// ```ignore
    /// let events = conn.escan("my-stream", 0, Some(100), Some(50))?;
    /// ```
    ///
    /// # Returns
    /// Returns events from the stream across all partitions.
    fn escan<>(stream_id: &'a str, start_version: u64, end_version: Option<u64>, count: Option<u64>) -> (EventBatch) {
        cmd("ESCAN").arg(stream_id).arg(start_version).arg(match end_version {
            Some(seq) => RangeValue::Value(seq),
            None => RangeValue::End,
        }).arg("COUNT").arg(count.unwrap_or(100))
    }

    /// Scan events in a stream by stream ID within a specific partition.
    ///
    /// # Parameters
    /// - `stream_id`: Stream identifier to scan
    /// - `partition_key`: UUID to scan within a specific partition only
    /// - `start_version`: Starting version number
    /// - `end_version`: Ending version number (`None` means scan to end)
    /// - `count`: Maximum number of events to return (defaults to 100)
    ///
    /// # Example
    /// ```ignore
    /// let partition_key = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    /// let batch = conn.escan_with_partition_key("my-stream", partition_key, 0, Some(100), Some(50))?;
    /// ```
    ///
    /// # Returns
    /// Returns an `EventBatch` containing events from the specified partition only.
    fn escan_with_partition_key<>(stream_id: &'a str, partition_key: Uuid, start_version: u64, end_version: Option<u64>, count: Option<u64>) -> (EventBatch) {
        cmd("ESCAN").arg(stream_id).arg(start_version).arg(match end_version {
            Some(seq) => RangeValue::Value(seq),
            None => RangeValue::End,
        }).arg("COUNT").arg(count.unwrap_or(100)).arg("PARTITION_KEY").arg(partition_key.to_string())
    }

    /// Get the current sequence number for a partition by partition key.
    ///
    /// # Parameters
    /// - `partition_key`: UUID key to determine which partition to query
    ///
    /// # Example
    /// ```ignore
    /// let partition_key = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    /// let sequence = conn.epseq_by_key(partition_key)?;
    /// ```
    ///
    /// # Returns
    /// Returns the current sequence number for the partition.
    fn epseq_by_key<>(partition_key: Uuid) -> (u64) {
        cmd("EPSEQ").arg(partition_key.to_string())
    }

    /// Get the current sequence number for a partition by partition ID.
    ///
    /// # Parameters
    /// - `partition_id`: Numeric partition identifier (0-65535)
    ///
    /// # Example
    /// ```ignore
    /// let sequence = conn.epseq_by_id(42)?;
    /// ```
    ///
    /// # Returns
    /// Returns the current sequence number for the partition.
    fn epseq_by_id<>(partition_id: u16) -> (u64) {
        cmd("EPSEQ").arg(partition_id)
    }

    /// Get the current version number for a stream.
    ///
    /// # Parameters
    /// - `stream_id`: Stream identifier to get version for
    ///
    /// # Example
    /// ```ignore
    /// let version = conn.esver("my-stream")?;
    /// ```
    ///
    /// # Returns
    /// Returns the current version number for the stream.
    fn esver<>(stream_id: &'a str) -> (u64) {
        cmd("ESVER").arg(stream_id)
    }

    /// Get the current version number for a stream within a specific partition.
    ///
    /// # Parameters
    /// - `stream_id`: Stream identifier to get version for
    /// - `partition_key`: UUID to check specific partition
    ///
    /// # Example
    /// ```ignore
    /// let partition_key = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    /// let version = conn.esver_with_partition_key("my-stream", partition_key)?;
    /// ```
    ///
    /// # Returns
    /// Returns the current version number for the stream in the specified partition.
    fn esver_with_partition_key<>(stream_id: &'a str, partition_key: Uuid) -> (u64) {
        cmd("ESVER").arg(stream_id).arg("PARTITION_KEY").arg(partition_key.to_string())
    }

    /// Subscribe to receive events from the event store.
    ///
    /// # Example
    /// ```ignore
    /// let subscription = conn.esub()?;
    /// ```
    ///
    /// # Returns
    /// Returns subscription information.
    ///
    /// **Note:** Establishes a persistent connection to receive real-time events.
    fn esub<>() -> (SubscriptionInfo) {
        &mut cmd("ESUB")
    }
}

impl<T> Commands for T where T: ConnectionLike {}

impl<T> TypedCommands for T where T: ConnectionLike {}

#[cfg(feature = "aio")]
impl<T> AsyncCommands for T where T: redis::aio::ConnectionLike + Send + Sync + Sized {}

#[cfg(feature = "aio")]
impl<T> AsyncTypedCommands for T where T: redis::aio::ConnectionLike + Send + Sync + Sized {}
