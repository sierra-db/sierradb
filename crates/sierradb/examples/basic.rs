use sierradb::database::{DatabaseBuilder, ExpectedVersion, NewEvent, Transaction};
use sierradb::id::{uuid_to_partition_hash, uuid_v7_with_partition_hash};
use sierradb::{IterDirection, StreamId};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_dir_all("./target/db-debug");
    let db = DatabaseBuilder::new()
        .segment_size_bytes(33304748)
        .total_buckets(64)
        .bucket_ids_from_range(0..64)
        .writer_threads(8)
        .reader_threads(3)
        .open("./target/db-debug")?;

    struct AppendEvent {
        #[allow(dead_code)]
        partition_id: u16,
        stream_id: StreamId,
        expected_version: ExpectedVersion,
        event_name: String,
        timestamp: u64,
        metadata: Vec<u8>,
        payload: Vec<u8>,
    }

    let partition_id = 0;
    let stream_id = StreamId::new("\u{ffff}").unwrap();

    let events = [AppendEvent {
        partition_id,
        stream_id: stream_id.clone(),
        expected_version: ExpectedVersion::Any,
        event_name: "aa".to_string(),
        timestamp: 2305844100135687424,
        metadata: vec![],
        payload: vec![],
    }];

    let partition_key = Uuid::new_v4();
    let partition_hash = uuid_to_partition_hash(partition_key);

    let new_events: smallvec::SmallVec<[NewEvent; 4]> = events
        .iter()
        .map(|event| {
            // Ensure timestamp is valid (< 2^63)
            let timestamp = if event.timestamp >= (1u64 << 63) {
                1u64 << 62 // Use a safe timestamp if the generated one is too large
            } else {
                event.timestamp
            };

            NewEvent {
                event_id: uuid_v7_with_partition_hash(partition_hash),
                stream_id: event.stream_id.clone(),
                stream_version: event.expected_version,
                event_name: event.event_name.clone(),
                timestamp,
                metadata: event.metadata.clone(),
                payload: event.payload.clone(),
            }
        })
        .collect();

    let transaction = Transaction::new(partition_key, partition_id, new_events.clone()).unwrap();

    db.append_events(transaction).await.unwrap();

    let res = db
        .read_stream(
            partition_id,
            new_events.first().unwrap().stream_id.clone(),
            362,
            IterDirection::Reverse,
        )
        .await
        .unwrap();

    dbg!(res);

    Ok(())
}
