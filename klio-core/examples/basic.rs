use std::time::Duration;

use klio_core::StreamId;
use klio_core::database::{DatabaseBuilder, ExpectedVersion};
use klio_core::writer_thread_pool::{AppendEventsBatch, WriteEventRequest};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DatabaseBuilder::new("./target/db-temp")
        .segment_size(8 * 1024)
        .total_buckets(4)
        .bucket_ids_from_range(0..4)
        .reader_pool_num_threads(8)
        .writer_pool_num_threads(4)
        .flush_interval_duration(Duration::MAX)
        .flush_interval_events(1)
        .open()?;

    // let mut events = db.read_stream(0, StreamId::new("user-123")?).await?;
    // while let Some(event) = events.next().await? {
    //     dbg!(event);
    // }

    let partition_key = Uuid::new_v4();
    for i in 0..100 {
        let res = db
            .append_events(
                0,
                AppendEventsBatch::single(WriteEventRequest {
                    event_id: Uuid::new_v4(),
                    partition_key,
                    partition_id: 0,
                    stream_id: StreamId::new("user-456")?,
                    stream_version: ExpectedVersion::Any,
                    event_name: "MyEvent".to_string(),
                    timestamp: i,
                    metadata: vec![1, 2, 3],
                    payload: b"Hello!!!".to_vec(),
                })?,
            )
            .await?;

        dbg!(res.offsets);
    }

    Ok(())
}
