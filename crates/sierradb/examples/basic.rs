use std::time::Duration;

use sierradb::database::DatabaseBuilder;
use smallvec::smallvec;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DatabaseBuilder::new()
        // .segment_size(8 * 1024)
        .total_buckets(64)
        // .bucket_ids_from_range(0..4)
        .reader_threads(8)
        .writer_threads(4)
        .flush_interval_duration(Duration::MAX)
        .flush_interval_events(1)
        .open("./target/db")?;

    db.set_confirmations(225, smallvec![344], Uuid::nil(), 2)
        .await?;

    // let mut events = db.read_stream(0, StreamId::new("user-123")?).await?;
    // while let Some(event) = events.next().await? {
    //     dbg!(event);
    // }

    // let partition_key = Uuid::new_v4();
    // let partition_hash = uuid_to_partition_hash(partition_key);
    // let partition_id = partition_hash % 4; // using 4 arbitrarily here for number
    // of buckets for i in 0..100 {
    //     let batch = Transaction::new(
    //         partition_hash,
    //         smallvec![NewEvent {
    //             event_id: Uuid::new_v4(),
    //             partition_key,
    //             partition_id: 0,
    //             stream_id: StreamId::new("user-456")?,
    //             stream_version: ExpectedVersion::Any,
    //             event_name: "MyEvent".to_string(),
    //             timestamp: i,
    //             metadata: vec![1, 2, 3],
    //             payload: b"Hello!!!".to_vec(),
    //         }],
    //     )?;
    //     let res = db.append_events(partition_id, batch).await?;

    //     dbg!(res.offsets);
    // }

    Ok(())
}
