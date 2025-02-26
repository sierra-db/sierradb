use std::{
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use eventus_v2::{
    bucket::writer_thread_pool::{AppendEventsBatch, WriteRequestEvent},
    database::DatabaseBuilder,
    id::uuid_v7_with_stream_hash,
};
use tracing::Level;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .with_max_level(Level::INFO)
        .init();

    let db = DatabaseBuilder::new("target/db")
        .segment_size(64_000_000)
        .num_buckets(16)
        .writer_pool_size(16)
        .reader_pool_size(8)
        .flush_interval(Duration::from_millis(400))
        .open()?;

    let stream_id = "user-moira";
    let partition_key = uuid_v7_with_stream_hash(stream_id);

    let start = Instant::now();
    for i in 0..100_000 {
        let event_id = uuid_v7_with_stream_hash(stream_id);
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
        db.append_events(AppendEventsBatch::single(WriteRequestEvent {
            event_id,
            partition_key,
            stream_version: i,
            timestamp,
            stream_id: stream_id.into(),
            event_name: "ActuallySuperGay".to_string(),
            metadata: vec![],
            payload: vec![1, 2, 3],
        })?)
        .await?;
    }
    let end = start.elapsed();
    println!("{end:?}");
    thread::sleep(Duration::from_secs(1));

    // let event = db.read_event(event_id).await?;
    // dbg!(event);

    println!("====");

    // let mut stream_iter = db.read_stream("user-moira").await?;
    // while let Some(event) = stream_iter.next().await? {
    //     dbg!(event.stream_version);
    // }

    // thread::sleep(Duration::from_secs(1));

    Ok(())
}
