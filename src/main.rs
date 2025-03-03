use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use eventus_v2::{
    bucket::writer_thread_pool::{AppendEventsBatch, WriteEventRequest},
    database::{DatabaseBuilder, ExpectedVersion, Quorum},
    id::uuid_v7_with_stream_hash,
};
use tokio::sync::Semaphore;
use tracing::{Level, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .with_max_level(Level::INFO)
        .init();

    let db = DatabaseBuilder::new("target/db")
        .segment_size(100_000_000)
        .num_buckets(4)
        .replication_factor(3)
        .writer_pool_num_threads(4)
        .reader_pool_num_threads(8)
        .flush_interval_duration(Duration::MAX)
        .flush_interval_events(1) // Flush every event written
        .open()?;

    // Pre-generate 10,000 stream_id and partition_key pairs.
    let num_streams = 10_000;
    let mut streams = Vec::with_capacity(num_streams);
    for i in 0..num_streams {
        let stream_id = Arc::from(format!("stream-{:05}", i));
        let partition_key = uuid_v7_with_stream_hash(&stream_id);
        streams.push((stream_id, partition_key));
    }

    // Total number of writes to perform.
    let num_writes = 1_000;
    let start = Instant::now();

    // Use FuturesUnordered for concurrent asynchronous writes.
    // let mut futures = FuturesUnordered::new();
    let semaphore = Arc::new(Semaphore::new(num_writes));
    for i in 0..num_writes {
        // Round-robin through the 10,000 pre-generated stream/partition pairs.
        let (stream_id, partition_key) = streams[i % num_streams].clone();
        let event_id = uuid_v7_with_stream_hash(&stream_id);
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
        let db = db.clone();
        let permit = Arc::clone(&semaphore).try_acquire_owned().unwrap();
        tokio::spawn(async move {
            let req = WriteEventRequest {
                event_id,
                partition_key,
                stream_version: ExpectedVersion::Any,
                timestamp,
                stream_id: stream_id.clone(),
                event_name: "SomeEvent".to_string(),
                metadata: vec![],
                payload: vec![1, 2, 3],
            };
            let res = db
                .append_events(
                    Arc::new(AppendEventsBatch::single(req).unwrap()),
                    Quorum::Majority,
                )
                .await;
            if let Err(err) = res.into_quorum_result() {
                error!("{err}");
            }

            let _permit = permit;
        });

        // Spawn the async write and add it to the unordered set.
        // futures.push();
    }

    // Await all write futures.
    // while let Some(result) = futures.next().await {
    //     // Handle errors as needed.
    //     result?;
    // }
    let _ = semaphore.acquire_many(num_writes as u32).await.unwrap();

    let elapsed = start.elapsed();
    println!("Benchmark completed in: {:?}", elapsed);
    Ok(())

    // let stream_id = "user-moira";
    // let partition_key = uuid!("01954393-2f13-ae47-8297-67a33f5422ab"); // uuid_v7_with_stream_hash(stream_id);

    // let start = Instant::now();
    // for i in 0..100_000 {
    //     let event_id = uuid_v7_with_stream_hash(stream_id);
    //     let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
    //     db.append_events(AppendEventsBatch::single(WriteEventRequest {
    //         event_id,
    //         partition_key,
    //         stream_version: ExpectedVersion::Any,
    //         timestamp,
    //         stream_id: stream_id.into(),
    //         event_name: "ActuallySuperGay".to_string(),
    //         metadata: vec![],
    //         payload: vec![1, 2, 3],
    //     })?)
    //     .await?;
    // }
    // let end = start.elapsed();
    // println!("{end:?}");
    // thread::sleep(Duration::from_secs(1));

    // let latest = db
    //     .read_stream_latest_version(&Arc::from(stream_id), extract_stream_hash(partition_key))
    //     .await?;
    // dbg!(latest);

    // let event = db.read_event(event_id).await?;
    // dbg!(event);

    // println!("====");

    // let mut stream_iter = db.read_stream("user-moira").await?;
    // while let Some(event) = stream_iter.next().await? {
    //     dbg!(event.stream_version);
    // }

    // thread::sleep(Duration::from_secs(1));

    // Ok(())
}
