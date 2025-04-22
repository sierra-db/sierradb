use std::time::{Duration, Instant};

use clap::Parser;
use klio_core::StreamId;
use klio_core::bucket::BucketId;
use klio_core::bucket::segment::EventRecord;
use klio_core::database::{DatabaseBuilder, ExpectedVersion};
use klio_core::writer_thread_pool::{AppendEventsBatch, WriteEventRequest};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Distributed event store
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Address to listen on
    #[arg(long)]
    listen_addr: String,

    /// Total number of buckets
    #[arg(long)]
    num_buckets: u16,

    /// Buckets range
    #[arg(long)]
    buckets_min: BucketId,

    /// Buckets range
    #[arg(long)]
    buckets_max: BucketId,

    /// Buckets range
    #[arg(long)]
    replication_factor: u8,

    /// Buckets range
    #[arg(long)]
    writer_pool_num_threads: u16,

    /// Buckets range
    #[arg(long, default_value_t = 8)]
    read_pool_num_threads: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .without_time()
        .with_env_filter(EnvFilter::new("WARN"))
        .init();

    // let args = Args::parse();

    let db = DatabaseBuilder::new("target/fuzz_db")
        // .segment_size(100_000_000)
        // .total_buckets(args.num_buckets)
        // .bucket_ids_from_range(args.buckets_min..=args.buckets_max)
        // // .replication_factor(args.replication_factor)
        // .writer_pool_num_threads(args.writer_pool_num_threads)
        // .reader_pool_num_threads(args.read_pool_num_threads)
        // .flush_interval_duration(Duration::MAX)
        // .flush_interval_events(1) // Flush every event written
        .segment_size(64 * 1024 * 1024)
        .total_buckets(4)
        .bucket_ids_from_range(0..4)
        .writer_pool_num_threads(4)
        .reader_pool_num_threads(4)
        .flush_interval_duration(Duration::MAX)
        .flush_interval_events(1) // Flush every event written
        .open()?;

    // let event_id = Uuid::new_v4();
    // db.append_events(
    //     0,
    //     AppendEventsBatch::single(WriteEventRequest {
    //         event_id,
    //         partition_key: Uuid::nil(),
    //         partition_id: 0,
    //         stream_id: StreamId::new("my-stream")?,
    //         stream_version: ExpectedVersion::NoStream,
    //         event_name: "FirstEvent".to_string(),
    //         timestamp: 1233456,
    //         metadata: vec![1, 2, 3],
    //         payload: b"my payload".to_vec(),
    //     })?,
    // )
    // .await?;

    // let event_name = db
    //     .read_event(0, event_id)
    //     .await
    //     .unwrap()
    //     .unwrap()
    //     .event_name;
    // println!("Read event: {event_name}");

    // let event_id = Uuid::new_v4();
    // db.append_events(
    //     0,
    //     AppendEventsBatch::single(WriteEventRequest {
    //         event_id,
    //         partition_key: Uuid::nil(),
    //         partition_id: 0,
    //         stream_id: StreamId::new("my-other-stream")?,
    //         stream_version: ExpectedVersion::NoStream,
    //         event_name: "SecondEvent".to_string(),
    //         timestamp: 1233456,
    //         metadata: vec![1, 2, 3],
    //         payload: b"my payload".to_vec(),
    //     })?,
    // )
    // .await?;

    // let event_name = db
    //     .read_event(0, event_id)
    //     .await
    //     .unwrap()
    //     .unwrap()
    //     .event_name;
    // println!("Read event: {event_name}");

    // let event_id = Uuid::new_v4();
    // db.append_events(
    //     0,
    //     AppendEventsBatch::single(WriteEventRequest {
    //         event_id,
    //         partition_key: Uuid::nil(),
    //         partition_id: 0,
    //         stream_id: StreamId::new("my-stream")?,
    //         stream_version: ExpectedVersion::Exact(0),
    //         event_name: "ThirdEvent".to_string(),
    //         timestamp: 1233456,
    //         metadata: vec![1, 2, 3],
    //         payload: b"my payload".to_vec(),
    //     })?,
    // )
    // .await?;

    // let event_name = db
    //     .read_event(0, event_id)
    //     .await
    //     .unwrap()
    //     .unwrap()
    //     .event_name;
    // println!("Read event: {event_name}");

    // println!("\n-------\n");

    // let mut events = db.read_stream(3, StreamId::new("￿򏿿򭴠")?).await?;
    // let mut count = 0;
    // let mut total_len = 0;
    // let start = Instant::now();
    // while let Some(event) = events.next().await? {
    //     count += 1;
    //     total_len += event.len();
    //     // let EventRecord {
    //     //     partition_id,
    //     //     partition_sequence,
    //     //     stream_version,
    //     //     stream_id,
    //     //     event_name,
    //     //     ..
    //     // } = event;
    //     // println!(
    //     //     "[p:{partition_id}/{partition_sequence}]
    //     // [s:{stream_id}/{stream_version}] {event_name}", );
    // }

    let mut count = 0;
    let mut total_len = 0;
    let start = Instant::now();
    for partition_id in 0..4 {
        let mut events = db.read_partition(partition_id).await?;
        while let Some(event) = events.next().await? {
            count += 1;
            total_len += event.len();
            //         println!(
            //             "[p:{partition_id}/{partition_sequence}]
            // [s:{stream_id}/{stream_version}] {event_name}",
            //         );
        }
    }

    let end = start.elapsed();
    println!("Took {end:?}");
    println!("COUNT IS {count}");
    println!("Total size is {total_len}");

    Ok(())
}
