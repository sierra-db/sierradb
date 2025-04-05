use std::{
    io,
    ops::Range,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, builder::ValueRange};
use eventus_v2::{
    bucket::{
        BucketId,
        writer_thread_pool::{AppendEventsBatch, WriteEventRequest},
    },
    database::{DatabaseBuilder, ExpectedVersion},
    id::{id_to_partition, uuid_v7_with_stream_hash},
    swarm::{BucketOwnerMsg, Swarm},
};
use libp2p::{
    gossipsub::{self, TopicHash},
    identity::Keypair,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    select, signal,
    sync::Semaphore,
};
use tracing::{Level, error};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Distributed event store
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
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
        // .with_max_level(Level::INFO)
        .without_time()
        // .with_env_filter(EnvFilter::new("INFO,kameo=TRACE,eventus_v2=INFO"))
        .with_env_filter(EnvFilter::new("WARN"))
        .init();

    let args = Args::parse();

    let db = DatabaseBuilder::new("target/db")
        .segment_size(100_000_000)
        .bucket_ids_from_range(args.buckets_min..=args.buckets_max)
        .replication_factor(args.replication_factor)
        .writer_pool_num_threads(args.writer_pool_num_threads)
        .reader_pool_num_threads(args.read_pool_num_threads)
        .flush_interval_duration(Duration::MAX)
        .flush_interval_events(1) // Flush every event written
        .open()?;

    let mut swarm = Swarm::new(
        Keypair::generate_ed25519(),
        db,
        args.num_buckets,
        args.replication_factor,
        (args.buckets_min..=args.buckets_max).collect(),
    )?;

    swarm.listen_on(args.listen_addr.parse()?)?;

    // let mut pause_fut = Box::pin(pause());
    // loop {
    //     select! {
    //         _ = &mut pause_fut => break,
    //         _ = swarm.next() => {}
    //     }
    // }

    // let bucket_owners_data = bincode::serde::encode_to_vec(
    //     &BucketOwnerMsg {
    //         bucket_ids: (args.buckets_min..=args.buckets_max).collect(),
    //         peer_id: *swarm.swarm.local_peer_id(),
    //     },
    //     bincode::config::standard(),
    // )?;
    // if let Err(err) = swarm.swarm.behaviour_mut().gossipsub.publish(
    //     gossipsub::IdentTopic::new("bucket_owners"),
    //     bucket_owners_data,
    // ) {
    //     error!("failed to publish gossip: {err}");
    // }

    let swarm_tx = swarm.spawn();

    signal::ctrl_c().await?;
    std::process::exit(0);

    pause().await?;

    let partition_key = uuid::uuid!("2aeb2bf0-dde7-4b73-9cf7-b37e09ecb896");
    // let partition_key = Uuid::new_v4();
    println!("{partition_key}");
    let event_id = Uuid::new_v4();
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
    let req = WriteEventRequest {
        event_id,
        partition_key,
        stream_version: ExpectedVersion::Any,
        timestamp,
        stream_id: Arc::from("my-stream5"),
        event_name: "SomeEvent".to_string(),
        metadata: vec![],
        payload: vec![1, 2, 3],
    };
    swarm_tx
        .append_events(
            id_to_partition(partition_key),
            Arc::new(AppendEventsBatch::single(req)?),
        )?
        .await?;

    return Ok(());

    /*
    // Pre-generate 10,000 stream_id and partition_key pairs.
    let num_streams = 10_000;
    let mut streams = Vec::with_capacity(num_streams);
    for i in 0..num_streams {
        let stream_id = Arc::from(format!("stream-{i:05}"));
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
                    bucket_ids[id_to_partition(partition_key) as usize % bucket_ids.len()],
                    Arc::new(AppendEventsBatch::single(req).unwrap()),
                )
                .await;
            if let Err(err) = res {
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
    */
}

async fn pause() -> tokio::io::Result<()> {
    let mut stdout = tokio::io::stdout();
    stdout.write_all(b"Press any key to continue...").await?;
    stdout.flush().await?;

    let mut stdin = tokio::io::stdin();
    let mut buffer = [0; 1];
    stdin.read_exact(&mut buffer).await?;

    Ok(())
}
