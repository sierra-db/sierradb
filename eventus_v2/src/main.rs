use std::{
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use eventus_v2::{
    bucket::writer_thread_pool::{AppendEventsBatch, WriteRequestEvent},
    database::DatabaseBuilder,
    id::uuid_v7_with_stream_hash,
};
use tracing::Level;
use uuid::uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .with_max_level(Level::TRACE)
        .init();

    let db = DatabaseBuilder::new("datav2")
        .flush_interval(Duration::from_millis(500))
        .num_buckets(4)
        .writer_pool_size(2)
        .reader_pool_size(2)
        .open()?;
    let db = Box::leak(Box::new(db));

    // let stream_id = "user-moira";
    // let event_id = uuid_v7_with_stream_hash(stream_id);
    // let correlation_id = uuid!("0195342a-38aa-99a7-8297-665721686848"); // uuid_v7_with_stream_hash(stream_id);
    // let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
    // db.append_events(AppendEventsBatch::single(4, WriteRequestEvent {
    //     event_id,
    //     correlation_id,
    //     stream_version: 1,
    //     timestamp,
    //     stream_id: stream_id.into(),
    //     event_name: "ActuallySuperGay".to_string(),
    //     metadata: vec![],
    //     payload: vec![1, 2, 3],
    // })?)
    // .await?;

    let event = db
        .read_event(uuid!("0195342a-38aa-b447-8297-6c9f0bb2a7b4"))
        .await?;
    dbg!(event);

    println!("====");

    let mut stream_iter = db.read_stream("user-moira").await?;
    while let Some(event) = stream_iter.next().await? {
        dbg!(event);
    }

    thread::sleep(Duration::from_secs(1));

    Ok(())
}

// use std::{
//     thread,
//     time::{Duration, Instant, SystemTime, UNIX_EPOCH},
// };

// // use crossbeam::sync::WaitGroup;
// use eventus_v2::{
//     bucket::{
//         BucketSegmentId,
//         flusher::spawn_flusher,
//         reader_thread_pool::ReaderThreadPool,
//         segment::{
//             AppendEvent, AppendEventBody, AppendEventHeader, BucketSegmentReader,
//             BucketSegmentWriter, OpenBucketSegment,
//         },
//         segment_set::OpenBucketSegmentSet,
//         writer_thread_pool::{WriteRequestEvent, WriterThreadPool},
//     },
//     id::generate_event_id,
// };
// use rand::seq::SliceRandom;
// use rand::{Rng, rng};
// use smallvec::smallvec;
// use tracing::info;
// use uuid::{Uuid, uuid};

// const NUM_EVENTS: usize = 1_000_000;

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // tracing_subscriber::fmt::init();

//     // let reader_pool = ReaderThreadPool::spawn(8);
//     let writer_pool = WriterThreadPool::new("datav2", 64, 16, Duration::from_secs(10))?;
//     // let flush_tx = spawn_flusher();
//     // let mut bucket_segment_set =
//     //     OpenBucketSegmentSet::open(BucketSegmentId::new(0, 0), ".", reader_pool, flush_tx)?;

//     // let record = segment_set.segment.read_record(48)?;
//     // dbg!(record);

//     // let event =
//     //     bucket_segment_set.read_event_by_id(&uuid!("01951d50-6cd7-0000-7f9f-515599cf309f"))?;
//     // dbg!(event);

//     // let events = segment_set.read_stream_events("hello-world")?;
//     // dbg!(events);

//     let mut rng = rand::rng();

//     for _ in 0..5_000_000 {
//         let correlation_id = Uuid::new_v4();
//         let event_id = generate_event_id(&correlation_id);
//         let transaction_id = Uuid::nil();
//         let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64;
//         let payload = (0..rng.random_range(32..300))
//             .map(|_| rng.random())
//             .collect();
//         // Submits the write to happen in the background
//         writer_pool
//             .append_events(transaction_id, smallvec![WriteRequestEvent {
//                 event_id,
//                 correlation_id,
//                 stream_version: 0,
//                 timestamp,
//                 stream_id: format!("stream-{}", rng.random::<u64>()).into(),
//                 event_name: format!("MyEvent{}", rng.random::<u64>()),
//                 metadata: vec![],
//                 payload,
//             }])
//             .await
//             .await??;
//     }

//     let start = Instant::now();

//     println!("done.. we'll sit here a moment");
//     writer_pool
//         .join()
//         .into_iter()
//         .collect::<Result<(), _>>()
//         .unwrap();

//     println!("Finished in {:?}", start.elapsed());

//     // segment_set.append_commit(&transaction_id, timestamp, 1)?;

//     // segment_set.segment.writer().flush()?;

//     // println!("{event_id}");

//     Ok(())

//     // let mut writer = BucketSegmentWriter::create("data.bin", 0, flush_tx)?;

//     // let mut offsets = Vec::with_capacity(NUM_EVENTS);

//     // for _ in 0..NUM_EVENTS {
//     //     let event_id = Uuid::new_v4();
//     //     let correlation_id = Uuid::new_v4();
//     //     let transaction_id = Uuid::new_v4();
//     //     let stream_id = "test-stream";
//     //     let event_name = "TestEvent";
//     //     let metadata = b"{}";
//     //     let payload = b"Some event data";

//     //     let body = AppendEventBody::new(stream_id, event_name, metadata, payload);
//     //     let header =
//     //         AppendEventHeader::new(&event_id, &correlation_id, &transaction_id, 1, 0, body)
//     //             .unwrap();
//     //     let event = AppendEvent::new(header, body);
//     //     let (offset, _) = writer.append_event(event).expect("Failed to write event");
//     //     offsets.push(offset);
//     // }

//     // writer.flush().expect("Failed to flush writer");

//     // let mut shuffled_offsets = offsets.clone();
//     // shuffled_offsets.shuffle(&mut rng());
//     // let mut iter = shuffled_offsets.iter().cycle();

//     // let mut reader = BucketSegmentReader::open("data.bin", writer.flushed_offset())?;

//     // let start = Instant::now();
//     // for _ in 0..1_000_000 {
//     //     reader.read_record(*iter.next().unwrap(), false)?;
//     // }
//     // println!("{:?}", start.elapsed());

//     // let reader_pool = ReaderThreadPool::spawn(8);
//     // let segment = OpenBucketSegment::new(
//     //     BucketSegmentId::new(0, 0),
//     //     reader.try_clone()?,
//     //     reader_pool,
//     //     writer,
//     // );

//     // let mut iter = shuffled_offsets.iter().cycle();

//     // // let wg = WaitGroup::new();
//     // let start = Instant::now();
//     // for _ in 0..1_000_000 {
//     //     // let wg = wg.clone();
//     //     segment.spawn_read_record(*iter.next().unwrap(), |_| {
//     //         // mem::drop(wg);
//     //     });
//     // }
//     // // wg.wait();
//     // println!("{:?}", start.elapsed());

//     // loop {
//     //     std::thread::park();
//     // }
// }
