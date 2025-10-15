use std::time::Instant;

// use crossbeam::sync::WaitGroup;
use eventus_v2::bucket::{
    flusher::spawn_flusher,
    segment::{
        AppendEvent, AppendEventBody, AppendEventHeader, BucketSegmentReader, BucketSegmentWriter,
        OpenBucketSegment, ReaderThreadPool,
    },
};
use rand::rng;
use rand::seq::SliceRandom;
use uuid::Uuid;

const NUM_EVENTS: usize = 1_000_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let flush_tx = spawn_flusher();

    let mut writer = BucketSegmentWriter::create("data.bin", 0, flush_tx)?;

    let mut offsets = Vec::with_capacity(NUM_EVENTS);

    for _ in 0..NUM_EVENTS {
        let event_id = Uuid::new_v4();
        let correlation_id = Uuid::new_v4();
        let transaction_id = Uuid::new_v4();
        let stream_id = b"test-stream";
        let event_name = b"TestEvent";
        let metadata = b"{}";
        let payload = b"Some event data";

        let header = AppendEventHeader {
            event_id: &event_id,
            correlation_id: &correlation_id,
            transaction_id: &transaction_id,
            stream_version: 1,
            timestamp: 0,
            stream_id_len: stream_id.len() as u16,
            event_name_len: event_name.len() as u16,
            metadata_len: metadata.len() as u32,
            payload_len: payload.len() as u32,
        };

        let body = AppendEventBody {
            stream_id,
            event_name,
            metadata,
            payload,
        };

        let event = AppendEvent::new(header, body);
        let (offset, _) = writer.append_event(event).expect("Failed to write event");
        offsets.push(offset);
    }

    writer.flush().expect("Failed to flush writer");

    let mut shuffled_offsets = offsets.clone();
    shuffled_offsets.shuffle(&mut rng());
    let mut iter = shuffled_offsets.iter().cycle();

    let mut reader = BucketSegmentReader::open("data.bin", writer.flushed_offset())?;

    let start = Instant::now();
    for _ in 0..1_000_000 {
        reader.read_record(*iter.next().unwrap(), false)?;
    }
    println!("{:?}", start.elapsed());

    let reader_pool = ReaderThreadPool::spawn(8);
    let segment = OpenBucketSegment::new(0, 0, reader.try_clone()?, reader_pool, writer)?;

    let mut iter = shuffled_offsets.iter().cycle();

    // let wg = WaitGroup::new();
    let start = Instant::now();
    for _ in 0..1_000_000 {
        // let wg = wg.clone();
        segment.spawn_read_record(*iter.next().unwrap(), |_| {
            // mem::drop(wg);
        });
    }
    // wg.wait();
    println!("{:?}", start.elapsed());

    loop {
        std::thread::park();
    }
}
