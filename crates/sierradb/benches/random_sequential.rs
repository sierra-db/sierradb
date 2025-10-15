use std::fs;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use criterion::{Criterion, criterion_group, criterion_main};
use rand::rng;
use rand::seq::SliceRandom;
use sierradb::StreamId;
use sierradb::bucket::segment::{
    AppendEvent, BucketSegmentReader, BucketSegmentWriter, FlushedOffset,
};
use tempfile::NamedTempFile;
use uuid::Uuid;

const NUM_EVENTS: usize = 1_000_000;
const FILE_PATH: &str = "test_segment.db";

fn setup_test_file() -> (BucketSegmentWriter, Vec<u64>) {
    let _ = fs::remove_file(FILE_PATH);
    let mut writer = BucketSegmentWriter::create(FILE_PATH, 0).expect("Failed to open writer");
    let mut offsets = Vec::with_capacity(NUM_EVENTS);

    for i in 0..NUM_EVENTS {
        let event_id = Uuid::new_v4();
        let partition_key = Uuid::new_v4();
        let transaction_id = Uuid::new_v4();
        let stream_id = StreamId::new("test-stream").unwrap();
        let event_name = "TestEvent";
        let metadata = b"{}";
        let payload = b"Some event data";

        let event = AppendEvent {
            event_id: &event_id,
            partition_key: &partition_key,
            partition_id: 0,
            partition_sequence: i as u64,
            stream_version: 0,
            stream_id: &stream_id,
            event_name,
            metadata,
            payload,
        };
        let (offset, _) = writer
            .append_event(&transaction_id, 0, 0, event)
            .expect("Failed to write event");
        offsets.push(offset);
    }

    writer.flush().expect("Failed to flush writer");
    (writer, offsets)
}

fn benchmark_reads(c: &mut Criterion) {
    let (_writer, offsets) = setup_test_file();
    let mut reader = BucketSegmentReader::open(
        FILE_PATH,
        FlushedOffset::new(Arc::new(AtomicU64::new(u64::MAX))),
    )
    .expect("Failed to open reader");

    let mut shuffled_offsets = offsets.clone();
    shuffled_offsets.shuffle(&mut rng());

    let mut group = c.benchmark_group("Event Reads");

    // Helper function to create looping iterators
    fn looping_iter(data: &[u64]) -> impl Iterator<Item = &'_ u64> {
        data.iter().cycle()
    }

    group.bench_function("Random lookup (sequential=false)", |b| {
        let mut iter = looping_iter(&shuffled_offsets);
        b.iter(|| {
            let offset = iter.next().unwrap();
            reader
                .read_record(*offset, false)
                .expect("Failed to read event");
        });
    });

    // Benchmark sequential access with `sequential = false`
    group.bench_function("Sequential lookup (sequential=true)", |b| {
        let mut iter = looping_iter(&offsets);
        b.iter(|| {
            let offset = iter.next().unwrap();
            reader
                .read_record(*offset, true)
                .expect("Failed to read event");
        });
    });

    group.finish();
}

fn benchmark_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("Writes");

    group.bench_function("Append event", |b| {
        let file = NamedTempFile::new().unwrap();
        let mut writer = BucketSegmentWriter::open(file.path()).expect("Failed to open writer");
        b.iter(|| {
            let event_id = Uuid::new_v4();
            let partition_key = Uuid::new_v4();
            let transaction_id = Uuid::new_v4();
            let stream_id = unsafe { StreamId::new_unchecked("test-stream") };
            let event_name = "TestEvent";
            let metadata = b"{}";
            let payload = b"Some event data";

            let event = AppendEvent {
                event_id: &event_id,
                partition_key: &partition_key,
                partition_id: 0,
                partition_sequence: 0,
                stream_version: 0,
                stream_id: &stream_id,
                event_name,
                metadata,
                payload,
            };
            writer
                .append_event(&transaction_id, 0, 0, event)
                .expect("Failed to write event");
        });
    });

    group.bench_function("Append commit", |b| {
        let file = NamedTempFile::new().unwrap();
        let mut writer = BucketSegmentWriter::open(file.path()).expect("Failed to open writer");
        b.iter(|| {
            let transaction_id = Uuid::new_v4();

            writer
                .append_commit(&transaction_id, 0, 0, 0)
                .expect("Failed to write commit");
        });
    });
}

criterion_group!(benches, benchmark_reads, benchmark_writes);
criterion_main!(benches);
