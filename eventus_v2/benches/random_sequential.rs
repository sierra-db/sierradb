use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;
use std::{fs, mem};

use criterion::{criterion_group, criterion_main, Criterion};
use crossbeam::sync::WaitGroup;
use eventus_v2::bucket::flusher::FlushSender;
use eventus_v2::bucket::segment::{
    AppendEvent, AppendEventBody, AppendEventHeader, BucketSegmentReader, BucketSegmentWriter,
    FlushedOffset, OpenBucketSegment, ReaderThreadPool,
};
use rand::rng;
use rand::seq::SliceRandom;
use tempfile::NamedTempFile;
use uuid::Uuid;

const NUM_EVENTS: usize = 1_000_000;
const FILE_PATH: &str = "test_segment.db";

fn setup_test_file() -> (BucketSegmentWriter, Vec<u64>) {
    let _ = fs::remove_file(FILE_PATH);
    let mut writer = BucketSegmentWriter::create(FILE_PATH, 0, FlushSender::local())
        .expect("Failed to open writer");
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
    (writer, offsets)
}

fn benchmark_reads(c: &mut Criterion) {
    let (writer, offsets) = setup_test_file();
    let mut reader = BucketSegmentReader::open(
        FILE_PATH,
        FlushedOffset::new(Arc::new(AtomicU64::new(u64::MAX))),
    )
    .expect("Failed to open reader");
    let reader_pool = ReaderThreadPool::spawn(8);
    let segment =
        OpenBucketSegment::new(0, 0, reader.try_clone().unwrap(), reader_pool, writer).unwrap();

    let mut shuffled_offsets = offsets.clone();
    shuffled_offsets.shuffle(&mut rng());

    let mut group = c.benchmark_group("Event Reads");

    // Helper function to create looping iterators
    fn looping_iter(data: &[u64]) -> impl Iterator<Item = &'_ u64> {
        data.iter().cycle()
    }

    // Benchmark random access with `sequential = false`
    group.bench_function("Random lookup (sequential=false, singlethreaded)", |b| {
        let mut iter = looping_iter(&shuffled_offsets);
        b.iter(|| {
            let offset = iter.next().unwrap();
            reader
                .read_record(*offset, false)
                .expect("Failed to read event");
        });
    });

    group.bench_function("Random lookup (sequential=false, multithreaded)", |b| {
        b.iter_custom(|iters| {
            let wg = WaitGroup::new();
            let mut iter = looping_iter(&shuffled_offsets);
            let start = Instant::now();

            for _ in 0..iters {
                let wg = wg.clone();
                let offset = iter.next().unwrap();
                segment.spawn_read_record(*offset, move |res| {
                    res.unwrap().unwrap();
                    mem::drop(wg);
                });
            }

            wg.wait();
            start.elapsed()
        });
    });

    // Benchmark random access with `sequential = true`
    // group.bench_function("Random lookup (sequential=true)", |b| {
    //     let mut iter = looping_iter(&shuffled_offsets);
    //     b.iter(|| {
    //         let offset = iter.next().unwrap();
    //         reader
    //             .read_record(*offset, true)
    //             .expect("Failed to read event");
    //     });
    // });

    // Benchmark sequential access with `sequential = false`
    // group.bench_function("Sequential lookup (sequential=false)", |b| {
    //     let mut iter = looping_iter(&offsets);
    //     b.iter(|| {
    //         let offset = iter.next().unwrap();
    //         reader
    //             .read_record(*offset, false)
    //             .expect("Failed to read event");
    //     });
    // });

    // Benchmark sequential access with `sequential = true`
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
        let mut writer = BucketSegmentWriter::open(file.path(), FlushSender::local())
            .expect("Failed to open writer");
        b.iter(|| {
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
            writer.append_event(event).expect("Failed to write event");
        });
    });

    group.bench_function("Append commit", |b| {
        let file = NamedTempFile::new().unwrap();
        let mut writer = BucketSegmentWriter::open(file.path(), FlushSender::local())
            .expect("Failed to open writer");
        b.iter(|| {
            let transaction_id = Uuid::new_v4();

            writer
                .append_commit(&transaction_id, 0, 0)
                .expect("Failed to write commit");
        });
    });
}

criterion_group!(benches, benchmark_reads, benchmark_writes);
criterion_main!(benches);
