use std::hint::black_box;
use std::path::PathBuf;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use seglog::read::{ReadHint, Reader};
use seglog::write::Writer;
use tempfile::TempDir;

const SEGMENT_SIZE: usize = 1024 * 1024 * 1024; // 1 GB

fn create_temp_segment() -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("failed to create temp dir");
    let path = dir.path().join("bench.seg");
    (dir, path)
}

// Write Benchmarks

fn bench_writer_append_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer_append_small");
    let data = vec![0u8; 1024]; // 1 KB

    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("1kb_records", |b| {
        b.iter(|| {
            let (_dir, path) = create_temp_segment();
            let mut writer =
                Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");

            for _ in 0..1000 {
                writer.append(black_box(&data)).expect("failed to append");
            }
        });
    });
    group.finish();
}

fn bench_writer_append_medium(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer_append_medium");
    let data = vec![0u8; 64 * 1024]; // 64 KB

    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("64kb_records", |b| {
        b.iter(|| {
            let (_dir, path) = create_temp_segment();
            let mut writer =
                Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");

            for _ in 0..100 {
                writer.append(black_box(&data)).expect("failed to append");
            }
        });
    });
    group.finish();
}

fn bench_writer_append_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer_append_large");
    let data = vec![0u8; 1024 * 1024]; // 1 MB

    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("1mb_records", |b| {
        b.iter(|| {
            let (_dir, path) = create_temp_segment();
            let mut writer =
                Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");

            for _ in 0..10 {
                writer.append(black_box(&data)).expect("failed to append");
            }
        });
    });
    group.finish();
}

fn bench_writer_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer_sync");

    group.bench_function("sync_after_writes", |b| {
        b.iter_with_setup(
            || {
                let (_dir, path) = create_temp_segment();
                let mut writer =
                    Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");

                // Write some data
                let data = vec![0u8; 1024];
                for _ in 0..100 {
                    writer.append(&data).expect("failed to append");
                }

                (_dir, writer)
            },
            |(_dir, mut writer)| {
                black_box(writer.sync().expect("failed to sync"));
            },
        );
    });
    group.finish();
}

fn bench_writer_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer_throughput");
    let data = vec![0u8; 4096]; // 4 KB records

    group.throughput(Throughput::Bytes((data.len() * 1000) as u64));
    group.bench_function("sequential_writes", |b| {
        b.iter(|| {
            let (_dir, path) = create_temp_segment();
            let mut writer =
                Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");

            for _ in 0..1000 {
                writer.append(black_box(&data)).expect("failed to append");
            }
            writer.sync().expect("failed to sync");
        });
    });
    group.finish();
}

// Read Benchmarks

fn bench_reader_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("reader_sequential");

    // Prepare segment with data
    let (_dir, path) = create_temp_segment();
    let mut writer = Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");
    let data = vec![0u8; 4096]; // 4 KB

    for _ in 0..1000 {
        writer.append(&data).expect("failed to append");
    }
    writer.sync().expect("failed to sync");
    let flushed = writer.flushed_offset();
    drop(writer);

    group.throughput(Throughput::Bytes((data.len() * 1000) as u64));
    group.bench_function("iterate_all_records", |b| {
        b.iter(|| {
            let mut reader =
                Reader::open(&path, Some(flushed.clone())).expect("failed to open reader");
            let mut iter = reader.iter(0);
            let mut count = 0;

            while let Some(result) = iter.next_record().transpose() {
                let (_, data) = result.expect("failed to read");
                black_box(&data);
                count += 1;
            }

            black_box(count);
        });
    });
    group.finish();
}

fn bench_reader_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("reader_random");

    // Prepare segment with data
    let (_dir, path) = create_temp_segment();
    let mut writer = Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");
    let data = vec![0u8; 4096]; // 4 KB

    let mut offsets = Vec::new();
    for _ in 0..1000 {
        let (offset, _) = writer.append(&data).expect("failed to append");
        offsets.push(offset);
    }
    writer.sync().expect("failed to sync");
    let flushed = writer.flushed_offset();
    drop(writer);

    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("random_reads", |b| {
        b.iter_with_setup(
            || Reader::open(&path, Some(flushed.clone())).expect("failed to open reader"),
            |mut reader| {
                for offset in &offsets {
                    let data = reader
                        .read_record(*offset, ReadHint::Random)
                        .expect("failed to read");
                    black_box(&data);
                }
            },
        );
    });
    group.finish();
}

fn bench_reader_sequential_hint(c: &mut Criterion) {
    let mut group = c.benchmark_group("reader_sequential_hint");

    // Prepare segment with data
    let (_dir, path) = create_temp_segment();
    let mut writer = Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");
    let data = vec![0u8; 4096]; // 4 KB

    let mut offsets = Vec::new();
    for _ in 0..1000 {
        let (offset, _) = writer.append(&data).expect("failed to append");
        offsets.push(offset);
    }
    writer.sync().expect("failed to sync");
    let flushed = writer.flushed_offset();
    drop(writer);

    group.throughput(Throughput::Bytes((data.len() * 1000) as u64));
    group.bench_function("sequential_hint_reads", |b| {
        b.iter_with_setup(
            || Reader::open(&path, Some(flushed.clone())).expect("failed to open reader"),
            |mut reader| {
                for offset in &offsets {
                    let data = reader
                        .read_record(*offset, ReadHint::Sequential)
                        .expect("failed to read");
                    black_box(&data);
                }
            },
        );
    });
    group.finish();
}

fn bench_read_small_records(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_small_records");
    let data = vec![0u8; 1024]; // 1 KB

    // Prepare segment
    let (_dir, path) = create_temp_segment();
    let mut writer = Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");

    for _ in 0..1000 {
        writer.append(&data).expect("failed to append");
    }
    writer.sync().expect("failed to sync");
    let flushed = writer.flushed_offset();
    drop(writer);

    group.throughput(Throughput::Bytes((data.len() * 1000) as u64));
    group.bench_function("1kb_records", |b| {
        b.iter(|| {
            let mut reader =
                Reader::open(&path, Some(flushed.clone())).expect("failed to open reader");
            let mut iter = reader.iter(0);

            while let Some(result) = iter.next_record().transpose() {
                let (_, data) = result.expect("failed to read");
                black_box(&data);
            }
        });
    });
    group.finish();
}

fn bench_read_large_records(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_large_records");
    let data = vec![0u8; 1024 * 1024]; // 1 MB

    // Prepare segment
    let (_dir, path) = create_temp_segment();
    let mut writer = Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");

    for _ in 0..10 {
        writer.append(&data).expect("failed to append");
    }
    writer.sync().expect("failed to sync");
    let flushed = writer.flushed_offset();
    drop(writer);

    group.throughput(Throughput::Bytes((data.len() * 10) as u64));
    group.bench_function("1mb_records", |b| {
        b.iter(|| {
            let mut reader =
                Reader::open(&path, Some(flushed.clone())).expect("failed to open reader");
            let mut iter = reader.iter(0);

            while let Some(result) = iter.next_record().transpose() {
                let (_, data) = result.expect("failed to read");
                black_box(&data);
            }
        });
    });
    group.finish();
}

fn bench_read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_throughput");
    let data = vec![0u8; 4096]; // 4 KB

    // Prepare segment
    let (_dir, path) = create_temp_segment();
    let mut writer = Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");

    for _ in 0..10000 {
        writer.append(&data).expect("failed to append");
    }
    writer.sync().expect("failed to sync");
    let flushed = writer.flushed_offset();
    drop(writer);

    group.throughput(Throughput::Bytes((data.len() * 10000) as u64));
    group.bench_function("sequential_reads", |b| {
        b.iter(|| {
            let mut reader =
                Reader::open(&path, Some(flushed.clone())).expect("failed to open reader");
            let mut iter = reader.iter(0);

            while let Some(result) = iter.next_record().transpose() {
                let (_, data) = result.expect("failed to read");
                black_box(&data);
            }
        });
    });
    group.finish();
}

// Mixed Workload Benchmarks

fn bench_iter_vs_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_vs_random");

    // Prepare segment
    let (_dir, path) = create_temp_segment();
    let mut writer = Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");
    let data = vec![0u8; 4096];

    let mut offsets = Vec::new();
    for _ in 0..1000 {
        let (offset, _) = writer.append(&data).expect("failed to append");
        offsets.push(offset);
    }
    writer.sync().expect("failed to sync");
    let flushed = writer.flushed_offset();
    drop(writer);

    group.throughput(Throughput::Bytes((data.len() * 1000) as u64));

    group.bench_function("iterator", |b| {
        b.iter(|| {
            let mut reader =
                Reader::open(&path, Some(flushed.clone())).expect("failed to open reader");
            let mut iter = reader.iter(0);

            while let Some(result) = iter.next_record().transpose() {
                let (_, data) = result.expect("failed to read");
                black_box(&data);
            }
        });
    });

    group.bench_function("random_reads", |b| {
        b.iter_with_setup(
            || Reader::open(&path, Some(flushed.clone())).expect("failed to open reader"),
            |mut reader| {
                for offset in &offsets {
                    let data = reader
                        .read_record(*offset, ReadHint::Random)
                        .expect("failed to read");
                    black_box(&data);
                }
            },
        );
    });

    group.finish();
}

fn bench_record_size_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_size_comparison");

    let sizes = vec![
        ("128b", 128),
        ("1kb", 1024),
        ("4kb", 4096),
        ("64kb", 64 * 1024),
        ("1mb", 1024 * 1024),
    ];

    for (name, size) in sizes {
        let data = vec![0u8; size];

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("write", name), &data, |b, data| {
            b.iter_with_setup(
                || {
                    let (_dir, path) = create_temp_segment();
                    let writer =
                        Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");
                    (_dir, path, writer)
                },
                |(_dir, _path, mut writer)| {
                    writer.append(black_box(data)).expect("failed to append");
                },
            );
        });

        group.bench_with_input(BenchmarkId::new("read", name), &data, |b, data| {
            b.iter_with_setup(
                || {
                    let (_dir, path) = create_temp_segment();
                    let mut writer =
                        Writer::create(&path, SEGMENT_SIZE, 0).expect("failed to create writer");
                    writer.append(data).expect("failed to append");
                    writer.sync().expect("failed to sync");
                    let flushed = writer.flushed_offset();
                    let reader = Reader::open(&path, Some(flushed)).expect("failed to open reader");
                    (_dir, reader)
                },
                |(_dir, mut reader)| {
                    let data = reader
                        .read_record(0, ReadHint::Random)
                        .expect("failed to read");
                    black_box(&data);
                },
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_writer_append_small,
    bench_writer_append_medium,
    bench_writer_append_large,
    bench_writer_sync,
    bench_writer_throughput,
    bench_reader_sequential,
    bench_reader_random,
    bench_reader_sequential_hint,
    bench_read_small_records,
    bench_read_large_records,
    bench_read_throughput,
    bench_iter_vs_random,
    bench_record_size_comparison,
);

criterion_main!(benches);
