use criterion::{criterion_group, criterion_main, Criterion};

use eventus::{message::MessageBuf, Segment};
use testutil::TestDir;

mod testutil {
    include!("../src/testutil.rs");
}

fn bench_segment_append(c: &mut Criterion) {
    let path = TestDir::new();

    let mut seg = Segment::new(path, 100u64, 1000 * 1024 * 1024).unwrap();
    let payload = b"01234567891011121314151617181920";

    c.bench_function("segment append", |b| {
        b.iter(|| {
            let mut buf = MessageBuf::default();
            buf.push(payload).unwrap();
            seg.append(&mut buf).unwrap();
        })
    });
}

fn bench_segment_append_flush(c: &mut Criterion) {
    let path = TestDir::new();

    let mut seg = Segment::new(path, 100u64, 1000 * 1024 * 1024).unwrap();
    let payload = b"01234567891011121314151617181920";

    c.bench_function("segment append flush", |b| {
        b.iter(|| {
            let mut buf = MessageBuf::default();
            buf.push(payload).unwrap();
            seg.append(&mut buf).unwrap();
            seg.flush_sync().unwrap();
        })
    });
}

criterion_group!(benches, bench_segment_append, bench_segment_append_flush);
criterion_main!(benches);
