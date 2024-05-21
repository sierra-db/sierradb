use criterion::{criterion_group, criterion_main, Criterion};

use commitlog::{message::MessageBuf, CommitLog, LogOptions};
use testutil::TestDir;

mod testutil {
    include!("../src/testutil.rs");
}

fn commitlog_append_10000(c: &mut Criterion) {
    let dir = TestDir::new();
    let mut log = CommitLog::new(LogOptions::new(&dir)).unwrap();

    c.bench_function("append 10000", |b| {
        b.iter(|| {
            for _ in 0..10_000 {
                log.append_msg(
                    "my_stream",
                    "719c3b4556066a1c7a06c9d55959d003d9b46273aabe2 \
                 eae15ef4ba78321ae2a68b0997a4abbd035a4cdbc8b27d701089a5af63a8b \
                 81f9dc16a874d0eda0983b79c1a6f79fe3ae61612ba2558562a85595f2f3f \
                 07fab8faba1b849685b61aad6b131b7041ca79cc662b4c5aad4d1b78fb103 \
                 4fafa2fe4f30207395e399c6d724",
                )
                .unwrap();
            }
            log.flush().unwrap();
        })
    });
}

fn commitlog_append_10000_batched(c: &mut Criterion) {
    let dir = TestDir::new();
    let mut log = CommitLog::new(LogOptions::new(&dir)).unwrap();

    c.bench_function("append 10000 batched", |b| {
        b.iter(|| {
            let mut buf = MessageBuf::default();
            for _ in 0..200 {
                for _ in 0..50 {
                    buf.push(
                        "719c3b4556066a1c7a06c9d55959d003d9b46273aabe2 \
                     eae15ef4ba78321ae2a68b0997a4abbd035a4cdbc8b27d701089a5af63a8b \
                     81f9dc16a874d0eda0983b79c1a6f79fe3ae61612ba2558562a85595f2f3f \
                     07fab8faba1b849685b61aad6b131b7041ca79cc662b4c5aad4d1b78fb103 \
                     4fafa2fe4f30207395e399c6d724",
                    )
                    .unwrap();
                }
                log.append("my_stream", &mut buf).unwrap();
                unsafe {
                    buf.unsafe_clear();
                }
            }
            log.flush().unwrap();
        })
    });
}

criterion_group!(
    benches,
    commitlog_append_10000,
    commitlog_append_10000_batched
);
criterion_main!(benches);
