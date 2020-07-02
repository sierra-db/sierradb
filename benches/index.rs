use criterion::{black_box, criterion_group, criterion_main, Criterion};

use commitlog::index::{Index, IndexBuf};
use testutil::TestDir;

mod testutil {
    include!("../src/testutil.rs");
}

fn bench_find_exact(c: &mut Criterion) {
    let dir = TestDir::new();
    let mut index = Index::new(&dir, 10u64, 9000usize).unwrap();

    for i in 0..10 {
        let mut buf = IndexBuf::new(20, 10u64);
        for j in 0..200 {
            let off = 10u32 + (i * j);
            buf.push(off as u64, off);
        }
        index.append(buf).unwrap();
    }

    index.flush_sync().unwrap();
    c.bench_function("find extract", |b| {
        b.iter(|| {
            index.find(black_box(943)).unwrap();
        })
    });
}

fn bench_insert_flush(c: &mut Criterion) {
    let dir = TestDir::new();
    let mut index = Index::new(&dir, 10u64, 9000usize).unwrap();

    c.bench_function("insert flush", |b| {
        b.iter(|| {
            let mut buf = IndexBuf::new(20, 10u64);
            for j in 0..20 {
                let off = 10u32 + j;
                buf.push(off as u64, off);
            }
            index.append(buf).unwrap();
            index.flush_sync().unwrap();
        })
    });
}

criterion_group!(benches, bench_find_exact, bench_insert_flush);
criterion_main!(benches);
