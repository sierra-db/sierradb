use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use eventus::StreamIndex;
use testutil::TestDir;

mod testutil {
    include!("../src/testutil.rs");
}

const NUM_ENTRIES: u64 = 1000;

fn fixed_size_hashmap_insert_benchmark(c: &mut Criterion) {
    c.bench_function("fixed_size_hashmap_insert", |b| {
        b.iter_batched(
            || {
                let temp_dir = TestDir::new();
                StreamIndex::new(temp_dir.as_ref(), 0, NUM_ENTRIES).unwrap()
            },
            |mut map| {
                for i in 0..NUM_ENTRIES {
                    let key = format!("key_{:0>8}", i);
                    map.insert(&key, i as u64).unwrap();
                }
                map.flush_sync().unwrap();
            },
            BatchSize::SmallInput,
        )
    });
}

fn fixed_size_hashmap_get_benchmark(c: &mut Criterion) {
    c.bench_function("fixed_size_hashmap_get", |b| {
        b.iter_batched(
            || {
                let temp_dir = TestDir::new();
                let mut map = StreamIndex::new(temp_dir.as_ref(), 0, NUM_ENTRIES).unwrap();
                for i in 0..NUM_ENTRIES {
                    let key = format!("key_{:0>8}", i);
                    map.insert(&key, i as u64).unwrap();
                }
                map.flush_sync().unwrap();
                map
            },
            |mut map| {
                for i in 0..NUM_ENTRIES {
                    let key = format!("key_{:0>8}", i);
                    let values = map.get(&key).unwrap();
                    assert_eq!(values, vec![i as u64]);
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    fixed_size_hashmap_insert_benchmark,
    fixed_size_hashmap_get_benchmark
);
criterion_main!(benches);
