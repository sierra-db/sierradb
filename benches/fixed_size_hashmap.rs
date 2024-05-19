use std::{fs, time::Instant};

use commitlog::FixedSizeHashMap;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use testutil::TestDir;

mod testutil {
    include!("../src/testutil.rs");
}

const NUM_ENTRIES: usize = 1000;

fn fixed_size_hashmap_insert_benchmark(c: &mut Criterion) {
    c.bench_function("fixed_size_hashmap_insert", |b| {
        b.iter_batched(
            || {
                let temp_dir = TestDir::new();
                let index_path = temp_dir.as_ref().join("index.dat");
                let data_path = temp_dir.as_ref().join("data.dat");

                let mut map = FixedSizeHashMap::new(&index_path, &data_path, NUM_ENTRIES).unwrap();
                map.load_next_index_offset().unwrap();
                map
            },
            |mut map| {
                for i in 0..NUM_ENTRIES {
                    let key = format!("key_{:0>8}", i);
                    map.insert(key.as_bytes(), i as u64).unwrap();
                }
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
                let index_path = temp_dir.as_ref().join("index.dat");
                let data_path = temp_dir.as_ref().join("data.dat");

                let mut map = FixedSizeHashMap::new(&index_path, &data_path, NUM_ENTRIES).unwrap();
                map.load_next_index_offset().unwrap();
                map
            },
            |mut map| {
                for i in 0..NUM_ENTRIES {
                    let key = format!("key_{:0>8}", i);
                    map.insert(key.as_bytes(), i as u64).unwrap();
                }

                for i in 0..NUM_ENTRIES {
                    let key = format!("key_{:0>8}", i);
                    let values = map.get(key.as_bytes()).unwrap();
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
