use criterion::{criterion_group, criterion_main, Criterion};

use commitlog::message::{set_offsets, MessageBuf};

fn bench_message_construct(c: &mut Criterion) {
    c.bench_function("message construct", |b| {
        b.iter(|| {
            let mut msg_buf = MessageBuf::default();
            msg_buf
                .push(
                    "719c3b4556066a1c7a06c9d55959d003d9b4627
3aabe2eae15ef4ba78321ae2a68b0997a4abbd035a4cdbc8b27d701089a5af63a
8b81f9dc16a874d0eda0983b79c1a6f79fe3ae61612ba2558562a85595f2f3f07
fab8faba1b849685b61aad6b131b7041ca79cc662b4c5aad4d1b78fb1034fafa2
fe4f30207395e399c6d724",
                )
                .unwrap();
            msg_buf
                .push(
                    "2cea26f165640d448a9b89f1f871e6fca80a125
5b1daea6752bf99d8c5f90e706deaecddf304b2bf5a5e72e32b29bc7c54018265
d17317a670ea406fd7e6b485a19f5fb1efe686badb6599d45106b95b55695cd4e
24729edb312a5dec1bc80e8d8b3ee4b69af1f3a9c801e7fb527e65f7c13c62bb3
7261c0",
                )
                .unwrap();
            set_offsets(&mut msg_buf, 1250);
        })
    });
}

criterion_group!(benches, bench_message_construct);
criterion_main!(benches);
