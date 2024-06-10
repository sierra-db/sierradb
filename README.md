# Commit Log

Sequential, disk-backed commit log library for Rust. The library can be used in various higher-level distributed abstractions on top of a distributed log such as [Paxos](https://github.com/zowens/paxos-rs), [Chain Replication](https://github.com/zowens/chain-replication) or Raft.

[![Crates.io](https://img.shields.io/crates/v/commitlog.svg?maxAge=2592000)](https://crates.io/crates/commitlog)
[![Docs.rs](https://docs.rs/commitlog/badge.svg)](https://docs.rs/commitlog/)
[![Travis](https://travis-ci.org/zowens/commitlog.svg?branch=master)](https://travis-ci.org/zowens/commitlog/)

[Documentation](https://docs.rs/commitlog/)

## Usage

First, add this to your `Cargo.toml`:

```toml
[dependencies]
commitlog = "0.2"
```

```rust
use commitlog::*;
use commitlog::message::*;

fn main() {
    // open a directory called 'log' for segment and index storage
    let opts = LogOptions::new("log");
    let mut log = CommitLog::new(opts).unwrap();

    // append to the log
    log.append_msg("my_stream", "hello world").unwrap(); // offset 0
    log.append_msg("my_stream", "second message").unwrap(); // offset 1

    // read the messages
    let messages = log.read(0, ReadLimit::default()).unwrap();
    for msg in messages.iter() {
        println!("{} - {}", msg.offset(), String::from_utf8_lossy(msg.payload()));
    }

    // prints:
    //    0 - hello world
    //    1 - second message
}
```

## Prior Art

- [Apache Kafka](https://kafka.apache.org/)
- [Jocko](https://github.com/travisjeffery/jocko) + [EXCELLENT Blog Post](https://medium.com/the-hoard/how-kafkas-storage-internals-work-3a29b02e026)
