# Eventus

Sequential, disk-backed event log library for Rust. The library can be used in various higher-level distributed abstractions on top of a distributed log such as [Paxos](https://github.com/zowens/paxos-rs), [Chain Replication](https://github.com/zowens/chain-replication) or Raft.

[![Crates.io](https://img.shields.io/crates/v/eventus.svg?maxAge=2592000)](https://crates.io/crates/eventus)
[![Docs.rs](https://docs.rs/eventus/badge.svg)](https://docs.rs/eventus/)
[![Travis](https://travis-ci.org/tqwewe/eventus.svg?branch=master)](https://travis-ci.org/tqwewe/eventus/)

[Documentation](https://docs.rs/eventus/)

## Usage

First, add this to your `Cargo.toml`:

```toml
[dependencies]
eventus = "0.2"
```

```rust
use eventus::*;
use eventus::message::*;

fn main() {
    // open a directory called 'log' for segment and index storage
    let opts = LogOptions::new("log");
    let mut log = EventLog::new(opts).unwrap();

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

## Credits

This project is a heavily modified fork of [commitlog](https://github.com/zowens/commitlog), which helped provide a very useful backbone to the storing of events.
