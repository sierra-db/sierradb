# seglog

A simple, high-performance segment log implementation for Rust.

`seglog` provides low-level read and write operations for fixed-size segment files with built-in CRC32C validation. It's designed for event sourcing systems, write-ahead logs, and other append-only storage use cases.

## Features

- **Single writer, multiple concurrent readers** - Lock-free reads with atomic offset coordination
- **CRC32C validation** - Automatic data integrity checking on every read
- **Optimized I/O** - Reduces syscalls with optimistic reads (~40% faster for small records)
- **Fixed-size segments** - Pre-allocated files with configurable headers
- **Corruption recovery** - Detects and recovers from partial writes
- **Zero-copy reads** - Returns borrowed data when possible

## Quick Example

```rust
use seglog::write::Writer;
use seglog::read::{Reader, ReadHint};

// Create a 1MB segment
let mut writer = Writer::create("segment.log", 1024 * 1024, 0)?;

// Append records
let (offset, _) = writer.append(b"event data")?;
writer.sync()?; // Flush to disk

// Read concurrently
let flushed = writer.flushed_offset();
let mut reader = Reader::open("segment.log", Some(flushed))?;
let data = reader.read_record(offset, ReadHint::Random)?;

assert_eq!(&*data, b"event data");
```

## Record Format

Each record consists of an 8-byte header followed by variable-length data:

```
┌─────────────┬─────────────┬────────────────┐
│ Length (4B) │ CRC32C (4B) │ Data (N bytes) │
└─────────────┴─────────────┴────────────────┘
```

## Performance Optimizations

### Read Hints

- **`ReadHint::Sequential`** - Uses 64KB read-ahead buffer for streaming access
- **`ReadHint::Random`** - Optimistic reads (header + 2KB) to reduce syscalls

### Optimistic Reads

For random access, the reader performs a single syscall to read the header plus 2KB of data. Since most events in event sourcing are small (< 2KB), this eliminates one syscall per read, improving performance by ~40%.

## Header Support

Reserve space at the beginning of segments for application-specific headers:

```rust
const HEADER_SIZE: u64 = 64;
let mut writer = Writer::create("segment.log", 1024 * 1024, HEADER_SIZE)?;

// Write header data
writer.file().write_all_at(b"MAGIC", 0)?;

// Records automatically start after header
writer.append(b"data")?;
```

## Concurrent Safety

The `FlushedOffset` provides atomic coordination between writers and readers:

- Writers update it after calling `sync()`
- Readers check it to avoid reading uncommitted data
- Shared via `Arc` for efficient cloning across threads

This ensures readers never see partial writes or corrupted data.

## Use Cases

- **Event sourcing** - Store domain events in append-only segments
- **Write-ahead logs** - Durable transaction logging
- **Message queues** - Persistent queue segments
- **Time-series data** - Append-only metric storage

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
