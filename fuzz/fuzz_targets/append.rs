#![no_main]

extern crate arbitrary;

use std::time::Duration;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use once_cell::sync::OnceCell;
use sierradb::StreamId;
use sierradb::database::ExpectedVersion;
use sierradb::database::{Database, DatabaseBuilder};
use sierradb::writer_thread_pool::AppendEventsBatch;
use sierradb::writer_thread_pool::WriteEventRequest;
use std::path::PathBuf;
use tokio::runtime::Runtime;
use uuid::Uuid;

// Static instances using OnceCell for thread-safe initialization
static RUNTIME_DB: OnceCell<(Runtime, Database)> = OnceCell::new();

// Fixed number of buckets
const TOTAL_BUCKETS: u16 = 4;

// Initialize the database and runtime once
fn init() -> (&'static Runtime, &'static Database) {
    // First initialize runtime if needed
    let (runtime, db) = RUNTIME_DB.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        // Use a unique directory for each fuzzing run to avoid conflicts
        let db_path = PathBuf::from("target/fuzz_db");
        // Remove any existing database files
        let _ = std::fs::remove_dir_all(&db_path);

        // Create the database using the runtime
        let db = runtime.block_on(async {
            DatabaseBuilder::new(db_path)
                .segment_size(64 * 1024 * 1024)
                .total_buckets(TOTAL_BUCKETS)
                .bucket_ids_from_range(0..TOTAL_BUCKETS)
                .writer_pool_num_threads(TOTAL_BUCKETS)
                .reader_pool_num_threads(TOTAL_BUCKETS)
                .flush_interval_duration(Duration::MAX)
                .flush_interval_events(1) // Flush every event written
                .open()
                .unwrap()
        });

        (runtime, db)
    });

    (runtime, db)
}

#[derive(Clone, Debug)]
struct AppendEvent {
    bucket_id: u16,
    event_id: Uuid,
    partition_id: u16,
    stream_id: StreamId,
    event_name: String,
    timestamp: u64,
    metadata: Vec<u8>,
    payload: Vec<u8>,
}

impl<'a> Arbitrary<'a> for AppendEvent {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let bucket_id = u16::arbitrary(u)?;

        let event_id_bytes = <[u8; 16]>::arbitrary(u)?;
        let event_id = Uuid::from_bytes(event_id_bytes);

        let partition_id = u16::arbitrary(u)?;

        // Stream ID with length 1-64
        let stream_id_len: usize = u.int_in_range(1..=64)?;
        let stream_id_string: String = u
            .arbitrary_iter::<char>()?
            .take(stream_id_len)
            .collect::<Result<_, _>>()?;
        if stream_id_string.is_empty() {
            return Err(arbitrary::Error::NotEnoughData);
        }
        let stream_id =
            StreamId::new(stream_id_string).map_err(|_| arbitrary::Error::IncorrectFormat)?;

        // Choose a random maximum byte length for the event name (0 to u8::MAX)
        let max_event_name_bytes = u.int_in_range(0..=u8::MAX as usize)?;

        // Event name with dynamic adjustment to ensure byte length <= chosen max
        let mut event_name = String::new();

        // Add characters until we're close to the limit
        while event_name.len() < max_event_name_bytes {
            // Try to get another character
            if let Ok(c) = u.arbitrary::<char>() {
                // Check if adding this char would exceed the limit
                let char_bytes = c.len_utf8();
                if event_name.len() + char_bytes <= max_event_name_bytes {
                    event_name.push(c);
                } else {
                    break; // Adding this char would exceed limit
                }
            } else {
                break; // No more data available
            }
        }

        // Timestamp less than 2^63
        let timestamp = u.int_in_range(0..=(1 << 63) - 1)?;

        // Metadata - reasonably sized for fuzzing (too large would slow testing)
        // Using a smaller max size for practical fuzzing but representing the domain
        // constraint
        let metadata_len: usize = u.int_in_range(0..=16384)?; // Using 16KB as practical max
        let metadata: Vec<u8> = (0..metadata_len)
            .map(|_| u.arbitrary::<u8>())
            .collect::<Result<_, _>>()?;

        // Payload - reasonably sized for fuzzing
        let payload_len: usize = u.int_in_range(0..=16384)?; // Using 16KB as practical max
        let payload: Vec<u8> = (0..payload_len)
            .map(|_| u.arbitrary::<u8>())
            .collect::<Result<_, _>>()?;

        Ok(AppendEvent {
            bucket_id,
            event_id,
            partition_id,
            stream_id,
            event_name,
            timestamp,
            metadata,
            payload,
        })
    }

    fn size_hint(_depth: usize) -> (usize, Option<usize>) {
        // Fixed costs: u16 + [u8; 16] + u16 + range(1..=64) + u64
        let fixed_min = 2 + 16 + 2 + 1 + 8;

        // Minimum total: fixed costs + minimum stream_id (1 char) + event_name
        // (potentially empty)
        // + empty metadata and payload vectors
        let min = fixed_min + 1;

        // Maximum is trickier due to indeterminate String and Vec sizes
        // Maximum stream_id (64 chars * 4 bytes worst case)
        let max_stream_id = 64 * 4;

        // Maximum event_name (255 chars * 4 bytes worst case)
        let max_event_name = 255 * 4;

        // Maximum metadata and payload (using practical 16KB limit for each)
        let max_metadata = 16384;
        let max_payload = 16384;

        // Calculate maximum total size
        let max = fixed_min + max_stream_id + max_event_name + max_metadata + max_payload;

        (min, Some(max))
    }
}

fuzz_target!(|event: AppendEvent| {
    let (runtime, db) = init();

    runtime.block_on(async move {
        db.append_events(
            event.bucket_id % 8,
            AppendEventsBatch::single(WriteEventRequest {
                event_id: event.event_id,
                partition_key: Uuid::nil(),
                partition_id: event.partition_id % TOTAL_BUCKETS,
                stream_id: event.stream_id,
                stream_version: ExpectedVersion::Any,
                event_name: event.event_name,
                timestamp: event.timestamp,
                metadata: event.metadata,
                payload: event.payload,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    });
});
