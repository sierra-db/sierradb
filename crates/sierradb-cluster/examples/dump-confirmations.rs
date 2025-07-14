use std::path::Path;
use std::{env, fs};

use sierradb::bucket::BucketId;
use sierradb_cluster::confirmation::ChecksummedState;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .without_time()
        .with_env_filter(EnvFilter::new(
            "sierradb_cluster=DEBUG,sierradb_server=DEBUG,sierradb=TRACE,INFO",
        ))
        .init();

    let dir = env::args().nth(1).ok_or("missing db path argument")?;
    let buckets_dir = Path::new(&dir).join("buckets");

    if !buckets_dir.exists() {
        println!("No buckets directory found at {}", buckets_dir.display());
        return Ok(());
    }

    // Iterate through each bucket directory
    for bucket_entry in fs::read_dir(buckets_dir)? {
        let bucket_entry = bucket_entry?;
        if !bucket_entry.file_type()?.is_dir() {
            continue;
        }

        // Extract bucket ID from directory name
        let bucket_name = bucket_entry.file_name();
        let Some(bucket_name_str) = bucket_name.to_str() else {
            continue;
        };
        let Ok(bucket_id) = bucket_name_str.parse::<BucketId>() else {
            continue;
        };

        // Access the 'confirmation' subdirectory within this bucket
        let confirmation_path = bucket_entry
            .path()
            .join("confirmation")
            .join("bucket_state.current.dat");
        if !confirmation_path.exists() {
            continue;
        }

        let bytes = std::fs::read(confirmation_path)?;

        let (checksummed_state, _): (ChecksummedState, _) =
            bincode::decode_from_slice(&bytes, bincode::config::standard())?;

        let checksum_valid = checksummed_state.validate();
        let state = checksummed_state.deserialize()?;

        println!(
            "Bucket {bucket_id} (Checksum: {})",
            if checksum_valid { "✅" } else { "❌" }
        );
        for (partition_id, partition_state) in state.partition_states {
            println!(
                "    Partition {partition_id}: {} / {} ({} unconfirmed)",
                partition_state.confirmed_watermark.get(),
                partition_state.highest_version,
                partition_state.unconfirmed_events.len(),
            );
        }
    }

    Ok(())
}
