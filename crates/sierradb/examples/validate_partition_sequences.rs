use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::Duration;
use std::{env, fs};

use sierradb::bucket::segment::{BucketSegmentReader, Record};
use sierradb::bucket::{BucketId, BucketSegmentId, PartitionId, SegmentId};
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .without_time()
        .with_env_filter(EnvFilter::new(
            "sierradb_cluster=INFO,sierradb_server=INFO,sierradb=INFO,INFO",
        ))
        .init();

    let dir = env::args().nth(1).ok_or("missing db path argument")?;
    let buckets_dir = Path::new(&dir).join("buckets");

    if !buckets_dir.exists() {
        println!("No buckets directory found at {}", buckets_dir.display());
        return Ok(());
    }

    let mut bucket_segments = BTreeMap::new();

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

        // Access the 'segments' subdirectory within this bucket
        let segments_dir = bucket_entry.path().join("segments");
        if !segments_dir.exists() {
            continue;
        }

        // Iterate through each segment directory
        for segment_entry in fs::read_dir(&segments_dir)? {
            let segment_entry = segment_entry?;
            if !segment_entry.file_type()?.is_dir() {
                continue;
            }

            // Extract segment ID from directory name
            let segment_name = segment_entry.file_name();
            let Some(segment_name_str) = segment_name.to_str() else {
                continue;
            };
            let Ok(segment_id) = segment_name_str.parse::<SegmentId>() else {
                continue;
            };

            let bsid = BucketSegmentId::new(bucket_id, segment_id);

            // Look for the events file in this segment directory
            let events_file = segment_entry.path().join("data.evts");
            if events_file.exists() {
                bucket_segments.insert(bsid, events_file);
            }
        }
    }

    let mut next_sequences: HashMap<PartitionId, u64> = HashMap::new();

    'bucket_segment_loop: for (bsid, events_file) in bucket_segments {
        let mut reader = BucketSegmentReader::open(events_file, None)?;
        let mut iter = reader.iter();

        while let Some(record) = iter.next_record().transpose() {
            match record {
                Ok(Record::Event(event)) => {
                    let expected = match next_sequences.entry(event.partition_id) {
                        Entry::Occupied(mut entry) => {
                            *entry.get_mut() += 1;
                            *entry.get()
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(0);
                            0
                        }
                    };

                    println!(
                        "Found event for {bsid} partition id {}, expecting {expected}",
                        event.partition_id
                    );

                    if event.partition_sequence != expected {
                        println!("FOUND AN INCONSISTENT SEQUENCE - expected {expected}");
                        dbg!(event);
                        // std::process::exit(1);
                        std::thread::sleep(Duration::from_secs(2));
                    }
                }
                Ok(Record::Commit(_)) => {}
                Err(err) => {
                    println!("ERROR: {err}");
                    continue 'bucket_segment_loop;
                }
            }
        }
    }

    Ok(())
}
