use std::path::Path;
use std::{env, fs};

use sierradb::bucket::segment::{BucketSegmentReader, Record};
use sierradb::bucket::{BucketId, BucketSegmentId, SegmentId};

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

        // Access the 'segments' subdirectory within this bucket
        let segments_dir = bucket_entry.path().join("segments");
        if !segments_dir.exists() {
            continue;
        }

        // Iterate through each segment directory
        'segment_loop: for segment_entry in fs::read_dir(&segments_dir)? {
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
                let mut reader = BucketSegmentReader::open(events_file, None)?;
                let mut iter = reader.iter();

                while let Some(record) = iter.next_record(false).transpose() {
                    match record {
                        Ok(Record::Event(event)) => {
                            println!(
                                "{bsid} -- {}/{} {}@{} - {} (confirmations: {})",
                                event.partition_id,
                                event.partition_sequence,
                                event.stream_id,
                                event.stream_version,
                                event.event_name,
                                event.confirmation_count,
                            )
                        }
                        Ok(Record::Commit(_)) => {}
                        Err(err) => {
                            println!("ERROR: {err}");
                            continue 'segment_loop;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
