use std::{env, fs};

use sierradb::bucket::{
    SegmentKind,
    segment::{BucketSegmentReader, Record},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = env::args().nth(1).ok_or("missing db path argument")?;

    'outer: for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(file_name_str) = file_name.to_str() else {
            continue;
        };

        let Some((bsid, segment_kind)) = SegmentKind::parse_file_name(file_name_str) else {
            continue;
        };

        if let SegmentKind::Events = segment_kind {
            let mut reader = BucketSegmentReader::open(entry.path(), None)?;

            let mut iter = reader.iter();

            while let Some(record) = iter.next_record().transpose() {
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
                        continue 'outer;
                    }
                }
            }
        }
    }

    Ok(())
}
