use std::{collections::HashSet, time::SystemTime};

use eventus::{EventLog, LogOptions, ReadLimit};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level("trace".parse().unwrap())
        .init();

    let opts = LogOptions::new("../oddselite2/.log-temp3");
    let log = EventLog::new(opts).unwrap();

    let mut stream_ids = HashSet::new();

    let start = SystemTime::now();
    let mut total = 0;
    let mut iterations = 0;
    let mut pos = 0;
    loop {
        let entries = log
            .read(pos, ReadLimit::max_bytes(1024 * 64), 100)
            .expect("Unable to read messages from the log");
        iterations += 1;
        total += entries.len();
        let entries_is_empty = entries.is_empty();

        for entry in entries {
            stream_ids.insert(entry.stream_id);

            pos = entry.id + 1;
            if pos >= 256_000 {
                break;
            }
        }

        if pos >= 256_000 {
            let end = SystemTime::now();
            println!(
                "Read {} messages in {:?}, {} iterations",
                total,
                end.duration_since(start),
                iterations
            );
            println!("Got {} stream ids", stream_ids.len());
            break;
        } else if entries_is_empty {
            panic!("entries is empty?");
        }
    }

    Ok(())
}
