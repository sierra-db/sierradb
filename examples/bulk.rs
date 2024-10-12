use eventus::{message::*, *};
use std::time::{self, SystemTime};

const BATCH_SIZE: u32 = 200;
const BATCHES: u32 = 10_000;

#[tokio::main]
async fn main() {
    env_logger::init();

    // open a directory called 'log' for segment and index storage
    let opts = LogOptions::new(format!(
        ".log{}",
        SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    ));
    let mut log = EventLog::new(opts).unwrap();

    let start = SystemTime::now();
    for i in 0..BATCHES {
        // let mut events = Vec::new();
        // for j in (0..BATCH_SIZE) {
        //     buf.push(i as u64, format!("{}-{}", i, j))
        //         .expect("Total size of messages exceeds usize::MAX");
        // }
        // log.append_to_stream("my_stream", ExpectedVersion::Any, &mut buf)
        //     .expect("Unable to append batch");

        if i == 99 || i == 50 {
            log.flush().expect("Unable to flush");
        }
        // println!("appended batch");
    }

    let end = SystemTime::now();
    println!(
        "Appended {} messages in {:?}",
        BATCH_SIZE * BATCHES,
        end.duration_since(start)
    );

    // read the log
    let start = SystemTime::now();
    let mut total = 0;
    let mut iterations = 0;
    let mut pos = 0;
    loop {
        let entries = log
            .read(pos, ReadLimit::max_bytes(10_240), 100)
            .expect("Unable to read messages from the log");
        match entries.iter().last().map(|m| m.id) {
            Some(off) => {
                iterations += 1;
                total += entries.len();
                assert!(pos < off);
                pos = off + 1;
            }
            None => {
                let end = SystemTime::now();
                println!(
                    "Read {} messages in {:?}, {} iterations",
                    total,
                    end.duration_since(start),
                    iterations
                );
                break;
            }
        }
    }
}
