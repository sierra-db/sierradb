use commitlog::{message::*, *};
use std::time::{self, SystemTime};

fn main() {
    // open a directory called 'log' for segment and index storage
    let opts = LogOptions::new(format!(
        ".log{}",
        SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    ));
    let mut log = CommitLog::new(opts).unwrap();

    // append to the log
    log.append_msg("hello world").unwrap(); // offset 0
    log.append_msg("second message").unwrap(); // offset 1

    // read the messages
    let messages = log.read(0, ReadLimit::default()).unwrap();
    for msg in messages.iter() {
        println!(
            "{} - {}",
            msg.offset(),
            String::from_utf8_lossy(msg.payload())
        );
    }

    // prints:
    //    0 - hello world
    //    1 - second message
}
