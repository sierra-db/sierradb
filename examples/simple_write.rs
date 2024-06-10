use commitlog::*;
use serde::{Deserialize, Serialize};
use std::time::{self, SystemTime};

#[tokio::main]
async fn main() {
    // open a directory called 'log' for segment and index storage
    let mut opts = LogOptions::new(format!(
        ".log{}",
        SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    ));
    opts.segment_max_entries(256_000);
    let mut log = CommitLog::new(opts).unwrap();

    #[derive(Serialize, Deserialize)]
    struct Event {
        foo: String,
    }

    // append to the log
    for _ in 0..300 {
        log.append_msg(
            "key2",
            bincode::serialize(&Event {
                foo: "hi".to_string(),
            })
            .unwrap(),
        )
        .unwrap(); // offset 0
        log.append_msg(
            "key2",
            bincode::serialize(&Event {
                foo: "there".to_string(),
            })
            .unwrap(),
        )
        .unwrap(); // offset 1
        log.append_msg(
            "key2",
            bincode::serialize(&Event {
                foo: "bro".to_string(),
            })
            .unwrap(),
        )
        .unwrap(); // offset 2
        log.append_msg(
            "key1",
            bincode::serialize(&Event {
                foo: "bro".to_string(),
            })
            .unwrap(),
        )
        .unwrap(); // offset 2
        log.append_msg(
            "key2",
            bincode::serialize(&Event {
                foo: "bro".to_string(),
            })
            .unwrap(),
        )
        .unwrap(); // offset 2
        log.append_msg(
            "key1",
            bincode::serialize(&Event {
                foo: "bro".to_string(),
            })
            .unwrap(),
        )
        .unwrap(); // offset 2
    }

    // let msgs = log.read_stream("key1").await.unwrap();
    // for msg in msgs {
    //     println!("{:?}", msg.foo);
    // }

    // println!("---");

    // let msgs = log.read_stream("key2").await.unwrap();
    // for msg in msgs {
    //     println!("{:?}", msg.foo);
    // }

    // prints:
    //    0 - hello world
    //    1 - second message
}
