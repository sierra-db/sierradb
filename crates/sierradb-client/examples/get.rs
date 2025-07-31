use std::time::Duration;

use redis::Value;
use sierradb_client::{Commands, EAppendOptions, EMAppendEvent, stream_partition_key};
use uuid::Uuid;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1:9090/")?;
    let mut con = client.get_connection()?;

    let info = con.eappend(
        "foo",
        "bar",
        EAppendOptions::default().payload(b"hola".to_vec()),
    )?;
    println!("Appended event!");
    dbg!(&info);

    std::thread::sleep(Duration::from_millis(200));

    let event = con.eget(info.event_id)?;
    println!("Retrieved event!");
    dbg!(event);

    let info = con.emappend(
        stream_partition_key("mypartition"),
        &[
            EMAppendEvent::new("mypartition", "OogaBooga").payload(b"hooowo"),
            EMAppendEvent::new("fdsjha", "ItGoesOn!").payload(b"hah"),
        ],
    )?;
    println!("Appended events!");
    dbg!(&info);

    Ok(())
}
