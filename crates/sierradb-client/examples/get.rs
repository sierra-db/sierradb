use std::time::Duration;

use sierradb_client::{Commands, EAppendOptions, EMAppendEvent, stream_partition_key};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", stream_partition_key("mypartition"));
    return Ok(());

    let client = redis::Client::open("redis://127.0.0.1:9090/")?;
    let mut con = client.get_connection()?;

    let info = con.eappend("foo", "bar", EAppendOptions::new().payload(b"hola"))?;
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

    let batch = con.epscan_by_id(665, 0, None, None)?;
    dbg!(batch);

    let batch = con.escan_with_partition_key(
        "mypartition",
        stream_partition_key("mypartition"),
        0,
        None,
        None,
    )?;
    dbg!(batch);

    Ok(())
}
