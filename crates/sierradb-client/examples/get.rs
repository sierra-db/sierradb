use redis::cluster::ClusterClient;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nodes = vec![
        "redis://127.0.0.1:9090/",
        // "redis://127.0.0.1:6378/",
        // "redis://127.0.0.1:6377/",
    ];
    let client = ClusterClient::new(nodes).unwrap();
    let _connection = client.get_connection().unwrap();

    // let client = redis::Client::open("redis://127.0.0.1:9090/")?;
    // let mut con = client.get_connection()?;

    // let res = con
    //     .eappend(
    //         "fiz",
    //         "gjksfhg",
    //         EAppendOptions::new().expected_version(ExpectedVersion::Exact(1)),
    //     )
    //     .unwrap_err();
    // dbg!(res.kind());
    // dbg!(res.detail());
    // dbg!(res.code());
    // dbg!(res.category());

    // let info = con.eappend("foo", "bar",
    // EAppendOptions::new().payload(b"hola"))?; println!("Appended event!");
    // dbg!(&info);

    // std::thread::sleep(Duration::from_millis(200));

    // let event = con.eget(info.event_id)?;
    // println!("Retrieved event!");
    // dbg!(event);

    // let info = con.emappend(
    //     stream_partition_key("mypartition"),
    //     &[
    //         EMAppendEvent::new("mypartition", "OogaBooga").payload(b"hooowo"),
    //         EMAppendEvent::new("fdsjha", "ItGoesOn!").payload(b"hah"),
    //     ],
    // )?;
    // println!("Appended events!");
    // dbg!(&info);

    // let batch = con.epscan_by_id(665, 0, None, None)?;
    // dbg!(batch);

    // let batch = con.escan_with_partition_key(
    //     "mypartition",
    //     stream_partition_key("mypartition"),
    //     0,
    //     None,
    //     None,
    // )?;
    // dbg!(batch);

    Ok(())
}
