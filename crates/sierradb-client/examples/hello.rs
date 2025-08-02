use sierradb_client::{Commands, EAppendOptions, stream_partition_key};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1:9090/")?;
    let mut con = client.get_connection()?;

    let user_kye = "user-kye".to_string();
    let order_abc = "order-abc".to_string();
    let res = con.eappend(
        &order_abc,
        "PaymentProcessed",
        EAppendOptions::default().partition_key(stream_partition_key(&user_kye)),
    )?;

    dbg!(res);

    // let events =
    //     con.escan_with_partition_key(&order_abc, stream_partition_key(&user_kye),
    // 0, None, None)?;

    // dbg!(events);

    Ok(())
}
