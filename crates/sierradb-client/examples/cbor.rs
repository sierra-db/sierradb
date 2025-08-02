use std::collections::HashMap;

use serde::Serialize;
use sierradb_client::{Commands, EAppendOptions};

#[derive(Serialize)]
struct UserInfo {
    name: String,
    ips: Vec<String>,
    is_awesome: bool,
    meta: HashMap<String, (bool, u16)>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1:9090/")?;
    let mut con = client.get_connection()?;

    let value = UserInfo {
        name: "Ari".to_string(),
        ips: vec!["192.168.1.1".to_string(), "192.168.1.10".to_string()],
        is_awesome: true,
        meta: HashMap::from_iter([("Howdy".to_string(), (false, 10))]),
    };
    let mut bytes = Vec::new();
    ciborium::into_writer(&value, &mut bytes)?;

    let meta: HashMap<String, &'static str> = HashMap::from_iter([("Hellow".to_string(), "world")]);
    let mut meta_bytes = Vec::new();
    ciborium::into_writer(&meta, &mut meta_bytes)?;

    let res = con.eappend(
        "user-ari",
        "UserUpdated",
        EAppendOptions::new().payload(&bytes).metadata(&meta_bytes),
    )?;

    dbg!(res);

    Ok(())
}
