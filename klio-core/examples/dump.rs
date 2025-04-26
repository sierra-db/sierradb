use std::time::Duration;

use klio_core::database::DatabaseBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = DatabaseBuilder::new("./target/db-0")
        .segment_size(256_000_000)
        .total_buckets(4)
        .bucket_ids(vec![0, 2])
        .reader_pool_num_threads(4)
        .writer_pool_num_threads(2)
        .flush_interval_duration(Duration::MAX)
        .flush_interval_events(1)
        .open()?;

    for partition_id in 0..8 {
        let mut events = db.read_partition(partition_id).await?;
        while let Some(event) = events.next().await? {
            dbg!(event);
        }
    }

    Ok(())
}
