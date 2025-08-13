use std::collections::HashMap;

use sierradb_client::{SierraAsyncClientExt, SierraMessage};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .without_time()
        .with_env_filter(EnvFilter::new("warn"))
        .init();

    // Create Redis client
    let client = redis::Client::open("redis://127.0.0.1:9090?protocol=resp3")?;

    // Create subscription manager
    let mut manager = client.subscription_manager().await?;

    println!("Creating subscriptions...");

    // Create multiple concurrent subscriptions
    // let mut stream_sub = manager
    //     .subscribe_to_stream_from_version("mypartition", 0)
    //     .await?;
    let mut partition_sub = manager
        .subscribe_to_partitions_with_sequences(HashMap::from_iter([(204, 0), (364, 0)]), Some(2))
        .await?;

    // println!("Stream subscription ID: {}", stream_sub.subscription_id());
    println!(
        "Partition subscription ID: {}",
        partition_sub.subscription_id()
    );
    println!("Waiting for events...");

    // Handle messages from multiple subscriptions concurrently
    loop {
        tokio::select! {
            // msg = stream_sub.next_message() => {
            //     match msg {
            //         Some(SierraMessage::Event { event, cursor }) => {
            //             println!("[Stream] Event: {} v{} - {} (cursor: {})",
            //                     event.event_name,
            //                     event.stream_version,
            //                     event.stream_id,
            //                     cursor);
            //             stream_sub.acknowledge_up_to_cursor(cursor).await?;
            //         }
            //         Some(SierraMessage::SubscriptionConfirmed { subscription_count }) => {
            //             println!("[Stream] Subscription confirmed (total active: {subscription_count})");
            //         }
            //         None => {
            //             println!("[Stream] Subscription closed");
            //             break;
            //         }
            //     }
            // }
            msg = partition_sub.next_message() => {
                match msg {
                    Some(SierraMessage::Event { event, cursor }) => {
                        println!("[Partition] Event: {} seq{} - {} (cursor: {})",
                            event.event_name,
                            event.partition_sequence,
                            event.stream_id,
                            cursor,
                        );
                        partition_sub.acknowledge_up_to_cursor(cursor).await?;
                    }
                    Some(SierraMessage::SubscriptionConfirmed { subscription_count }) => {
                        println!("[Partition] Subscription confirmed (total active: {subscription_count})");
                    }
                    None => {
                        println!("[Partition] Subscription closed");
                        break;
                    }
                }

            }
        }
    }

    Ok(())
}
