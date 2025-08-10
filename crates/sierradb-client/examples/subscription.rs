use sierradb_client::{SierraAsyncClientExt, SierraMessage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create Redis client
    let client = redis::Client::open("redis://127.0.0.1:9090?protocol=resp3")?;

    // Create subscription manager
    let mut manager = client.subscription_manager().await?;

    println!("Creating subscriptions...");

    // Create multiple concurrent subscriptions
    let mut stream_sub = manager
        .subscribe_to_stream_from_version("example-stream", 0)
        .await?;
    let mut partition_sub = manager.subscribe_to_partition_from_sequence(204, 0).await?;

    println!("Stream subscription ID: {}", stream_sub.subscription_id());
    println!(
        "Partition subscription ID: {}",
        partition_sub.subscription_id()
    );
    println!("Waiting for events...");

    // Handle messages from multiple subscriptions concurrently
    loop {
        tokio::select! {
            msg = stream_sub.next_message() => {
                match msg {
                    Some(SierraMessage::Event(event)) => {
                        println!("[Stream] Event: {} v{} - {}",
                                event.event_name,
                                event.stream_version,
                                event.stream_id);
                    }
                    Some(SierraMessage::SubscriptionConfirmed { subscription_count }) => {
                        println!("[Stream] Subscription confirmed (total active: {subscription_count})");
                    }
                    None => {
                        println!("[Stream] Subscription closed");
                        break;
                    }
                }
            }
            msg = partition_sub.next_message() => {
                match msg {
                    Some(SierraMessage::Event(event)) => {
                        println!("[Partition] Event: {} seq{} - {}",
                                event.event_name,
                                event.partition_sequence,
                                event.stream_id);
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
