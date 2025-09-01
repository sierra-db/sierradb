use std::collections::HashMap;
use uuid::Uuid;

use sierradb_client::{
    AsyncTypedCommands, EAppendOptions, EMAppendEvent, SierraAsyncClientExt, SierraMessage,
    stream_partition_key,
};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::SubscriberBuilder::default()
        .without_time()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    println!("🚀 Starting comprehensive SierraDB client test...");

    // Create Redis client
    let client = redis::Client::open("redis://127.0.0.1:9090?protocol=resp3")?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("\n=== Basic Commands ===");

    // Test HELLO command
    println!("📋 Testing HELLO command...");
    match conn.hello(3).await {
        Ok(server_info) => println!("✅ Server info: {server_info:?}"),
        Err(err) => println!("❌ HELLO failed: {err}"),
    }

    // Test PING command
    println!("🏓 Testing PING command...");
    match conn.ping().await {
        Ok(response) => println!("✅ PING response: {response}"),
        Err(err) => println!("❌ PING failed: {err}"),
    }

    println!("\n=== Event Storage Commands ===");

    let test_stream = "test-stream";
    let test_partition_key = stream_partition_key(test_stream);

    // Test EAPPEND command
    println!("📝 Testing EAPPEND command...");
    let append_options = EAppendOptions::new()
        .payload(br#"{"user_id": 123, "action": "login"}"#)
        .metadata(br#"{"source": "web_app", "ip": "192.168.1.1"}"#);

    match conn
        .eappend(test_stream, "UserLoggedIn", append_options)
        .await
    {
        Ok(append_info) => {
            println!("✅ Event appended:");
            println!("   Event ID: {}", append_info.event_id);
            println!(
                "   Partition: {} ({})",
                append_info.partition_id, append_info.partition_key
            );
            println!("   Stream version: {}", append_info.stream_version);
            println!("   Partition sequence: {}", append_info.partition_sequence);
        }
        Err(err) => println!("❌ EAPPEND failed: {err}"),
    }

    // Test EMAPPEND command
    println!("📝 Testing EMAPPEND command...");
    let events = [
        EMAppendEvent::new("test-stream-1", "EventA").payload(br#"{"data": "value1"}"#),
        EMAppendEvent::new("test-stream-2", "EventB").payload(br#"{"data": "value2"}"#),
    ];

    match conn.emappend(test_partition_key, &events).await {
        Ok(multi_info) => {
            println!("✅ Multi-event append completed:");
            println!(
                "   Partition: {} ({})",
                multi_info.partition_id, multi_info.partition_key
            );
            println!(
                "   Sequence range: {} - {}",
                multi_info.first_partition_sequence, multi_info.last_partition_sequence
            );
            println!("   Events: {}", multi_info.events.len());
        }
        Err(err) => println!("❌ EMAPPEND failed: {err}"),
    }

    // Test EGET command
    println!("🔍 Testing EGET command...");
    let test_event_id = Uuid::new_v4(); // This will likely return None since it's random
    match conn.eget(test_event_id).await {
        Ok(Some(event)) => {
            println!("✅ Event found:");
            println!("   Event: {} v{}", event.event_name, event.stream_version);
            println!("   Stream: {}", event.stream_id);
        }
        Ok(None) => println!("✅ EGET completed (event not found, as expected)"),
        Err(err) => println!("❌ EGET failed: {err}"),
    }

    println!("\n=== Version/Sequence Queries ===");

    // Test ESVER command
    println!("🔢 Testing ESVER command...");
    match conn.esver(test_stream).await {
        Ok(version) => println!("✅ Stream version: {version:?}"),
        Err(err) => println!("❌ ESVER failed: {err}"),
    }

    // Test ESVER with partition key
    println!("🔢 Testing ESVER with partition key...");
    match conn
        .esver_with_partition_key(test_stream, test_partition_key)
        .await
    {
        Ok(version) => println!("✅ Stream version (partition): {version:?}"),
        Err(err) => println!("❌ ESVER with partition failed: {err}"),
    }

    // Test EPSEQ by key
    println!("🔢 Testing EPSEQ by key...");
    match conn.epseq_by_key(test_partition_key).await {
        Ok(sequence) => println!("✅ Partition sequence: {sequence:?}"),
        Err(err) => println!("❌ EPSEQ by key failed: {err}"),
    }

    // Test EPSEQ by ID
    println!("🔢 Testing EPSEQ by ID...");
    match conn.epseq_by_id(689).await {
        Ok(sequence) => println!("✅ Partition 689 sequence: {sequence:?}"),
        Err(err) => println!("❌ EPSEQ by ID failed: {err}"),
    }

    println!("\n=== Scanning Commands ===");

    // Test ESCAN
    println!("📖 Testing ESCAN command...");
    match conn.escan(test_stream, 0, Some(10), Some(5)).await {
        Ok(batch) => {
            println!("✅ Stream scan completed:");
            println!("   Events: {}", batch.events.len());
            println!("   Has more: {}", batch.has_more);
            for event in &batch.events {
                println!("   - {} v{}", event.event_name, event.stream_version);
            }
        }
        Err(err) => println!("❌ ESCAN failed: {err}"),
    }

    // Test ESCAN with partition key
    println!("📖 Testing ESCAN with partition key...");
    match conn
        .escan_with_partition_key(test_stream, test_partition_key, 0, Some(10), Some(5))
        .await
    {
        Ok(batch) => {
            println!("✅ Stream scan (partition) completed:");
            println!("   Events: {}", batch.events.len());
            println!("   Has more: {}", batch.has_more);
        }
        Err(err) => println!("❌ ESCAN with partition failed: {err}"),
    }

    // Test EPSCAN by key
    println!("📖 Testing EPSCAN by key...");
    match conn
        .epscan_by_key(test_partition_key, 0, Some(10), Some(5))
        .await
    {
        Ok(batch) => {
            println!("✅ Partition scan completed:");
            println!("   Events: {}", batch.events.len());
            println!("   Has more: {}", batch.has_more);
        }
        Err(err) => println!("❌ EPSCAN by key failed: {err}"),
    }

    // Test EPSCAN by ID
    println!("📖 Testing EPSCAN by ID...");
    match conn.epscan_by_id(689, 0, Some(10), Some(5)).await {
        Ok(batch) => {
            println!("✅ Partition scan (ID) completed:");
            println!("   Events: {}", batch.events.len());
            println!("   Has more: {}", batch.has_more);
        }
        Err(err) => println!("❌ EPSCAN by ID failed: {err}"),
    }

    println!("\n=== Subscription Tests ===");

    // Create subscription manager for advanced subscription features
    let mut manager = client.subscription_manager().await?;

    // Test stream subscription from latest
    println!("📡 Testing stream subscription from latest...");
    let mut stream_sub = match manager.subscribe_to_stream_from_latest(test_stream).await {
        Ok(sub) => {
            println!("✅ Stream subscription created: {}", sub.subscription_id());
            Some(sub)
        }
        Err(err) => {
            println!("❌ Stream subscription failed: {err}");
            None
        }
    };

    // Test partition subscription from latest
    println!("📡 Testing partition subscription from latest...");
    let mut partition_sub = match manager.subscribe_to_all_partitions_from_latest().await {
        Ok(sub) => {
            println!(
                "✅ Partition subscription created: {}",
                sub.subscription_id()
            );
            Some(sub)
        }
        Err(err) => {
            println!("❌ Partition subscription failed: {err}");
            None
        }
    };

    // Test multiple partition subscription with sequences
    println!("📡 Testing multiple partition subscription...");
    let partition_sequences = HashMap::from([(689, 0), (690, 0)]);
    let mut multi_partition_sub = match manager
        .subscribe_to_partitions_with_sequences(partition_sequences, Some(10))
        .await
    {
        Ok(sub) => {
            println!(
                "✅ Multi-partition subscription created: {}",
                sub.subscription_id()
            );
            Some(sub)
        }
        Err(err) => {
            println!("❌ Multi-partition subscription failed: {err}");
            None
        }
    };

    // Add some more events to trigger subscription messages
    println!("\n📝 Adding more events to trigger subscriptions...");
    for i in 0..3 {
        let options =
            EAppendOptions::new().payload(format!(r#"{{"test_event": {i}}}"#).into_bytes());
        let _ = conn
            .eappend(format!("test-stream-{i}"), "TestEvent", options)
            .await;
    }

    // Listen for subscription messages briefly
    println!("👂 Listening for subscription messages (5 seconds)...");
    let mut message_count = 0;
    let timeout = tokio::time::Duration::from_secs(5);
    let start_time = tokio::time::Instant::now();

    while start_time.elapsed() < timeout && message_count < 10 {
        tokio::select! {
            msg = async {
                if let Some(ref mut sub) = stream_sub {
                    sub.next_message().await
                } else {
                    None
                }
            } => {
                if let Some(msg) = msg {
                    match msg {
                        SierraMessage::Event { event, cursor } => {
                            println!("📨 [Stream] Event: {} (cursor: {})", event.event_name, cursor);
                            if let Some(ref sub) = stream_sub {
                                let _ = sub.acknowledge_up_to_cursor(cursor).await;
                            }
                            message_count += 1;
                        }
                        SierraMessage::SubscriptionConfirmed { subscription_count } => {
                            println!("✅ [Stream] Subscription confirmed (active: {subscription_count})");
                        }
                    }
                }
            }
            msg = async {
                if let Some(ref mut sub) = partition_sub {
                    sub.next_message().await
                } else {
                    None
                }
            } => {
                if let Some(msg) = msg {
                    match msg {
                        SierraMessage::Event { event, cursor } => {
                            println!("📨 [Partition] Event: {} (cursor: {})", event.event_name, cursor);
                            if let Some(ref sub) = partition_sub {
                                let _ = sub.acknowledge_up_to_cursor(cursor).await;
                            }
                            message_count += 1;
                        }
                        SierraMessage::SubscriptionConfirmed { subscription_count } => {
                            println!("✅ [Partition] Subscription confirmed (active: {subscription_count})");
                        }
                    }
                }
            }
            msg = async {
                if let Some(ref mut sub) = multi_partition_sub {
                    sub.next_message().await
                } else {
                    None
                }
            } => {
                if let Some(msg) = msg {
                    match msg {
                        SierraMessage::Event { event, cursor } => {
                            println!("📨 [Multi-Partition] Event: {} (cursor: {})", event.event_name, cursor);
                            if let Some(ref sub) = multi_partition_sub {
                                let _ = sub.acknowledge_up_to_cursor(cursor).await;
                            }
                            message_count += 1;
                        }
                        SierraMessage::SubscriptionConfirmed { subscription_count } => {
                            println!("✅ [Multi-Partition] Subscription confirmed (active: {subscription_count})");
                        }
                    }
                }
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                // Continue listening
            }
        }
    }

    // Clean up subscriptions
    if let Some(sub) = stream_sub {
        let _ = sub.unsubscribe().await;
        println!("🧹 Stream subscription closed");
    }
    if let Some(sub) = partition_sub {
        let _ = sub.unsubscribe().await;
        println!("🧹 Partition subscription closed");
    }
    if let Some(sub) = multi_partition_sub {
        let _ = sub.unsubscribe().await;
        println!("🧹 Multi-partition subscription closed");
    }

    println!("\n🎉 Comprehensive test completed!");
    println!("📊 Received {message_count} subscription messages");

    Ok(())
}
