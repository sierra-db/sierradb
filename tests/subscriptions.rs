use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use kameo::actor::{ActorRef, Spawn};
use libp2p::identity::Keypair;
use sierradb::{
    StreamId,
    database::{DatabaseBuilder, ExpectedVersion, NewEvent, Transaction},
    id::{uuid_to_partition_hash, uuid_v7_with_partition_hash},
};
use sierradb_cluster::{
    ClusterActor, ClusterArgs,
    subscription::{FromSequences, Subscribe, SubscriptionEvent, SubscriptionMatcher},
    write::execute::ExecuteTransaction,
};
use smallvec::smallvec;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

#[tokio::test]
async fn test_subscriptions() -> Result<(), Box<dyn std::error::Error>> {
    // Setup once
    let dir = tempfile::tempdir()?;
    let database = DatabaseBuilder::new()
        .total_buckets(4)
        .bucket_ids_from_range(0..4)
        .open(dir.path())?;

    let keypair = Keypair::generate_ed25519();
    let cluster_ref = ClusterActor::spawn(ClusterArgs {
        keypair,
        database,
        listen_addrs: vec![],
        node_count: 1,
        node_index: 0,
        bucket_count: 4,
        partition_count: 32,
        replication_factor: 1,
        assigned_partitions: HashSet::from_iter(0..32),
        heartbeat_timeout: Duration::from_millis(1_000),
        heartbeat_interval: Duration::from_millis(6_000),
        replication_buffer_size: 1_000,
        replication_buffer_timeout: Duration::from_millis(8_000),
        replication_catchup_timeout: Duration::from_millis(2_000),
        mdns: false,
    });

    // Write events once
    const EVENT_COUNT: usize = 2_000;
    let mut partition_sequences = HashMap::new();
    let mut stream_versions = HashMap::new();

    for _ in 0..EVENT_COUNT {
        let partition_key = Uuid::new_v4();
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % 32;

        let next_sequence = *partition_sequences
            .entry(partition_id)
            .and_modify(|seq| *seq += 1)
            .or_insert(0);
        let expected_sequence = ExpectedVersion::from_next_version(next_sequence);

        let next_version = *stream_versions
            .entry(partition_key.to_string())
            .and_modify(|ver| *ver += 1)
            .or_insert(0);
        let expected_version = ExpectedVersion::from_next_version(next_version);

        let transaction = Transaction::new(
            partition_key,
            partition_id,
            smallvec![NewEvent {
                event_id: uuid_v7_with_partition_hash(partition_hash),
                stream_id: StreamId::new(partition_key.to_string())?,
                stream_version: expected_version,
                event_name: "MyEvent".to_string(),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64,
                metadata: vec![],
                payload: vec![],
            }],
        )?
        .expected_partition_sequence(expected_sequence);
        cluster_ref
            .tell(ExecuteTransaction::new(transaction))
            .await?;
    }

    // Wait for all writes to complete
    // Since we're using tell (fire-and-forget), give some time for all messages to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Test different window sizes without rewriting
    for window_size in [10_000, 100, 10] {
        test_subscription_with_window_size(&cluster_ref, window_size, EVENT_COUNT).await?;
    }

    Ok(())
}

async fn test_subscription_with_window_size(
    cluster_ref: &ActorRef<ClusterActor>,
    window_size: u64,
    expected_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let subscription_id = Uuid::new_v4();
    let (last_ack_tx, last_ack_rx) = watch::channel(None);
    let (update_tx, mut update_rx) = mpsc::unbounded_channel();

    cluster_ref
        .ask(Subscribe {
            subscription_id,
            matcher: SubscriptionMatcher::AllPartitions {
                from_sequences: FromSequences::Partitions {
                    from_sequences: HashMap::new(),
                    fallback: Some(0),
                },
            },
            last_ack_rx,
            update_tx,
            window_size,
        })
        .await?;

    let mut count = 0;
    let mut received_events = HashSet::new();
    let mut partitions = HashMap::new();
    let mut last_event_id = Uuid::nil();

    // Read with intelligent timeout
    while count < expected_count {
        let timeout = if count < expected_count - 1 {
            Duration::from_secs(10) // Plenty of time for expected events
        } else {
            Duration::from_millis(1) // Short check for "done"
        };

        match tokio::time::timeout(timeout, update_rx.recv()).await {
            Ok(Some(SubscriptionEvent::Record { record, cursor, .. })) => {
                assert_ne!(last_event_id, record.event_id);
                last_event_id = record.event_id;
                assert!(received_events.insert(record.event_id));
                match partitions.entry(record.partition_id) {
                    Entry::Occupied(mut entry) => {
                        assert_eq!(
                            record.partition_sequence.checked_sub(1),
                            Some(*entry.get()),
                            "Partition ID {}",
                            record.partition_id
                        );
                        *entry.get_mut() = record.partition_sequence;
                    }
                    Entry::Vacant(entry) => {
                        assert_eq!(record.partition_sequence, 0);
                        entry.insert(0);
                    }
                }
                last_ack_tx.send(Some(cursor))?;
                count += 1;
            }
            Ok(Some(ev)) => panic!("Unexpected subscription event: {ev:?}"),
            Ok(None) => panic!("Channel closed before receiving all events"),
            Err(_) if count == expected_count => break,
            Err(_) => {
                eprintln!(
                    "\n=== TIMEOUT: Received {}/{} events ===",
                    count, expected_count
                );
                eprintln!("Partition status:");
                for partition_id in 0..32 {
                    if let Some(last_seq) = partitions.get(&partition_id) {
                        eprintln!(
                            "  Partition {}: last received seq {}",
                            partition_id, last_seq
                        );
                    }
                }

                panic!(
                    "Timeout after {timeout:?} waiting for events (got {count}/{expected_count})"
                )
            }
        }
    }

    assert_eq!(count, expected_count);
    Ok(())
}
