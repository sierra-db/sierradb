use std::{
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use kameo::Actor;
use libp2p::identity::Keypair;
use sierradb::{
    StreamId,
    database::{DatabaseBuilder, ExpectedVersion, NewEvent, Transaction},
    id::{uuid_to_partition_hash, uuid_v7_with_partition_hash},
};
use sierradb_cluster::{
    ClusterActor, ClusterArgs, ResetCluster,
    read::GetPartitionSequence,
    subscription::{FromSequences, Subscribe, SubscriptionEvent, SubscriptionMatcher},
    write::execute::ExecuteTransaction,
};
use smallvec::smallvec;
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

#[tokio::test]
async fn test_subscriptions() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let database = DatabaseBuilder::new()
        .segment_size(256_000_000)
        .total_buckets(4)
        .bucket_ids_from_range(0..4)
        .flush_interval_duration(Duration::ZERO)
        .flush_interval_events(1)
        .open(dir.path())?;
    let mut tempdirs = vec![dir];

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

    for window_size in [10_000, 100, 10] {
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

        let mut partition_sequences = HashMap::new();
        let mut stream_versions = HashMap::new();
        for _ in 0..2_000 {
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
            let _append = cluster_ref
                .ask(ExecuteTransaction::new(transaction))
                .await?;
        }

        let mut count = 0;
        let mut received_events = HashSet::new();
        let mut last_event_id = Uuid::nil();
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(1), update_rx.recv()).await
        {
            if let SubscriptionEvent::Record { record, cursor, .. } = event {
                assert_ne!(last_event_id, record.event_id);
                last_event_id = record.event_id;
                assert!(received_events.insert(record.event_id));
                last_ack_tx.send(Some(cursor))?;
                count += 1;
            }
        }

        assert_eq!(count, 2_000);

        let mut total = 0;
        for partition_id in 0..32 {
            total += cluster_ref
                .ask(GetPartitionSequence { partition_id })
                .await?
                .map(|n| n + 1)
                .unwrap_or(0);
        }

        assert_eq!(total, 2_000);

        tokio::time::sleep(Duration::from_millis(10)).await;

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
        while let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(1), update_rx.recv()).await
        {
            if let SubscriptionEvent::Record { cursor, .. } = event {
                last_ack_tx.send(Some(cursor))?;
                count += 1;
            }
        }

        assert_eq!(count, 2_000);

        let dir = tempfile::tempdir()?;
        let database = DatabaseBuilder::new()
            .segment_size(256_000_000)
            .total_buckets(4)
            .bucket_ids_from_range(0..4)
            .flush_interval_duration(Duration::ZERO)
            .flush_interval_events(1)
            .open(dir.path())?;
        tempdirs.push(dir);
        cluster_ref.ask(ResetCluster { database }).await?;
    }

    Ok(())
}
