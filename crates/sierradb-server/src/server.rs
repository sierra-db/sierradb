use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use libp2p::bytes::BytesMut;
use sierradb::database::{NewEvent, Transaction};
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash, uuid_v7_with_partition_hash};
use sierradb_cluster::ClusterActor;
use sierradb_cluster::read::{GetStreamVersion, ReadEvent, ReadPartition, ReadStream};
use sierradb_cluster::write::execute::ExecuteTransaction;
use sierradb_protocol::ErrorCode;
use smallvec::{SmallVec, smallvec};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tracing::warn;
use uuid::Uuid;

use crate::error::MapRedisError;
use crate::request::{
    EAppend, EGet, EMAppend, EPScan, EPSeq, ESVer, EScan, ESub, FromArgs, Hello, RangeValue,
};
use crate::value::{Value, ValueDecoder};

pub struct Server {
    cluster_ref: ActorRef<ClusterActor>,
    num_partitions: u16,
}

impl Server {
    pub fn new(cluster_ref: ActorRef<ClusterActor>, num_partitions: u16) -> Self {
        Server {
            cluster_ref,
            num_partitions,
        }
    }

    pub async fn listen(&mut self, addr: impl ToSocketAddrs) -> io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    let cluster_ref = self.cluster_ref.clone();
                    let num_partitions = self.num_partitions;
                    tokio::spawn(async move {
                        let res = Conn::new(cluster_ref, num_partitions, socket).run().await;
                        if let Err(err) = res {
                            warn!("connection error: {err}");
                        }
                    });
                }
                Err(err) => warn!("failed to accept connection: {err}"),
            }
        }
    }
}

struct Conn {
    cluster_ref: ActorRef<ClusterActor>,
    num_partitions: u16,
    socket: TcpStream,
    read: BytesMut,
    write: BytesMut,
    decoder: ValueDecoder,
}

impl Conn {
    fn new(cluster_ref: ActorRef<ClusterActor>, num_partitions: u16, socket: TcpStream) -> Self {
        let read = BytesMut::new();
        let write = BytesMut::new();
        let decoder = ValueDecoder::default();

        Conn {
            cluster_ref,
            socket,
            read,
            write,
            decoder,
            num_partitions,
        }
    }

    async fn run(mut self) -> io::Result<()> {
        loop {
            if let Some(value) = self.decoder.try_decode(&mut self.read)? {
                let response = match self.handle_request(value).await {
                    Ok(value) => value,
                    Err(error_value) => error_value,
                };
                response.encode(&mut self.write);
                self.socket.write_all(&self.write).await?;
                self.socket.flush().await?;
                self.write.clear();
            }

            if self.socket.read_buf(&mut self.read).await? == 0 && self.read.is_empty() {
                return Ok(());
            }
        }
    }

    async fn handle_request(&mut self, request: Value) -> Result<Value, Value> {
        match request {
            Value::Array(items) => {
                if items.is_empty() {
                    return Ok(Value::Error("Empty command".into()));
                }

                macro_rules! handle_req {
                    ($cmd:ty, $handle:ident) => {
                        match <$cmd>::from_args(&items[1..]) {
                            Ok(cmd) => match self.$handle(cmd).await {
                                Ok(ok) => Ok(ok),
                                Err(err) => Ok(err),
                            },
                            Err(err) => Ok(err),
                        }
                    };
                }

                match &items[0].as_str() {
                    Ok(cmd) => match cmd.to_uppercase().as_str() {
                        "HELLO" => handle_req!(Hello, handle_hello),
                        "PING" => self.handle_ping(),
                        "EAPPEND" => handle_req!(EAppend, handle_eappend),
                        "EMAPPEND" => handle_req!(EMAppend, handle_emappend),
                        "EGET" => handle_req!(EGet, handle_eget),
                        "EPSCAN" => handle_req!(EPScan, handle_epscan),
                        "ESCAN" => handle_req!(EScan, handle_escan),
                        "EPSEQ" => handle_req!(EPSeq, handle_epseq),
                        "ESVER" => handle_req!(ESVer, handle_esver),
                        "ESUB" => handle_req!(ESub, handle_esub),
                        _ => Ok(Value::Error(format!("Unknown command: {cmd}"))),
                    },
                    _ => Ok(Value::Error("Expected command name as bulk string".into())),
                }
            }
            _ => Ok(Value::Error("Expected array".into())),
        }
    }

    async fn handle_hello(&mut self, Hello { version: _ }: Hello) -> Result<Value, Value> {
        Ok(Value::Array(vec![
            Value::Integer(3),                     // protocol version
            Value::String("SIERRADB".to_string()), // server name
            Value::Integer(self.num_partitions as i64),
        ]))
    }

    fn handle_ping(&mut self) -> Result<Value, Value> {
        Ok(Value::String("PONG".to_string()))
    }

    async fn handle_eappend(
        &mut self,
        EAppend {
            stream_id,
            event_name,
            event_id,
            partition_key,
            expected_version,
            timestamp,
            payload,
            metadata,
        }: EAppend,
    ) -> Result<Value, Value> {
        let partition_key = partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes()));
        let partition_hash = uuid_to_partition_hash(partition_key);
        let event_id = event_id.unwrap_or_else(|| uuid_v7_with_partition_hash(partition_hash));

        let partition_id = partition_hash % self.num_partitions;
        let timestamp = timestamp
            .map(|timestamp| {
                timestamp.checked_mul(1_000_000).ok_or(Value::Error(
                    ErrorCode::InvalidArg
                        .with_message(
                            "Invalid timestamp format: expected milliseconds, got nanoseconds",
                        )
                        .to_string(),
                ))
            })
            .unwrap_or_else(|| {
                Ok(SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_redis_err()?
                    .as_nanos() as u64)
            })?;

        let transaction = match Transaction::new(
            partition_key,
            partition_id,
            smallvec![NewEvent {
                event_id,
                stream_id,
                stream_version: expected_version,
                event_name,
                timestamp,
                metadata,
                payload,
            }],
        ) {
            Ok(transaction) => transaction,
            Err(err) => return Ok(Value::Error(err.to_string())),
        };

        let append = self
            .cluster_ref
            .ask(ExecuteTransaction::new(transaction))
            .await
            .map_redis_err()?;

        let mut stream_versions = append.stream_versions.into_iter();
        let (_, stream_version) = stream_versions.next().unwrap();
        debug_assert_eq!(stream_versions.next(), None);
        debug_assert_eq!(
            append.first_partition_sequence,
            append.last_partition_sequence
        );

        let response = vec![
            Value::String(event_id.to_string()),
            Value::String(partition_key.to_string()),
            Value::Integer(partition_id as i64),
            Value::Integer(append.first_partition_sequence as i64),
            Value::Integer(stream_version as i64),
            Value::Integer((timestamp / 1_000_000) as i64),
        ];

        Ok(Value::Array(response))
    }

    async fn handle_emappend(
        &mut self,
        EMAppend {
            partition_key,
            events,
        }: EMAppend,
    ) -> Result<Value, Value> {
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % self.num_partitions;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_redis_err()?
            .as_nanos() as u64;

        let events: SmallVec<[_; 4]> = events
            .into_iter()
            .map(|event| {
                let event_id = event
                    .event_id
                    .unwrap_or_else(|| uuid_v7_with_partition_hash(partition_hash));
                let timestamp = event
                    .timestamp
                    .map(|timestamp| {
                        timestamp.checked_mul(1_000_000).ok_or(Value::Error(
                            ErrorCode::InvalidArg.with_message("Invalid timestamp format: expected milliseconds, got nanoseconds").to_string(),
                        ))
                    })
                    .transpose()?
                    .unwrap_or(now);
                Ok(NewEvent {
                    event_id,
                    stream_id: event.stream_id,
                    stream_version: event.expected_version,
                    event_name: event.event_name,
                    timestamp,
                    metadata: event.metadata,
                    payload: event.payload,
                })
            })
            .collect::<Result<_, Value>>()?;
        let event_ids_timestamps: SmallVec<[_; 4]> = events
            .iter()
            .map(|event| (event.event_id, event.timestamp))
            .collect();

        let transaction = match Transaction::new(partition_key, partition_id, events) {
            Ok(transaction) => transaction,
            Err(err) => return Ok(Value::Error(err.to_string())),
        };

        let result = self
            .cluster_ref
            .ask(ExecuteTransaction::new(transaction))
            .await
            .map_redis_err()?;

        let stream_versions = result.stream_versions.into_iter();
        let event_infos = event_ids_timestamps
            .into_iter()
            .zip(stream_versions)
            .map(|((event_id, timestamp), (stream_id, stream_version))| {
                Value::Array(vec![
                    Value::String(event_id.to_string()),
                    Value::String(stream_id.to_string()),
                    Value::Integer(stream_version as i64),
                    Value::Integer((timestamp / 1_000_000) as i64),
                ])
            })
            .collect();

        let response = vec![
            Value::String(partition_key.to_string()),
            Value::Integer(partition_id as i64),
            Value::Integer(result.first_partition_sequence as i64),
            Value::Integer(result.last_partition_sequence as i64),
            Value::Array(event_infos),
        ];

        Ok(Value::Array(response))
    }

    async fn handle_eget(&mut self, EGet { event_id }: EGet) -> Result<Value, Value> {
        match self
            .cluster_ref
            .ask(ReadEvent::new(event_id))
            .await
            .map_redis_err()?
        {
            Some(record) => {
                let response = vec![
                    Value::String(record.event_id.to_string()),
                    Value::String(record.partition_key.to_string()),
                    Value::Integer(record.partition_id as i64),
                    Value::String(record.transaction_id.to_string()),
                    Value::Integer(record.partition_sequence as i64),
                    Value::Integer(record.stream_version as i64),
                    Value::Integer((record.timestamp / 1_000_000) as i64),
                    Value::String(record.stream_id.to_string()),
                    Value::String(record.event_name),
                    Value::Bulk(record.metadata),
                    Value::Bulk(record.payload),
                ];

                Ok(Value::Array(response))
            }
            None => Ok(Value::Null),
        }
    }

    async fn handle_epscan(
        &mut self,
        EPScan {
            partition,
            start_sequence,
            end_sequence,
            count,
        }: EPScan,
    ) -> Result<Value, Value> {
        let end_sequence = match end_sequence {
            RangeValue::Start => {
                return Err(Value::Error(
                    ErrorCode::Syntax.with_message("end_sequence cannot be '-'"),
                ));
            }
            RangeValue::End => None,
            RangeValue::Value(n) => Some(n),
        };

        let records = self
            .cluster_ref
            .ask(ReadPartition {
                partition_id: partition.into_partition_id(self.num_partitions),
                start_sequence,
                end_sequence,
                count: count.unwrap_or(100),
            })
            .await
            .map_redis_err()?;

        let events = records
            .events
            .into_iter()
            .map(|event| {
                Value::Array(vec![
                    Value::String(event.event_id.to_string()),
                    Value::String(event.partition_key.to_string()),
                    Value::Integer(event.partition_id as i64),
                    Value::String(event.transaction_id.to_string()),
                    Value::Integer(event.partition_sequence as i64),
                    Value::Integer(event.stream_version as i64),
                    Value::Integer((event.timestamp / 1_000_000) as i64),
                    Value::String(event.stream_id.to_string()),
                    Value::String(event.event_name),
                    Value::Bulk(event.metadata),
                    Value::Bulk(event.payload),
                ])
            })
            .collect();
        let response = vec![Value::Boolean(records.has_more), Value::Array(events)];

        Ok(Value::Array(response))
    }

    async fn handle_escan(
        &mut self,
        EScan {
            stream_id,
            partition_key,
            start_version,
            end_version,
            count,
        }: EScan,
    ) -> Result<Value, Value> {
        let partition_key = partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes()));
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % self.num_partitions;

        let end_version = match end_version {
            RangeValue::Start => {
                return Err(Value::Error(
                    ErrorCode::Syntax.with_message("end_version cannot be '-'"),
                ));
            }
            RangeValue::End => None,
            RangeValue::Value(n) => Some(n),
        };

        let records = self
            .cluster_ref
            .ask(ReadStream {
                stream_id,
                partition_id,
                start_version,
                end_version,
                count: count.unwrap_or(100),
            })
            .await
            .map_redis_err()?;

        let events = records
            .events
            .into_iter()
            .map(|event| {
                Value::Array(vec![
                    Value::String(event.event_id.to_string()),
                    Value::String(event.partition_key.to_string()),
                    Value::Integer(event.partition_id as i64),
                    Value::String(event.transaction_id.to_string()),
                    Value::Integer(event.partition_sequence as i64),
                    Value::Integer(event.stream_version as i64),
                    Value::Integer((event.timestamp / 1_000_000) as i64),
                    Value::String(event.stream_id.to_string()),
                    Value::String(event.event_name),
                    Value::Bulk(event.metadata),
                    Value::Bulk(event.payload),
                ])
            })
            .collect();
        let response = vec![Value::Boolean(records.has_more), Value::Array(events)];

        Ok(Value::Array(response))
    }

    async fn handle_epseq(&mut self, EPSeq { partition: _ }: EPSeq) -> Result<Value, Value> {
        // let seq = match self
        //     .watermarks
        //     .get(partition.into_partition_id(self.num_partitions))
        // {
        //     Some(watermark) => watermark.get().checked_sub(1),
        //     None => {
        //         let seq = self
        //             .cluster_ref
        //             .ask(GetPartitionSequence {
        //                 skip_local_check: true,
        //             })
        //             .await
        //             .map_redis_err()?;
        //     }
        // };

        // Ok(seq.map(|s| Value::Integer(s as i64)).unwrap_or(Value::Null))
        todo!()
    }

    async fn handle_esver(
        &mut self,
        ESVer {
            stream_id,
            partition_key,
        }: ESVer,
    ) -> Result<Value, Value> {
        let partition_key = partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes()));
        let partition_id = uuid_to_partition_hash(partition_key) % self.num_partitions;

        let version = self
            .cluster_ref
            .ask(GetStreamVersion {
                partition_id,
                stream_id,
            })
            .await
            .map_redis_err()?;

        match version {
            Some(version) => Ok(Value::Integer(version as i64)),
            None => Ok(Value::Null),
        }
    }

    async fn handle_esub(&mut self, ESub {}: ESub) -> Result<Value, Value> {
        Ok(Value::String("Not implemented".to_string()))
    }
}
