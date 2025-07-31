use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use libp2p::bytes::BytesMut;
use sierradb::database::{NewEvent, Transaction};
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash, uuid_v7_with_partition_hash};
use sierradb_cluster::ClusterActor;
use sierradb_cluster::read::ReadEvent;
use sierradb_cluster::write::execute::ExecuteTransaction;
use smallvec::{SmallVec, smallvec};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tracing::warn;
use uuid::Uuid;

use crate::request::{EAppend, EGet, EMAppend, EPScan, EPSeq, ESVer, EScan, ESub, FromArgs};
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
                self.handle_request(value).await?.encode(&mut self.write);
                self.socket.write_all(&self.write).await?;
                self.socket.flush().await?;
                self.write.clear();
            }

            if self.socket.read_buf(&mut self.read).await? == 0 && self.read.is_empty() {
                return Ok(());
            }
        }
    }

    async fn handle_request(&mut self, request: Value) -> io::Result<Value> {
        match request {
            Value::Array(items) => {
                if items.is_empty() {
                    return Ok(Value::Error("Empty command".into()));
                }

                macro_rules! handle_req {
                    ($cmd:ty, $handle:ident) => {
                        match <$cmd>::from_args(&items[1..]) {
                            Ok(cmd) => self.$handle(cmd).await,
                            Err(err) => Ok(err),
                        }
                    };
                }

                match &items[0].as_str() {
                    Ok(cmd) => match cmd.to_uppercase().as_str() {
                        "EAPPEND" => handle_req!(EAppend, handle_eappend),
                        "EMAPPEND" => handle_req!(EMAppend, handle_emappend),
                        "EGET" => handle_req!(EGet, handle_eget),
                        "EPSCAN" => handle_req!(EPScan, handle_epscan),
                        "ESCAN" => handle_req!(EScan, handle_escan),
                        "EPSEQ" => handle_req!(EPSeq, handle_epseq),
                        "ESVER" => handle_req!(ESVer, handle_esver),
                        "ESUB" => handle_req!(ESub, handle_esub),
                        "PING" => Ok(Value::String("PONG".to_string())),
                        _ => Ok(Value::Error(format!("Unknown command: {cmd}"))),
                    },
                    _ => Ok(Value::Error("Expected command name as bulk string".into())),
                }
            }
            _ => Ok(Value::Error("Expected array".into())),
        }
    }

    async fn handle_eappend(
        &mut self,
        EAppend {
            stream_id,
            event_name,
            event_id,
            partition_key,
            expected_version,
            payload,
            metadata,
        }: EAppend,
    ) -> io::Result<Value> {
        let partition_key = partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes()));
        let partition_hash = uuid_to_partition_hash(partition_key);
        let event_id = event_id.unwrap_or_else(|| uuid_v7_with_partition_hash(partition_hash));

        let partition_id = partition_hash % self.num_partitions;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| io::Error::other("system time error"))?
            .as_nanos() as u64;

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

        match self
            .cluster_ref
            .ask(ExecuteTransaction::new(transaction))
            .await
            .map_err(io::Error::other)
        {
            Ok(result) => {
                let mut stream_versions = result.stream_versions.into_iter();
                let (_, stream_version) = stream_versions.next().unwrap();
                debug_assert_eq!(stream_versions.next(), None);
                debug_assert_eq!(
                    result.first_partition_sequence,
                    result.last_partition_sequence
                );

                let response = vec![
                    Value::String(event_id.to_string()),
                    Value::String(partition_key.to_string()),
                    Value::Integer(partition_id as i64),
                    Value::Integer(result.first_partition_sequence as i64),
                    Value::Integer(stream_version as i64),
                    Value::Integer(timestamp as i64),
                ];

                Ok(Value::Array(response))
            }
            Err(err) => Ok(Value::Error(err.to_string())),
        }
    }

    async fn handle_emappend(
        &mut self,
        EMAppend {
            partition_key,
            events,
        }: EMAppend,
    ) -> io::Result<Value> {
        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % self.num_partitions;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| io::Error::other("system time error"))?
            .as_nanos() as u64;

        let events: SmallVec<[_; 4]> = events
            .into_iter()
            .map(|event| {
                let event_id = event
                    .event_id
                    .unwrap_or_else(|| uuid_v7_with_partition_hash(partition_hash));
                NewEvent {
                    event_id,
                    stream_id: event.stream_id,
                    stream_version: event.expected_version,
                    event_name: event.event_name,
                    timestamp,
                    metadata: event.metadata,
                    payload: event.payload,
                }
            })
            .collect();
        let event_ids: SmallVec<[_; 4]> = events.iter().map(|event| event.event_id).collect();

        let transaction = match Transaction::new(partition_key, partition_id, events) {
            Ok(transaction) => transaction,
            Err(err) => return Ok(Value::Error(err.to_string())),
        };

        match self
            .cluster_ref
            .ask(ExecuteTransaction::new(transaction))
            .await
            .map_err(io::Error::other)
        {
            Ok(result) => {
                let stream_versions = result.stream_versions.into_iter();
                let event_infos = event_ids
                    .into_iter()
                    .zip(stream_versions)
                    .map(|(event_id, (stream_id, stream_version))| {
                        Value::Array(vec![
                            Value::String(event_id.to_string()),
                            Value::String(stream_id.to_string()),
                            Value::Integer(stream_version as i64),
                        ])
                    })
                    .collect();

                let response = vec![
                    Value::String(partition_key.to_string()),
                    Value::Integer(partition_id as i64),
                    Value::Integer(result.first_partition_sequence as i64),
                    Value::Integer(result.last_partition_sequence as i64),
                    Value::Integer(timestamp as i64),
                    Value::Array(event_infos),
                ];

                Ok(Value::Array(response))
            }
            Err(err) => Ok(Value::Error(err.to_string())),
        }
    }

    async fn handle_eget(&mut self, EGet { event_id }: EGet) -> io::Result<Value> {
        match self.cluster_ref.ask(ReadEvent::new(event_id)).await {
            Ok(Some(record)) => {
                let response = vec![
                    Value::String(record.event_id.to_string()),
                    Value::String(record.partition_key.to_string()),
                    Value::Integer(record.partition_id as i64),
                    Value::String(record.transaction_id.to_string()),
                    Value::Integer(record.partition_sequence as i64),
                    Value::Integer(record.stream_version as i64),
                    Value::Integer(record.timestamp as i64),
                    Value::String(record.stream_id.to_string()),
                    Value::String(record.event_name),
                    Value::Bulk(record.metadata),
                    Value::Bulk(record.payload),
                ];

                Ok(Value::Array(response))
            }
            Ok(None) => Ok(Value::Null),
            Err(err) => Ok(Value::Error(err.to_string())),
        }
    }

    async fn handle_epscan(&mut self, EPScan { .. }: EPScan) -> io::Result<Value> {
        Ok(Value::String("Not implemented".to_string()))
    }

    async fn handle_escan(&mut self, EScan { .. }: EScan) -> io::Result<Value> {
        Ok(Value::String("Not implemented".to_string()))
    }

    async fn handle_epseq(&mut self, EPSeq { .. }: EPSeq) -> io::Result<Value> {
        Ok(Value::String("Not implemented".to_string()))
    }

    async fn handle_esver(&mut self, ESVer { .. }: ESVer) -> io::Result<Value> {
        Ok(Value::String("Not implemented".to_string()))
    }

    async fn handle_esub(&mut self, ESub {}: ESub) -> io::Result<Value> {
        Ok(Value::String("Not implemented".to_string()))
    }
}
