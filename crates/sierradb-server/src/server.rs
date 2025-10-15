use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::ActorRef;
use libp2p::bytes::BytesMut;
use sierradb::database::{NewEvent, Transaction};
use sierradb::id::{NAMESPACE_PARTITION_KEY, uuid_to_partition_hash, uuid_v7_with_partition_hash};
use sierradb_cluster::swarm::Swarm;
use smallvec::smallvec;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tracing::warn;
use uuid::Uuid;

use crate::request::{Append, FromArgs, SRead};
use crate::value::{Value, ValueDecoder};

pub struct Server {
    swarm_ref: ActorRef<Swarm>,
    num_partitions: u16,
}

impl Server {
    pub fn new(swarm_ref: ActorRef<Swarm>, num_partitions: u16) -> Self {
        Server {
            swarm_ref,
            num_partitions,
        }
    }

    pub async fn listen(&mut self, addr: impl ToSocketAddrs) -> io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    let swarm_ref = self.swarm_ref.clone();
                    let num_partitions = self.num_partitions;
                    tokio::spawn(async move {
                        let res = Conn::new(swarm_ref, num_partitions, socket).run().await;
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
    swarm_ref: ActorRef<Swarm>,
    num_partitions: u16,
    socket: TcpStream,
    read: BytesMut,
    write: BytesMut,
    decoder: ValueDecoder,
}

impl Conn {
    fn new(swarm_ref: ActorRef<Swarm>, num_partitions: u16, socket: TcpStream) -> Self {
        let read = BytesMut::new();
        let write = BytesMut::new();
        let decoder = ValueDecoder::default();

        Conn {
            swarm_ref,
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

                match &items[0].as_str() {
                    Ok(cmd) => match cmd.to_lowercase().as_str() {
                        "append" => match Append::from_args(&items[1..]) {
                            Ok(append) => self.handle_append(append).await,
                            Err(err) => Ok(err),
                        },
                        "get" => self.handle_get(&items).await,
                        "pread" => self.handle_read_partition(&items).await,
                        "sread" => match SRead::from_args(&items[1..]) {
                            Ok(sread) => self.handle_read_stream(sread).await,
                            Err(err) => Ok(err),
                        },
                        "pinfo" => todo!(),
                        "sinfo" => todo!(),
                        "subscribe" => todo!(),
                        _ => Ok(Value::Error(format!("Unknown command: {}", cmd))),
                    },
                    _ => Ok(Value::Error("Expected command name as bulk string".into())),
                }
            }
            _ => Ok(Value::Error("Expected array".into())),
        }
    }

    async fn handle_append(
        &mut self,
        Append {
            stream_id,
            event_name,
            event_id,
            partition_key,
            version,
            payload,
            metadata,
        }: Append,
    ) -> io::Result<Value> {
        let partition_key = partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes()));
        let event_id = event_id
            .unwrap_or_else(|| uuid_v7_with_partition_hash(uuid_to_partition_hash(partition_key)));

        let partition_hash = uuid_to_partition_hash(partition_key);
        let partition_id = partition_hash % self.num_partitions;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| io::Error::other("system time error"))?
            .as_nanos() as u64;

        let transaction = match Transaction::new(
            uuid_to_partition_hash(partition_key),
            smallvec![NewEvent {
                event_id,
                partition_key,
                partition_id,
                stream_id,
                stream_version: version,
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
            .swarm_ref
            .ask(transaction)
            .await
            .map_err(io::Error::other)?
            .await
            .map_err(io::Error::other)?
        {
            Ok(result) => {
                let mut stream_versions = result.stream_versions.into_iter();
                let (_, stream_version) = stream_versions.next().unwrap();
                debug_assert_eq!(stream_versions.next(), None);

                let response = vec![
                    Value::String(event_id.to_string()),
                    Value::String(partition_key.to_string()),
                    Value::Integer(partition_id as i64),
                    Value::Integer(result.partition_sequence as i64),
                    Value::Integer(stream_version as i64),
                    Value::Integer(timestamp as i64),
                ];

                Ok(Value::Array(response))
            }
            Err(err) => Ok(Value::Error(err.to_string())),
        }
    }

    async fn handle_get(&mut self, _args: &[Value]) -> io::Result<Value> {
        Ok(Value::String("Not implemented".to_string()))
    }

    async fn handle_read_partition(&mut self, _args: &[Value]) -> io::Result<Value> {
        Ok(Value::String("Not implemented".to_string()))
    }

    async fn handle_read_stream(
        &mut self,
        SRead {
            stream_id,
            partition_key,
        }: SRead,
    ) -> io::Result<Value> {
        let partition_key = partition_key
            .unwrap_or_else(|| Uuid::new_v5(&NAMESPACE_PARTITION_KEY, stream_id.as_bytes()));

        Ok(Value::String(format!("Not implemented: {partition_key}")))
    }
}
