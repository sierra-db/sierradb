use std::collections::HashMap;

use kameo::actor::ActorRef;
use libp2p::bytes::BytesMut;
use redis_protocol::resp3;
use redis_protocol::resp3::decode::complete::decode_bytes_mut;
use redis_protocol::resp3::types::BytesFrame;
use sierradb::bucket::segment::EventRecord;
use sierradb_cluster::ClusterActor;
use sierradb_cluster::subscription::SubscriptionEvent;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, watch};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::request::{Command, encode_event, number, simple_str};

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

pub struct Conn {
    pub cluster_ref: ActorRef<ClusterActor>,
    pub num_partitions: u16,
    pub socket: TcpStream,
    pub read: BytesMut,
    pub write: BytesMut,
    pub subscription_channel: Option<(
        mpsc::WeakUnboundedSender<SubscriptionEvent>,
        mpsc::UnboundedReceiver<SubscriptionEvent>,
    )>,
    pub subscriptions: HashMap<Uuid, watch::Sender<Option<u64>>>,
}

impl Conn {
    fn new(cluster_ref: ActorRef<ClusterActor>, num_partitions: u16, socket: TcpStream) -> Self {
        let read = BytesMut::new();
        let write = BytesMut::new();

        Conn {
            cluster_ref,
            socket,
            read,
            write,
            num_partitions,
            subscription_channel: None,
            subscriptions: HashMap::new(),
        }
    }

    async fn run(mut self) -> io::Result<()> {
        loop {
            match &mut self.subscription_channel {
                Some((_, rx)) => {
                    tokio::select! {
                        res = self.socket.read_buf(&mut self.read) => {
                            match res {
                                Ok(bytes_read) => {
                                    if bytes_read == 0 && self.read.is_empty() {
                                        // Clean up subscriptions on disconnect
                                        self.cleanup_subscriptions();
                                        return Ok(());
                                    }

                                    // Try to decode and handle requests
                                    while let Some((frame, _, _)) =
                                        decode_bytes_mut(&mut self.read).map_err(io::Error::other)?
                                    {
                                        let response = self.handle_request(frame).await?;
                                        if let Some(resp) = response {
                                            resp3::encode::complete::extend_encode(&mut self.write, &resp, false)
                                                .map_err(io::Error::other)?;

                                            self.socket.write_all(&self.write).await?;
                                            self.socket.flush().await?;
                                            self.write.clear();
                                        }
                                    }
                                }
                                Err(err) => return Err(err),
                            }
                        }
                        msg = rx.recv() => {
                            match msg {
                                Some(SubscriptionEvent::Record { subscription_id, cursor, record }) => self.send_subscription_event(subscription_id, cursor, record).await?,
                                Some(SubscriptionEvent::Error { subscription_id, error }) => {
                                    warn!(%subscription_id, "subscription error: {error}");
                                }
                                Some(SubscriptionEvent::Closed { subscription_id }) => {
                                    debug!(
                                        subscription_id = %subscription_id,
                                        "closed subscription"
                                    );
                                    self.subscriptions.remove(&subscription_id);
                                    if self.subscriptions.is_empty() {
                                        self.cleanup_subscriptions();
                                    }
                                }
                                None => self.cleanup_subscriptions(),
                            }
                        }
                    }
                }
                None => {
                    // Not in subscription mode - block normally on socket reads
                    let bytes_read = self.socket.read_buf(&mut self.read).await?;
                    if bytes_read == 0 && self.read.is_empty() {
                        return Ok(());
                    }

                    // Try to decode and handle requests
                    while let Some((frame, _, _)) =
                        decode_bytes_mut(&mut self.read).map_err(io::Error::other)?
                    {
                        let response = self.handle_request(frame).await?;
                        if let Some(resp) = response {
                            resp3::encode::complete::extend_encode(&mut self.write, &resp, false)
                                .map_err(io::Error::other)?;

                            self.socket.write_all(&self.write).await?;
                            self.socket.flush().await?;
                            self.write.clear();
                        }
                    }
                }
            }
        }
    }

    fn cleanup_subscriptions(&mut self) {
        self.subscriptions.clear();
        self.subscription_channel = None;
    }

    async fn send_subscription_event(
        &mut self,
        subscription_id: Uuid,
        cursor: u64,
        record: EventRecord,
    ) -> io::Result<()> {
        resp3::encode::complete::extend_encode(
            &mut self.write,
            &BytesFrame::Push {
                data: vec![
                    simple_str("message"),
                    simple_str(subscription_id.to_string()),
                    number(cursor as i64),
                    encode_event(record),
                ],
                attributes: None,
            },
            false,
        )
        .map_err(io::Error::other)?;

        self.socket.write_all(&self.write).await?;
        self.socket.flush().await?;
        self.write.clear();

        Ok(())
    }

    async fn handle_request(&mut self, frame: BytesFrame) -> Result<Option<BytesFrame>, io::Error> {
        match frame {
            BytesFrame::Array { data, .. } => {
                if data.is_empty() {
                    return Ok(Some(BytesFrame::SimpleError {
                        data: "empty command".into(),
                        attributes: None,
                    }));
                }

                let cmd = match Command::try_from(&data[0]) {
                    Ok(cmd) => cmd,
                    Err(err) => {
                        return Ok(Some(BytesFrame::SimpleError {
                            data: err.into(),
                            attributes: None,
                        }));
                    }
                };
                let args = &data[1..];
                cmd.handle(args, self).await
            }
            _ => Ok(Some(BytesFrame::SimpleError {
                data: "expected array command".into(),
                attributes: None,
            })),
        }
    }
}
