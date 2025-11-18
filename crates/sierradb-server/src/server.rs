use std::collections::HashMap;
use std::sync::Arc;

use kameo::actor::ActorRef;
use libp2p::bytes::BytesMut;
use redis_protocol::resp3;
use redis_protocol::resp3::decode::complete::decode_bytes_mut;
use redis_protocol::resp3::types::BytesFrame;
use sierradb::bucket::BucketId;
use sierradb::bucket::segment::EventRecord;
use sierradb::cache::SegmentBlockCache;
use sierradb_cluster::ClusterActor;
use sierradb_cluster::subscription::SubscriptionEvent;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::request::{Command, encode_event, number, simple_str};

pub struct Server {
    cluster_ref: ActorRef<ClusterActor>,
    caches: Arc<HashMap<BucketId, Arc<SegmentBlockCache>>>,
    num_partitions: u16,
    cache_capacity_bytes: usize,
    shutdown: CancellationToken,
    conns: JoinSet<io::Result<()>>,
}

impl Server {
    pub fn new(
        cluster_ref: ActorRef<ClusterActor>,
        caches: Arc<HashMap<BucketId, Arc<SegmentBlockCache>>>,
        num_partitions: u16,
        cache_capacity_bytes: usize,
        shutdown: CancellationToken,
    ) -> Self {
        Server {
            cluster_ref,
            caches,
            num_partitions,
            cache_capacity_bytes,
            shutdown,
            conns: JoinSet::new(),
        }
    }

    pub async fn listen(mut self, addr: impl ToSocketAddrs) -> io::Result<JoinSet<io::Result<()>>> {
        let listener = TcpListener::bind(addr).await?;
        loop {
            tokio::select! {
                res = listener.accept() => {
                    match res {
                        Ok((stream, _)) => {
                            stream.set_nodelay(true)?;
                            let cluster_ref = self.cluster_ref.clone();
                            let caches = self.caches.clone();
                            let num_partitions = self.num_partitions;
                            let cache_capacity_bytes = self.cache_capacity_bytes;
                            let shutdown = self.shutdown.clone();
                            self.conns.spawn(async move {
                                let res = Conn::new(
                                    cluster_ref,
                                    caches,
                                    num_partitions,
                                    cache_capacity_bytes,
                                    stream,
                                    shutdown,
                                )
                                .run()
                                .await;
                                if let Err(err) = &res {
                                    warn!("connection error: {err}");
                                }
                                res
                            });
                        }
                        Err(err) => warn!("failed to accept connection: {err}"),
                    }
                }
                _ = self.shutdown.cancelled() => {
                    return Ok(self.conns);
                }
            }
        }
    }
}

pub struct Conn {
    pub cluster_ref: ActorRef<ClusterActor>,
    pub caches: Arc<HashMap<BucketId, Arc<SegmentBlockCache>>>,
    pub num_partitions: u16,
    pub cache_capacity_bytes: usize,
    pub stream: TcpStream,
    pub shutdown: CancellationToken,
    pub read: BytesMut,
    pub write: BytesMut,
    pub subscription_channel: Option<(
        mpsc::WeakUnboundedSender<SubscriptionEvent>,
        mpsc::UnboundedReceiver<SubscriptionEvent>,
    )>,
    pub subscriptions: HashMap<Uuid, watch::Sender<Option<u64>>>,
}

impl Conn {
    fn new(
        cluster_ref: ActorRef<ClusterActor>,
        caches: Arc<HashMap<BucketId, Arc<SegmentBlockCache>>>,
        num_partitions: u16,
        cache_capacity_bytes: usize,
        stream: TcpStream,
        shutdown: CancellationToken,
    ) -> Self {
        let read = BytesMut::new();
        let write = BytesMut::new();

        Conn {
            cluster_ref,
            caches,
            num_partitions,
            cache_capacity_bytes,
            stream,
            shutdown,
            read,
            write,
            subscription_channel: None,
            subscriptions: HashMap::new(),
        }
    }

    async fn run(mut self) -> io::Result<()> {
        loop {
            match &mut self.subscription_channel {
                Some((_, rx)) => {
                    tokio::select! {
                        res = self.stream.read_buf(&mut self.read) => {
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

                                            self.stream.write_all(&self.write).await?;
                                            self.stream.flush().await?;
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
                        _ = self.shutdown.cancelled() => {
                            rx.close();
                            return self.stream.shutdown().await;
                        }
                    }
                }
                None => {
                    tokio::select! {
                        res = self.stream.read_buf(&mut self.read) => {
                            // Not in subscription mode - block normally on socket reads
                            let bytes_read = res?;
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

                                    self.stream.write_all(&self.write).await?;
                                    self.stream.flush().await?;
                                    self.write.clear();
                                }
                            }
                        }
                        _ = self.shutdown.cancelled() => {
                            return self.stream.shutdown().await;
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

        self.stream.write_all(&self.write).await?;
        self.stream.flush().await?;
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
