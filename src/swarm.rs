use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin, task,
};

use futures::{FutureExt, StreamExt, ready};
use libp2p::{
    PeerId, StreamProtocol, gossipsub, identify,
    identity::Keypair,
    mdns, ping,
    request_response::{self, ProtocolSupport},
    swarm::{NetworkBehaviour, SwarmEvent},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::{bucket::BucketId, error::SwarmError};

#[derive(Clone, Debug)]
pub struct SwarmRef {
    swarm_tx: SwarmSender,
    local_peer_id: PeerId,
}

pub struct Swarm {
    swarm: libp2p::Swarm<Behaviour>,
    cmd_tx: Option<mpsc::UnboundedSender<Command>>,
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    owned_buckets: HashSet<BucketId>,
    bucket_owners: HashMap<BucketId, String>,
    // response_sender: mpsc::Sender<EventStoreResponse>,
    // pending_events: HashMap<Uuid, mpsc::Sender<EventStoreResponse>>,
}

impl Swarm {
    pub fn new(key: Keypair) -> Result<Self, SwarmError> {
        let swarm = libp2p::SwarmBuilder::with_existing_identity(key)
            .with_tokio()
            .with_quic()
            .with_behaviour(|key| {
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub::Config::default(),
                )?;

                let req_resp = request_response::Behaviour::new(
                    [(
                        StreamProtocol::new("/eventus/events/1"),
                        ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                );

                let mdns =
                    mdns::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;

                let ping = ping::Behaviour::new(ping::Config::new());

                let identify = identify::Behaviour::new(identify::Config::new(
                    "/eventus/1.0.0".to_string(),
                    key.public(),
                ));

                Ok(Behaviour {
                    gossipsub,
                    req_resp,
                    mdns,
                    ping,
                    identify,
                })
            })?
            .build();

        Ok(Self::with_swarm(swarm))
    }

    pub fn with_swarm(swarm: libp2p::Swarm<Behaviour>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        Swarm {
            swarm,
            cmd_tx: Some(cmd_tx),
            cmd_rx,
            owned_buckets: HashSet::new(),
            bucket_owners: HashMap::new(),
        }
    }

    pub fn spawn(mut self) -> SwarmRef {
        let tx = self.cmd_tx.take().unwrap();
        let swarm_ref = SwarmRef {
            swarm_tx: SwarmSender(tx),
            local_peer_id: *self.swarm.local_peer_id(),
        };
        tokio::spawn(self.run());

        swarm_ref
    }

    async fn run(mut self) {
        loop {
            tokio::select! {
                Some(cmd) = self.cmd_rx.recv() => self.handle_command(cmd),
                Some(event) = self.swarm.next() => self.handle_swarm_event(event),
            }
        }
        error!("swarm stopped running");
    }

    fn handle_command(&mut self, command: Command) {}

    fn handle_swarm_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(event)) => {
                self.handle_gossipsub_event(event)
            }
            SwarmEvent::Behaviour(BehaviourEvent::ReqResp(event)) => {
                self.handle_req_resp_event(event)
            }
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => self.handle_mdns_event(event),
            SwarmEvent::Behaviour(BehaviourEvent::Ping(event)) => self.handle_ping_event(event),
            SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => {
                self.handle_identify_event(event)
            }
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                num_established,
                concurrent_dial_errors,
                established_in,
            } => todo!(),
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                endpoint,
                num_established,
                cause,
            } => todo!(),
            SwarmEvent::IncomingConnection {
                connection_id,
                local_addr,
                send_back_addr,
            } => todo!(),
            SwarmEvent::IncomingConnectionError {
                connection_id,
                local_addr,
                send_back_addr,
                error,
            } => todo!(),
            SwarmEvent::OutgoingConnectionError {
                connection_id,
                peer_id,
                error,
            } => todo!(),
            SwarmEvent::NewListenAddr {
                listener_id,
                address,
            } => todo!(),
            SwarmEvent::ExpiredListenAddr {
                listener_id,
                address,
            } => todo!(),
            SwarmEvent::ListenerClosed {
                listener_id,
                addresses,
                reason,
            } => todo!(),
            SwarmEvent::ListenerError { listener_id, error } => todo!(),
            SwarmEvent::Dialing {
                peer_id,
                connection_id,
            } => todo!(),
            SwarmEvent::NewExternalAddrCandidate { address } => todo!(),
            SwarmEvent::ExternalAddrConfirmed { address } => todo!(),
            SwarmEvent::ExternalAddrExpired { address } => todo!(),
            SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => todo!(),
            _ => todo!(),
        }
    }

    fn handle_gossipsub_event(&mut self, event: gossipsub::Event) {}

    fn handle_req_resp_event(&mut self, event: request_response::Event<Req, Resp>) {}

    fn handle_mdns_event(&mut self, event: mdns::Event) {}

    fn handle_ping_event(&mut self, event: ping::Event) {}

    fn handle_identify_event(&mut self, event: identify::Event) {}
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    gossipsub: gossipsub::Behaviour,
    req_resp: request_response::cbor::Behaviour<Req, Resp>,
    mdns: mdns::tokio::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
}

enum Command {}

#[derive(Debug, Serialize, Deserialize)]
enum Req {}

#[derive(Debug, Serialize, Deserialize)]
enum Resp {}

#[derive(Clone, Debug)]
pub(crate) struct SwarmSender(mpsc::UnboundedSender<Command>);

impl SwarmSender {
    pub(crate) fn send(&self, cmd: Command) -> Result<(), SwarmError> {
        self.0.send(cmd).map_err(|_| SwarmError::SwarmNotRunning)
    }

    pub(crate) fn send_with_reply<T>(
        &self,
        cmd_fn: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<SwarmFuture<T>, SwarmError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = cmd_fn(reply_tx);
        self.send(cmd)?;

        Ok(SwarmFuture(reply_rx))
    }
}

/// `SwarmFuture` represents a future that contains the response from a remote actor.
///
/// This future is returned when sending a message to a remote actor via the actor swarm.
/// If the response is not needed, the future can simply be dropped without awaiting it.
#[derive(Debug)]
pub struct SwarmFuture<T>(oneshot::Receiver<T>);

impl<T> Future for SwarmFuture<T> {
    type Output = T;

    fn poll(mut self: pin::Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        task::Poll::Ready(
            ready!(self.0.poll_unpin(cx))
                .expect("the oneshot sender should never be dropped before being sent to"),
        )
    }
}
