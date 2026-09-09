//! Signaling server.
#![allow(unused)]
use clap::Parser;
use common::WsExchangeMsg;
use dashmap::DashMap;
use futures_util::{
    SinkExt, future,
    stream::{StreamExt, TryStreamExt},
};
use serde_json::error::Category;
use std::{net::SocketAddr, pin::Pin, sync::LazyLock};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::{error::Error as TungsteniteError, protocol::Message};
use uuid::Uuid;

// Some terms I use a little loosely here:
// - "channel" is a broadcast. Peers subscribe to the broadcast. They can recv
// notifications (in the form of SDP offers) when they first join the channel,
// or when a new peer join.

// TODO: I dunno if this code can handle timeout...

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    let args = Opts::parse();

    let listener = match TcpListener::bind(args.host).await {
        Ok(lis) => lis,
        Err(e) => return Err(format!("{e}")),
    };
    log::info!(
        "Listening on {}",
        match listener.local_addr() {
            Ok(addr) => Ok(addr),
            Err(e) => Err(format!("{e}")),
        }?
    );

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(async move {
            match handle_connection(stream, addr).await {
                Ok(()) => {}
                Err(e) => match e {
                    TungsteniteError::ConnectionClosed => {
                        log::info!("Connection {addr} closed");
                        remove_peer_addr(&addr).await;
                    }
                    TungsteniteError::AttackAttempt => {
                        log::warn!("Attack attempt detected! Nuking the suspecting peer at once");
                        remove_peer_addr(&addr).await;
                    }
                    _ => {
                        log::warn!("Misc. error: {e}");
                        remove_peer_addr(&addr).await;
                    }
                },
            }
        });
    }

    Ok(())
}

/// When this runs, it's probably a new peer's connecting...
async fn handle_connection(
    raw_stream: TcpStream,
    addr: SocketAddr,
) -> Result<(), TungsteniteError> {
    log::info!("Incoming TCP connection from {addr}");

    let ws_stream = tokio_tungstenite::accept_async(raw_stream).await?;
    log::info!("WebSocket connection established for {addr}");

    // insert the new peer's info into the tables.
    // there shouldn't be *that* many signals, right?
    let (tx, mut rx) = mpsc::channel::<WsExchangeMsg>(16);
    let mut uuid = Uuid::new_v4();
    while PEER_UUID_AND_SENDER.get(&uuid).is_some() {
        uuid = Uuid::new_v4();
    }
    PEER_UUID_AND_SENDER.insert(uuid, tx);
    PEER_ADDR_AND_UUID.insert(addr, uuid);
    log::info!("Peer created: {uuid}");

    // then send the peer its PeerID.
    let (mut outgoing, mut incoming) = ws_stream.split();
    outgoing
        .send(Message::from(
            serde_json::to_string(&WsExchangeMsg::JoinPeerId(uuid))
                .expect("Cannot serialize JoinPeerId to JSON"),
        ))
        .await;

    // the current peer receives Peer IDs of all other peers.
    // All other peers receive the current peer's ID.
    for kv in PEER_UUID_AND_SENDER.iter().filter(|kv| kv.key() != &uuid) {
        let msg_to_others = WsExchangeMsg::NewPeer { peer_id: uuid };
        let msg_to_self = WsExchangeMsg::ExistingPeer { peer_id: *kv.key() };

        let send_to_self = match serde_json::to_string(&msg_to_self) {
            Ok(ret) => ret,
            Err(e) => {
                log::error!("Cannot serialize message {msg_to_self:?}: {e}");
                // TODO: we should retry, but the current peer and this peer cannot make
                // a PeerConnection for now.
                continue;
            }
        };

        match outgoing.send(Message::from(send_to_self)).await {
            Ok(()) => {}
            Err(e) => {
                match &e {
                    TungsteniteError::ConnectionClosed => {
                        log::warn!("Signaling connection closed!");
                        // TODO: notify that this peer has left.
                        return Err(e);
                    }
                    TungsteniteError::Io(io_err) => {
                        log::error!("I/O error: {io_err}");
                        // TODO: notify that this peer has left.
                        return Err(e);
                    }
                    _ => {
                        log::warn!("WebSocket error ignored: {e}");
                        // TODO: retry instead of continue
                        continue;
                    }
                }
            }
        }
        match kv.value().send(msg_to_others).await {
            Ok(()) => {}
            Err(e) => {
                log::warn!("Cannot send {uuid} to {}: {e}", kv.key());
                // TODO: we shouldn't just log and do nothing else for this error.
                continue;
            }
        }
    }

    let uuid = Pin::new(&uuid);
    let broadcast_incoming = async move {
        while let Some(msg) = incoming.next().await {
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => {
                    log::warn!("Message error: {e}");
                    // probably some kind of I/O error (stream closed, ...); just remove the peer.
                    continue;
                }
            };
            log::info!("Recv msg from {}: {}", *uuid, msg.to_text().unwrap());

            // make sure the message is valid before broadcasting...
            let msg: WsExchangeMsg = match serde_json::from_str(&msg.to_text().unwrap()) {
                Ok(s) => s,
                Err(e) => match e.classify() {
                    Category::Io => {
                        log::error!("{}: deserialize to string somehow causes I/O error", *uuid);
                        // TODO: resend and retry... But how can this case even happen to be fair.
                        continue;
                    }
                    _ => {
                        log::warn!("Serializing for {}: {e}", *uuid);
                        continue;
                    }
                },
            };
            match &msg {
                &WsExchangeMsg::Sdp {
                    send_to_id,
                    answering_peer_id,
                    ..
                } => {
                    if PEER_UUID_AND_SENDER.get(&answering_peer_id).is_none() {
                        log::warn!(
                            "Answering peer {answering_peer_id} does not exist (anymore). Skipping..."
                        );
                        continue;
                    }

                    let send_to_kv = match PEER_UUID_AND_SENDER.get(&send_to_id) {
                        Some(kv) => kv,
                        None => {
                            log::warn!(
                                "Send-to peer {send_to_id} does not exist (anymore). Skipping..."
                            );
                            continue;
                        }
                    };
                    // and forward the message...
                    match send_to_kv.send(msg).await {
                        Ok(()) => {
                            log::info!("SDP exchanged from {send_to_id} to {answering_peer_id}");
                        }
                        Err(e) => {
                            log::warn!("Cannot forward message to {send_to_id}: {e}");
                        }
                    }
                }
                &WsExchangeMsg::IceCandidate {
                    send_to_id,
                    answering_peer_id,
                    ..
                } => {
                    // basically copy-paste of WsExchangeMsg::Sdp
                    if PEER_UUID_AND_SENDER.get(&answering_peer_id).is_none() {
                        log::warn!(
                            "Answering peer {answering_peer_id} does not exist (anymore). Skipping..."
                        );
                        continue;
                    }

                    let send_to_kv = match PEER_UUID_AND_SENDER.get(&send_to_id) {
                        Some(kv) => kv,
                        None => {
                            log::warn!(
                                "Send-to peer {send_to_id} does not exist (anymore). Skipping..."
                            );
                            continue;
                        }
                    };
                    // and forward the message...
                    match send_to_kv.send(msg).await {
                        Ok(()) => {
                            log::info!(
                                "ICE candidate exchanged from {send_to_id} to {answering_peer_id}"
                            );
                        }
                        Err(e) => {
                            log::warn!("Cannot forward message to {send_to_id}: {e}");
                        }
                    }
                }
                _ => {
                    log::warn!("Received wrong type of message: {msg:?}");
                    continue;
                }
            }
        }
    };
    tokio::pin!(broadcast_incoming);

    let recv_from_others = async move {
        while let Some(msg) = rx.recv().await {
            let send_msg = match serde_json::to_string(&msg) {
                Ok(s) => s,
                Err(e) => match e.classify() {
                    Category::Io => {
                        log::error!("{}: deserialize to string somehow causes I/O error", *uuid);
                        continue;
                    }
                    _ => {
                        log::warn!("Serializing for {}: {e}", *uuid);
                        continue;
                    }
                },
            };
            outgoing.send(Message::from(send_msg)).await;
        }
    };
    tokio::pin!(recv_from_others);

    tokio::select! {
        _ = broadcast_incoming => {
            log::info!("Broadcast incoming for {} done", *uuid);
        }
        _ = recv_from_others => {
            log::info!("Receiving for others for {} done", *uuid);
        }
    };

    // notify other peers that this peer is done and is quitting.
    log::info!("Trying to remove {}...", *uuid);
    for recp in PEER_UUID_AND_SENDER.iter().filter(|kv| kv.key() != &*uuid) {
        let msg = WsExchangeMsg::LeavePeerId(*uuid);
        match recp.value().send(msg).await {
            Ok(()) => {
                log::debug!("Sent remove signal of {} to {}", uuid, recp.key());
            }
            Err(e) => {
                log::debug!("Send leaving ID to {} ignored: {e}", recp.key());
            }
        }
    }
    // and remove this peer from the list
    remove_peer_addr(&addr).await;
    // this actually never prints...
    log::info!("Peer {} removed", *uuid);
    log::info!(
        "Remaining peers: {:?}",
        PEER_UUID_AND_SENDER
            .iter()
            .map(|kv| *kv.key())
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// Remove the peer with the specified address.
async fn remove_peer_addr(addr: &SocketAddr) {
    // `cloned` in hopes we drop the lock ASAP.
    let uuid = PEER_ADDR_AND_UUID.get(&addr).map(|opt| opt.value().clone());
    if let Some(uuid) = uuid {
        PEER_UUID_AND_SENDER.remove(&uuid);
        // drop borrow
        let uuid = 0;
        PEER_ADDR_AND_UUID.remove(&addr);
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Opts {
    /// Address to bind (default 127.0.0.1:6969)
    #[arg(short='a', long, default_value_t="127.0.0.1:6969".into())]
    host: String,
}

static PEER_UUID_AND_SENDER: LazyLock<DashMap<Uuid, mpsc::Sender<WsExchangeMsg>>> =
    LazyLock::new(DashMap::new);
static PEER_ADDR_AND_UUID: LazyLock<DashMap<SocketAddr, Uuid>> = LazyLock::new(DashMap::new);
