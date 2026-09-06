//! Signaling server.
#![allow(unused)]
use bytes::Bytes;
use clap::Parser;
use dashmap::{DashMap, DashSet};
use matchbox_signaling::SignalingServer;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, net::SocketAddr, sync::LazyLock};
use tokio::{net::TcpListener, sync::broadcast, task};
use webrtc::peer_connection::RTCSessionDescription;

// Some terms I use a little loosely here:
// - "channel" is a broadcast. Peers subscribe to the broadcast. They can recv
// notifications (in the form of SDP offers) when they first join the channel,
// or when a new peer join.

// `on_connection_request` is where we handle most of the logic, since that's where we know about
// the HTTP request and its content.

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Opts {
    /// address to bind to (default 0.0.0.0)
    #[arg(short='a', long, default_value_t="0.0.0.0".into())]
    addr: String,
    #[arg(short = 'p', long, default_value_t = 0)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    let args = Opts::parse();
    let addr: SocketAddr = match format!("{}:{}", args.addr, args.port).parse() {
        Ok(addr) => addr,
        Err(e) => {
            return Err(format!("Cannot bind to {}:{}: {e}", args.addr, args.port));
        }
    };
    let mut server = SignalingServer::client_server_builder(addr)
        .on_connection_request(|connection| {
            log::info!("Connecting: {connection:?}");
            Ok(true) // Allow all connections
        })
        .on_id_assignment(|(socket, id)| log::info!("{socket} received {id}"))
        .on_host_connected(|id| log::info!("Host joined: {id}"))
        .on_host_disconnected(|id| log::info!("Host left: {id}"))
        .on_client_connected(|id| log::info!("Client joined: {id}"))
        .on_client_disconnected(|id| log::info!("Client left: {id}"))
        .trace()
        .build();
    match server.bind() {
        Ok(sock_addr) => log::info!("Listening on ws://{sock_addr}"),
        Err(e) => return Err(format!("{e}")),
    }

    server.serve().await.map_err(|e| format!("{e}"))
}

/// All the info needed to help peers establish PeerConnections, hopefully.
struct ChannelData {
    /// A map of (channel-name, channel-offers).
    /// Each offer is of a peer in the channel that
    offers: DashSet<RTCSessionDescription>,
    /// In case a new peer joins the channel, this broadcast is how they know
    /// about a new peer joining.
    channel: broadcast::Sender<RTCSessionDescription>,
}

static CHANNELS: LazyLock<DashMap<String, ChannelData>> = LazyLock::new(|| Default::default());
