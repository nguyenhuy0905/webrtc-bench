//! Signaling server.
#![allow(unused)]
use bytes::Bytes;
use clap::Parser;
use dashmap::{DashMap, DashSet};
use matchbox_signaling::{NoCallbacks, SignalingServer};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::broadcast, task};

// Some terms I use a little loosely here:
// - "channel" is a broadcast. Peers subscribe to the broadcast. They can recv
// notifications (in the form of SDP offers) when they first join the channel,
// or when a new peer join.

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    let args = Opts::parse();
    let addr: SocketAddr = match format!("{}:{}", args.addr, args.port).parse() {
        Ok(addr) => addr,
        Err(e) => {
            return Err(format!("Invalid address {}:{}: {e}", args.addr, args.port));
        }
    };
    let mut server = SignalingServer::full_mesh_builder(addr)
        .on_connection_request(|connection| {
            log::info!("Connecting: {connection:?}");
            Ok(true) // Allow all connections
        })
        .on_id_assignment(|(socket, id)| log::info!("{socket} received {id}"))
        .on_peer_connected(|id| log::info!("Joined: {id}"))
        .on_peer_disconnected(|id| log::info!("Left: {id}"))
        .trace()
        .build();
    match server.bind() {
        Ok(sock_addr) => log::info!("Listening on ws://{sock_addr}"),
        Err(e) => return Err(format!("{e}")),
    }

    server.serve().await.map_err(|e| format!("{e}"))
}

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Opts {
    /// Address to bind (default 127.0.0.1)
    #[arg(short='a', long, default_value_t="127.0.0.1".into())]
    addr: String,
    /// Port to bind (default 6969)
    #[arg(short = 'p', long, default_value_t = 6969)]
    port: u16,
}
