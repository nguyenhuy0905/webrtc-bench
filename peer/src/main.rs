#![allow(unused)]

// game plan here:
// 0. set up some sort of signaling server. Of course.
// 1. find a way for the peers to ping the signaling server.
// 1.1 by "ping" I mean send/recv SDPs.

// So, `matchbox` turned out to not be such a bright idea.
// I'll make my own WebSocket (building from `tokio-tungstenite`) then

use clap::Parser;
use dashmap::DashMap;
use std::sync::LazyLock;
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::tungstenite::{error::Error as TungsteniteError, protocol::Message};
use uuid::Uuid;
use webrtc::peer_connection::RTCPeerConnection;

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Opts {
    /// Address of the signaling server (default 127.0.0.1:6969)
    #[arg(short='a', long, default_value_t="127.0.0.1:6969".into())]
    host: String,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    let args = Opts::parse();

    let (ws_stream, _) = tokio_tungstenite::connect_async(args.host)
        .await
        .map_err(|e| format!("{e}"))?;

    let (ws_tx, ws_rx) = mpsc::unbounded_channel::<Message>();

    // TODO:
    // - Keep a map of peers. For each peer:
    //   - Create a RTCPeerConnection.

    Ok(())
}

/// This peer's own UUID. We'll only receive this after asking the signaling server to join.
static SELF_UUID: LazyLock<RwLock<Option<Uuid>>> = LazyLock::new(|| RwLock::new(None));
/// We got stuff to send, we send to each of them.
/// And if one leaves, we remove that one's peer connection.
static OTHER_PEERS: LazyLock<DashMap<Uuid, RTCPeerConnection>> = LazyLock::new(DashMap::new);
