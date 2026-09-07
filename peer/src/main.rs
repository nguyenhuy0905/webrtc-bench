#![allow(unused)]

// game plan here:
// 0. set up some sort of signaling server. Of course.
// 1. find a way for the peers to ping the signaling server.
// 1.1 by "ping" I mean send/recv SDPs.

use clap::Parser;
use matchbox_socket::{Error as SocketError, PeerState, RtcIceServerConfig, WebRtcSocketBuilder};
use std::{
    cell::Cell,
    sync::{Arc, OnceLock},
};
use tokio::{
    runtime::Runtime,
    sync::mpsc,
    time::{self, Duration, Instant},
};

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Opts {
    /// Address of the signaling server (default 127.0.0.1)
    #[arg(short='a', long, default_value_t="127.0.0.1".into())]
    addr: String,
    /// Port of the signaling server (default 6969)
    #[arg(short = 'p', long, default_value_t = 6969)]
    port: u16,
}

const CHANNEL_ID: usize = 0;

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    let args = Opts::parse();

    let (mut socket, loop_fut) =
        WebRtcSocketBuilder::new(format!("ws://{}:{}", args.addr, args.port))
            .ice_server(RtcIceServerConfig {
                urls: vec!["stun:127.0.0.1:3478".into()],
                ..Default::default()
            }).add_unreliable_channel().build();

    let loop_fut = async {
        match loop_fut.await {
            Ok(()) => log::info!("Exited cleanly"),
            Err(e) => {
                match e {
                    SocketError::ConnectionFailed(e) => {
                        log::warn!("couldn't connect to signaling server, please check your connection: {e}");
                        // todo: show prompt and reconnect?
                    }
                    SocketError::Disconnected(e) => {
                        log::warn!("you were kicked, or your connection went down, or the signaling server stopped: {e}");
                    }
                }
            }
        }
    };
    tokio::pin!(loop_fut);

    let sleep = time::sleep(Duration::from_millis(1000));
    tokio::pin!(sleep);

    loop {
        // New or quitted peers.
        for (peer, state) in socket.update_peers() {
            match state {
                PeerState::Connected => {
                    log::info!("New peer: {peer}");
                    let packet = "ping".as_bytes().to_vec().into_boxed_slice();
                    socket.channel_mut(CHANNEL_ID).send(packet, peer);
                }
                PeerState::Disconnected => {
                    log::info!("Peer left: {peer}");
                }
            }
        }

        // Accept any messages incoming
        for (peer, packet) in socket.channel_mut(CHANNEL_ID).receive() {
            let message = String::from_utf8_lossy(&packet);
            println!("Message from {peer}: {message:?}");
        }
        // does this every 1 sec 'cuz we only testing this out...

        tokio::select! {
            _ = &mut sleep => {
                sleep.as_mut().reset(Instant::now() + Duration::from_millis(1000));
            }
            _ = &mut loop_fut => {
                break;
            }
        }
    }
    Ok(())
}
