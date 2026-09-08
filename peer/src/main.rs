#![allow(unused)]

// game plan here:
// 0. set up some sort of signaling server. Of course.
// 1. find a way for the peers to ping the signaling server.
// 1.1 by "ping" I mean send/recv SDPs.

// So, `matchbox` turned out to not be such a bright idea.
// I'll make my own WebSocket (building from `tokio-tungstenite`) then

use clap::Parser;
use common::WsExchangeMsg;
use dashmap::DashMap;
use futures_util::{
    future,
    stream::{StreamExt, TryStreamExt},
    SinkExt,
};
use rtc::{
    peer_connection::configuration::media_engine::MIME_TYPE_H264,
    rtp_transceiver::rtp_sender::{
        RTCRtpCodec, RTCRtpCodecParameters, RTCRtpEncodingParameters, RtpCodecKind,
    },
};
use std::sync::{Arc, LazyLock};
use tokio::sync::{mpsc, OnceCell, RwLock};
use tokio_tungstenite::tungstenite::{error::Error as TungsteniteError, protocol::Message};
use uuid::Uuid;
use webrtc::peer_connection::{
    register_default_interceptors, MediaEngine, PeerConnectionEventHandler,
    RTCConfigurationBuilder, RTCIceGatheringState, RTCIceServer, RTCPeerConnection, Registry,
};

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

    let (ws_stream, _) = tokio_tungstenite::connect_async(format!("ws://{}", args.host))
        .await
        .map_err(|e| format!("{e}"))?;

    let (mut write_stream, mut read_stream) = ws_stream.split();
    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel::<WsExchangeMsg>();

    // 0.1 configure media engine.
    // We can't really do much else if this fails...
    let mut media_engine = MediaEngine::default();
    let video_codec = RTCRtpCodecParameters {
        rtp_codec: RTCRtpCodec {
            mime_type: MIME_TYPE_H264.to_owned(),
            clock_rate: 90_000,
            channels: 0,
            // what does this mean? I dunno.
            sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
                .to_owned(),
            rtcp_feedback: vec![],
        },
        // h264 or something...
        payload_type: 102,
    };
    media_engine
        .register_codec(video_codec, RtpCodecKind::Video)
        .map_err(|e| format!("{e}"))?;
    let registry = register_default_interceptors(Registry::new(), &mut media_engine)
        .map_err(|e| format!("{e}"))?;

    // 0.2 base config that all the PeerConnections would use.
    let config = RTCConfigurationBuilder::new()
        .with_ice_servers(vec![RTCIceServer {
            // the STUN server we control.
            urls: vec!["stun:127.0.0.1:3478".to_string()],
            ..Default::default()
        }])
        .build();

    // 1. Tell the signaling server that I wanna join the channel. (to be fair, that's inferred from
    //    the fact we initiated the WebSocket connecction).
    // write_stream
    //     .send(Message::from(
    //         serde_json::to_string(&WsExchangeMsg::Join).unwrap(),
    //     ))
    //     .await;
    // then we can initialize our PeerID.
    let self_id: WsExchangeMsg = serde_json::from_str(
        read_stream
            .next()
            .await
            .expect("Could not get this peer's ID")
            // HACK: these errors should be handled.
            .unwrap()
            .to_text()
            .unwrap(),
    )
    .expect("Cannot parse first WebSocket response to WsExchangeMsg");
    // I'm pretty sure WebSocket is a reliable stream.
    if let WsExchangeMsg::JoinPeerId(self_id) = self_id {
        SELF_UUID
            .set(self_id)
            .expect("Somehow SELF_UUID is already set");
        log::info!("Initialize self as {self_id}");
    } else {
        return Err("WsExchangeMsg didn't return JoinPeerId".to_string());
    }

    // 2. For each `ExistingPeer`, this peer is the offerer. And for each `NewPeer`, this peer is
    //    the answerer. `NewPeer` *can* override existing peer.

    read_stream
        .try_for_each(|msg| {
            // make sure message is sort-of valid
            let msg = match msg.to_text() {
                Ok(s) => s,
                Err(e) => {
                    log::warn!("Non-text message? {e}");
                    return future::ok(());
                }
            };
            let msg: WsExchangeMsg = match serde_json::from_str(msg) {
                Ok(ret) => ret,
                Err(e) => {
                    log::warn!("Cannot understand message: {msg}");
                    return future::ok(());
                }
            };

            // make sure message is very valid.
            match msg {
                WsExchangeMsg::JoinPeerId(_) => {
                    log::warn!("Unexpected message: {msg:?}");
                    return future::ok(());
                }
                WsExchangeMsg::ExistingPeer { peer_id } => {
                    log::info!("WIP: Create PeerConnection for {peer_id}, with this peer being the offerer")
                }
                WsExchangeMsg::NewPeer { peer_id } => {
                    log::info!("WIP: Create PeerConnection for {peer_id}, with this peer being the answerer")
                }
                WsExchangeMsg::Sdp {
                    send_to_id,
                    answering_peer_id,
                    sdp,
                } => {
                    if send_to_id != *SELF_UUID.get().expect("SELF_UUID should already be set!") {
                        log::warn!("Received a message destined to {send_to_id}");
                        return future::ok(());
                    }
                    log::info!("WIP: finish creating peer connection for {answering_peer_id}")
                    // let mut peer_data = match OTHER_PEERS.get_mut(&answering_peer_id) {
                    //     Some(data) => data,
                    //     None => {
                    //         log::warn!("Peer {answering_peer_id} doesn't exist somehow");
                    //         return future::ok(());
                    //     }
                    // };
                    // if matches!(peer_data.value().1, PeerSetupStage::Done) {
                    //     log::warn!("Trying to add SDP to an already-set-up peer");
                    //     return future::ok(());
                    // }
                    // peer_data.value_mut().0.set_remote_description(sdp);
                    // peer_data.value_mut().1 = PeerSetupStage::Done;
                }
            }
            future::ok(())
        })
        .await;

    log::info!("WebSocket connection with signaling server closed");

    Ok(())
}

/// This peer's own UUID. We'll only receive this after asking the signaling server to join.
static SELF_UUID: OnceCell<Uuid> = OnceCell::const_new();
/// We got stuff to send, we send to each of them.
/// And if one leaves, we remove that one's peer connection.
static OTHER_PEERS: LazyLock<DashMap<Uuid, (RTCPeerConnection, PeerSetupStage)>> =
    LazyLock::new(DashMap::new);

enum PeerSetupStage {
    WaitingAnswer,
    WaitingOffer,
    Done,
}

struct WebRtcHandler {
    /// Will send a notification once.
    /// We don't use trickle ICE for now...
    gather_ice_complete: mpsc::Sender<()>,
}
#[async_trait::async_trait]
impl PeerConnectionEventHandler for WebRtcHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if matches!(state, RTCIceGatheringState::Complete) {
            self.gather_ice_complete
                .send(())
                .await
                .expect("ICE gathering receiver end dropped before sent!");
            log::info!("ICE gathering complete!");
        }
    }
}
