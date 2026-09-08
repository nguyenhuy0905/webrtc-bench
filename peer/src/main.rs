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
    SinkExt, future,
    stream::{StreamExt, TryStreamExt},
};
use rtc::{
    peer_connection::configuration::media_engine::MIME_TYPE_H264,
    rtp_transceiver::rtp_sender::{
        RTCRtpCodec, RTCRtpCodecParameters, RTCRtpEncodingParameters, RtpCodecKind,
    },
};
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{OnceCell, RwLock, mpsc, oneshot};
use tokio_tungstenite::tungstenite::{error::Error as TungsteniteError, protocol::Message};
use uuid::Uuid;
use webrtc::{
    peer_connection::{
        MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
        RTCConfiguration, RTCConfigurationBuilder, RTCIceGatheringState, RTCIceServer,
        RTCPeerConnection, Registry, register_default_interceptors,
    },
    runtime::TokioRuntime,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Opts {
    /// Address of the signaling server (default 127.0.0.1:6969)
    #[arg(short='a', long, default_value_t="127.0.0.1:6969".into())]
    host: String,
    /// Path to video file
    #[arg(short='p', long, default_value_t="input.h264".into())]
    video_file: String,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    let args = Opts::parse();

    // connect to signaling server
    let (ws_stream, _) = tokio_tungstenite::connect_async(format!("ws://{}", args.host))
        .await
        .map_err(|e| format!("{e}"))?;
    let (mut write_stream, mut read_stream) = ws_stream.split();

    // 0.0 <C-c> handler, so that the web socket properly closes
    let (ctrlc_tx, mut ctrlc_rx) = mpsc::channel::<()>(1);
    ctrlc::set_handler(move || {
        ctrlc_tx.try_send(());
    })
    .map_err(|e| format!("{e}"))?;

    // 0.1 configure media engine.
    // We can't really do much else if this fails...

    // 0.3 the runtime
    let runtime = Arc::new(TokioRuntime);

    // 1. Tell the signaling server that I wanna join the channel. (to be fair, that's inferred from
    //    the fact we initiated the WebSocket connection).

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

    tokio::spawn(async move {
        while let Some(msg) = read_stream.next().await {
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => {
                    match e {
                        TungsteniteError::ConnectionClosed => {
                            // can't recover...
                            log::warn!("Signaling connection closed!");
                            break;
                        }
                        TungsteniteError::Io(io_err) => {
                            // can't recover...
                            log::error!("I/O error: {io_err}");
                            break;
                        }
                        _ => {
                            log::warn!("WebSocket error ignored: {e}");
                            continue;
                        }
                    }
                }
            };
            // make sure message is sort-of valid
            let msg = match msg.to_text() {
                Ok(s) => s,
                Err(e) => {
                    log::warn!("Non-text message? {e}");
                    continue;
                }
            };
            let msg: WsExchangeMsg = match serde_json::from_str(msg) {
                Ok(ret) => ret,
                Err(e) => {
                    log::warn!("Cannot understand message: {msg}");
                    continue;
                }
            };

            // make sure message is very valid.
            handle_message(msg).await;
        }
    });

    ctrlc_rx.recv().await;

    log::info!("WebSocket connection with signaling server closed");

    Ok(())
}

async fn handle_message(msg: WsExchangeMsg) -> Result<(), String> {
    match msg {
        WsExchangeMsg::JoinPeerId(_) => {
            log::warn!("Unexpected message: {msg:?}");
            return Ok(());
        }
        WsExchangeMsg::ExistingPeer { peer_id } => {
            log::info!(
                "WIP: Create PeerConnection for {peer_id}, with this peer being the offerer"
            );

            let (ice_gather_tx, mut ice_gather_rx) = mpsc::channel::<()>(1);
            let handler = Arc::new(WebRtcHandler {
                gather_ice_complete: ice_gather_tx,
            });
            let mut media_engine = MediaEngine::default();
            let video_codec = RTCRtpCodecParameters {
                rtp_codec: RTCRtpCodec {
                    mime_type: MIME_TYPE_H264.to_owned(),
                    clock_rate: 90_000,
                    channels: 0,
                    // what does this mean? I dunno.
                    sdp_fmtp_line:
                        "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
                            .to_owned(),
                    rtcp_feedback: vec![],
                },
                // h264 or something...
                payload_type: 102,
            };
            media_engine
                .register_codec(video_codec, RtpCodecKind::Video)
                .map_err(|e| format!("{e}"))?;
            let registry = match register_default_interceptors(Registry::new(), &mut media_engine) {
                Ok(reg) => reg,
                Err(e) => {
                    // really, how could this fail?
                    log::error!("Registering default interceptors failed: {e}");
                    return Ok(());
                }
            };
            let peer_conn = match PeerConnectionBuilder::new()
                .with_configuration(PEER_CONF.clone())
                .with_media_engine(media_engine.clone())
                .with_interceptor_registry(registry)
                .with_handler(handler)
                .with_udp_addrs(vec!["0.0.0.0:0"])
                .with_runtime(RUNTIME.clone())
                .build()
                .await
            {
                Ok(conn) => conn,
                Err(e) => {
                    log::error!("Cannot create connection with {peer_id}: {e}");
                    // TODO: we should probably retry, but anyways...
                    return Ok(());
                }
            };

            let offer = match peer_conn.create_offer(None).await {
                Ok(offer) => offer,
                Err(e) => {
                    log::error!("Cannot create offer: {e}");
                    // TODO: we should probably retry, but anyways...
                    return Ok(());
                }
            };
            match peer_conn.set_local_description(offer).await {
                Ok(()) => {}
                Err(e) => {
                    log::error!("Cannot set offer as local description: {e}");
                    return Ok(());
                }
            }
            match OTHER_PEERS.insert(
                peer_id,
                (Arc::new(peer_conn), PeerSetupStage::WaitingAnswer),
            ) {
                Some(_) => {
                    log::warn!("Peed ID {peer_id} already exists, overwriting...");
                }
                None => {
                    log::info!(
                        "Created PeerConnection for {peer_id}, waiting for answer from peer..."
                    );
                }
            }

            // wait for ICE gathering to complete
            ice_gather_rx.recv().await;
        }
        WsExchangeMsg::NewPeer { peer_id } => {
            log::info!(
                "WIP: Create PeerConnection for {peer_id}, with this peer being the answerer"
            )
        }
        WsExchangeMsg::Sdp {
            send_to_id,
            answering_peer_id,
            sdp,
        } => {
            if send_to_id != *SELF_UUID.get().expect("SELF_UUID should already be set!") {
                log::warn!("Received a message destined to {send_to_id}");
                return Ok(());
            }
            log::info!("WIP: finish creating peer connection for {answering_peer_id}")
            // let mut peer_data = match OTHER_PEERS.get_mut(&answering_peer_id) {
            //     Some(data) => data,
            //     None => {
            //         log::warn!("Peer {answering_peer_id} doesn't exist somehow");
            //         return Ok(());
            //     }
            // };
            // if matches!(peer_data.value().1, PeerSetupStage::Done) {
            //     log::warn!("Trying to add SDP to an already-set-up peer");
            //     return Ok(());
            // }
            // peer_data.value_mut().0.set_remote_description(sdp);
            // peer_data.value_mut().1 = PeerSetupStage::Done;
        }
        WsExchangeMsg::LeavePeerId(leaving_peer_id) => {
            log::info!("Peer {leaving_peer_id} leaving");
            OTHER_PEERS.remove(&leaving_peer_id);
        }
    }
    Ok(())
}

/// This peer's own UUID. We'll only receive this after asking the signaling server to join.
static SELF_UUID: OnceCell<Uuid> = OnceCell::const_new();
/// We got stuff to send, we send to each of them.
/// And if one leaves, we remove that one's peer connection.
static OTHER_PEERS: LazyLock<DashMap<Uuid, (Arc<dyn PeerConnection>, PeerSetupStage)>> =
    LazyLock::new(DashMap::new);
static PEER_CONF: LazyLock<RTCConfiguration> = LazyLock::new(|| {
    RTCConfigurationBuilder::new()
        .with_ice_servers(vec![RTCIceServer {
            // the STUN server we control.
            urls: vec!["stun:127.0.0.1:3478".to_string()],
            ..Default::default()
        }])
        .build()
});
static RUNTIME: LazyLock<Arc<TokioRuntime>> = LazyLock::new(|| Arc::new(TokioRuntime));

enum PeerSetupStage {
    WaitingAnswer,
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
