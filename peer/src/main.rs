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
    stream::{SplitSink, SplitStream, StreamExt, TryStreamExt},
    SinkExt,
};
use rtc::{
    peer_connection::configuration::media_engine::MIME_TYPE_H264,
    rtp_transceiver::rtp_sender::{
        RTCRtpCodec, RTCRtpCodecParameters, RTCRtpEncodingParameters, RtpCodecKind,
    },
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, LazyLock,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex, OnceCell, RwLock},
};
use tokio_tungstenite::{
    tungstenite::{error::Error as TungsteniteError, protocol::Message},
    MaybeTlsStream, WebSocketStream,
};
use uuid::Uuid;
use webrtc::{
    peer_connection::{
        register_default_interceptors, MediaEngine, PeerConnection, PeerConnectionBuilder,
        PeerConnectionEventHandler, RTCConfiguration, RTCConfigurationBuilder,
        RTCIceGatheringState, RTCIceServer, RTCPeerConnection, RTCSessionDescription, Registry,
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
        .map_err(|e| e.to_string())?;
    let (mut write_stream, mut read_stream) = ws_stream.split();

    // 0.0 <C-c> handler, so that the web socket properly closes
    let (ctrlc_tx, mut ctrlc_rx) = mpsc::channel::<()>(1);
    ctrlc::set_handler(move || {
        ctrlc_tx.try_send(());
    })
    .map_err(|e| e.to_string())?;

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

    tokio::spawn(handle_signal(read_stream, write_stream));

    ctrlc_rx.recv().await;

    log::info!("WebSocket connection with signaling server closed");

    Ok(())
}

async fn handle_signal(
    mut read_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    mut write_stream: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
) -> Result<(), String> {
    let write_stream = Arc::new(Mutex::new(write_stream));
    while let Some(msg) = read_stream.next().await {
        // convert message from Result<Message, TungsteniteError> to WsExchangeMsg.
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
        log::trace!("Received message: {msg:?}");

        if let Some(send_back) = handle_message(msg).await? {
            let write_stream = write_stream.clone();
            // so that we can go read more stuff right away.
            tokio::spawn(async move {
                if let Err(e) = write_stream
                    .lock()
                    .await
                    .send(Message::from(
                        serde_json::to_string(&send_back)
                            .expect("Cannot serialize sendback message"),
                    ))
                    .await
                {
                    log::warn!("Send-back message error: {e}");
                };
            });
            log::debug!("Sent back message to signaling server");
        }
    }
    Ok(())
}

/// Given a WsExchangeMsg, deal with it!
/// Return: Some(message) if there's something needed to be sent back to the signaling server.
/// TODO: don't return a String as error... we can do better.
async fn handle_message(msg: WsExchangeMsg) -> Result<Option<WsExchangeMsg>, String> {
    // make sure message is very valid.
    match msg {
        WsExchangeMsg::JoinPeerId(_) => {
            log::warn!("Unexpected message: {msg:?}");
            return Ok(None);
        }
        WsExchangeMsg::ExistingPeer { peer_id } => {
            if let Some(return_offer) = add_peer(peer_id, true).await? {
                return Ok(Some(WsExchangeMsg::Sdp {
                    send_to_id: peer_id,
                    answering_peer_id: *SELF_UUID.get().expect("SELF_UUID should already be set!"),
                    sdp: return_offer,
                }));
            }
        }
        WsExchangeMsg::NewPeer { peer_id } => {
            add_peer(peer_id, false).await?;
        }
        WsExchangeMsg::Sdp {
            send_to_id,
            answering_peer_id,
            sdp,
        } => {
            if send_to_id != *SELF_UUID.get().expect("SELF_UUID should already be set!") {
                log::warn!("Received a message destined to {send_to_id}");
                return Ok(None);
            }
            // log::info!("WIP: finish creating peer connection for {answering_peer_id}");
            let send_back_opt = finish_configure_peer_connection(answering_peer_id, sdp).await?;
            if let Some(send_back) = send_back_opt {
                return Ok(Some(WsExchangeMsg::Sdp {
                    send_to_id: answering_peer_id,
                    answering_peer_id: send_to_id,
                    sdp: send_back,
                }));
            }
        }
        WsExchangeMsg::LeavePeerId(leaving_peer_id) => {
            log::info!("Peer {leaving_peer_id} leaving");
            OTHER_PEERS.remove(&leaving_peer_id);
        }
        WsExchangeMsg::IceCandidate {
            send_to_id,
            answering_peer_id,
            candidate,
        } => {
            if send_to_id != *SELF_UUID.get().expect("SELF_UUID should already be set!") {
                log::warn!("Received a message destined to {send_to_id}");
                return Ok(None);
            }
            let peer_conn = match OTHER_PEERS.get(&answering_peer_id) {
                Some(kv) => kv.value().0.clone(),
                None => {
                    log::warn!("Peer {answering_peer_id} doesn't exist; cannot add ICE candidate");
                    return Ok(None);
                }
            };
            log::info!("ICE candidate added: {candidate:?}");
            if let Err(e) = peer_conn.add_ice_candidate(candidate).await {
                // TODO: we probably can handle ICE exchange fail...
                log::error!("Failed to add ICE candidate: {e}");
                return Ok(None);
            }
        }
    }
    Ok(None)
}

/// By "empty" I mean a peer connection that hasn't been bound to a local or remote SDP yet.
/// Return: if success, (peer-connection, ice-recv) where ice-recv is a receiver that receives when
/// a ICE-gathering-complete signal is sent.
async fn create_empty_peer_connection(
    peer_id: Uuid,
) -> Result<impl PeerConnection, String> {
    let handler = Arc::new(WebRtcHandler {
    });
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
        .map_err(|e| e.to_string())?;
    let registry = match register_default_interceptors(Registry::new(), &mut media_engine) {
        Ok(reg) => reg,
        Err(e) => {
            // really, how could this fail?
            log::error!("Registering default interceptors failed: {e}");
            return Err(e.to_string());
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
            return Err(e.to_string());
        }
    };
    Ok(peer_conn)
}

/// (Half)-Configures and adds a peer to OTHER_PEERS table.
/// Half-configure because we still need to wait for an answer (if self_offer is true) or an offer
/// (otherwise) from the other peer.
/// Parameters:
/// - peer_id: UUID of the peer to connect to.
/// - self_offer: if true, this peer's local description is an offer, otherwise an answer.
/// Return: if an offer needs to be sent, return Some(offer)
/// TODO: don't just return a String as error.
async fn add_peer(
    peer_id: Uuid,
    self_offer: bool,
) -> Result<Option<RTCSessionDescription>, String> {
    let peer_conn = create_empty_peer_connection(peer_id).await?;
    log::trace!("Created empty peer connection for {peer_id}");

    if self_offer {
        let offer = match peer_conn.create_offer(None).await {
            Ok(offer) => offer,
            Err(e) => {
                log::error!("Cannot create offer: {e}");
                // TODO: we should probably retry, but anyways...
                return Err(e.to_string());
            }
        };

        match peer_conn.set_local_description(offer).await {
            Ok(()) => {}
            Err(e) => {
                // this is probably an error on *my* end.
                log::error!("Cannot set offer or answer as local description: {e}");
                return Ok(None);
            }
        }
    }

    log::trace!("Gathering ICE...");
    
    let return_offer = if self_offer {
        peer_conn.local_description().await
    } else {
        None
    };

    match OTHER_PEERS.insert(
        peer_id,
        (
            Arc::new(peer_conn),
            if self_offer {
                PeerSetupStage::WaitingAnswer
            } else {
                PeerSetupStage::WaitingOffer
            },
        ),
    ) {
        Some(_) => {
            log::warn!(
                "Peer ID {peer_id} already exists, overwriting and waiting for {} from peer...",
                if self_offer { "answer" } else { "offer" }
            );
        }
        None => {
            log::info!(
                "Created PeerConnection for {peer_id}, waiting for {} from peer...",
                if self_offer { "answer" } else { "offer" }
            );
        }
    }

    Ok(return_offer)
}

/// Given a half-configured PeerConnection (created by [`add_peer`]), and the SDP needed, complete
/// the PeerConnection setup.
/// Return the answer to be sent to the other end, if applicable.
async fn finish_configure_peer_connection(
    peer_id: Uuid,
    sdp: RTCSessionDescription,
) -> Result<Option<RTCSessionDescription>, String> {
    let (peer_conn, setup_stage) = match OTHER_PEERS.get(&peer_id) {
        None => {
            // TODO: we should return something other than a String.
            // But, this is probably an error we can't really handle anyways.
            return Err("Peer {peer_id} doesn't exist!".into());
        }
        Some(kv) => (kv.value().0.clone(), kv.value().1.clone()),
    };
    match setup_stage {
        PeerSetupStage::Done => Err("Peer {peer_id} is already set up!".into()),
        PeerSetupStage::WaitingAnswer => {
            peer_conn.set_remote_description(sdp);
            log::info!("Set up PeerConnection with {peer_id}");
            Ok(None)
        }
        PeerSetupStage::WaitingOffer => {
            peer_conn.set_remote_description(sdp);
            let answer = peer_conn
                .create_answer(None)
                .await
                .map_err(|e| e.to_string())?;
            peer_conn.set_local_description(answer);
            log::info!("Need to send local description to {peer_id}");
            Ok(peer_conn.local_description().await)
        }
    }
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

#[derive(Clone, Copy)]
enum PeerSetupStage {
    WaitingOffer,
    WaitingAnswer,
    Done,
}

struct WebRtcHandler {
    // TODO: ping the ICE candidate when `on_ice_candidate`
}
#[async_trait::async_trait]
impl PeerConnectionEventHandler for WebRtcHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        log::debug!("gathering state: {:?}", state);
    }
}
