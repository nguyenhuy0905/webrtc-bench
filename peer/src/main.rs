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
    stream::{SplitSink, SplitStream, StreamExt},
    SinkExt,
};
use rtc::{
    peer_connection::configuration::media_engine::MIME_TYPE_H264,
    rtp_transceiver::rtp_sender::{RTCRtpCodec, RTCRtpCodecParameters, RtpCodecKind},
};
use std::sync::{Arc, LazyLock};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex, OnceCell},
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
        RTCIceGatheringState, RTCIceServer, RTCPeerConnectionIceEvent, RTCSessionDescription,
        Registry,
    },
    data_channel::DataChannel,
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
    let (write_stream, mut read_stream) = ws_stream.split();

    // 0.0 <C-c> handler, so that the web socket properly closes
    let (ctrlc_tx, mut ctrlc_rx) = mpsc::channel::<()>(1);
    ctrlc::set_handler(move || {
        let _ = ctrlc_tx.try_send(());
    })
    .map_err(|e| e.to_string())?;

    // 0.1 configure media engine.
    // We can't really do much else if this fails...

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

/// Grab the WebSocket read and write streams and handle any message that needs to be sent/recv.
async fn handle_signal(
    mut read_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    mut write_stream: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
) -> Result<(), String> {
    // let write_stream = Arc::new(Mutex::new(write_stream));

    // we can't clone the write stream, so...
    let (outgoing, mut incoming) = mpsc::channel::<WsExchangeMsg>(4);

    // send stuff to the server. A.k.a simply forward what is put into `outgoing`.
    tokio::spawn(async move {
        // explicitly move inside...
        // let mut incoming = incoming;
        while let Some(msg) = incoming.recv().await {
            // this shouldn't fail...
            let msg_str = serde_json::to_string(&msg).unwrap();
            if let Err(e) = write_stream.send(Message::from(msg_str)).await {
                log::warn!("Error sending message: {e}");
            }
        }
    });

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
                log::warn!("Cannot understand message {msg}: {e}");
                continue;
            }
        };
        log::trace!("Received message: {msg:?}");

        let og = outgoing.clone();
        // so that we can go back to processing messages right away.
        tokio::spawn(async move {
            if let Err(e) = handle_message(msg, og).await {
                log::warn!("Handle message error: {e}");
            }
        });
    }
    Ok(())
}

/// Given a WsExchangeMsg, deal with it!
/// Return: Some(message) if there's something needed to be sent back to the signaling server.
/// TODO: don't return a String as error... we can do better.
async fn handle_message(
    msg: WsExchangeMsg,
    outgoing: mpsc::Sender<WsExchangeMsg>,
) -> Result<(), String> {
    // make sure message is very valid.
    match msg {
        WsExchangeMsg::JoinPeerId(_) => {
            log::warn!("Unexpected message: {msg:?}");
            return Ok(());
        }
        WsExchangeMsg::ExistingPeer { peer_id } => {
            // log::warn!("Add existing peer {peer_id}. This part of the code is bugged");
            add_existing_peer(peer_id, outgoing.clone()).await?;
        }
        WsExchangeMsg::NewPeer { peer_id } => {
            // log::warn!("Add new peer {peer_id}. This part of the code is bugged");
            add_new_peer(peer_id, outgoing.clone()).await?;
        }
        WsExchangeMsg::Sdp {
            send_to_id,
            answering_peer_id,
            sdp,
        } => {
            log::warn!("Add SDP from {answering_peer_id}. This part of the code is bugged");
            if send_to_id != *SELF_UUID.get().unwrap() {
                log::warn!("Received a message destined to {send_to_id}");
                return Ok(());
            }
            finish_configure_peer_connection(answering_peer_id, sdp, outgoing.clone()).await?;
            log::info!("PeerConnection with {answering_peer_id} fully done!");
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
                return Ok(());
            }
            let peer_conn = match OTHER_PEERS.get(&answering_peer_id) {
                Some(kv) => kv.value().0.clone(),
                None => {
                    log::warn!("Peer {answering_peer_id} doesn't exist; cannot add ICE candidate");
                    return Ok(());
                }
            };
            log::info!("ICE candidate added: {candidate:?}");
            if let Err(e) = peer_conn.add_ice_candidate(candidate).await {
                // TODO: we probably can handle ICE exchange fail...
                log::error!("Failed to add ICE candidate: {e}");
                return Ok(());
            }
        }
    }
    Ok(())
}

/// By "empty" I mean a peer connection that hasn't been bound to a local or remote SDP yet.
/// Returns the peer connection if successful.
async fn create_empty_peer_connection(
    peer_id: Uuid,
    outgoing: mpsc::Sender<WsExchangeMsg>,
) -> Result<impl PeerConnection, String> {
    let handler = Arc::new(WebRtcHandler {
        other_peer_id: peer_id,
        ws_out_tx: outgoing,
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

/// (Half)-Configures and adds an existing peer to OTHER_PEERS table.
/// Half-configure because we still need to wait for an offer from the other peer.
/// After the peer is added (a.k.a. after `await`ing on this function finishes), this PeerConnection
/// will send an SDP offer to the signaling server, delivered to the other peer, then the other peer
/// should send back an answer...
async fn add_existing_peer(
    peer_id: Uuid,
    outgoing: mpsc::Sender<WsExchangeMsg>,
) -> Result<(), String> {
    let peer_conn = create_empty_peer_connection(peer_id, outgoing).await?;
    log::trace!("Created empty peer connection for {peer_id}");

    match OTHER_PEERS.insert(peer_id, (Arc::new(peer_conn), PeerSetupStage::WaitingOffer)) {
        Some(_) => {
            log::warn!(
                "Peer ID {peer_id} already exists, overwriting and waiting for offer from peer...",
            );
        }
        None => {
            log::info!("Created PeerConnection for {peer_id}, waiting for offer from peer...",);
        }
    }
    Ok(())
}

/// (Half)-Configures and adds a new peer to OTHER_PEERS table. This PeerConnection has its offer
/// set as remote description, and sends its offer via `outgoing`.
/// Half-configure because we still need to wait for an answer from the other peer.
/// After the peer is added (a.k.a. after `await`ing on this function finishes), this PeerConnection
/// will
async fn add_new_peer(peer_id: Uuid, outgoing: mpsc::Sender<WsExchangeMsg>) -> Result<(), String> {
    let peer_conn = create_empty_peer_connection(peer_id, outgoing.clone()).await?;
    log::trace!("Created empty peer connection for {peer_id}");

    if let Err(e) = peer_conn.create_data_channel("Data", None).await {
        log::error!("Failed to create data channel: {e}");
    }
    let offer = peer_conn
        .create_offer(None)
        .await
        .map_err(|e| e.to_string())?;
    peer_conn
        .set_local_description(offer)
        .await
        .map_err(|e| e.to_string())?;
    if let Err(e) = outgoing
        .send(WsExchangeMsg::Sdp {
            send_to_id: peer_id,
            answering_peer_id: *SELF_UUID.get().unwrap(),
            sdp: peer_conn.local_description().await.unwrap(),
        })
        .await
    {
        log::error!("Cannot send SDP to {peer_id}: {e}");
        return Err(e.to_string());
    };
    log::debug!("Sent SDP to {peer_id}");

    match OTHER_PEERS.insert(
        peer_id,
        (Arc::new(peer_conn), PeerSetupStage::WaitingAnswer),
    ) {
        Some(_) => {
            log::warn!(
                "Peer ID {peer_id} already exists, overwriting and waiting for answer from peer...",
            );
        }
        None => {
            log::info!("Created PeerConnection for {peer_id}, waiting for answer from peer...",);
        }
    }
    Ok(())
}

async fn finish_configure_peer_connection(
    peer_id: Uuid,
    sdp: RTCSessionDescription,
    outgoing: mpsc::Sender<WsExchangeMsg>
) -> Result<(), String> {
    let (peer_conn, setup_stage) = match OTHER_PEERS.get(&peer_id) {
        None => {
            // TODO: we should return something other than a String.
            // But, this is probably an error we can't really handle anyways.
            return Err("Peer {peer_id} doesn't exist!".into());
        }
        Some(kv) => (kv.value().0.clone(), kv.value().1),
    };
    log::trace!("Checking setup stage...");
    match setup_stage {
        PeerSetupStage::Done => return Err("Peer {peer_id} is already set up!".into()),
        PeerSetupStage::WaitingAnswer => {
            if let Err(e) = peer_conn.set_remote_description(sdp).await {
                log::error!("PeerConnection with {peer_id}: {e}");
                return Err(e.to_string());
            }
            log::info!("Set up PeerConnection with {peer_id}");
        }
        PeerSetupStage::WaitingOffer => {
            if let Err(e) = peer_conn.set_remote_description(sdp).await {
                log::error!("PeerConnection with {peer_id}: {e}");
                return Err(e.to_string());
            }
            let answer = peer_conn
                .create_answer(None)
                .await
                .map_err(|e| e.to_string())?;
            peer_conn.set_local_description(answer).await;
            log::info!("Need to send local description to {peer_id}");
            if let Err(e) = outgoing.send(WsExchangeMsg::Sdp {
                send_to_id: peer_id,
                answering_peer_id: *SELF_UUID.get().unwrap(),
                sdp: peer_conn.local_description().await.unwrap()
            }).await {
                return Err(e.to_string());
            }
        }
    }
    OTHER_PEERS.get_mut(&peer_id).map(|mut kv| kv.value_mut().1 = PeerSetupStage::Done);

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

#[derive(Clone, Copy)]
enum PeerSetupStage {
    WaitingOffer,
    WaitingAnswer,
    Done,
}

struct WebRtcHandler {
    other_peer_id: Uuid,
    ws_out_tx: mpsc::Sender<WsExchangeMsg>,
}
#[async_trait::async_trait]
impl PeerConnectionEventHandler for WebRtcHandler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        match event.candidate.to_json() {
            Ok(candidate_init) => {
                if let Err(e) = self
                    .ws_out_tx
                    .send(WsExchangeMsg::IceCandidate {
                        send_to_id: self.other_peer_id,
                        answering_peer_id: *SELF_UUID
                            .get()
                            .expect("PEER_UUID should have been set!"),
                        candidate: candidate_init,
                    })
                    .await
                {
                    log::error!("Cannot send ICE candidate: {e}");
                }
            }
            Err(e) => {
                log::error!("Cannot turn ICE candidate to JSON: {e}");
            }
        }
    }

    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        log::debug!("gathering state: {state}");
    }

    async fn on_data_channel(&self, channel: Arc<dyn DataChannel>) {
        log::info!("onDataChannel run!");
    }
}
