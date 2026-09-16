// game plan here:
// 0. set up some sort of signaling server. Of course. [UPDATE: DONE]
// 1. find a way for the peers to ping the signaling server. [UPDATE: DONE]
// 1.1 by "ping" I mean send/recv SDPs. [UPDATE: DONE]
// 2. create a track from a local H.264 file.
//   - currently we are using a data channel to force the PeerConnection to go through.

// So, `matchbox` turned out to not be such a bright idea.
// I'll make my own WebSocket (building from `tokio-tungstenite`) then

// TODO: move sending SDP offer/answer to `on_negotiation_needed`.

mod globals;
mod handle;

use anyhow::Context;
use clap::Parser;
use common::WsExchangeMsg;
use futures_util::{
    stream::{SplitSink, SplitStream, StreamExt},
    SinkExt,
};
use globals::{
    PeerInfo, CTRLC_BROADCAST, H26X_FRAME_DURATION, OTHER_PEERS, PEER_CONF, RUNTIME, SELF_UUID,
    VIDEO_CODEC, VIDEO_FILE_NAME,
};
use rtc::{
    media::{io::h26x_reader::sample_reader::H26xSampleReader, Sample},
    rtp_transceiver::{
        rtp_sender::{RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind},
        PayloadType,
    },
};
use std::{
    fs::File,
    io::BufReader,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{
    tungstenite::{error::Error as TungsteniteError, protocol::Message},
    MaybeTlsStream, WebSocketStream,
};
use uuid::Uuid;
#[allow(unused)]
use webrtc::{
    media_stream::{track_local::static_sample::TrackLocalStaticSample, MediaStreamTrack, Track},
    peer_connection::{
        register_default_interceptors, MediaEngine, PeerConnection, PeerConnectionBuilder,
        RTCSdpType, RTCSessionDescription, RTCSignalingState, Registry,
    },
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

fn main() -> anyhow::Result<()> {
    RUNTIME.block_on(main_async())
}

async fn main_async() -> anyhow::Result<()> {
    env_logger::init();
    let args = Opts::parse();

    // initialize some stuff
    globals::VIDEO_FILE_NAME.get_or_init(|| args.video_file);

    // connect to signaling server
    // this will tell the signaling server that this peer wants to join the channel. Currently,
    // there's only one channel to join.
    let (ws_stream, _) = tokio_tungstenite::connect_async(format!("ws://{}", args.host)).await?;
    let (write_stream, mut read_stream) = ws_stream.split();

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
        anyhow::bail!("WsExchangeMsg didn't return JoinPeerId");
    }

    handle_signal(read_stream, write_stream).await?;

    Ok(())
}

/// Grab the WebSocket read and write streams and handle any message that needs to be sent/recv.
async fn handle_signal(
    read_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    mut write_stream: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
) -> anyhow::Result<()> {
    let (outgoing, mut incoming) = mpsc::channel::<WsExchangeMsg>(4);

    // send stuff to the server. A.k.a simply forward what is put into `outgoing`.
    tokio::spawn(async move {
        while let Some(msg) = incoming.recv().await {
            // this shouldn't fail...
            let msg_str = serde_json::to_string(&msg).unwrap();
            if let Err(e) = write_stream.send(Message::from(msg_str)).await {
                log::warn!("Error sending message: {e:?}");
            }
        }
    });

    let wait_for_ctrlc = async {
        let mut ctrlc_rx = CTRLC_BROADCAST.subscribe();
        #[allow(unused)]
        ctrlc_rx.recv().await;
    };
    tokio::select! {
        _ = signal_loop(read_stream, outgoing.clone()) => {
            log::info!("Signaling socket closed");
        },
        _ = wait_for_ctrlc => {
            log::info!("Received C-c.");
        }
    };

    // be graceful
    outgoing
        .send(WsExchangeMsg::LeavePeerId(*SELF_UUID.get().unwrap()))
        .await?;

    Ok(())
}

/// Converts message from Result<Message, TungsteniteError> to WsExchangeMsg.
fn convert_message(msg: Result<Message, TungsteniteError>) -> anyhow::Result<WsExchangeMsg> {
    msg.context("WebSocket error")?
        .to_text()
        .context("Converting to text message")
        .and_then(|msg| serde_json::from_str(msg).context("Converting text message to JSON"))
}

async fn signal_loop(
    mut read_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    outgoing: mpsc::Sender<WsExchangeMsg>,
) {
    while let Some(msg) = read_stream.next().await {
        let msg = match convert_message(msg) {
            Ok(msg) => msg,
            Err(e) => {
                log::warn!("Skipping message: {e:?}");
                continue;
            }
        };
        log::trace!("Received message: {msg:?}");

        if let Err(e) = handle_message(msg, outgoing.clone()).await {
            log::warn!("Error handling message: {e:?}");
        }
    }
}

/// Broken to a separate function so that I don't have to keep, `if Err(e) = ... {log();}`.
async fn handle_message(
    msg: WsExchangeMsg,
    outgoing: mpsc::Sender<WsExchangeMsg>,
) -> anyhow::Result<()> {
    match msg {
        WsExchangeMsg::NewPeer(peer_id) => {
            log::warn!("TODO: new peer {peer_id}");
            if peer_id == *SELF_UUID.get().unwrap() {
                anyhow::bail!("Repeated peer ID {peer_id}");
            }
            add_new_peer(peer_id, outgoing.clone())
                .await
                .context("Cannot add new peer {peer_id}")?;
            log::info!("Peer {peer_id} added");

            // SAFETY: we just added this peer in `add_new_peer`.
            let peer_info = OTHER_PEERS.get(&peer_id).unwrap();
            let conn = &peer_info.conn;
            let offer = conn
                .create_offer(None)
                .await
                .context("Cannot create offer to send to {peer_id}")?;
            conn.set_local_description(offer)
                .await
                .context("Cannot set local description for {peer_id}")?;

            // SAFETY: we just set local description above
            let sdp = conn.local_description().await.unwrap();
            outgoing
                .send(WsExchangeMsg::Sdp {
                    // SAFETY: set when first opening signaling connection
                    from_id: *SELF_UUID.get().unwrap(),
                    to_id: peer_id,
                    sdp,
                })
                .await
                .context("Cannot send offer to {peer_id}")?;

            log::info!("Sent offer to {peer_id}, waiting answer");
        }
        WsExchangeMsg::LeavePeerId(peer_id) => {
            log::warn!("TODO Peer {peer_id} leaving");
            if let Some(kv) = OTHER_PEERS.remove(&peer_id) {
                kv.1.conn
                    .close()
                    .await
                    .context("Cannot close {peer_id} connection")?;
                log::info!("Closed {peer_id}'s connection");
            }
        }
        WsExchangeMsg::Sdp {
            from_id,
            to_id,
            sdp,
        } => {
            // SAFETY: should already be set when initiating signaling connection
            if *SELF_UUID.get().unwrap() != to_id {
                anyhow::bail!("Received SDP destined for {to_id}");
            }
            // TODO: if peer is not found, create the peer.
            let kv = if let Some(kv) = OTHER_PEERS.get(&from_id) {
                kv
            } else {
                log::debug!("Peer {from_id} not found. Creating the peer connection...");
                add_new_peer(from_id, outgoing.clone())
                    .await
                    .context("Cannot add offering/answering peer {from_id}")?;
                // SAFETY: we just added the peer.
                OTHER_PEERS.get(&from_id).unwrap()
            };
            let peer_info = kv.value();

            // grabbed from https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API/Perfect_negotiation
            let ready_for_offer = !peer_info.making_offer.load(Ordering::Acquire)
                && (*peer_info.signal_state.read().await == RTCSignalingState::Stable
                    || peer_info.set_remote_answer_pending.load(Ordering::Acquire));
            let offer_collision = (sdp.sdp_type == RTCSdpType::Offer) && !ready_for_offer;
            peer_info
                .ignore_offer
                .store(!is_polite(from_id) && offer_collision, Ordering::Release);

            if peer_info.ignore_offer.load(Ordering::Acquire) {
                anyhow::bail!("Ignore offer from {from_id} as you are impolite");
            }

            let sdp_type = sdp.sdp_type;
            if offer_collision {
                log::info!("We are polite, we roll back");
                peer_info
                    .conn
                    .set_local_description(
                        RTCSessionDescription::rollback(None)
                            .context("Cannot create a rollback local description")?,
                    )
                    .await
                    .context("Cannot rollback local description")?;
            } else {
                peer_info.conn.set_remote_description(sdp).await.with_context(|| format!("Cannot set remote description from {from_id}"))?;
            }

            // peer_info
            //     .set_remote_answer_pending
            //     .store(sdp_type == RTCSdpType::Answer, Ordering::Release);
            // peer_info
            //     .conn
            //     .set_remote_description(sdp)
            //     .await
            //     .with_context(|| {
            //         format!(
            //             "Cannot set {} from {from_id} as remote description",
            //             if sdp_type == RTCSdpType::Offer {
            //                 "offer"
            //             } else {
            //                 "answer"
            //             }
            //         )
            //     })?;
            // peer_info
            //     .set_remote_answer_pending
            //     .store(false, Ordering::Release);
            //
            if sdp_type == RTCSdpType::Offer {
                let answer = peer_info
                    .conn
                    .create_answer(None)
                    .await
                    .context("Cannot create answer for {from_id}")?;
                peer_info
                    .conn
                    .set_local_description(answer)
                    .await
                    .with_context(|| {
                        format!("Cannot set local description for connection with {from_id}")
                    })?;
                outgoing
                    .send(WsExchangeMsg::Sdp {
                        from_id: to_id,
                        to_id: from_id,
                        sdp: peer_info.conn.local_description().await.ok_or_else(|| {
                            anyhow::anyhow!(
                                "Cannot retrieve local description for connection with {from_id}"
                            )
                        })?,
                    })
                    .await
                    .with_context(|| format!("Cannot send SDP answer back to {from_id}"))?;
                // start streaming away!
                // peer_info
                //     .start_stream_tx
                //     .send(())
                //     .await
                //     .context("Cannot ping to start streaming for {from_id}")?;
            }
        }
        WsExchangeMsg::IceCandidate {
            from_id,
            to_id,
            candidate,
        } => {
            if to_id != *SELF_UUID.get().expect("SELF_UUID should already be set!") {
                anyhow::bail!("Received a message destined to {to_id}");
            }
            let kv = OTHER_PEERS
                .get(&from_id)
                .context("Peer {from_id} doesn't exist")?;
            kv.value()
                .conn
                .add_ice_candidate(candidate)
                .await
                .or_else(|e| {
                    if !kv.value().ignore_offer.load(Ordering::Acquire) {
                        log::debug!("ICE error ignored as peer is ignoring offer: {e:?}");
                        Ok(())
                    } else {
                        Err(e)
                    }
                })
                .with_context(|| format!("Failed to add ICE candidate to {from_id}"))?;
        }
        _ => {
            anyhow::bail!("Unexpected message: {msg:?}");
        }
    }

    Ok(())
}

/// Creates a peer with the video track added, and adds to OTHER_PEERS table.
async fn add_new_peer(peer_id: Uuid, outgoing: mpsc::Sender<WsExchangeMsg>) -> anyhow::Result<()> {
    let peer_conn = Arc::new(create_empty_peer_conn(peer_id, outgoing.clone()).await?);
    // TODO: add a media track. Then `on_negotiation_needed` will be triggered.
    // TODO: SSRC collision can technically happen...
    let ssrc = rand::random::<u32>();
    let video_track = Arc::new(
        TrackLocalStaticSample::new(MediaStreamTrack::new(
            // TODO: these, as suggested by the specs, should be UUIDs.
            // stream ID
            "video-stream-1".to_string(),
            // track ID
            "video-track-1".to_string(),
            // label
            "Video track".to_string(),
            RtpCodecKind::Video,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(ssrc),
                    ..Default::default()
                },
                // why do I have to repeat myself here...
                codec: VIDEO_CODEC.rtp_codec.clone(),
                ..Default::default()
            }],
        ))
        .context("Cannot create video track")?,
    );
    // notify when to start a stream
    let (start_stream_tx, mut start_stream_rx) = mpsc::channel::<()>(1);

    OTHER_PEERS.insert(
        peer_id,
        PeerInfo::new(peer_conn.clone(), start_stream_tx.clone()),
    );

    log::debug!("Adding track...");
    let sender = peer_conn
        .add_track(video_track.clone())
        .await
        .context("Cannot add track to peer connection")?;

    let payload_type = sender
        .get_parameters()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .and_then(|negotiate| {
            negotiate
                .rtp_parameters
                .codecs
                .first()
                .map(|codec| codec.payload_type)
                .ok_or_else(|| anyhow::anyhow!("No negotiated codec!"))
        })?;

    // then spawn a stream sending video
    tokio::spawn(async move {
        if let Some(()) = start_stream_rx.recv().await {
            log::info!("Start playing file from {}", VIDEO_FILE_NAME.get().unwrap());
            if let Err(e) = stream_video(video_track, payload_type).await {
                log::error!("Cannot stream video: {e:?}");
            }
        }
    });

    Ok(())
}

/// By "empty" I mean a peer connection that hasn't been bound to a local or remote SDP yet.
/// Returns the peer connection if successful.
async fn create_empty_peer_conn(
    peer_id: Uuid,
    outgoing: mpsc::Sender<WsExchangeMsg>,
) -> anyhow::Result<impl PeerConnection> {
    // set up the media engine and registry
    let mut media_engine = MediaEngine::default();
    media_engine
        .register_codec(VIDEO_CODEC.clone(), RtpCodecKind::Video)
        .context("Cannot register H264 codec")?;
    let registry = register_default_interceptors(Registry::new(), &mut media_engine)
        .context("Cannot register interceptor")?;

    PeerConnectionBuilder::new()
        .with_configuration(PEER_CONF.clone())
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .with_handler(Arc::new(handle::WebRtcHandler::new(peer_id, outgoing)))
        .with_udp_addrs(vec!["0.0.0.0:0"])
        .build()
        .await
        .context("Failed to create PeerConnection with {peer_id}")
}

async fn stream_video(
    video_track: Arc<TrackLocalStaticSample>,
    payload_type: PayloadType,
) -> anyhow::Result<()> {
    let file = File::open(VIDEO_FILE_NAME.get().unwrap()).context("Cannot open H264 file")?;
    let reader = BufReader::new(file);
    // really, you must've had SSRC here already
    let ssrc = *video_track.ssrcs().await.first().unwrap();
    // the number is 2^20, the bool means it's not H265
    let mut video_reader = H26xSampleReader::new(reader, 1_048_576, false);
    // we only need 30fps, so don't go overboard.
    let mut tick = tokio::time::interval(H26X_FRAME_DURATION);
    // let mut instant = Instant::now();
    // let mut timestamp: u32 = rand::random();
    loop {
        let sample = match video_reader.next_sample() {
            Ok(sample) => sample,
            Err(e) => {
                log::info!("All video frames parsed and sent: {e}");
                break;
            }
        };

        if let Err(e) = video_track
            .sample_writer(ssrc, payload_type)
            .write_sample(&Sample {
                data: sample.data,
                duration: if sample.timed {
                    H26X_FRAME_DURATION
                } else {
                    Duration::ZERO
                },
                ..Default::default()
            })
            .await
        {
            log::warn!("Error sending video: {e}");
            break;
        };
        if sample.timed {
            tick.tick().await;
        }
    }

    Ok(())
}

/// Determine if this peer should be polite with the other peer.
/// Polite peers don't ignore other peers' offers. So, if both ends try to give offer, the
/// polite peer drops its offer.
/// SAFETY: SELF_UUID must already be set before calling.
pub fn is_polite(other_id: Uuid) -> bool {
    return *SELF_UUID.get().unwrap() < other_id;
}

// async fn main_async() -> Result<(), String> {
//     env_logger::init();
//     let args = Opts::parse();
//
//     // initialize some stuff
//     VIDEO_FILE_NAME.set(args.video_file);
//
//     // connect to signaling server
//     let (ws_stream, _) = tokio_tungstenite::connect_async(format!("ws://{}", args.host))
//         .await
//         .map_err(|e| e.to_string())?;
//     let (write_stream, mut read_stream) = ws_stream.split();
//
//     // 0.1 configure media engine.
//     // We can't really do much else if this fails...
//
//     // 1. Tell the signaling server that I wanna join the channel. (to be fair, that's inferred from
//     //    the fact we initiated the WebSocket connection).
//
//     // then we can initialize our PeerID.
//     let self_id: WsExchangeMsg = serde_json::from_str(
//         read_stream
//             .next()
//             .await
//             .expect("Could not get this peer's ID")
//             // HACK: these errors should be handled.
//             .unwrap()
//             .to_text()
//             .unwrap(),
//     )
//     .expect("Cannot parse first WebSocket response to WsExchangeMsg");
//     // I'm pretty sure WebSocket is a reliable stream.
//     if let WsExchangeMsg::JoinPeerId(self_id) = self_id {
//         SELF_UUID
//             .set(self_id)
//             .expect("Somehow SELF_UUID is already set");
//         log::info!("Initialize self as {self_id}");
//     } else {
//         return Err("WsExchangeMsg didn't return JoinPeerId".to_string());
//     }
//
//     // 2. For each `ExistingPeer`, this peer is the offerer. And for each `NewPeer`, this peer is
//     //    the answerer. `NewPeer` *can* override existing peer.
//
//     handle_signal(read_stream, write_stream).await?;
//
//     close_peer_connections().await;
//     log::info!("WebSocket connection with signaling server closed");
//
//     Ok(())
// }
//
// /// Grab the WebSocket read and write streams and handle any message that needs to be sent/recv.
// async fn handle_signal(
//     mut read_stream: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
//     mut write_stream: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
// ) -> Result<(), String> {
//     // let write_stream = Arc::new(Mutex::new(write_stream));
//
//     // we can't clone the write stream, so...
//     let (outgoing, mut incoming) = mpsc::channel::<WsExchangeMsg>(4);
//
//     // send stuff to the server. A.k.a simply forward what is put into `outgoing`.
//     tokio::spawn(async move {
//         // explicitly move inside...
//         // let mut incoming = incoming;
//         while let Some(msg) = incoming.recv().await {
//             // this shouldn't fail...
//             let msg_str = serde_json::to_string(&msg).unwrap();
//             if let Err(e) = write_stream.send(Message::from(msg_str)).await {
//                 log::warn!("Error sending message: {e}");
//             }
//         }
//     });
//
//     let og = outgoing.clone();
//     let signal_loop = async move {
//         let og = og.clone();
//         while let Some(msg) = read_stream.next().await {
//             // convert message from Result<Message, TungsteniteError> to WsExchangeMsg.
//             let msg = match msg {
//                 Ok(msg) => msg,
//                 Err(e) => {
//                     match e {
//                         TungsteniteError::ConnectionClosed => {
//                             // can't recover...
//                             log::warn!("Signaling connection closed!");
//                             break;
//                         }
//                         TungsteniteError::Io(io_err) => {
//                             // can't recover...
//                             log::error!("I/O error: {io_err}");
//                             break;
//                         }
//                         _ => {
//                             log::warn!("WebSocket error ignored: {e}");
//                             continue;
//                         }
//                     }
//                 }
//             };
//             // make sure message is sort-of valid
//             let msg = match msg.to_text() {
//                 Ok(s) => s,
//                 Err(e) => {
//                     log::warn!("Non-text message? {e}");
//                     continue;
//                 }
//             };
//             let msg: WsExchangeMsg = match serde_json::from_str(msg) {
//                 Ok(ret) => ret,
//                 Err(e) => {
//                     log::warn!("Cannot understand message {msg}: {e}");
//                     continue;
//                 }
//             };
//             log::trace!("Received message: {msg:?}");
//
//             let og = og.clone();
//             // so that we can go back to processing messages right away.
//             tokio::spawn(async move {
//                 if let Err(e) = handle_message(msg, og).await {
//                     log::warn!("Handle message error: {e}");
//                 }
//             });
//         }
//     };
//     let wait_for_ctrlc = async {
//         let mut ctrlc_rx = CTRLC_BROADCAST.subscribe();
//         ctrlc_rx.recv().await;
//     };
//
//     tokio::select! {
//         _ = signal_loop => {
//             log::info!("Signaling socket closed!");
//         }
//         _ = wait_for_ctrlc => {
//             log::info!("Received C-c.");
//         }
//     }
//
//     // be graceful
//     outgoing
//         .send(WsExchangeMsg::LeavePeerId(*SELF_UUID.get().unwrap()))
//         .await;
//
//     Ok(())
// }
//
// /// Given a WsExchangeMsg, deal with it!
// /// Return: Some(message) if there's something needed to be sent back to the signaling server.
// /// TODO: don't return a String as error... we can do better.
// async fn handle_message(
//     msg: WsExchangeMsg,
//     outgoing: mpsc::Sender<WsExchangeMsg>,
// ) -> Result<(), String> {
//     // make sure message is very valid.
//     match msg {
//         WsExchangeMsg::JoinPeerId(_) => {
//             log::warn!("Unexpected message: {msg:?}");
//             return Ok(());
//         }
//         WsExchangeMsg::ExistingPeer { peer_id } => {
//             // log::warn!("Add existing peer {peer_id}. This part of the code is bugged");
//             add_existing_peer(peer_id, outgoing.clone()).await?;
//         }
//         WsExchangeMsg::NewPeer { peer_id } => {
//             // log::warn!("Add new peer {peer_id}. This part of the code is bugged");
//             add_new_peer(peer_id, outgoing.clone()).await?;
//         }
//         WsExchangeMsg::Sdp {
//             send_to_id,
//             answering_peer_id,
//             sdp,
//         } => {
//             if send_to_id != *SELF_UUID.get().unwrap() {
//                 log::warn!("Received a message destined to {send_to_id}");
//                 return Ok(());
//             }
//             finish_configure_peer_connection(answering_peer_id, sdp, outgoing.clone()).await?;
//             log::info!("PeerConnection with {answering_peer_id} fully done!");
//         }
//         WsExchangeMsg::LeavePeerId(leaving_peer_id) => {
//             log::info!("Peer {leaving_peer_id} leaving");
//             if let Some(kv) = OTHER_PEERS.remove(&leaving_peer_id) {
//                 kv.1 .0.close().await;
//                 log::debug!("Closed {leaving_peer_id}'s connection");
//             }
//         }
//         WsExchangeMsg::IceCandidate {
//             send_to_id,
//             answering_peer_id,
//             candidate,
//         } => {
//             if send_to_id != *SELF_UUID.get().expect("SELF_UUID should already be set!") {
//                 log::warn!("Received a message destined to {send_to_id}");
//                 return Ok(());
//             }
//             let peer_conn = match OTHER_PEERS.get(&answering_peer_id) {
//                 Some(kv) => kv.value().0.clone(),
//                 None => {
//                     log::warn!("Peer {answering_peer_id} doesn't exist; cannot add ICE candidate");
//                     return Ok(());
//                 }
//             };
//             log::info!("ICE candidate added: {candidate:?}");
//             if let Err(e) = peer_conn.add_ice_candidate(candidate).await {
//                 // TODO: we probably can handle ICE exchange fail...
//                 log::error!("Failed to add ICE candidate: {e}");
//                 return Ok(());
//             }
//         }
//     }
//     Ok(())
// }
//
// /// By "empty" I mean a peer connection that hasn't been bound to a local or remote SDP yet.
// /// Returns the peer connection if successful.
// async fn create_empty_peer_connection(
//     peer_id: Uuid,
//     outgoing: mpsc::Sender<WsExchangeMsg>,
// ) -> Result<(impl PeerConnection, mpsc::Sender<()>), String> {
//     let handler = Arc::new(WebRtcHandler {
//         runtime: RUNTIME.clone(),
//         other_peer_id: peer_id,
//         ws_out_tx: outgoing,
//     });
//     // set up the media engine and registry
//     let mut media_engine = MediaEngine::default();
//     if let Err(e) = media_engine.register_codec(VIDEO_CODEC.clone(), RtpCodecKind::Video) {
//         log::error!("Cannot register H264 video codec: {e}");
//         return Err(e.to_string());
//     }
//     let registry = match register_default_interceptors(Registry::new(), &mut media_engine) {
//         Ok(reg) => reg,
//         Err(e) => {
//             // really, how could this fail?
//             log::error!("Registering default interceptors failed: {e}");
//             return Err(e.to_string());
//         }
//     };
//
//     // gotta add all the tracks before I send the peer rolling
//     let peer_conn = match PeerConnectionBuilder::new()
//         .with_configuration(PEER_CONF.clone())
//         .with_media_engine(media_engine.clone())
//         .with_interceptor_registry(registry)
//         .with_handler(handler)
//         .with_udp_addrs(vec!["0.0.0.0:0"])
//         .build()
//         .await
//     {
//         Ok(conn) => conn,
//         Err(e) => {
//             log::error!("Cannot create connection with {peer_id}: {e}");
//             // TODO: we should probably retry, but anyways...
//             return Err(e.to_string());
//         }
//     };
//
//     // add the video track
//
//     // TODO: SSRC collision technically can happen.
//     let ssrc = rand::random::<u32>();
//     // create the track to send video
//     let video_track = Arc::new(
//         match TrackLocalStaticSample::new(MediaStreamTrack::new(
//             // TODO: these, as suggested by the specs, should be UUIDs.
//             // stream ID
//             "video-stream-1".to_string(),
//             // track ID
//             "video-track-1".to_string(),
//             // label
//             "Video track".to_string(),
//             RtpCodecKind::Video,
//             vec![RTCRtpEncodingParameters {
//                 rtp_coding_parameters: RTCRtpCodingParameters {
//                     ssrc: Some(ssrc),
//                     ..Default::default()
//                 },
//                 // why do I have to repeat myself here...
//                 codec: VIDEO_CODEC.rtp_codec.clone(),
//                 ..Default::default()
//             }],
//         )) {
//             Ok(track) => track,
//             Err(e) => {
//                 // most likely error on my end
//                 log::error!("Cannot create video track: {e}");
//                 return Err(e.to_string());
//             }
//         },
//     );
//     let sender = match peer_conn.add_track(video_track.clone()).await {
//         Ok(sender) => sender,
//         Err(e) => {
//             log::error!("Cannot add track: {e}");
//             return Err(e.to_string());
//         }
//     };
//
//     let (start_stream_tx, mut start_stream_rx) = mpsc::channel::<()>(1);
//
//     // check negotiated payload type
//     let payload_type = sender
//         .get_parameters()
//         .await
//         .map_err(|e| e.to_string())
//         .and_then(|negotiate| {
//             negotiate
//                 .rtp_parameters
//                 .codecs
//                 .first()
//                 .map(|codec| codec.payload_type)
//                 .ok_or_else(|| "no negotiated codec!".to_string())
//         })?;
//     // then spawn a stream sending video
//     // tokio::spawn(async move {
//     //     start_stream_rx.recv().await;
//     //     log::info!("Start playing file from {}", VIDEO_FILE_NAME.get().unwrap());
//     //     if let Err(e) = stream_video(video_track, payload_type).await {
//     //         log::error!("Cannot stream video: {e}");
//     //     }
//     // });
//
//     Ok((peer_conn, start_stream_tx))
// }
//
// /// (Half)-Configures and adds an existing peer to OTHER_PEERS table.
// /// Half-configure because we still need to wait for an offer from the other peer.
// /// After the connection is added (a.k.a. after `await`ing on this function finishes), it waits for
// /// an offer from the other peer.
// async fn add_existing_peer(
//     peer_id: Uuid,
//     outgoing: mpsc::Sender<WsExchangeMsg>,
// ) -> Result<(), String> {
//     let (peer_conn, start_stream_tx) = create_empty_peer_connection(peer_id, outgoing).await?;
//     log::trace!("Created empty peer connection for {peer_id}");
//
//     match OTHER_PEERS.insert(
//         peer_id,
//         (
//             Arc::new(peer_conn),
//             PeerSetupStage::WaitingOffer,
//             start_stream_tx,
//         ),
//     ) {
//         Some(_) => {
//             log::warn!(
//                 "Peer ID {peer_id} already exists, overwriting and waiting for offer from peer...",
//             );
//         }
//         None => {
//             log::info!("Created PeerConnection for {peer_id}, waiting for offer from peer...",);
//         }
//     }
//     Ok(())
// }
//
// /// (Half)-Configures and adds a new peer to OTHER_PEERS table. This PeerConnection has its offer
// /// set as remote description, and sends its offer via `outgoing`.
// ///
// /// Half-configure because we still need to wait for an answer from the other peer.
// async fn add_new_peer(peer_id: Uuid, outgoing: mpsc::Sender<WsExchangeMsg>) -> Result<(), String> {
//     let (peer_conn, start_stream_tx) =
//         create_empty_peer_connection(peer_id, outgoing.clone()).await?;
//     log::trace!("Created empty peer connection for {peer_id}");
//
//     let offer = peer_conn
//         .create_offer(None)
//         .await
//         .map_err(|e| e.to_string())?;
//     peer_conn
//         .set_local_description(offer)
//         .await
//         .map_err(|e| e.to_string())?;
//     if let Err(e) = outgoing
//         .send(WsExchangeMsg::Sdp {
//             send_to_id: peer_id,
//             answering_peer_id: *SELF_UUID.get().unwrap(),
//             sdp: peer_conn.local_description().await.unwrap(),
//         })
//         .await
//     {
//         log::error!("Cannot send SDP to {peer_id}: {e}");
//         return Err(e.to_string());
//     };
//     log::debug!("Sent SDP to {peer_id}");
//
//     match OTHER_PEERS.insert(
//         peer_id,
//         (
//             Arc::new(peer_conn),
//             PeerSetupStage::WaitingAnswer,
//             start_stream_tx,
//         ),
//     ) {
//         Some(_) => {
//             log::warn!(
//                 "Peer ID {peer_id} already exists, overwriting and waiting for answer from peer...",
//             );
//         }
//         None => {
//             log::info!("Created PeerConnection for {peer_id}, waiting for answer from peer...",);
//         }
//     }
//
//     Ok(())
// }
//
// /// When this function is called, the PeerConnection of `peer_id` should have been half-set-up.
// /// The PeerConnection to that peer will be fully set up (assuming no error occurs) according to the
// /// `PeerSetupStage` it's in upon calling this function.
// /// # Parameters
// /// - `peer_id`: UUID of the peer.
// /// - `sdp`: The SDP received from the specified peer.
// /// - `outgoing`: a Sender to send messages to the signaling server. Think ICE candidate that the
// /// other peer should know about.
// ///
// /// UPDATE: this function also configures a video track to send to the other end.
// async fn finish_configure_peer_connection(
//     peer_id: Uuid,
//     sdp: RTCSessionDescription,
//     outgoing: mpsc::Sender<WsExchangeMsg>,
// ) -> Result<(), String> {
//     let (peer_conn, setup_stage) = match OTHER_PEERS.get(&peer_id) {
//         None => {
//             // TODO: we should return something other than a String.
//             // But, this is probably an error we can't really handle anyways.
//             return Err("Peer {peer_id} doesn't exist!".into());
//         }
//         Some(kv) => (kv.value().0.clone(), kv.value().1),
//     };
//     log::trace!("Checking setup stage...");
//     match setup_stage {
//         PeerSetupStage::Done => return Err("Peer {peer_id} is already set up!".into()),
//         PeerSetupStage::WaitingAnswer => {
//             if let Err(e) = peer_conn.set_remote_description(sdp).await {
//                 log::error!("PeerConnection with {peer_id}: {e}");
//                 return Err(e.to_string());
//             }
//             log::info!("Set up PeerConnection with {peer_id}");
//         }
//         PeerSetupStage::WaitingOffer => {
//             if let Err(e) = peer_conn.set_remote_description(sdp).await {
//                 log::error!("PeerConnection with {peer_id}: {e}");
//                 return Err(e.to_string());
//             }
//             let answer = peer_conn
//                 .create_answer(None)
//                 .await
//                 .map_err(|e| e.to_string())?;
//             peer_conn.set_local_description(answer).await;
//             log::info!("Need to send local description to {peer_id}");
//             if let Err(e) = outgoing
//                 .send(WsExchangeMsg::Sdp {
//                     send_to_id: peer_id,
//                     answering_peer_id: *SELF_UUID.get().unwrap(),
//                     sdp: peer_conn.local_description().await.unwrap(),
//                 })
//                 .await
//             {
//                 return Err(e.to_string());
//             }
//         }
//     }
//     OTHER_PEERS
//         .get_mut(&peer_id)
//         .map(|mut kv| {kv.value_mut().1 = PeerSetupStage::Done; kv.value_mut().2.try_send(());});
//
//     Ok(())
// }
//
// /// We have the video file, we have the track. We stream.
// async fn stream_video(
//     video_track: Arc<TrackLocalStaticSample>,
//     payload_type: PayloadType,
// ) -> Result<(), String> {
//     let file = File::open(VIDEO_FILE_NAME.get().unwrap()).map_err(|e| e.to_string())?;
//     let reader = BufReader::new(file);
//     // really, you must've had SSRC here already
//     let ssrc = *video_track.ssrcs().await.first().unwrap();
//     // the number is 2^20, the bool means it's not H265
//     let mut video_reader = H26xSampleReader::new(reader, 1_048_576, false);
//     // we only need 30fps, so don't go overboard.
//     let mut tick = tokio::time::interval(H26X_FRAME_DURATION);
//     // let mut instant = Instant::now();
//     // let mut timestamp: u32 = rand::random();
//     loop {
//         let sample = match video_reader.next_sample() {
//             Ok(sample) => sample,
//             Err(e) => {
//                 log::info!("All video frames parsed and sent: {e}");
//                 break;
//             }
//         };
//
//         if let Err(e) = video_track
//             .sample_writer(ssrc, payload_type)
//             .write_sample(&Sample {
//                 data: sample.data,
//                 duration: if sample.timed {
//                     H26X_FRAME_DURATION
//                 } else {
//                     Duration::ZERO
//                 },
//                 ..Default::default()
//             })
//             .await
//         {
//             log::warn!("Error sending: {e}");
//         };
//         if sample.timed {
//             tick.tick().await;
//         }
//     }
//
//     Ok(())
// }
//
// /// For each PeerConnection in `OTHER_PEERS`, call `close` on them.
// async fn close_peer_connections() {
//     for mut kv in OTHER_PEERS.iter_mut() {
//         if let Err(e) = kv.value_mut().0.close().await {
//             log::error!(
//                 "Error trying to close connection to {} manually: {e}",
//                 kv.key()
//             );
//         }
//     }
// }
//
// /// We got stuff to send, we send to each of them.
// /// And if one leaves, we remove that one's peer connection.
// static OTHER_PEERS: LazyLock<
//     DashMap<Uuid, (Arc<dyn PeerConnection>, PeerSetupStage, mpsc::Sender<()>)>,
// > = LazyLock::new(DashMap::new);
// /// THe configuration shared by all peers.
// static PEER_CONF: LazyLock<RTCConfiguration> = LazyLock::new(|| {
//     RTCConfigurationBuilder::new()
//         .with_ice_servers(vec![RTCIceServer {
//             // the STUN server we control.
//             urls: vec!["stun:127.0.0.1:3478".to_string()],
//             ..Default::default()
//         }])
//         .build()
// });
// /// <C-c> signal.
// static CTRLC_BROADCAST: LazyLock<broadcast::Sender<()>> = LazyLock::new(|| {
//     let (ctrlc_tx, _) = broadcast::channel::<()>(1);
//     let ctrlc_tx_ret = ctrlc_tx.clone();
//     ctrlc::set_handler(move || {
//         let _ = ctrlc_tx.send(());
//     })
//     .unwrap();
//     ctrlc_tx_ret
// });
// /// ~30fps
// static H26X_FRAME_DURATION: Duration = Duration::from_millis(33);
// static VIDEO_CODEC: LazyLock<RTCRtpCodecParameters> = LazyLock::new(|| RTCRtpCodecParameters {
//     rtp_codec: RTCRtpCodec {
//         mime_type: MIME_TYPE_H264.to_owned(),
//         clock_rate: 90_000,
//         channels: 0,
//         // what does this mean? I dunno.
//         sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
//             .to_owned(),
//         // sdp_fmtp_line: "".to_string(),
//         rtcp_feedback: vec![],
//     },
//     // h264 or something...
//     payload_type: 102,
// });
// /// I love global states
// static VIDEO_FILE_NAME: OnceLock<String> = OnceLock::new();
//
