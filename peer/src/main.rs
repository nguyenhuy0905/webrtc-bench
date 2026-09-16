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
    sync::Arc,
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
    _outgoing: mpsc::Sender<WsExchangeMsg>,
) -> anyhow::Result<()> {
    match msg {
        WsExchangeMsg::NewPeer(peer_id) => {
            log::warn!("TODO: new peer {peer_id}");
        }
        WsExchangeMsg::LeavePeerId(peer_id) => {
            log::warn!("TODO Peer {peer_id} leaving");
        }
        WsExchangeMsg::Sdp {
            from_id,
            ..
        } => {
            log::warn!("TODO handle SDP from {from_id}");
        }
        WsExchangeMsg::IceCandidate {
            ..
        } => {
            log::warn!("TODO handle ICE candidate");
        }
        _ => {
            anyhow::bail!("Unexpected message: {msg:?}");
        }
    }

    Ok(())
}

#[allow(unused)]
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
