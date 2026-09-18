// game plan here:
// 0. set up some sort of signaling server. Of course. [UPDATE: DONE]
// 1. find a way for the peers to ping the signaling server. [UPDATE: DONE]
// 1.1 by "ping" I mean send/recv SDPs. [UPDATE: DONE]
// 2. create a track from a local H.264 file.
//   - currently we are using a data channel to force the PeerConnection to go through.

// So, `matchbox` turned out to not be such a bright idea.
// I'll make my own WebSocket (building from `tokio-tungstenite`) then

// TODO: save the received video to a video file

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
    PeerInfo, CSV_FILE, CTRLC_BROADCAST, H26X_FRAME_DURATION, OTHER_PEERS, PEER_CONF, RUNTIME,
    SELF_UUID, VIDEO_CODEC, VIDEO_FILE_NAME, VIDEO_SSRC,
};
// use rand::distr::Distribution as _;
use rtc::{
    interceptor::{interceptor, Interceptor, Packet, StreamInfo, TaggedPacket},
    media::{
        io::{h26x_reader::sample_reader::H26xSampleReader, h26x_writer::H26xWriter},
        Sample,
    },
    rtcp::receiver_report::ReceiverReport,
    rtp_transceiver::{
        rtp_sender::{RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind},
        PayloadType,
    },
    sansio,
    shared::{error::Error, time::SystemInstant},
};
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Write as _},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};
use tokio_tungstenite::{
    tungstenite::{error::Error as TungsteniteError, protocol::Message},
    MaybeTlsStream, WebSocketStream,
};
use uuid::Uuid;
#[allow(unused)]
use webrtc::{
    media_stream::{
        track_local::{static_sample::TrackLocalStaticSample, TrackLocal as _, TrackLocalEvent},
        MediaStreamTrack, Track,
    },
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
    // /// Save video to file
    #[arg(short='s', long, default_value_t=format!("save-video-{}.h264", Uuid::new_v4()))]
    video_save_to_file: String,
}

fn main() -> anyhow::Result<()> {
    RUNTIME.block_on(main_async())
}

async fn main_async() -> anyhow::Result<()> {
    env_logger::init();
    let args = Opts::parse();
    // log::info!("Saving video to {}", args.video_save_to_file);

    // initialize some stuff
    globals::VIDEO_FILE_NAME.get_or_init(|| args.video_file);
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&args.video_save_to_file)
        .with_context(|| format!("Cannot open file {}", args.video_save_to_file))?;
    globals::VIDEO_SAVE_FILE
        .get_or_init(|| Mutex::new(H26xWriter::new(BufWriter::new(file), false)));

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
        let csv_file = Mutex::new(BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(format!("stats-{self_id}.csv"))
                .with_context(|| format!("Cannot open or create CSV file stat-{self_id}.csv"))?,
        ));
        csv_file
            .lock()
            .await
            .write(b"")
            .context("Cannot write CSV file header")?;
        CSV_FILE
            .set(csv_file)
            .expect("Somehow CSV_FILE is already set");
        log::info!("Saving stats to stat-{self_id}.csv");
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

        // NOTE: it's normal to see ICE handling error message from one peer, due to remote
        // connection not being added yet.
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
            let new_peer = Arc::new(
                create_empty_peer_conn(peer_id, outgoing.clone())
                    .await
                    .with_context(|| format!("Cannot add new peer {peer_id}"))?,
            );
            let start_stream_tx = add_media_to_connection(new_peer.clone())
                .await
                .with_context(|| format!("Cannot add media to {peer_id}"))?;
            OTHER_PEERS.insert(peer_id, PeerInfo::new(new_peer.clone(), start_stream_tx));

            log::info!("Peer {peer_id} added! TODO send offer to peer");
            let offer = new_peer
                .create_offer(None)
                .await
                .with_context(|| format!("Cannot create offer for {peer_id}"))?;
            new_peer
                .set_local_description(offer)
                .await
                .with_context(|| {
                    format!("Cannot set local description for peer connection with {peer_id}")
                })?;
            outgoing
                .send(WsExchangeMsg::Sdp {
                    // SAFETY: SELF_UUID is already initiated when connecting to signaling server.
                    from_id: *SELF_UUID.get().unwrap(),
                    to_id: peer_id,
                    sdp: new_peer.local_description().await.ok_or_else(|| {
                        anyhow::anyhow!(
                            "Cannot query local description for connection to {peer_id}"
                        )
                    })?,
                })
                .await
                .with_context(|| format!("Cannot send offer to {peer_id}"))?;
        }
        WsExchangeMsg::LeavePeerId(peer_id) => {
            log::warn!("TODO Peer {peer_id} leaving");
        }
        WsExchangeMsg::Sdp {
            from_id,
            to_id,
            sdp,
        } => {
            if to_id != *SELF_UUID.get().unwrap() {
                anyhow::bail!("Received SDP destined to {to_id}");
            }
            let sdp_type = sdp.sdp_type;
            match sdp_type {
                RTCSdpType::Offer => {
                    const CONTEXT: &'static str = "Receive and handle SDP offer";
                    log::warn!("TODO handle offer from {from_id}");
                    if OTHER_PEERS.get(&from_id).is_some() {
                        anyhow::bail!(
                            "SDP offer: we don't support re-negotiation yet! From {from_id}"
                        );
                    }
                    let other_peer = Arc::new(
                        create_empty_peer_conn(from_id, outgoing.clone())
                            .await
                            .with_context(|| {
                                format!("Cannot create empty peer connection for {from_id}")
                            })
                            .context(CONTEXT)?,
                    );
                    let start_stream_tx = add_media_to_connection(other_peer.clone())
                        .await
                        .with_context(|| format!("Cannot add media to connection with {from_id}"))
                        .context(CONTEXT)?;
                    other_peer
                        .set_remote_description(sdp)
                        .await
                        .with_context(|| format!("Cannot set remote description from {from_id}"))
                        .context(CONTEXT)?;
                    OTHER_PEERS.insert(from_id, PeerInfo::new(other_peer.clone(), start_stream_tx));
                    log::info!("Offering peer {from_id} added.");

                    // generate answer and send back
                    let answer = other_peer
                        .create_answer(None)
                        .await
                        .with_context(|| format!("Cannot create answer for {from_id}"))
                        .context(CONTEXT)?;
                    other_peer
                        .set_local_description(answer)
                        .await
                        .with_context(|| {
                            format!("Cannot set local description for connection to {from_id}")
                        })
                        .context(CONTEXT)?;
                    outgoing
                        .send(WsExchangeMsg::Sdp {
                            from_id: to_id,
                            to_id: from_id,
                            sdp: other_peer
                                .local_description()
                                .await
                                .ok_or_else(|| {
                                    anyhow::anyhow!(
                                    "Cannot query local description for connection to {from_id}"
                                )
                                })
                                .context(CONTEXT)?,
                        })
                        .await
                        .with_context(|| format!("Cannot send answer to {from_id}"))?;
                }
                RTCSdpType::Answer => {
                    const CONTEXT: &'static str = "Receive and handle answer";
                    log::warn!("TODO handle answer from {from_id}");
                    let other_peer = OTHER_PEERS
                        .get(&from_id)
                        .with_context(|| format!("Peer {from_id} doesn't exist (yet)"))
                        .context(CONTEXT)?;
                    other_peer
                        .value()
                        .conn
                        .set_remote_description(sdp)
                        .await
                        .context(CONTEXT)
                        .with_context(|| {
                            format!("Cannot set remote description for connection to {from_id}")
                        })?;
                }
                _ => {
                    log::warn!("SDP type {sdp_type:?} not supported");
                }
            }
        }
        WsExchangeMsg::IceCandidate {
            from_id,
            to_id,
            candidate,
        } => {
            // log::warn!("TODO handle ICE candidate from {from_id}");
            if to_id != *SELF_UUID.get().unwrap() {
                anyhow::bail!("Received ICE candidate destined to {to_id}");
            }
            let peer_info = OTHER_PEERS.get(&from_id).ok_or_else(|| {
                anyhow::anyhow!("ICE candidate: Peer {from_id} doesn't exist (yet)")
            })?;
            peer_info
                .conn
                .add_ice_candidate(candidate)
                .await
                .with_context(|| format!("Cannot add ICE candidate from {from_id}"))?;
        }
        _ => {
            anyhow::bail!("Unexpected message: {msg:?}");
        }
    }

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
    // let registry = configure_rtcp_reports(Registry::new());
    let registry = register_default_interceptors(Registry::new(), &mut media_engine)
        .context("Cannot register interceptor")?;
    let registry = registry.with(RTCPFwdInterceptor::new);

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
    // the bool means it's not H265
    let mut video_reader = H26xSampleReader::new(reader, 1024 * 1024, false);
    let mut tick = tokio::time::interval(H26X_FRAME_DURATION);

    // get the RTCP RR
    let vtr = video_track.clone();
    tokio::spawn(async move {
        let video_track = vtr;
        while let Some(TrackLocalEvent::OnRtcpPacket(packets)) = video_track.poll().await {
            for packet in packets {
                let now = (SystemInstant::now().ntp(Instant::now()) >> 16) as u32;
                let Some(rr) = packet.as_any().downcast_ref::<ReceiverReport>() else {
                    continue;
                };
                let Some(report) = rr.reports.first() else {
                    continue;
                };
                // no SR sent yet. Can't do much.
                if report.last_sender_report == 0 && report.delay == 0 {
                    continue;
                }
                // middle NTP of current instant.
                // formula in RFC3350, section 6.4.2
                let rtt = now - report.delay - report.last_sender_report;
                // RTT in milliseconds
                let rtt_float: f32 =
                    ((rtt >> 16) as f32 + ((rtt & 0x0000_FFFF) as f32) / 65_536f32) * 1_000f32;
                log::info!("RTT: {rtt_float:.3}ms");
            }
        }
    });

    // send da video
    loop {
        let sample = match video_reader.next_sample() {
            Ok(sample) => sample,
            Err(e) => {
                log::info!("All video frames parsed and sent: {e}");
                break;
            }
        };

        video_track
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
            .await?;
        if sample.timed {
            tick.tick().await;
        }
    }

    Ok(())
}

/// Returns, if success, the notification channel to start the video stream
async fn add_media_to_connection(
    peer_conn: Arc<dyn PeerConnection>,
) -> anyhow::Result<mpsc::Sender<()>> {
    let video_track = Arc::new(
        TrackLocalStaticSample::new(MediaStreamTrack::new(
            // TODO: these, as suggested by the specs, should be UUIDs.
            // stream ID
            format!("video-stream-{}", rand::random::<u32>()),
            // track ID
            format!("video-track-{}", rand::random::<u32>()),
            // label
            "Video track".to_owned(),
            RtpCodecKind::Video,
            vec![RTCRtpEncodingParameters {
                rtp_coding_parameters: RTCRtpCodingParameters {
                    ssrc: Some(*VIDEO_SSRC),
                    ..Default::default()
                },
                // why do I have to repeat myself here...
                codec: VIDEO_CODEC.rtp_codec.clone(),
                ..Default::default()
            }],
        ))
        .with_context(|| format!("Cannot create video track"))?,
    );

    // notify when to start a stream
    let (start_stream_tx, mut start_stream_rx) = mpsc::channel::<()>(1);

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

    Ok(start_stream_tx)
}

#[derive(Interceptor)]
struct RTCPFwdInterceptor<P: Interceptor> {
    #[next]
    next: P,
    read_queue: VecDeque<TaggedPacket>,
}

impl<P: Interceptor> RTCPFwdInterceptor<P> {
    fn new(next: P) -> Self {
        Self {
            next,
            read_queue: VecDeque::new(),
        }
    }
}

#[interceptor]
impl<P: Interceptor> RTCPFwdInterceptor<P> {
    #[overrides]
    fn handle_read(&mut self, msg: TaggedPacket) -> Result<(), Self::Error> {
        if let Packet::Rtcp(rtcp_packets) = &msg.message {
            self.read_queue.push_back(TaggedPacket {
                now: msg.now,
                transport: msg.transport,
                message: Packet::Rtcp(rtcp_packets.clone()),
            });
        }
        self.next.handle_read(msg)
    }

    #[overrides]
    fn poll_read(&mut self) -> Option<Self::Rout> {
        // First return any queued RTCP packets
        if let Some(pkt) = self.read_queue.pop_front() {
            return Some(pkt);
        }
        // Then check next interceptor
        self.next.poll_read()
    }

    #[overrides]
    fn close(&mut self) -> Result<(), Self::Error> {
        self.read_queue.clear();
        self.next.close()
    }
}
