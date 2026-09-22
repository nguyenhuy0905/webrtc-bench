//! I love global variables.

use dashmap::DashMap;
use rtc::{
    peer_connection::configuration::media_engine::{MIME_TYPE_H264, MIME_TYPE_OPUS},
    rtp_transceiver::rtp_sender::{RTCRtpCodec, RTCRtpCodecParameters},
};
use std::{
    fs::File,
    io::BufWriter,
    sync::{Arc, LazyLock, OnceLock},
    time::Duration,
};
use tokio::sync::{Mutex, broadcast};
use uuid::Uuid;
use webrtc::peer_connection::{PeerConnection, RTCConfiguration};

/// To be allocated by the signaling server.
pub static SELF_UUID: OnceLock<Uuid> = OnceLock::new();
/// It's, peer information. All the thing you'd ever need to manage a peer connection
pub struct PeerInfo {
    /// The WebRTC connection
    /// Of course, you shouldn't change stuff here unless you're of module peer::globals.
    pub conn: Arc<dyn PeerConnection>,
    /// A sender to signify the track(s) related to this peer to start.
    /// Of course, you shouldn't change stuff here unless you're of module peer::globals.
    pub start_stream_tx: broadcast::Sender<()>,
}

impl PeerInfo {
    pub fn new(conn: Arc<dyn PeerConnection>, start_stream_tx: broadcast::Sender<()>) -> Self {
        Self {
            conn,
            start_stream_tx,
        }
    }
}

pub static OTHER_PEERS: LazyLock<DashMap<Uuid, PeerInfo>> = LazyLock::new(DashMap::new);
/// The tokio runtime
pub static RUNTIME: LazyLock<Arc<tokio::runtime::Runtime>> = LazyLock::new(|| {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .name("tokio-runtime")
            .build()
            .unwrap(),
    )
});
pub static VIDEO_CODEC: LazyLock<RTCRtpCodecParameters> = LazyLock::new(|| RTCRtpCodecParameters {
    rtp_codec: RTCRtpCodec {
        mime_type: MIME_TYPE_H264.to_owned(),
        clock_rate: 90_000,
        channels: 0,
        // what does this mean? I dunno.
        sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
            .to_owned(),
        // sdp_fmtp_line: "".to_string(),
        rtcp_feedback: vec![],
    },
    // h264 or something...
    payload_type: 102,
});
pub static AUDIO_CODEC: LazyLock<RTCRtpCodecParameters> = LazyLock::new(|| RTCRtpCodecParameters {
    rtp_codec: RTCRtpCodec {
        mime_type: MIME_TYPE_OPUS.to_owned(),
        clock_rate: 48_000,
        channels: 2,
        sdp_fmtp_line: "".to_owned(),
        rtcp_feedback: vec![],
    },
    payload_type: 120,
});
/// The configuration shared by all peers.
pub static PEER_CONF: OnceLock<RTCConfiguration> = OnceLock::new();
/// ~24fps
pub static H26X_FRAME_DURATION: Duration = Duration::from_millis(41);
pub static OGG_FRAME_DURATION: Duration = Duration::from_millis(20);
/// I love global states
pub static VIDEO_FILE_NAME: OnceLock<String> = OnceLock::new();
/// I love global states
pub static AUDIO_FILE_NAME: OnceLock<String> = OnceLock::new();
// NOTE we don't handle SSRC collision for now.
pub static VIDEO_SSRC: LazyLock<u32> = LazyLock::new(rand::random);
// NOTE we don't handle SSRC collision for now.
pub static AUDIO_SSRC: LazyLock<u32> = LazyLock::new(rand::random);
// // I really love global states
// pub static VIDEO_SAVE_FILE: OnceLock<Mutex<H26xWriter<BufWriter<File>>>> = OnceLock::new();
// // I really love global states
// pub static AUDIO_SAVE_FILE: OnceLock<Mutex<OggWriter<BufWriter<File>>>> = OnceLock::new();
// I really really love global states
/// CSV file:
/// peer-uuid,rtt
pub static CSV_VIDEO_FILE: OnceLock<Mutex<BufWriter<File>>> = OnceLock::new();
/// CSV file:
/// peer-uuid,rtt
pub static CSV_AUDIO_FILE: OnceLock<Mutex<BufWriter<File>>> = OnceLock::new();
/// <C-c> signal.
pub static CTRLC_BROADCAST: LazyLock<broadcast::Sender<()>> = LazyLock::new(|| {
    let (ctrlc_tx, _) = broadcast::channel::<()>(1);
    let ctrlc_tx_ret = ctrlc_tx.clone();
    ctrlc::set_handler(move || {
        let _ = ctrlc_tx.send(());
    })
    .unwrap();
    ctrlc_tx_ret
});
