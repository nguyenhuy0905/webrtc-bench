//! I love global variables.

use dashmap::DashMap;
use rtc::{
    peer_connection::configuration::media_engine::MIME_TYPE_H264,
    rtp_transceiver::rtp_sender::{RTCRtpCodec, RTCRtpCodecParameters},
};
use std::{
    sync::{atomic::AtomicBool, Arc, LazyLock, OnceLock},
    time::Duration,
};
use tokio::sync::{broadcast, mpsc, RwLock};
use uuid::Uuid;
use webrtc::peer_connection::{
    PeerConnection, RTCConfiguration, RTCConfigurationBuilder, RTCIceServer, RTCSignalingState,
};

/// To be allocated by the signaling server.
pub static SELF_UUID: OnceLock<Uuid> = OnceLock::new();
/// It's, peer information. All the thing you'd ever need to manage a peer connection
pub struct PeerInfo {
    /// The WebRTC connection
    /// Of course, you shouldn't change stuff here unless you're of module peer::globals.
    pub conn: Arc<dyn PeerConnection>,
    #[allow(unused)]
    /// A sender to signify the track(s) related to this peer to start.
    /// Of course, you shouldn't change stuff here unless you're of module peer::globals.
    pub start_stream_tx: mpsc::Sender<()>,
    // the three booleans down here, is copy from mdn docs on "perfect negotiation".
    /// Whether the peer handler is making an offer
    /// Of course, you shouldn't change stuff here unless you're of module peer::handle.
    pub making_offer: AtomicBool,
    /// Whether the peer handler is *not* taking any offer
    /// You're probably changing this in peer::main.
    pub ignore_offer: AtomicBool,
    /// Toggled on while setting remote description, when remote description is an answer.
    pub set_remote_answer_pending: AtomicBool,
    /// Updated every time the event handler's `on_signaling_state_change` is triggered.
    pub signal_state: RwLock<RTCSignalingState>,
}

impl PeerInfo {
    pub fn new(
        conn: Arc<dyn PeerConnection>,
        start_stream_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            conn,
            start_stream_tx,
            making_offer: AtomicBool::from(false),
            ignore_offer: AtomicBool::from(false),
            set_remote_answer_pending: AtomicBool::from(false),
            signal_state: RwLock::new(RTCSignalingState::Stable),
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
/// THe configuration shared by all peers.
pub static PEER_CONF: LazyLock<RTCConfiguration> = LazyLock::new(|| {
    RTCConfigurationBuilder::new()
        .with_ice_servers(vec![RTCIceServer {
            // the STUN server we control.
            urls: vec!["stun:127.0.0.1:3478".to_string()],
            ..Default::default()
        }])
        .build()
});
/// ~30fps
pub static H26X_FRAME_DURATION: Duration = Duration::from_millis(33);
/// I love global states
pub static VIDEO_FILE_NAME: OnceLock<String> = OnceLock::new();
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
