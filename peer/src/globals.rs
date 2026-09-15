//! I love global variables.

use dashmap::DashMap;
use std::{sync::{Arc, LazyLock, OnceLock}, time::Duration};
use uuid::Uuid;
use webrtc::peer_connection::PeerConnection;
use rtc::{
    peer_connection::configuration::media_engine::MIME_TYPE_H264,
    rtp_transceiver::rtp_sender::{RTCRtpCodec, RTCRtpCodecParameters},
};
use webrtc::peer_connection::{RTCConfigurationBuilder, RTCConfiguration, RTCIceServer};
use tokio::sync::{mpsc, broadcast, RwLock};

/// We need this to see how we should set local and remote descriptions during
/// `finish_configure_peer_connection`.
#[derive(Clone, Copy)]
pub enum PeerSetupStage {
    WaitingOffer,
    WaitingAnswer,
    Done,
}

/// To be allocated by the signaling server.
pub static SELF_UUID: OnceLock<Uuid> = OnceLock::new();
/// (uuid, (peer-conn, setup-stage, notifier-to-start-video-stream))
pub static OTHER_PEERS: LazyLock<DashMap<Uuid, (Arc<dyn PeerConnection>, RwLock<PeerSetupStage>, mpsc::Sender<()>)>> =
    LazyLock::new(DashMap::new);
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
