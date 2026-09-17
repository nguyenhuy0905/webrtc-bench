#[allow(unused)]
use crate::globals::{OTHER_PEERS, SELF_UUID};
use common::WsExchangeMsg;
use rtc::{
    media::io::Writer,
    // peer_connection::configuration::media_engine::MIME_TYPE_H264,
    rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
    rtp_transceiver::rtp_sender::RtpCodecKind,
    statistics::{
        stats::rtp_stream::received::{
            inbound::RTCInboundRtpStreamStats, RTCReceivedRtpStreamStats,
        },
        StatsSelector,
    },
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use uuid::Uuid;
use webrtc::{
    media_stream::track_remote::{TrackRemote, TrackRemoteEvent},
    peer_connection::{
        PeerConnectionEventHandler, RTCPeerConnectionIceEvent, RTCPeerConnectionState,
    },
};

/// Handler to use for a PeerConnection.
pub struct WebRtcHandler {
    pub(crate) other_peer_id: Uuid,
    /// There are some signaling messages we want delivered to the other peer, e.g. when this peer
    /// gets a new ICE candidate.
    pub(crate) ws_out_tx: mpsc::Sender<WsExchangeMsg>,
    // pub(crate) video_writer: Mutex<H26xWriter<File>>
}

impl WebRtcHandler {
    /// Was kept as an artifact...
    pub fn new(other_peer_id: Uuid, ws_out_tx: mpsc::Sender<WsExchangeMsg>) -> Self {
        Self {
            other_peer_id,
            ws_out_tx,
        }
    }
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for WebRtcHandler {
    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        // basically send the ICE candidate to the other peer via the signaling server.
        match event.candidate.to_json() {
            Ok(candidate_init) => {
                if let Err(e) = self
                    .ws_out_tx
                    .send(WsExchangeMsg::IceCandidate {
                        from_id: *SELF_UUID.get().expect("PEER_UUID should have been set!"),
                        to_id: self.other_peer_id,
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

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let kind = track.kind().await;
        let media_ssrc = *track
            .ssrcs()
            .await
            .first()
            .expect("track should expose SSRCs before on_track");
        log::info!("On track with {}", self.other_peer_id);

        // let mime_type = track
        //     .codec(media_ssrc)
        //     .await
        //     .map(|c| c.mime_type.to_lowercase())
        //     .unwrap_or(MIME_TYPE_H264.to_lowercase());

        // Send PLI every 3 seconds for video tracks to request keyframes
        if kind == RtpCodecKind::Video {
            let pli_track = track.clone();
            tokio::spawn(Box::pin(async move {
                let mut result = webrtc::error::Result::<()>::Ok(());
                while result.is_ok() {
                    let timeout = tokio::time::sleep(Duration::from_secs(3));
                    tokio::select! {
                        _ = timeout => {
                            result = pli_track
                                .write_rtcp(vec![Box::new(PictureLossIndication {
                                    sender_ssrc: 0,
                                    media_ssrc,
                                })])
                                .await;
                        }
                    }
                }
            }));
        }

        // SAFETY: this peer is still on?
        let peer_conn = OTHER_PEERS
            .get(&self.other_peer_id)
            .unwrap()
            .value()
            .conn
            .clone();
        // saving track to disk
        tokio::spawn(async move {
            while let Some(evt) = track.poll().await {
                if let TrackRemoteEvent::OnRtpPacket(packet) = evt {
                    let mut w = crate::globals::VIDEO_SAVE_FILE.get().unwrap().lock().await;
                    if let Err(err) = w.write_rtp(&packet) {
                        println!("video write_rtp error: {err}");
                        break;
                    }
                }
            }
        });
        // stat-logging every now and then
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let report = peer_conn
                    .get_stats(Instant::now(), StatsSelector::None)
                    .await;
                if report.is_empty() {
                    continue;
                }

                for RTCInboundRtpStreamStats {
                    received_rtp_stream_stats:
                        RTCReceivedRtpStreamStats {
                            packets_received,
                            jitter,
                            ..
                        },
                    // frames_received,
                    ..
                } in report.inbound_rtp_streams()
                {
                    log::info!("Periodic stats:");
                    log::info!("\tPackets received: {packets_received}");
                    log::info!("\tJitter: {jitter:.3}");
                }
            }
        });
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state != RTCPeerConnectionState::Connected {
            return;
        }

        if let Some(kv) = OTHER_PEERS.get(&self.other_peer_id) {
            if let Err(e) = kv.value().start_stream_tx.send(()).await {
                log::warn!(
                    "Cannot send start stream signal for connection with {}: {e}",
                    self.other_peer_id
                );
            }
            // connected. TODO: Start logging stats
        }
    }
}
