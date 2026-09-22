#[allow(unused)]
use crate::globals::{OTHER_PEERS, SELF_UUID, VIDEO_SSRC};
use common::WsExchangeMsg;
use rtc::{
    media::io::{Writer as _, h26x_writer::H26xWriter, ogg_writer::OggWriter},
    // peer_connection::configuration::media_engine::MIME_TYPE_H264,
    rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication,
    rtp_transceiver::rtp_sender::RtpCodecKind,
    // statistics::{
    //     stats::rtp_stream::{
    //         received::{inbound::RTCInboundRtpStreamStats, RTCReceivedRtpStreamStats},
    //         sent::remote_outbound::RTCRemoteOutboundRtpStreamStats,
    //     },
    //     StatsSelector,
    // },
};
use std::{
    fs::{File, OpenOptions},
    io::BufWriter,
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use uuid::Uuid;
use webrtc::{
    media_stream::track_remote::{TrackRemote, TrackRemoteEvent},
    peer_connection::{
        PeerConnectionEventHandler,
        RTCPeerConnectionIceEvent,
        RTCPeerConnectionState,
        // RTCStatsReportEntry,
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
        log::info!(
            "On track with {}, SSRC {}, {:?}",
            self.other_peer_id,
            media_ssrc,
            kind
        );

        if kind == RtpCodecKind::Video {
            let mut video_save: H26xWriter<BufWriter<File>> = H26xWriter::new(
                BufWriter::new(
                    OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(format!(
                            "save-video-{}-{}.h264",
                            *SELF_UUID.get().unwrap(),
                            self.other_peer_id
                        ))
                        .expect("cannot open video file to save"),
                ),
                false,
            );
            log::info!("Saving video");
            let pli_track = track.clone();
            // Send PLI every 3 seconds for video tracks to request keyframes
            tokio::spawn(Box::pin(async move {
                let mut result = webrtc::error::Result::<()>::Ok(());
                while result.is_ok() {
                    let timeout = tokio::time::sleep(Duration::from_secs(3));
                    tokio::select! {
                        _ = timeout => {
                            result = pli_track
                                .write_rtcp(vec![Box::new(PictureLossIndication {
                                    sender_ssrc: *VIDEO_SSRC,
                                    media_ssrc,
                                })])
                                .await;
                            // log::info!("Sent PLI");
                        }
                    }
                }
            }));

            tokio::spawn(async move {
                while let Some(evt) = track.poll().await {
                    if let TrackRemoteEvent::OnRtpPacket(packet) = evt
                        && let Err(err) = video_save.write_rtp(&packet)
                    {
                        println!("video write_rtp error: {err}");
                        break;
                    }
                }
            });
        } else {
            // audio
            let mut audio_save: OggWriter<BufWriter<File>> = OggWriter::new(
                BufWriter::new(
                    OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(format!(
                            "save-audio-{}-{}.ogg",
                            *SELF_UUID.get().unwrap(),
                            self.other_peer_id
                        ))
                        .expect("cannot open audio file to save"),
                ),
                48_000,
                2,
            )
            .expect("cannot open OPUS writer");
            log::info!("Saving audio");
            tokio::spawn(async move {
                while let Some(evt) = track.poll().await {
                    if let TrackRemoteEvent::OnRtpPacket(packet) = evt
                        && let Err(err) = audio_save.write_rtp(&packet)
                    {
                        println!("audio write_rtp error: {err}");
                        break;
                    }
                }
            });
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if state != RTCPeerConnectionState::Connected {
            return;
        }

        if let Some(kv) = OTHER_PEERS.get(&self.other_peer_id)
            && let Err(e) = kv.value().start_stream_tx.send(())
        {
            log::warn!(
                "Cannot send start stream signal for connection with {}: {e}",
                self.other_peer_id
            );
            // connected. TODO: Start logging stats
        }
    }
}
