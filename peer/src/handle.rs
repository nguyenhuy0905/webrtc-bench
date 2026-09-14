use std::sync::Arc;
use uuid::Uuid;
use tokio::sync::mpsc;
use common::WsExchangeMsg;
use webrtc::{peer_connection::{PeerConnectionEventHandler, RTCPeerConnectionIceEvent}, media_stream::track_remote::TrackRemote};
use crate::globals::SELF_UUID;

/// Handler to use for a PeerConnection.
pub struct WebRtcHandler {
    pub(crate) other_peer_id: Uuid,
    /// There are some signaling messages we want delivered to the other peer, e.g. when this peer
    /// gets a new ICE candidate.
    pub(crate) ws_out_tx: mpsc::Sender<WsExchangeMsg>,
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
                        from_id: self.other_peer_id,
                        to_id: *SELF_UUID
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

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        log::info!("On track with {}", self.other_peer_id);
    }

    async fn on_negotiation_needed(&self) {
        log::info!("Negotiation with {} needed", self.other_peer_id);
    }
}
