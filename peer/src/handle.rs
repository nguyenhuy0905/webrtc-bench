#[allow(unused)]
use crate::globals::{OTHER_PEERS, SELF_UUID};
use common::WsExchangeMsg;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;
use webrtc::{peer_connection::{PeerConnectionEventHandler, RTCPeerConnectionIceEvent}, media_stream::track_remote::TrackRemote};

/// Handler to use for a PeerConnection.
pub struct WebRtcHandler {
    pub(crate) other_peer_id: Uuid,
    /// There are some signaling messages we want delivered to the other peer, e.g. when this peer
    /// gets a new ICE candidate.
    pub(crate) ws_out_tx: mpsc::Sender<WsExchangeMsg>,
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
                        from_id: self.other_peer_id,
                        to_id: *SELF_UUID.get().expect("PEER_UUID should have been set!"),
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

    // async fn on_signaling_state_change(&self, state: RTCSignalingState) {
    //     if let Some(kv) = OTHER_PEERS.get(&self.other_peer_id) {
    //         *kv.value().signal_state.write().await = state;
    //     }
    // }

    async fn on_track(&self, _track: Arc<dyn TrackRemote>) {
        log::info!("On track with {}", self.other_peer_id);
    }

    // async fn on_negotiation_needed(&self) {
    //     log::info!(
    //         "Negotiation with {} needed. This peer is {}",
    //         self.other_peer_id,
    //         if self.is_polite() {
    //             "polite"
    //         } else {
    //             "impolite"
    //         }
    //     );
    //     let Some(peer_conn) = OTHER_PEERS
    //         .get(&self.other_peer_id)
    //         .map(|kv| kv.value().conn.clone())
    //     else {
    //         log::warn!("{} doesn't exist anymore...", self.other_peer_id);
    //         return;
    //     };
    //     // SAFETY: at this point, we know the peer exists
    //     let offer = match peer_conn.create_offer(None).await {
    //         Ok(offer) => offer,
    //         Err(e) => {
    //             log::warn!("Cannot create offer for {}: {e}", self.other_peer_id);
    //             return;
    //         }
    //     };
    //     if let Err(e) = peer_conn.set_local_description(offer.clone()).await {
    //         log::warn!(
    //             "Cannnot set local description for connection to {}: {e}",
    //             self.other_peer_id
    //         );
    //         return;
    //     }
    //     if let Err(e) = self
    //         .ws_out_tx
    //         .send(WsExchangeMsg::Sdp {
    //             from_id: *SELF_UUID.get().unwrap(),
    //             to_id: self.other_peer_id,
    //             sdp: offer,
    //         })
    //         .await
    //     {
    //         log::warn!("Cannot send offer to {}: {e}", self.other_peer_id);
    //         return;
    //     }
    //
    //     log::info!("Sent offer to {}", self.other_peer_id);
    // }
}
