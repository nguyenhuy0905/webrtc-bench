//! Common types and methods that `peer` and `signal` both use.
#![allow(unused)]
use serde::{Deserialize, Serialize};
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use uuid::Uuid;

/// Message(s) to be exchanged via WebSocket between the signaling server and any peer.
/// We assume 1 channel only, and each peer only has 1 SDP offer. Hopefully adding more than
/// 1 channel isn't going to be difficult.
#[derive(Clone, Serialize, Deserialize)]
pub enum WsExchangeMsg {
    /// Request from a peer to join. For now we assume 1 channel only.
    Join {
        offer: RTCSessionDescription,
    },
    /// Response to a join request, from the signaling server, with the peer ID.
    JoinPeerId(Uuid),
    /// When a peer first joins, other peers are broadcasted the new peer's UUID, and the new peer
    /// gets the other peers' UUIDs and offers as well.
    PeerSdp {
        peer_id: Uuid,
        offer: RTCSessionDescription,
    },
    /// Answer generated from a peer in response to an offer.
    Answer {
        answering_peer_id: Uuid,
        answer: RTCSessionDescription,
    }
}
