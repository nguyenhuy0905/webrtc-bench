//! Common types and methods that `peer` and `signal` both use.
#![allow(unused)]
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use webrtc::peer_connection::{RTCIceCandidateInit, RTCSessionDescription};

/// Message(s) to be exchanged via WebSocket between the signaling server and any peer.
/// We assume 1 channel only, and each peer only has 1 SDP offer. Hopefully adding more than
/// 1 channel isn't going to be difficult.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum WsExchangeMsg {
    // Join isn't needed, creating the WebSocket channel is inferred as a join request.
    /// Response to a join request, from the signaling server, with the peer ID.
    JoinPeerId(Uuid),
    /// Will be sent `on_negotiation_needed`.
    Offer {
        from_id: Uuid,
        to_id: Uuid,
        offer: RTCSessionDescription,
    },
    /// An answer to an offer.
    Answer {
        from_id: Uuid,
        to_id: Uuid,
        answer: RTCSessionDescription,
    },
    /// When a peer first joins, other peers are broadcasted the new peer's UUID. Those peers will
    /// ping the signaling server their offers, and the signaling server forwards that to the new
    /// peer.
    NewPeer(Uuid),
    /// A peer just left
    LeavePeerId(Uuid),
    /// ICE candidate exchange.
    IceCandidate {
        from_id: Uuid,
        to_id: Uuid,
        candidate: RTCIceCandidateInit,
    },
}
