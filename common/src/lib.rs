//! Common types and methods that `peer` and `signal` both use.
#![allow(unused)]
use serde::{Serialize, Deserialize};
use webrtc::peer_connection::RTCSessionDescription;

/// What the client should give when requesting, in JSON form.
#[derive(Serialize, Deserialize)]
pub struct ChannelOpenReq {
    /// Just an identifier, can be any thing really.
    pub name: String,
    /// Generated from PeerConnection's `create_offer`.
    pub offer: RTCSessionDescription,
}

