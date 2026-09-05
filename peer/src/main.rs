#![allow(unused)]

// game plan here:
// 0. set up some sort of signaling server. Of course.
// 1. find a way for the peers to ping the signaling server.
// 1.1 by "ping" I mean send/recv SDPs.

use std::{
    cell::Cell,
    sync::{Arc, OnceLock},
};
use tokio::{runtime::Runtime, sync::mpsc};
use webrtc::{
    data_channel::RTCDataChannelInit,
    peer_connection::{
        PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
        RTCIceGatheringState, RTCIceServer, RTCPeerConnection, RTCPeerConnectionState,
    },
    runtime::broadcast_channel,
};

static RUNTIME: OnceLock<Arc<Runtime>> = OnceLock::new();

fn runtime() -> &'static Arc<Runtime> {
    RUNTIME.get_or_init(|| Arc::new(Runtime::new().unwrap()))
}

struct BroadcastHandler {
    runtime: Arc<Runtime>,
    // to ping `main` that we're done gathering ICE agents.
    // I'd like to use a oneshot channel, but life won't allow me to share the
    // thing between threads, 'cuz oneshot kills itself.
    gather_complete_tx: mpsc::Sender<()>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for BroadcastHandler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if matches!(state, RTCIceGatheringState::Complete) {
            log::info!("Done gathering ICE");
            let _ = self.gather_complete_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        if matches!(state, RTCPeerConnectionState::Failed) {
            log::error!("Connection failed: {state}");
            return;
        }
        log::info!("Connection state changed to: {state}");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let local_config = RTCConfigurationBuilder::default()
        .with_ice_servers(vec![RTCIceServer {
            urls: vec!["stun:127.0.0.1:3478".into()],
            ..Default::default()
        }])
        .build();

    let (gather_complete_tx, gather_complete_rx) = mpsc::channel::<()>(1);

    let mut local_peer = PeerConnectionBuilder::new()
        .with_configuration(local_config)
        .with_handler(Arc::new(BroadcastHandler {
            runtime: runtime().clone(),
            gather_complete_tx,
        }))
        .with_udp_addrs(vec!["0.0.0.0:0"])
        .build()
        .await?;
    let channel = local_peer
        .create_data_channel(
            "onichan",
            Some({
                let mut ret = RTCDataChannelInit::default();
                ret.ordered = false;
                ret
            }),
        )
        .await?;
    log::trace!("Created data channel from local peer");

    let offer = local_peer.create_offer(None).await?;
    return Ok(());
}
