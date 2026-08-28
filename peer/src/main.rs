#![allow(unused)]

// Stuff isn't working yet. There should be a NOTE down there.

use base64::{
    engine::{general_purpose::GeneralPurposeConfig, simd::Simd as B64Simd},
    Engine,
};
use std::{
    io::{self, Write},
    pin::Pin,
    sync::{Arc, LazyLock},
};
use webrtc::{
    peer_connection::{
        register_default_interceptors, MediaEngine, PeerConnection, PeerConnectionBuilder,
        PeerConnectionEventHandler, RTCConfiguration, RTCConfigurationBuilder,
        RTCIceGatheringState, RTCIceServer, RTCPeerConnectionIceEvent, RTCPeerConnectionState,
        RTCSessionDescription, Registry,
    },
    runtime::{channel, Sender, TokioRuntime},
};

static RUNTIME: LazyLock<Arc<TokioRuntime>> = LazyLock::new(|| Arc::new(TokioRuntime));

#[derive(Clone)]
struct ConnHandler {
    // notify when ICE gathering is done.
    ice_gather_done_channel: Sender<()>,
    done_channel: Sender<()>,
}

impl PeerConnectionEventHandler for ConnHandler {
    fn on_ice_candidate<'a, 'async_trait>(
        &'a self,
        event: RTCPeerConnectionIceEvent,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
    where
        Self: Sync + 'async_trait,
        'a: 'async_trait,
    {
        Box::pin(async move {
            log::trace!("New ICE candidate: {:?}", event.candidate);
            // we'll only try 1 ICE agent...
            let _ = self.ice_gather_done_channel.try_send(());
        })
        // todo!()
    }

    fn on_connection_state_change<'a, 'async_trait>(
        &'a self,
        state: RTCPeerConnectionState,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
    where
        'a: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            log::info!("Peer connection state changed: {state}");
            if state == RTCPeerConnectionState::Failed {
                log::error!("peer connection failed...");
                let _ = self.done_channel.try_send(());
            }
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    log::trace!("Hello, world");

    let cfg: RTCConfiguration = RTCConfigurationBuilder::default()
        .with_ice_servers(vec![RTCIceServer {
            urls: vec!["turn:127.0.0.1:6969?transport=udp".to_owned()],
            username: "lenin".into(),
            credential: "lenin420".into(),
        }])
        .build();

    let (ice_gather_tx, mut ice_gather_rx) = channel::<()>(1);
    let (done_tx, mut done_rx) = channel::<()>(1);
    let handler = Arc::new(ConnHandler {
        ice_gather_done_channel: ice_gather_tx,
        done_channel: done_tx,
    });

    let registry = Registry::new();
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;
    let registry = register_default_interceptors(registry, &mut media_engine)?;

    let pc = PeerConnectionBuilder::new()
        .with_configuration(cfg)
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .with_runtime(RUNTIME.clone())
        .with_handler(handler)
        .with_udp_addrs(vec!["0.0.0.0:0"])
        .build()
        .await?;

    let mut sdp = String::new();
    io::stdout().flush();
    let n = io::stdin().read_line(&mut sdp)?;
    if sdp.is_empty() {
        log::error!("Empty SDP string");
        return Err("empty SDP string".into());
    }
    let b64_engine = B64Simd::standard(GeneralPurposeConfig::new());
    let sdp = serde_json::from_str::<RTCSessionDescription>(str::from_utf8(
        &b64_engine.decode(sdp.trim().as_bytes())?,
    )?)?;

    pc.set_remote_description(sdp).await?;
    let answer = pc.create_offer(None).await?;
    // NOTE: currently err out here
    pc.set_local_description(answer).await?;

    // wait for ICE gathering to complete...
    let _ = ice_gather_rx.recv().await;

    if let Some(local_desc) = pc.local_description().await {
        log::trace!("local description: {local_desc}");
        let json_data = serde_json::to_string(&local_desc)?;
        let local_b64 = b64_engine.encode(&json_data);
        log::trace!("local description in base64: {local_b64}");
        println!("Paste to browser: {local_b64}");
    }

    log::trace!("Local description set successfully");

    done_rx.recv().await;

    pc.close().await?;

    Ok(())
}
