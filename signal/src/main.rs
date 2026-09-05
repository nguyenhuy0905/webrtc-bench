#![allow(unused)]
use bytes::Bytes;
use clap::Parser;
use dashmap::{DashMap, DashSet};
use http_body_util::Full;
use hyper::{Error, Method, Request, Response, StatusCode, body, server::conn::http1};
use std::{convert::Infallible, net::SocketAddr, sync::LazyLock};
use tokio::{net::TcpListener, sync::broadcast, task};
use webrtc::peer_connection::RTCSessionDescription;

// Some terms I use a little loosely here:
// - "channel" is a broadcast. Peers subscribe to the broadcast. They can recv
// notifications (in the form of SDP offers) when they first join the channel,
// or when a new peer join.

// TODO for this file in particular:
// - [ ] For POST /channel, peer sends their SDP offer, and server will:
//  1. Check if a channel with the same name already exists; if yes, return a
//  FORBIDDEN (and probably some JSON saying the channel already exists, but we
//  probably don't need that for now). If no, return a CREATED, create a new
//  channel under that name, add the SDP offer into the channel data's offers,
//  subscribe the peer to the broadcast channel (in some way, probably by
//  sending the peer an offer for a WebRTC DataChannel).
//  - [ ] For GET /channel, if channel doesn't exist, send an empty OK back.
//  Else send an OK with the JSON array representing *all* the SDP offers
//  currently stored.
//  - [ ] Anything else? Not sure...

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Opts {
    /// address to bind to (default 0.0.0.0)
    #[arg(short='a', long, default_value_t="0.0.0.0".into())]
    addr: String,
    #[arg(short = 'p', long, default_value_t = 0)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    let args = Opts::parse();
    let addr: SocketAddr = match format!("{}:{}", args.addr, args.port).parse() {
        Ok(addr) => addr,
        Err(e) => {
            return Err(format!("Cannot bind to {}:{}: {e}", args.addr, args.port));
        }
    };
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            return Err(format!("Cannot bind to {addr}: {e}"));
        }
    };
    match listener.local_addr() {
        Ok(local_addr) => log::info!("Listening on http://{}", local_addr),
        Err(e) => {
            return Err(format!("Error getting server address: {e}"));
        }
    }

    loop {
        let (stream, _) = match listener.accept().await {
            Ok((stream, client_addr)) => {
                log::info!("New client: {client_addr}");
                (stream, client_addr)
            }
            Err(e) => {
                log::error!("Client accept failed: {e}");
                continue;
            }
        };

        task::spawn(async move {});
    }

    Ok(())
}

/// All the info needed to help peers establish PeerConnections, hopefully.
struct ChannelData {
    /// A map of (channel-name, channel-offers).
    /// Each offer is of a peer in the channel that
    offers: DashSet<RTCSessionDescription>,
    /// In case a new peer joins the channel, this broadcast is how they know
    /// about a new peer joining.
    channel: broadcast::Sender<RTCSessionDescription>,
}

static CHANNELS: LazyLock<DashMap<String, ChannelData>> = LazyLock::new(|| Default::default());

/// Responses:
/// - GET "/channel" -> JSON representing the offer, or something like "nah
/// channel doesn't exist".
/// - POST "/channel" (with JSON representing the offer) -> OK (aka, channel
/// has been created), or ERR (channel already exists).
/// - TODO: add more...
/// Also, our server is infallible.
async fn handle_request(req: Request<body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/channel") => todo!("GET /channel"),
        (&Method::POST, "/channel") => todo!("POST /channel"),
        _ => todo!("Handle the case of \"this path doesnt' exist\""),
    }
}
