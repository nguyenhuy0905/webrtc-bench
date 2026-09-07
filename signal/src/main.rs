//! Signaling server.
#![allow(unused)]
use clap::Parser;
use dashmap::DashMap;
use futures_util::{
    future,
    stream::{StreamExt, TryStreamExt},
    SinkExt,
};
use std::{net::SocketAddr, sync::LazyLock, pin::Pin};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::{error::Error as TungsteniteError, protocol::Message};

// Some terms I use a little loosely here:
// - "channel" is a broadcast. Peers subscribe to the broadcast. They can recv
// notifications (in the form of SDP offers) when they first join the channel,
// or when a new peer join.

// TODO: I dunno if this code can handle timeout...

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    let args = Opts::parse();

    let listener = match TcpListener::bind(args.host).await {
        Ok(lis) => lis,
        Err(e) => return Err(format!("{e}")),
    };
    log::info!(
        "Listening on {}",
        match listener.local_addr() {
            Ok(addr) => Ok(addr),
            Err(e) => Err(format!("{e}")),
        }?
    );

    while let Ok((stream, addr)) = listener.accept().await {
        tokio::spawn(async move {
            match handle_connection(stream, addr).await {
                Ok(()) => {}
                Err(e) => match e {
                    TungsteniteError::ConnectionClosed => {
                        log::info!("Connection {addr} closed");
                        PEERS.remove(&addr);
                    }
                    TungsteniteError::AttackAttempt => {
                        log::warn!("Attack attempt detected! Nuking the suspecting peer at once");
                        PEERS.remove(&addr);
                    }
                    _ => {
                        log::warn!("Misc. error: {e}");
                        PEERS.remove(&addr);
                    }
                },
            }
        });
    }

    Ok(())
}

/// When this runs, it's probably a new peer's connecting...
async fn handle_connection(
    raw_stream: TcpStream,
    addr: SocketAddr,
) -> Result<(), TungsteniteError> {
    log::info!("Incoming TCP connection from {addr}");

    let ws_stream = tokio_tungstenite::accept_async(raw_stream).await?;
    log::info!("WebSocket connection established for {addr}");

    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
    PEERS.insert(addr, tx);
    let addr = Pin::new(&addr);
    let (mut outgoing, incoming) = ws_stream.split();
    let broadcast_incoming = incoming.try_for_each_concurrent(4, |msg| {
        log::info!("Recv msg from {addr}: {}", msg.to_text().unwrap());
        for recp in PEERS
            .iter()
            .filter(|kv| kv.key() != &*addr)
            .map(|kv| kv.value().clone())
        {
            match recp.send(msg.clone()) {
                Ok(()) => {},
                Err(e) => {
                    log::debug!("recp.send ignore: {e}");
                }
            }
        }
        future::ok(())
    });
    tokio::pin!(broadcast_incoming);

    let recv_from_others = async move {
        while let Some(msg) = rx.recv().await {
            outgoing.send(msg).await;
        }
    };
    tokio::pin!(recv_from_others);

    tokio::select! {
        _ = broadcast_incoming => {
            log::info!("Broadcast incoming for {} done", *addr);
        }
        _ = recv_from_others => {
            log::info!("Receiving for others for {} done", *addr);
        }
    };

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Opts {
    /// Address to bind (default 127.0.0.1:6969)
    #[arg(short='a', long, default_value_t="127.0.0.1:6969".into())]
    host: String,
}

static PEERS: LazyLock<DashMap<SocketAddr, mpsc::UnboundedSender<Message>>> = LazyLock::new(DashMap::new);
