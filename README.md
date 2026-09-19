# WebRTC benchmark
## Preface
- Really, [the webrtc.rs repo](https://github.com/webrtc-rs/webrtc) has most of
what we need as examples. So, I'll refer to the examples quite a bit in here.

### Useful examples
- [Stream video and audio from disk](https://github.com/webrtc-rs/webrtc/tree/master/examples/play-from-disk-h26x).
- [RTCP processing](https://github.com/webrtc-rs/webrtc/tree/master/examples/rtcp-processing).

## Architecture?
> [!NOTE]
> The implementation details on the Excalidraw is outdated. But the setup is still the same.

- [The Excalidraw of how I plan to set up](https://excalidraw.com/#json=UqrxBjltDgnpfdRbsdIhh,iWbbKnik0c_q24VdLUTs4g).
- TL;DW: full-mesh peer-to-peer, save for one node that's used as the signaling and STUN/TURN server.

## Requirements
- Linux (otherwise, replace `run0` and `ufw` (and `nftables`) with similar tools).
    - `run0` (or `sudo`, pick whichever you like more) (your Linux distribution needs `systemd` if you want `run0`).
    - `nftables`.
        - `ufw` is optional, if you know how to write `nftables` rules.
            - If you do choose `ufw`, make sure your Linux distribution uses `systemd`.
        - There's a compatibility package from `nftables` or `iptables`, if you prefer the latter.
    - [`coturn`](https://github.com/coturn/coturn).
    - `tmux` recommended, especially for the server as you will need to handle multiple running processes.
- Be on an architecture officially supported by `rustc` (e.g. x86_64, ARMv8).
- Network connection. At minimum, the peers can reach the signaling server, and vice versa.
- If the peers are behind different NAT layers, make sure none is behind a symmetric NAT. Otherwise, you'll need to relay traffic (a.k.a. can't use STUN. TURN only).
- The stable Rust toolchain. Latest stable as the time of writing is version 1.97. Nightly toolchain would work as well, but this is tested on stable.

> [!NOTE]
> Our `--release` build configuration is non-existent, and could be much improved.

## Setup for signaling server
> [!NOTE]
> We might set up an NTP server on the node we dedicate as the signal server as well. In which case, you'll need to open another port for NTP on the server, and point the NTP clients of the peers there.

- For a benchmark setup, we'll let the signal server also be the STUN/TURN server.

- The address of the STUN/TURN server is hardcoded. In [peer/src/globals.rs](peer/src/globals.rs),
find `PEER_CONF`, and change the `urls`, listed a few lines below.
- If you want the peers to save some video output, uncomment the code under the `TrackRemoteEvent::OnRtpPacket(_)` match arm, and replace the `_` in `OnRtpPacket(_)` with `packet`. And you might have to go `use` some extra symbols.
- You'll need to allow UDP in on 2 ports if you use the default setup here: ports 6969 and 3478.

```fish
# run0 does the same thing as sudo here, so if it doesn't work on your setup,
# just replace with sudo
run0 ufw enable
run0 ufw allow 3478/udp
run0 ufw allow 6969/udp
run0 ufw reload
```

> [!NOTE]
> `ufw` only seems to support some very simplistic rate-limiting. We might have to pull out the sledgehammer and use `nftables` directly.
> And `nftables` is quite a bit harder to use...

### STUN or TURN
- We use this thing called [coturn](https://github.com/coturn/coturn) for the TURN and STUN servers.
- For STUN, `turnserver -S -p 3478` works fine.
For TURN:
    - First, generate a key-pair, PEM format. Check `openssl-genpkey`. For the example command below, these are `turn_key` and `turn_key.pub`.
    - Then, use the private key to sign a X.509 certificate. Check `openssl-x509`. For the example command below, it's `turn_cert.pem`
    - Then, create a user database or use the default one of `coturn`. There should be some guide in their site...
        - The code currently expect users: `lenin`, password `lenin420`.
        - In the example below, the database file is named `test_user_db.sql`.

```fish
# generate database (your database schema might not be installed in the exact same place...)
sqlite3 test_user_db.sql < /usr/share/turnserver/schema.sql
# add user
turnadmin -b test_user_db.sql -a -u lenin -r soviet.russia -p lenin420
# run server
turnserver -b test_user_db.sql -a --cert turn_cert.pem --pkey turn_key -p 3478 -L 127.0.0.1 -r soviet.russia
```

## Setup for peers/clients
- You shouldn't need to open any ports.
- Prepare a video you want to run.
- If you use the TURN setup, search the code for `with_ice_server` in [`peer/src/globals.rs`](peer/src/globals.rs) and comment out the STUN configuration, and uncomment the TURN configuration.

> [!NOTE]
> To see the peer's logs, the environment variable `RUST_LOG` needs to be set. I recommend setting it to `peer=info`.

```fish
# run the peer
RUST_LOG=peer=info cargo run --bin peer
```

- Each peer writes its own CSV file. The CSV file has format: `PeerID,DelayMs`. The first column is the peer ID whose Receiver Report this peer receives, the second is the latency derived from the Report's data.
- The CSV file is named `stats-[UUID].csv`. `[UUID]` is replaced with the randomly-generated UUID of the peer.

> [!NOTE]
> The file writer is a `BufWriter`, so for a while after starting the video, you won't see any data written into the CSV file.

> [!WARN]
> When a peer finishes sending its video, it doesn't stop. To stop the peers, you'll have to send SIGTERM (a.k.a. Ctrl+C). It's necessary to kill the peer this way.

### Saving the stream
> [!WARN]
> With how I set it up, it'll only work when there are only 2 peers. So, it's currently commented out (in `peer/src/handle.rs`)
- As this is currently disabled, but the save files are still generated, you might find a bunch of empty, very long-named `.h264` files.
