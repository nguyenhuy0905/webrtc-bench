# WebRTC benchmark
## Preface
- Really, [the webrtc.rs repo](https://github.com/webrtc-rs/webrtc) has most of
what we need as examples. So, I'll refer to the examples quite a bit in here.

### Useful examples
- [Stream video and audio from disk](https://github.com/webrtc-rs/webrtc/tree/master/examples/play-from-disk-h26x).
- [Getting the stats](https://github.com/webrtc-rs/webrtc/tree/master/examples/stats). This does include RTT.
- [Broadcasting](https://github.com/webrtc-rs/webrtc/tree/master/examples/broadcast). Do note this is a star topology.

## Architecture?
- [The Excalidraw of how I plan to set up](https://excalidraw.com/#json=UqrxBjltDgnpfdRbsdIhh,iWbbKnik0c_q24VdLUTs4g).
- I'm thinking of, all peers join one broadcast channel, and throw videos to one another.
- Then we get RTT.
- I wonder how we'd go about doing subjective quality tests though.

## Setup
- We use this thing called [coturn](https://github.com/coturn/coturn) for the TURN and STUN servers.
    - For TURN, create a key pair, then use the private key to sign the X.509 cert. Y'know, usual `openssl` commands. RTFM.
    - For STUN, `turnserver -S -p 3478`

### If you wanna do TURN
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

- If you use the TURN example above, this is the username and credential:

```js
{
    urls: "turn:localhost:3478?transport=udp",
    username: "lenin",
    credential: "lenin420"
}
```

### How do we stream videos?
- The `rtc` crate has some nice utilities to read from H264 and OPUS files...

### Read the stats
- From the WebRTC statistics API. `PeerConnection::get_stats` in our case, with
the `webrtc` crate.
> [!NOTE]
> It seems `webrtc` sends no RTCP SR or RR packets.
