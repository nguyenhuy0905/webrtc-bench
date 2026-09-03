# WebRTC benchmark
## Preface
- Really, [the webrtc.rs repo](https://github.com/webrtc-rs/webrtc) has most of
what we need as examples. So, I'll refer to the examples quite a bit in here.

### Useful examples
- [Stream video and audio from disk](https://github.com/webrtc-rs/webrtc/tree/master/examples/play-from-disk-h26x).
- [Getting the stats](https://github.com/webrtc-rs/webrtc/tree/master/examples/stats). This does include RTT.
- [Broadcasting](https://github.com/webrtc-rs/webrtc/tree/master/examples/broadcast).

## Architecture?
- I'm thinking of, all peers join one broadcast channel, and throw videos to one another.
- Then we get RTT.
- I wonder how we'd go about doing subjective quality tests though.

## Setup
- We use this thing called [coturn](https://github.com/coturn/coturn) for the TURN and STUN servers.
    - We might just use STUN for now. Just `turnserver -S`. The port it uses by default is 3478.
    - For TURN, create a key pair, then use the private key to sign the X.509 cert. Y'know, usual `openssl` commands. RTFM.
    - For STUN, we might just point to a free STUN server...

### If you wanna do TURN
- First, generate a key-pair, PEM format. Check `openssl-genpkey`. For the example command below, these are `turn_key` and `turn_key.pub`.
- Then, use the private key to sign a X.509 certificate. Check `openssl-x509`. For the example command below, it's `turn_cert.pem`
- Then, create a user database or use the default one of `coturn`. There should be some guide in their site...
    - The code currently expect users: `lenin`, password `lenin420`; `stalin` password `stalin420`. Both are under realm `soviet.russia`.
    - In the example below, the database file is named `test_user_db.sql`.

```fish
# generate database (your database schema might not be installed in the exact same place...)
sqlite3 test_user_db.sql < /usr/share/turnserver/schema.sql
# add user
turnadmin -b test_user_db.sql -a -u lenin -r soviet.russia -p lenin420
# run server
turnserver -b test_user_db.sql -a --cert turn_cert.pem --pkey turn_key -p 6969 -L 127.0.0.1 -r soviet.russia
# in another terminal, launch a web server
python3 -m http.server
```

- To see how it *should* work, toy with one of the data channel examples in [the webrtc.rs repo](https://github.com/webrtc-rs/webrtc/tree/master).
    - I don't have any luck with the Google's STUN server. So, you might need to modify the ICE server it's using... Change on both the jsfiddle of the examples, and the example Rust code.
    - For STUN, just change `urls` to `stun:localhost:3478`.
    - If you use the TURN example above:

```js
{
    urls: "turn:localhost:6969?transport=udp",
    username: "lenin", // or "stalin"
    credential: "lenin420" // or "stalin420"
}
```

### How do we stream videos?
- FFmpeg or GStreamer can stream a single channel as RTP (consult [the FFmpeg
example here](https://trac.ffmpeg.org/wiki/StreamingGuide)).
