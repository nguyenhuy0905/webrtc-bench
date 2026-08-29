# WebRTC benchmark
## Architecture?
- Assume peers can discover each other, for now.
- General idea; peers can send videos between each other.
    - Latency of a peer is the average of latency of contents received from all other peers.

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
# now go to localhost:8000/js
# copy the code in the first box. Let's call this $BROWSER_SDP_BASE64
# in yet another terminal,
echo $BROWSER_SDP_BASE64 | cargo run
# ...currently, it errs out before it generates a answer SDP...
# ...but once it's fixed, all you'd need to do is copy the code the program generates in the 2nd box. Then "Start Session".
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
