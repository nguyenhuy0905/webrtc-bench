# Tasks and to-dos
## Signal server
- It's an HTTP server.
- Accepts two arguments, `-a [address to bind to] -p [port to bind to]`
- [ ] For `POST /channel`, peer sends the channel name and SDP offer, and server
will check if a channel with the same name already exists:
    1. if yes, return a FORBIDDEN (and probably some JSON saying the
    channel already exists, but we probably don't need that for now).
    2. If no, return a CREATED, create a new channel under that name,
    add the SDP offer into the channel data's offers, subscribe the peer to
    the broadcast channel (in some way, probably by sending the peer an
    offer for a WebRTC DataChannel).
- [ ] For `GET /channel`, if channel doesn't exist, send an empty OK back.
Else send an OK with a JSON array representing *all* the SDP offers
currently stored.
- [ ] Anything else? Not sure...
## Client
- [ ]First try to `GET /channel`; if the channel exists (and the peer gets the
offers), create a `RTCPeerConnection` and send answer for each offer. And also
one `RTCPeerConnection` with the server for a `DataChannel` to receive any new
offer.
- [ ]If not, try to `POST /channel` with that name and the SDP. If fails, too
bad.
- We only connect to one STUN or TURN server we control for now. So, ICE
trickle doesn't matter.
## Others
- One of the nodes is the NTP server. Probably will bolt that onto the
signaling server node as well.
- `nftables` configurations to rate-limit... Wait, no, `ufw` has limit rules.
Use that one, the syntax is a lot less cursed than `nft` by itself... But, you
will need to install `nft` either way.
