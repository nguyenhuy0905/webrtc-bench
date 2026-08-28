# WebRTC benchmark
## Architecture?
- Assume peers can discover each other, for now.
- General idea; peers can send videos between each other.
    - Latency of a peer is the average of latency of contents received from all other peers.

## Setup
- We use this thing called [coturn](https://github.com/coturn/coturn) for the TURN and STUN servers.
    - We might just use STUN for now.
    - For TURN, create a key pair, then use the private key to sign the X.509 cert. Y'know, usual `openssl` commands. RTFM.
    - For STUN, we might just point to a free STUN server...
