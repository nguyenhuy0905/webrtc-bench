# Tasks and to-dos
## Signal server
- DONE! Save for the ICE candidate exchange part...
## Hmm, this is like my 5th or 6th rewrite of the signal handling
- Weird things happens when one tries to make a bi-directional video stream.
- Let's grab this from MDN:
1. The caller captures local Media via MediaDevices.getUserMedia
2. The caller creates RTCPeerConnection and calls RTCPeerConnection.addTrack() (Since addStream is deprecating)
3. The caller calls RTCPeerConnection.createOffer() to create an offer.
4. The caller calls RTCPeerConnection.setLocalDescription() to set that offer as the local description (that is, the description of the local end of the connection).
5. After setLocalDescription(), the caller asks STUN servers to generate the ice candidates
6. The caller uses the signaling server to transmit the offer to the intended receiver of the call.
7. The recipient receives the offer and calls RTCPeerConnection.setRemoteDescription() to record it as the remote description (the description of the other end of the connection).
8. The recipient does any setup it needs to do for its end of the call: capture its local media, and attach each media tracks into the peer connection via RTCPeerConnection.addTrack()
9. The recipient then creates an answer by calling RTCPeerConnection.createAnswer().
10. The recipient calls RTCPeerConnection.setLocalDescription(), passing in the created answer, to set the answer as its local description. The recipient now knows the configuration of both ends of the connection.
11. The recipient uses the signaling server to send the answer to the caller.
12. The caller receives the answer.
13. The caller calls RTCPeerConnection.setRemoteDescription() to set the answer as the remote description for its end of the call. It now knows the configuration of both peers. Media begins to flow as configured.
