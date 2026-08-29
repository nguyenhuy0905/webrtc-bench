const iceCfg = {
  iceServers: [
    {
      urls: "turn:localhost:6969?transport=udp",
      username: "stalin",
      credential: "stalin420"
    }
  ]
}

let pc = new RTCPeerConnection(iceCfg);

let log = msg => {
  document.getElementById('div').innerHTML += msg + '<br>'
}

pc.ontrack = function (event) {
  var el = document.createElement(event.track.kind)
  el.srcObject = event.stream[0]
  el.autoplay = true
  el.controls = true

  document.getElementById("vid").appendChild(el)
}

pc.onicecandidate = event => {
  if (event.candidate === null) {
    document.getElementById("localSessionDesc").value = btoa(JSON.stringify(pc.localDescription))
  }
}

pc.addTransceiver('video', {'direction': 'sendrecv'})
pc.addTransceiver('audio', {'direction': 'sendrecv'})
pc.createOffer().then(d => pc.setLocalDescription(d)).catch(log)

window.startSession = () => {
  let sd = document.getElementById('remoteSessionDescription').value
  if (sd === '') {
    return alert('Session Description must not be empty')
  }

  try {
    pc.setRemoteDescription(new RTCSessionDescription(JSON.parse(atob(sd))))
  } catch (e) {
    alert(e)
  }
}
