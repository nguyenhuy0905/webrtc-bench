let submitButton = document.getElementById("submit");
let startButton = document.getElementById("start");
let stopButton = document.getElementById("stop");
let localPeerConnection = null;
let remotePeerConnection = null;

submitButton.onclick = (event) => {
  let username = document.getElementById("username").value;
  let password = document.getElementById("password").value;

  console.log(`ICE candidate credential: ${username} = ${password}`);
  peerConnection = new RTCPeerConnection({
    iceServers: [{
      urls: "turn:127.0.0.1:6969?transport=udp",
      username: username,
      credential: password
    }]
  });
};
startButton.onclick = (event) => {
  if(localPeerConnection === null) {
    console.error("Please put in username, password, then authenticate first");
    return;
  }

  let stream = await navigator.mediaDevices.getUserMedia({
    audio: false,
    video: true
  });
  peerConnection.onicecandidate = (event) => {
    console.log(`New ICE candidate: ${event.candidate}`);
  };
  peerConnection.createOffer()
    .then((offer) => peerConnection.setLocalDescription(offer))
    .catch((err) => {
      console.error(`${err.name}: ${err.message}`)
    });
};
