// capture my face
let constraints = {audio: false, video: true};

navigator.mediaDevices.getUserMedia(constraints).then((stream) => {
  let video = document.getElementById("video");
  video.srcObject = stream;
  video.onloadmetadata = () => {
    video.play();
  };
}).catch((err) => {
  console.error(`${err.name}: ${err.message}`)
});
