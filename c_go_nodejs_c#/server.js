const http = require("http");

const server = http.createServer((req, res) => {
  if (req.url === "/ping") {
    // I/O simülasyonu
    setTimeout(() => {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("pong");
    }, 10);
  }
});

server.listen(3000, () => {
  console.log("Node server running on :3000");
});
