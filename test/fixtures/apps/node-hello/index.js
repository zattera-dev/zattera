// node-hello exercises the nixpacks auto-detect path (no Dockerfile).
const http = require("http");
const port = process.env.PORT || 8080;
http
  .createServer((req, res) => {
    if (req.url === "/healthz") {
      res.end("ok\n");
      return;
    }
    res.end((process.env.FIXTURE_MESSAGE || "Hello from node fixture") + "\n");
  })
  .listen(port, () => console.log(`node-hello listening on :${port}`));
