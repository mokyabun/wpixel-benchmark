// Node.js + ws echo server (/ws on PORT) and room server (/room, room.js).
// CORES=1: one process. CORES>1: node:cluster with that many workers; the
// primary distributes connections (round-robin, the Linux default).
// Worker i (from 0) owns room shard port PORT+1+i.
'use strict';
const cluster = require('node:cluster');
const http = require('node:http');
const { WebSocketServer } = require('ws');
const { listenRooms } = require('./room');

const PORT = Number(process.env.PORT || 8080);
const CORES = Number(process.env.CORES || 1);
const MAX_PAYLOAD = 1 << 20;

if (CORES > 1 && cluster.isPrimary) {
  for (let i = 0; i < CORES; i++) cluster.fork({ SHARD: i });
  cluster.on('exit', (w, code) => {
    console.error(`worker ${w.process.pid} exited (${code})`);
    process.exit(1);
  });
} else {
  const server = http.createServer((req, res) => {
    if (req.url === '/health') {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
      return;
    }
    res.writeHead(404);
    res.end();
  });

  const wss = new WebSocketServer({ server, path: '/ws', perMessageDeflate: false, maxPayload: MAX_PAYLOAD });
  wss.on('connection', (ws) => {
    ws.on('message', (data, isBinary) => ws.send(data, { binary: isBinary }));
    ws.on('error', () => ws.terminate());
  });

  server.listen(PORT, '0.0.0.0');
  listenRooms(PORT + 1 + Number(process.env.SHARD || 0));
}
