// Room endpoint; the protocol is in loader/room.go.
// A process owns one shard port and every room on it; the single JS thread
// orders each room's broadcasts. Outgoing data is buffered by ws itself.
'use strict';
const http = require('node:http');
const { WebSocketServer } = require('ws');

const HISTORY_SIZE = 20;
const MAX_BODY = 1024;
const ERR_INVALID = '{"t":"err","code":"invalid"}';

const rooms = new Map();

function getRoom(id) {
  let room = rooms.get(id);
  if (!room) {
    room = { v: 0, lastUser: 0, members: new Set(), recent: [] };
    rooms.set(id, room);
  }
  return room;
}

// The message if it passes validation, else null.
function parse(data, isBinary) {
  if (isBinary) return null;
  let m;
  try {
    m = JSON.parse(data.toString());
  } catch {
    return null;
  }
  if (m === null || typeof m !== 'object' || m.t !== 'msg' || !Number.isSafeInteger(m.s) || m.s < 0 ||
      typeof m.body !== 'string') return null;
  let chars = 0;
  for (const _ of m.body) if (++chars > MAX_BODY) return null;
  return chars >= 1 ? m : null;
}

function join(ws, room) {
  const u = ++room.lastUser;
  ws.send(`{"t":"hello","u":${u},"v":${room.v},"recent":[${room.recent.join(',')}]}`);
  room.members.add(ws);

  ws.on('message', (data, isBinary) => {
    const m = parse(data, isBinary);
    if (!m) {
      ws.send(ERR_INVALID);
      return;
    }
    room.v++;
    const out = JSON.stringify({ t: 'msg', u, s: m.s, v: room.v, body: m.body });
    room.recent.push(out);
    if (room.recent.length > HISTORY_SIZE) room.recent.shift();
    for (const member of room.members) member.send(out);
  });
  ws.on('close', () => room.members.delete(ws));
  ws.on('error', () => ws.terminate());
}

function listenRooms(port) {
  const server = http.createServer((req, res) => {
    res.writeHead(req.url === '/health' ? 200 : 404);
    res.end(req.url === '/health' ? 'ok' : undefined);
  });
  const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 64 * 1024 });
  server.on('upgrade', (req, socket, head) => {
    const url = new URL(req.url, 'http://localhost');
    const id = url.searchParams.get('id');
    if (url.pathname !== '/room' || !id) {
      socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => join(ws, getRoom(id)));
  });
  server.listen(port, '0.0.0.0');
}

module.exports = { listenRooms };
