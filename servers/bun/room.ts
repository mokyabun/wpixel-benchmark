// Room endpoint; the protocol is in loader/room.go.
// A process owns one shard port and every room on it; the single JS thread
// orders each room's broadcasts. A room is a Bun pub/sub topic, so the fan-out
// itself (server.publish) runs natively.
import type { ServerWebSocket } from "bun";

const HISTORY_SIZE = 20;
const MAX_BODY = 1024;
const ERR_INVALID = '{"t":"err","code":"invalid"}';

type Room = { topic: string; v: number; lastUser: number; recent: string[] };
type Member = { room: Room; u: number };

const rooms = new Map<string, Room>();

function getRoom(id: string): Room {
  let room = rooms.get(id);
  if (!room) {
    room = { topic: `room:${id}`, v: 0, lastUser: 0, recent: [] };
    rooms.set(id, room);
  }
  return room;
}

// The message if it passes validation, else null.
function parse(message: string | Buffer): { s: number; body: string } | null {
  if (typeof message !== "string") return null;
  let m: any;
  try {
    m = JSON.parse(message);
  } catch {
    return null;
  }
  if (m === null || typeof m !== "object" || m.t !== "msg" || !Number.isSafeInteger(m.s) || m.s < 0 ||
      typeof m.body !== "string") return null;
  let chars = 0;
  for (const _ of m.body) if (++chars > MAX_BODY) return null;
  return chars >= 1 ? m : null;
}

export function listenRooms(port: number) {
  const server = Bun.serve<Member>({
    port,
    hostname: "0.0.0.0",
    fetch(req, server) {
      const url = new URL(req.url);
      if (url.pathname === "/health") return new Response("ok");
      const id = url.searchParams.get("id");
      if (url.pathname === "/room" && id && server.upgrade(req, { data: { room: getRoom(id), u: 0 } })) return undefined;
      return new Response(null, { status: 400 });
    },
    websocket: {
      perMessageDeflate: false,
      maxPayloadLength: 64 * 1024,
      idleTimeout: 0,
      open(ws: ServerWebSocket<Member>) {
        const room = ws.data.room;
        ws.data.u = ++room.lastUser;
        ws.send(`{"t":"hello","u":${ws.data.u},"v":${room.v},"recent":[${room.recent.join(",")}]}`);
        ws.subscribe(room.topic);
      },
      message(ws: ServerWebSocket<Member>, message) {
        const m = parse(message);
        if (!m) {
          ws.send(ERR_INVALID);
          return;
        }
        const room = ws.data.room;
        room.v++;
        const out = JSON.stringify({ t: "msg", u: ws.data.u, s: m.s, v: room.v, body: m.body });
        room.recent.push(out);
        if (room.recent.length > HISTORY_SIZE) room.recent.shift();
        server.publish(room.topic, out);
      },
    },
  });
}
