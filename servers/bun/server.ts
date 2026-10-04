// Bun + Bun.serve echo server (/ws on PORT) and room server (/room, room.ts).
// CORES=1: one process. CORES>1: this process spawns CORES children that all
// bind the same port with reusePort; the kernel (SO_REUSEPORT) spreads
// connections across them. The parent only supervises.
// Child i (from 0) owns room shard port PORT+1+i.
import { listenRooms } from "./room";

const PORT = Number(process.env.PORT ?? 8080);
const CORES = Number(process.env.CORES ?? 1);
const MAX_PAYLOAD = 1 << 20;

if (CORES > 1 && !process.env.WSBENCH_CHILD) {
  const children = Array.from({ length: CORES }, (_, i) =>
    Bun.spawn(["bun", "run", import.meta.path], {
      env: { ...process.env, WSBENCH_CHILD: "1", SHARD: String(i) },
      stdio: ["inherit", "inherit", "inherit"],
    }),
  );
  const code = await Promise.race(children.map((c) => c.exited));
  console.error(`child exited (${code})`);
  for (const c of children) c.kill();
  process.exit(1);
}

Bun.serve({
  port: PORT,
  hostname: "0.0.0.0",
  reusePort: true,
  fetch(req, server) {
    const { pathname } = new URL(req.url);
    if (pathname === "/health") return new Response("ok");
    if (pathname === "/ws" && server.upgrade(req)) return undefined;
    return new Response(null, { status: 404 });
  },
  websocket: {
    perMessageDeflate: false,
    maxPayloadLength: MAX_PAYLOAD,
    idleTimeout: 0, // the idle-connection scenario must not be reaped
    message(ws, message) {
      ws.send(message);
    },
  },
});

listenRooms(PORT + 1 + Number(process.env.SHARD ?? 0));
