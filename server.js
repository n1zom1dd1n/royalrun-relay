// Royal Run relay server.
//
// The browser version of Royal Run cannot open a server of its own, so phones cannot
// connect to it directly. This small server sits in between:
//
//   game (browser)  <--WebSocket-->  relay  <--WebSocket-->  phones
//
// The game connects to  /host  and gets a short room code (for example K7QX).
// The game also sends the controller page (HTML), and the relay serves it at /r/K7QX.
// A phone opens that address, the page connects to /r/K7QX/ws, and from then on
// the relay just passes messages back and forth.
//
// Messages between the relay and the game (JSON):
//   relay -> game : {r:"room",code}  {r:"open",id}  {r:"msg",id,d}  {r:"close",id}  {r:"hit"}
//   game  -> relay: {r:"page",html}  {r:"send",id,d}  {r:"all",d}  {r:"kick",id}  {r:"ping"}
// Phones send and receive exactly the same text as with the local (Wi-Fi) version.

const http = require("http");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 10000;
const CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";   // no 0/O, 1/I
const ROOM_GRACE_MS = 3 * 60 * 1000;    // a room stays alive this long after the game disconnects
const MAX_ROOMS = 500;
const MAX_PHONES_PER_ROOM = 8;
const MAX_PHONE_MSG = 4 * 1024;
const MAX_PAGE = 256 * 1024;

const rooms = new Map();   // code -> { code, host, page, phones: Map<id, ws>, nextId, emptySince }

function newCode() {
  for (let attempt = 0; attempt < 1000; attempt++) {
    let c = "";
    for (let i = 0; i < 4; i++) c += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function toHost(room, obj) {
  send(room.host, obj);
}

const notFoundPage = (code) => `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Royal Run</title>
<style>body{background:#150d29;color:#fff;font-family:system-ui,sans-serif;text-align:center;padding:40px 20px}
h1{color:#ffd54f}</style></head><body><h1>Royal Run</h1>
<p>Room <b>${code}</b> is not open.</p><p>Open Multiplayer in the game and scan the new QR code.</p>
<p><a style="color:#80eaff" href="">Try again</a></p></body></html>`;

// ---------------- HTTP ----------------

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  const parts = url.pathname.split("/").filter(Boolean);

  if (parts.length === 0) {
    res.writeHead(200, { "Content-Type": "text/plain" });
    return res.end("Royal Run relay is running. Rooms open: " + rooms.size);
  }
  if (parts[0] === "ping" || (parts.length === 3 && parts[0] === "r" && parts[2] === "ping")) {
    res.writeHead(200, { "Content-Type": "text/plain", "Access-Control-Allow-Origin": "*" });
    return res.end("ok");
  }
  if (parts[0] === "r" && parts.length === 2) {
    const code = parts[1].toUpperCase();
    const room = rooms.get(code);
    res.setHeader("Cache-Control", "no-store");
    if (!room || !room.page) {
      res.writeHead(404, { "Content-Type": "text/html; charset=utf-8" });
      return res.end(notFoundPage(code.replace(/[^A-Z0-9]/g, "")));
    }
    toHost(room, { r: "hit" });
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    return res.end(room.page);
  }
  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("not found");
});

// ---------------- WebSocket ----------------

const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_PAGE + 1024 });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, "http://x");
  const parts = url.pathname.split("/").filter(Boolean);

  if (parts.length === 1 && parts[0] === "host") {
    wss.handleUpgrade(req, socket, head, (ws) => onHost(ws, (url.searchParams.get("room") || "").toUpperCase()));
    return;
  }
  if (parts.length === 3 && parts[0] === "r" && parts[2] === "ws") {
    const room = rooms.get(parts[1].toUpperCase());
    wss.handleUpgrade(req, socket, head, (ws) => onPhone(ws, room));
    return;
  }
  socket.destroy();
});

function onHost(ws, wantedCode) {
  let room = wantedCode && rooms.get(wantedCode);
  if (room && room.host && room.host.readyState === 1) room = null;   // that room already has a game
  if (!room) {
    if (rooms.size >= MAX_ROOMS) { ws.close(1013, "server full"); return; }
    const code = newCode();
    if (!code) { ws.close(1013, "server full"); return; }
    room = { code, host: null, page: null, phones: new Map(), nextId: 1, emptySince: 0 };
    rooms.set(code, room);
  }
  room.host = ws;
  room.emptySince = 0;
  ws.isAlive = true;
  send(ws, { r: "room", code: room.code });
  // Phones that stayed connected while the game was away: close them, the controller page
  // reconnects by itself within a second and joins again, so the game sees them properly.
  for (const p of room.phones.values()) p.close(4005, "game reconnected");

  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", (data, isBinary) => {
    if (isBinary) return;
    let m;
    try { m = JSON.parse(data.toString()); } catch { return; }
    switch (m.r) {
      case "page":
        if (typeof m.html === "string" && m.html.length <= MAX_PAGE) room.page = m.html;
        break;
      case "send": {
        const p = room.phones.get(m.id);
        if (p && typeof m.d === "string" && p.readyState === 1) p.send(m.d);
        break;
      }
      case "all":
        if (typeof m.d === "string") for (const p of room.phones.values()) if (p.readyState === 1) p.send(m.d);
        break;
      case "kick": {
        const p = room.phones.get(m.id);
        if (p) p.close(1000, "removed");
        break;
      }
      case "ping":
        send(ws, { r: "pong" });
        break;
    }
  });
  ws.on("close", () => {
    if (room.host === ws) {
      room.host = null;
      room.emptySince = Date.now();
    }
  });
  ws.on("error", () => {});
}

function onPhone(ws, room) {
  if (!room) { ws.close(4004, "room not found"); return; }
  if (room.phones.size >= MAX_PHONES_PER_ROOM) { ws.close(4003, "room full"); return; }
  const id = room.nextId++;
  room.phones.set(id, ws);
  ws.isAlive = true;
  toHost(room, { r: "open", id });

  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("message", (data, isBinary) => {
    if (isBinary || data.length > MAX_PHONE_MSG) return;
    toHost(room, { r: "msg", id, d: data.toString() });
  });
  ws.on("close", () => {
    if (room.phones.get(id) === ws) {
      room.phones.delete(id);
      toHost(room, { r: "close", id });
    }
  });
  ws.on("error", () => {});
}

// Keep connections alive (hosting proxies close idle sockets) and drop dead ones.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  }
  const now = Date.now();
  for (const room of rooms.values()) {
    if (!room.host && room.emptySince && now - room.emptySince > ROOM_GRACE_MS) {
      for (const p of room.phones.values()) p.close(4004, "game closed");
      rooms.delete(room.code);
    }
  }
}, 25000);

server.listen(PORT, () => console.log("Royal Run relay listening on port " + PORT));
