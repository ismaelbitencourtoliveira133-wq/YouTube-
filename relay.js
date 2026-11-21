#!/usr/bin/env node
/**
 * Watch Party relay server.
 *
 * Zero dependencies: Node 18+ has a WebSocket client but not a server, so the
 * RFC 6455 server side is implemented here directly (~150 lines). That keeps the
 * project installable with `node server/relay.js` and nothing else.
 *
 * Room semantics
 *  - First connection to a room becomes the host (it is the only one allowed to
 *    load a video / pause / seek for everyone).
 *  - Guests get every state snapshot, action and seek the host produces.
 *  - The host is authoritative for position; guests only receive commands.
 *
 * Run:  PORT=8080 node server/relay.js
 */

'use strict';

const http = require('http');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '8080', 10);
// On Render / Railway / Fly the host is always 0.0.0.0; locally that is fine too.
const HOST = process.env.HOST || '0.0.0.0';
const MAX_ROOMS = 500;
const MAX_PER_ROOM = 8;

/** room code -> { host: ws|null, clients: Set<ws>, state: {...}, createdAt } */
const rooms = new Map();

function nowMs() {
  return Date.now();
}

function makeState() {
  return {
    video: '',
    pos: 0,
    paused: true,
    rate: 1,
    ts: nowMs()
  };
}

function send(ws, payload) {
  if (!ws || ws.destroyed) return;
  const data = JSON.stringify(payload);
  if (ws.readyState !== 1 /* OPEN */) return;
  try {
    ws.send(data);
  } catch (err) {
    // Socket died mid-write; the close handler cleans up.
  }
}

function broadcast(room, payload, except) {
  for (const client of room.clients) {
    if (client === except) continue;
    send(client, payload);
  }
}

function stats() {
  let clients = 0;
  for (const room of rooms.values()) clients += room.clients.size;
  return { rooms: rooms.size, clients };
}

// ---------------------------------------------------------------- frame codec

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function encodeFrame(opcode, payload) {
  const len = payload.length;
  let header;

  if (len < 126) {
    header = Buffer.alloc(2);
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  header[0] = 0x80 | opcode;
  return Buffer.concat([header, payload]);
}

function encodeText(text) {
  return encodeFrame(0x1, Buffer.from(text, 'utf8'));
}

/** Appends bytes to ws._inbox and returns any complete frames. */
function drainFrames(ws, chunk) {
  ws._inbox = ws._inbox ? Buffer.concat([ws._inbox, chunk]) : chunk;
  const frames = [];

  while (ws._inbox.length >= 2) {
    const buf = ws._inbox;
    const opcode = buf[0] & 0x0f;
    const masked = (buf[1] & 0x80) !== 0;
    let length = buf[1] & 0x7f;
    let offset = 2;

    if (length === 126) {
      if (buf.length < offset + 2) break;
      length = buf.readUInt16BE(offset);
      offset += 2;
    } else if (length === 127) {
      if (buf.length < offset + 8) break;
      const big = buf.readBigUInt64BE(offset);
      if (big > BigInt(1 << 20)) throw new Error('frame too large');
      length = Number(big);
      offset += 8;
    }

    let mask = null;
    if (masked) {
      if (buf.length < offset + 4) break;
      mask = buf.subarray(offset, offset + 4);
      offset += 4;
    }

    if (buf.length < offset + length) break;

    const payload = Buffer.from(buf.subarray(offset, offset + length));
    if (mask) {
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
    }

    frames.push({ opcode, payload });
    ws._inbox = buf.subarray(offset + length);
  }

  return frames;
}

// ---------------------------------------------------------------- http server

const server = http.createServer((req, res) => {
  const statsNow = stats();
  res.writeHead(200, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  res.end(JSON.stringify({
    ok: true,
    service: 'watchparty-relay',
    uptime: Math.round(process.uptime()),
    rooms: statsNow.rooms,
    clients: statsNow.clients
  }));
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers['upgrade'] || '').toLowerCase() !== 'websocket') {
    socket.destroy();
    return;
  }

  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n` +
    'Server: WatchParty/1.0\r\n\r\n'
  );
  socket.setNoDelay(true);

  const ws = {
    socket,
    destroyed: false,
    readyState: 1,
    room: null,
    roomCode: '',
    isHost: false,
    user: 'guest',
    send(text) {
      if (this.destroyed || this.readyState !== 1) return;
      socket.write(encodeText(text));
    }
  };

  ws.onData = (chunk) => {
    let frames;
    try {
      frames = drainFrames(ws, chunk);
    } catch (err) {
      socket.destroy();
      return;
    }

    for (const frame of frames) {
      handleFrame(ws, frame);
    }
  };

  socket.on('data', ws.onData);

  socket.on('error', () => cleanup(ws));
  socket.on('close', () => cleanup(ws));
  socket.setTimeout(0);
});

function handleFrame(ws, frame) {
  switch (frame.opcode) {
    case 0x8: // close
      send(ws, JSON.stringify({ t: 'bye' }));
      ws.readyState = 3;
      ws.socket.end(Buffer.from([0x88, 0x00]));
      break;

    case 0x9: // ping -> pong with same payload
      if (!ws.destroyed) ws.socket.write(encodeFrame(0xA, frame.payload));
      break;

    case 0x1: {
      let message;
      try {
        message = JSON.parse(frame.payload.toString('utf8'));
      } catch (err) {
        send(ws, JSON.stringify({ t: 'error', msg: 'mensagem inválida' }));
        return;
      }
      if (!message || typeof message.t !== 'string') return;
      handleMessage(ws, message);
      break;
    }

    default:
      // ignore binary/other
      break;
  }
}

// ---------------------------------------------------------------- rooms

function handleMessage(ws, message) {
  switch (message.t) {
    case 'join':
      joinRoom(ws, String(message.room || '').toUpperCase(), message.host === true, message.user);
      break;

    case 'leave':
      cleanup(ws);
      break;

    case 'state':
      applyState(ws, message);
      break;

    case 'play':
      if (!isAuthoritative(ws)) return;
      {
        const room = ws.room;
        room.state.paused = false;
        room.state.ts = nowMs();
        broadcast(room, { t: 'action', act: 'play', ts: room.state.ts });
        broadcast(room, snapshot(room));
      }
      break;

    case 'pause':
      if (!isAuthoritative(ws)) return;
      {
        const room = ws.room;
        room.state.paused = true;
        room.state.ts = nowMs();
        broadcast(room, { t: 'action', act: 'pause', ts: room.state.ts });
        broadcast(room, snapshot(room));
      }
      break;

    case 'seek':
      if (!isAuthoritative(ws)) return;
      {
        const room = ws.room;
        const pos = clampPosition(message.pos, room.state.pos);
        room.state.pos = pos;
        room.state.ts = nowMs();
        broadcast(room, { t: 'seek', pos, ts: room.state.ts });
        broadcast(room, snapshot(room));
      }
      break;

    case 'load':
      if (!isAuthoritative(ws)) return;
      {
        const room = ws.room;
        const video = String(message.video || '');
        if (!video || !/^[A-Za-z0-9_-]{11}$/.test(video)) {
          send(ws, JSON.stringify({ t: 'error', msg: 'vídeo inválido' }));
          return;
        }
        room.state.video = video;
        room.state.pos = 0;
        room.state.paused = true;
        room.state.ts = nowMs();
        broadcast(room, { t: 'load', video, ts: room.state.ts });
        broadcast(room, snapshot(room));
      }
      break;

    case 'ping':
      send(ws, JSON.stringify({ t: 'pong', ts: message.ts || nowMs() }));
      break;

    default:
      break;
  }
}

/** Only the host can transport the whole room. */
function isAuthoritative(ws) {
  if (!ws.room) return false;
  if (!ws.isHost) {
    send(ws, JSON.stringify({ t: 'error', msg: 'somente o criador controla a sala' }));
    return false;
  }
  return true;
}

function clampPosition(value, fallback) {
  const num = Number(value);
  if (!Number.isFinite(num) || num < 0) return fallback;
  return Math.min(num, 60 * 60 * 12);
}

function snapshot(room) {
  return {
    t: 'state',
    video: room.state.video,
    pos: room.state.pos,
    paused: room.state.paused,
    rate: room.state.rate,
    ts: room.state.ts
  };
}

function joinRoom(ws, code, wantHost, user) {
  if (!/^[A-Z0-9]{4,12}$/.test(code)) {
    send(ws, JSON.stringify({ t: 'error', msg: 'código inválido' }));
    return;
  }

  if (ws.room) leaveCurrentRoom(ws);

  if (rooms.size >= MAX_ROOMS && !rooms.has(code)) {
    send(ws, JSON.stringify({ t: 'error', msg: 'servidor cheio' }));
    return;
  }

  let room = rooms.get(code);
  const isFirst = !room;

  if (!room) {
    room = { host: null, clients: new Set(), state: makeState(), createdAt: nowMs() };
    rooms.set(code, room);
  }

  if (room.clients.size >= MAX_PER_ROOM) {
    send(ws, JSON.stringify({ t: 'error', msg: 'sala cheia' }));
    return;
  }

  ws.room = room;
  ws.roomCode = code;
  ws.user = String(user || 'guest').slice(0, 24);
  ws.isHost = wantHost || isFirst;
  room.clients.add(ws);

  if (ws.isHost) {
    room.host = ws;
  }

  room.code = code;

  send(ws, JSON.stringify({
    t: 'joined',
    host: ws.isHost,
    users: room.clients.size,
    code,
    ts: nowMs()
  }));

  // A fresh guest gets the current state immediately: no waiting for a snapshot.
  send(ws, snapshot(room));
  broadcast(room, { t: 'users', users: room.clients.size });
}

function leaveCurrentRoom(ws) {
  const room = ws.room;
  if (!room) return;

  room.clients.delete(ws);

  if (room.host === ws) {
    room.host = room.clients.values().next().value || null;
    if (room.host) {
      // Promote the next client so the room stays controllable.
      room.host.isHost = true;
      send(room.host, JSON.stringify({ t: 'joined', host: true, users: room.clients.size, code: room.code }));
    }
  }

  broadcast(room, { t: 'users', users: room.clients.size });

  if (room.clients.size === 0) {
    rooms.delete(ws.roomCode);
  }

  ws.room = null;
  ws.roomCode = '';
  ws.isHost = false;
}

function cleanup(ws) {
  if (ws.destroyed) return;
  ws.destroyed = true;
  try {
    leaveCurrentRoom(ws);
  } catch (err) {
    // ignore
  }
  try {
    ws.socket.destroy();
  } catch (err) {
    // ignore
  }
}

// ---------------------------------------------------------------- keepalive

const sweeper = setInterval(() => {
  const cutoff = nowMs() - 1000 * 60 * 60 * 6;
  for (const [code, room] of rooms.entries()) {
    if (room.clients.size === 0 && room.createdAt < cutoff) rooms.delete(code);
  }
}, 1000 * 60 * 10);
sweeper.unref();

// Render/Railway/Google Cloud Run inject $PORT and expect the bind to 0.0.0.0.
// The app connects with ws:// (or wss:// behind a TLS proxy), so no extra flag.
server.listen(PORT, HOST, () => {
  console.log(`[watchparty] relay listening on ${HOST}:${PORT}`);
  console.log(`[watchparty] health: http://${HOST}:${PORT}/`);
  if (process.env.PUBLIC_URL) {
    console.log(`[watchparty] public: ${process.env.PUBLIC_URL}`);
  }
});
