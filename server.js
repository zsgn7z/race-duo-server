'use strict';
/**
 * RACE DUO - Multiplayer server (zero dependencies, Node 16+)
 * Authoritative: rooms, codes, ready, start, state relay, validation,
 * boost accounting, finish/winner, reconnect, rematch.
 */
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const TRACKS = [
  { id: 0, length: 6000 },
  { id: 1, length: 8000 },
  { id: 2, length: 10000 },
];
const MAX_SPEED = 2200;          // world units / sec (normal)
const BOOST_MULT = 1.7;
const BOOST_MS = 2500;
const ENERGY_PER_PICKUP = 10;
const ENERGY_MAX = 100;
const MIN_PICKUP_GAP_MS = 250;
const COUNTDOWN_MS = 4000;
const RECONNECT_GRACE_MS = 20000;
const TICK_MS = 66;              // ~15 Hz broadcast
const MAX_MSG = 2048;

const rooms = new Map();         // code -> room
const tokens = new Map();        // token -> {code, slot}

const now = () => Date.now();
const rnd = () => crypto.randomBytes(8).toString('hex');

/* ---------------- minimal WebSocket (RFC 6455) ---------------- */
function wsAccept(key) {
  return crypto.createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
}
function frame(opcode, payload) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, payload]);
}
class Conn {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.alive = true;
    this.player = null; this.lastPong = now(); this.msgCount = 0; this.msgWindow = now();
    socket.on('data', d => this.onData(d));
    socket.on('end', () => { try { socket.end(); } catch (e) {} this.onClose(); });
    socket.on('close', () => this.onClose());
    socket.on('error', () => this.onClose());
  }
  send(obj) {
    if (!this.alive) return;
    try { this.socket.write(frame(1, Buffer.from(JSON.stringify(obj)))); } catch (e) { /* ignore */ }
  }
  close() { if (!this.alive) return; try { this.socket.write(frame(8, Buffer.alloc(0))); this.socket.end(); } catch (e) {} this.onClose(); }
  onClose() {
    if (!this.alive) return; this.alive = false;
    if (this.player) handleDisconnect(this);
  }
  onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > MAX_MSG * 4) return this.close();
    for (;;) {
      const b = this.buf;
      if (b.length < 2) return;
      const op = b[0] & 0x0f; const masked = (b[1] & 0x80) !== 0;
      let len = b[1] & 0x7f; let off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_MSG) return this.close();
      if (!masked) return this.close();
      if (b.length < off + 4 + len) return;
      const mask = b.slice(off, off + 4);
      const data = Buffer.from(b.slice(off + 4, off + 4 + len));
      for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
      this.buf = b.slice(off + 4 + len);
      if (op === 8) return this.close();
      if (op === 9) { this.socket.write(frame(10, data)); continue; }
      if (op === 10) { this.lastPong = now(); continue; }
      if (op === 1) this.onMessage(data.toString('utf8'));
    }
  }
  onMessage(text) {
    const t = now();
    if (t - this.msgWindow > 1000) { this.msgWindow = t; this.msgCount = 0; }
    if (++this.msgCount > 60) return;           // rate limit
    let m; try { m = JSON.parse(text); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    try { handleMessage(this, m); } catch (e) { console.error('handler error', e); }
  }
}

/* ---------------- rooms ---------------- */
function genCode() {
  for (let i = 0; i < 200; i++) {
    const c = String(Math.floor(1000 + Math.random() * 9000));
    if (!rooms.has(c)) return c;
  }
  return null;
}
function newPlayer(conn, name, slot) {
  return {
    slot, name: String(name || 'Player ' + (slot + 1)).slice(0, 16), conn, token: rnd(),
    connected: true, ready: false, rematch: false, vehicle: 0,
    x: 0, y: 0, vx: 0, boost: false, energy: 0, boostUntil: 0, lastPickup: 0,
    lastStateAt: 0, finished: false, finishAt: 0, dropTimer: null,
  };
}
function resetPlayerForRace(p) {
  p.x = 0; p.y = 0; p.vx = 0; p.boost = false; p.energy = 0; p.boostUntil = 0;
  p.lastStateAt = 0; p.finished = false; p.finishAt = 0; p.ready = false; p.rematch = false;
}
function bcast(room, obj, except) {
  for (const p of room.players) if (p && p.connected && p !== except) p.conn.send(obj);
}
function lobbyView(room) {
  return {
    t: 'lobby', code: room.code, phase: room.phase, track: room.track, host: 0,
    players: [0, 1].map(i => {
      const p = room.players[i];
      return p ? { slot: i, name: p.name, connected: p.connected, ready: p.ready, vehicle: p.vehicle } : null;
    }),
  };
}
function pushLobby(room) { bcast(room, lobbyView(room)); }

function destroyRoom(room) {
  for (const p of room.players) if (p) { if (p.dropTimer) clearTimeout(p.dropTimer); tokens.delete(p.token); }
  if (room.tick) clearInterval(room.tick);
  rooms.delete(room.code);
}

/* ---------------- message handling ---------------- */
function handleMessage(conn, m) {
  const p = conn.player; const room = p && rooms.get(p.code);
  switch (m.t) {
    case 'ping': return conn.send({ t: 'pong', c: m.c, s: now() });

    case 'create': {
      if (conn.player) return conn.send({ t: 'error', code: 'ALREADY_IN_ROOM' });
      const code = genCode(); if (!code) return conn.send({ t: 'error', code: 'SERVER_FULL' });
      const r = { code, players: [null, null], phase: 'lobby', track: 0, raceId: 0, startAt: 0, winner: -1, tick: null, created: now() };
      const pl = newPlayer(conn, m.name, 0); pl.code = code; conn.player = pl; r.players[0] = pl;
      rooms.set(code, r); tokens.set(pl.token, { code, slot: 0 });
      conn.send({ t: 'joined', slot: 0, token: pl.token, code });
      return pushLobby(r);
    }

    case 'join': {
      if (conn.player) return conn.send({ t: 'error', code: 'ALREADY_IN_ROOM' });
      const r = rooms.get(String(m.code || '').trim());
      if (!r) return conn.send({ t: 'error', code: 'ROOM_NOT_FOUND' });
      if (r.players[1] && r.players[1].connected) return conn.send({ t: 'error', code: 'ROOM_FULL' });
      if (r.phase === 'racing' || r.phase === 'countdown') return conn.send({ t: 'error', code: 'RACE_IN_PROGRESS' });
      if (r.players[1]) { tokens.delete(r.players[1].token); }
      const pl = newPlayer(conn, m.name, 1); pl.code = r.code; conn.player = pl; r.players[1] = pl;
      tokens.set(pl.token, { code: r.code, slot: 1 });
      conn.send({ t: 'joined', slot: 1, token: pl.token, code: r.code });
      return pushLobby(r);
    }

    case 'rejoin': {
      const info = tokens.get(String(m.token || ''));
      const r = info && rooms.get(info.code);
      if (!r) return conn.send({ t: 'error', code: 'ROOM_NOT_FOUND' });
      const pl = r.players[info.slot];
      if (!pl || pl.token !== m.token) return conn.send({ t: 'error', code: 'ROOM_NOT_FOUND' });
      if (pl.dropTimer) { clearTimeout(pl.dropTimer); pl.dropTimer = null; }
      if (pl.conn && pl.conn !== conn && pl.conn.alive) { pl.conn.player = null; pl.conn.close(); }
      pl.conn = conn; pl.connected = true; conn.player = pl;
      conn.send({ t: 'joined', slot: info.slot, token: pl.token, code: r.code, rejoined: true });
      bcast(r, { t: 'peer', slot: info.slot, connected: true }, pl);
      if (r.phase === 'racing' || r.phase === 'countdown') {
        conn.send({ t: 'resume', phase: r.phase, startAt: r.startAt, track: r.track, trackLength: TRACKS[r.track].length, serverNow: now(), you: { x: pl.x } });
      }
      return pushLobby(r);
    }

    case 'leave': { if (p && room) removePlayer(room, p, true); return; }
  }

  if (!p || !room) return conn.send({ t: 'error', code: 'NOT_IN_ROOM' });

  switch (m.t) {
    case 'name': p.name = String(m.name || p.name).slice(0, 16); return pushLobby(room);

    case 'vehicle': {
      if (room.phase !== 'lobby' && room.phase !== 'results') return;
      const v = m.id | 0; if (v >= 0 && v < 3) { p.vehicle = v; pushLobby(room); } return;
    }

    case 'track': {
      if (p.slot !== 0 || (room.phase !== 'lobby' && room.phase !== 'results')) return;
      const id = m.id | 0; if (id >= 0 && id < TRACKS.length) { room.track = id; pushLobby(room); } return;
    }

    case 'ready': {
      if (room.phase !== 'lobby') return;
      p.ready = !!m.ready; return pushLobby(room);
    }

    case 'start': {      // host starts when both connected + ready
      if (p.slot !== 0 || room.phase !== 'lobby') return;
      const a = room.players[0], b = room.players[1];
      if (!a || !b || !a.connected || !b.connected) return conn.send({ t: 'error', code: 'WAITING_FOR_PLAYER' });
      if (!a.ready || !b.ready) return conn.send({ t: 'error', code: 'NOT_READY' });
      return startRace(room);
    }

    case 'state': {      // validated client position report
      if (room.phase !== 'racing' || p.finished) return;
      const t = now();
      if (t < room.startAt) return;                      // no movement before GO
      const x = Number(m.x), y = Number(m.y), vx = Number(m.vx);
      if (!isFinite(x) || !isFinite(y) || !isFinite(vx)) return;
      const dt = p.lastStateAt ? Math.min((t - p.lastStateAt) / 1000, 1) : (t - room.startAt) / 1000;
      p.lastStateAt = t;
      const boosting = t < p.boostUntil;
      const cap = MAX_SPEED * (boosting ? BOOST_MULT : 1);
      const maxDx = cap * dt * 1.25 + 40;
      let nx = x, corrected = false;
      if (nx - p.x > maxDx) { nx = p.x + maxDx; corrected = true; }
      if (nx < p.x - 400) { nx = p.x - 400; corrected = true; }
      if (nx < 0) nx = 0;
      p.x = nx; p.y = Math.max(-2000, Math.min(2000, y)); p.vx = Math.max(-cap, Math.min(cap, vx));
      p.boost = boosting;
      if (corrected) conn.send({ t: 'correct', x: p.x, y: p.y });
      return;
    }

    case 'pickup': {     // boost energy pickup (rate limited, capped)
      if (room.phase !== 'racing') return;
      const t = now(); if (t - p.lastPickup < MIN_PICKUP_GAP_MS) return;
      p.lastPickup = t; p.energy = Math.min(ENERGY_MAX, p.energy + ENERGY_PER_PICKUP);
      return conn.send({ t: 'energy', v: p.energy });
    }

    case 'boost': {
      if (room.phase !== 'racing' || now() < room.startAt) return;
      if (p.energy < ENERGY_MAX || now() < p.boostUntil) return conn.send({ t: 'boostDenied' });
      p.energy = 0; p.boostUntil = now() + BOOST_MS;
      conn.send({ t: 'boostOk', ms: BOOST_MS, v: 0 });
      return bcast(room, { t: 'event', e: 'boost', slot: p.slot }, p);
    }

    case 'hit': {        // collision events are cosmetic for the peer; server only relays
      if (room.phase !== 'racing') return;
      return bcast(room, { t: 'event', e: 'hit', slot: p.slot, kind: String(m.kind || '').slice(0, 12) }, p);
    }

    case 'finish': {
      if (room.phase !== 'racing' || p.finished) return;
      const len = TRACKS[room.track].length;
      if (p.x < len - 60) return conn.send({ t: 'finishDenied', x: p.x });   // server-side position decides
      p.finished = true; p.finishAt = now();
      if (room.winner < 0) room.winner = p.slot;
      return checkRaceEnd(room);
    }

    case 'rematch': {
      if (room.phase !== 'results') return;
      p.rematch = !!(m.v === undefined ? true : m.v);
      bcast(room, { t: 'rematchVote', slot: p.slot, v: p.rematch });
      const a = room.players[0], b = room.players[1];
      if (a && b && a.connected && b.connected && a.rematch && b.rematch) {
        room.phase = 'lobby'; resetPlayerForRace(a); resetPlayerForRace(b);
        a.ready = true; b.ready = true;           // both agreed -> go straight to countdown
        return startRace(room);
      }
      return;
    }
  }
}

function startRace(room) {
  room.phase = 'countdown'; room.winner = -1; room.raceId++;
  for (const p of room.players) if (p) resetPlayerForRace(p);
  room.startAt = now() + COUNTDOWN_MS;
  room.seed = (Math.random() * 1e9) | 0;
  const trk = TRACKS[room.track];
  bcast(room, {
    t: 'start', raceId: room.raceId, track: room.track, trackLength: trk.length, seed: room.seed,
    startAt: room.startAt, serverNow: now(),
    players: room.players.map(p => p && { slot: p.slot, name: p.name, vehicle: p.vehicle }),
  });
  if (room.tick) clearInterval(room.tick);
  room.tick = setInterval(() => tickRoom(room), TICK_MS);
}

function tickRoom(room) {
  const t = now();
  if (room.phase === 'countdown' && t >= room.startAt) room.phase = 'racing';
  if (room.phase !== 'racing') return;
  const len = TRACKS[room.track].length;
  bcast(room, {
    t: 'snap', s: t,
    p: room.players.map(p => p && { x: Math.round(p.x), y: Math.round(p.y), vx: Math.round(p.vx), b: p.boost ? 1 : 0, f: p.finished ? 1 : 0, pr: +(Math.min(1, p.x / len)).toFixed(4) }),
  });
  // race timeout safety (5 min)
  if (t - room.startAt > 300000) { finishRace(room); }
}

function checkRaceEnd(room) {
  const ps = room.players.filter(Boolean);
  // end when winner set and everyone finished or after 8s grace for the other player
  if (ps.every(p => !p.connected || p.finished)) return finishRace(room);
  if (!room.endTimer) room.endTimer = setTimeout(() => { room.endTimer = null; if (room.phase === 'racing') finishRace(room); }, 8000);
  bcast(room, { t: 'event', e: 'finish', slot: room.winner });
}

function finishRace(room) {
  if (room.phase === 'results') return;
  if (room.endTimer) { clearTimeout(room.endTimer); room.endTimer = null; }
  if (room.tick) { clearInterval(room.tick); room.tick = null; }
  room.phase = 'results';
  if (room.winner < 0) {                          // nobody finished: furthest wins
    const [a, b] = room.players; room.winner = (b && b.x > (a ? a.x : 0)) ? 1 : 0;
  }
  for (const p of room.players) if (p) p.rematch = false;
  bcast(room, {
    t: 'result', winner: room.winner, raceId: room.raceId,
    players: room.players.map(p => p && { slot: p.slot, name: p.name, x: Math.round(p.x), finished: p.finished, time: p.finished ? p.finishAt - room.startAt : null }),
  });
  pushLobby(room);
}

/* ---------------- disconnects ---------------- */
function handleDisconnect(conn) {
  const p = conn.player; if (!p) return;
  const room = rooms.get(p.code); if (!room) return;
  if (p.conn !== conn) return;                    // replaced by rejoin
  p.connected = false; p.ready = false;
  bcast(room, { t: 'peer', slot: p.slot, connected: false }, p);
  pushLobby(room);
  p.dropTimer = setTimeout(() => removePlayer(room, p, false), RECONNECT_GRACE_MS);
  // if the other one is still racing alone they can finish; if no one is connected destroy later
  if ((room.phase === 'racing' || room.phase === 'countdown')) {
    const other = room.players[1 - p.slot];
    if (!other || !other.connected) { /* wait for grace expiry */ }
  }
}

function removePlayer(room, p, voluntary) {
  if (p.dropTimer) { clearTimeout(p.dropTimer); p.dropTimer = null; }
  tokens.delete(p.token);
  room.players[p.slot] = null;
  if (p.conn) { p.conn.player = null; if (voluntary) p.conn.send({ t: 'left' }); }
  const other = room.players[1 - p.slot];
  if (!other) return destroyRoom(room);
  if (p.slot === 0) {                              // host left: promote guest to slot 0
    other.slot = 0; room.players = [other, null];
    tokens.set(other.token, { code: room.code, slot: 0 });
  }
  bcast(room, { t: 'peerLeft', voluntary });
  if (room.phase === 'racing' || room.phase === 'countdown') {
    room.winner = other.slot; finishRace(room);
  } else if (room.phase === 'results') { room.phase = 'lobby'; }
  other.ready = false; other.rematch = false;
  if (room.phase !== 'results') room.phase = 'lobby';
  pushLobby(room);
}

/* ---------------- HTTP + upgrade ---------------- */
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, rooms: rooms.size, time: now() }));
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('RACE DUO server');
});
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + wsAccept(key) + '\r\n\r\n');
  socket.setNoDelay(true);
  const conn = new Conn(socket);
  conn.send({ t: 'hello', serverNow: now(), tracks: TRACKS });
});

// keepalive + stale-room sweeper
setInterval(() => {
  for (const room of rooms.values()) {
    for (const p of room.players) if (p && p.connected && p.conn.alive) { try { p.conn.socket.write(frame(9, Buffer.alloc(0))); } catch (e) {} }
    if (now() - room.created > 3600000) destroyRoom(room);
  }
}, 15000);

if (require.main === module) server.listen(PORT, () => console.log('RACE DUO server on :' + PORT));
module.exports = { server, rooms };
