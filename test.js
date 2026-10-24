const net = require('net'), crypto = require('crypto');
const { server } = require('./server.js');
function client(port) {
  return new Promise(res => {
    const s = net.connect(port, '127.0.0.1'); let buf = Buffer.alloc(0), up = false; const q = [], waiters = [];
    const c = { msgs: q, send(o) { const d = Buffer.from(JSON.stringify(o)); const m = crypto.randomBytes(4);
        let h = d.length < 126 ? Buffer.from([0x81, 0x80 | d.length]) : Buffer.from([0x81, 0xfe, d.length >> 8, d.length & 255]);
        const x = Buffer.from(d); for (let i = 0; i < x.length; i++) x[i] ^= m[i & 3]; s.write(Buffer.concat([h, m, x])); },
      wait(t, ms = 3000) { return new Promise((r, j) => { const f = () => { const i = q.findIndex(m => m.t === t); if (i >= 0) return r(q.splice(i, 1)[0]); }; const tm = setTimeout(() => j(new Error('timeout ' + t)), ms);
        const iv = setInterval(() => { const i = q.findIndex(m => m.t === t); if (i >= 0) { clearInterval(iv); clearTimeout(tm); r(q.splice(i, 1)[0]); } }, 10); }); },
      close() { s.destroy(); } };
    s.write('GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: ' + crypto.randomBytes(16).toString('base64') + '\r\nSec-WebSocket-Version: 13\r\n\r\n');
    s.on('data', d => { buf = Buffer.concat([buf, d]);
      if (!up) { const i = buf.indexOf('\r\n\r\n'); if (i < 0) return; up = true; buf = buf.slice(i + 4); res(c); }
      for (;;) { if (buf.length < 2) break; let l = buf[1] & 127, o = 2; if (l === 126) { if (buf.length < 4) break; l = buf.readUInt16BE(2); o = 4; }
        if (buf.length < o + l) break; const op = buf[0] & 15; const p = buf.slice(o, o + l); buf = buf.slice(o + l); if (op === 1) q.push(JSON.parse(p.toString())); } });
  });
}
const ok = (c, m) => { if (!c) { console.error('FAIL:', m); process.exit(1); } console.log('PASS:', m); };
(async () => {
  await new Promise(r => server.listen(0, r)); const port = server.address().port;
  const A = await client(port), B = await client(port);
  await A.wait('hello'); await B.wait('hello');
  A.send({ t: 'create', name: 'Ali' }); const ja = await A.wait('joined');
  ok(/^\d{4}$/.test(ja.code) && ja.slot === 0, 'room created with 4-digit code ' + ja.code);
  B.send({ t: 'join', code: '0000' }); ok((await B.wait('error')).code === 'ROOM_NOT_FOUND', 'bad code rejected');
  B.send({ t: 'join', code: ja.code, name: 'Sara' }); const jb = await B.wait('joined'); ok(jb.slot === 1, 'player 2 joined');
  A.send({ t: 'start' }); ok((await A.wait('error')).code === 'NOT_READY', 'cannot start before ready');
  A.send({ t: 'ready', ready: true }); B.send({ t: 'ready', ready: true });
  await new Promise(r => setTimeout(r, 100)); A.send({ t: 'start' });
  const sa = await A.wait('start'), sb = await B.wait('start'); ok(sa.seed === sb.seed && sa.startAt > Date.now(), 'race started with shared seed + countdown');
  await new Promise(r => setTimeout(r, sa.startAt - Date.now() + 100));
  // B cheats: teleports
  B.send({ t: 'state', x: 5000, y: 0, vx: 100 }); await new Promise(r => setTimeout(r, 100));
  ok((await B.wait('correct')).x < 1000, 'teleport cheat corrected by server');
  B.send({ t: 'finish' }); ok((await B.wait('finishDenied')) !== null, 'fake finish denied');
  B.send({ t: 'boost' }); ok((await B.wait('boostDenied')) !== null, 'boost without energy denied');
  // A races legitimately
  let x = 0; const len = sa.trackLength;
  while (x < len) { await new Promise(r => setTimeout(r, 70)); x += 120; A.send({ t: 'state', x, y: 0, vx: 1700 }); }
  await new Promise(r => setTimeout(r, 100)); A.send({ t: 'finish' });
  const ra = await A.wait('result', 12000); ok(ra.winner === 0, 'server declared A winner');
  A.send({ t: 'rematch' }); B.send({ t: 'rematch' });
  const s2 = await A.wait('start'); ok(s2.raceId === 2, 'rematch restarted in same room');
  // reconnect
  B.close(); const pe = await A.wait('peer'); ok(pe.connected === false, 'server detected disconnect'); const B2 = await client(port); await B2.wait('hello');
  B2.send({ t: 'rejoin', token: jb.token }); const rj = await B2.wait('joined'); ok(rj.rejoined, 'player reconnected with token');
  console.log('ALL TESTS PASSED'); process.exit(0);
})().catch(e => { console.error('ERR', e.message); process.exit(1); });
