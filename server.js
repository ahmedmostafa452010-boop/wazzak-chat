const express = require('express'), http = require('http'), { Server } = require('socket.io');
const app = express(); app.set('trust proxy', true); app.get('/', (q, r) => r.sendFile(__dirname + '/index.html'));
const server = http.createServer(app), io = new Server(server);
let waiting = null;
const partner = new Map(), last = new Map(), reports = {}, bans = {};
const ip = s => String(s.handshake.headers['x-forwarded-for'] || s.handshake.address || '').split(',')[0].trim();
const get = id => io.sockets.sockets.get(id);
function leave(s) {
  const p = partner.get(s.id);
  if (p) { partner.delete(s.id); partner.delete(p); const o = get(p); if (o) o.emit('ended'); }
  if (waiting === s.id) waiting = null;
}
io.on('connection', s => {
  const online = () => io.emit('online', io.engine.clientsCount); online();
  s.on('find', () => {
    if ((bans[ip(s)] || 0) > Date.now()) { s.emit('banned'); return; }
    leave(s);
    if (waiting && waiting !== s.id && get(waiting)) {
      const o = waiting; waiting = null;
      partner.set(s.id, o); partner.set(o, s.id);
      s.emit('matched'); get(o).emit('matched');
    } else waiting = s.id;
  });
  s.on('msg', t => {
    const p = partner.get(s.id); if (!p || typeof t !== 'string') return;
    const n = Date.now(); if (n - (last.get(s.id) || 0) < 400) return; last.set(s.id, n);
    io.to(p).emit('msg', t.slice(0, 300));
  });
  s.on('report', () => {
    const p = partner.get(s.id), o = p && get(p); if (!o) return;
    const k = ip(o); reports[k] = (reports[k] || 0) + 1;
    if (reports[k] >= 3) { bans[k] = Date.now() + 36e5; reports[k] = 0; o.emit('banned'); o.disconnect(true); }
    leave(s);
  });
  s.on('leave', () => leave(s));
  s.on('disconnect', () => { leave(s); last.delete(s.id); online(); });
});
server.listen(process.env.PORT || 3000, () => console.log('running'));
