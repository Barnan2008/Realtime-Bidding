'use strict';
// Zero-dependency server: Node >= 22.5 (built-in node:sqlite, hand-written RFC 6455 WebSocket).
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { DatabaseSync } = require('node:sqlite');
const PORT = process.env.PORT || 3000, EXT = 30000, PUB = path.join(__dirname, 'public');
const db = new DatabaseSync(process.env.DB_FILE || path.join(__dirname, 'data.db'));
db.exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS users(id TEXT PRIMARY KEY,name TEXT NOT NULL,token TEXT UNIQUE NOT NULL);
CREATE TABLE IF NOT EXISTS auctions(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT,region TEXT,address TEXT,type TEXT,descr TEXT,
 beds INT,baths INT,area INT,seed INT,palette INT,start_price INT,inc INT,price INT,high_id TEXT,high_name TEXT,
 bids INT DEFAULT 0,version INT DEFAULT 0,ends_at INT,status TEXT DEFAULT 'open');
CREATE TABLE IF NOT EXISTS bids(id INTEGER PRIMARY KEY AUTOINCREMENT,auction_id INT NOT NULL,user_id TEXT NOT NULL,user_name TEXT,
 amount INT NOT NULL,request_id TEXT UNIQUE,at INT,UNIQUE(auction_id,amount));`); // UNIQUE(auction,amount): DB-level guard
const q = {
  all: db.prepare('SELECT * FROM auctions ORDER BY status DESC, ends_at'), auction: db.prepare('SELECT * FROM auctions WHERE id=?'),
  recent: db.prepare('SELECT user_name,amount,at FROM bids WHERE auction_id=? ORDER BY amount DESC LIMIT 6'),
  byReq: db.prepare('SELECT 1 FROM bids WHERE request_id=?'), ins: db.prepare('INSERT INTO bids(auction_id,user_id,user_name,amount,request_id,at) VALUES(?,?,?,?,?,?)'),
  upd: db.prepare('UPDATE auctions SET price=?,high_id=?,high_name=?,bids=bids+1,ends_at=?,version=version+1 WHERE id=?'),
  due: db.prepare("SELECT id FROM auctions WHERE status='open' AND ends_at<=?"), close: db.prepare("UPDATE auctions SET status='closed',version=version+1 WHERE id=?"),
  user: db.prepare('SELECT id,name FROM users WHERE token=?'), newUser: db.prepare('INSERT INTO users VALUES(?,?,?)'),
  seed: db.prepare('INSERT INTO auctions(title,region,address,type,descr,beds,baths,area,seed,palette,start_price,inc,price,ends_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'),
};
const pub = a => ({ id: a.id, title: a.title, region: a.region, address: a.address, type: a.type, descr: a.descr, beds: a.beds, baths: a.baths, area: a.area,
  seed: a.seed, palette: a.palette, startPrice: a.start_price, inc: a.inc, price: a.price, highId: a.high_id, highName: a.high_name, bids: a.bids,
  version: a.version, endsAt: a.ends_at, status: a.status, recent: q.recent.all(a.id).map(b => ({ user: b.user_name, amount: b.amount, at: b.at })) });
function tx(fn) { db.exec('BEGIN IMMEDIATE'); try { const r = fn(); db.exec('COMMIT'); return r; } catch (e) { db.exec('ROLLBACK'); throw e; } }
const userByToken = t => (t && q.user.get(String(t))) || null;

function seedListings() {
  const M = 60e3, H = 60 * M, now = Date.now();
  [['Lunar Oasis Villa', 'Arizona', '1874 Forest Haven Rd, Sedona', 'Villa', 'A flowing glass villa with panoramic desert views and warm light at dusk.', 3, 2, 695, 8 * M, 300000, 5000],
   ['Glass Ridge Chalet', 'Colorado', '12 Alder Crest, Aspen', 'Chalet', 'Floor-to-ceiling glass above the tree line, built around a central fireplace.', 4, 3, 540, 2 * H, 640000, 10000],
   ['Cobalt Dune Pavilion', 'California', '88 Mirage Way, Palm Springs', 'Pavilion', 'A single-level pavilion wrapped around a mirror-still pool.', 2, 2, 380, 5 * H, 420000, 5000],
   ['Mistral Cove Residence', 'Greece', 'Oia Cliffside 4, Santorini', 'Villa', 'Whitewashed curves stepping down to a private cove and caldera sunsets.', 5, 4, 810, 9 * H, 1250000, 25000],
   ['Aurora Pod House', 'Norway', 'Fjellveien 7, Tromsø', 'Retreat', 'A compact pod home with a glass roof made for watching the northern lights.', 1, 1, 120, 26 * H, 180000, 2500],
   ['Saltwind Retreat', 'Mexico', 'Calle Brisa 21, Tulum', 'Retreat', 'Open-air living, a rooftop deck and a ten-minute walk to the beach.', 3, 2, 430, 50 * H, 360000, 5000]]
    .forEach(([t, r, ad, ty, d, b, ba, ar, dur, sp, inc], i) => q.seed.run(t, r, ad, ty, d, b, ba, ar, i + 1, i % 5, sp, inc, sp, now + dur));
}
if (!db.prepare("SELECT 1 FROM auctions WHERE status='open'").get()) seedListings();

// ---- placing a bid: one IMMEDIATE (write-locked) transaction; highest wins, stale rejected, idempotent per requestId
function bid(user, aid, amount, rid) {
  if (!user) return { ok: false, code: 'auth', message: 'Choose a display name to bid.' };
  aid = +aid; amount = +amount;
  if (!Number.isSafeInteger(aid) || !Number.isSafeInteger(amount) || amount <= 0 || amount > 1e10 || typeof rid !== 'string' || !rid || rid.length > 64)
    return { ok: false, code: 'invalid', message: 'Enter a whole-number bid.' };
  let r;
  try {
    r = tx(() => {
      const a = q.auction.get(aid);
      if (!a) return { ok: false, code: 'not_found', message: 'Listing not found.' };
      if (q.byReq.get(rid)) return { ok: true, dup: true, a };                       // retry after reconnect: no double-apply
      const now = Date.now();
      if (a.status !== 'open' || now >= a.ends_at) return { ok: false, code: 'closed', message: 'This auction has ended.', a };
      if (a.high_id === user.id) return { ok: false, code: 'self', message: 'You already hold the highest bid.', a };
      const min = a.bids ? a.price + a.inc : a.start_price;
      if (amount < min) return { ok: false, code: 'outbid', message: `Outbid. Current bid is $${a.price.toLocaleString()}; next bid must be at least $${min.toLocaleString()}.`, a };
      q.ins.run(aid, user.id, user.name, amount, rid, now);
      const ends = a.ends_at - now < EXT ? now + EXT : a.ends_at;                     // anti-sniping extension
      q.upd.run(amount, user.id, user.name, ends, aid);
      return { ok: true, a: q.auction.get(aid) };
    });
  } catch (e) { return { ok: false, code: 'outbid', message: 'Another bid landed first. Check the new price and try again.' }; }
  const auction = r.a && pub(r.a);
  if (r.ok && !r.dup) broadcast({ type: 'auction', auction });
  return { ok: r.ok, code: r.code, message: r.message, auction };
}
setInterval(() => { for (const { id } of q.due.all(Date.now())) {
  const a = tx(() => { const x = q.auction.get(id); if (x.status !== 'open') return null; q.close.run(id); return q.auction.get(id); });
  if (a) broadcast({ type: 'auction', auction: pub(a) }); } }, 1000);

// ---- minimal WebSocket (RFC 6455)
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', MAX = 65536, clients = new Set();
function frame(op, p) { const n = p.length; let h;
  if (n < 126) h = Buffer.from([0x80 | op, n]); else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); } return Buffer.concat([h, p]); }
class Conn {
  constructor(s, user) { this.s = s; this.user = user; this.buf = Buffer.alloc(0); this.parts = []; this.alive = true; this.closed = false; this.tokens = 20;
    s.setNoDelay(true); s.on('data', d => { this.buf = Buffer.concat([this.buf, d]); while (!this.closed && this.parse()); }); s.on('close', () => this.end()); s.on('error', () => s.destroy()); }
  send(o) { this.raw(frame(1, Buffer.from(typeof o === 'string' ? o : JSON.stringify(o)))); }
  raw(b) { if (this.closed) return; if (this.s.writableLength > 1 << 20) return this.s.destroy(); this.s.write(b); }
  parse() {
    const b = this.buf; if (b.length < 2) return false;
    const fin = !!(b[0] & 0x80), op = b[0] & 15; let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return false; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return false; const big = b.readBigUInt64BE(2); if (big > BigInt(MAX)) { this.s.destroy(); return false; } len = Number(big); off = 10; }
    if (len > MAX || !(b[1] & 128)) { this.s.destroy(); return false; }
    if (b.length < off + 4 + len) return false;
    const mask = b.subarray(off, off + 4), p = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) p[i] ^= mask[i & 3];
    this.buf = b.subarray(off + 4 + len); this.onFrame(fin, op, p); return true;
  }
  onFrame(fin, op, p) {
    if (op === 8) { this.raw(frame(8, p.subarray(0, 2))); this.s.end(); }
    else if (op === 9) this.raw(frame(10, p));
    else if (op === 10) this.alive = true;
    else if (op <= 2) { this.parts.push(p); if (this.parts.reduce((n, x) => n + x.length, 0) > MAX) return this.s.destroy();
      if (fin) { const t = Buffer.concat(this.parts).toString(); this.parts = []; onMsg(this, t); } }
  }
  end() { if (this.closed) return; this.closed = true; clients.delete(this); broadcast({ type: 'presence', online: clients.size }); }
}
function broadcast(o) { const f = frame(1, Buffer.from(JSON.stringify(o))); for (const c of clients) c.raw(f); }
function onMsg(c, t) {
  let m; try { m = JSON.parse(t); } catch { return; }
  if (--c.tokens < 0) return c.send({ type: 'ack', reqId: m.reqId, ok: false, code: 'rate_limited', message: 'Slow down a little.' });
  if (m.type === 'bid') c.send({ type: 'ack', reqId: m.reqId, ...bid(c.user, m.auctionId, m.amount, m.reqId) });
  else if (m.type === 'ping') c.send({ type: 'pong' });
}
setInterval(() => clients.forEach(c => c.tokens = 20), 1000);
setInterval(() => { for (const c of clients) { if (!c.alive) { c.s.destroy(); continue; } c.alive = false; c.raw(frame(9, Buffer.alloc(0))); } }, 20000); // drop dead sockets

// ---- HTTP
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };
const readJson = req => new Promise((ok, no) => { let s = ''; req.on('data', d => { s += d; if (s.length > 1e4) { req.destroy(); no(); } }); req.on('end', () => { try { ok(JSON.parse(s || '{}')); } catch { ok({}); } }); });
const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x'), J = (c, o) => { res.writeHead(c, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(o)); };
  try {
    if (u.pathname.startsWith('/api/')) {
      const user = userByToken((req.headers.authorization || '').replace('Bearer ', ''));
      if (req.method === 'GET' && u.pathname === '/api/auctions') return J(200, { now: Date.now(), auctions: q.all.all().map(pub) });
      if (req.method === 'GET' && u.pathname === '/api/health') return J(200, { ok: true, online: clients.size });
      const body = req.method === 'POST' ? await readJson(req) : {};
      if (req.method === 'POST' && u.pathname === '/api/session') {
        if (user) return J(200, { user });
        const name = String(body.name || '').replace(/[^\p{L}\p{N} _.-]/gu, '').trim().slice(0, 24);
        if (name.length < 2) return J(400, { ok: false, message: 'Use at least 2 letters or numbers.' });
        const nu = { id: crypto.randomUUID(), name }, token = crypto.randomBytes(24).toString('hex'); q.newUser.run(nu.id, nu.name, token);
        return J(200, { user: nu, token });
      }
      const m = u.pathname.match(/^\/api\/auctions\/(\d+)\/bids$/);
      if (req.method === 'POST' && m) return J(200, bid(user, m[1], body.amount, body.requestId));
      return J(404, { ok: false, message: 'Not found.' });
    }
    const f = path.join(PUB, path.normalize('/' + decodeURIComponent(u.pathname === '/' ? '/index.html' : u.pathname)));
    fs.readFile(f, (e, d) => { if (e) { res.writeHead(404); return res.end('Not found'); } res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' }); res.end(d); });
  } catch { J(500, { ok: false, message: 'Server error.' }); }
});
server.on('upgrade', (req, s) => {
  const u = new URL(req.url, 'http://x'), k = req.headers['sec-websocket-key'];
  if (u.pathname !== '/ws' || !k) return s.destroy();
  const user = userByToken(u.searchParams.get('token'));
  s.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + crypto.createHash('sha1').update(k + GUID).digest('base64') + '\r\n\r\n');
  const c = new Conn(s, user); clients.add(c);
  c.send({ type: 'snapshot', auctions: q.all.all().map(pub), now: Date.now(), you: user });  // fresh persisted state on every (re)connect
  broadcast({ type: 'presence', online: clients.size });
});
if (require.main === module) server.listen(PORT, () => console.log(`Auction running at http://localhost:${PORT}`));
module.exports = server;
