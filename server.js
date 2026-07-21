const http = require('http');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const PORT = process.env.PORT || 8080;
const AGENTS = new Map();

// ── Minimal WebSocket implementation (zero dependencies) ──────────

class WebSocketServer extends EventEmitter {
  constructor(server) {
    super();
    server.on('upgrade', (req, socket) => {
      const key = req.headers['sec-websocket-key'];
      if (!key) { socket.destroy(); return; }
      const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-5AB9DC11B85B').digest('base64');
      socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
      this.emit('connection', new WSClient(socket));
    });
  }
}

class WSClient extends EventEmitter {
  constructor(socket) {
    super();
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    socket.on('data', d => this._feed(d));
    socket.on('close', () => this.emit('close'));
    socket.on('error', () => {});
  }
  _feed(data) {
    this.buf = Buffer.concat([this.buf, data]);
    while (this.buf.length >= 2) {
      const op = this.buf[0] & 0x0f;
      const masked = (this.buf[1] & 0x80) !== 0;
      let len = this.buf[1] & 0x7f;
      let off = 2;
      if (len === 126) { if (this.buf.length < 4) break; len = this.buf.readUInt16BE(2); off = 4; }
      if (len === 127) { if (this.buf.length < 10) break; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      let mask = null;
      if (masked) { if (this.buf.length < off + 4) break; mask = this.buf.slice(off, off + 4); off += 4; }
      if (this.buf.length < off + len) break;
      let payload = this.buf.slice(off, off + len);
      if (mask) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
      this.buf = this.buf.slice(off + len);
      if (op === 1) this.emit('message', payload.toString('utf8'));
      if (op === 8) { this.emit('close'); this.socket.end(); return; }
      if (op === 9) this._raw(0x0a, payload);
    }
  }
  send(msg) { this._raw(0x01, Buffer.from(msg, 'utf8')); }
  _raw(op, payload) {
    const h = Buffer.alloc(2);
    h[0] = 0x80 | op;
    if (payload.length < 126) { h[1] = payload.length; this.socket.write(Buffer.concat([h, payload])); }
    else if (payload.length < 65536) { h[1] = 126; const e = Buffer.alloc(2); e.writeUInt16BE(payload.length); this.socket.write(Buffer.concat([h, e, payload])); }
    else { h[1] = 127; const e = Buffer.alloc(8); e.writeBigUInt64BE(BigInt(payload.length)); this.socket.write(Buffer.concat([h, e, payload])); }
  }
  close() { this._raw(0x08, Buffer.alloc(0)); this.socket.end(); }
}

// ── Relay Server ──────────────────────────────────────────────────

function sendTo(name, msg) {
  const data = JSON.stringify(msg);
  for (const [id, info] of AGENTS) {
    if (info.name === name) { info.ws.send(data); return true; }
  }
  return false;
}

function broadcast(senderId, msg) {
  const data = JSON.stringify(msg);
  for (const [id, info] of AGENTS) {
    if (id !== senderId) info.ws.send(data);
  }
}

function statusMsg() {
  const list = [];
  for (const [id, info] of AGENTS) list.push({ id, name: info.name, executor: !!info.executor, connected: info.connected });
  return JSON.stringify({ type: 'status', agents: list, count: list.length });
}

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');

  if (req.method === 'GET' && req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(`<!DOCTYPE html>
<html><head><title>Agent Relay</title><meta charset="utf-8"></head>
<body><h1>Agent Relay</h1><p>Connected agents: <span id="count">0</span></p>
<ul id="agents"></ul>
<pre id="log"></pre>
<script>
const ws = new WebSocket('wss://' + location.host);
ws.onmessage = e => {
  const d = JSON.parse(e.data);
  if (d.type === 'status') { document.getElementById('count').textContent = d.count;
    document.getElementById('agents').innerHTML = d.agents.map(a => '<li>' + a.name + (a.executor?' (executor)':'') + '</li>').join(''); }
  else document.getElementById('log').textContent += JSON.stringify(d) + '\\n';
};
ws.onopen = () => ws.send(JSON.stringify({type:'join',name:'Web UI'}));
</script></body></html>`);
    return;
  }

  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', agents: AGENTS.size }));
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer(server);

wss.on('connection', ws => {
  const id = crypto.randomUUID();
  let name = 'unknown';
  let isExecutor = false;

  ws.on('message', raw => {
    try {
      const msg = JSON.parse(raw);

      if (msg.type === 'join') {
        name = msg.name || 'anonymous';
        isExecutor = !!msg.executor;
        AGENTS.set(id, { ws, name, executor: isExecutor, connected: new Date().toISOString() });
        broadcast(id, { type: 'join', id, name, executor: isExecutor, agents: AGENTS.size });
        broadcast(null, JSON.parse(statusMsg()));
        return;
      }

      if (msg.type === 'chat') {
        broadcast(id, { type: 'chat', from: name, id, text: msg.text, timestamp: new Date().toISOString() });
        return;
      }

      if (msg.type === 'mcp_call') {
        const target = msg.target || 'opencode';
        const sent = sendTo(target, {
          type: 'mcp_call',
          callId: msg.callId,
          method: msg.method,
          params: msg.params,
          fromName: name,
          fromId: id,
        });
        ws.send(JSON.stringify({ type: 'mcp_call_ack', callId: msg.callId, delivered: sent, target }));
        return;
      }

      if (msg.type === 'mcp_result') {
        // Forward result back to the caller (agent that made the mcp_call)
        const sent = sendTo(msg.fromName, {
          type: 'mcp_result',
          callId: msg.callId,
          result: msg.result,
          error: msg.error,
        });
        if (!sent) {
          ws.send(JSON.stringify({ type: 'mcp_result_undelivered', callId: msg.callId }));
        }
        return;
      }

      if (msg.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
        return;
      }

      broadcast(id, { ...msg, from: name, id });
    } catch (e) {
      ws.send(JSON.stringify({ type: 'error', message: e.message }));
    }
  });

  ws.on('close', () => {
    AGENTS.delete(id);
    broadcast(null, { type: 'leave', id, name, agents: AGENTS.size });
    broadcast(null, JSON.parse(statusMsg()));
  });
});

server.listen(PORT, () => {
  console.log(`Agent Relay running on port ${PORT}`);
  console.log(`WebSocket: ws://localhost:${PORT}`);
  console.log(`Health: http://localhost:${PORT}/health`);
});
