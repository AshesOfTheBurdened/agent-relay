#!/usr/bin/env node
'use strict';
/*
 * agent-relay-bridge.js — MCP stdio server bridging opencode <-> agent-relay fleet.
 * Self-contained (Node built-ins only). Raw RFC6455 client (the `ws` lib breaks
 * on Railway's TLS proxy). SOVEREIGN: no filtering of tool/params.
 *
 * Tools exposed:
 *   relay_execute      run one tool on one agent
 *   relay_broadcast    run one tool on many agents in parallel, aggregate
 *   relay_status       fleet-wide inventory (system_info on all agents)
 *   relay_auto         pick the best agent by capability hint, then run
 *   relay_copy         copy a file between two agents (composes read_file+write_file)
 *   fleet_memory_get   read shared fleet memory
 *   fleet_memory_set   write shared fleet memory
 *   fleet_memory_search  search shared fleet memory keys
 *   fleet_capabilities   show the capability routing table
 *   relay_queue_status   show offline-queued calls
 */

const http = require('http');
const https = require('https');
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { URL } = require('url');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------
const RELAY_URL = process.env.RELAY_URL;
const TOKEN = process.env.AGENT_RELAY_TOKEN;
const BRIDGE_NAME = process.env.BRIDGE_NAME || 'opencode-bridge';
const CALL_TIMEOUT_MS = parseInt(process.env.CALL_TIMEOUT_MS || '60000', 10);
const HEARTBEAT_MS = 25000;
const RECONNECT_MS = 2000;

const CFG_DIR = path.join(os.homedir(), '.config', 'opencode');
const MEM_FILE = path.join(CFG_DIR, 'fleet-memory.json');
const AUDIT_FILE = path.join(CFG_DIR, 'relay-audit.jsonl');
const QUEUE_MAX_PER_AGENT = 100;
const QUEUE_TTL_MS = 10 * 60 * 1000;

// The fleet. Override with FLEET_AGENTS="a,b,c" if your names differ.
const FLEET = (process.env.FLEET_AGENTS ||
  'agent-ubuntu,agent-kali,agent-windows,agent-android,agent-fedora')
  .split(',').map((s) => s.trim()).filter(Boolean);

// Capability routing table for relay_auto.
const CAPS = {
  'agent-kali':    ['security', 'pentest', 'nmap', 'recon', 'exploit', 'scan', 'linux'],
  'agent-ubuntu':  ['general', 'build', 'docker', 'dev', 'deploy', 'linux'],
  'agent-fedora':  ['general', 'build', 'rpm', 'dev', 'linux'],
  'agent-windows': ['powershell', 'exe', 'dotnet', 'iis', 'windows'],
  'agent-android': ['mobile', 'termux', 'apk', 'device', 'android'],
};

if (!RELAY_URL || !TOKEN) {
  process.stderr.write('WARN: RELAY_URL or AGENT_RELAY_TOKEN missing — tools will return "relay not connected" until env vars are set\n');
}

// ---------------------------------------------------------------------------
// Minimal RFC 6455 WebSocket client (client frames always masked)
// ---------------------------------------------------------------------------
class WSClient {
  constructor(url, h) {
    this.url = new URL(url);
    this.onOpen = h.onOpen || (() => {});
    this.onMessage = h.onMessage || (() => {});
    this.onClose = h.onClose || (() => {});
    this.socket = null;
    this.buf = Buffer.alloc(0);
    this.fragments = [];
  }

  connect() {
    const secure = this.url.protocol === 'wss:';
    const lib = secure ? https : http;
    const port = this.url.port || (secure ? 443 : 80);
    const p = (this.url.pathname || '/') + (this.url.search || '');
    const req = lib.request({
      host: this.url.hostname, port, path: p, method: 'GET',
      headers: {
        Connection: 'Upgrade', Upgrade: 'websocket',
        'Sec-WebSocket-Key': crypto.randomBytes(16).toString('base64'),
        'Sec-WebSocket-Version': '13', Host: this.url.host,
      },
    });
    req.on('upgrade', (res, socket, head) => {
      this.socket = socket;
      socket.on('data', (d) => this._onData(d));
      socket.on('close', () => this.onClose());
      socket.on('error', () => this.onClose());
      if (head && head.length) this._onData(head);
      this.onOpen();
    });
    req.on('error', () => this.onClose());
    req.end();
  }

  _onData(c) { this.buf = Buffer.concat([this.buf, c]); this._parse(); }

  _parse() {
    for (;;) {
      const f = this._readFrame();
      if (!f) break;
      const { fin, opcode, payload } = f;
      if (opcode === 0x8) { this.close(); return; }
      if (opcode === 0x9) { this._sendFrame(0xA, payload); continue; }
      if (opcode === 0xA) continue;
      if (opcode === 0x0) {
        this.fragments.push(payload);
        if (fin) { this._emit(Buffer.concat(this.fragments)); this.fragments = []; }
      } else {
        if (fin) this._emit(payload); else this.fragments = [payload];
      }
    }
  }

  _emit(p) { this.onMessage(p.toString('utf8')); }

  _readFrame() {
    const b = this.buf;
    if (b.length < 2) return null;
    const fin = (b[0] & 0x80) !== 0;
    const opcode = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < off + 2) return null; len = b.readUInt16BE(off); off += 2; }
    else if (len === 127) { if (b.length < off + 8) return null; len = Number(b.readBigUInt64BE(off)); off += 8; }
    let key = null;
    if (masked) { if (b.length < off + 4) return null; key = b.slice(off, off + 4); off += 4; }
    if (b.length < off + len) return null;
    let payload = b.slice(off, off + len);
    if (masked) { const o = Buffer.alloc(len); for (let i = 0; i < len; i++) o[i] = payload[i] ^ key[i & 3]; payload = o; }
    this.buf = b.slice(off + len);
    return { fin, opcode, payload };
  }

  _sendFrame(opcode, payload) {
    if (!this.socket) return;
    const len = payload.length, mask = crypto.randomBytes(4);
    let h;
    if (len < 126) { h = Buffer.alloc(2); h[1] = len; }
    else if (len < 65536) { h = Buffer.alloc(4); h[1] = 126; h.writeUInt16BE(len, 2); }
    else { h = Buffer.alloc(10); h[1] = 127; h.writeBigUInt64BE(BigInt(len), 2); }
    h[0] = 0x80 | opcode; h[1] |= 0x80;
    const m = Buffer.alloc(len);
    for (let i = 0; i < len; i++) m[i] = payload[i] ^ mask[i & 3];
    this.socket.write(Buffer.concat([h, mask, m]));
  }

  send(o) { this._sendFrame(0x1, Buffer.from(JSON.stringify(o), 'utf8')); }
  close() { try { this._sendFrame(0x8, Buffer.alloc(0)); } catch (_) {} if (this.socket) this.socket.destroy(); }
}

// ---------------------------------------------------------------------------
// Audit log
// ---------------------------------------------------------------------------
function audit(obj) {
  try { fs.appendFileSync(AUDIT_FILE, JSON.stringify(obj) + '\n'); } catch (_) {}
}

// ---------------------------------------------------------------------------
// Offline queue
// ---------------------------------------------------------------------------
const offlineQueue = new Map();

function isOfflineError(msg) {
  return /not connected|no executor|offline|unavailable|no agent|not found|no route/i.test(String(msg));
}

function enqueue(agent, tool, params) {
  if (!offlineQueue.has(agent)) offlineQueue.set(agent, []);
  const q = offlineQueue.get(agent);
  if (q.length >= QUEUE_MAX_PER_AGENT) q.shift();
  q.push({ tool, params, enqueuedAt: Date.now() });
  audit({ ts: new Date().toISOString(), event: 'queued', agent, tool });
}

function queueStatus() {
  const out = {};
  for (const [agent, q] of offlineQueue) out[agent] = q.length;
  return out;
}

// ---------------------------------------------------------------------------
// Relay connection
// ---------------------------------------------------------------------------
class Relay {
  constructor() {
    this.ws = null;
    this.pending = new Map();
    this.connected = false;
    this.heartbeat = null;
    this.online = new Set();
  }

  connect() {
    this.ws = new WSClient(RELAY_URL, {
      onOpen: () => this._onOpen(),
      onMessage: (m) => this._onMessage(m),
      onClose: () => this._onClose(),
    });
    this.ws.connect();
  }

  _onOpen() {
    this.ws.send({ type: 'join', name: BRIDGE_NAME, token: TOKEN, executor: false, capabilities: [] });
    this.connected = true;
    this.heartbeat = setInterval(() => { try { this.ws.send({ type: 'ping' }); } catch (_) {} }, HEARTBEAT_MS);
  }

  _onClose() {
    this.connected = false;
    this.online.clear();
    if (this.heartbeat) clearInterval(this.heartbeat);
    for (const [, p] of this.pending) { clearTimeout(p.timer); p.reject(new Error('relay disconnected')); }
    this.pending.clear();
    setTimeout(() => this.connect(), RECONNECT_MS);
  }

  _onMessage(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }
    if (msg.type === 'pong') return;
    if (msg.type === 'ping') { this.ws.send({ type: 'pong' }); return; }

    if (msg.type === 'presence' || msg.type === 'join') {
      const name = msg.name || msg.agent;
      if (name && msg.online !== false) { this.online.add(name); this._flush(name); }
      return;
    }
    if (msg.type === 'mcp_result') {
      const p = this.pending.get(msg.relayCallId);
      if (!p) return;
      clearTimeout(p.timer);
      this.pending.delete(msg.relayCallId);
      if (msg.error) p.reject(new Error(typeof msg.error === 'string' ? msg.error : JSON.stringify(msg.error)));
      else p.resolve(msg.result);
    }
  }

  _flush(agent) {
    const q = offlineQueue.get(agent);
    if (!q || !q.length) return;
    offlineQueue.delete(agent);
    const now = Date.now();
    for (const item of q) {
      if (now - item.enqueuedAt > QUEUE_TTL_MS) continue;
      this.call(agent, item.tool, item.params)
        .then((r) => audit({ ts: new Date().toISOString(), event: 'queue_flushed', agent, tool: item.tool, ok: true }))
        .catch((e) => audit({ ts: new Date().toISOString(), event: 'queue_flushed', agent, tool: item.tool, ok: false, error: e.message }));
    }
  }

  call(agent, tool, params) {
    return new Promise((resolve, reject) => {
      if (!this.connected) return reject(new Error('relay not connected'));
      const relayCallId = crypto.randomUUID();
      const timer = setTimeout(() => { this.pending.delete(relayCallId); reject(new Error('call timeout')); }, CALL_TIMEOUT_MS);
      this.pending.set(relayCallId, { resolve, reject, timer });
      this.ws.send({ type: 'mcp_call', relayCallId, executor: agent, method: tool, params });
    });
  }
}

const relay = new Relay();

// Lazy connect: only connect to relay when first tool call arrives.
let relayConnected = false;
async function ensureRelay() {
  if (relayConnected) return;
  if (!RELAY_URL || !TOKEN) throw new Error('RELAY_URL and AGENT_RELAY_TOKEN not set');
  relayConnected = true;
  relay.connect();
  for (let i = 0; i < 200; i++) {
    if (relay.connected) return;
    await new Promise(r => setTimeout(r, 100));
  }
  throw new Error('relay connection timeout');
}

const _origCall = relay.call.bind(relay);
relay.call = async (agent, tool, params) => {
  await ensureRelay();
  audit({ ts: new Date().toISOString(), event: 'call', agent, tool, params });
  try {
    const result = await _origCall(agent, tool, params);
    audit({ ts: new Date().toISOString(), event: 'result', agent, tool, ok: true });
    return result;
  } catch (e) {
    audit({ ts: new Date().toISOString(), event: 'result', agent, tool, ok: false, error: e.message });
    throw e;
  }
};

// ---------------------------------------------------------------------------
// Fleet helpers
// ---------------------------------------------------------------------------
async function broadcast(tool, params, agents) {
  const targets = (agents && agents.length) ? agents : FLEET;
  const settled = await Promise.allSettled(
    targets.map((a) =>
      relay.call(a, tool, params)
        .then((result) => ({ agent: a, ok: true, result }))
        .catch((e) => ({ agent: a, ok: false, error: e.message }))
    )
  );
  const out = {};
  for (const s of settled) { const v = s.value; out[v.agent] = v.ok ? v.result : { error: v.error }; }
  return out;
}

function pickAgent(hint) {
  const h = String(hint || '').toLowerCase();
  for (const agent of FLEET) {
    const tags = CAPS[agent] || [];
    if (tags.some((t) => h.includes(t))) return agent;
  }
  return FLEET[0];
}

async function relayCopy(fromAgent, fromPath, toAgent, toPath) {
  const read = await relay.call(fromAgent, 'read_file', { path: fromPath });
  const content = (typeof read === 'string') ? read : (read.content != null ? read.content : JSON.stringify(read));
  return relay.call(toAgent, 'write_file', { path: toPath, content });
}

// ---------------------------------------------------------------------------
// Fleet memory
// ---------------------------------------------------------------------------
function memLoad() { try { return JSON.parse(fs.readFileSync(MEM_FILE, 'utf8')); } catch (_) { return {}; } }
function memSave(m) { fs.writeFileSync(MEM_FILE, JSON.stringify(m, null, 2)); }
function memGet(key) { const m = memLoad(); return key in m ? m[key] : null; }
function memSet(key, value) { const m = memLoad(); m[key] = value; memSave(m); return { ok: true, key }; }
function memSearch(substr) {
  const m = memLoad(); const s = String(substr || '').toLowerCase(); const out = {};
  for (const [k, v] of Object.entries(m)) if (k.toLowerCase().includes(s)) out[k] = v;
  return out;
}

// ---------------------------------------------------------------------------
// MCP tools
// ---------------------------------------------------------------------------
const TOOLS = [
  {
    name: 'relay_execute',
    description: 'Run one tool on one agent with full access. Set queue=true to defer if the agent is offline.',
    inputSchema: {
      type: 'object',
      properties: {
        agent: { type: 'string', description: 'Target agent name' },
        tool: { type: 'string', description: 'Remote tool name' },
        params: { type: 'object', description: 'Tool parameters' },
        queue: { type: 'boolean', description: 'Queue if agent offline' },
      },
      required: ['agent', 'tool'],
    },
  },
  {
    name: 'relay_broadcast',
    description: 'Run one tool on many agents in parallel and aggregate results into a per-agent map.',
    inputSchema: {
      type: 'object',
      properties: {
        tool: { type: 'string' },
        params: { type: 'object' },
        agents: { type: 'array', items: { type: 'string' }, description: 'Omit for whole fleet' },
      },
      required: ['tool'],
    },
  },
  {
    name: 'relay_status',
    description: 'Fleet-wide inventory: system_info on every agent in one call.',
    inputSchema: { type: 'object', properties: { agents: { type: 'array', items: { type: 'string' } } } },
  },
  {
    name: 'relay_auto',
    description: 'Pick the best agent for a capability hint (e.g. "security"->kali, "windows"->windows) then run the tool there.',
    inputSchema: {
      type: 'object',
      properties: { hint: { type: 'string' }, tool: { type: 'string' }, params: { type: 'object' } },
      required: ['hint', 'tool'],
    },
  },
  {
    name: 'relay_copy',
    description: 'Copy a file from one agent to another (composes read_file + write_file through the relay).',
    inputSchema: {
      type: 'object',
      properties: {
        from_agent: { type: 'string' }, from_path: { type: 'string' },
        to_agent: { type: 'string' }, to_path: { type: 'string' },
      },
      required: ['from_agent', 'from_path', 'to_agent', 'to_path'],
    },
  },
  {
    name: 'fleet_memory_get',
    description: 'Read a value from shared fleet memory.',
    inputSchema: { type: 'object', properties: { key: { type: 'string' } }, required: ['key'] },
  },
  {
    name: 'fleet_memory_set',
    description: 'Write a value to shared fleet memory (persists across sessions).',
    inputSchema: { type: 'object', properties: { key: { type: 'string' }, value: {} }, required: ['key', 'value'] },
  },
  {
    name: 'fleet_memory_search',
    description: 'Search shared fleet memory keys by substring.',
    inputSchema: { type: 'object', properties: { substr: { type: 'string' } }, required: ['substr'] },
  },
  {
    name: 'fleet_capabilities',
    description: 'Show the capability routing table used by relay_auto.',
    inputSchema: { type: 'object', properties: {} },
  },
  {
    name: 'relay_queue_status',
    description: 'Show how many calls are queued per offline agent.',
    inputSchema: { type: 'object', properties: {} },
  },
];

// ---------------------------------------------------------------------------
// MCP stdio (JSON-RPC 2.0 + Content-Length framing)
// ---------------------------------------------------------------------------
let stdinBuf = Buffer.alloc(0);

function sendRpc(msg) {
  const body = Buffer.from(JSON.stringify(msg), 'utf8');
  process.stdout.write(`Content-Length: ${body.length}\r\n\r\n`);
  process.stdout.write(body);
}
const respond = (id, result) => sendRpc({ jsonrpc: '2.0', id, result });
const respondError = (id, code, message) => sendRpc({ jsonrpc: '2.0', id, error: { code, message } });
const toolResult = (id, obj, isError) =>
  respond(id, { content: [{ type: 'text', text: typeof obj === 'string' ? obj : JSON.stringify(obj, null, 2) }], isError: !!isError });

async function dispatch(name, args) {
  switch (name) {
    case 'relay_execute': {
      try {
        return await relay.call(args.agent, args.tool, args.params || {});
      } catch (e) {
        if (args.queue && isOfflineError(e.message)) {
          enqueue(args.agent, args.tool, args.params || {});
          return { queued: true, agent: args.agent, note: 'agent offline; will run on reconnect' };
        }
        throw e;
      }
    }
    case 'relay_broadcast': return broadcast(args.tool, args.params || {}, args.agents);
    case 'relay_status':    return broadcast('system_info', {}, args.agents);
    case 'relay_auto':      return relay.call(pickAgent(args.hint), args.tool, args.params || {});
    case 'relay_copy':      return relayCopy(args.from_agent, args.from_path, args.to_agent, args.to_path);
    case 'fleet_memory_get':     return memGet(args.key);
    case 'fleet_memory_set':     return memSet(args.key, args.value);
    case 'fleet_memory_search':  return memSearch(args.substr);
    case 'fleet_capabilities':   return { fleet: FLEET, capabilities: CAPS };
    case 'relay_queue_status':   return queueStatus();
    default: throw new Error(`unknown tool: ${name}`);
  }
}

async function handle(msg) {
  if (msg.method === 'initialize') {
    respond(msg.id, { protocolVersion: '2024-11-05', capabilities: { tools: {} }, serverInfo: { name: 'agent-relay-bridge', version: '2.0.0' } });
  } else if (msg.method === 'notifications/initialized') {
    // no-op
  } else if (msg.method === 'tools/list') {
    respond(msg.id, { tools: TOOLS });
  } else if (msg.method === 'tools/call') {
    const args = (msg.params && msg.params.arguments) || {};
    const name = msg.params && msg.params.name;
    try {
      const result = await dispatch(name, args);
      toolResult(msg.id, result);
    } catch (e) {
      toolResult(msg.id, `Error: ${e.message}`, true);
    }
  } else if (msg.id !== undefined) {
    respondError(msg.id, -32601, `Method not found: ${msg.method}`);
  }
}

process.stdin.on('data', (chunk) => {
  stdinBuf = Buffer.concat([stdinBuf, chunk]);
  for (;;) {
    const headerEnd = stdinBuf.indexOf('\r\n\r\n');
    if (headerEnd === -1) break;
    const header = stdinBuf.slice(0, headerEnd).toString('utf8');
    const m = header.match(/Content-Length:\s*(\d+)/i);
    if (!m) { stdinBuf = stdinBuf.slice(headerEnd + 4); continue; }
    const len = parseInt(m[1], 10);
    const total = headerEnd + 4 + len;
    if (stdinBuf.length < total) break;
    const body = stdinBuf.slice(headerEnd + 4, total).toString('utf8');
    stdinBuf = stdinBuf.slice(total);
    try { handle(JSON.parse(body)); } catch (_) {}
  }
});

process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));

process.stderr.write(`agent-relay-bridge ready (RELAY_URL=${RELAY_URL ? 'set' : 'unset'}, TOKEN=${TOKEN ? 'set' : 'unset'})\n`);
