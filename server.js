'use strict';

const http = require('http');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const { WebSocketConnection, acceptKey, isValidClientKey, MAX_MESSAGE_BYTES } = require('./websocket');

const DEFAULTS = Object.freeze({
  port: Number(process.env.PORT) || 8080,
  token: process.env.AGENT_RELAY_TOKEN || '',
  maxMessageBytes: Number(process.env.MAX_MESSAGE_BYTES) || MAX_MESSAGE_BYTES,
  heartbeatIntervalMs: Number(process.env.HEARTBEAT_INTERVAL_MS) || 25_000,
  heartbeatTimeoutMs: Number(process.env.HEARTBEAT_TIMEOUT_MS) || 75_000,
  joinTimeoutMs: Number(process.env.JOIN_TIMEOUT_MS) || 15_000,
  callTimeoutMs: Number(process.env.CALL_TIMEOUT_MS) || 60_000,
  rateLimitMaxPerWindow: Number(process.env.RATE_LIMIT_MAX) || 100,
  rateLimitWindowMs: Number(process.env.RATE_LIMIT_WINDOW_MS) || 1_000,
});

function sendJson(ws, message) {
  try { return ws.send(JSON.stringify(message)); } catch { return false; }
}

function validName(value) {
  if (typeof value !== 'string') return null;
  const name = value.trim();
  return name && name.length <= 80 && !/[\u0000-\u001f\u007f]/.test(name) ? name : null;
}

function validShortString(value, maximum = 128) {
  return typeof value === 'string' && value.length > 0 && value.length <= maximum ? value : null;
}

function tokenMatches(candidate, expected) {
  if (!expected) return true;
  if (typeof candidate !== 'string') return false;
  const supplied = Buffer.from(candidate);
  const required = Buffer.from(expected);
  return supplied.length === required.length && crypto.timingSafeEqual(supplied, required);
}

class WebSocketServer extends EventEmitter {
  constructor(server, options) {
    super();
    this.closing = false;
    this.connections = new Set();
    server.on('upgrade', (req, socket, head) => {
      if (this.closing) return rejectUpgrade(socket, 503, 'Server is shutting down');
      const key = req.headers['sec-websocket-key'];
      if (req.method !== 'GET' || String(req.headers.upgrade || '').toLowerCase() !== 'websocket' ||
          req.headers['sec-websocket-version'] !== '13' || !isValidClientKey(key)) {
        return rejectUpgrade(socket, 400, 'Invalid WebSocket upgrade');
      }
      socket.write([
        'HTTP/1.1 101 Switching Protocols',
        'Upgrade: websocket',
        'Connection: Upgrade',
        `Sec-WebSocket-Accept: ${acceptKey(key)}`,
        '',
        '',
      ].join('\r\n'));
      const connection = new WebSocketConnection(socket, {
        initialData: head,
        requireMasked: true,
        maxMessageBytes: options.maxMessageBytes,
      });
      this.connections.add(connection);
      connection.once('close', () => this.connections.delete(connection));
      this.emit('connection', connection, req);
    });
  }

  closeAll(code = 1001, reason = 'Server shutting down') {
    this.closing = true;
    for (const connection of this.connections) {
      connection.close(code, reason);
      const forceClose = setTimeout(() => {
        if (!connection.closed) connection.terminate();
      }, 3_000);
      forceClose.unref?.();
    }
  }
}

function rejectUpgrade(socket, status, message) {
  socket.write(`HTTP/1.1 ${status} ${message}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
  socket.destroy();
}

function createRelay(overrides = {}) {
  const config = { ...DEFAULTS, ...overrides };
  const agents = new Map();
  const pendingCalls = new Map();
  const startedAt = Date.now();
  const stats = { connections: 0, joins: 0, messages: 0, rejected: 0, mcpCalls: 0, mcpTimeouts: 0 };
  const rateLimits = new Map();
  let accepting = true;

  function checkRate(agentId) {
    if (!config.rateLimitMaxPerWindow) return true;
    const now = Date.now();
    let timestamps = rateLimits.get(agentId);
    if (!timestamps) { rateLimits.set(agentId, [now]); return true; }
    const cutoff = now - config.rateLimitWindowMs;
    timestamps = timestamps.filter(t => t > cutoff);
    if (timestamps.length >= config.rateLimitMaxPerWindow) { rateLimits.set(agentId, timestamps); return false; }
    timestamps.push(now);
    rateLimits.set(agentId, timestamps);
    return true;
  }

  const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    res.setHeader('Access-Control-Allow-Origin', '*');
    if (req.method !== 'GET') return respond(res, 405, { error: 'Method not allowed' });
    if (url.pathname === '/health') {
      return respond(res, accepting ? 200 : 503, {
        status: accepting ? 'ok' : 'stopping',
        agents: agents.size,
        pendingCalls: pendingCalls.size,
        uptimeSeconds: Math.floor((Date.now() - startedAt) / 1000),
      });
    }
    if (url.pathname === '/ready') return respond(res, accepting ? 200 : 503, { ready: accepting });
    if (url.pathname === '/metrics') return metrics(res, agents, pendingCalls, stats, startedAt);
    if (url.pathname === '/') return home(res, Boolean(config.token));
    respond(res, 404, { error: 'Not found' });
  });

  const wss = new WebSocketServer(server, config);

  const status = () => ({
    type: 'status',
    agents: [...agents.entries()].map(([id, info]) => ({
      id, name: info.name, executor: info.executor, connected: info.connected,
    })).sort((a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id)),
    count: agents.size,
  });

  const broadcast = (message, exceptId = null) => {
    for (const [id, info] of agents) if (id !== exceptId) sendJson(info.ws, message);
  };
  const broadcastStatus = () => broadcast(status());

  function forgetCall(relayCallId, error) {
    const call = pendingCalls.get(relayCallId);
    if (!call) return;
    pendingCalls.delete(relayCallId);
    clearTimeout(call.timer);
    if (error) {
      const caller = agents.get(call.callerId);
      if (caller) sendJson(caller.ws, { type: 'mcp_result', callId: call.callId, error });
    }
  }

  function removeAgent(id) {
    const info = agents.get(id);
    if (!info) return;
    agents.delete(id);
    rateLimits.delete(id);
    for (const [relayCallId, call] of pendingCalls) {
      if (call.callerId === id) forgetCall(relayCallId);
      else if (call.executorId === id) forgetCall(relayCallId, 'Executor disconnected before responding');
    }
    broadcast({ type: 'leave', id, name: info.name, agents: agents.size });
    broadcastStatus();
    console.log(`[relay] ${info.name} (${id}) disconnected; ${agents.size} active`);
  }

  function resolveExecutor(target, targetId) {
    if (targetId) {
      const info = agents.get(targetId);
      return info && info.executor ? { id: targetId, info } : { error: 'Target executor is unavailable' };
    }
    const candidates = [...agents.entries()].filter(([, info]) => info.executor && info.name === target);
    if (candidates.length === 1) return { id: candidates[0][0], info: candidates[0][1] };
    if (candidates.length > 1) return { error: 'Target name is ambiguous; use targetId', candidates: candidates.map(([id]) => id) };
    return { error: 'Target executor is unavailable' };
  }

  wss.on('connection', ws => {
    const id = crypto.randomUUID();
    let joined = false;
    let joinTimer = setTimeout(() => {
      if (!joined) ws.close(1008, 'Join required');
    }, config.joinTimeoutMs);
    joinTimer.unref?.();
    stats.connections++;
    console.log(`[relay] connection opened: ${id}`);

    const reject = (code, message, close = false) => {
      stats.rejected++;
      sendJson(ws, { type: 'error', code, message });
      if (close) ws.close(1008, message);
    };

    ws.on('socketError', error => console.warn(`[relay] socket ${id}: ${error.message}`));
    ws.on('pong', () => { const info = agents.get(id); if (info) info.lastPong = Date.now(); });
    ws.on('ping', () => { const info = agents.get(id); if (info) info.lastPong = Date.now(); });
    ws.on('binary', () => reject('unsupported_frame', 'Binary messages are not supported'));
    ws.on('message', raw => {
      stats.messages++;
      let message;
      try { message = JSON.parse(raw); } catch { return reject('invalid_json', 'Message must be valid JSON'); }
      if (!message || typeof message !== 'object' || Array.isArray(message) || typeof message.type !== 'string') {
        return reject('invalid_message', 'Message must be an object with a type');
      }

      if (message.type === 'join') {
        if (joined) return reject('already_joined', 'Connection is already registered');
        const name = validName(message.name);
        if (!name) return reject('invalid_name', 'name must be 1-80 printable characters');
        if (!tokenMatches(message.token, config.token)) return reject('unauthorized', 'Invalid relay token', true);
        if ([...agents.values()].some(a => a.name === name)) return reject('name_taken', 'Name is already in use', true);
        joined = true;
        clearTimeout(joinTimer);
        const info = { ws, name, executor: Boolean(message.executor), connected: new Date().toISOString(), lastPong: Date.now() };
        agents.set(id, info);
        stats.joins++;
        sendJson(ws, { type: 'joined', id, name, executor: info.executor, heartbeatIntervalMs: config.heartbeatIntervalMs });
        broadcast({ type: 'join', id, name, executor: info.executor, agents: agents.size }, id);
        broadcastStatus();
        console.log(`[relay] ${name} (${id}) joined${info.executor ? ' as executor' : ''}; ${agents.size} active`);
        return;
      }

      if (!joined) return reject('join_required', 'Send join before other messages');
      const sender = agents.get(id);
      if (!sender) return;
      sender.lastPong = Date.now();
      if (!checkRate(id)) return reject('rate_limited', 'Too many messages — slow down');

      if (message.type === 'status') return sendJson(ws, status());
      if (message.type === 'ping') return sendJson(ws, { type: 'pong', timestamp: new Date().toISOString() });
      if (message.type === 'chat') {
        if (typeof message.text !== 'string' || !message.text.length || Buffer.byteLength(message.text) > 64 * 1024) {
          return reject('invalid_chat', 'text must be a non-empty string up to 64 KiB');
        }
        return broadcast({ type: 'chat', from: sender.name, fromId: id, text: message.text, timestamp: new Date().toISOString() }, id);
      }
      if (message.type === 'mcp_call') {
        const callId = validShortString(message.callId);
        const method = validShortString(message.method);
        const target = validName(message.target || 'opencode');
        const targetId = message.targetId === undefined ? null : validShortString(message.targetId, 128);
        if (!callId || !method || (!target && !targetId)) return reject('invalid_call', 'callId, method, and target or targetId are required');
        const destination = resolveExecutor(target, targetId);
        if (destination.error) {
          return sendJson(ws, { type: 'mcp_call_ack', callId, delivered: false, target: target || targetId, error: destination.error, candidates: destination.candidates });
        }
        const relayCallId = crypto.randomUUID();
        const timer = setTimeout(() => {
          stats.mcpTimeouts++;
          forgetCall(relayCallId, `Call timed out after ${config.callTimeoutMs}ms`);
        }, config.callTimeoutMs);
        timer.unref?.();
        pendingCalls.set(relayCallId, { callerId: id, executorId: destination.id, callId, timer });
        stats.mcpCalls++;
        const delivered = sendJson(destination.info.ws, {
          type: 'mcp_call', relayCallId, callId, method, params: message.params ?? {},
          fromName: sender.name, fromId: id, replyTo: id,
        });
        if (!delivered) forgetCall(relayCallId, 'Target executor is unavailable');
        return sendJson(ws, { type: 'mcp_call_ack', callId, relayCallId, delivered, target: destination.info.name, targetId: destination.id });
      }
      if (message.type === 'mcp_result') {
        let relayCallId = validShortString(message.relayCallId, 128);
        if (!relayCallId && validShortString(message.callId)) {
          const matches = [...pendingCalls.entries()].filter(([, call]) => call.executorId === id && call.callId === message.callId);
          if (matches.length === 1) relayCallId = matches[0][0];
        }
        const call = relayCallId && pendingCalls.get(relayCallId);
        if (!call || call.executorId !== id) return reject('unknown_call', 'No pending call matches this result');
        pendingCalls.delete(relayCallId);
        clearTimeout(call.timer);
        const caller = agents.get(call.callerId);
        const delivered = Boolean(caller && sendJson(caller.ws, {
          type: 'mcp_result', callId: call.callId, relayCallId,
          result: message.result, error: message.error,
        }));
        return sendJson(ws, { type: 'mcp_result_ack', callId: call.callId, relayCallId, delivered });
      }
      reject('unsupported_type', `Unsupported message type: ${message.type}`);
    });

    ws.on('close', () => {
      clearTimeout(joinTimer);
      if (joined) removeAgent(id);
      else console.log(`[relay] unregistered connection closed: ${id}`);
    });
  });

  const heartbeat = setInterval(() => {
    const now = Date.now();
    for (const [, info] of agents) {
      if (now - info.lastPong > config.heartbeatTimeoutMs) {
        console.warn(`[relay] evicting stale agent ${info.name}`);
        info.ws.close(1001, 'Heartbeat timeout');
      } else info.ws.ping();
    }
  }, config.heartbeatIntervalMs);
  heartbeat.unref?.();

  async function stop() {
    if (!accepting) return;
    accepting = false;
    wss.closeAll();
    clearInterval(heartbeat);
    for (const relayCallId of pendingCalls.keys()) forgetCall(relayCallId, 'Relay is shutting down');
    await new Promise(resolve => server.close(resolve));
  }

  return { server, stop, agents, pendingCalls, stats, config };
}

function respond(res, status, payload) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(payload));
}

function metrics(res, agents, pendingCalls, stats, startedAt) {
  const seconds = Math.floor((Date.now() - startedAt) / 1000);
  res.writeHead(200, { 'Content-Type': 'text/plain; version=0.0.4; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end([
    '# HELP agent_relay_connected_agents Current registered WebSocket agents.',
    '# TYPE agent_relay_connected_agents gauge', `agent_relay_connected_agents ${agents.size}`,
    '# TYPE agent_relay_pending_calls gauge', `agent_relay_pending_calls ${pendingCalls.size}`,
    '# TYPE agent_relay_connections_total counter', `agent_relay_connections_total ${stats.connections}`,
    '# TYPE agent_relay_messages_total counter', `agent_relay_messages_total ${stats.messages}`,
    '# TYPE agent_relay_mcp_timeouts_total counter', `agent_relay_mcp_timeouts_total ${stats.mcpTimeouts}`,
    '# TYPE process_uptime_seconds gauge', `process_uptime_seconds ${seconds}`,
    '',
  ].join('\n'));
}

function home(res, requiresToken) {
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(`<!doctype html><meta charset="utf-8"><title>Agent Relay</title>
<style>body{font:16px system-ui;max-width:48rem;margin:3rem auto;padding:0 1rem}pre{background:#111;color:#ddd;padding:1rem;overflow:auto}li{margin:.25rem 0}</style>
<h1>Agent Relay</h1><p>Connected agents: <strong id="count">0</strong></p><ul id="agents"></ul><pre id="log" aria-live="polite"></pre>
<script>const token=${requiresToken ? "prompt('Relay token')||''" : "''"},proto=location.protocol==='https:'?'wss':'ws',ws=new WebSocket(proto+'://'+location.host);
const log=x=>document.querySelector('#log').textContent+=JSON.stringify(x)+'\\n';
ws.onopen=()=>ws.send(JSON.stringify({type:'join',name:'Web UI',token}));
ws.onmessage=e=>{const d=JSON.parse(e.data);if(d.type==='status'){document.querySelector('#count').textContent=d.count;const list=document.querySelector('#agents');list.replaceChildren(...d.agents.map(a=>{const item=document.createElement('li');item.textContent=a.name+(a.executor?' (executor)':'');return item}))}else log(d)};
ws.onclose=()=>log({type:'disconnected'});</script>`);
}

if (require.main === module) {
  const relay = createRelay();
  relay.server.listen(relay.config.port, () => {
    const auth = relay.config.token ? 'enabled' : 'DISABLED (development only)';
    console.log(`[relay] listening on :${relay.config.port}; authentication ${auth}`);
  });
  let stopping = false;
  const shutdown = signal => {
    if (stopping) return;
    stopping = true;
    console.log(`[relay] received ${signal}; draining connections`);
    const force = setTimeout(() => process.exit(1), 8_000);
    force.unref();
    relay.stop().then(() => process.exit(0)).catch(error => { console.error(error); process.exit(1); });
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

module.exports = { createRelay, DEFAULTS };
