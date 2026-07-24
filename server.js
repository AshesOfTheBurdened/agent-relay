'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const config = require('./config');
const { CHANNELS, encode, decode, isBinaryFrame } = require('./protocol');
const { authenticate, authorize } = require('./auth');
const { MessageStore } = require('./store');
const { CircuitBreaker } = require('./circuit-breaker');
const { logger, child } = require('./logger');
const metrics = require('./metrics');

let WebSocketServer, WebSocket;
try {
  const ws = require('ws');
  WebSocketServer = ws.WebSocketServer;
  WebSocket = ws.WebSocket;
} catch {
  WebSocketServer = null;
}

const DEFAULT_OPTIONS = Object.freeze({
  legacyToken: config.legacyToken,
  jwtSecret: config.jwtSecret,
  maxMessageSize: config.maxMessageSize,
  mcpCallTimeoutMs: config.mcpCallTimeoutMs,
  heartbeatIntervalMs: config.heartbeatIntervalMs,
  heartbeatTimeoutMs: config.heartbeatTimeoutMs,
  maxBufferedAmount: config.maxBufferedAmount,
  messageHistorySize: config.messageHistorySize,
  redisEnabled: config.redisEnabled,
  redisUrl: config.redisUrl,
});

function createRelay(options = {}) {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  if (opts.legacyToken) config.legacyToken = opts.legacyToken;
  const agents = new Map();
  const mcpCalls = new Map();
  const deadLetters = [];
  const circuitBreaker = new CircuitBreaker();
  const messageStore = new MessageStore(opts.messageHistorySize);
  let redis = null;
  let redisSub = null;
  let accepting = true;
  let heartbeatInterval;
  let shuttingDown = false;

  async function initRedis() {
    if (!opts.redisEnabled) return;
    try {
      const Redis = require('ioredis');
      redis = new Redis(opts.redisUrl, { maxRetriesPerRequest: 3, lazyConnect: true });
      redis.on('error', (err) => logger.warn({ err: err.message }, 'Redis error'));
      await redis.connect();
      await messageStore.connectRedis(redis);
      redisSub = new Redis(opts.redisUrl, { maxRetriesPerRequest: 3, lazyConnect: true });
      redisSub.on('error', (err) => logger.warn({ err: err.message }, 'Redis sub error'));
      await redisSub.connect();
      await redisSub.subscribe('relay:broadcast');
      redisSub.on('message', (_channel, data) => {
        try { const msg = JSON.parse(data); routeToLocalAgents(msg); } catch {}
      });
      logger.info('Redis connected and pub/sub initialized');
    } catch (err) {
      logger.warn({ err: err.message }, 'Redis unavailable');
    }
  }

  const server = http.createServer((req, res) => {
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    if (url.pathname === '/health' || url.pathname === '/ready') {
      res.writeHead(accepting ? 200 : 503, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      res.end(JSON.stringify({
        status: accepting ? 'ok' : 'stopping',
        agents: agents.size, pendingMcpCalls: mcpCalls.size,
        uptime: process.uptime(), circuitBreakers: circuitBreaker.status(),
      }));
      return;
    }
    if (url.pathname === '/metrics') {
      metrics.register.metrics().then((data) => {
        res.writeHead(200, { 'Content-Type': metrics.register.contentType, 'Cache-Control': 'no-store' });
        res.end(data);
      }).catch(() => res.writeHead(500).end(''));
      return;
    }
    if (url.pathname.startsWith('/api/')) { handleAdminApi(req, res, url); return; }
    if (url.pathname === '/' || url.pathname === '/dashboard') {
      const htmlPath = path.join(__dirname, 'public', 'dashboard.html');
      try {
        const html = fs.readFileSync(htmlPath, 'utf8');
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(html);
      } catch { sendHomePage(res); }
      return;
    }
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));
  });

  function sendHomePage(res) {
    const requiresToken = Boolean(opts.legacyToken);
    const tokenScript = requiresToken ? "const token=prompt('Relay token')||'';" : "const token='';";
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(`<!doctype html><meta charset="utf-8"><title>Agent Relay</title>
<style>body{font:16px system-ui;max-width:48rem;margin:3rem auto;padding:0 1rem}pre{background:#111;color:#ddd;padding:1rem;overflow:auto}li{margin:.25rem 0}</style>
<h1>Agent Relay v2</h1><p>Connected agents: <strong id="count">0</strong></p><ul id="agents"></ul><pre id="log" aria-live="polite"></pre>
<script>${tokenScript}const proto=location.protocol==='https:'?'wss':'ws',ws=new WebSocket(proto+'://'+location.host);
const log=x=>document.querySelector('#log').textContent+=JSON.stringify(x)+'\\n';
ws.onopen=()=>ws.send(JSON.stringify({type:'join',name:'Web UI',token}));
ws.onmessage=e=>{const d=JSON.parse(e.data);if(d.type==='status'){document.querySelector('#count').textContent=d.count;const list=document.querySelector('#agents');list.replaceChildren(...d.agents.map(a=>{const item=document.createElement('li');item.textContent=a.name+(a.executor?' (executor)':'');return item}))}else log(d)};
ws.onclose=()=>log({type:'disconnected'});</script>`);
  }

  if (WebSocketServer) {
    const wss = new WebSocketServer({ server, maxPayload: opts.maxMessageSize, clientTracking: false });

    wss.on('connection', (ws, req) => {
      const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
      const log = child({ ip });
      log.info('New WebSocket connection');

      ws.remoteIp = ip;
      ws.isAlive = true;
      ws.agentName = null;
      ws.sessionId = null;
      ws.protocolVersion = 1;
      ws.connectedAt = Date.now();

      ws.on('pong', () => { ws.isAlive = true; });
      ws.on('close', () => handleClose(ws, log));
      ws.on('error', () => {});

      ws.on('message', (data, isBinary) => {
        try {
          if (isBinary && isBinaryFrame(data)) {
            ws.protocolVersion = 2;
            const frame = decode(data);
            if (!frame) { sendControl(ws, { type: 'error', message: 'Invalid binary frame' }); return; }
            routeByChannel(ws, frame.channel, frame.msg, log);
          } else {
            const text = typeof data === 'string' ? data : Buffer.from(data).toString('utf8');
            let msg;
            try { msg = JSON.parse(text); } catch { sendControl(ws, { type: 'error', message: 'Invalid JSON' }); return; }
            routeLegacy(ws, msg, log);
          }
        } catch (err) { log.warn({ err: err.message }, 'Error handling message'); }
      });
    });

    heartbeatInterval = setInterval(() => {
      for (const [name, agent] of agents) {
        if (!agent.ws.isAlive) {
          logger.warn({ agent: name }, 'Heartbeat timeout — terminating');
          agent.ws.terminate();
          continue;
        }
        agent.ws.isAlive = false;
        try { agent.ws.ping(); } catch {}
      }
    }, opts.heartbeatIntervalMs);
    heartbeatInterval.unref?.();
  }

  function routeByChannel(ws, channel, msg, log) {
    const name = ws.agentName;
    switch (channel) {
      case CHANNELS.CONTROL: handleControl(ws, msg, log); break;
      case CHANNELS.CHAT:
        if (!name) return;
        if (!authorize(agents.get(name), 'chat')) { sendControl(ws, { type: 'error', message: 'Unauthorized: chat' }); return; }
        handleChat(ws, msg, log);
        break;
      case CHANNELS.MCP: if (name) handleMcp(ws, msg, log); break;
      case CHANNELS.STREAM: if (name) handleStream(ws, msg, log); break;
      case CHANNELS.PRESENCE: if (name) handlePresence(ws, msg, log); break;
    }
  }

  function routeLegacy(ws, msg, log) {
    const name = ws.agentName;
    switch (msg.type) {
      case 'join': case 'hello': handleControl(ws, msg, log); break;
      case 'chat': if (name) handleChat(ws, msg, log); break;
      case 'mcp_call': case 'mcp_result': case 'mcp_call_ack': case 'mcp_result_ack': case 'mcp.tools':
        if (name) handleMcp(ws, msg, log); break;
      case 'ping': sendControl(ws, { type: 'pong', timestamp: new Date().toISOString() }); break;
      case 'status': sendControl(ws, statusMessage()); break;
      default: sendControl(ws, { type: 'error', message: `Unknown type: ${msg.type}` });
    }
  }

  const legacyAuth = {
    authenticate: (_token) => {
      if (!opts.legacyToken) return null;
      const bufA = Buffer.from(String(_token || ''));
      const bufB = Buffer.from(opts.legacyToken);
      if (bufA.length !== bufB.length) return null;
      return crypto.timingSafeEqual(bufA, bufB) ? { name: null, role: 'agent', permissions: ['chat', 'mcp.call', 'mcp.execute'] } : null;
    },
  };

  function handleControl(ws, msg, log) {
    switch (msg.type) {
      case 'hello': case 'join': {
        const authn = authenticate(msg.token);
        if (!authn) {
          sendControl(ws, { type: 'error', code: 'unauthorized', message: 'Authentication failed' });
          ws.close(4001, 'Auth failed');
          return;
        }
        const name = authn.name || msg.name;
        if (!name || typeof name !== 'string' || name.length > 80) {
          sendControl(ws, { type: 'error', message: 'Valid agent name required' });
          ws.close(4002, 'No name'); return;
        }
        if (agents.has(name)) {
          sendControl(ws, { type: 'error', message: `Name "${name}" already connected` });
          ws.close(4003, 'Duplicate name'); return;
        }
        const sessionId = msg.sessionId || crypto.randomUUID();
        ws.agentName = name; ws.sessionId = sessionId; ws.executor = Boolean(msg.executor);
          agents.set(name, {
            ws, role: authn.role, permissions: authn.permissions, executor: ws.executor,
            sessionId, lastSeq: msg.lastSeq || 0, isAlive: true, connectedAt: Date.now(), ip: ws.remoteIp,
          });
        metrics.activeConnections.inc();
        log = child({ agent: name, sessionId });
        log.info({ role: authn.role, executor: ws.executor }, 'Agent joined');
        sendControl(ws, {
          type: 'joined', name, id: sessionId, sessionId,
          seq: messageStore.currentSeq(), executor: ws.executor,
          agents: [...agents.keys()], version: 2,
        });
        if (msg.lastSeq > 0) {
          const missed = messageStore.getAfter(msg.lastSeq);
          if (missed.length > 0) {
            log.info({ count: missed.length, fromSeq: msg.lastSeq }, 'Replaying missed messages');
            for (const entry of missed) sendToAgent(name, entry.channel, entry.msg);
          }
          metrics.reconnectTotal.inc();
        }
        broadcast(CHANNELS.CONTROL, { type: 'join', id: sessionId, name, executor: ws.executor, agents: agents.size }, name);
        broadcastStatus(); return;
      }
      case 'heartbeat': {
        const a = ws.agentName ? agents.get(ws.agentName) : null;
        if (a) a.lastHeartbeat = Date.now();
        if (ws.protocolVersion === 2) ws.send(encode(CHANNELS.CONTROL, { type: 'heartbeat.ack', timestamp: Date.now() }));
        else ws.send(JSON.stringify({ type: 'pong', timestamp: new Date().toISOString() }));
        break;
      }
      case 'goodbye': log.info('Agent sent goodbye'); break;
      case 'status': sendControl(ws, statusMessage()); break;
      case 'ack': break;
      default: sendControl(ws, { type: 'error', message: `Unknown control: ${msg.type}` });
    }
  }

  function handleChat(ws, msg, log) {
    const name = ws.agentName;
    if (!agents.has(name)) return;
    const chatMsg = { type: 'chat', from: name, text: String(msg.text || '').slice(0, 10_000), timestamp: new Date().toISOString(), to: msg.to || null };
    metrics.messagesTotal.inc({ channel: 'chat', type: 'chat' });
    messageStore.push(CHANNELS.CHAT, chatMsg);
    if (chatMsg.to) sendToAgent(chatMsg.to, CHANNELS.CHAT, chatMsg);
    else broadcast(CHANNELS.CHAT, chatMsg, name);
    if (redis) redis.publish('relay:broadcast', JSON.stringify({ channel: CHANNELS.CHAT, msg: chatMsg, exclude: name })).catch(() => {});
  }

  function handleMcp(ws, msg, log) {
    const name = ws.agentName;
    const agent = agents.get(name);
    if (!agent) return;
    const legacy = ws.protocolVersion === 1;

    switch (msg.type) {
      case 'mcp_call': case 'mcp.call': {
        const method = msg.method || msg.call?.name;
        const target = msg.target || msg.executor || msg.to;
        const targetId = msg.targetId || null;
        const callId = msg.callId || msg.call?.id;
        const relayCallId = msg.relayCallId || crypto.randomUUID();
        const params = msg.params ?? msg.call?.params ?? {};
        if (!authorize(agent, 'mcp.call')) { sendMcpResult(ws, { relayCallId, callId, error: 'Unauthorized: mcp.call' }, legacy); return; }
        let destination;
        if (targetId) { const info = agents.get(targetId); destination = info && info.executor ? { id: targetId, info } : null; }
        else if (target) { const candidates = [...agents.entries()].filter(([n, a]) => a.executor && n === target); if (candidates.length === 1) destination = { id: candidates[0][0], info: candidates[0][1] }; }
        if (!destination) { sendMcpResult(ws, { relayCallId, callId, error: 'Target executor unavailable' }, legacy); return; }
        if (!circuitBreaker.canRequest(destination.id)) { metrics.mcpCallErrors.inc({ reason: 'circuit_open' }); sendMcpResult(ws, { relayCallId, callId, error: `Circuit breaker open for ${target}` }, legacy); return; }
        const startTime = Date.now();
        const timer = setTimeout(() => {
          logger.warn({ relayCallId, executor: destination.id }, 'MCP call timed out');
          circuitBreaker.recordFailure(destination.id);
          metrics.mcpCallErrors.inc({ reason: 'timeout' });
          sendMcpResult(ws, { relayCallId, callId, error: `Call timed out after ${opts.mcpCallTimeoutMs}ms` }, legacy);
          mcpCalls.delete(relayCallId); metrics.mcpCallsPending.dec();
        }, opts.mcpCallTimeoutMs);
        mcpCalls.set(relayCallId, { callerId: name, executorId: destination.id, callId, timer, startTime, relayCallId });
        metrics.mcpCallsPending.inc();
        const forwarded = sendToAgent(destination.id, CHANNELS.MCP, { type: 'mcp_call', relayCallId, callId, method, params, fromName: name, fromId: ws.sessionId });
        if (!forwarded) {
          clearTimeout(timer); mcpCalls.delete(relayCallId); metrics.mcpCallsPending.dec();
          circuitBreaker.recordFailure(destination.id);
          sendMcpResult(ws, { relayCallId, callId, error: `Executor ${target} not connected` }, legacy);
        } else if (legacy) ws.send(JSON.stringify({ type: 'mcp_call_ack', callId, relayCallId, delivered: true }));
        break;
      }
      case 'mcp_result': case 'mcp.result': {
        const rid = msg.relayCallId;
        const entry = mcpCalls.get(rid);
        if (!entry) return;
        clearTimeout(entry.timer); mcpCalls.delete(rid); metrics.mcpCallsPending.dec();
        metrics.mcpCallDuration.observe((Date.now() - entry.startTime) / 1000);
        if (msg.result?.isError || msg.error) { circuitBreaker.recordFailure(entry.executorId); metrics.mcpCallErrors.inc({ reason: 'tool_error' }); }
        else { circuitBreaker.recordSuccess(entry.executorId); }
        const caller = agents.get(entry.callerId);
        if (caller) {
          const resultMsg = { type: 'mcp_result', callId: entry.callId, relayCallId: rid, result: msg.result, error: msg.error };
          if (caller.ws.protocolVersion === 2) caller.ws.send(encode(CHANNELS.MCP, resultMsg));
          else caller.ws.send(JSON.stringify(resultMsg));
        }
        ws.send(JSON.stringify({ type: 'mcp_result_ack', callId: entry.callId, relayCallId: rid, delivered: Boolean(caller) }));
        break;
      }
      case 'mcp.tools': broadcastMCP(CHANNELS.MCP, { type: 'mcp.tools', from: name, tools: msg.tools || [] }, name); break;
    }
  }

  function handleStream(ws, msg) { if (msg.to) sendToAgent(msg.to, CHANNELS.STREAM, { type: 'stream.chunk', from: ws.agentName, streamId: msg.streamId, chunk: msg.chunk, done: msg.done || false }); }
  function handlePresence(ws, msg) { broadcast(CHANNELS.PRESENCE, { type: 'presence', from: ws.agentName, status: msg.status, metadata: msg.metadata, timestamp: Date.now() }, ws.agentName); }

  function handleClose(ws, log) {
    const name = ws.agentName;
    if (!name) return;
    agents.delete(name); metrics.activeConnections.dec(); log.info('Agent disconnected');
    for (const [relayCallId, entry] of mcpCalls) {
      if (entry.callerId === name || entry.executorId === name) {
        clearTimeout(entry.timer); mcpCalls.delete(relayCallId); metrics.mcpCallsPending.dec();
        if (entry.callerId !== name) {
          const caller = agents.get(entry.callerId);
          if (caller) sendToAgent(entry.callerId, CHANNELS.MCP, { type: 'mcp_result', callId: entry.callId, relayCallId, result: { isError: true, content: [{ type: 'text', text: `Executor ${name} disconnected` }] } });
        }
      }
    }
    broadcast(CHANNELS.CONTROL, { type: 'leave', id: ws.sessionId, name, agents: agents.size }, name);
    broadcastStatus();
  }

  function sendControl(ws, msg) { if (ws.protocolVersion === 2) ws.send(encode(CHANNELS.CONTROL, msg)); else ws.send(JSON.stringify(msg)); }
  function sendMcpResult(ws, msg, legacy) { ws.send(legacy ? JSON.stringify({ type: 'mcp_result', ...msg }) : encode(CHANNELS.MCP, { type: 'mcp.result', ...msg })); }

  function sendToAgent(name, channel, msg) {
    const agent = agents.get(name);
    if (!agent || agent.ws.readyState !== 1) return false;
    if (agent.ws.bufferedAmount > opts.maxBufferedAmount) {
      logger.warn({ agent: name, buffered: agent.ws.bufferedAmount }, 'Agent backpressure');
      deadLetters.push({ msg, reason: 'backpressure', agent: name, timestamp: Date.now() });
      metrics.deadLetters.inc({ reason: 'backpressure' }); return false;
    }
    if (agent.ws.protocolVersion === 2) agent.ws.send(encode(channel, msg));
    else agent.ws.send(JSON.stringify(msg));
    return true;
  }

  function broadcast(channel, msg, excludeName) { for (const [name, agent] of agents) { if (name === excludeName) continue; sendToAgent(name, channel, msg); } }
  function broadcastMCP(channel, msg, excludeName) { for (const [name, agent] of agents) { if (name === excludeName) continue; if (agent.ws.protocolVersion === 2) agent.ws.send(encode(channel, msg)); else agent.ws.send(JSON.stringify(msg)); } }
  function broadcastStatus() { broadcast(CHANNELS.CONTROL, statusMessage()); }

  function statusMessage() {
    return { type: 'status', agents: [...agents.entries()].map(([n, a]) => ({ id: a.sessionId, name: n, executor: a.executor, connected: new Date(a.connectedAt).toISOString() })).sort((a, b) => a.name.localeCompare(b.name)), count: agents.size, timestamp: new Date().toISOString() };
  }

  function routeToLocalAgents(data) { const { channel, msg, exclude } = data; for (const [name] of agents) { if (name === exclude) continue; sendToAgent(name, channel, msg); } }

  function handleAdminApi(req, res, url) {
    const authHeader = req.headers.authorization;
    if (authHeader !== `Bearer ${opts.jwtSecret}`) { res.writeHead(401, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ error: 'Unauthorized' })); return; }
    res.setHeader('Content-Type', 'application/json');
    switch (url.pathname) {
      case '/api/admin/agents': res.end(JSON.stringify({ agents: [...agents.entries()].map(([n, a]) => ({ name: n, role: a.role, executor: a.executor, sessionId: a.sessionId, connectedAt: new Date(a.connectedAt).toISOString(), ip: a.ip })) })); break;
      case '/api/admin/calls': res.end(JSON.stringify({ pending: [...mcpCalls.entries()].map(([id, e]) => ({ relayCallId: id, caller: e.callerId, executor: e.executorId, elapsed: Date.now() - e.startTime })) })); break;
      case '/api/admin/deadletters': res.end(JSON.stringify({ deadLetters: deadLetters.slice(-100) })); break;
      case '/api/admin/circuits': res.end(JSON.stringify(circuitBreaker.status())); break;
      case '/api/admin/broadcast': { let body = ''; req.on('data', (c) => { body += c; }); req.on('end', () => { try { const { text } = JSON.parse(body); const msg = { type: 'chat', from: 'system', text, timestamp: new Date().toISOString() }; broadcast(CHANNELS.CHAT, msg); if (redis) redis.publish('relay:broadcast', JSON.stringify({ channel: CHANNELS.CHAT, msg })).catch(() => {}); res.end(JSON.stringify({ ok: true })); } catch { res.writeHead(400); res.end(JSON.stringify({ error: 'Invalid JSON' })); } }); break; }
      default: res.writeHead(404); res.end(JSON.stringify({ error: 'Not found' }));
    }
  }

  async function stop() {
    if (shuttingDown) return; shuttingDown = true; accepting = false;
    logger.info('Shutting down gracefully...');
    if (heartbeatInterval) clearInterval(heartbeatInterval);
    for (const [, agent] of agents) { try { if (agent.ws.readyState === 1) { agent.ws.send(JSON.stringify({ type: 'server.shutdown' })); agent.ws.close(1001); } } catch {} }
    agents.clear();
    await new Promise((resolve) => { const to = setTimeout(resolve, 5000); if (WebSocketServer && server._wsServer) server._wsServer.close(() => { clearTimeout(to); resolve(); }); else { clearTimeout(to); resolve(); } });
    for (const [, entry] of mcpCalls) clearTimeout(entry.timer);
    mcpCalls.clear();
    if (redis) { try { await redis.quit(); } catch {} }
    if (redisSub) { try { await redisSub.quit(); } catch {} }
    await messageStore.close();
    await new Promise((resolve) => server.close(resolve));
    logger.info('Server closed');
  }

  const relay = { server, stop, agents, pendingCalls: mcpCalls, deadLetters, messageStore, circuitBreaker, config: opts, stats: { connections: 0, joins: 0, messages: 0, rejected: 0, mcpCalls: 0, mcpTimeouts: 0 } };

  return relay;
}

if (require.main === module) {
  if (!config.legacyToken && config.env === 'production') logger.warn('RELAY_TOKEN not set — authentication DISABLED in production!');
  const relay = createRelay();
  relay.server.listen(config.port, config.host, () => {
    logger.info({ port: config.port, host: config.host, env: config.env, ws: WebSocketServer ? 'ws@latest' : 'unavailable' }, `Relay v2 listening on ${config.host}:${config.port}`);
  });
  const shutdown = (signal) => { logger.info({ signal }, 'Received shutdown signal'); relay.stop().then(() => process.exit(0)).catch((err) => { logger.error({ err: err.message }); process.exit(1); }); };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

module.exports = { createRelay };
