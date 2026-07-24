#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { execSync, execFileSync } = require('child_process');
const config = require('./config');
const { CHANNELS, encode, decode, isBinaryFrame, HEADER_SIZE } = require('./protocol');
const { logger, child } = require('./logger');

const log = child({ agent: config.agentName });

let ws = null;
let sessionId = null;
let lastSeq = 0;
let reconnectAttempt = 0;
let reconnectTimer = null;
let heartbeatTimer = null;
let shuttingDown = false;

const MAX_FILE_SIZE = 10 * 1024 * 1024;
const MAX_OUTPUT_SIZE = 100 * 1024;
const COMMAND_TIMEOUT = 30_000;

function safePath(input) {
  const resolved = path.resolve(config.mcpWorkspace, String(input || ''));
  let real;
  try {
    real = fs.realpathSync(resolved);
  } catch {
    const dir = path.dirname(resolved);
    let realDir;
    try { realDir = fs.realpathSync(dir); } catch { realDir = dir; }
    real = path.join(realDir, path.basename(resolved));
  }
  const base = path.resolve(config.mcpWorkspace);
  if (!real.startsWith(base + path.sep) && real !== base) {
    throw new Error('Path escapes workspace');
  }
  return real;
}

const tools = {};

function registerTool(name, fn) {
  tools[name] = fn;
}

registerTool('execute_command', (args = {}) => {
  const command = typeof args.command === 'string' ? args.command : '';
  const cwd = safePath(args.cwd || '.');
  const timeout = Math.min(Math.max(1_000, Number(args.timeout) || COMMAND_TIMEOUT), 60_000);
  if (!command) return { content: [{ type: 'text', text: 'command is required' }], isError: true };
  try {
    const stdout = execSync(command, { cwd, timeout, encoding: 'utf8', maxBuffer: MAX_OUTPUT_SIZE, windowsHide: true });
    return { content: [{ type: 'text', text: stdout || '(no output)' }] };
  } catch (error) {
    const text = (error.stdout || '') + (error.stderr ? '\n' + error.stderr : '') || error.message;
    return { content: [{ type: 'text', text: text.slice(0, MAX_OUTPUT_SIZE) }], isError: true };
  }
});

registerTool('read_file', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    const stats = fs.statSync(p);
    if (stats.size > MAX_FILE_SIZE) {
      return { content: [{ type: 'text', text: `File too large (${stats.size} bytes, max ${MAX_FILE_SIZE})` }], isError: true };
    }
    const lines = fs.readFileSync(p, 'utf8').split('\n');
    const offset = Math.max(0, Number(args.offset) || 0);
    const limit = Math.max(0, Number(args.limit) || 0);
    const end = limit ? Math.min(offset + limit, lines.length) : lines.length;
    return {
      content: [{ type: 'text', text: lines.slice(offset, end).join('\n') }],
      totalLines: lines.length, startLine: offset, path: p,
    };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('write_file', (args = {}) => {
  try {
    const p = safePath(args.path || '');
    const content = typeof args.content === 'string' ? args.content : '';
    fs.mkdirSync(path.dirname(p), { recursive: true });
    fs.writeFileSync(p, content, 'utf8');
    return { content: [{ type: 'text', text: `Written ${Buffer.byteLength(content)} bytes to ${args.path}` }], size: Buffer.byteLength(content), path: args.path };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('search_files', (args = {}) => {
  try {
    const p = safePath(args.path || '.');
    const pattern = typeof args.pattern === 'string' && args.pattern ? args.pattern : '*';
    const output = execFileSync('find', [p, '-type', 'f', '-name', pattern], { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    const matches = output.split('\n').filter(Boolean).slice(0, 100);
    return { content: [{ type: 'text', text: matches.join('\n') || '(no matches)' }], count: matches.length, truncated: matches.length === 100 };
  } catch (error) {
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('search_content', (args = {}) => {
  try {
    const p = safePath(args.path || '.');
    const pattern = typeof args.pattern === 'string' ? args.pattern : '';
    if (!pattern) return { content: [{ type: 'text', text: 'pattern is required' }], isError: true };
    const output = execFileSync('grep', ['-rIn', '--include=*.js', '--include=*.ts', '--include=*.tsx', '--include=*.jsx', '--include=*.json', '--include=*.html', '--include=*.css', '--include=*.md', '--', pattern, p], { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    const matches = output.split('\n').filter(Boolean).slice(0, 200).map(line => {
      const [file, lineNumber, ...content] = line.split(':');
      return { file, line: Number(lineNumber) || 0, content: content.join(':') };
    });
    return {
      content: [{ type: 'text', text: matches.map(m => `${m.file}:${m.line}:${m.content}`).join('\n') || '(no matches)' }],
      count: matches.length, truncated: matches.length === 200,
    };
  } catch (error) {
    if (error.status === 1) return { content: [{ type: 'text', text: '(no matches)' }], count: 0 };
    return { content: [{ type: 'text', text: error.message }], isError: true };
  }
});

registerTool('get_environment', () => {
  const toolList = ['node', 'npm', 'git', 'python3', 'curl', 'grep', 'find', 'sed', 'awk', 'gcc', 'rustc', 'cargo', 'go', 'docker'];
  const available = {};
  for (const tool of toolList) {
    try { execFileSync(process.platform === 'win32' ? 'where' : 'which', [tool], { stdio: 'ignore', timeout: 2_000, windowsHide: true }); available[tool] = true; }
    catch { available[tool] = false; }
  }
  return {
    content: [{ type: 'text', text: JSON.stringify({
      agent: config.agentName, workspace: config.mcpWorkspace,
      platform: process.platform, arch: process.arch,
      nodeVersion: process.version, pid: process.pid,
      tools: available, uptime: process.uptime(),
    }, null, 2) }],
  };
});

let WebSocketLib;
try {
  WebSocketLib = require('ws');
} catch {
  WebSocketLib = null;
}

function connect() {
  if (shuttingDown) return;
  if (!WebSocketLib) {
    log.error('ws library not available — install with: npm install ws');
    return;
  }

  log.info({ url: config.relayUrl, attempt: reconnectAttempt }, 'Connecting to relay...');

  ws = new WebSocketLib(config.relayUrl, {
    maxPayload: config.maxMessageSize,
    handshakeTimeout: 10_000,
    rejectUnauthorized: process.env.RELAY_TLS_REJECT_UNAUTHORIZED !== 'false',
  });

  ws.on('open', () => {
    reconnectAttempt = 0;
    log.info('Connected!');

    sendControl({
      type: 'hello',
      name: config.agentName,
      token: config.agentToken,
      sessionId,
      lastSeq,
      executor: true,
    });

    heartbeatTimer = setInterval(() => {
      sendControl({ type: 'heartbeat', timestamp: Date.now() });
    }, config.heartbeatIntervalMs);
    heartbeatTimer.unref?.();
  });

  ws.on('message', (data, isBinary) => {
    try {
      if (isBinary && isBinaryFrame(data)) {
        const frame = decode(data);
        if (!frame) return;
        routeMessage(frame.channel, frame.msg);
      } else {
        const text = typeof data === 'string' ? data : Buffer.from(data).toString('utf8');
        let msg;
        try { msg = JSON.parse(text); } catch { return; }
        routeLegacyMessage(msg);
      }
    } catch (err) {
      log.warn({ err: err.message }, 'Error handling message');
    }
  });

  ws.on('close', (code, reason) => {
    log.warn({ code, reason: reason?.toString?.() }, 'Disconnected');
    cleanup();
    scheduleReconnect();
  });

  ws.on('error', (err) => {
    log.error({ err: err.message }, 'WebSocket error');
  });
}

function cleanup() {
  if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
}

function scheduleReconnect() {
  if (shuttingDown) return;
  const base = Math.min(1000 * Math.pow(2, reconnectAttempt), 30_000);
  const jitter = base * 0.3 * Math.random();
  const delay = Math.round(base + jitter);
  reconnectAttempt++;
  log.info({ delay, attempt: reconnectAttempt }, `Reconnecting in ${delay}ms...`);
  reconnectTimer = setTimeout(connect, delay);
  reconnectTimer.unref?.();
}

function routeMessage(channel, msg) {
  switch (channel) {
    case CHANNELS.CONTROL: handleControl(msg); break;
    case CHANNELS.CHAT: handleChat(msg); break;
    case CHANNELS.MCP: handleMcp(msg); break;
    case CHANNELS.STREAM: handleStream(msg); break;
    case CHANNELS.PRESENCE: handlePresence(msg); break;
  }
}

function routeLegacyMessage(msg) {
  switch (msg.type) {
    case 'joined': sessionId = msg.id; lastSeq = msg.seq || 0; log.info({ sessionId }, 'Registered'); break;
    case 'status': log.info(`[status] ${msg.count} online`); break;
    case 'chat': handleChat(msg); break;
    case 'mcp_call': handleMcp(msg); break;
    case 'error': log.error(`[relay] ${msg.message}`); break;
    case 'pong': break;
    case 'server.shutdown': log.warn('Server shutting down, will reconnect...'); break;
  }
}

function handleControl(msg) {
  switch (msg.type) {
    case 'joined':
      sessionId = msg.id || msg.sessionId;
      lastSeq = msg.seq || 0;
      log.info({ sessionId }, 'Registered');
      break;
    case 'welcome':
      sessionId = msg.sessionId;
      lastSeq = msg.seq || 0;
      log.info({ sessionId, agents: msg.agents }, 'Joined relay');
      break;
    case 'status':
      log.info(`[status] ${msg.count} online: ${(msg.agents || []).map(a => `${a.name}${a.executor ? '*' : ''}`).join(', ')}`);
      break;
    case 'error':
      log.error(`[relay] ${msg.code || 'error'}: ${msg.message}`);
      break;
    case 'heartbeat.ack':
      break;
    case 'server.shutdown':
      log.warn('Server shutting down...');
      break;
    default:
      if (msg.type !== 'pong' && msg.type !== 'leave') {
        log.debug({ type: msg.type }, 'Unhandled control message');
      }
  }
}

function handleChat(msg) {
  if (msg.from && msg.text) {
    log.info(`[chat ${msg.from}] ${msg.text}`);
  }
}

async function handleMcp(msg) {
  if (msg.type === 'mcp_call') {
    const method = msg.method || msg.call?.name;
    const callId = msg.callId || msg.call?.id;
    const relayCallId = msg.relayCallId;
    const params = msg.params ?? msg.call?.params ?? {};

    const tool = tools[method];
    let result;

    log.info(`[exec] ${method} (call ${callId})`);

    try {
      if (!tool) throw new Error(`Tool not found: ${method}`);
      result = await Promise.resolve(tool(params));
    } catch (error) {
      result = { content: [{ type: 'text', text: error.message }], isError: true };
    }

    const response = { type: 'mcp_result', relayCallId, callId, result };
    ws.send(JSON.stringify(response));
  }
}

function handleStream(msg) {
  log.debug({ from: msg.from, streamId: msg.streamId }, 'Stream');
}

function handlePresence(msg) {
  log.debug({ from: msg.from, status: msg.status }, 'Presence');
}

function sendControl(msg) {
  if (!ws || ws.readyState !== 1) return false;
  ws.send(JSON.stringify(msg));
  return true;
}

function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info({ signal }, 'Shutting down...');

  cleanup();
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }

  if (ws && ws.readyState === 1) {
    sendControl({ type: 'goodbye', name: config.agentName });
    ws.close(1001, 'Agent shutting down');
  }

  setTimeout(() => process.exit(0), 500).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

if (require.main === module) {
  if (!config.agentToken) log.warn('AGENT_RELAY_TOKEN not set');
  log.info(`Starting "${config.agentName}" in ${config.mcpWorkspace}`);
  connect();
}

module.exports = { tools, safePath };
