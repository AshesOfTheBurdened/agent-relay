#!/usr/bin/env node
const http = require('http');
const { execSync } = require('child_process');
const { readFileSync, writeFileSync, existsSync } = require('fs');
const { readdir } = require('fs/promises');
const { resolve, isAbsolute } = require('path');
const crypto = require('crypto');

const RELAY_URL = process.env.RELAY_URL || 'ws://localhost:8080';
const AGENT_NAME = process.env.AGENT_NAME || 'opencode';
const WORKSPACE = process.env.MCP_WORKSPACE || process.env.HOME || '/home/nixuser/workspace';
const RECONNECT_DELAY = 3000;

// ── MCP Tool Handlers ─────────────────────────────────────────────

function executeCommand(args) {
  const cmd = args.command || '';
  const cwd = args.cwd || WORKSPACE;
  const timeout = Math.min(args.timeout || 30000, 60000);
  try {
    const out = execSync(cmd, { cwd, timeout, encoding: 'utf-8', maxBuffer: 10 * 1024 * 1024 });
    return { stdout: out, stderr: '', exitCode: 0 };
  } catch (e) {
    return { stdout: e.stdout || '', stderr: e.stderr || e.message, exitCode: e.status || 1 };
  }
}

function readFile(args) {
  const p = args.path || '';
  const abs = isAbsolute(p) ? p : resolve(WORKSPACE, p);
  const offset = args.offset || 0;
  const limit = args.limit || 0;
  try {
    let content = readFileSync(abs, 'utf-8');
    const lines = content.split('\n');
    const totalLines = lines.length;
    const start = Math.max(0, offset);
    const end = limit > 0 ? Math.min(start + limit, totalLines) : totalLines;
    return { content: lines.slice(start, end).join('\n'), totalLines, startLine: start, path: abs };
  } catch (e) {
    return { error: e.message, path: abs };
  }
}

function writeFile(args) {
  const p = args.path || '';
  const abs = isAbsolute(p) ? p : resolve(WORKSPACE, p);
  try {
    writeFileSync(abs, args.content || '', 'utf-8');
    return { path: abs, size: (args.content || '').length };
  } catch (e) {
    return { error: e.message, path: abs };
  }
}

function searchFiles(args) {
  const p = args.path || WORKSPACE;
  const pattern = args.pattern || '';
  const abs = isAbsolute(p) ? p : resolve(WORKSPACE, p);
  try {
    const result = execSync(`find "${abs}" -type f -name "${pattern}" 2>/dev/null | head -100`, { encoding: 'utf-8', timeout: 10000 });
    const matches = result.split('\n').filter(Boolean);
    return { matches, count: matches.length, path: abs };
  } catch (e) {
    return { matches: [], error: e.message, path: abs };
  }
}

function searchContent(args) {
  const p = args.path || WORKSPACE;
  const pattern = args.pattern || '';
  const abs = isAbsolute(p) ? p : resolve(WORKSPACE, p);
  try {
    const { execSync } = require('child_process');
    const result = execSync(`grep -rn '${pattern.replace(/'/g, "'\\''")}' "${abs}" --include="*.{js,ts,tsx,jsx,json,html,css,md}" 2>/dev/null | head -200`, { encoding: 'utf-8', timeout: 10000 });
    const matches = result.split('\n').filter(Boolean).map(line => {
      const parts = line.split(':');
      return { file: parts[0], line: parseInt(parts[1]) || 0, content: parts.slice(2).join(':') };
    });
    return { matches, count: matches.length, path: abs };
  } catch (e) {
    return { matches: [], error: e.message };
  }
}

function getEnvironment() {
  try {
    const { execSync } = require('child_process');
    const tools = ['node', 'npm', 'git', 'python3', 'curl', 'grep', 'find', 'sed', 'awk', 'gcc', 'rustc', 'cargo', 'go'];
    const available = {};
    for (const t of tools) {
      try { execSync(`which ${t} 2>/dev/null`, { timeout: 2000 }); available[t] = true; } catch { available[t] = false; }
    }
    return {
      agent: AGENT_NAME,
      workspace: WORKSPACE,
      platform: process.platform,
      arch: process.arch,
      nodeVersion: process.version,
      tools: available,
    };
  } catch (e) {
    return { agent: AGENT_NAME, workspace: WORKSPACE, error: e.message };
  }
}

const TOOLS = {
  execute_command: executeCommand,
  read_file: readFile,
  write_file: writeFile,
  search_files: searchFiles,
  search_content: searchContent,
  get_environment: getEnvironment,
};

// ── WebSocket Client ──────────────────────────────────────────────

function connect() {
  console.log(`[agent] Connecting to ${RELAY_URL} as "${AGENT_NAME}" (executor)...`);

  const protocol = RELAY_URL.startsWith('wss') ? require('tls') : require('net');
  let isWss = RELAY_URL.startsWith('wss');

  // Parse URL
  const url = new URL(RELAY_URL);
  const port = parseInt(url.port) || (isWss ? 443 : 80);

  const key = crypto.randomBytes(16).toString('base64');
  const req = http.request({
    hostname: url.hostname,
    port,
    method: 'GET',
    path: url.pathname || '/',
    headers: {
      'Upgrade': 'websocket',
      'Connection': 'Upgrade',
      'Sec-WebSocket-Key': key,
      'Sec-WebSocket-Version': '13',
    },
    rejectUnauthorized: false,
  });

  req.on('upgrade', (res, socket) => {
    console.log('[agent] Connected to relay');
    const accept = res.headers['sec-websocket-accept'];
    if (!accept) { console.error('[agent] No WebSocket accept header'); return; }

    // Send join
    sendFrame(socket, { type: 'join', name: AGENT_NAME, executor: true });

    let buf = Buffer.alloc(0);

    socket.on('data', data => {
      buf = Buffer.concat([buf, data]);
      while (buf.length >= 2) {
        const op = buf[0] & 0x0f;
        const masked = (buf[1] & 0x80) !== 0;
        let len = buf[1] & 0x7f;
        let off = 2;
        if (len === 126) { if (buf.length < 4) break; len = buf.readUInt16BE(2); off = 4; }
        if (len === 127) { if (buf.length < 10) break; len = Number(buf.readBigUInt64BE(2)); off = 10; }
        let mask = null;
        if (masked) { if (buf.length < off + 4) break; mask = buf.slice(off, off + 4); off += 4; }
        if (buf.length < off + len) break;
        let payload = buf.slice(off, off + len);
        if (mask) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
        buf = buf.slice(off + len);
        if (op === 1) handleMessage(payload.toString('utf8'), socket);
        if (op === 8) { console.log('[agent] Relay closed connection'); reconnect(); return; }
        if (op === 9) sendRaw(socket, 0x0a, payload);
      }
    });

    socket.on('close', () => {
      console.log('[agent] Disconnected from relay');
      reconnect();
    });

    socket.on('error', e => {
      console.error('[agent] Socket error:', e.message);
    });

    // Keepalive ping every 30s
    const interval = setInterval(() => {
      if (!socket.destroyed) sendRaw(socket, 0x09, Buffer.alloc(0));
      else { clearInterval(interval); }
    }, 30000);
  });

  req.on('error', e => {
    console.error('[agent] Connection error:', e.message);
    reconnect();
  });

  req.end();
}

function sendFrame(socket, msg) {
  const data = Buffer.from(JSON.stringify(msg), 'utf8');
  const h = Buffer.alloc(2);
  h[0] = 0x80 | 0x01;
  if (data.length < 126) { h[1] = data.length; socket.write(Buffer.concat([h, data])); }
  else if (data.length < 65536) { h[1] = 126; const e = Buffer.alloc(2); e.writeUInt16BE(data.length); socket.write(Buffer.concat([h, e, data])); }
  else { h[1] = 127; const e = Buffer.alloc(8); e.writeBigUInt64BE(BigInt(data.length)); socket.write(Buffer.concat([h, e, data])); }
}

function sendRaw(socket, op, payload) {
  const h = Buffer.alloc(2);
  h[0] = 0x80 | op;
  if (payload.length < 126) { h[1] = payload.length; socket.write(Buffer.concat([h, payload])); }
  else if (payload.length < 65536) { h[1] = 126; const e = Buffer.alloc(2); e.writeUInt16BE(payload.length); socket.write(Buffer.concat([h, e, payload])); }
  else { h[1] = 127; const e = Buffer.alloc(8); e.writeBigUInt64BE(BigInt(payload.length)); socket.write(Buffer.concat([h, e, payload])); }
}

function handleMessage(raw, socket) {
  try {
    const msg = JSON.parse(raw);

    if (msg.type === 'mcp_call') {
      const tool = TOOLS[msg.method];
      if (!tool) {
        sendFrame(socket, {
          type: 'mcp_result',
          callId: msg.callId,
          fromName: msg.fromName,
          error: `Tool not found: ${msg.method}`,
        });
        return;
      }

      // Execute (handle sync and async)
      const resultOrPromise = tool(msg.params || {});
      Promise.resolve(resultOrPromise).then(result => {
        sendFrame(socket, {
          type: 'mcp_result',
          callId: msg.callId,
          fromName: msg.fromName,
          result,
        });
      }).catch(err => {
        sendFrame(socket, {
          type: 'mcp_result',
          callId: msg.callId,
          fromName: msg.fromName,
          error: err.message,
        });
      });
      return;
    }

    if (msg.type === 'chat') {
      console.log(`[chat ${msg.from}] ${msg.text}`);
      return;
    }

    if (msg.type === 'status') {
      console.log(`[status] ${msg.count} agent(s) online: ${msg.agents.map(a => a.name + (a.executor ? '*' : '')).join(', ')}`);
      return;
    }

    if (msg.type === 'pong') return;
  } catch (e) {
    console.error('[agent] Parse error:', e.message);
  }
}

let reconnectTimer = null;
function reconnect() {
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(connect, RECONNECT_DELAY);
}

// ── Main ──────────────────────────────────────────────────────────

if (require.main === module) {
  connect();
  console.log(`[agent] Agent "${AGENT_NAME}" starting`);
  console.log(`[agent] Workspace: ${WORKSPACE}`);
  console.log(`[agent] Relay: ${RELAY_URL}`);
}
