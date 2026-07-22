#!/usr/bin/env node
'use strict';

const http = require('http');
const https = require('https');
const crypto = require('crypto');
const { execSync, execFileSync } = require('child_process');
const { readFileSync, writeFileSync } = require('fs');
const { resolve, relative, isAbsolute, sep } = require('path');
const { WebSocketConnection, acceptKey } = require('./websocket');

const RELAY_URL = process.env.RELAY_URL || 'ws://localhost:8080';
const AGENT_NAME = process.env.AGENT_NAME || 'opencode';
const RELAY_TOKEN = process.env.AGENT_RELAY_TOKEN || '';
const WORKSPACE = resolve(process.env.MCP_WORKSPACE || process.cwd());
const RECONNECT_INITIAL_MS = Math.max(250, Number(process.env.RECONNECT_INITIAL_MS) || 1_000);
const RECONNECT_MAX_MS = Math.max(RECONNECT_INITIAL_MS, Number(process.env.RECONNECT_MAX_MS) || 30_000);
const CONNECTION_TIMEOUT_MS = Math.max(1_000, Number(process.env.CONNECTION_TIMEOUT_MS) || 15_000);
const HEARTBEAT_INTERVAL_MS = Math.max(5_000, Number(process.env.AGENT_HEARTBEAT_INTERVAL_MS) || 20_000);

function workspacePath(candidate = '.') {
  const absolute = resolve(WORKSPACE, candidate);
  const pathFromWorkspace = relative(WORKSPACE, absolute);
  if (pathFromWorkspace === '' || (!pathFromWorkspace.startsWith(`..${sep}`) && pathFromWorkspace !== '..' && !isAbsolute(pathFromWorkspace))) return absolute;
  throw new Error('Path must stay inside MCP_WORKSPACE');
}

// These handlers intentionally expose a local execution surface. Deploy this agent only with a relay token
// and a workspace/account that is scoped to the work you want a remote agent to perform.
function executeCommand(args = {}) {
  const command = typeof args.command === 'string' ? args.command : '';
  const cwd = workspacePath(args.cwd || '.');
  const timeout = Math.min(Math.max(1_000, Number(args.timeout) || 30_000), 60_000);
  if (!command) return { stdout: '', stderr: 'command is required', exitCode: 2 };
  try {
    const stdout = execSync(command, { cwd, timeout, encoding: 'utf8', maxBuffer: 10 * 1024 * 1024, windowsHide: true });
    return { stdout, stderr: '', exitCode: 0 };
  } catch (error) {
    return { stdout: error.stdout || '', stderr: error.stderr || error.message, exitCode: Number.isInteger(error.status) ? error.status : 1 };
  }
}

function readFile(args = {}) {
  try {
    const path = workspacePath(args.path || '');
    const lines = readFileSync(path, 'utf8').split('\n');
    const offset = Math.max(0, Number(args.offset) || 0);
    const limit = Math.max(0, Number(args.limit) || 0);
    const end = limit ? Math.min(offset + limit, lines.length) : lines.length;
    return { content: lines.slice(offset, end).join('\n'), totalLines: lines.length, startLine: offset, path };
  } catch (error) { return { error: error.message }; }
}

function writeFile(args = {}) {
  try {
    const path = workspacePath(args.path || '');
    const content = typeof args.content === 'string' ? args.content : '';
    writeFileSync(path, content, 'utf8');
    return { path, size: Buffer.byteLength(content) };
  } catch (error) { return { error: error.message }; }
}

function searchFiles(args = {}) {
  try {
    const path = workspacePath(args.path || '.');
    const pattern = typeof args.pattern === 'string' && args.pattern ? args.pattern : '*';
    const output = execFileSync('find', [path, '-type', 'f', '-name', pattern], { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    const matches = output.split('\n').filter(Boolean).slice(0, 100);
    return { matches, count: matches.length, path, truncated: matches.length === 100 };
  } catch (error) { return { matches: [], error: error.message }; }
}

function searchContent(args = {}) {
  try {
    const path = workspacePath(args.path || '.');
    const pattern = typeof args.pattern === 'string' ? args.pattern : '';
    if (!pattern) return { matches: [], count: 0, path };
    const output = execFileSync('grep', ['-rIn', '--include=*.js', '--include=*.ts', '--include=*.tsx', '--include=*.jsx', '--include=*.json', '--include=*.html', '--include=*.css', '--include=*.md', '--', pattern, path], { encoding: 'utf8', timeout: 10_000, maxBuffer: 2 * 1024 * 1024, windowsHide: true });
    const matches = output.split('\n').filter(Boolean).slice(0, 200).map(line => {
      const [file, lineNumber, ...content] = line.split(':');
      return { file, line: Number(lineNumber) || 0, content: content.join(':') };
    });
    return { matches, count: matches.length, path, truncated: matches.length === 200 };
  } catch (error) {
    // grep uses exit status 1 when it finds no matches; that is a successful empty result.
    if (error.status === 1) return { matches: [], count: 0, path: args.path || WORKSPACE };
    return { matches: [], error: error.message };
  }
}

function getEnvironment() {
  const tools = ['node', 'npm', 'git', 'python3', 'curl', 'grep', 'find', 'sed', 'awk', 'gcc', 'rustc', 'cargo', 'go'];
  const available = {};
  for (const tool of tools) {
    try { execFileSync(process.platform === 'win32' ? 'where' : 'which', [tool], { stdio: 'ignore', timeout: 2_000, windowsHide: true }); available[tool] = true; }
    catch { available[tool] = false; }
  }
  return { agent: AGENT_NAME, workspace: WORKSPACE, platform: process.platform, arch: process.arch, nodeVersion: process.version, tools: available };
}

const TOOLS = { execute_command: executeCommand, read_file: readFile, write_file: writeFile, search_files: searchFiles, search_content: searchContent, get_environment: getEnvironment };

let activeConnection = null;
let reconnectTimer = null;
let reconnectAttempt = 0;
let stopping = false;

function scheduleReconnect(reason) {
  if (stopping || reconnectTimer) return;
  const capped = Math.min(RECONNECT_MAX_MS, RECONNECT_INITIAL_MS * (2 ** reconnectAttempt));
  const delay = Math.round(capped * (0.8 + Math.random() * 0.4));
  reconnectAttempt++;
  console.warn(`[agent] ${reason}; reconnecting in ${delay}ms`);
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, delay);
  reconnectTimer.unref?.();
}

function connect() {
  if (stopping || activeConnection) return;
  let url;
  try {
    url = new URL(RELAY_URL);
    if (!['ws:', 'wss:'].includes(url.protocol)) throw new Error('RELAY_URL must use ws:// or wss://');
  } catch (error) { console.error(`[agent] Invalid RELAY_URL: ${error.message}`); return; }

  const secure = url.protocol === 'wss:';
  const key = crypto.randomBytes(16).toString('base64');
  const request = (secure ? https : http).request({
    hostname: url.hostname,
    port: Number(url.port) || (secure ? 443 : 80),
    method: 'GET',
    path: `${url.pathname || '/'}${url.search || ''}`,
    headers: { Upgrade: 'websocket', Connection: 'Upgrade', 'Sec-WebSocket-Key': key, 'Sec-WebSocket-Version': '13' },
    // Never disable TLS validation by default. Set this only for a deliberately self-signed development relay.
    rejectUnauthorized: process.env.RELAY_TLS_REJECT_UNAUTHORIZED !== 'false',
  });
  request.setTimeout(CONNECTION_TIMEOUT_MS, () => request.destroy(new Error('Connection timed out')));
  request.once('upgrade', (response, socket, head) => {
    if (response.statusCode !== 101 || response.headers['sec-websocket-accept'] !== acceptKey(key)) {
      socket.destroy();
      return scheduleReconnect('Relay returned an invalid WebSocket handshake');
    }
    const connection = new WebSocketConnection(socket, { initialData: head, maskOutgoing: true });
    const connectedAt = Date.now();
    activeConnection = connection;
    console.log(`[agent] Connected to ${RELAY_URL} as "${AGENT_NAME}"`);
    const join = { type: 'join', name: AGENT_NAME, executor: true };
    if (RELAY_TOKEN) join.token = RELAY_TOKEN;
    connection.send(JSON.stringify(join));
    const heartbeat = setInterval(() => connection.ping(), HEARTBEAT_INTERVAL_MS);
    heartbeat.unref?.();
    connection.on('socketError', error => console.warn(`[agent] Socket error: ${error.message}`));
    connection.on('message', raw => handleMessage(raw, connection));
    connection.on('close', () => {
      clearInterval(heartbeat);
      if (activeConnection !== connection) return;
      activeConnection = null;
      if (Date.now() - connectedAt >= 20_000) reconnectAttempt = 0;
      scheduleReconnect('Relay connection closed');
    });
  });
  request.once('response', response => {
    response.resume();
    scheduleReconnect(`Relay rejected WebSocket upgrade (${response.statusCode})`);
  });
  request.once('error', error => scheduleReconnect(`Connection error: ${error.message}`));
  request.end();
}

async function handleMessage(raw, connection) {
  let message;
  try { message = JSON.parse(raw); } catch { return console.warn('[agent] Received invalid JSON from relay'); }
  if (message.type === 'joined') return console.log(`[agent] Registered as ${message.name} (${message.id})`);
  if (message.type === 'status') return console.log(`[status] ${message.count} online: ${message.agents.map(agent => `${agent.name}${agent.executor ? '*' : ''}`).join(', ')}`);
  if (message.type === 'chat') return console.log(`[chat ${message.from}] ${message.text}`);
  if (message.type === 'error') return console.error(`[relay] ${message.code || 'error'}: ${message.message}`);
  if (message.type !== 'mcp_call') return;

  const tool = TOOLS[message.method];
  let result;
  let error;
  try {
    if (!tool) throw new Error(`Tool not found: ${message.method}`);
    result = await Promise.resolve(tool(message.params || {}));
  } catch (cause) { error = cause.message || String(cause); }
  if (!connection.isOpen) return;
  connection.send(JSON.stringify({
    type: 'mcp_result', relayCallId: message.relayCallId, callId: message.callId,
    ...(error ? { error } : { result }),
  }));
}

function shutdown(signal) {
  if (stopping) return;
  stopping = true;
  clearTimeout(reconnectTimer);
  console.log(`[agent] Received ${signal}; disconnecting`);
  if (activeConnection) activeConnection.close(1000, 'Agent shutting down');
  setTimeout(() => process.exit(0), 500).unref();
}

if (require.main === module) {
  if (!RELAY_TOKEN) console.warn('[agent] AGENT_RELAY_TOKEN is not set. Do not expose an executor on an unauthenticated relay.');
  console.log(`[agent] Starting "${AGENT_NAME}" in ${WORKSPACE}`);
  connect();
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

module.exports = { TOOLS, workspacePath };
