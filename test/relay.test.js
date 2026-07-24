'use strict';

const assert = require('node:assert/strict');
const { once } = require('node:events');
const { spawn } = require('node:child_process');
const test = require('node:test');
const { createRelay } = require('../server');

let WebSocket;
try { WebSocket = require('ws'); } catch { WebSocket = null; }

async function startRelay(options = {}) {
  const relay = createRelay({ legacyToken: 'test-token', ...options });
  relay.server.listen(0, '127.0.0.1');
  await once(relay.server, 'listening');
  return { relay, port: relay.server.address().port };
}

function connect(port) {
  return new Promise((resolve, reject) => {
    if (!WebSocket) return reject(new Error('ws library not available'));
    const ws = new WebSocket(`ws://127.0.0.1:${port}`, { maxPayload: 1024 * 1024 });
    ws.on('open', () => resolve(ws));
    ws.on('error', reject);
    setTimeout(() => reject(new Error('Connection timeout')), 3000);
  });
}

function waitForMessage(ws, predicate, timeout = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { cleanup(); reject(new Error('Timed out waiting for message')); }, timeout);
    const onMessage = (data) => {
      const message = JSON.parse(data.toString());
      if (predicate(message)) { cleanup(); resolve(message); }
    };
    const cleanup = () => { clearTimeout(timer); ws.off('message', onMessage); };
    ws.on('message', onMessage);
  });
}

async function join(ws, name, executor = false) {
  const joined = waitForMessage(ws, (m) => m.type === 'joined');
  ws.send(JSON.stringify({ type: 'join', name, executor, token: 'test-token' }));
  return joined;
}

async function closeAll(context, clients) {
  for (const ws of clients) { if (ws.readyState === 1) ws.close(); }
  await Promise.all(clients.map((ws) => once(ws, 'close').catch(() => {})));
  await context.relay.stop();
}

async function stopChild(child) {
  if (child.exitCode !== null) return;
  const exited = once(child, 'exit');
  child.kill('SIGTERM');
  await Promise.race([exited, new Promise((r) => setTimeout(r, 2000))]);
  if (child.exitCode === null) child.kill('SIGKILL');
}

test('authenticated agents can join, see presence, and chat', { skip: !WebSocket }, async () => {
  const context = await startRelay();
  const alice = await connect(context.port);
  const bob = await connect(context.port);
  try {
    const aliceJoined = await join(alice, 'alice');
    assert.equal(aliceJoined.name, 'alice');
    const aliceSeesBob = waitForMessage(alice, (m) => m.type === 'status' && m.count === 2);
    await join(bob, 'bob');
    const chatAtBob = waitForMessage(bob, (m) => m.type === 'chat' && m.text === 'hello, bob');
    alice.send(JSON.stringify({ type: 'chat', text: 'hello, bob' }));
    assert.equal((await chatAtBob).from, 'alice');
    assert.equal((await aliceSeesBob).agents.length, 2);
  } finally { await closeAll(context, [alice, bob]); }
});

test('MCP calls route by opaque relay IDs and return to the original caller', { skip: !WebSocket }, async () => {
  const context = await startRelay();
  const caller = await connect(context.port);
  const executor = await connect(context.port);
  try {
    await join(caller, 'caller');
    await join(executor, 'executor', true);
    const callAtExecutor = waitForMessage(executor, (m) => m.type === 'mcp_call');
    const ackAtCaller = waitForMessage(caller, (m) => m.type === 'mcp_call_ack');
    caller.send(JSON.stringify({ type: 'mcp_call', callId: 'client-call-7', target: 'executor', method: 'get_environment', params: {} }));
    const [call, ack] = await Promise.all([callAtExecutor, ackAtCaller]);
    assert.equal(ack.delivered, true);
    assert.ok(call.relayCallId);
    const resultAtCaller = waitForMessage(caller, (m) => m.type === 'mcp_result' && m.callId === 'client-call-7');
    executor.send(JSON.stringify({ type: 'mcp_result', relayCallId: call.relayCallId, result: { ok: true } }));
    assert.deepEqual((await resultAtCaller).result, { ok: true });
  } finally { await closeAll(context, [caller, executor]); }
});

test('the relay rejects unauthenticated connections', { skip: !WebSocket }, async () => {
  const context = await startRelay();
  const intruder = await connect(context.port);
  try {
    const denied = waitForMessage(intruder, (m) => m.code === 'unauthorized');
    intruder.send(JSON.stringify({ type: 'join', name: 'intruder', token: 'wrong' }));
    assert.equal((await denied).type, 'error');
  } finally { await closeAll(context, [intruder]); }
});

test('the bundled executor performs a real authenticated request and response', { skip: !WebSocket }, async () => {
  const context = await startRelay();
  const caller = await connect(context.port);
  let executor;
  try {
    await join(caller, 'caller');
    const online = waitForMessage(caller, (m) => m.type === 'status' && m.agents.some((a) => a.name === 'integration-executor'));
    executor = spawn(process.execPath, ['agent.js'], {
      cwd: process.cwd(),
      env: {
        ...process.env, RELAY_URL: `ws://127.0.0.1:${context.port}`,
        AGENT_RELAY_TOKEN: 'test-token', AGENT_NAME: 'integration-executor',
        MCP_WORKSPACE: process.cwd(), NODE_ENV: 'test',
      },
      stdio: 'ignore',
    });
    await online;
    const result = waitForMessage(caller, (m) => m.type === 'mcp_result' && m.callId === 'agent-check');
    caller.send(JSON.stringify({ type: 'mcp_call', callId: 'agent-check', target: 'integration-executor', method: 'get_environment', params: {} }));
    const res = (await result).result;
    const text = res.content ? JSON.parse(res.content[0].text) : res;
    assert.equal(text.agent, 'integration-executor');
  } finally {
    if (executor) await stopChild(executor);
    await closeAll(context, [caller]);
  }
});
