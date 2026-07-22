'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const http = require('node:http');
const { once } = require('node:events');
const { spawn } = require('node:child_process');
const test = require('node:test');
const { createRelay } = require('../server');
const { WebSocketConnection } = require('../websocket');

async function startRelay(options = {}) {
  const relay = createRelay({ token: 'test-token', heartbeatIntervalMs: 10_000, ...options });
  relay.server.listen(0, '127.0.0.1');
  await once(relay.server, 'listening');
  return { relay, port: relay.server.address().port };
}

function connect(port, { masked = true } = {}) {
  return new Promise((resolve, reject) => {
    const key = crypto.randomBytes(16).toString('base64');
    const request = http.request({
      hostname: '127.0.0.1', port, method: 'GET', path: '/',
      headers: { Upgrade: 'websocket', Connection: 'Upgrade', 'Sec-WebSocket-Key': key, 'Sec-WebSocket-Version': '13' },
    });
    request.once('upgrade', (response, socket, head) => resolve(new WebSocketConnection(socket, { initialData: head, maskOutgoing: masked })));
    request.once('error', reject);
    request.end();
  });
}

function waitForMessage(client, predicate, timeout = 1_500) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { cleanup(); reject(new Error('Timed out waiting for message')); }, timeout);
    const onMessage = raw => {
      const message = JSON.parse(raw);
      if (predicate(message)) { cleanup(); resolve(message); }
    };
    const cleanup = () => { clearTimeout(timer); client.off('message', onMessage); };
    client.on('message', onMessage);
  });
}

async function join(client, name, executor = false) {
  const joined = waitForMessage(client, message => message.type === 'joined');
  client.send(JSON.stringify({ type: 'join', name, executor, token: 'test-token' }));
  return joined;
}

async function closeAll(context, clients) {
  const closures = clients.filter(client => !client.closed).map(client => once(client, 'close').catch(() => {}));
  for (const client of clients) client.close();
  await Promise.all(closures);
  await context.relay.stop();
}

async function stopChild(child) {
  if (child.exitCode !== null) return;
  const exited = once(child, 'exit');
  child.kill('SIGTERM');
  await Promise.race([exited, new Promise(resolve => setTimeout(resolve, 2_000))]);
  if (child.exitCode === null) child.kill('SIGKILL');
}

test('authenticated agents can join, see presence, and chat', async () => {
  const context = await startRelay();
  const alice = await connect(context.port);
  const bob = await connect(context.port);
  try {
    const aliceJoined = await join(alice, 'alice');
    assert.equal(aliceJoined.name, 'alice');
    const aliceSeesBob = waitForMessage(alice, message => message.type === 'status' && message.count === 2);
    await join(bob, 'bob');
    const chatAtBob = waitForMessage(bob, message => message.type === 'chat' && message.text === 'hello, bob');
    alice.send(JSON.stringify({ type: 'chat', text: 'hello, bob' }));
    assert.equal((await chatAtBob).from, 'alice');
    assert.equal((await aliceSeesBob).agents.length, 2);
  } finally { await closeAll(context, [alice, bob]); }
});

test('MCP calls route by opaque relay IDs and return to the original caller', async () => {
  const context = await startRelay();
  const caller = await connect(context.port);
  const executor = await connect(context.port);
  try {
    await join(caller, 'caller');
    const executorJoined = join(executor, 'executor', true);
    await executorJoined;
    const callAtExecutor = waitForMessage(executor, message => message.type === 'mcp_call');
    const ackAtCaller = waitForMessage(caller, message => message.type === 'mcp_call_ack');
    caller.send(JSON.stringify({ type: 'mcp_call', callId: 'client-call-7', target: 'executor', method: 'get_environment', params: {} }));
    const [call, ack] = await Promise.all([callAtExecutor, ackAtCaller]);
    assert.equal(ack.delivered, true);
    assert.ok(call.relayCallId);
    const resultAtCaller = waitForMessage(caller, message => message.type === 'mcp_result' && message.callId === 'client-call-7');
    executor.send(JSON.stringify({ type: 'mcp_result', relayCallId: call.relayCallId, result: { ok: true } }));
    assert.deepEqual((await resultAtCaller).result, { ok: true });
  } finally { await closeAll(context, [caller, executor]); }
});

test('the relay rejects unauthenticated and unmasked client messages', async () => {
  const context = await startRelay();
  const unauthenticated = await connect(context.port);
  const unmasked = await connect(context.port, { masked: false });
  try {
    const denied = waitForMessage(unauthenticated, message => message.code === 'unauthorized');
    unauthenticated.send(JSON.stringify({ type: 'join', name: 'intruder', token: 'wrong' }));
    assert.equal((await denied).type, 'error');
    const closed = once(unmasked, 'close');
    unmasked.send(JSON.stringify({ type: 'join', name: 'unmasked', token: 'test-token' }));
    await closed;
    assert.equal(context.relay.agents.size, 0);
  } finally { await closeAll(context, [unauthenticated, unmasked]); }
});

test('the bundled executor performs a real authenticated request and response', async () => {
  const context = await startRelay();
  const caller = await connect(context.port);
  let executor;
  try {
    await join(caller, 'caller');
    const online = waitForMessage(caller, message => message.type === 'status' && message.agents.some(agent => agent.name === 'integration-executor'));
    executor = spawn(process.execPath, ['agent.js'], {
      cwd: process.cwd(),
      env: { ...process.env, RELAY_URL: `ws://127.0.0.1:${context.port}`, AGENT_RELAY_TOKEN: 'test-token', AGENT_NAME: 'integration-executor', MCP_WORKSPACE: process.cwd() },
      stdio: 'ignore',
    });
    await online;
    const result = waitForMessage(caller, message => message.type === 'mcp_result' && message.callId === 'agent-check');
    caller.send(JSON.stringify({ type: 'mcp_call', callId: 'agent-check', target: 'integration-executor', method: 'get_environment', params: {} }));
    assert.equal((await result).result.agent, 'integration-executor');
  } finally {
    if (executor) await stopChild(executor);
    await closeAll(context, [caller]);
  }
});
