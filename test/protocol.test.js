'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { CHANNELS, FLAGS, encode, decode, HEADER_SIZE } = require('../protocol');

describe('Protocol v2', () => {
  it('encodes and decodes a JSON message', () => {
    const msg = { type: 'chat', from: 'agent-a', text: 'hello world' };
    const frame = encode(CHANNELS.CHAT, msg, { msgpack: false });
    const result = decode(frame);
    assert.equal(result.channel, CHANNELS.CHAT);
    assert.deepEqual(result.msg, msg);
  });

  it('encodes and decodes a MessagePack message', () => {
    const msg = { type: 'mcp.call', call: { name: 'test', params: { x: 42 } } };
    const frame = encode(CHANNELS.MCP, msg);
    const result = decode(frame);
    assert.equal(result.channel, CHANNELS.MCP);
    assert.deepEqual(result.msg, msg);
  });

  it('compresses large payloads', () => {
    const msg = { type: 'chat', text: 'x'.repeat(2000) };
    const frame = encode(CHANNELS.CHAT, msg);
    const result = decode(frame);
    assert.equal(result.msg.text.length, 2000);
  });

  it('handles the reliable flag', () => {
    const msg = { type: 'test' };
    const frame = encode(CHANNELS.CONTROL, msg, { reliable: true });
    const result = decode(frame);
    assert.equal(result.flags & FLAGS.RELIABLE, FLAGS.RELIABLE);
  });

  it('returns null for truncated frames', () => {
    const buf = Buffer.alloc(3);
    assert.equal(decode(buf), null);
  });

  it('detects binary frames correctly', () => {
    const frame = encode(CHANNELS.CONTROL, { test: true }, { msgpack: false });
    const { isBinaryFrame } = require('../protocol');
    assert.ok(isBinaryFrame(frame));
    assert.ok(!isBinaryFrame(Buffer.from('hello')));
    assert.ok(!isBinaryFrame(Buffer.alloc(2)));
  });
});
