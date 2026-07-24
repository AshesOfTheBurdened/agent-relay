'use strict';

const zlib = require('zlib');

const CHANNELS = {
  CONTROL: 0x00,
  CHAT: 0x01,
  MCP: 0x02,
  STREAM: 0x03,
  PRESENCE: 0x04,
};

const FLAGS = {
  MSGPACK: 0x01,
  COMPRESSED: 0x02,
  RELIABLE: 0x04,
  BROADCAST: 0x08,
};

const HEADER_SIZE = 6;
const COMPRESS_THRESHOLD = 512;

function encode(channel, msg, options = {}) {
  const useMsgpack = options.msgpack !== false;
  const reliable = options.reliable || false;

  let payload;
  let flags = 0;

  if (useMsgpack) {
    try {
      const { pack } = require('msgpackr');
      payload = pack(msg);
      flags |= FLAGS.MSGPACK;
    } catch {
      payload = Buffer.from(JSON.stringify(msg), 'utf8');
    }
  } else {
    payload = Buffer.from(JSON.stringify(msg), 'utf8');
  }

  if (payload.length > COMPRESS_THRESHOLD) {
    try {
      payload = zlib.deflateRawSync(payload);
      flags |= FLAGS.COMPRESSED;
    } catch {}
  }

  if (reliable) flags |= FLAGS.RELIABLE;
  if (options.broadcast) flags |= FLAGS.BROADCAST;

  const header = Buffer.alloc(HEADER_SIZE);
  header.writeUInt8(channel, 0);
  header.writeUInt8(flags, 1);
  header.writeUInt32BE(payload.length, 2);

  return Buffer.concat([header, payload]);
}

function decode(buffer) {
  if (buffer.length < HEADER_SIZE) return null;

  const channel = buffer.readUInt8(0);
  const flags = buffer.readUInt8(1);
  const length = buffer.readUInt32BE(2);

  if (buffer.length < HEADER_SIZE + length) return null;

  let payload = buffer.subarray(HEADER_SIZE, HEADER_SIZE + length);

  if (flags & FLAGS.COMPRESSED) {
    try {
      payload = zlib.inflateRawSync(payload);
    } catch {
      return null;
    }
  }

  let msg;
  if (flags & FLAGS.MSGPACK) {
    try {
      const { unpack } = require('msgpackr');
      msg = unpack(payload);
    } catch {
      return null;
    }
  } else {
    try {
      msg = JSON.parse(payload.toString('utf8'));
    } catch {
      return null;
    }
  }

  return { channel, flags, msg };
}

function isBinaryFrame(data) {
  return Buffer.isBuffer(data) && data.length >= HEADER_SIZE;
}

module.exports = { CHANNELS, FLAGS, HEADER_SIZE, encode, decode, isBinaryFrame };
