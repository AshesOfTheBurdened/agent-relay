'use strict';

const crypto = require('crypto');
const { EventEmitter } = require('events');

const MAX_MESSAGE_BYTES = 1024 * 1024;

function acceptKey(key) {
  return crypto.createHash('sha1')
    .update(`${key}258EAFA5-E914-47DA-95CA-5AB9DC11B85B`)
    .digest('base64');
}

function isValidClientKey(key) {
  if (typeof key !== 'string') return false;
  try { return Buffer.from(key, 'base64').length === 16; } catch { return false; }
}

function makeClosePayload(code, reason) {
  const reasonBytes = Buffer.from(String(reason || ''), 'utf8').subarray(0, 123);
  const payload = Buffer.alloc(2 + reasonBytes.length);
  payload.writeUInt16BE(code, 0);
  reasonBytes.copy(payload, 2);
  return payload;
}

class WebSocketConnection extends EventEmitter {
  constructor(socket, options = {}) {
    super();
    this.socket = socket;
    this.maskOutgoing = Boolean(options.maskOutgoing);
    this.requireMasked = Boolean(options.requireMasked);
    this.maxMessageBytes = options.maxMessageBytes || MAX_MESSAGE_BYTES;
    this.buffer = options.initialData && options.initialData.length ? Buffer.from(options.initialData) : Buffer.alloc(0);
    this.state = 'open';
    this.fragmentOpcode = null;
    this.fragments = [];
    this.fragmentBytes = 0;
    this.closed = false;

    socket.setNoDelay(true);
    socket.setKeepAlive(true, 60_000);
    socket.on('data', data => this._feed(data));
    socket.on('close', () => this._finishClose());
    socket.on('error', error => this.emit('socketError', error));
    if (this.buffer.length) this._feed(Buffer.alloc(0));
  }

  get isOpen() { return this.state === 'open' && !this.socket.destroyed; }

  send(text) { return this._writeFrame(0x1, Buffer.from(String(text), 'utf8')); }
  ping(payload = Buffer.alloc(0)) { return this._writeFrame(0x9, payload); }
  pong(payload = Buffer.alloc(0)) { return this._writeFrame(0xA, payload); }

  close(code = 1000, reason = '') {
    if (this.state !== 'open') return;
    this.state = 'closing';
    this._writeFrame(0x8, makeClosePayload(code, reason), true);
    this.socket.end();
  }

  terminate() {
    if (!this.socket.destroyed) this.socket.destroy();
  }

  _writeFrame(opcode, payload, allowClosing = false) {
    if ((!this.isOpen && !allowClosing) || this.socket.destroyed) return false;
    const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
    if (body.length > this.maxMessageBytes && opcode < 0x8) return false;
    if (opcode >= 0x8 && body.length > 125) return false;

    let extra = body.length < 126 ? 0 : body.length < 65536 ? 2 : 8;
    const maskLength = this.maskOutgoing ? 4 : 0;
    const header = Buffer.alloc(2 + extra + maskLength);
    header[0] = 0x80 | opcode;
    let offset = 2;
    if (body.length < 126) header[1] = (this.maskOutgoing ? 0x80 : 0) | body.length;
    else if (body.length < 65536) {
      header[1] = (this.maskOutgoing ? 0x80 : 0) | 126;
      header.writeUInt16BE(body.length, offset);
      offset += 2;
    } else {
      header[1] = (this.maskOutgoing ? 0x80 : 0) | 127;
      header.writeBigUInt64BE(BigInt(body.length), offset);
      offset += 8;
    }

    let outgoing = body;
    if (this.maskOutgoing) {
      const mask = crypto.randomBytes(4);
      mask.copy(header, offset);
      outgoing = Buffer.alloc(body.length);
      for (let i = 0; i < body.length; i++) outgoing[i] = body[i] ^ mask[i % 4];
    }
    try { return this.socket.write(Buffer.concat([header, outgoing])); } catch { return false; }
  }

  _feed(data) {
    if (this.closed) return;
    if (data.length) this.buffer = Buffer.concat([this.buffer, data]);
    while (this.buffer.length >= 2) {
      const first = this.buffer[0];
      const opcode = first & 0x0f;
      const fin = Boolean(first & 0x80);
      const hasRsv = Boolean(first & 0x70);
      const masked = Boolean(this.buffer[1] & 0x80);
      let length = this.buffer[1] & 0x7f;
      let offset = 2;

      if (hasRsv || ![0, 1, 2, 8, 9, 10].includes(opcode)) return this._protocolError('Unsupported WebSocket frame');
      if (this.requireMasked && !masked) return this._protocolError('Client frames must be masked');
      if (opcode >= 8 && (!fin || length > 125)) return this._protocolError('Invalid control frame');
      if (length === 126) {
        if (this.buffer.length < 4) break;
        length = this.buffer.readUInt16BE(2);
        offset = 4;
      } else if (length === 127) {
        if (this.buffer.length < 10) break;
        const value = this.buffer.readBigUInt64BE(2);
        if (value > BigInt(this.maxMessageBytes)) return this._tooLarge();
        length = Number(value);
        offset = 10;
      }
      if (length > this.maxMessageBytes && opcode < 8) return this._tooLarge();
      if (masked) {
        if (this.buffer.length < offset + 4) break;
      }
      const mask = masked ? this.buffer.subarray(offset, offset + 4) : null;
      if (masked) offset += 4;
      if (this.buffer.length < offset + length) break;

      let payload = this.buffer.subarray(offset, offset + length);
      this.buffer = this.buffer.subarray(offset + length);
      if (masked) {
        payload = Buffer.from(payload);
        for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i % 4];
      }
      if (!this._handleFrame(opcode, fin, payload)) return;
    }
  }

  _handleFrame(opcode, fin, payload) {
    if (opcode === 0) {
      if (this.fragmentOpcode === null) return this._protocolError('Unexpected continuation frame');
      this.fragments.push(payload);
      this.fragmentBytes += payload.length;
      if (this.fragmentBytes > this.maxMessageBytes) return this._tooLarge();
      if (fin) {
        const complete = Buffer.concat(this.fragments, this.fragmentBytes);
        const originalOpcode = this.fragmentOpcode;
        this.fragmentOpcode = null;
        this.fragments = [];
        this.fragmentBytes = 0;
        return this._emitData(originalOpcode, complete);
      }
      return true;
    }
    if (opcode === 1 || opcode === 2) {
      if (this.fragmentOpcode !== null) return this._protocolError('New data frame during fragmentation');
      if (fin) return this._emitData(opcode, payload);
      this.fragmentOpcode = opcode;
      this.fragments = [payload];
      this.fragmentBytes = payload.length;
      return true;
    }
    if (opcode === 8) {
      if (payload.length === 1) return this._protocolError('Invalid close frame');
      if (this.state === 'open') {
        this.state = 'closing';
        this._writeFrame(0x8, payload, true);
      }
      this.socket.end();
      return false;
    }
    if (opcode === 9) {
      this.pong(payload);
      this.emit('ping', payload);
      return true;
    }
    this.emit('pong', payload);
    return true;
  }

  _emitData(opcode, payload) {
    if (opcode === 1) this.emit('message', payload.toString('utf8'));
    else this.emit('binary', payload);
    return true;
  }

  _protocolError(reason) { this.close(1002, reason); return false; }
  _tooLarge() { this.close(1009, 'Message too large'); return false; }

  _finishClose() {
    if (this.closed) return;
    this.closed = true;
    this.state = 'closed';
    this.emit('close');
  }
}

module.exports = { WebSocketConnection, acceptKey, isValidClientKey, MAX_MESSAGE_BYTES };
