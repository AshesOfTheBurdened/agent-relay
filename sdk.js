'use strict';

const WebSocket = require('ws');
const crypto = require('crypto');
const { CHANNELS, encode, decode, isBinaryFrame } = require('./protocol');
const config = require('./config');

class Agent {
  constructor(options = {}) {
    this.name = options.name || config.agentName;
    this.relayUrl = options.relay || config.relayUrl;
    this.token = options.token || config.agentToken;
    this.reconnect = {
      baseMs: options.reconnect?.baseMs || 1000,
      maxMs: options.reconnect?.maxMs || 30_000,
    };

    this.ws = null;
    this.sessionId = null;
    this.lastSeq = 0;
    this.attempt = 0;
    this.running = false;
    this.tools = new Map();
    this.handlers = new Map();
    this.pendingCalls = new Map();
    this.heartbeatTimer = null;
    this.reconnectTimer = null;
  }

  tool(name, params, handler) {
    this.tools.set(name, { name, params, handler });
    return this;
  }

  on(event, handler) {
    if (!this.handlers.has(event)) this.handlers.set(event, []);
    this.handlers.get(event).push(handler);
    return this;
  }

  _emit(event, data) {
    const handlers = this.handlers.get(event) || [];
    for (const h of handlers) {
      try { h(data); } catch (err) { console.error(`Handler error [${event}]:`, err); }
    }
  }

  async call(executor, toolName, params, timeoutMs = 30_000) {
    return new Promise((resolve, reject) => {
      const relayCallId = crypto.randomUUID();
      const timer = setTimeout(() => {
        this.pendingCalls.delete(relayCallId);
        reject(new Error(`Call timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.pendingCalls.set(relayCallId, { resolve, reject, timer });
      this._send(CHANNELS.MCP, {
        type: 'mcp.call', relayCallId, executor, call: { name: toolName, params },
      });
    });
  }

  chat(text, to = null) {
    this._send(CHANNELS.CHAT, { type: 'chat', text, to });
  }

  start() {
    this.running = true;
    this._connect();
    process.on('SIGTERM', () => this.stop());
    process.on('SIGINT', () => this.stop());
    return this;
  }

  async stop() {
    this.running = false;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    for (const [, pending] of this.pendingCalls) {
      clearTimeout(pending.timer);
      pending.reject(new Error('Agent shutting down'));
    }
    this.pendingCalls.clear();
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this._send(CHANNELS.CONTROL, { type: 'goodbye' });
      this.ws.close(1001, 'Agent stopping');
      await new Promise((r) => setTimeout(r, 300));
    }
  }

  _connect() {
    if (!this.running) return;
    this.ws = new WebSocket(this.relayUrl, { maxPayload: config.maxMessageSize });

    this.ws.on('open', () => {
      this.attempt = 0;
      this._send(CHANNELS.CONTROL, {
        type: 'hello', name: this.name, token: this.token,
        sessionId: this.sessionId, lastSeq: this.lastSeq,
        tools: [...this.tools.values()].map((t) => ({ name: t.name, params: t.params })),
      });
      this.heartbeatTimer = setInterval(() => {
        this._send(CHANNELS.CONTROL, { type: 'heartbeat' });
      }, config.heartbeatIntervalMs);
      this._emit('connected', { name: this.name });
    });

    this.ws.on('message', (data, isBinary) => {
      if (isBinary && isBinaryFrame(data)) {
        const frame = decode(data);
        if (frame) this._route(frame.channel, frame.msg);
      } else {
        try { this._routeLegacy(JSON.parse(data.toString())); } catch {}
      }
    });

    this.ws.on('close', () => {
      if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
      this._emit('disconnected', {});
      this._scheduleReconnect();
    });

    this.ws.on('error', () => {});
  }

  _scheduleReconnect() {
    if (!this.running) return;
    const base = Math.min(this.reconnect.baseMs * Math.pow(2, this.attempt), this.reconnect.maxMs);
    const jitter = base * 0.3 * Math.random();
    const delay = Math.round(base + jitter);
    this.attempt++;
    this.reconnectTimer = setTimeout(() => this._connect(), delay);
  }

  _route(channel, msg) {
    switch (channel) {
      case CHANNELS.CONTROL:
        if (msg.type === 'welcome' || msg.type === 'joined') {
          this.sessionId = msg.sessionId || msg.id;
          this.lastSeq = msg.seq || 0;
        }
        this._emit(msg.type, msg);
        break;
      case CHANNELS.CHAT: this._emit('chat', msg); break;
      case CHANNELS.MCP: this._handleMcp(msg); break;
      case CHANNELS.STREAM: this._emit('stream', msg); break;
      case CHANNELS.PRESENCE: this._emit('presence', msg); break;
    }
  }

  _routeLegacy(msg) {
    if (msg.type === 'chat') this._emit('chat', msg);
    else if (msg.type === 'mcp_call' || msg.type === 'mcp.result') this._handleMcp(msg);
    else if (msg.type === 'joined' || msg.type === 'welcome') {
      this.sessionId = msg.id || msg.sessionId;
      this.lastSeq = msg.seq || 0;
      this._emit(msg.type, msg);
    } else this._emit(msg.type, msg);
  }

  async _handleMcp(msg) {
    if (msg.type === 'mcp_call' || msg.type === 'mcp.call') {
      const tool = this.tools.get(msg.method || msg.call?.name);
      const relayCallId = msg.relayCallId;
      const result = tool
        ? await tool.handler(msg.params ?? msg.call?.params ?? {}).catch((e) => ({
            isError: true, content: [{ type: 'text', text: e.message }],
          }))
        : { isError: true, content: [{ type: 'text', text: `Unknown tool: ${msg.method || msg.call?.name}` }] };
      this._send(CHANNELS.MCP, { type: 'mcp.result', relayCallId, result });
    }
    if (msg.type === 'mcp_result' || msg.type === 'mcp.result') {
      const pending = this.pendingCalls.get(msg.relayCallId);
      if (pending) {
        clearTimeout(pending.timer);
        this.pendingCalls.delete(msg.relayCallId);
        if (msg.result?.isError || msg.error) {
          pending.reject(new Error(msg.error || msg.result?.content?.[0]?.text || 'Call failed'));
        } else {
          pending.resolve(msg.result || msg);
        }
      }
    }
  }

  _send(channel, msg) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(encode(channel, msg));
    }
  }
}

module.exports = { Agent };
