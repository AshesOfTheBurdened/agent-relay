'use strict';

const path = require('path');

function envInt(key, fallback) {
  const v = parseInt(process.env[key], 10);
  return Number.isFinite(v) ? v : fallback;
}

function envBool(key, fallback) {
  const v = process.env[key];
  if (v === undefined) return fallback;
  return v === 'true' || v === '1';
}

const config = {
  port: envInt('PORT', 8080),
  host: process.env.HOST || '0.0.0.0',
  env: process.env.NODE_ENV || 'development',

  jwtSecret: process.env.JWT_SECRET || 'dev-secret-change-me',
  jwtExpiry: process.env.JWT_EXPIRY || '24h',
  legacyToken: process.env.RELAY_TOKEN || '',

  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
  redisEnabled: envBool('REDIS_ENABLED', false),

  maxMessageSize: envInt('MAX_MESSAGE_SIZE', 1024 * 1024),
  mcpCallTimeoutMs: envInt('MCP_CALL_TIMEOUT_MS', 30_000),
  heartbeatIntervalMs: envInt('HEARTBEAT_INTERVAL_MS', 15_000),
  heartbeatTimeoutMs: envInt('HEARTBEAT_TIMEOUT_MS', 45_000),
  messageHistorySize: envInt('MESSAGE_HISTORY_SIZE', 2000),
  maxBufferedAmount: envInt('MAX_BUFFERED_AMOUNT', 1024 * 1024),

  agentName: process.env.AGENT_NAME || 'executor',
  relayUrl: process.env.RELAY_URL || 'ws://localhost:8080',
  agentToken: process.env.AGENT_RELAY_TOKEN || '',
  mcpWorkspace: path.resolve(process.env.MCP_WORKSPACE || process.cwd()),

  reconnectBaseMs: 1000,
  reconnectMaxMs: 30_000,
  reconnectJitter: 0.3,
};

module.exports = config;
