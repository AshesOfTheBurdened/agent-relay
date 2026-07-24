'use strict';

let client;
try {
  client = require('prom-client');
} catch {
  client = null;
}

let register, activeConnections, messagesTotal, mcpCallDuration;
let mcpCallErrors, mcpCallsPending, deadLetters, reconnectTotal;

if (client) {
  register = new client.Registry();
  client.collectDefaultMetrics({ register, prefix: 'relay_', eventLoopMonitoringPrecision: 1000 });

  activeConnections = new client.Gauge({
    name: 'relay_connections_active', help: 'Active WebSocket connections', registers: [register],
  });
  messagesTotal = new client.Counter({
    name: 'relay_messages_total', help: 'Total messages', labelNames: ['channel', 'type'], registers: [register],
  });
  mcpCallDuration = new client.Histogram({
    name: 'relay_mcp_call_duration_seconds', help: 'MCP call duration (seconds)',
    buckets: [0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30], registers: [register],
  });
  mcpCallErrors = new client.Counter({
    name: 'relay_mcp_call_errors_total', help: 'MCP call errors', labelNames: ['reason'], registers: [register],
  });
  mcpCallsPending = new client.Gauge({
    name: 'relay_mcp_calls_pending', help: 'Pending MCP calls', registers: [register],
  });
  deadLetters = new client.Counter({
    name: 'relay_dead_letters_total', help: 'Dead-lettered messages', labelNames: ['reason'], registers: [register],
  });
  reconnectTotal = new client.Counter({
    name: 'relay_reconnections_total', help: 'Agent reconnections', registers: [register],
  });
} else {
  register = {
    async metrics() { return '# prom-client not available\n'; },
    contentType: 'text/plain',
  };
  const noop = { inc: () => {}, dec: () => {}, observe: () => {}, set: () => {} };
  activeConnections = noop; messagesTotal = noop; mcpCallDuration = noop;
  mcpCallErrors = noop; mcpCallsPending = noop; deadLetters = noop; reconnectTotal = noop;
}

module.exports = {
  register, activeConnections, messagesTotal, mcpCallDuration,
  mcpCallErrors, mcpCallsPending, deadLetters, reconnectTotal,
};
