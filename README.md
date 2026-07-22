# Agent Relay

A small, dependency-free WebSocket relay for coordinating AI agents. It is designed for long-running deployments: authenticated connections, protocol-correct WebSockets, heartbeats, reconnect backoff, graceful shutdown, call timeouts, and health/metrics endpoints are built in.

## Run the relay

Node.js 20 or newer is required.

```bash
git clone https://github.com/AshesOfTheBurdened/agent-relay.git
cd agent-relay
export AGENT_RELAY_TOKEN="$(openssl rand -hex 32)"
npm start
```

For local experimentation only, the token may be omitted. Do not expose an unauthenticated relay to a network: an executor can run local tools on behalf of connected agents.

The relay listens on `PORT` (default `8080`). It exposes:

| Endpoint | Purpose |
| --- | --- |
| `/health` | Liveness, active-agent count, pending-call count, and uptime |
| `/ready` | `200` while accepting new connections; `503` while draining |
| `/metrics` | Prometheus-compatible operational counters |
| `/` | Small live presence page; it asks for the relay token when authentication is enabled |

## Run an executor agent

`agent.js` connects to the relay, advertises itself as an executor, and supports a small MCP-style tool set (`execute_command`, `read_file`, `write_file`, `search_files`, `search_content`, and `get_environment`). Tool paths and working directories are restricted to `MCP_WORKSPACE`.

```bash
export RELAY_URL="wss://relay.example.com"
export AGENT_RELAY_TOKEN="the-same-secret"
export AGENT_NAME="build-agent"
export MCP_WORKSPACE="/srv/agent-workspace"
node agent.js
```

The executor validates the WebSocket handshake and TLS certificates. A deliberately self-signed development relay can be allowed with `RELAY_TLS_REJECT_UNAUTHORIZED=false`; do not use that setting in production.

## Protocol

All application messages are JSON text frames. Every connection must register first.

```json
{ "type": "join", "name": "planner", "token": "...", "executor": false }
```

Chat is broadcast to all other joined agents:

```json
{ "type": "chat", "text": "Task decomposition is ready." }
```

To call a tool on an executor, target its unique name or its `id` from a `status` message:

```json
{
  "type": "mcp_call",
  "callId": "local-request-42",
  "target": "build-agent",
  "method": "get_environment",
  "params": {}
}
```

The relay first returns `mcp_call_ack`. It forwards the call with a relay-generated `relayCallId`, then returns `mcp_result` to the original caller. That opaque ID prevents name collisions and prevents another agent from spoofing a result. If multiple executors use the same name, calls by name are rejected as ambiguous; use `targetId` instead.

## Reliability and tuning

The relay pings registered agents every `HEARTBEAT_INTERVAL_MS` (default 25 seconds) and removes agents that have not answered by `HEARTBEAT_TIMEOUT_MS` (default 75 seconds). Executors reconnect with capped exponential backoff and jitter. Pending MCP calls expire after `CALL_TIMEOUT_MS` (default 60 seconds), and callers receive a clear error if the executor disconnects.

| Variable | Default | Meaning |
| --- | ---: | --- |
| `AGENT_RELAY_TOKEN` | empty | Required shared secret in production |
| `MAX_MESSAGE_BYTES` | 1 MiB | Maximum WebSocket application message size |
| `JOIN_TIMEOUT_MS` | 15,000 | Time allowed to send `join` after connecting |
| `HEARTBEAT_INTERVAL_MS` | 25,000 | Server heartbeat cadence |
| `HEARTBEAT_TIMEOUT_MS` | 75,000 | Stale-agent eviction threshold |
| `CALL_TIMEOUT_MS` | 60,000 | Maximum MCP call lifetime |
| `RECONNECT_INITIAL_MS` | 1,000 | Executor's initial retry delay |
| `RECONNECT_MAX_MS` | 30,000 | Executor's retry-delay cap |

On `SIGTERM` or `SIGINT`, the relay stops accepting upgrades, notifies clients, expires pending calls, and closes its HTTP listener. This works cleanly with rolling deployment platforms and their health checks.

## Verify

```bash
npm test
npm run check
```

The integration tests cover authenticated presence/chat, end-to-end MCP request/result routing, and rejection of unauthenticated or protocol-invalid client frames.

## Deploy

The provided `Dockerfile`, `railway.json`, and `render.yaml` are ready for a single-replica relay. Configure `AGENT_RELAY_TOKEN` as a platform secret and use `/health` for the platform health check. Render will prompt for the token when initially creating the Blueprint; add it manually for an existing service. For high availability, run multiple replicas only behind a WebSocket-aware load balancer with session affinity **and** a shared state/message backend; this repository intentionally keeps no external dependency, so its in-memory agent registry is per process.
