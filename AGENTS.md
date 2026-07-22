# Agent Relay — Project Info

## Structure
- `server.js` — WebSocket relay server (zero deps). Handles WebSocket upgrades, agent registry, MCP call routing, heartbeats, health/metrics endpoints.
- `agent.js` — Executor agent. Connects to relay, exposes 6 tools: `execute_command`, `read_file`, `write_file`, `search_files`, `search_content`, `get_environment`. Sandboxed to `MCP_WORKSPACE`.
- `websocket.js` — RFC 6455 WebSocket frame parser/serializer (~200 lines, no deps).
- `relay-client.js` — `RelayClient` class for connecting to relay and calling executors programmatically.
- `bridge-demo.js` — End-to-end demo: starts relay, spawns agent.js, makes calls via RelayClient.
- `test/relay.test.js` — Integration tests using `node --test`.

## Running tests
```
cd agent-relay && npm test
```

## Running the relay
```
AGENT_RELAY_TOKEN="$(openssl rand -hex 32)" npm start
```

## Running an executor agent
```
RELAY_URL="ws://localhost:8080" AGENT_RELAY_TOKEN="..." AGENT_NAME="my-agent" node agent.js
```

## Deploy
- **Railway**: Connect repo, set `AGENT_RELAY_TOKEN` secret. Config in `railway.json`.
- **Render**: Blueprint deploy from repo, prompts for `AGENT_RELAY_TOKEN`. Config in `render.yaml`.

## RelayClient API (relay-client.js)
```js
const { RelayClient } = require('./relay-client');
const client = new RelayClient({ url, name, token });
client.connect();

// Call executor by name
const result = await client.call('agent-name', 'execute_command', { command: 'ls' });

// Call executor by ID
const result = await client.callById('uuid', 'read_file', { path: 'foo.txt' });

// Chat, status, events
client.chat('hello');
client.requestStatus();
client.on('chat', msg => ...);
client.on('agents', map => ...);
client.disconnect();
```

## Key protocol (from server.js)
- `join` → `joined` — register with name/token
- `mcp_call` → `mcp_call_ack` → `mcp_result` — call executor tool
- `chat` — broadcast text to all agents
- `status` — get connected agents list
- Heartbeat: server pings, client pongs; eviction after timeout
