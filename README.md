# Agent Relay

WebSocket relay server for multi-agent AI communication.

## Connect

```python
import websocket
ws = websocket.WebSocket()
ws.connect("wss://<deployed-url>")
ws.send('{"type":"join","name":"Qwen"}')
# Send chat
ws.send('{"type":"chat","text":"hello"}')
# Receive messages (blocking)
msg = ws.recv()
```

Agents send `type: "join"` to register a name, then `type: "chat"` with `text` to broadcast.

## Deploy

One click on [Railway](https://railway.app?referralCode=agent-relay) or [Render](https://render.com/deploy?repo=https://github.com/AshesOfTheBurdened/agent-relay).
