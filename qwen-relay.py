#!/usr/bin/env python3
"""
Qwen Agent Relay Client
Connects to the agent-relay v2 WebSocket relay.
Python 3.10+, websockets library required.

Usage:
  python3 qwen-relay.py --name qwen-agent --token YOUR_TOKEN --relay wss://relay.example.com
"""

import asyncio
import json
import sys
import time
import random
import argparse
import traceback
import signal

try:
    import websockets
except ImportError:
    print("ERROR: websockets library not installed. Run: pip install websockets")
    sys.exit(1)

try:
    import subprocess
except ImportError:
    subprocess = None


class QwenAgent:
    def __init__(self, name, token, relay_url, workspace="/tmp"):
        self.name = name
        self.token = token
        self.relay_url = relay_url
        self.workspace = workspace
        self.ws = None
        self.session_id = None
        self.last_seq = 0
        self.reconnect_attempt = 0
        self.running = True
        self.pending_calls = {}
        self.heartbeat_interval = 15

    async def connect(self):
        while self.running:
            try:
                print(f"[qwen] Connecting to {self.relay_url} as '{self.name}'...")
                self.ws = await websockets.connect(
                    self.relay_url,
                    max_size=1024 * 1024,
                    ping_interval=self.heartbeat_interval,
                    ping_timeout=10,
                )
                self.reconnect_attempt = 0
                print(f"[qwen] Connected!")

                await self.send({
                    "type": "join",
                    "name": self.name,
                    "token": self.token,
                    "sessionId": self.session_id,
                    "lastSeq": self.last_seq,
                })

                async for raw in self.ws:
                    try:
                        msg = json.loads(raw)
                        await self.handle_message(msg)
                    except json.JSONDecodeError:
                        print(f"[qwen] Invalid JSON: {raw[:100]}")
                    except Exception as e:
                        print(f"[qwen] Handler error: {e}")

            except websockets.exceptions.ConnectionClosed as e:
                print(f"[qwen] Disconnected (code={e.code})")
            except (OSError, asyncio.TimeoutError) as e:
                print(f"[qwen] Connection error: {e}")
            except Exception as e:
                print(f"[qwen] Unexpected error: {e}")
                traceback.print_exc()

            if self.running:
                await self.schedule_reconnect()

    async def schedule_reconnect(self):
        base = min(1000 * (2 ** self.reconnect_attempt), 30000)
        jitter = base * 0.3 * random.random()
        delay = (base + jitter) / 1000.0
        self.reconnect_attempt += 1
        print(f"[qwen] Reconnecting in {delay:.1f}s (attempt {self.reconnect_attempt})")
        await asyncio.sleep(delay)

    async def send(self, msg):
        if self.ws:
            try:
                await self.ws.send(json.dumps(msg))
            except Exception as e:
                print(f"[qwen] Send error: {e}")

    async def handle_message(self, msg):
        msg_type = msg.get("type")
        if msg_type == "joined":
            self.session_id = msg.get("id") or msg.get("sessionId")
            self.last_seq = msg.get("seq", 0)
            print(f"[qwen] Registered as '{self.name}' (session: {self.session_id})")
            print(f"[qwen] Online agents: {', '.join(msg.get('agents', []))}")

        elif msg_type == "welcome":
            self.session_id = msg.get("sessionId")
            self.last_seq = msg.get("seq", 0)
            print(f"[qwen] Welcome! session={self.session_id}")

        elif msg_type == "status":
            agents = [f"{a['name']}*" if a.get("executor") else a["name"] for a in msg.get("agents", [])]
            print(f"[qwen] {msg.get('count', 0)} online: {', '.join(agents)}")

        elif msg_type == "chat":
            from_name = msg.get("from", "?")
            text = msg.get("text", "")
            print(f"[chat {from_name}] {text}")

        elif msg_type == "mcp_call":
            await self.handle_mcp_call(msg)

        elif msg_type == "mcp_result":
            rid = msg.get("relayCallId")
            if rid in self.pending_calls:
                fut = self.pending_calls.pop(rid)
                fut.set_result(msg)

        elif msg_type == "error":
            print(f"[qwen] Error: {msg.get('message', '')}")

        elif msg_type == "pong":
            pass

        elif msg_type == "server.shutdown":
            print(f"[qwen] Server shutting down, will reconnect...")

    async def handle_mcp_call(self, msg):
        method = msg.get("method", "") or (msg.get("call") or {}).get("name", "")
        call_id = msg.get("callId", "") or (msg.get("call") or {}).get("id", "")
        relay_call_id = msg.get("relayCallId", "")
        params = msg.get("params", {}) or (msg.get("call") or {}).get("params", {})

        print(f"[qwen] MCP call: {method}({json.dumps(params)[:200]})")

        result = await self.execute_tool(method, params)

        await self.send({
            "type": "mcp_result",
            "relayCallId": relay_call_id,
            "callId": call_id,
            "result": result,
        })

    async def execute_tool(self, method, params):
        if method == "get_environment":
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "agent": self.name,
                        "platform": sys.platform,
                        "python": sys.version,
                    }, indent=2),
                }]
            }

        elif method == "chat":
            text = params.get("text", "")
            print(f"[chat received] {text}")
            return {"content": [{"type": "text", "text": f"Echo: {text}"}]}

        elif method == "execute_command" or method == "execute_bash":
            command = params.get("command", "")
            if not command:
                return {"isError": True, "content": [{"type": "text", "text": "No command provided"}]}
            return await self.run_bash(command)

        elif method == "read_file":
            path = params.get("path", "")
            try:
                with open(path, "r") as f:
                    content = f.read()
                return {"content": [{"type": "text", "text": content}]}
            except Exception as e:
                return {"isError": True, "content": [{"type": "text", "text": str(e)}]}

        elif method == "write_file":
            path = params.get("path", "")
            content = params.get("content", "")
            try:
                with open(path, "w") as f:
                    f.write(content)
                return {"content": [{"type": "text", "text": f"Written {len(content)} bytes"}]}
            except Exception as e:
                return {"isError": True, "content": [{"type": "text", "text": str(e)}]}

        else:
            return {"isError": True, "content": [{"type": "text", "text": f"Unknown tool: {method}"}]}

    async def run_bash(self, command):
        if not subprocess:
            return {"isError": True, "content": [{"type": "text", "text": "subprocess not available"}]}
        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
            text = stdout.decode()
            if stderr:
                text += "\n[stderr]\n" + stderr.decode()
            return {"content": [{"type": "text", "text": text or "(no output)"}]}
        except asyncio.TimeoutError:
            proc.kill()
            return {"isError": True, "content": [{"type": "text", "text": "Command timed out"}]}
        except Exception as e:
            return {"isError": True, "content": [{"type": "text", "text": str(e)}]}

    def stop(self):
        self.running = False


async def main():
    parser = argparse.ArgumentParser(description="Qwen Agent Relay Client")
    parser.add_argument("--name", default="qwen-agent", help="Agent name")
    parser.add_argument("--token", required=True, help="Relay token")
    parser.add_argument("--relay", default="wss://agent-relay-production-c50f.up.railway.app",
                        help="Relay WebSocket URL")
    parser.add_argument("--workspace", default="/tmp", help="Workspace directory")
    args = parser.parse_args()

    agent = QwenAgent(args.name, args.token, args.relay, args.workspace)

    shutdown_event = asyncio.Event()

    def shutdown():
        print("\n[qwen] Shutting down...")
        agent.stop()
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    await asyncio.gather(
        agent.connect(),
        shutdown_event.wait(),
    )


if __name__ == "__main__":
    asyncio.run(main())
