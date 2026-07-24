#!/usr/bin/env python3
"""
Termux Agent — Android phone agent for agent-relay v2.
Optimized for Termux on Android. Minimal dependencies.

Install:
  pkg install python -y && pip install websockets && curl -O https://raw.githubusercontent.com/AshesOfTheBurdened/agent-relay/main/termux-agent.py

Run:
  python3 termux-agent.py --name agent-android --token YOUR_TOKEN

Keeps the phone awake with termux-wake-lock when available.
Auto-reconnects with exponential backoff + jitter.
"""

import asyncio
import json
import os
import sys
import random
import argparse
import traceback
import platform

try:
    import websockets
except ImportError:
    print("ERROR: Install websockets: pip install websockets")
    sys.exit(1)

HOME = os.environ.get("HOME", "/data/data/com.termux/files/home")
IS_ANDROID = os.path.exists("/system/bin/sh") or "com.termux" in HOME


class TermuxAgent:
    def __init__(self, name, token, relay_url):
        self.name = name
        self.token = token
        self.relay_url = relay_url
        self.workspace = HOME
        self.ws = None
        self.session_id = None
        self.last_seq = 0
        self.reconnect_attempt = 0
        self.running = True
        self.wake_lock_held = False

    async def connect(self):
        while self.running:
            try:
                print(f"[termux] Connecting to {self.relay_url} as '{self.name}'...")
                self.ws = await websockets.connect(
                    self.relay_url, max_size=1024 * 1024,
                    ping_interval=15, ping_timeout=10,
                )
                self.reconnect_attempt = 0
                print(f"[termux] Connected!")

                await self._send({
                    "type": "join",
                    "name": self.name,
                    "token": self.token,
                    "sessionId": self.session_id,
                    "lastSeq": self.last_seq,
                    "executor": True,
                })

                self._acquire_wake_lock()

                async for raw in self.ws:
                    try:
                        msg = json.loads(raw)
                        await self._handle(msg)
                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        print(f"[termux] Handler error: {e}")

            except websockets.exceptions.ConnectionClosed as e:
                print(f"[termux] Disconnected (code={e.code})")
            except (OSError, asyncio.TimeoutError) as e:
                print(f"[termux] Connection error: {e}")
            except Exception as e:
                print(f"[termux] Error: {e}")
                traceback.print_exc()

            self._release_wake_lock()
            if self.running:
                await self._reconnect_delay()

    def _acquire_wake_lock(self):
        if IS_ANDROID and not self.wake_lock_held:
            try:
                os.system("termux-wake-lock 2>/dev/null")
                self.wake_lock_held = True
                print("[termux] Wake lock acquired")
            except:
                pass

    def _release_wake_lock(self):
        if IS_ANDROID and self.wake_lock_held:
            try:
                os.system("termux-wake-unlock 2>/dev/null")
            except:
                pass
            self.wake_lock_held = False

    async def _reconnect_delay(self):
        base = min(1000 * (2 ** self.reconnect_attempt), 30000)
        jitter = base * 0.3 * random.random()
        delay = (base + jitter) / 1000.0
        self.reconnect_attempt += 1
        print(f"[termux] Reconnect in {delay:.1f}s (attempt {self.reconnect_attempt})")
        await asyncio.sleep(delay)

    async def _send(self, msg):
        if self.ws:
            try:
                await self.ws.send(json.dumps(msg))
            except:
                pass

    async def _handle(self, msg):
        t = msg.get("type")

        if t == "joined":
            self.session_id = msg.get("id") or msg.get("sessionId")
            self.last_seq = msg.get("seq", 0)
            names = ", ".join(msg.get("agents", []))
            print(f"[termux] Registered! Online: {names or 'just me'}")

        elif t == "status":
            names = ", ".join(
                f"{a['name']}*" if a.get("executor") else a["name"]
                for a in msg.get("agents", [])
            )
            print(f"[termux] [{msg.get('count')}] {names}")

        elif t == "chat":
            print(f"[chat {msg.get('from')}] {msg.get('text')}")

        elif t == "mcp_call":
            await self._handle_call(msg)

        elif t == "error":
            print(f"[termux] Error: {msg.get('message')}")

        elif t == "pong" or t == "heartbeat.ack":
            pass

        elif t == "server.shutdown":
            print("[termux] Server shutting down...")

    async def _handle_call(self, msg):
        method = msg.get("method") or (msg.get("call") or {}).get("name", "")
        call_id = msg.get("callId") or (msg.get("call") or {}).get("id", "")
        rid = msg.get("relayCallId", "")
        params = msg.get("params") or (msg.get("call") or {}).get("params", {})

        result = await self._exec(method, params)

        await self._send({
            "type": "mcp_result",
            "relayCallId": rid,
            "callId": call_id,
            "result": result,
        })

    async def _exec(self, method, params):
        handlers = {
            "get_environment": self._cmd_env,
            "execute_command": self._cmd_sh,
            "read_file": self._cmd_read,
            "write_file": self._cmd_write,
            "list_directory": self._cmd_ls,
            "battery_status": self._cmd_battery,
            "send_notification": self._cmd_notify,
            "clipboard_get": self._cmd_clip_get,
            "clipboard_set": self._cmd_clip_set,
            "torch": self._cmd_torch,
            "take_photo": self._cmd_photo,
            "sms_list": self._cmd_sms,
            "device_info": self._cmd_device,
            "sensor_data": self._cmd_sensors,
        }

        h = handlers.get(method)
        if h:
            return await h(params)
        return {"isError": True, "content": [{"type": "text", "text": f"Unknown tool: {method}"}]}

    def _text(self, text):
        return {"content": [{"type": "text", "text": str(text)}]}

    def _err(self, text):
        return {"isError": True, "content": [{"type": "text", "text": str(text)}]}

    async def _sh(self, cmd, timeout=30):
        try:
            proc = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            out = stdout.decode(errors="replace")
            err = stderr.decode(errors="replace")
            if err:
                out += "\n[stderr]\n" + err
            return out or "(no output)"
        except asyncio.TimeoutError:
            proc.kill()
            return "(timed out)"
        except Exception as e:
            return str(e)

    async def _sh_json(self, cmd, timeout=10):
        raw = await self._sh(cmd, timeout)
        try:
            return json.loads(raw)
        except:
            return None

    async def _cmd_env(self, _p):
        env = {
            "agent": self.name,
            "platform": "android",
            "python": sys.version,
            "arch": platform.machine(),
            "host": platform.node(),
            "termux": IS_ANDROID,
        }
        if IS_ANDROID:
            for k in ["TERMUX_VERSION", "TERMUX_APP__UID", "TERMUX_APK_RELEASE"]:
                v = os.environ.get(k)
                if v:
                    env[k.lower()] = v
        return self._text(json.dumps(env, indent=2))

    async def _cmd_sh(self, p):
        cmd = p.get("command", "")
        if not cmd:
            return self._err("command required")
        timeout = min(int(p.get("timeout", 30)), 60)
        out = await self._sh(cmd, timeout)
        return self._text(out)

    async def _cmd_read(self, p):
        path = p.get("path", "")
        try:
            with open(path) as f:
                return self._text(f.read())
        except Exception as e:
            return self._err(str(e))

    async def _cmd_write(self, p):
        path = p.get("path", "")
        content = p.get("content", "")
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w") as f:
                f.write(content)
            return self._text(f"Wrote {len(content)}b to {path}")
        except Exception as e:
            return self._err(str(e))

    async def _cmd_ls(self, p):
        path = p.get("path", ".")
        try:
            entries = os.listdir(path)
            return self._text("\n".join(sorted(entries)) or "(empty)")
        except Exception as e:
            return self._err(str(e))

    async def _cmd_battery(self, _p):
        d = await self._sh_json("termux-battery-status 2>/dev/null", 5)
        if d:
            return self._text(json.dumps(d, indent=2))
        return self._err("termux-battery-status not available (install Termux:API)")

    async def _cmd_notify(self, p):
        title = p.get("title", "Agent Relay")
        content = p.get("content", "")
        action = f"--action {p.get('action', '')}" if p.get("action") else ""
        cmd = f"termux-notification --title {json.dumps(title)} --content {json.dumps(content)} {action} --id agent-relay 2>/dev/null"
        await self._sh(cmd, 5)
        return self._text(f"Notification sent: {title}")

    async def _cmd_clip_get(self, _p):
        raw = await self._sh("termux-clipboard-get 2>/dev/null", 5)
        return self._text(raw or "(empty)")

    async def _cmd_clip_set(self, p):
        text = p.get("text", "")
        await self._sh(f"termux-clipboard-set {json.dumps(text)} 2>/dev/null", 5)
        return self._text(f"Clipboard set ({len(text)} chars)")

    async def _cmd_torch(self, p):
        on = p.get("on", True)
        cmd = "termux-torch on 2>/dev/null" if on else "termux-torch off 2>/dev/null"
        await self._sh(cmd, 5)
        return self._text(f"Torch {'on' if on else 'off'}")

    async def _cmd_photo(self, p):
        camera = p.get("camera", "back")
        path = p.get("path", f"{HOME}/agent-photo.jpg")
        cmd = f"termux-camera-photo -c {camera} {path} 2>/dev/null"
        out = await self._sh(cmd, 15)
        if os.path.exists(path):
            return self._text(f"Photo saved to {path} ({os.path.getsize(path)} bytes)")
        return self._text(out or "Photo capture failed")

    async def _cmd_sms(self, p):
        action = p.get("action", "list")
        if action == "list":
            d = await self._sh_json("termux-sms-list -l 20 2>/dev/null", 10)
            if d:
                return self._text(json.dumps(d, indent=2, default=str))
            return self._err("termux-sms-list not available")
        elif action == "send":
            number = p.get("number", "")
            text = p.get("text", "")
            cmd = f"termux-sms-send -n {json.dumps(number)} {json.dumps(text)} 2>/dev/null"
            await self._sh(cmd, 10)
            return self._text(f"SMS sent to {number}")

    async def _cmd_device(self, _p):
        info = {}
        info["battery"] = await self._sh_json("termux-battery-status 2>/dev/null", 5)
        info["wifi"] = await self._sh_json("termux-wifi-connectioninfo 2>/dev/null", 5)
        info["location"] = await self._sh_json("termux-location 2>/dev/null", 5)
        info["sensors"] = await self._sh_json("termux-sensor -s '.*' -n 1 --delay 100 2>/dev/null", 5)
        return self._text(json.dumps(info, indent=2, default=str))

    async def _cmd_sensors(self, _p):
        d = await self._sh_json("termux-sensor -s '.*' -n 1 --delay 100 2>/dev/null", 5)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-sensor not available")

    def stop(self):
        self.running = False
        self._release_wake_lock()


async def main():
    parser = argparse.ArgumentParser(description="Termux Agent — Android phone agent")
    parser.add_argument("--name", default="agent-android", help="Agent name")
    parser.add_argument("--token", required=True, help="Relay token")
    parser.add_argument("--relay", default="wss://agent-relay-production-c50f.up.railway.app",
                        help="Relay WebSocket URL")
    args = parser.parse_args()

    agent = TermuxAgent(args.name, args.token, args.relay)
    print(f"[termux] Starting '{args.name}' on {platform.machine()}")
    print(f"[termux] Android: {'yes' if IS_ANDROID else 'no'}")

    shutdown_event = asyncio.Event()

    def shutdown():
        print("\n[termux] Shutting down...")
        agent.stop()
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    await asyncio.gather(agent.connect(), shutdown_event.wait())


if __name__ == "__main__":
    asyncio.run(main())
