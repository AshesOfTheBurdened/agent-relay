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
import signal
import time
import re

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
            "torch_flash": self._cmd_torch_flash,
            "take_photo": self._cmd_photo,
            "sms_list": self._cmd_sms,
            "sms_send": self._cmd_sms_send,
            "device_info": self._cmd_device,
            "sensor_data": self._cmd_sensors,
            "accelerometer": self._cmd_sensor_single,
            "gyroscope": self._cmd_sensor_single,
            "magnetometer": self._cmd_sensor_single,
            "light_sensor": self._cmd_sensor_single,
            "proximity": self._cmd_sensor_single,
            "pressure": self._cmd_sensor_single,
            "humidity": self._cmd_sensor_single,
            "gravity": self._cmd_sensor_single,
            "linear_acceleration": self._cmd_sensor_single,
            "rotation_vector": self._cmd_sensor_single,
            "step_counter": self._cmd_sensor_single,
            "temperature": self._cmd_sensor_single,
            "camera_info": self._cmd_camera_info,
            "video_record": self._cmd_video,
            "media_play": self._cmd_media_play,
            "media_record": self._cmd_media_record,
            "tts": self._cmd_tts,
            "volume": self._cmd_volume,
            "vibrate": self._cmd_vibrate,
            "wifi_scan": self._cmd_wifi_scan,
            "wifi_enable": self._cmd_wifi_enable,
            "wifi_hotspot": self._cmd_wifi_hotspot,
            "bluetooth_scan": self._cmd_bluetooth_scan,
            "bluetooth_enable": self._cmd_bluetooth_enable,
            "cell_info": self._cmd_cell_info,
            "location": self._cmd_location,
            "gps_status": self._cmd_gps_status,
            "display_info": self._cmd_display,
            "storage_info": self._cmd_storage,
            "installed_apps": self._cmd_apps,
            "contacts": self._cmd_contacts,
            "call_log": self._cmd_call_log,
            "make_call": self._cmd_call,
            "notification_list": self._cmd_notification_list,
            "notification_remove": self._cmd_notification_remove,
            "wallpaper": self._cmd_wallpaper,
            "nfc_status": self._cmd_nfc,
            "fingerprint": self._cmd_fingerprint,
            "rotate": self._cmd_rotate,
            "wake_lock": self._cmd_wake_lock,
            "cpu_info": self._cmd_cpu,
            "thermal": self._cmd_thermal,
            "uptime": self._cmd_uptime,
            "reboot": self._cmd_reboot,
            "echo": self._cmd_echo,
            "hash": self._cmd_hash,
            "base64": self._cmd_base64,
            "whoami": self._cmd_whoami,
            "hostname": self._cmd_hostname,
            "calendar": self._cmd_calendar,
        }

        if method in ("accelerometer", "gyroscope", "magnetometer", "light_sensor", "proximity",
                      "pressure", "humidity", "gravity", "linear_acceleration", "rotation_vector",
                      "step_counter", "temperature"):
            return await self._cmd_sensor_single(method, params)

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
        d = await self._sh_json("termux-sms-list -l 20 2>/dev/null", 10)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-sms-list not available")

    async def _cmd_sms_send(self, p):
        number = p.get("number", "")
        text = p.get("text", "")
        if not number:
            return self._err("number required")
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

    async def _cmd_sensor_single(self, sensor_name, params):
        sensor = params.get("sensor", sensor_name.replace("_", ""))
        d = await self._sh_json(f"termux-sensor -s '{sensor}' -n 1 --delay 100 2>/dev/null", 5)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err(f"Sensor '{sensor}' not available")

    async def _cmd_torch_flash(self, p):
        count = int(p.get("count", 3))
        duration = float(p.get("duration", 0.3))
        result = []
        for i in range(count):
            os.system("termux-torch on 2>/dev/null")
            await asyncio.sleep(duration)
            os.system("termux-torch off 2>/dev/null")
            if i < count - 1:
                await asyncio.sleep(duration)
            result.append(f"Flash {i + 1}/{count}")
        return self._text("\n".join(result))

    async def _cmd_camera_info(self, _p):
        d = await self._sh_json("termux-camera-info 2>/dev/null", 5)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-camera-info not available")

    async def _cmd_video(self, p):
        camera = p.get("camera", "back")
        duration = int(p.get("duration", 5))
        path = p.get("path", f"{HOME}/agent-video.mp4")
        cmd = f"termux-camera-photo -c {camera} --duration {duration} {json.dumps(path)} 2>/dev/null"
        out = await self._sh(cmd, duration + 5)
        if os.path.exists(path):
            return self._text(f"Video saved to {path} ({os.path.getsize(path)} bytes)")
        return self._text(out or "Video capture failed")

    async def _cmd_media_play(self, p):
        path = p.get("path", "")
        if not path:
            return self._err("path required")
        await self._sh(f"termux-media-player play {json.dumps(path)} 2>/dev/null", 5)
        return self._text(f"Playing: {path}")

    async def _cmd_media_record(self, p):
        duration = int(p.get("duration", 10))
        path = p.get("path", f"{HOME}/agent-recording.{p.get('format', 'm4a')}")
        limit = f"--limit {duration}" if duration else ""
        cmd = f"termux-media-recorder {limit} {json.dumps(path)} 2>/dev/null"
        out = await self._sh(cmd, duration + 5)
        if os.path.exists(path):
            return self._text(f"Recording saved to {path} ({os.path.getsize(path)} bytes)")
        return self._text(out or "Recording failed")

    async def _cmd_tts(self, p):
        text = p.get("text", "")
        if not text:
            return self._err("text required")
        engine = f"--engine {p['engine']}" if p.get("engine") else ""
        pitch = f"--pitch {p['pitch']}" if p.get("pitch") else ""
        rate = f"--rate {p['rate']}" if p.get("rate") else ""
        lang = f"{p['lang']}" if p.get("lang") else ""
        cmd = f"termux-tts-speak {engine} {pitch} {rate} {lang} {json.dumps(text)} 2>/dev/null"
        await self._sh(cmd, 10)
        return self._text(f"TTS: {text[:100]}")

    async def _cmd_volume(self, p):
        action = p.get("action", "get")
        if action == "get":
            stream = p.get("stream", "music")
            d = await self._sh_json(f"termux-volume 2>/dev/null", 5)
            if d:
                return self._text(json.dumps(d, indent=2, default=str))
            return self._err("termux-volume not available")
        elif action == "set":
            stream = p.get("stream", "music")
            level = int(p.get("level", 50))
            await self._sh(f"termux-volume {stream} {level} 2>/dev/null", 5)
            return self._text(f"Volume set: {stream} = {level}")

    async def _cmd_vibrate(self, p):
        duration = int(p.get("duration", 1000))
        await self._sh(f"termux-vibrate -d {duration} 2>/dev/null", 5)
        return self._text(f"Vibrated for {duration}ms")

    async def _cmd_wifi_scan(self, _p):
        d = await self._sh_json("termux-wifi-scaninfo 2>/dev/null", 10)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-wifi-scaninfo not available")

    async def _cmd_wifi_enable(self, p):
        enabled = p.get("enabled", True)
        val = "true" if enabled else "false"
        await self._sh(f"termux-wifi-enable {val} 2>/dev/null", 5)
        return self._text(f"WiFi {'enabled' if enabled else 'disabled'}")

    async def _cmd_wifi_hotspot(self, p):
        action = p.get("action", "start")
        ssid = p.get("ssid", "agent-relay-hotspot")
        passphrase = p.get("passphrase", "")
        if action == "start":
            cmd = f"termux-wifi-hotspot --ssid {json.dumps(ssid)}"
            if passphrase:
                cmd += f" --passphrase {json.dumps(passphrase)}"
            cmd += " 2>/dev/null"
            await self._sh(cmd, 10)
            return self._text(f"Hotspot '{ssid}' started")
        else:
            await self._sh("termux-wifi-hotspot stop 2>/dev/null", 5)
            return self._text("Hotspot stopped")

    async def _cmd_bluetooth_scan(self, _p):
        d = await self._sh_json("termux-bt-scan 2>/dev/null", 15)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-bt-scan not available")

    async def _cmd_bluetooth_enable(self, p):
        enabled = p.get("enabled", True)
        val = "1" if enabled else "0"
        await self._sh(f"termux-bt-enable {val} 2>/dev/null", 5)
        return self._text(f"Bluetooth {'enabled' if enabled else 'disabled'}")

    async def _cmd_cell_info(self, _p):
        d = await self._sh_json("termux-telephony-cellinfo 2>/dev/null", 5)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-telephony-cellinfo not available")

    async def _cmd_location(self, p):
        provider = p.get("provider", "gps")
        d = await self._sh_json(f"termux-location -p {provider} 2>/dev/null", 10)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-location not available")

    async def _cmd_gps_status(self, _p):
        d = await self._sh_json("termux-location -p gps -r 2>/dev/null", 10)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("GPS status not available")

    async def _cmd_display(self, _p):
        info = {}
        info["brightness"] = await self._sh("termux-brightness 2>/dev/null", 5)
        info["sensors"] = await self._sh_json("termux-sensor -s 'Rotation Vector' -n 1 2>/dev/null", 5)
        return self._text(json.dumps(info, indent=2, default=str))

    async def _cmd_storage(self, _p):
        d = {}
        for pth in ["/storage/emulated/0", "/sdcard", "/data"]:
            if os.path.exists(pth):
                try:
                    usage = await self._sh(f"df -h {pth} 2>/dev/null | tail -1", 5)
                    d[pth] = usage.strip()
                except:
                    pass
        d["home"] = HOME
        return self._text(json.dumps(d, indent=2))

    async def _cmd_apps(self, _p):
        d = await self._sh_json("pm list packages 2>/dev/null | sort", 10)
        if not d:
            raw = await self._sh("pm list packages 2>/dev/null | head -200", 10)
            return self._text(raw) if raw.strip() else self._err("Cannot list packages")
        return self._text(json.dumps(d, indent=2))

    async def _cmd_contacts(self, _p):
        d = await self._sh_json("termux-contact-list 2>/dev/null", 10)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-contact-list not available")

    async def _cmd_call_log(self, _p):
        d = await self._sh_json("termux-call-log 2>/dev/null", 10)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-call-log not available")

    async def _cmd_call(self, p):
        number = p.get("number", "")
        if not number:
            return self._err("number required")
        await self._sh(f"termux-telephony-call {json.dumps(number)} 2>/dev/null", 5)
        return self._text(f"Calling {number}")

    async def _cmd_notification_list(self, _p):
        d = await self._sh_json("termux-notification-list 2>/dev/null", 5)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-notification-list not available")

    async def _cmd_notification_remove(self, p):
        nid = p.get("id", "agent-relay")
        await self._sh(f"termux-notification-remove {json.dumps(nid)} 2>/dev/null", 5)
        return self._text(f"Notification '{nid}' removed")

    async def _cmd_wallpaper(self, p):
        action = p.get("action", "get")
        if action == "set":
            path = p.get("path", "")
            if not path:
                return self._err("path required")
            await self._sh(f"termux-wallpaper -f {json.dumps(path)} 2>/dev/null", 5)
            return self._text(f"Wallpaper set to {path}")
        raw = await self._sh("termux-wallpaper 2>/dev/null", 5)
        return self._text(raw or "Wallpaper info not available")

    async def _cmd_nfc(self, _p):
        d = await self._sh_json("termux-nfc 2>/dev/null", 5)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-nfc not available")

    async def _cmd_fingerprint(self, _p):
        d = await self._sh_json("termux-fingerprint 2>/dev/null", 10)
        if d:
            return self._text(json.dumps(d, indent=2, default=str))
        return self._err("termux-fingerprint not available")

    async def _cmd_rotate(self, p):
        enabled = p.get("enabled", True)
        val = "true" if enabled else "false"
        await self._sh(f"termux-sensor -s 'Rotation Vector' -n 1 2>/dev/null", 2)
        os.system(f"settings put system accelerometer_rotation {'1' if enabled else '0'} 2>/dev/null")
        return self._text(f"Auto-rotate {'enabled' if enabled else 'disabled'}")

    async def _cmd_wake_lock(self, p):
        action = p.get("action", "acquire")
        if action == "acquire" or action == "lock":
            self._acquire_wake_lock()
            return self._text("Wake lock acquired")
        else:
            self._release_wake_lock()
            return self._text("Wake lock released")

    async def _cmd_cpu(self, _p):
        info = {}
        try:
            with open("/proc/cpuinfo") as f:
                raw = f.read()
                cores = re.findall(r"processor\s+:\s+(\d+)", raw)
                model = re.findall(r"Hardware\s+:\s+(.+)", raw)
                info["cores"] = len(cores)
                if model:
                    info["model"] = model[0].strip()
                info["arch"] = platform.machine()
        except:
            info["arch"] = platform.machine()
        info["load"] = os.getloadavg() if hasattr(os, "getloadavg") else "N/A"
        return self._text(json.dumps(info, indent=2))

    async def _cmd_thermal(self, _p):
        thermal = {}
        try:
            for f in sorted(os.listdir("/sys/class/thermal/") or []):
                tpath = f"/sys/class/thermal/{f}/temp"
                if os.path.exists(tpath):
                    with open(tpath) as tf:
                        raw = tf.read().strip()
                        thermal[f] = f"{float(raw) / 1000:.1f}°C"
        except:
            pass
        return self._text(json.dumps(thermal or {"error": "No thermal zones"}, indent=2))

    async def _cmd_uptime(self, _p):
        raw = await self._sh("uptime 2>/dev/null", 5)
        return self._text(raw or str(time.monotonic()))

    async def _cmd_reboot(self, p):
        confirm = p.get("confirm", "")
        if confirm != "yes":
            return self._err("Set confirm='yes' to reboot")
        await self._sh("termux-reboot 2>/dev/null", 5)
        return self._text("Rebooting...")

    async def _cmd_echo(self, p):
        return self._text(p.get("text", ""))

    async def _cmd_hash(self, p):
        text = p.get("text", "")
        algo = p.get("algorithm", "sha256")
        import hashlib
        h = hashlib.new(algo, text.encode())
        return self._text(h.hexdigest())

    async def _cmd_base64(self, p):
        text = p.get("text", "")
        action = p.get("action", "encode")
        import base64 as b64
        if action == "encode":
            return self._text(b64.b64encode(text.encode()).decode())
        return self._text(b64.b64decode(text.encode()).decode(errors="replace"))

    async def _cmd_whoami(self, _p):
        return self._text(os.environ.get("USER", "unknown"))

    async def _cmd_hostname(self, _p):
        return self._text(platform.node())

    async def _cmd_calendar(self, _p):
        raw = await self._sh("date '+%Y-%m-%d %H:%M:%S %Z' 2>/dev/null", 5)
        return self._text(raw.strip())

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
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    await asyncio.gather(agent.connect(), shutdown_event.wait())


if __name__ == "__main__":
    asyncio.run(main())
