#!/usr/bin/env python3
"""Hermes Web UI — persistent runner backend.

Serves the mobile-friendly frontend and maintains a persistent Hermes agent
subprocess. Browser connects via WebSocket for live chat — each message runs
`hermes chat -q --resume` on the persistent runner so the agent stays cached.
"""

import asyncio
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import websockets
from websockets.asyncio.server import serve as ws_serve

HERMES_HOME = Path(os.environ.get("HERMES_HOME") or Path.home() / ".hermes")
PORT = int(os.environ.get("PORT", 8081))
WS_PORT = int(os.environ.get("WS_PORT", 8083))
STATIC_DIR = Path(__file__).parent
HERMES_VENV = "/usr/local/lib/hermes-agent/venv/bin/python3"
RUNNER = STATIC_DIR / "hermes_runner.py"

# ── Runner pool (one subprocess per tab) ──
import uuid as _uuid


class RunnerPool:
    """Manages multiple hermes_runner.py subprocesses, one per tab."""

    REASONING_LEVELS = ["none", "minimal", "low", "medium", "high", "maximum"]

    def __init__(self):
        self._runners: dict[str, subprocess.Popen] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._busy: dict[str, bool] = {}
        self._reasoning: dict[str, str] = {}
        self._session_ids: dict[str, str] = {}

    def start(self, tab_id: str) -> None:
        """Spawn a hermes_runner.py subprocess for *tab_id*."""
        if tab_id in self._runners and self._runners[tab_id].poll() is None:
            return  # already running
        proc = subprocess.Popen(
            [HERMES_VENV, str(RUNNER)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "TERM": "xterm-256color", "PYTHONUNBUFFERED": "1"},
            text=True,
        )
        self._runners[tab_id] = proc
        self._locks[tab_id] = threading.Lock()
        self._busy[tab_id] = False
        # Default reasoning from config or "medium"
        self._reasoning[tab_id] = "medium"
        sys.stderr.write(f"[hermes-web] runner started tab={tab_id} pid={proc.pid}\n")

    def call(self, tab_id: str, message: str) -> dict:
        """Send a message to the runner for *tab_id* and read the response.

        Thread-safe per-tab. Restarts the runner on crash.
        Includes the tab's reasoning level in the JSON sent to the runner.
        """
        self._busy[tab_id] = True
        lock = self._locks[tab_id]
        with lock:
            try:
                proc = self._runners.get(tab_id)
                if proc is None or proc.poll() is not None:
                    self.start(tab_id)
                    proc = self._runners[tab_id]

                # Send JSON with reasoning level
                payload = json.dumps({"text": message, "reasoning": self._reasoning.get(tab_id, "medium")})
                proc.stdin.write(payload + "\n")
                proc.stdin.flush()

                line = proc.stdout.readline()
                if not line:
                    raise RuntimeError(f"runner tab={tab_id} closed stdout")

                result = json.loads(line.strip())
                # Track session_id for conversation history recovery
                if result.get("session_id"):
                    self._session_ids[tab_id] = result["session_id"]
                return result
            except Exception as e:
                sys.stderr.write(f"[hermes-web] runner error tab={tab_id}: {e}, restarting...\n")
                self._close(tab_id)
                self.start(tab_id)
                return {"content": f"Runner error: {e}", "session_id": ""}
            finally:
                self._busy[tab_id] = False

    def close(self, tab_id: str) -> None:
        """Terminate the runner for *tab_id* and remove it from the pool."""
        self._close(tab_id)
        self._runners.pop(tab_id, None)
        self._locks.pop(tab_id, None)
        self._busy.pop(tab_id, None)
        self._reasoning.pop(tab_id, None)
        self._session_ids.pop(tab_id, None)

    def set_reasoning(self, tab_id: str, level: str) -> bool:
        """Set reasoning level for *tab_id*. Returns True if valid."""
        if level not in self.REASONING_LEVELS:
            return False
        self._reasoning[tab_id] = level
        return True

    def get_reasoning(self, tab_id: str) -> str | None:
        return self._reasoning.get(tab_id)

    def _close(self, tab_id: str) -> None:
        proc = self._runners.get(tab_id)
        if proc:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def close_all(self) -> None:
        for tid in list(self._runners.keys()):
            self.close(tid)

    def list(self) -> list[dict]:
        """Return list of active tab info, including session messages if available."""
        tabs = []
        for tid, proc in self._runners.items():
            tab_info = {
                "tab_id": tid,
                "alive": proc.poll() is None,
                "busy": self._busy.get(tid, False),
                "reasoning": self._reasoning.get(tid, "medium"),
            }
            # Load session messages from state.db for conversation recovery
            sid = self._session_ids.get(tid)
            if sid:
                tab_info["session_id"] = sid
                try:
                    db = HERMES_HOME / "state.db"
                    if db.exists():
                        conn = sqlite3.connect(str(db))
                        cur = conn.execute(
                            "SELECT role, content FROM messages WHERE session_id = ? ORDER BY id ASC LIMIT 50",
                            (sid,),
                        )
                        msgs = [
                            {"role": r, "content": c or ""}
                            for r, c in cur.fetchall()
                            if r in ("user", "assistant")
                        ]
                        conn.close()
                        if msgs:
                            tab_info["messages"] = msgs
                except Exception:
                    pass
            tabs.append(tab_info)
        return tabs

    def any_busy(self) -> bool:
        return any(self._busy.values())


_pool = RunnerPool()


# ── WebSocket handler (multiplexed by tab_id) ──
async def ws_handler(websocket):
    """Handle a WebSocket connection with per-tab routing.

    Messages are JSON:
      {text: "...", tab_id: "..."}         → process on that tab's runner
      {action: "new_tab", tab_id: "..."}   → spawn a runner for this tab
      {action: "close_tab", tab_id: "..."} → kill the runner for this tab
      {action: "get_tabs"}                 → list active tab info
      {action: "set_reasoning", tab_id: "..", level: "medium"} → set reasoning
    """
    try:
        async for raw in websocket:
            # Parse incoming message
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                # Legacy plain-text fallback — treat as message on auto-tab
                tab_id = "default"
                if tab_id not in _pool._runners:
                    _pool.start(tab_id)
                await websocket.send(json.dumps({"type": "thinking", "tab_id": tab_id}))
                result = await asyncio.get_event_loop().run_in_executor(
                    None, _pool.call, tab_id, raw
                )
                content = result.get("content", "")
                if content:
                    await websocket.send(json.dumps({"type": "response", "content": content, "tab_id": tab_id}))
                continue

            action = data.get("action")
            tab_id = data.get("tab_id", "default")

            if action == "new_tab":
                _pool.start(tab_id)
                await websocket.send(json.dumps({"type": "tab_created", "tab_id": tab_id}))

            elif action == "close_tab":
                _pool.close(tab_id)
                await websocket.send(json.dumps({"type": "tab_closed", "tab_id": tab_id}))

            elif action == "get_tabs":
                tabs = _pool.list()
                await websocket.send(json.dumps({"type": "tabs", "tabs": tabs}))

            elif action == "set_reasoning":
                level = data.get("level", "medium")
                ok = _pool.set_reasoning(tab_id, level)
                await websocket.send(json.dumps({
                    "type": "reasoning_set",
                    "tab_id": tab_id,
                    "level": level if ok else None,
                    "ok": ok,
                }))

            elif action == "reset_tab":
                # Kill the runner and start fresh for the same tab
                _pool.close(tab_id)
                _pool.start(tab_id)
                await websocket.send(json.dumps({"type": "tab_reset", "tab_id": tab_id}))

            elif data.get("text") is not None:
                # Normal message — process on the given tab's runner
                text = data["text"]
                await websocket.send(json.dumps({"type": "thinking", "tab_id": tab_id}))
                result = await asyncio.get_event_loop().run_in_executor(
                    None, _pool.call, tab_id, text
                )
                content = result.get("content", "")
                if content:
                    await websocket.send(json.dumps({"type": "response", "content": content, "tab_id": tab_id}))
    except websockets.exceptions.ConnectionClosed:
        pass


# ── HTTP server (static files + REST API) ──
class Handler(SimpleHTTPRequestHandler):

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[hermes-web] {args[0]}\n")

    def _send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, path):
        if path in ("", "/"):
            path = "index.html"
        rel = path.lstrip("/")
        filepath = STATIC_DIR / rel
        if not filepath.exists() or not filepath.is_file():
            self.send_error(404)
            return
        ext = filepath.suffix.lower()
        mime_map = {
            ".html": "text/html",
            ".css": "text/css",
            ".js": "application/javascript",
            ".json": "application/json",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".svg": "image/svg+xml",
            ".ico": "image/x-icon",
            ".woff2": "font/woff2",
        }
        ctype = mime_map.get(ext, "application/octet-stream")
        try:
            data = filepath.read_bytes()
        except Exception:
            self.send_error(500)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/test":
            self._send_json({
                "status": "ok",
                "service": "hermes-webui",
                "version": "1.0.0",
            })

        elif path == "/session":
            session = self._load_recent_session()
            if session:
                self._send_json(session)
            else:
                self._send_json({"conversation_id": None, "messages": []})

        elif path == "/api/status":
            self._send_json({
                "busy": _pool.any_busy(),
                "alive": len(_pool.list()) > 0 and any(t["alive"] for t in _pool.list()),
                "tabs": _pool.list(),
            })

        elif path == "/api/system":
            self._send_json(self._get_system_data())

        else:
            self._serve_static(path)

    def _load_recent_session(self):
        db = HERMES_HOME / "state.db"
        if not db.exists():
            return None
        try:
            conn = sqlite3.connect(str(db))
            cur = conn.execute(
                "SELECT session_id, title, created_at FROM sessions ORDER BY created_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                conn.close()
                return None
            sid, title, ts = row
            cur = conn.execute(
                "SELECT role, content FROM messages WHERE session_id = ? ORDER BY id ASC LIMIT 50",
                (sid,),
            )
            msgs = [
                {"role": r, "content": c or ""}
                for r, c in cur.fetchall()
                if r in ("user", "assistant")
            ]
            conn.close()
            return {"conversation_id": sid, "title": title or "", "messages": msgs}
        except Exception:
            return None

    def _get_system_data(self):
        cpu_lines = []
        try:
            with open("/proc/loadavg") as f:
                cpu_lines = f.read().strip().split()
        except Exception:
            pass
        mem = {}
        try:
            for line in open("/proc/meminfo"):
                parts = line.split()
                if parts[0].startswith("Mem"):
                    mem[parts[0].rstrip(":")] = int(parts[1])
        except Exception:
            pass
        disk = {}
        try:
            r = subprocess.run(
                ["df", "-BG", "/"], capture_output=True, text=True, timeout=5
            )
            parts = r.stdout.splitlines()[1].split()
            disk = {"total": parts[1], "used": parts[2], "avail": parts[3], "pct": parts[4]}
        except Exception:
            pass
        uptime_secs = 0
        try:
            with open("/proc/uptime") as f:
                uptime_secs = int(float(f.read().split()[0]))
        except Exception:
            pass
        procs = []
        try:
            r = subprocess.run(
                ["ps", "aux", "--sort=-%mem", "--no-headers"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            for line in r.stdout.splitlines()[:8]:
                cols = line.split(None, 10)
                if len(cols) >= 11:
                    raw = cols[10]
                    short = raw
                    if "/hermes-agent/venv/bin/python" in raw:
                        m = re.search(r"python3?\s+(.+)$", raw)
                        if m:
                            script = m.group(1).strip()
                            if "hermes_cli.main gateway" in script:
                                short = "hermes (gateway)"
                            elif "hermes dashboard" in script:
                                short = "hermes (dashboard)"
                            elif "server.py" in script:
                                short = "hermes-webui (server)"
                            elif "auth-server" in script:
                                short = "auth-server"
                            elif "/hermes" in script or "hermes_runner" in script:
                                short = "hermes (runner)"
                            else:
                                short = script[:50]
                        else:
                            short = "hermes (python)"
                    elif "/reasonix/cli-" in raw:
                        short = "reasonix (serve)"
                    elif "/usr/bin/dockerd" in raw:
                        short = "dockerd"
                    elif "nginx:" in raw:
                        m = re.search(r"nginx:\s*(\S+)", raw)
                        short = f"nginx ({m.group(1)})" if m else "nginx"
                    elif "sshd" in raw:
                        short = "sshd"
                    else:
                        short = raw[:55]
                    procs.append(
                        {
                            "user": cols[0],
                            "pid": cols[1],
                            "cpu": cols[2],
                            "mem": cols[3],
                            "rss": cols[5],
                            "cmd": short,
                        }
                    )
        except Exception:
            pass
        return {
            "cpu": {
                "cores": os.cpu_count() or 0,
                "load_1m": cpu_lines[0] if cpu_lines else "?",
                "load_5m": cpu_lines[1] if len(cpu_lines) > 1 else "?",
                "load_15m": cpu_lines[2] if len(cpu_lines) > 2 else "?",
            },
            "memory": {
                "total_gb": round(mem.get("MemTotal", 0) / 1024 / 1024, 1),
                "used_gb": round((mem.get("MemTotal", 0) - mem.get("MemAvailable", 0)) / 1024 / 1024, 1),
                "avail_gb": round(mem.get("MemAvailable", 0) / 1024 / 1024, 1),
                "pct": round((1 - mem.get("MemAvailable", 0) / max(mem.get("MemTotal", 1), 1)) * 100, 1),
            },
            "disk": disk,
            "uptime_secs": uptime_secs,
            "processes": procs,
            "server_infra": self._get_server_infra(),
        }

    # ── Known backends (from nginx route config) ──
    _KNOWN_BACKENDS = [
        {"name": "Hermes Web UI",  "port": 8081, "route": "/",          "desc": "Custom dashboard"},
        {"name": "Reasonix",       "port": 8788, "route": "/reasonix",  "desc": "Terminal emulator"},
        {"name": "Hermes Dash",    "port": 9118, "route": "/hermes",    "desc": "Official dashboard"},
        {"name": "Auth Server",    "port": 9090, "route": "/_auth",     "desc": "Session auth"},
    ]

    def _get_server_infra(self):
        """Discover running servers and their route assignments.

        Scans listening TCP ports, matches them against known backends,
        and health-checks each one. Returns:
          servers — list of {name, port, pid, cmd, listening, healthy, route, desc}
          nginx   — {pid, version, uptime, process}
        """
        import socket as _sk

        # ── Parse `ss -tlnp` to find what's listening ──
        listeners = {}  # port -> {pid, cmd}
        try:
            r = subprocess.run(
                ["ss", "-tlnp"],
                capture_output=True, text=True, timeout=5,
            )
            for line in r.stdout.splitlines():
                parts = line.split()
                if len(parts) < 4:
                    continue
                m = re.search(r":(\d+)$", parts[3])
                if not m:
                    continue
                port = int(m.group(1))
                pid_cmd = ""
                if len(parts) > 5:
                    pid_cmd = parts[5]
                pid_m = re.search(r"pid=(\d+)", pid_cmd)
                pid = int(pid_m.group(1)) if pid_m else None
                # Use PID to get full command line from /proc for better naming
                cmd = "?"
                if pid:
                    try:
                        with open(f"/proc/{pid}/cmdline") as cf:
                            raw = cf.read().replace("\0", " ").strip()
                            if raw:
                                cmd = raw
                    except Exception:
                        pass
                if cmd == "?":
                    cmd_m = re.search(r'users:\(\("([^"]+)"', pid_cmd)
                    cmd = cmd_m.group(1) if cmd_m else (pid_cmd if pid_cmd else "?")
                listeners[port] = {"pid": pid, "cmd": cmd}
        except Exception:
            pass

        # ── Build server list ──
        servers = []
        for bk in self._KNOWN_BACKENDS:
            port = bk["port"]
            listener = listeners.get(port)
            listening = listener is not None
            pid = listener["pid"] if listener else None
            cmd = listener["cmd"] if listener else ""
            pname = cmd
            if "auth-server" in cmd:
                pname = "auth-server"
            elif "server.py" in cmd:
                pname = "hermes-webui"
            elif "reasonix" in cmd.lower():
                pname = "reasonix"
            elif "dashboard" in cmd.lower() or "hermes_dash" in cmd.lower():
                pname = "hermes-dash"
            elif "nginx" in cmd.lower():
                pname = "nginx"
            # Health check — TCP connect
            healthy = False
            if listening:
                try:
                    s = _sk.socket(_sk.AF_INET, _sk.SOCK_STREAM)
                    s.settimeout(1.5)
                    s.connect(("127.0.0.1", port))
                    s.close()
                    healthy = True
                except Exception:
                    healthy = False
            servers.append({
                "name": bk["name"],
                "port": port,
                "route": bk["route"],
                "desc": bk["desc"],
                "listening": listening,
                "healthy": healthy,
                "pid": pid,
                "process": pname,
                "path": cmd[:120] if cmd and cmd != "?" else None,
            })

        # ── Nginx info ──
        nginx_info = {"pid": None, "version": None, "uptime_secs": None, "running": False}
        try:
            r = subprocess.run(
                ["pidof", "nginx"],
                capture_output=True, text=True, timeout=5,
            )
            if r.stdout.strip():
                nginx_info["pid"] = int(r.stdout.strip().split()[0])
                nginx_info["running"] = True
        except Exception:
            pass
        try:
            r = subprocess.run(
                ["nginx", "-v"],
                capture_output=True, text=True, timeout=5,
            )
            m = re.search(r"nginx/(\S+)", r.stderr or r.stdout or "")
            if m:
                nginx_info["version"] = m.group(1)
        except Exception:
            pass
        if nginx_info["pid"]:
            try:
                with open(f"/proc/{nginx_info['pid']}/stat") as f:
                    parts = f.read().split()
                    if len(parts) > 21:
                        start_jiffies = int(parts[21])
                        clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
                        uptime_secs = 0
                        try:
                            with open("/proc/uptime") as f2:
                                uptime_secs = float(f2.read().split()[0])
                        except Exception:
                            pass
                        nginx_info["uptime_secs"] = int(uptime_secs - start_jiffies / clk_tck)
            except Exception:
                pass

        return {"servers": servers, "nginx": nginx_info}


# ── Boot ──
def main():
    # WebSocket server (asyncio, background thread)
    async def ws_main():
        async with ws_serve(ws_handler, "127.0.0.1", WS_PORT):
            await asyncio.Event().wait()

    ws_thread = threading.Thread(
        target=lambda: asyncio.run(ws_main()), daemon=True, name="hermes-ws"
    )
    ws_thread.start()

    # HTTP server (main thread)
    httpd = HTTPServer(("127.0.0.1", PORT), Handler)
    print(f"  \033[33m⎔ hermes\033[0m  —  http://127.0.0.1:{PORT}/")
    print(f"   agent: persistent runner (hermes chat -q --resume)")
    print(f"   ws:    127.0.0.1:{WS_PORT}")
    print(f"   ctrl+c to stop\n")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutdown")
        httpd.server_close()
        _pool.close_all()


if __name__ == "__main__":
    main()