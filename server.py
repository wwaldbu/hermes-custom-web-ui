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

# ── Persistent Hermes runner ──
_runner_proc: subprocess.Popen | None = None
_runner_lock = threading.Lock()
_runner_busy = False  # True while a message is being processed


def start_runner():
    """Start the persistent hermes_runner.py subprocess."""
    global _runner_proc
    _runner_proc = subprocess.Popen(
        [HERMES_VENV, str(RUNNER)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ, "TERM": "xterm-256color", "PYTHONUNBUFFERED": "1"},
        text=True,
    )
    sys.stderr.write(f"[hermes-web] runner started pid={_runner_proc.pid}\n")


def call_runner(message: str) -> dict:
    """Send a message to the runner and read the response.

    Thread-safe. Sets _runner_busy while processing.
    Returns dict with 'content' and 'session_id'.
    """
    global _runner_proc, _runner_busy
    _runner_busy = True
    with _runner_lock:
        try:
            if _runner_proc is None or _runner_proc.poll() is not None:
                start_runner()

            # Write message
            _runner_proc.stdin.write(message + "\n")
            _runner_proc.stdin.flush()

            # Read response (one JSON line)
            line = _runner_proc.stdout.readline()
            if not line:
                raise RuntimeError("runner closed stdout")

            return json.loads(line.strip())
        except Exception as e:
            # Restart on error
            sys.stderr.write(f"[hermes-web] runner error: {e}, restarting...\n")
            try:
                _runner_proc.terminate()
                _runner_proc.wait(timeout=5)
            except Exception:
                pass
            start_runner()
            return {"content": f"Runner error: {e}", "session_id": ""}
        finally:
            _runner_busy = False


# ── WebSocket handler ──
async def ws_handler(websocket):
    """Handle a WebSocket connection: user messages → runner → response.

    Sends a `{"type":"thinking"}` signal immediately on receipt so the
    frontend can animate a typing indicator before the response arrives.
    """
    try:
        async for msg in websocket:
            # Signal thinking immediately
            await websocket.send(json.dumps({"type": "thinking"}))

            # Process (blocks until runner responds)
            result = await asyncio.get_event_loop().run_in_executor(
                None, call_runner, msg
            )
            content = result.get("content", "")
            if content:
                await websocket.send(json.dumps({"type": "response", "content": content}))
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
                "busy": _runner_busy,
                "alive": _runner_proc is not None and _runner_proc.poll() is None,
                "runner_pid": _runner_proc.pid if _runner_proc and _runner_proc.poll() is None else None,
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
    start_runner()

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
        if _runner_proc:
            _runner_proc.terminate()
            try:
                _runner_proc.wait(timeout=3)
            except Exception:
                _runner_proc.kill()


if __name__ == "__main__":
    main()