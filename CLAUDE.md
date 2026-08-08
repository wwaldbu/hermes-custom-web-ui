# Hermes Custom Dashboard

A single-page web dashboard for Hermes Agent with a terminal-style chat interface and live system monitoring.

Only two views: **Terminal** (chat with the agent) and **System** (live resource gauges + server infra).

## Architecture

```
nginx (port 443) → path-based routing
├── /         → server.py (port 8081) — Custom Dashboard
├── /ws       → WebSocket bridge (port 8083) — persistent Hermes agent
├── /reasonix → Reasonix (port 8788)
├── /hermes   → Hermes Dashboard (port 9118)
└── /_auth    → auth-server.py (port 9090) — session cookie auth
```

## Chat architecture

```
Browser (WebSocket) → server.py (WS bridge) → hermes_runner.py (persistent subprocess)
                                                   ↓
                                           hermes chat -q --resume
```

- **server.py** runs HTTP (static files + REST API) on 8081 and a WebSocket server on 8083
- **hermes_runner.py** is a persistent subprocess that loops: read stdin → run `hermes chat -q` → write JSON to stdout
- The runner stays alive between messages, keeping Python modules OS-cached
- No PTY, no ANSI, no terminal chrome — clean text responses via WebSocket
- Session continuity via `--resume` (Hermes session ID tracked in runner)

## Stack

- **Server**: Python stdlib `http.server` — no framework, no dependencies
- **Frontend**: Single `index.html` — inline CSS + vanilla JS, no build step
- **Auth**: `auth_request` via nginx → Python auth server (HMAC session cookies)
- **HTTPS**: Let's Encrypt via certbot on `westwald.io`

## Files

| File | Purpose |
|------|---------|
| `server.py` | HTTP server — serves index.html, WebSocket bridge, `/api/system`, `/api/status` |
| `index.html` | SPA dashboard — Terminal chat, System monitor |
| `hermes_runner.py` | Persistent Hermes agent subprocess (stdin/stdout JSON loop) |

## Server endpoints

| Route | Method | Description |
|-------|--------|-------------|
| `/` | GET | Serves `index.html` |
| `/test` | GET | Health check (`{"status":"ok"}`) |
| `/session` | GET | Loads most recent Hermes session from state.db |
| `/api/status` | GET | Runner liveness (`{busy, alive, runner_pid}`) |
| `/api/system` | GET | CPU, RAM, disk, uptime, processes, server infra |
| `/ws` | WebSocket | Persistent agent chat (JSON: thinking + response) |

## Frontend views

- **Terminal** — Chat interface with Hermes (markdown rendering, collapsible tool call cards, thinking blocks, voice input via Web Speech API)
- **System** — Live CPU/RAM/DISK/UPTIME gauges (5s refresh), top processes, server infra discovery (nginx + backends via `ss -tlnp`)

## Key implementation details

- Inline markdown renderer with syntax-highlighted code blocks
- Collapsible tool call cards and thinking blocks
- Voice mode using browser-native `webkitSpeechRecognition`
- Deep dark theme with frosted glass surfaces and gradient accents
- Mobile responsive with slide-in sidebar on <680px (hamburger menu)