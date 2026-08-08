#!/usr/bin/env python3
"""Hermes persistent runner.

Reads messages (one per line) from stdin, runs `hermes chat -q --resume`,
and writes JSON responses to stdout. Stays alive between calls so the
Python/toolchain cache stays hot.

Protocol:
  stdin:  one message per line
  stdout: one JSON line per response: {"content":"...","session_id":"..."}
"""

import json
import os
import re
import subprocess
import sys

HERMES_VENV = "/usr/local/lib/hermes-agent/venv/bin/python3"
ENV = {**os.environ, "TERM": "xterm-256color"}


def run_hermes(query: str, sid: str | None) -> dict:
    """Run `hermes chat -q` and return parsed result."""
    cmd = [
        HERMES_VENV, "-m", "hermes_cli.main",
        "chat", "-q", query,
        "--quiet", "--no-restore-cwd",
    ]
    if sid:
        cmd += ["--resume", sid]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300, env=ENV)
    except subprocess.TimeoutExpired:
        return {"content": "Request timed out after 5 minutes.", "session_id": sid or ""}
    except Exception as e:
        return {"content": f"Agent error: {e}", "session_id": sid or ""}

    stdout = (proc.stdout or "").strip()
    stderr = proc.stderr or ""

    # Extract new session ID from stderr
    new_sid = sid
    m = re.search(r"session_id:\s*(\S+)", stderr)
    if m:
        new_sid = m.group(1)

    if not stdout and proc.returncode != 0:
        stdout = f"Agent exited ({proc.returncode})"

    return {"content": stdout or "(no response)", "session_id": new_sid or ""}


def main():
    sid = None
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        result = run_hermes(line, sid)
        if result.get("session_id"):
            sid = result["session_id"]

        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()