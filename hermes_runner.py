#!/usr/bin/env python3
"""Hermes persistent runner with PTY streaming.

Reads one JSON message per line from stdin, runs `hermes chat -q --resume`
with a pseudo-terminal so output streams in real time, and writes JSON
lines to stdout per chunk. Keeps the runner alive between calls so the
Python/toolchain cache stays hot.

Protocol (stdin → one JSON line):
  {"text": "...", "reasoning": "medium"}   (reasoning optional)

Protocol (stdout → multiple JSON lines per message):
  {"type": "token", "content": "partial..."}    (zero or more)
  {"type": "done", "content": "full...", "session_id": "..."}
"""

import json
import os
import pty
import re
import select
import signal
import subprocess
import sys
import time

REASONING_LEVELS = {"none", "minimal", "low", "medium", "high", "maximum"}
HERMES_VENV = "/usr/local/lib/hermes-agent/venv/bin/python3"
STREAM_TIMEOUT = 600  # seconds before we kill hung processes


def run_hermes_stream(query: str, sid: str | None, reasoning: str | None = None):
    """Run `hermes chat -q` with a PTY, yielding chunks as they arrive.

    Yields:
      ("token", chunk_text) for each readable chunk from the subprocess.
      ("done", full_text, session_id) once the process exits.
    """
    cmd = [
        HERMES_VENV, "-m", "hermes_cli.main",
        "chat", "-q", query,
        "--quiet", "--no-restore-cwd",
    ]
    if sid:
        cmd += ["--resume", sid]
    if reasoning and reasoning in REASONING_LEVELS:
        cmd += ["--reasoning", reasoning]

    # ── PTY setup ──
    master_fd, slave_fd = pty.openpty()
    env = {**os.environ, "TERM": "xterm-256color"}

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=slave_fd,
        stderr=subprocess.PIPE,  # captured separately for session_id
        env=env,
        text=True,
        close_fds=True,
    )
    os.close(slave_fd)
    proc.stdin.close()  # no input to the subprocess itself

    # ── Streaming read loop ──
    full_output: list[str] = []
    deadline = time.monotonic() + STREAM_TIMEOUT

    try:
        while True:
            # Check timeout
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                os.kill(proc.pid, signal.SIGTERM)
                yield "token", "\n\n[Agent timed out after 10 minutes]"
                break

            r, _, _ = select.select([master_fd], [], [], min(remaining, 1.0))
            if r:
                try:
                    chunk = os.read(master_fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                text = chunk.decode("utf-8", errors="replace")
                full_output.append(text)
                stripped = text.strip()
                if stripped:
                    yield "token", text

            # If process exited, drain any remaining data from the PTY
            if proc.poll() is not None:
                try:
                    while True:
                        chunk = os.read(master_fd, 4096)
                        if not chunk:
                            break
                        text = chunk.decode("utf-8", errors="replace")
                        full_output.append(text)
                        if text.strip():
                            yield "token", text
                except OSError:
                    pass
                break
    finally:
        # Clean up
        try:
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=3)
            except Exception:
                pass
        try:
            os.close(master_fd)
        except OSError:
            pass

    # Read stderr to extract session_id
    stderr_text = ""
    try:
        stderr_text = (proc.stderr or "").read() if proc.stderr else ""
    except Exception:
        pass

    new_sid = sid
    m = re.search(r"session_id:\s*(\S+)", stderr_text)
    if m:
        new_sid = m.group(1)

    full_text = "".join(full_output).strip()
    if not full_text and proc.returncode and proc.returncode != 0:
        full_text = f"Agent exited ({proc.returncode})"

    yield "done", full_text or "(no response)", new_sid or ""


def main():
    sid = None
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Parse JSON or fall back to plain text
        text = line
        reasoning = None
        try:
            data = json.loads(line)
            if isinstance(data, dict):
                text = data.get("text", line)
                reasoning = data.get("reasoning")
        except json.JSONDecodeError:
            pass

        # Stream tokens
        for event in run_hermes_stream(text, sid, reasoning=reasoning):
            if event[0] == "token":
                print(json.dumps({"type": "token", "content": event[1]}), flush=True)
            elif event[0] == "done":
                _content, _sid = event[1], event[2]
                if _sid:
                    sid = _sid
                print(json.dumps({"type": "done", "content": _content, "session_id": _sid}), flush=True)


if __name__ == "__main__":
    main()