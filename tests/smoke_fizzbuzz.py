#!/usr/bin/env python3
"""End-to-end smoke test: drive tau under tmux to build fizzbuzz.

Launches ``tau`` inside a tmux pane in a throwaway working directory,
then plays a two-step conversation:

1. Ask tau to implement fizzbuzz in ``fizzbuzz.py`` and verify the
   generated program prints the canonical sequence.
2. Ask tau to change the output to all-caps ``FIZZ`` / ``BUZZ`` and
   verify the program now prints the uppercase sequence.

Validation runs the produced file with the Python interpreter rather
than scraping the TUI, so it checks real behaviour.  ``write``/``edit``
to paths under the working directory are auto-approved by tau; any
``bash`` approval prompts are answered automatically.

Run it with::

    uv run python tests/smoke_fizzbuzz.py

It exits 0 on success, 1 on failure, and 77 (skipped) when tmux or an
API key is unavailable.  Set ``TAU_SMOKE_KEEP=1`` to leave the tmux
session and temp dir around for debugging.
"""

from __future__ import annotations

import os
import pathlib
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

TAU_DIR = pathlib.Path(__file__).resolve().parent.parent
SOCKET = "tau-smoke"
SESSION = "tau"
TARGET = "fizzbuzz.py"

READY_TIMEOUT = 120.0
TURN_TIMEOUT = 240.0
POLL_INTERVAL = 2.0
IDLE_POLL = 1.0

SKIP_EXIT = 77

PROMPT_1 = (
    "Implement fizzbuzz in Python in a file named fizzbuzz.py. "
    "It should print the numbers from 1 to 100, one per line, "
    "replacing multiples of 3 with Fizz, multiples of 5 with Buzz, "
    "and multiples of 15 with FizzBuzz. Just write the file."
)
PROMPT_2 = (
    "Now update fizzbuzz.py so it prints FIZZ and BUZZ in all capitals "
    "instead of Fizz and Buzz (so multiples of 15 print FIZZBUZZ)."
)

# Markers that mean tau has finished booting and is ready for input.
READY_MARKERS = ("message tau", "connected — model", "connected - model")
# Markers that mean something went wrong during boot.
FAIL_MARKERS = ("traceback (most recent", "command not found", "no module")
# Composer placeholder text: busy mid-turn vs. idle/ready for input.
BUSY_MARKERS = ("tau is thinking", "type to queue")
IDLE_MARKER = "message tau"
# An active approval prompt renders these option labels.  Matching the
# option line avoids false positives from tau's "approved: <tool>"
# confirmation note, which also contains the word "approve".
PROMPT_MARKERS = ("[y] yes", "[n] no")


def _log(msg: str) -> None:
    print(f"[smoke] {msg}", flush=True)


def _tmux(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["tmux", "-L", SOCKET, *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _capture() -> str:
    proc = _tmux("capture-pane", "-t", SESSION, "-p", check=False)
    return proc.stdout


def _send_text(text: str) -> None:
    _tmux("send-keys", "-t", SESSION, "-l", text)
    time.sleep(0.5)
    _tmux("send-keys", "-t", SESSION, "Enter")


def _send_key(key: str) -> None:
    _tmux("send-keys", "-t", SESSION, "-l", key)


def _prereqs_ok() -> bool:
    if shutil.which("tmux") is None:
        _log("tmux not found on PATH — skipping")
        return False
    keys = ("AI_GATEWAY_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY")
    if not any(os.environ.get(k) for k in keys):
        _log("no API key in environment — skipping")
        return False
    return True


def _start(workdir: pathlib.Path) -> None:
    cmd = (
        f"cd {shlex.quote(str(workdir))} && "
        f"exec uv run --project {shlex.quote(str(TAU_DIR))} tau"
    )
    _tmux(
        "new-session",
        "-d",
        "-s",
        SESSION,
        "-x",
        "200",
        "-y",
        "50",
        cmd,
    )


def _wait_ready() -> bool:
    deadline = time.time() + READY_TIMEOUT
    while time.time() < deadline:
        pane = _capture().lower()
        if any(m in pane for m in READY_MARKERS):
            return True
        if any(m in pane for m in FAIL_MARKERS):
            _log("tau failed to start:\n" + _capture())
            return False
        time.sleep(POLL_INTERVAL)
    _log("timed out waiting for tau to become ready")
    return False


def _classify(i: int) -> str:
    if i % 15 == 0:
        return "fizzbuzz"
    if i % 3 == 0:
        return "fizz"
    if i % 5 == 0:
        return "buzz"
    return str(i)


def _run_program(path: pathlib.Path) -> str | None:
    """Execute the produced program; return stdout or None on failure."""
    if not path.exists():
        return None
    try:
        proc = subprocess.run(
            [sys.executable, str(path)],
            capture_output=True,
            text=True,
            timeout=15,
            cwd=str(path.parent),
        )
    except (subprocess.SubprocessError, OSError):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout


def _matches(output: str | None, *, upper: bool) -> bool:
    """Check the first 15 emitted lines against the fizzbuzz sequence."""
    if output is None:
        return False
    lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    if len(lines) < 15:
        return False
    for i in range(1, 16):
        want = _classify(i)
        got = lines[i - 1]
        if want.isdigit():
            if got != want:
                return False
        elif upper:
            if got != want.upper():
                return False
        elif got.casefold() != want:
            return False
    return True


def _wait_idle(timeout: float) -> bool:
    """Wait for tau to finish the current turn.

    Detects the end of a turn by watching the composer placeholder flip
    from its busy text back to the idle ``message tau…`` prompt.  Any
    bash approval prompt that appears mid-turn is answered first.
    Returns True once tau is idle again, False on timeout.
    """
    deadline = time.time() + timeout
    seen_busy = False
    while time.time() < deadline:
        pane = _capture().lower()
        if all(m in pane for m in PROMPT_MARKERS):
            _log("approving pending tool prompt")
            seen_busy = True
            _send_key("a")
            time.sleep(1.0)
            continue
        if any(m in pane for m in BUSY_MARKERS):
            seen_busy = True
        elif seen_busy and IDLE_MARKER in pane:
            return True
        time.sleep(IDLE_POLL)
    return False


def _teardown(workdir: pathlib.Path) -> None:
    if os.environ.get("TAU_SMOKE_KEEP") == "1":
        _log(f"keeping tmux socket {SOCKET!r} and {workdir}")
        return
    _tmux("kill-server", check=False)
    shutil.rmtree(workdir, ignore_errors=True)


def main() -> int:
    if not _prereqs_ok():
        return SKIP_EXIT

    workdir = pathlib.Path(tempfile.mkdtemp(prefix="tau-smoke-"))
    target = workdir / TARGET
    _log(f"workdir: {workdir}")

    try:
        _start(workdir)
        if not _wait_ready():
            return 1
        _log("tau is ready")

        # Step 1: implement fizzbuzz, then validate once tau is idle.
        _log("sending step 1 (implement fizzbuzz)")
        _send_text(PROMPT_1)
        if not _wait_idle(TURN_TIMEOUT):
            _log("step 1: timed out waiting for tau to finish")
            _log("final pane:\n" + _capture())
            return 1
        if not _matches(_run_program(target), upper=False):
            _log("step 1: fizzbuzz output did not match expected sequence")
            _log("final pane:\n" + _capture())
            return 1
        _log("step 1 (lowercase fizzbuzz): ok")
        step1_output = _run_program(target)

        # Step 2: switch to uppercase, then validate once tau is idle.
        _log("sending step 2 (uppercase FIZZ/BUZZ)")
        _send_text(PROMPT_2)
        if not _wait_idle(TURN_TIMEOUT):
            _log("step 2: timed out waiting for tau to finish")
            _log("final pane:\n" + _capture())
            return 1
        output = _run_program(target)
        if not _matches(output, upper=True):
            _log("step 2: output is not the uppercase FIZZBUZZ sequence")
            _log("final pane:\n" + _capture())
            return 1
        if output == step1_output:
            _log("step 2: output did not change from step 1")
            return 1
        _log("step 2 (uppercase FIZZBUZZ): ok")

        _log("PASS — fizzbuzz implemented and updated to uppercase")
        return 0
    finally:
        _teardown(workdir)


if __name__ == "__main__":
    raise SystemExit(main())
