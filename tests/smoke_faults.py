#!/usr/bin/env python3
"""Smoke test: inject API failures and check tau surfaces and recovers.

Runs tau under tmux (reusing the driver helpers from
``smoke_fizzbuzz``) with its API base URL pointed at a local
fault-injecting proxy (``fault_proxy.FaultProxy``) that relays to the
real backend.  Two redirection strategies, picked by available key:

- ``ANTHROPIC_API_KEY``: run with ``TAU_MODEL=anthropic:...`` and the
  provider's native ``ANTHROPIC_BASE_URL`` override.
- ``AI_GATEWAY_API_KEY``: the gateway provider has no base-URL env
  override, so a ``sitecustomize.py`` shim on ``PYTHONPATH`` patches
  ``ai.providers.ai_gateway.provider._BASE_URL`` at interpreter
  startup (it is read at call time by ``from_modelsdev_provider``).

Three scenarios:

1. ``error``  — the API returns an error status for the whole turn
   (more times than tau's retry budget).  Expect: tau shows an
   ``error:`` system message and goes idle.
1b. transient — a single 529, then the API recovers.  Expect: tau
   retries silently (a retry notice, no ``error:``) and completes.
2. ``cut``    — the SSE stream is severed mid-response, repeatedly.
   Expect: tau retries, exhausts, surfaces an error, and goes idle.
3. recovery   — with faults cleared, a normal turn asks tau to write
   a file; the file's presence proves the session survived.

Run it with::

    uv run python tests/smoke_faults.py

Exits 0 on success, 1 on failure, 77 (skipped) when tmux or
``ANTHROPIC_API_KEY`` is unavailable.  Set ``TAU_SMOKE_KEEP=1`` to
keep the tmux session and temp dir around for debugging.
"""

from __future__ import annotations

import json
import os
import pathlib
import re
import shlex
import shutil
import sys
import tempfile
import time
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import smoke_fizzbuzz as base  # noqa: E402
from fault_proxy import FaultProxy  # noqa: E402

# Use a dedicated tmux socket so this test can't collide with the
# fizzbuzz smoke test.  The base helpers read these globals at call
# time, so patching them redirects every helper.
base.SOCKET = "tau-smoke-faults"

ERROR_TIMEOUT = 120.0
TARGET = "ok.txt"

PROMPT_TRIVIAL = "Reply with just the word ok."
PROMPT_RECOVERY = (
    f"Create a file named {TARGET} containing exactly the word OK. "
    "Just write the file."
)


def _log(msg: str) -> None:
    print(f"[smoke-faults] {msg}", flush=True)


_SHIM = """\
import os

url = os.environ.get("TAU_SMOKE_GATEWAY_URL")
if url:
    try:
        from ai.providers.ai_gateway import provider

        provider._BASE_URL = url
    except Exception:
        pass
"""


def _backend() -> str | None:
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.environ.get("AI_GATEWAY_API_KEY"):
        return "gateway"
    return None


def _prereqs_ok() -> bool:
    if shutil.which("tmux") is None:
        _log("tmux not found on PATH — skipping")
        return False
    if _backend() is None:
        _log("neither ANTHROPIC_API_KEY nor AI_GATEWAY_API_KEY set — skipping")
        return False
    return True


def _setup_backend(workdir: pathlib.Path, port: int) -> tuple[str, str, str]:
    """Return (upstream_host, model_id, env_prefix) for the backend."""
    if _backend() == "anthropic":
        model = os.environ.get("TAU_SMOKE_MODEL", "anthropic:claude-opus-4.8")
        env = (
            f"TAU_MODEL={shlex.quote(model)} "
            f"ANTHROPIC_BASE_URL=http://127.0.0.1:{port}"
        )
        return "api.anthropic.com", model, env
    model = os.environ.get(
        "TAU_SMOKE_MODEL", "gateway:anthropic/claude-opus-4.8"
    )
    shim_dir = workdir / ".shim"
    shim_dir.mkdir(exist_ok=True)
    (shim_dir / "sitecustomize.py").write_text(_SHIM)
    env = (
        f"TAU_MODEL={shlex.quote(model)} "
        f"PYTHONPATH={shlex.quote(str(shim_dir))} "
        f"TAU_SMOKE_GATEWAY_URL=http://127.0.0.1:{port}/v3/ai"
    )
    return "ai-gateway.vercel.sh", model, env


def _start(workdir: pathlib.Path, env: str) -> None:
    cmd = (
        f"cd {shlex.quote(str(workdir))} && exec env {env} "
        f"uv run --project {shlex.quote(str(base.TAU_DIR))} tau"
    )
    base._tmux(
        "new-session", "-d", "-s", base.SESSION, "-x", "200", "-y", "50", cmd
    )


def _set_fault(port: int, **spec: object) -> None:
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/_fault",
        data=json.dumps(spec or {"mode": None}).encode(),
        method="POST",
    )
    urllib.request.urlopen(req, timeout=10).close()


def _clear_fault(port: int) -> None:
    _set_fault(port, mode=None)


_ERROR_LINE = re.compile(r"^\s*error: ", re.MULTILINE)


def _pane_error_count() -> int:
    """Count tau system-error lines (not exception class names)."""
    return len(_ERROR_LINE.findall(base._capture()))


def _run_turn_expecting_error(prompt: str, errors_before: int) -> bool:
    """Send a prompt; require a new error message and a return to idle.

    Unlike ``base._wait_idle`` this doesn't insist on seeing a busy
    marker first — an injected failure can end the turn faster than
    the pane poll interval.
    """
    base._send_text(prompt)
    deadline = time.time() + ERROR_TIMEOUT
    while time.time() < deadline:
        raw = base._capture()
        pane = raw.lower()
        busy = any(m in pane for m in base.BUSY_MARKERS)
        if (
            not busy
            and base.IDLE_MARKER in pane
            and len(_ERROR_LINE.findall(raw)) > errors_before
        ):
            return True
        time.sleep(base.IDLE_POLL)
    _log("timed out waiting for an error + idle composer")
    _log("final pane:\n" + base._capture())
    return False


def _teardown(workdir: pathlib.Path, proxy: FaultProxy) -> None:
    proxy.stop()
    if os.environ.get("TAU_SMOKE_KEEP") == "1":
        _log(f"keeping tmux socket {base.SOCKET!r} and {workdir}")
        return
    base._tmux("kill-server", check=False)
    shutil.rmtree(workdir, ignore_errors=True)


def main() -> int:
    if not _prereqs_ok():
        return base.SKIP_EXIT

    workdir = pathlib.Path(tempfile.mkdtemp(prefix="tau-smoke-faults-"))
    # Upstream host depends on the backend; bind the proxy after we
    # know it.  Port 0 → OS-assigned, so compute env after start.
    upstream = (
        "api.anthropic.com"
        if _backend() == "anthropic"
        else "ai-gateway.vercel.sh"
    )
    proxy = FaultProxy(upstream)
    proxy.start()
    _, model, env = _setup_backend(workdir, proxy.port)
    _log(f"workdir: {workdir}; proxy on 127.0.0.1:{proxy.port} → {upstream}")

    try:
        _start(workdir, env)
        if not base._wait_ready():
            return 1
        _log(f"tau is ready (model: {model}, backend: {_backend()})")

        # Scenario 1: hard API error for the whole turn.  `times` is
        # generous so any future retry logic still exhausts into a
        # surfaced error rather than silently passing.
        _log("scenario 1: API returns 529 for the whole turn")
        errors, hits = _pane_error_count(), proxy.state.hits
        _set_fault(proxy.port, mode="error", status=529, times=20)
        if not _run_turn_expecting_error(PROMPT_TRIVIAL, errors):
            return 1
        _clear_fault(proxy.port)
        if proxy.state.hits == hits:
            _log("scenario 1: fault was never consumed by a request")
            return 1
        _log("scenario 1: ok — error surfaced, tau returned to idle")

        # Scenario 1b: a single transient 529 — tau should retry
        # silently and complete the turn without surfacing an error.
        _log("scenario 1b: single 529, expect silent retry")
        time.sleep(2.0)  # let the previous turn's bubbles finish painting
        pane = base._capture()
        errors, hits = _pane_error_count(), proxy.state.hits
        retries = pane.count("retrying in")
        _set_fault(proxy.port, mode="error", status=529, times=1)
        base._send_text(PROMPT_TRIVIAL)
        if not base._wait_idle(base.TURN_TIMEOUT):
            _log("scenario 1b: timed out waiting for tau to finish")
            _log("final pane:\n" + base._capture())
            return 1
        time.sleep(2.0)  # settle: bubbles can paint a frame after idle
        pane = base._capture()
        if _pane_error_count() > errors:
            _log("scenario 1b: error surfaced; expected a silent retry")
            _log("final pane:\n" + pane)
            return 1
        if pane.count("retrying in") <= retries:
            _log("scenario 1b: no retry notice in the pane")
            _log("final pane:\n" + pane)
            return 1
        if proxy.state.hits != hits + 1:
            _log("scenario 1b: fault not consumed exactly once")
            _log(f"request log: {proxy.state.log}")
            return 1
        _log("scenario 1b: ok — transient error retried silently")

        # Scenario 2: stream severed mid-response.
        _log("scenario 2: SSE stream cut mid-response")
        errors, hits = _pane_error_count(), proxy.state.hits
        _set_fault(proxy.port, mode="cut", after_bytes=1500, times=20)
        if not _run_turn_expecting_error(PROMPT_TRIVIAL, errors):
            return 1
        _clear_fault(proxy.port)
        if proxy.state.hits == hits:
            _log("scenario 2: fault was never consumed by a request")
            _log(f"request log: {proxy.state.log}")
            _log("final pane:\n" + base._capture())
            return 1
        _log("scenario 2: ok — error surfaced, tau returned to idle")

        # Scenario 3: faults cleared; a real turn must still work,
        # proving the session/history wasn't corrupted by the failures.
        _log("scenario 3: recovery turn (write a file)")
        base._send_text(PROMPT_RECOVERY)
        if not base._wait_idle(base.TURN_TIMEOUT):
            _log("scenario 3: timed out waiting for tau to finish")
            _log("final pane:\n" + base._capture())
            return 1
        target = workdir / TARGET
        if not target.exists() or "OK" not in target.read_text():
            _log("scenario 3: recovery file missing or wrong")
            _log("final pane:\n" + base._capture())
            return 1
        _log("scenario 3: ok — tau recovered and completed a real turn")

        _log(f"proxy injected {proxy.state.hits} faulted responses")
        _log(f"request log: {proxy.state.log}")
        _log("PASS — failures surfaced and tau recovered")
        return 0
    finally:
        _teardown(workdir, proxy)


if __name__ == "__main__":
    raise SystemExit(main())
