"""Fault-injecting HTTP forward proxy for smoke tests.

Relays requests to an upstream HTTPS API and can inject failures on
demand.  Faults are programmed at runtime via ``POST /_fault`` with a
JSON body:

    {"mode": "error", "status": 529, "times": 3}
    {"mode": "cut", "after_bytes": 1500, "times": 1}
    {"mode": null}

- ``error``: respond immediately with the given status and an
  anthropic-style error JSON body, without contacting upstream.
- ``cut``: forward the upstream response but abruptly close the
  connection after roughly ``after_bytes`` of body, simulating a
  mid-stream network failure.
- ``times`` limits how many subsequent API requests the fault applies
  to (default 1).  ``{"mode": null}`` clears any remaining fault.

Usage::

    proxy = FaultProxy("api.anthropic.com")
    proxy.start()
    ...  # point the client at http://127.0.0.1:{proxy.port}
    proxy.stop()
"""

from __future__ import annotations

import http.client
import http.server
import json
import threading
from typing import Any

_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


class _FaultState:
    """Mutable fault configuration shared across handler threads."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.mode: str | None = None
        self.status = 529
        self.after_bytes = 1500
        self.times = 0
        self.hits = 0
        self.log: list[tuple[str, str | None]] = []

    def configure(self, spec: dict[str, Any]) -> None:
        with self.lock:
            self.mode = spec.get("mode")
            self.status = int(spec.get("status", 529))
            self.after_bytes = int(spec.get("after_bytes", 1500))
            self.times = int(spec.get("times", 1))

    def take(self, path: str) -> str | None:
        """Consume one application of the current fault, if armed."""
        with self.lock:
            if self.mode is None or self.times <= 0:
                self.log.append((path, None))
                return None
            self.times -= 1
            self.hits += 1
            self.log.append((path, self.mode))
            return self.mode


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: FaultProxy  # type: ignore[assignment]

    # Quiet the default stderr access log; the test prints its own.
    def log_message(self, fmt: str, *args: Any) -> None:
        pass

    def setup(self) -> None:
        super().setup()
        self.server.state.log.append(("<connect>", None))

    def finish(self) -> None:
        self.server.state.log.append(("<disconnect>", None))
        super().finish()

    def do_GET(self) -> None:
        self._dispatch()

    def do_POST(self) -> None:
        self._dispatch()

    def _dispatch(self) -> None:
        if self.path == "/_fault":
            self._handle_control()
            return
        state = self.server.state
        mode = state.take(self.path)
        if mode == "error":
            self._inject_error(state.status)
            return
        self._forward(cut_after=state.after_bytes if mode == "cut" else None)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _handle_control(self) -> None:
        spec = json.loads(self._read_body() or b"{}")
        self.server.state.configure(spec)
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()

    def _inject_error(self, status: int) -> None:
        body = json.dumps(
            {
                "type": "error",
                "error": {
                    "type": "api_error",
                    "message": f"injected fault (status {status})",
                },
            }
        ).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        # No keep-alive: a fresh connection per request keeps this toy
        # proxy's lifecycle trivial and avoids stale pooled connections
        # on the client side after an injected failure.
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        self.wfile.write(body)

    def _forward(self, *, cut_after: int | None) -> None:
        upstream = self.server.upstream
        conn = http.client.HTTPSConnection(upstream, timeout=300)
        headers = {
            k: v
            for k, v in self.headers.items()
            if k.lower() not in _HOP_BY_HOP and k.lower() != "host"
        }
        headers["Host"] = upstream
        headers["Connection"] = "close"
        try:
            conn.request(self.command, self.path, self._read_body(), headers)
            resp = conn.getresponse()
            self.send_response(resp.status)
            for k, v in resp.getheaders():
                if k.lower() in _HOP_BY_HOP or k.lower() == "content-length":
                    continue
                self.send_header(k, v)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Connection", "close")
            self.close_connection = True
            self.end_headers()
            sent = 0
            while True:
                chunk = resp.read(1024)
                if not chunk:
                    break
                self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
                self.wfile.flush()
                sent += len(chunk)
                if cut_after is not None and sent >= cut_after:
                    # Abrupt close without the terminating 0-chunk: the
                    # client sees an incomplete body, like a dropped
                    # network connection.
                    self.connection.close()
                    return
            self.wfile.write(b"0\r\n\r\n")
        finally:
            conn.close()


class FaultProxy:
    """Threaded fault-injecting forward proxy bound to 127.0.0.1."""

    def __init__(self, upstream: str) -> None:
        self.upstream = upstream
        self.state = _FaultState()
        self._httpd = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), _Handler
        )
        # Expose proxy attributes to handlers via the server object.
        self._httpd.upstream = upstream  # type: ignore[attr-defined]
        self._httpd.state = self.state  # type: ignore[attr-defined]
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, daemon=True
        )

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
