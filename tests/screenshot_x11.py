#!/usr/bin/env python3
"""Pixel-true screenshot of a tmux session in a real terminal.

Renders the given tmux session in an actual terminal emulator running
under a headless X server (Xvfb) and captures the emulator's window
pixels with ImageMagick.  Unlike ``snapshot_svg.py`` (which rebuilds
an image from tmux's cell grid), every pixel here went through the
terminal's real rendering path: real font rasterization, real glyph
layout, real colors.

Usage::

    uv run python tests/screenshot_x11.py SESSION OUT.png
    uv run python tests/screenshot_x11.py -L tau-smoke tau out.png \\
        --terminal xterm --geometry 120x30

The terminal is configurable: ``--terminal`` takes a preset name
(``xterm``, ``urxvt``) or a full command template containing a
``{command}`` placeholder (and optionally ``{cols}``, ``{rows}``,
``{font}``, ``{size}``), e.g.::

    --terminal 'alacritty -e sh -c {command}'

The emulator must export ``$WINDOWID`` to its child (xterm, urxvt,
kitty, and alacritty all do); the child reports it back so the capture
targets exactly that window.  Attaching a client resizes the tmux
session to the client, so ``--geometry`` becomes the pane size.

Exits 77 (skip) when Xvfb, ImageMagick, tmux, or the requested
terminal is unavailable; 1 on failure.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import select
import shlex
import shutil
import subprocess
import sys
import tempfile
import time

SKIP_EXIT = 77

XVFB_SCREEN = "1920x1200x24"
DISPLAY_TIMEOUT = 10.0
WINDOW_TIMEOUT = 15.0
STABLE_TIMEOUT = 15.0
POLL = 0.3

# Preset argv templates.  Placeholders are substituted per token, and
# {command} expands to a shell script the terminal must run.
PRESETS = {
    "xterm": [
        "xterm",
        "-fa",
        "{font}",
        "-fs",
        "{size}",
        "-geometry",
        "{cols}x{rows}",
        "+sb",
        "-e",
        "sh",
        "-c",
        "{command}",
    ],
    "urxvt": [
        "urxvt",
        "-fn",
        "xft:{font}:size={size}",
        "-geometry",
        "{cols}x{rows}",
        "+sb",
        "-e",
        "sh",
        "-c",
        "{command}",
    ],
}


def _fail(msg: str) -> None:
    sys.exit(f"error: {msg}")


def _skip(msg: str) -> None:
    print(f"skip: {msg}", file=sys.stderr)
    sys.exit(SKIP_EXIT)


def _template(args: argparse.Namespace) -> list[str]:
    return PRESETS.get(args.terminal) or shlex.split(args.terminal)


def _build_argv(args: argparse.Namespace, command: str) -> list[str]:
    template = _template(args)
    if not any("{command}" in tok for tok in template):
        _fail("terminal template must contain a {command} placeholder")
    cols, rows = args.geometry.split("x", 1)
    subst = {
        "command": command,
        "cols": cols,
        "rows": rows,
        "font": args.font,
        "size": str(args.font_size),
    }
    try:
        return [tok.format(**subst) for tok in template]
    except (KeyError, ValueError, IndexError) as exc:
        _fail(
            f"bad placeholder in terminal template: {exc!r} "
            "(escape literal braces as {{ and }})"
        )
        raise AssertionError from exc  # unreachable


def _start_xvfb() -> tuple[subprocess.Popen[bytes], str]:
    """Start Xvfb; return the process and its display (e.g. ':42')."""
    rfd, wfd = os.pipe()
    proc = subprocess.Popen(
        ["Xvfb", "-displayfd", str(wfd), "-screen", "0", XVFB_SCREEN],
        pass_fds=(wfd,),
        stderr=subprocess.DEVNULL,
    )
    os.close(wfd)
    buf = b""
    deadline = time.monotonic() + DISPLAY_TIMEOUT
    while time.monotonic() < deadline and b"\n" not in buf:
        ready, _, _ = select.select([rfd], [], [], 0.5)
        if ready:
            chunk = os.read(rfd, 64)
            if not chunk:
                break
            buf += chunk
    os.close(rfd)
    if b"\n" not in buf:
        proc.kill()
        proc.wait()
        _fail("Xvfb did not report a display number")
    return proc, f":{int(buf.strip())}"


def _wait_window_id(
    wid_file: pathlib.Path, term: subprocess.Popen[bytes]
) -> str:
    deadline = time.monotonic() + WINDOW_TIMEOUT
    while time.monotonic() < deadline:
        if term.poll() is not None:
            _fail(
                f"terminal exited (status {term.returncode}) before "
                "reporting a window id"
            )
        try:
            wid = wid_file.read_text().strip()
        except OSError:
            wid = ""
        if wid:
            return wid
        time.sleep(POLL)
    return ""


def _grab(display: str, window: str) -> bytes:
    """Capture *window* as PPM bytes (PPM carries no timestamps)."""
    for attempt in range(3):
        proc = subprocess.run(
            ["import", "-display", display, "-window", window, "ppm:-"],
            capture_output=True,
        )
        if proc.returncode == 0:
            return proc.stdout
        # A just-mapped window can fail XGetImage transiently.
        if attempt < 2:
            time.sleep(POLL)
    _fail(f"import: {proc.stderr.decode(errors='replace').strip()}")
    raise AssertionError  # unreachable


def _is_blank(ppm: bytes) -> bool:
    """True if every pixel in the PPM is the same color."""
    # P6 header: magic, dimensions, maxval — then raw RGB triples.
    parts = ppm.split(b"\n", 3)
    if len(parts) < 4:
        return True
    body = parts[3]
    return len(body) < 3 or body.count(body[:3]) * 3 >= len(body) - 2


def _stable_grab(display: str, window: str) -> bytes:
    """Capture until two consecutive non-blank grabs are identical.

    A freshly mapped terminal is solid background for longer than one
    poll interval, so identical-but-blank frames must not count as
    stable or we would race the first draw.
    """
    last = _grab(display, window)
    deadline = time.monotonic() + STABLE_TIMEOUT
    while time.monotonic() < deadline:
        time.sleep(POLL)
        cur = _grab(display, window)
        if cur == last and not _is_blank(cur):
            return cur
        last = cur
    print(
        "warning: screen never stabilized; using last capture", file=sys.stderr
    )
    return last


def screenshot(args: argparse.Namespace) -> None:
    term_bin = _template(args)[0]
    for binary, why in (
        ("Xvfb", "headless X server"),
        ("import", "ImageMagick capture"),
        ("convert", "ImageMagick convert"),
        ("tmux", "session control"),
    ):
        if shutil.which(binary) is None:
            _skip(f"{binary} not found ({why})")
    if shutil.which(term_bin) is None:
        # Missing preset terminal is an environment limitation (skip);
        # an explicitly requested one is a user error (fail).
        if args.terminal in PRESETS:
            _skip(f"{term_bin} not found (terminal emulator)")
        _fail(f"terminal {term_bin!r} not found")

    tmux = ["tmux"] + (["-L", args.socket] if args.socket else [])
    if (
        subprocess.run(
            [*tmux, "has-session", "-t", args.session], capture_output=True
        ).returncode
        != 0
    ):
        _fail(f"no tmux session {args.session!r}")

    with tempfile.TemporaryDirectory(prefix="tau-x11-") as tmp:
        wid_file = pathlib.Path(tmp) / "windowid"
        attach = " ".join(
            shlex.quote(t) for t in [*tmux, "attach", "-t", args.session]
        )
        command = (
            f'echo "$WINDOWID" > {shlex.quote(str(wid_file))}; exec {attach}'
        )
        xvfb, display = _start_xvfb()
        term: subprocess.Popen[bytes] | None = None
        try:
            term = subprocess.Popen(
                _build_argv(args, command),
                env={**os.environ, "DISPLAY": display},
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            window = _wait_window_id(wid_file, term)
            if not window:
                print(
                    "warning: no $WINDOWID reported; capturing whole screen",
                    file=sys.stderr,
                )
                window = "root"
            ppm = _stable_grab(display, window)
            proc = subprocess.run(
                ["convert", "ppm:-", str(args.outfile)],
                input=ppm,
                capture_output=True,
            )
            if proc.returncode != 0:
                _fail(
                    "convert: " + proc.stderr.decode(errors="replace").strip()
                )
        finally:
            if term is not None:
                term.terminate()
                try:
                    term.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    term.kill()
                    term.wait()
            xvfb.terminate()
            try:
                xvfb.wait(timeout=5)
            except subprocess.TimeoutExpired:
                xvfb.kill()
                xvfb.wait()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Capture a tmux session rendered by a real terminal "
            "emulator under Xvfb."
        )
    )
    parser.add_argument("session", help="tmux session name to attach")
    parser.add_argument(
        "outfile", type=pathlib.Path, help="output image path (.png)"
    )
    parser.add_argument(
        "-L",
        "--socket",
        default=None,
        help="tmux socket name (tmux -L), e.g. tau-smoke",
    )
    parser.add_argument(
        "--terminal",
        default="xterm",
        help=(
            "terminal preset (xterm, urxvt) or command template with "
            "a {command} placeholder"
        ),
    )
    parser.add_argument(
        "--geometry",
        default="120x30",
        help="terminal size in COLSxROWS (default 120x30)",
    )
    parser.add_argument(
        "--font",
        default="DejaVu Sans Mono",
        help="font family for terminal presets",
    )
    parser.add_argument(
        "--font-size", type=int, default=12, help="font size for presets"
    )
    args = parser.parse_args()
    if not re.fullmatch(r"\d+x\d+", args.geometry):
        _fail("geometry must be COLSxROWS, e.g. 120x30")

    screenshot(args)
    print(f"wrote {args.outfile}")


if __name__ == "__main__":
    main()
