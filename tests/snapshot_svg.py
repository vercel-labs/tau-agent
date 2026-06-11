#!/usr/bin/env python3
"""Save an SVG or PNG snapshot of a tmux pane.

Captures the current contents of a tmux session's active pane (with
colors/attributes) and renders it to an SVG via Rich.  When the output
filename ends in ``.png``, the SVG is rasterized with ``resvg``.  No
graphical terminal is involved — this reads tmux's cell grid directly.

Usage::

    uv run python tests/snapshot_svg.py SESSION OUTFILE.svg
    uv run python tests/snapshot_svg.py -L tau-smoke tau shot.png

Exits non-zero if the session does not exist or capture fails.

The SVG is post-processed for resvg compatibility: Rich's output
references the Fira Code webfont, which non-browser renderers cannot
fetch, and resvg drops text whose font family is unresolvable rather
than falling back.  The font stack is rewritten to an installed
monospace family resolved via fontconfig.  Target resvg (or a
browser): librsvg has known text-layout defects with Rich SVGs (it
strips leading no-break spaces and ignores ``textLength``).
"""

from __future__ import annotations

import argparse
import io
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

from rich.console import Console
from rich.text import Text


def _tmux(socket: str | None, *args: str) -> str:
    cmd = ["tmux"]
    if socket:
        cmd += ["-L", socket]
    cmd += list(args)
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"error: {' '.join(cmd)}: {proc.stderr.strip()}")
    return proc.stdout


def _local_monospace() -> str:
    """Resolve an installed monospace family via fontconfig."""
    try:
        proc = subprocess.run(
            ["fc-match", "-f", "%{family}", "monospace"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        family = proc.stdout.strip()
        if proc.returncode == 0 and family:
            return family.split(",")[0]
    except OSError:
        pass
    return "monospace"


def _resvg_compat(svg: str) -> str:
    """Swap Rich's unfetchable webfont for an installed family.

    resvg cannot fetch the Fira Code webfont and drops text whose
    font family is unresolvable, so strip the ``@font-face`` rules
    and rewrite the font stacks to a locally installed monospace.
    """
    svg = re.sub(r"@font-face\s*\{[^}]*\}", "", svg)
    family = _local_monospace()
    svg = svg.replace("Fira Code, monospace", f'"{family}", monospace')
    # Defensive: quoted form only occurs in the (already stripped)
    # @font-face rules as of Rich 15, but cheap to keep correct.
    svg = svg.replace('"Fira Code"', f'"{family}"')
    return svg.replace("font-family: arial", f'font-family: "{family}"')


def _rasterize(svg: str, out: pathlib.Path) -> None:
    """Render *svg* to PNG at *out* using resvg."""
    if shutil.which("resvg") is None:
        sys.exit("error: resvg not found on PATH (needed for .png output)")
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".svg",
        delete_on_close=False,
        encoding="utf-8",
    ) as tmp:
        tmp.write(svg)
        tmp.close()
        proc = subprocess.run(
            ["resvg", tmp.name, str(out)], capture_output=True, text=True
        )
    if proc.returncode != 0:
        sys.exit(f"error: resvg: {proc.stderr.strip()}")


def snapshot(
    session: str, out: pathlib.Path, socket: str | None, title: str
) -> None:
    width = int(
        _tmux(
            socket, "display-message", "-p", "-t", session, "#{pane_width}"
        ).strip()
    )
    ansi = _tmux(socket, "capture-pane", "-t", session, "-p", "-e", "-J")
    # Drop the trailing newline capture-pane appends so the SVG does not
    # gain an extra blank row.
    ansi = ansi.removesuffix("\n")

    console = Console(record=True, width=width, file=io.StringIO())
    console.print(Text.from_ansi(ansi))
    svg = _resvg_compat(console.export_svg(title=title))

    if out.suffix.lower() == ".png":
        _rasterize(svg, out)
    else:
        out.write_text(svg, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Save an SVG or PNG snapshot of a tmux pane."
    )
    parser.add_argument("session", help="tmux session name (target pane)")
    parser.add_argument(
        "outfile",
        type=pathlib.Path,
        help="path to write to (.svg, or .png to rasterize via resvg)",
    )
    parser.add_argument(
        "-L",
        "--socket",
        default=None,
        help="tmux socket name (tmux -L), e.g. tau-smoke",
    )
    parser.add_argument(
        "--title",
        default=None,
        help="window title in the SVG chrome (default: session name)",
    )
    args = parser.parse_args()

    snapshot(
        args.session,
        args.outfile,
        args.socket,
        args.title or args.session,
    )
    print(f"wrote {args.outfile}")


if __name__ == "__main__":
    main()
