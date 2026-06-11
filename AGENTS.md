# AGENTS.md — tau-agent

## Overview

`tau` is a single-process coding-agent TUI built on the `ai` library and
Textual.  It gives the model seven filesystem/shell tools (read, write,
edit, bash, grep, find, ls) with an approval gate for mutating
operations.

## Project layout

```
tau/
  app.py        — Textual app, chat loop, approval flow
  tools.py      — tool definitions (mirrors pi's seven built-ins)
  session.py    — JSONL session persistence and resume
pyproject.toml  — project metadata, dependencies, ruff/mypy config
```

## TODO list

There may be a task list in `.tau/TODO` — check it for current
priorities and open items.

## Running

```bash
uv sync          # install deps
uv run tau       # launch the TUI
```

## Linting & type-checking

```bash
uv run poe check    # format check + lint + type-check (matches CI)
```

Individual tasks are also available: `uv run poe format-check`,
`uv run poe lint`, `uv run poe type` (defined in `[tool.poe.tasks]`).

## Smoke test

```bash
uv run poe smoke    # drive tau under tmux to build & edit fizzbuzz
```

End-to-end check in `tests/smoke_fizzbuzz.py`: launches tau in a tmux
pane in a temp dir, asks it to implement and then uppercase fizzbuzz,
and validates by executing the generated file.  Requires `tmux` and an
API key; skips (exit 77) otherwise.  It is intentionally not part of
`poe check`/CI since it needs network and costs tokens.

## Seeing tau (screenshots for agents)

If you are an agent working on tau, you can run it for real and look
at it.  Launch it in a tmux pane on a private socket, drive it with
`send-keys`, snapshot the pane to a PNG, and read the PNG with your
image-capable `read` tool:

```bash
tmux -L tau-dev new-session -d -s tau -x 200 -y 50 'uv run tau'
# wait for boot, then type into the composer:
tmux -L tau-dev send-keys -t tau -l 'hello there'
sleep 0.5
tmux -L tau-dev send-keys -t tau Enter
# snapshot (→ .png rasterizes via resvg; → .svg for browsers):
uv run python tests/snapshot_svg.py -L tau-dev tau /tmp/shot.png
tmux -L tau-dev kill-server
```

Gotchas, learned the hard way:

- Every tmux command must repeat `-L tau-dev` — that's how the client
  finds the private server.  Use a fixed pane size (`-x`/`-y`) for
  reproducible captures.
- `send-keys -l` makes **all** arguments literal text — `Enter` would
  be typed as five letters.  Send literal text and key names in
  separate calls, with a short sleep before `Enter` so Textual's
  paste heuristics don't swallow it as a newline.
- For cheap programmatic assertions, skip images entirely:
  `tmux -L tau-dev capture-pane -t tau -p` gives the pane as plain
  text (this is what the smoke test greps).
- `tests/snapshot_svg.py` captures the pane with colors
  (`capture-pane -e`), renders SVG via Rich, rewrites the webfont to
  an installed monospace (resolved with `fc-match`), and rasterizes
  `.png` output with `resvg`.  Needs `tmux`; `.png` additionally
  needs `resvg` and fontconfig with any monospace font.
- Rasterize SVGs with `resvg` (or view in a browser).  Do not use
  librsvg/`rsvg-convert`: it strips leading no-break spaces and
  ignores `textLength`, so columns come out subtly misaligned.
- Wait for tau to be idle before snapshotting, or you'll capture a
  mid-turn spinner.  `tests/smoke_fizzbuzz.py` has the marker
  strings for ready/busy/idle/approval-prompt states.
- Verify claims about rendered output by measurement (e.g. pixel
  edge checks), not by eyeballing the screenshot — small alignment
  defects are easy to miss, especially in your own work.

## Conventions

- Python ≥ 3.12.
- Line length: 80 (`ruff` and project style).
- Lint rule set: E, F, I, UP, B, SIM (see `pyproject.toml`).
- No workspace jail — the approval gate is the safety mechanism.
- Approval-gated tools (`write`, `edit`, `bash`) require operator
  confirmation; reads are auto-approved.
- Sessions persist as JSONL under `.tau/sessions/`.
