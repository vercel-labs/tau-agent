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

## Conventions

- Python ≥ 3.12.
- Line length: 80 (`ruff` and project style).
- Lint rule set: E, F, I, UP, B, SIM (see `pyproject.toml`).
- No workspace jail — the approval gate is the safety mechanism.
- Approval-gated tools (`write`, `edit`, `bash`) require operator
  confirmation; reads are auto-approved.
- Sessions persist as JSONL under `.tau/sessions/`.
