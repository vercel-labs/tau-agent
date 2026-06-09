# tau-agent

`tau` is a coding-agent demo built on the
[`ai`](https://github.com/vercel-labs/ai-python) library.
The name is a loving homage to [pi](https://github.com/earendil-works/pi).

Single
process, Textual TUI, streaming replies, pi-style tool surface:

- **`read`** — read files; offset/limit pagination with continuation hints
- **`write`** — create / overwrite a file
- **`edit`** — exact-match str_replace, multiple disjoint edits per call
- **`bash`** — run a shell command in cwd, output truncated to the last 50KB / 2000 lines *(requires approval)*
- **`grep`** — regex search (skips `.git`, `node_modules`, etc.)
- **`find`** — glob match
- **`ls`** — directory listing

Anthropic and OpenAI's built in web search and web fetch tools are
also available when appropriate.

Approval-gated tools fire a `ToolApproval` hook; the composer turns
into a `[y/n]` prompt mid-turn.  Unrelated text typed during a
pending approval falls through to the message queue — the hook stays
pending until you give it a y or n.

## Setup

```bash
uv sync
```

## Running

```bash
uv run tau
```

Type a message, hit enter.  `ctrl+c` to quit. `ctrl+j` for newlines inside a message.
Escape interrupts current task.

## Smoke test

```bash
uv run poe smoke
```

Drives tau end-to-end under tmux: it asks tau to implement fizzbuzz in
a temp dir, then to switch the output to all-caps, validating each step
by running the generated file.  Needs `tmux` and an API key; skips when
they're unavailable.

## Environment

| Variable | Description | Default |
|----------|-------------|---------|
| `AI_GATEWAY_API_KEY` | Vercel AI Gateway API key | — |
| `TAU_MODEL` | Model id passed to `ai.get_model(...)` | `anthropic/claude-opus-4-8` |
