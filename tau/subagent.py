"""Subagent tool — delegate a read-only investigation to a child agent.

The child agent gets the read-only subset of tau's tools (read, grep,
find, ls) and no subagent tool of its own, so it can explore but not
mutate and not recurse.  Because the child is read-only, spawning
needs no operator approval, and the child's tool calls are resolved
here rather than prompting: approved within the working directory,
denied outside it.  (The parent's own reads outside cwd prompt the
operator; a subagent must not be a way around that gate.)

Built on the ``ai`` library's subagent pattern: the tool is an async
generator that yields the child's events upward, a ``MessageAggregator``
collects them, and the child's final assistant text becomes the tool
result the parent model sees.
"""

from __future__ import annotations

import pathlib

import ai
import ai.agents

from tau import tools

# Tools the child may use.  Hook approvals are granted only for these.
_CHILD_TOOLS = [tools.read, tools.grep, tools.find, tools.ls]
_CHILD_TOOL_NAMES = frozenset(t.name for t in _CHILD_TOOLS)

_SYSTEM_PROMPT = """\
You are a read-only investigator subagent.  You have the read, grep,
find, and ls tools; you cannot write, edit, or run commands, and you
can only access paths inside the working directory.

Carry out the task you are given by exploring the filesystem, then
report your findings.  Your final message is returned verbatim to the
agent that spawned you, so make it self-contained: include relevant
paths, line numbers, and short excerpts.  Be concise — findings, not
narration.
"""


def make_subagent_tool(
    model: ai.Model, params: ai.InferenceRequestParams
) -> ai.agents.AgentTool:
    """Build the subagent tool bound to *model* and *params*."""

    @ai.tool
    async def subagent(task: str) -> ai.agents.SubAgentTool:
        """Delegate a read-only investigation to a subagent.

        The subagent can read, grep, find, and ls within the working
        directory, but cannot write, edit, run commands, or access
        paths outside it.  It works through the task and returns a
        report.  Use it for explorations that would otherwise fill
        your context with intermediate results; give it a focused,
        self-contained task description and tell it what you want
        back.
        """
        child = ai.agents.agent(tools=_CHILD_TOOLS)
        messages = [
            ai.system_message(_SYSTEM_PROMPT),
            ai.user_message(task),
        ]
        async with child.run(model, messages, params=params) as stream:
            async for event in stream:
                if isinstance(event, ai.events.HookEvent):
                    _resolve_child_hook(event.hook)
                    continue
                yield event

    return subagent


def _resolve_child_hook(part: ai.messages.HookPart) -> None:
    """Resolve a child tool call: read-only tools, cwd-contained paths."""
    granted, reason = _check_child_call(
        part.metadata.get("tool", "?"),
        part.metadata.get("kwargs", {}) or {},
    )
    ai.resolve_hook(
        part.hook_id,
        ai.tools.ToolApproval(granted=granted, reason=reason),
    )


def _check_child_call(tool_name: str, kwargs: dict) -> tuple[bool, str]:
    if tool_name not in _CHILD_TOOL_NAMES:
        return False, f"tool {tool_name!r} not allowed in subagent"
    raw = kwargs.get("path")
    if raw is not None:
        path = pathlib.Path(raw).expanduser().resolve()
        if not path.is_relative_to(pathlib.Path.cwd().resolve()):
            return False, (
                f"path {str(raw)!r} is outside the working directory; "
                "subagents may only access paths within it"
            )
    return True, "read-only subagent tool within the working directory"
