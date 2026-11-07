"""Run tau's tools in a remote Vercel sandbox.

The repo is copied into the sandbox and a ``run_sandbox`` worker is
started there; ``SandboxAgent`` forwards every tool call to it, so the
tools see the sandbox's filesystem and run its shell commands.
"""

from __future__ import annotations

import contextlib
import dataclasses
import pathlib
from collections.abc import AsyncIterator, Sequence
from typing import Any, overload

import ai
import ai.agents.agent
import ai.types.messages
from vercel import sandbox
from vercel.api import session

from tau import run_sandbox, tools

_TOOLS_BY_NAME = {t.name: t for t in tools.TOOLS}

_REPO_ROOT = pathlib.Path(run_sandbox.__file__).parent.parent


def _call_tool(name: str, kwargs: dict[str, Any]) -> Any:
    """Runs in the sandbox worker."""
    return _TOOLS_BY_NAME[name].fn(**kwargs)


class SandboxAgent(ai.Agent):
    """An agent whose tools all run in a sandbox worker, unapproved."""

    def __init__(
        self,
        worker: run_sandbox.BaseWorker,
        *,
        extra_tools: Sequence[ai.agents.agent.AgentTool | ai.Tool] = (),
    ) -> None:
        self._worker = worker
        remote = [
            dataclasses.replace(t, fn=self._remote_fn(t)) for t in tools.TOOLS
        ]
        super().__init__(tools=[*remote, *extra_tools])

    def _remote_fn(self, tool: ai.agents.agent.AgentTool) -> Any:
        name = tool.name
        if tool.aggregator is not None:

            def stream(**kwargs: Any) -> Any:
                return self._worker.iterate(_call_tool, name, kwargs)

            return stream

        def call(**kwargs: Any) -> Any:
            return self._worker.run(_call_tool, name, kwargs)

        return call

    @overload
    def resolve(
        self, tool_part: ai.types.messages.ToolCallPart
    ) -> ai.agents.agent.ToolCall: ...
    @overload
    def resolve(
        self, tool_part: Sequence[ai.types.messages.ToolCallPart]
    ) -> list[ai.agents.agent.ToolCall]: ...

    def resolve(
        self,
        tool_part: ai.types.messages.ToolCallPart
        | Sequence[ai.types.messages.ToolCallPart],
    ) -> ai.agents.agent.ToolCall | list[ai.agents.agent.ToolCall]:
        # Skips the approval gate that ``ToolResolver.resolve`` adds.
        if not isinstance(tool_part, ai.types.messages.ToolCallPart):
            return [self.resolve(tp) for tp in tool_part]
        tool = self._tool_resolver.get(tool_part.tool_name)
        if tool is None:
            raise KeyError(
                f"No agent executor registered for tool {tool_part.tool_name!r}"
            )
        return ai.agents.agent.BoundToolCall(part=tool_part, tool=tool)


@contextlib.asynccontextmanager
async def sandbox_worker() -> AsyncIterator[run_sandbox.SandboxWorker]:
    """Create a sandbox with a copy of the repo and a worker running in it."""
    async with (
        session(),
        sandbox.create_sandbox() as box,
        run_sandbox.SandboxWorker(
            box, run_sandbox.__file__, src_dir=str(_REPO_ROOT)
        ) as worker,
    ):
        yield worker
