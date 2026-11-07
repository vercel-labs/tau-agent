"""Run tau's tools in a remote Vercel sandbox.

The repo is copied into the sandbox and a ``run_sandbox`` worker is
started there; ``SandboxAgent`` forwards every tool call to it, so the
tools see the sandbox's filesystem and run its shell commands.
"""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import importlib
import pathlib
from collections.abc import AsyncIterator, Sequence
from typing import Any

import ai
import ai.agents.agent
from vercel import sandbox
from vercel.api import session

from tau import run_sandbox

_REPO_ROOT = pathlib.Path(run_sandbox.__file__).parent.parent


def _call_tool(module: str, qualname: str, /, **kwargs: Any) -> Any:
    """Runs in the sandbox worker."""
    obj: Any = importlib.import_module(module)
    for attr in qualname.split("."):
        obj = getattr(obj, attr)
    if isinstance(obj, ai.agents.agent.AgentTool):
        obj = obj.fn
    return obj(**kwargs)


class SandboxAgent(ai.Agent):
    """An agent that runs ``sandbox_tools``, unapproved, in a sandbox worker."""

    def __init__(
        self,
        worker: run_sandbox.BaseWorker,
        *,
        sandbox_tools: Sequence[ai.agents.agent.AgentTool],
        extra_tools: Sequence[ai.agents.agent.AgentTool | ai.Tool] = (),
    ) -> None:
        remote = []
        for t in sandbox_tools:
            call = worker.run if t.aggregator is None else worker.iterate
            remote.append(
                dataclasses.replace(
                    t,
                    tool=t.tool.model_copy(update={"require_approval": False}),
                    fn=functools.partial(
                        call, _call_tool, t.fn.__module__, t.fn.__qualname__
                    ),
                )
            )
        super().__init__(tools=[*remote, *extra_tools])


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
