"""Run tau's tools in a remote Vercel sandbox.

``sandbox_worker()`` copies the repo into a sandbox, starts a
``run_sandbox`` worker there, and makes it the ambient worker.
``SandboxAgent``'s tools run in the ambient worker when there is one,
so they see the sandbox's filesystem and run its shell commands, and
run locally otherwise.
"""

from __future__ import annotations

import contextlib
import contextvars
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

_worker: contextvars.ContextVar[run_sandbox.BaseWorker | None] = (
    contextvars.ContextVar("tau_sandbox_worker", default=None)
)


def _call_tool(module: str, qualname: str, /, **kwargs: Any) -> Any:
    obj: Any = importlib.import_module(module)
    for attr in qualname.split("."):
        obj = getattr(obj, attr)
    if isinstance(obj, ai.agents.agent.AgentTool):
        obj = obj.fn
    return obj(**kwargs)


def _dispatch(
    module: str, qualname: str, streaming: bool, /, **kwargs: Any
) -> Any:
    worker = _worker.get()
    if worker is None:
        return _call_tool(module, qualname, **kwargs)
    call = worker.iterate if streaming else worker.run
    return call(_call_tool, module, qualname, **kwargs)


class SandboxAgent(ai.Agent):
    """An agent that runs ``sandbox_tools`` in the ambient sandbox worker.

    ``require_approval``, if not None, overrides the tools' own setting.
    """

    def __init__(
        self,
        *,
        sandbox_tools: Sequence[ai.agents.agent.AgentTool],
        extra_tools: Sequence[ai.agents.agent.AgentTool | ai.Tool] = (),
        require_approval: bool | None = None,
    ) -> None:
        wrapped = []
        for t in sandbox_tools:
            tool = t.tool
            if require_approval is not None:
                tool = tool.model_copy(
                    update={"require_approval": require_approval}
                )
            wrapped.append(
                dataclasses.replace(
                    t,
                    tool=tool,
                    fn=functools.partial(
                        _dispatch,
                        t.fn.__module__,
                        t.fn.__qualname__,
                        t.aggregator is not None,
                    ),
                )
            )
        super().__init__(tools=[*wrapped, *extra_tools])


@contextlib.asynccontextmanager
async def sandbox_worker() -> AsyncIterator[run_sandbox.SandboxWorker]:
    """Create a sandbox running a worker on a copy of the repo.

    The worker is the ambient sandbox worker until exit.
    """
    async with (
        session(),
        sandbox.create_sandbox() as box,
        run_sandbox.SandboxWorker(
            box, run_sandbox.__file__, src_dir=str(_REPO_ROOT)
        ) as worker,
    ):
        token = _worker.set(worker)
        try:
            yield worker
        finally:
            _worker.reset(token)
