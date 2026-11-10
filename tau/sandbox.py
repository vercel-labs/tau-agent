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
import copyreg
import dataclasses
import functools
import importlib
import pathlib
import pickle
import types
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from typing import Any

import ai
import ai.agents.agent
import ai.agents.runtime
from vercel import sandbox
from vercel.api import session

from tau import run_sandbox

_REPO_ROOT = pathlib.Path(run_sandbox.__file__).parent.parent

_worker: contextvars.ContextVar[run_sandbox.BaseWorker | None] = (
    contextvars.ContextVar("tau_sandbox_worker", default=None)
)


def _lookup(module: str, qualname: str) -> Any:
    obj: Any = importlib.import_module(module)
    for attr in qualname.split("."):
        obj = getattr(obj, attr)
    return obj


def _call_tool(module: str, qualname: str, /, **kwargs: Any) -> Any:
    obj = _lookup(module, qualname)
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


def _wrap_tool(
    t: ai.agents.agent.AgentTool, require_approval: bool | None
) -> ai.agents.agent.AgentTool:
    tool = t.tool
    if require_approval is not None:
        tool = tool.model_copy(update={"require_approval": require_approval})
    return dataclasses.replace(
        t,
        tool=tool,
        fn=functools.partial(
            _dispatch,
            t.fn.__module__,
            t.fn.__qualname__,
            t.aggregator is not None,
        ),
    )


def _unpickle_wrapped_tool(
    module: str, qualname: str, require_approval: bool
) -> ai.agents.agent.AgentTool:
    return _wrap_tool(_lookup(module, qualname), require_approval)


def _reduce_agent_tool(t: ai.agents.agent.AgentTool) -> Any:
    """Pickle tools by reference, since their validators are dynamic classes."""
    fn = t.fn
    if isinstance(fn, functools.partial) and fn.func is _dispatch:
        module, qualname, _ = fn.args
        args = (module, qualname, t.tool.require_approval)
        return _unpickle_wrapped_tool, args
    module, qualname = fn.__module__, fn.__qualname__
    try:
        found = _lookup(module, qualname)
    except (ImportError, AttributeError):
        found = None
    if found is not t:
        raise pickle.PicklingError(
            f"can't pickle tool {t.name!r}: not found as {module}.{qualname}"
        )
    return _lookup, (module, qualname)


def _mappingproxy(d: dict[Any, Any]) -> types.MappingProxyType[Any, Any]:
    return types.MappingProxyType(d)


copyreg.pickle(ai.agents.agent.AgentTool, _reduce_agent_tool)
copyreg.pickle(types.MappingProxyType, lambda m: (_mappingproxy, (dict(m),)))


def _remote_loop(
    agent: SandboxAgent, context: ai.Context
) -> AsyncGenerator[ai.events.AgentEvent]:
    """Runs in the worker, with the runtime that ``Agent.run`` sets up locally.

    Tools put partial results onto the runtime, so they have to be merged
    in here to make it back to the caller.
    """
    return ai.agents.runtime.run(ai.Agent.loop(agent, context))


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
        wrapped = [_wrap_tool(t, require_approval) for t in sandbox_tools]
        super().__init__(tools=[*wrapped, *extra_tools])

    async def loop(
        self, context: ai.Context
    ) -> AsyncGenerator[ai.events.AgentEvent]:
        worker = _worker.get()
        if worker is None:
            async for x in super().loop(context):
                yield x
        else:
            async with worker.iterate(_remote_loop, self, context) as s:
                async for x in s:
                    yield x


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
