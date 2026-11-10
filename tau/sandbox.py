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
import pathlib
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Sequence,
)
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


def _dispatch(fn: Callable[..., Any], streaming: bool, /, **kwargs: Any) -> Any:
    worker = _worker.get()
    if worker is None:
        return fn(**kwargs)
    call = worker.iterate if streaming else worker.run
    return call(fn, **kwargs)


def _wrap_tool(
    t: ai.agents.agent.AgentTool, require_approval: bool | None
) -> ai.agents.agent.AgentTool:
    tool = t.tool
    if require_approval is not None:
        tool = tool.model_copy(update={"require_approval": require_approval})
    fn = functools.partial(_dispatch, t.fn, t.aggregator is not None)
    return dataclasses.replace(t, tool=tool, fn=fn)


def with_resolved_key(model: ai.Model) -> ai.Model:
    """Copy ``model`` with its API key read now, so it pickles along.

    Otherwise the provider reads its key env var at call time, which
    a sandbox worker doesn't have.
    """
    key = model.provider.api_key
    if key is None:
        return model
    provider = model.provider.model_copy(update={"api_key_value": key})
    return model.model_copy(update={"provider": provider})


@dataclasses.dataclass
class _Added:
    """Messages the remote loop appended to its copy of the context."""

    messages: list[ai.messages.Message]


class _SyncingContext(ai.Context):
    """The worker's copy of the context, reporting each ``add`` at once.

    The ``_Added`` goes on the runtime queue, which is merged into the
    event stream ahead of the loop's own events, so it reaches the
    caller while the loop is still waiting on the next model response.
    """

    def add(
        self,
        message: ai.messages.Message | Sequence[ai.messages.Message] | None,
    ) -> None:
        before = len(self.messages)
        super().add(message)
        if len(self.messages) > before:
            queue = ai.agents.runtime.get_runtime()._event_queue
            queue.put_nowait(_Added(self.messages[before:]))  # type: ignore[arg-type]


def _remote_loop(
    agent: SandboxAgent, context: ai.Context
) -> AsyncGenerator[ai.events.AgentEvent | _Added]:
    """Runs in the worker, with the runtime that ``Agent.run`` sets up locally.

    Tools put partial results onto the runtime, so they have to be merged
    in here to make it back to the caller.
    """
    context.__class__ = _SyncingContext
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
                    if isinstance(x, _Added):
                        context.messages.extend(x.messages)
                    else:
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
