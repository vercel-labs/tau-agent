"""Run tau's tools in a remote Vercel sandbox.

``SandboxAgent``'s tools run in the ambient worker, any ``Worker`` made
ambient with ``use_worker()``, when there is one,
so they see the sandbox's filesystem and run its shell commands, and
run locally otherwise.  In ``"loop"`` mode the whole agent loop runs
in the worker, tools included; in ``"tools"`` mode only the tools do.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import functools
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Sequence,
)
from typing import Any, Literal, Protocol, overload

import ai
import ai.agents.agent
import ai.agents.hooks
import ai.agents.runtime

type Mode = Literal["tools", "loop"]


class RemoteIterator[T](Protocol):
    """An async iterator over a remote iterable; ``async with`` closes it."""

    def __aiter__(self) -> AsyncIterator[T]: ...

    def __anext__(self) -> Awaitable[T]: ...

    async def __aenter__(self) -> AsyncIterator[T]: ...

    async def __aexit__(self, *exc_info: object) -> None: ...


class Worker(Protocol):
    """Runs functions somewhere else, such as in a sandbox.

    Arguments, results and the functions themselves are pickled.  A
    sync function's context variable changes are seen by later calls.
    """

    @overload
    async def run[**P, R](
        self,
        func: Callable[P, Awaitable[R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...
    @overload
    async def run[**P, R](
        self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> R: ...

    def iterate[**P, T](
        self,
        func: Callable[P, Iterable[T] | AsyncIterable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RemoteIterator[T]: ...


_worker: contextvars.ContextVar[Worker | None] = contextvars.ContextVar(
    "tau_sandbox_worker", default=None
)
_mode: contextvars.ContextVar[Mode] = contextvars.ContextVar(
    "tau_sandbox_mode", default="loop"
)


def _dispatch(fn: Callable[..., Any], streaming: bool, /, **kwargs: Any) -> Any:
    worker = _worker.get()
    if worker is None:
        return fn(**kwargs)
    if streaming:
        return worker.iterate(fn, **kwargs)
    return worker.run(fn, **kwargs)


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


def _resolve_remote(
    label: str, resolution: dict[str, Any] | BaseException
) -> None:
    ai.resolve_hook(label, resolution)


async def _cancel_remote(label: str, reason: str | None) -> None:
    await ai.cancel_hook(label, reason=reason)


class _HookBridge:
    """Stands in locally for the remote loop's pending hooks.

    A stand-in is a live hook in the local run's registry, so the
    caller resolves or cancels it as usual; the outcome is then passed
    on to the hook in the worker.
    """

    def __init__(self, worker: Worker) -> None:
        self._worker = worker
        self._registry = ai.get_hook_registry()
        self._runtime = ai.agents.runtime.get_runtime()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._cancelled: set[str] = set()

    def forward(self, event: ai.events.HookEvent) -> bool:
        """Note a hook event from the worker; return whether to yield it.

        A cancellation the caller made here was already reported here.
        """
        hook = event.hook
        if hook.status == "cancelled":
            return hook.hook_id not in self._cancelled
        if hook.status == "pending":
            future: asyncio.Future[dict[str, Any]] = asyncio.Future()
            self._registry._live_hooks[hook.hook_id] = (
                future,
                hook.metadata,
                self._runtime,
            )
            self._tasks[hook.hook_id] = asyncio.create_task(
                self._pass_on(hook.hook_id, future)
            )
        return True

    async def _pass_on(
        self, label: str, future: asyncio.Future[dict[str, Any]]
    ) -> None:
        try:
            try:
                resolution: dict[str, Any] | BaseException = await future
            except asyncio.CancelledError as exc:
                if not future.cancelled():
                    raise
                self._cancelled.add(label)
                reason = exc.args[0] if exc.args else None
                await self._worker.run(_cancel_remote, label, reason)
                return
            except Exception as exc:
                resolution = exc
            await self._worker.run(_resolve_remote, label, resolution)
        finally:
            self._registry._live_hooks.pop(label, None)
            self._tasks.pop(label, None)

    async def aclose(self) -> None:
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


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
        if worker is None or _mode.get() == "tools":
            async for x in super().loop(context):
                yield x
        else:
            bridge = _HookBridge(worker)
            try:
                async with worker.iterate(_remote_loop, self, context) as s:
                    async for item in s:
                        if isinstance(item, _Added):
                            context.messages.extend(item.messages)
                        elif not isinstance(
                            item, ai.events.HookEvent
                        ) or bridge.forward(item):
                            yield item
            finally:
                await bridge.aclose()


def _install_hook_registry() -> None:
    """Give the worker a hook registry, for tools that call hooks.

    Being sync, it runs in the worker's root context, so every later
    request sees it.
    """
    ai.agents.hooks._hook_registry.set(ai.agents.hooks.HookRegistry())


@contextlib.asynccontextmanager
async def use_worker(
    worker: Worker, mode: Mode = "loop"
) -> AsyncIterator[None]:
    """Make ``worker`` the ambient worker, in ``mode``, until exit."""
    await worker.run(_install_hook_registry)
    token = _worker.set(worker)
    mode_token = _mode.set(mode)
    try:
        yield
    finally:
        _mode.reset(mode_token)
        _worker.reset(token)
