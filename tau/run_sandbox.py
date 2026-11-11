"""Run functions in another Python process.

The worker reads requests from stdin and writes replies to stdout. Each request
is a pickled ``(request_id, command)`` pair, with the command pickled
separately inside it, so a command that fails to unpickle can still be
answered. Commands:

* ``("init", main_path)`` loads ``main_path`` as the ``__main__`` module, so
  functions pickled by reference to ``__main__`` resolve.
* ``("run", func, args, kwargs)`` calls ``func(*args, **kwargs)``. If that
  returns an awaitable, such as from a coroutine function, it runs as a task on
  the worker's event loop, concurrently with other requests.
* ``("iter", id, func, args, kwargs)`` calls ``func(*args, **kwargs)`` and
  keeps the result, a sync or async generator or any iterable, under ``id``.
* ``("next", id)`` advances iterator ``id``, replying ``("yield", value)`` or,
  once it is exhausted, ``("done", return_value)``.
* ``("close", id)`` closes iterator ``id`` early.
* ``("cancel", request_id)`` cancels that request's task, if it is still
  running. Cancelled requests get no reply.

Sync functions run on the event loop's thread, so they run one at a time and
block other requests while they do. They also run in the worker's root
context, so context variables they set are seen by every later request; async
functions run in a copy of it.

Every other request gets exactly one reply, ``(request_id, reply)`` with
``reply`` a pickled ``("ok", value)`` or ``("err", exc)``. Replies arrive as
requests finish, in any order. Each is framed as an 8-byte big-endian length
followed by the pickle.
With ``--base64`` each frame is base64-encoded and newline-terminated, which
survives transports that decode output as text, such as sandbox command logs.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import dataclasses
import inspect
import itertools
import os
import pickle
import runpy
import struct
import sys
import threading
import types
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
)
from contextlib import (
    AbstractAsyncContextManager,
    AsyncExitStack,
    asynccontextmanager,
    suppress,
)
from typing import IO, Any, Protocol, Self, overload

import anyio
import anyio.abc
from anyio.abc import ByteReceiveStream, ByteSendStream, ObjectReceiveStream
from anyio.streams.buffered import BufferedByteReceiveStream

_LENGTH = struct.Struct(">Q")


async def protected[T](arg: Awaitable[T]) -> T:
    """Wait for a future, deferring cancellation until it is complete.

    If you do
        await protected(something())

    it is approximately equivalent to
        await something()

    except that if the coroutine containing it is cancelled,
    something() is protected from cancellation, and *additionally*
    CancelledError will not be delivered to the caller yet.
    If the caller gets cancelled while executing this, however,
    the cancellation state *will* remain armed, so that cancellation
    should be delivered the next time it blocks

    If anyio is imported, we will enter an anyio CancelScope. As an
    optimization, we only do it *after* but only *after* we take a
    CancelledError.

    This is useful for shielding cleanup code from cancellation,
    especially code in finally blocks.

    """
    loop = None
    with suppress(RuntimeError):
        loop = asyncio.get_running_loop()

    if not loop:
        with anyio.CancelScope(shield=True):
            return await arg

    done = False
    result: T | None = None
    exc: BaseException | None = None
    waiter: asyncio.Future[None] | None = None

    async def go() -> None:
        nonlocal done, result, exc
        try:
            result = await arg
        except BaseException as e:
            exc = e
            # swallow
        finally:
            done = True
            if waiter and not waiter.done():
                waiter.set_result(None)

    # The event loop only holds tasks weakly.
    _task = asyncio.Task(go(), loop=loop, eager_start=True)

    cancelled: asyncio.CancelledError | None = None
    scope: anyio.CancelScope | None = None
    try:
        while not done:
            try:
                waiter = asyncio.Future()
                await waiter
            except asyncio.CancelledError as cex:
                if not scope:
                    scope = anyio.CancelScope(shield=True)
                    scope.__enter__()

                cancelled = cex
    finally:
        if scope:
            scope.__exit__(None, None, None)

    if cancelled:
        current = asyncio.current_task()
        assert current
        # For asyncio cancellations, it is edge triggered, so
        # we need to recancel the task, so that it gets
        # delivered, but then also *uncancel* it, so the count
        # doesn't go up.
        current.cancel(cancelled.args[0] if cancelled.args else None)
        current.uncancel()

    if exc:
        raise exc
    return result  # type: ignore[return-value]


def encode_reply(
    request_id: int, reply: tuple[str, Any], *, b64: bool
) -> bytes:
    try:
        inner = pickle.dumps(reply)
    except Exception as exc:
        inner = pickle.dumps(("err", _pickleable_error(exc)))
    data = pickle.dumps((request_id, inner))
    frame = _LENGTH.pack(len(data)) + data
    if b64:
        return base64.b64encode(frame) + b"\n"
    return frame


class Base64ByteReceiveStream(ByteReceiveStream):
    """Decode newline-terminated base64 lines from a stream of text."""

    def __init__(self, stream: ObjectReceiveStream[str]) -> None:
        self._stream = stream
        self._text = ""
        self._data = b""
        self._eof = False

    async def receive(self, max_bytes: int = 65536) -> bytes:
        while not self._data:
            line, newline, rest = self._text.partition("\n")
            if newline:
                self._text = rest
            elif self._eof:
                self._text = ""
                if not line.strip():
                    raise anyio.EndOfStream
            else:
                try:
                    self._text += await self._stream.receive()
                except anyio.EndOfStream:
                    self._eof = True
                continue
            self._data = base64.b64decode(line.strip(), validate=True)
        data, self._data = self._data[:max_bytes], self._data[max_bytes:]
        return data

    async def aclose(self) -> None:
        await self._stream.aclose()


def unwrap_reply(data: bytes) -> Any:
    status, value = pickle.loads(data)
    if status == "err":
        raise value
    return value


def _load_main(path: str) -> None:
    path = os.path.abspath(path)
    sys.path.insert(0, os.path.dirname(path))
    # Same approach as multiprocessing's spawn start method: run the script
    # under a name that skips its `if __name__ == "__main__"` block.
    main = types.ModuleType("__mp_main__")
    sys.modules["__main__"] = sys.modules["__mp_main__"] = main
    main.__dict__.update(runpy.run_path(path, run_name="__mp_main__"))


def _pickleable_error(exc: BaseException) -> BaseException:
    try:
        pickle.dumps(exc)
        return exc
    except Exception:
        return RuntimeError(f"{type(exc).__name__}: {exc}")


class _Server:
    """Handles requests on the worker's event loop."""

    def __init__(self, out: IO[bytes], b64: bool) -> None:
        self._out = out
        self._b64 = b64
        self._tasks: dict[int, asyncio.Task[None]] = {}
        self._iterators: dict[int, Iterator[Any] | AsyncIterator[Any]] = {}

    def _reply(self, request_id: int, status: str, value: Any) -> None:
        self._out.write(
            encode_reply(request_id, (status, value), b64=self._b64)
        )
        self._out.flush()

    def start(self, request_id: int, payload: bytes) -> None:
        try:
            is_async, value = self._handle(pickle.loads(payload))
        except Exception as exc:
            self._reply(request_id, "err", _pickleable_error(exc))
            return
        if is_async:
            self._tasks[request_id] = asyncio.create_task(
                self._finish(request_id, value)
            )
        else:
            self._reply(request_id, "ok", value)

    async def _finish(self, request_id: int, awaitable: Awaitable[Any]) -> None:
        try:
            value = await awaitable
        except asyncio.CancelledError:
            return
        except Exception as exc:
            self._reply(request_id, "err", _pickleable_error(exc))
        else:
            self._reply(request_id, "ok", value)
        finally:
            self._tasks.pop(request_id, None)

    async def shutdown(self) -> None:
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _handle(self, command: tuple[Any, ...]) -> tuple[bool, Any]:
        """Return ``(False, value)``, or ``(True, awaitable)`` to await."""
        match command:
            case ("init", path):
                _load_main(path)
                return False, None
            case ("run", func, args, kwargs):
                result = func(*args, **kwargs)
                return inspect.isawaitable(result), result
            case ("iter", iter_id, func, args, kwargs):
                result = func(*args, **kwargs)
                self._iterators[iter_id] = (
                    result
                    if isinstance(result, AsyncIterator)
                    else iter(result)
                )
                return False, None
            case ("next", iter_id):
                it = self._iterators[iter_id]
                if isinstance(it, AsyncIterator):
                    return True, self._anext(iter_id, it)
                return False, self._next(iter_id, it)
            case ("close", iter_id):
                closing: Any = self._iterators.pop(iter_id, None)
                if hasattr(closing, "aclose"):
                    return True, closing.aclose()
                if hasattr(closing, "close"):
                    closing.close()
                return False, None
            case ("cancel", target):
                if task := self._tasks.get(target):
                    task.cancel()
                return False, None
            case _:
                raise ValueError(f"unknown command: {command!r}")

    def _next(self, iter_id: int, it: Iterator[Any]) -> tuple[str, Any]:
        try:
            return "yield", next(it)
        except StopIteration as stop:
            del self._iterators[iter_id]
            return "done", stop.value
        except BaseException:
            del self._iterators[iter_id]
            raise

    async def _anext(
        self, iter_id: int, it: AsyncIterator[Any]
    ) -> tuple[str, Any]:
        try:
            return "yield", await it.__anext__()
        except StopAsyncIteration:
            del self._iterators[iter_id]
            return "done", None
        except BaseException:
            self._iterators.pop(iter_id, None)
            raise


async def _serve(stdin: IO[bytes], out: IO[bytes], b64: bool) -> None:
    loop = asyncio.get_running_loop()
    requests: asyncio.Queue[tuple[int, bytes] | None] = asyncio.Queue()

    def read_requests() -> None:
        while True:
            try:
                request = pickle.load(stdin)
            except Exception:
                request = None
            loop.call_soon_threadsafe(requests.put_nowait, request)
            if request is None:
                return

    threading.Thread(target=read_requests, daemon=True).start()
    server = _Server(out, b64)
    while (request := await requests.get()) is not None:
        server.start(*request)
    await server.shutdown()


def worker(*, b64: bool) -> None:
    stdin = sys.stdin.buffer
    out = os.fdopen(os.dup(sys.stdout.fileno()), "wb")
    # Anything the called functions print goes to stderr, keeping stdout
    # for replies.
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]
    asyncio.run(_serve(stdin, out, b64))


class _Pending:
    def __init__(self) -> None:
        self.done = anyio.Event()
        self.reply: bytes | None = None
        self.error: BaseException | None = None


class Process(Protocol):
    """A running worker process: its stdin, its stdout, and how it ended.

    ``Worker`` takes an async context manager that starts one and, on
    exit, stops it.
    """

    @property
    def stdin(self) -> ByteSendStream: ...

    @property
    def stdout(self) -> ByteReceiveStream: ...

    async def exit_message(self) -> str:
        """Describe how the process ended, once its output has closed."""
        ...


class Worker:
    """A connection to a worker, used as an async context manager.

    ``process`` starts the worker process on entry and stops it on exit.
    Calls can be made concurrently from several tasks; each waits only for its
    own reply.
    """

    def __init__(
        self,
        process: AbstractAsyncContextManager[Process],
        main_path: str | None = None,
    ) -> None:
        if main_path is None:
            main_path = getattr(sys.modules["__main__"], "__file__", None)
        self._process_cm = process
        self._main_path = main_path
        self._iter_ids = itertools.count()
        self._request_ids = itertools.count()
        self._pending: dict[int, _Pending] = {}
        self._closed = False
        self._abandoned_iterators: list[int] = []

    async def __aenter__(self) -> Self:
        async with AsyncExitStack() as stack:
            self._process = await stack.enter_async_context(self._process_cm)
            await self._start()
            self._exit_stack = stack.pop_all()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._stop()
        await self._exit_stack.aclose()

    async def _start(self) -> None:
        """Start reading replies, then send ``init``."""
        self._stdin = self._process.stdin
        self._stdout = BufferedByteReceiveStream(self._process.stdout)
        self._send_lock = anyio.Lock()
        self._reader_scope = anyio.CancelScope()
        self._task_group = anyio.create_task_group()
        await self._task_group.__aenter__()
        self._task_group.start_soon(self._read_replies)
        try:
            if self._main_path is not None:
                await self._call(("init", self._main_path))
        except BaseException:
            await self._stop()
            raise

    async def _stop(self) -> None:
        self._reader_scope.cancel()
        await self._task_group.__aexit__(None, None, None)

    async def _closed_message(self) -> str:
        return await self._process.exit_message()

    async def _read_replies(self) -> None:
        with self._reader_scope:
            await self._read_replies_until_closed()

    async def _read_replies_until_closed(self) -> None:
        try:
            while True:
                (size,) = _LENGTH.unpack(
                    await self._stdout.receive_exactly(_LENGTH.size)
                )
                request_id, reply = pickle.loads(
                    await self._stdout.receive_exactly(size)
                )
                if pending := self._pending.get(request_id):
                    pending.reply = reply
                    pending.done.set()
        except (
            anyio.EndOfStream,
            anyio.IncompleteRead,
            anyio.BrokenResourceError,
        ):
            error: BaseException | None = None
        except Exception as exc:
            error = exc
        self._closed = True
        if self._pending:
            if error is None:
                error = EOFError(await self._closed_message())
            for pending in self._pending.values():
                pending.error = error
                pending.done.set()

    async def _send(self, request_id: int, command: tuple[Any, ...]) -> None:
        data = pickle.dumps((request_id, pickle.dumps(command)))
        async with self._send_lock:
            # Close iterators abandoned by RemoteIterator.__del__; nothing
            # waits for these replies, so the reader drops them.
            closes = []
            while self._abandoned_iterators:
                close = ("close", self._abandoned_iterators.pop())
                closes.append(
                    pickle.dumps((next(self._request_ids), pickle.dumps(close)))
                )
            # A send interrupted partway would leave half a request in the
            # stream.

            # XXX: but is this actually quite what we want?? We might
            # still want some kind of timeout... Hm.
            await protected(self._stdin.send(b"".join(closes) + data))

    async def _call(self, command: tuple[Any, ...]) -> Any:
        if self._closed:
            raise EOFError(await self._closed_message())
        request_id = next(self._request_ids)
        pending = self._pending[request_id] = _Pending()
        try:
            await self._send(request_id, command)
            await pending.done.wait()
        except anyio.get_cancelled_exc_class():
            if not pending.done.is_set() and not self._closed:
                # We are already cancelled, and taking the send lock is an
                # anyio checkpoint, so the whole send has to be protected.
                await protected(
                    self._send(next(self._request_ids), ("cancel", request_id))
                )
            raise
        finally:
            del self._pending[request_id]
        if pending.error is not None:
            raise pending.error
        assert pending.reply is not None
        return unwrap_reply(pending.reply)

    @overload
    async def run[**P, R](
        self, func: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs
    ) -> R: ...
    @overload
    async def run[**P, R](
        self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> R: ...
    async def run(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        return await self._call(("run", func, args, kwargs))

    def iterate[**P, T](
        self,
        func: Callable[P, Iterable[T] | AsyncIterable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RemoteIterator[T]:
        """Iterate remotely over ``func(*args, **kwargs)``, an item per trip.

        Use ``async with`` to close the remote iterator if you stop early;
        otherwise it stays open in the worker until the worker exits.
        """
        return RemoteIterator(self, func, args, kwargs)


class RemoteIterator[T]:
    """An async iterator over a sync or async iterable in a worker."""

    def __init__(
        self,
        worker: Worker,
        func: Callable[..., Iterable[T] | AsyncIterable[T]],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        self._worker = worker
        self._call_args = (func, args, kwargs)
        self._id = next(worker._iter_ids)
        self._started = False
        self._finished = False

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        if self._finished:
            raise StopAsyncIteration
        try:
            if not self._started:
                self._started = True
                await self._worker._call(("iter", self._id, *self._call_args))
            kind, value = await self._worker._call(("next", self._id))
        except BaseException:
            self._finished = True
            raise
        if kind == "done":
            self._finished = True
            raise StopAsyncIteration
        return value  # type: ignore[no-any-return]

    async def aclose(self) -> None:
        if self._started and not self._finished:
            self._finished = True
            await protected(self._worker._call(("close", self._id)))
        self._finished = True

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    def __del__(self) -> None:
        if self._started and not self._finished:
            self._worker._abandoned_iterators.append(self._id)


@dataclasses.dataclass
class _LocalProcess:
    stdin: ByteSendStream
    stdout: ByteReceiveStream
    proc: anyio.abc.Process

    async def exit_message(self) -> str:
        return f"worker exited with {await self.proc.wait()}"


@asynccontextmanager
async def local_process(
    python: str = sys.executable,
) -> AsyncIterator[Process]:
    """Start a worker subprocess on this machine."""
    async with await anyio.open_process(
        [python, __file__], stderr=None
    ) as proc:
        assert proc.stdin is not None and proc.stdout is not None
        yield _LocalProcess(proc.stdin, proc.stdout, proc)


def local_worker(
    main_path: str | None = None, *, python: str = sys.executable
) -> Worker:
    """A worker subprocess on this machine."""
    return Worker(local_process(python), main_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base64",
        action="store_true",
        help="base64-encode replies, one per line",
    )
    worker(b64=parser.parse_args().base64)
