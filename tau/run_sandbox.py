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
block other requests while they do.

Every other request gets exactly one reply, ``(request_id, reply)`` with
``reply`` a pickled ``("ok", value)`` or ``("err", exc)``. Replies arrive as
requests finish, in any order. Each is framed as an 8-byte big-endian length
followed by the pickle.
With ``--base64`` each frame is base64-encoded and newline-terminated, which
survives transports that decode output as text, such as sandbox command logs.
"""

import argparse
import asyncio
import base64
import inspect
import itertools
import os
import pickle
import runpy
import shlex
import stat
import struct
import subprocess
import sys
import threading
import types
import typing
from abc import ABC, abstractmethod
from contextlib import AsyncExitStack
from pathlib import Path

import anyio
from anyio.abc import ByteReceiveStream
from anyio.streams.buffered import BufferedByteReceiveStream

_LENGTH = struct.Struct(">Q")


async def protected[T](arg: typing.Awaitable[T]) -> T:
    '''Wait for a future, deferring cancellation until it is complete.

    If you do
        await deferred_shield(something())

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

    '''
    loop = None
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        pass

    if not loop:
        with anyio.CancelScope(shield=True):
            return await arg

    done = False
    result: T | None = None
    exc = None
    waiter: asyncio.Future[None] | None = None

    async def go():
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

    task = asyncio.Task(go(), loop=loop, eager_start=True)

    cancelled = None
    scope = None
    try:
        while not done:
            try:
                waiter = asyncio.Future()
                await waiter
            except asyncio.CancelledError as cex:
                if not scope and (anyio := sys.modules.get("anyio")):
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


def encode_reply(request_id, reply, *, b64):
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

    def __init__(self, stream):
        self._stream = stream
        self._text = ""
        self._data = b""
        self._eof = False

    async def receive(self, max_bytes=65536):
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

    async def aclose(self):
        await self._stream.aclose()


def unwrap_reply(data):
    status, value = pickle.loads(data)
    if status == "err":
        raise value
    return value


def _load_main(path):
    path = os.path.abspath(path)
    sys.path.insert(0, os.path.dirname(path))
    # Same approach as multiprocessing's spawn start method: run the script
    # under a name that skips its `if __name__ == "__main__"` block.
    main = types.ModuleType("__mp_main__")
    sys.modules["__main__"] = sys.modules["__mp_main__"] = main
    main.__dict__.update(runpy.run_path(path, run_name="__mp_main__"))


def _pickleable_error(exc):
    try:
        pickle.dumps(exc)
        return exc
    except Exception:
        return RuntimeError(f"{type(exc).__name__}: {exc}")


class _Server:
    """Handles requests on the worker's event loop."""

    def __init__(self, out, b64):
        self._out = out
        self._b64 = b64
        self._tasks = {}
        self._iterators = {}

    def _reply(self, request_id, status, value):
        self._out.write(encode_reply(request_id, (status, value), b64=self._b64))
        self._out.flush()

    def start(self, request_id, payload):
        try:
            is_async, value = self._handle(pickle.loads(payload))
        except Exception as exc:
            self._reply(request_id, "err", _pickleable_error(exc))
            return
        if is_async:
            self._tasks[request_id] = asyncio.create_task(self._finish(request_id, value))
        else:
            self._reply(request_id, "ok", value)

    async def _finish(self, request_id, awaitable):
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

    async def shutdown(self):
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _handle(self, command):
        """Return ``(False, value)``, or ``(True, awaitable)`` to finish in a task."""
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
                    result if hasattr(result, "__anext__") else iter(result)
                )
                return False, None
            case ("next", iter_id):
                it = self._iterators[iter_id]
                if hasattr(it, "__anext__"):
                    return True, self._anext(iter_id, it)
                return False, self._next(iter_id, it)
            case ("close", iter_id):
                it = self._iterators.pop(iter_id, None)
                if hasattr(it, "aclose"):
                    return True, it.aclose()
                if hasattr(it, "close"):
                    it.close()
                return False, None
            case ("cancel", target):
                if task := self._tasks.get(target):
                    task.cancel()
                return False, None
            case _:
                raise ValueError(f"unknown command: {command!r}")

    def _next(self, iter_id, it):
        try:
            return "yield", next(it)
        except StopIteration as stop:
            del self._iterators[iter_id]
            return "done", stop.value
        except BaseException:
            del self._iterators[iter_id]
            raise

    async def _anext(self, iter_id, it):
        try:
            return "yield", await it.__anext__()
        except StopAsyncIteration:
            del self._iterators[iter_id]
            return "done", None
        except BaseException:
            self._iterators.pop(iter_id, None)
            raise


async def _serve(stdin, out, b64):
    loop = asyncio.get_running_loop()
    requests = asyncio.Queue()

    def read_requests():
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


def worker(*, b64):
    stdin = sys.stdin.buffer
    out = os.fdopen(os.dup(sys.stdout.fileno()), "wb")
    # Anything the called functions print goes to stderr, keeping stdout for replies.
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    sys.stdout.reconfigure(line_buffering=True)
    asyncio.run(_serve(stdin, out, b64))


async def sync_dir(box, src_dir, target_dir):
    """Copies a local directory to a sandbox.

    Skips .venv*, .env*, .git, .tau.
    """
    src = Path(src_dir)
    await box.fs.mkdir(target_dir)
    async with box.fs.batch(cwd=target_dir) as batch:
        for root, dirs, files in os.walk(src):
            dirs[:] = [d for d in dirs if not _skipped(d)]
            for name in files:
                path = Path(root, name)
                if _skipped(name) or not path.is_file():
                    continue
                batch.write_bytes(
                    path.relative_to(src).as_posix(),
                    path.read_bytes(),
                    mode=stat.S_IMODE(path.stat().st_mode),
                )


def _skipped(name):
    return name.startswith((".venv", ".env")) or name in (".git", ".tau")


class _Pending:
    def __init__(self):
        self.done = anyio.Event()
        self.reply = None
        self.error = None


class BaseWorker(ABC):
    """A connection to a worker, used as an async context manager.

    Calls can be made concurrently from several tasks; each waits only for its
    own reply.
    """

    def __init__(self, main_path=None):
        if main_path is None:
            main_path = getattr(sys.modules["__main__"], "__file__", None)
        self._main_path = main_path
        self._iter_ids = itertools.count()
        self._request_ids = itertools.count()
        self._pending = {}
        self._closed = False
        self._abandoned_iterators = []

    async def _start(self, stdin, stdout):
        """Start reading replies from ``stdout``, then send ``init``.

        Subclasses call this from ``__aenter__`` and ``_stop`` from
        ``__aexit__``, in the same task.
        """
        self._stdin = stdin
        self._stdout = BufferedByteReceiveStream(stdout)
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

    async def _stop(self):
        self._reader_scope.cancel()
        await self._task_group.__aexit__(None, None, None)

    @abstractmethod
    async def __aenter__(self): ...

    @abstractmethod
    async def __aexit__(self, *exc_info): ...

    async def _closed_message(self):
        return "worker closed its output"

    async def _read_replies(self):
        with self._reader_scope:
            await self._read_replies_until_closed()

    async def _read_replies_until_closed(self):
        try:
            while True:
                (size,) = _LENGTH.unpack(await self._stdout.receive_exactly(_LENGTH.size))
                request_id, reply = pickle.loads(await self._stdout.receive_exactly(size))
                if pending := self._pending.get(request_id):
                    pending.reply = reply
                    pending.done.set()
        except (anyio.EndOfStream, anyio.IncompleteRead, anyio.BrokenResourceError):
            error = None
        except Exception as exc:
            error = exc
        self._closed = True
        if self._pending:
            if error is None:
                error = EOFError(await self._closed_message())
            for pending in self._pending.values():
                pending.error = error
                pending.done.set()

    async def _send(self, request_id, command):
        data = pickle.dumps((request_id, pickle.dumps(command)))
        async with self._send_lock:
            # Close iterators abandoned by RemoteIterator.__del__; nothing
            # waits for these replies, so the reader drops them.
            closes = []
            while self._abandoned_iterators:
                close = ("close", self._abandoned_iterators.pop())
                closes.append(pickle.dumps((next(self._request_ids), pickle.dumps(close))))
            # A send interrupted partway would leave half a request in the stream.

            # XXX: but is this actually quite what we want?? We might
            # still want some kind of timeout... Hm.
            await protected(self._stdin.send(b"".join(closes) + data))

    async def _call(self, command):
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
                await protected(self._send(next(self._request_ids), ("cancel", request_id)))
            raise
        finally:
            del self._pending[request_id]
        if pending.error is not None:
            raise pending.error
        return unwrap_reply(pending.reply)

    async def run(self, func, *args, **kwargs):
        return await self._call(("run", func, args, kwargs))

    def iterate(self, func, *args, **kwargs):
        """Iterate remotely over ``func(*args, **kwargs)``, one item per round trip.

        Use ``async with`` to close the remote iterator if you stop early;
        otherwise it stays open in the worker until the worker exits.
        """
        return RemoteIterator(self, func, args, kwargs)


class RemoteIterator:
    """An async iterator over a sync or async iterable in a worker."""

    def __init__(self, worker, func, args, kwargs):
        self._worker = worker
        self._call_args = (func, args, kwargs)
        self._id = next(worker._iter_ids)
        self._started = False
        self._finished = False

    def __aiter__(self):
        return self

    async def __anext__(self):
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
        return value

    async def aclose(self):
        if self._started and not self._finished:
            self._finished = True
            await protected(self._worker._call(("close", self._id)))
        self._finished = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        await self.aclose()

    def __del__(self):
        if self._started and not self._finished:
            self._worker._abandoned_iterators.append(self._id)


class LocalWorker(BaseWorker):
    """A worker subprocess on this machine."""

    def __init__(self, main_path=None, *, python=sys.executable):
        super().__init__(main_path)
        self._python = python

    async def __aenter__(self):
        self._proc = await anyio.open_process([self._python, __file__], stderr=None)
        try:
            await self._start(self._proc.stdin, self._proc.stdout)
        except BaseException:
            await self._proc.aclose()
            raise
        return self

    async def __aexit__(self, *exc_info):
        await self._stop()
        await self._proc.aclose()

    async def _closed_message(self):
        return f"worker exited with {await self._proc.wait()}"


class SandboxWorker(BaseWorker):
    """A worker in a sandbox, running a copy of ``src_dir`` with ``uv run``.

    ``main_path`` is a local path inside ``src_dir``.
    """

    def __init__(self, box, main_path=None, *, src_dir=".", target_dir="workspace", env=None):
        if main_path is None:
            main_path = sys.modules["__main__"].__file__
        remote_main = os.path.relpath(main_path, src_dir)
        if remote_main.startswith(os.pardir):
            raise ValueError(f"{main_path} is not inside {src_dir}")
        super().__init__(Path(remote_main).as_posix())
        self._box = box
        self._src_dir = src_dir
        self._target_dir = target_dir
        self._env = env

    def _uv_args(self):
        python = f"{sys.version_info.major}.{sys.version_info.minor}"
        script = Path(os.path.relpath(__file__, self._src_dir)).as_posix()
        return ["uv", "run", "--python", python, script]

    async def __aenter__(self):
        await sync_dir(self._box, self._src_dir, self._target_dir)
        uv, *args = self._uv_args()
        self._cmd = await self._box.create_command(
            uv,
            [*args, "--base64"],
            cwd=self._target_dir,
            env=self._env,
            stdin=subprocess.PIPE,
        )
        try:
            await self._start(self._cmd.stdin, Base64ByteReceiveStream(self._cmd.stdout))
        except BaseException:
            await protected(self._cmd.kill())
            raise
        return self

    async def __aexit__(self, *exc_info):
        await self._stop()
        try:
            await self._cmd.stdin.aclose()
            await self._cmd.wait()
        except Exception as exc:
            if not _sandbox_stopped(exc):
                raise
        await self._stdout.aclose()
        await self._cmd.stderr.aclose()

    async def _closed_message(self):
        returncode = await self._cmd.wait()
        stderr = await self._cmd.stderr.read()
        return f"worker exited with {returncode}\n{stderr}"


def _sandbox_stopped(exc):
    """Whether ``exc`` (or its cause) says the sandbox is already gone."""
    while exc is not None:
        if getattr(exc, "code", None) == "sandbox_stopped":
            return True
        exc = exc.__cause__
    return False


class PtySandboxWorker(SandboxWorker):
    """A sandbox worker attached to a PTY in raw mode, so replies are raw bytes.

    The worker's stderr goes to a file in the sandbox instead of the terminal.
    """

    _READY = b"RUN_SYNC_READY\n"

    async def __aenter__(self):
        await sync_dir(self._box, self._src_dir, self._target_dir)
        self._stderr_path = f"/tmp/run_sync-{os.urandom(4).hex()}.err"
        script = (
            f"cd {shlex.quote(self._target_dir)} && stty raw -echo"
            f" && printf {shlex.quote(self._READY.decode())}"
            f" && exec {shlex.join(self._uv_args())} 2>{self._stderr_path}"
        )
        self._stack = AsyncExitStack()
        try:
            self._pty = await self._stack.enter_async_context(
                self._box.open_interactive("sh", ["-c", script])
            )
            stdout = BufferedByteReceiveStream(self._pty.stream)
            await stdout.receive_until(self._READY, 65536)
            await self._start(self._pty.stream, stdout)
        except BaseException:
            await protected(self._stack.aclose())
            raise
        return self

    async def __aexit__(self, *exc_info):
        await self._stop()
        await self._stack.aclose()

    async def _closed_message(self):
        returncode = await self._pty.wait()
        stderr = await self._box.fs.read_text(self._stderr_path)
        return f"worker exited with {returncode}\n{stderr}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base64", action="store_true", help="base64-encode replies, one per line")
    worker(b64=parser.parse_args().base64)
