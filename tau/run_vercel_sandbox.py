"""Run functions in a Vercel sandbox, using a ``run_sandbox`` worker."""

from __future__ import annotations

import dataclasses
import os
import shlex
import stat
import subprocess
import sys
from collections.abc import AsyncIterator, Iterable
from contextlib import AsyncExitStack, asynccontextmanager
from os import PathLike
from pathlib import Path

from anyio.abc import ByteReceiveStream, ByteSendStream
from anyio.streams.buffered import BufferedByteReceiveStream
from vercel.sandbox import Command, InteractiveSession, Sandbox

from tau import run_sandbox
from tau.run_sandbox import (
    Base64ByteReceiveStream,
    Process,
    Worker,
    protected,
)

type StrPath = str | PathLike[str]


async def sync_dir(box: Sandbox, src_dir: StrPath, target_dir: str) -> None:
    """Copies a local directory to a sandbox.

    Skips .venv*, .env*, .git, .tau.
    """
    src = Path(src_dir)
    paths = []
    for root, dirs, files in os.walk(src):
        dirs[:] = [d for d in dirs if not _skipped(d)]
        for name in files:
            if not _skipped(name):
                paths.append(Path(root, name).relative_to(src))
    await sync_paths(box, src, target_dir, paths)


async def sync_paths(
    box: Sandbox, src_dir: StrPath, target_dir: str, paths: Iterable[StrPath]
) -> None:
    """Copies the given paths, relative to ``src_dir``, to a sandbox.

    Paths that aren't regular files are skipped.
    """
    src = Path(src_dir)
    await box.fs.mkdir(target_dir)
    async with box.fs.batch(cwd=target_dir) as batch:
        for rel in paths:
            path = src / rel
            if not path.is_file():
                continue
            batch.write_bytes(
                Path(rel).as_posix(),
                path.read_bytes(),
                mode=stat.S_IMODE(path.stat().st_mode),
            )


def _skipped(name: str) -> bool:
    return name.startswith((".venv", ".env")) or name in (".git", ".tau")


def _remote_main(main_path: str | None, src_dir: StrPath) -> str:
    if main_path is None:
        main_path = sys.modules["__main__"].__file__
        assert main_path is not None
    remote_main = os.path.relpath(main_path, src_dir)
    if remote_main.startswith(os.pardir):
        raise ValueError(f"{main_path} is not inside {src_dir}")
    return Path(remote_main).as_posix()


def _uv_args(src_dir: StrPath) -> list[str]:
    python = f"{sys.version_info.major}.{sys.version_info.minor}"
    script = Path(os.path.relpath(run_sandbox.__file__, src_dir)).as_posix()
    return ["uv", "run", "--python", python, script]


@dataclasses.dataclass
class _CommandProcess:
    stdin: ByteSendStream
    stdout: ByteReceiveStream
    cmd: Command

    async def exit_message(self) -> str:
        returncode = await self.cmd.wait()
        stderr = await self.cmd.stderr.read() if self.cmd.stderr else ""
        return f"worker exited with {returncode}\n{stderr}"


@asynccontextmanager
async def sandbox_process(
    box: Sandbox,
    *,
    src_dir: StrPath = ".",
    target_dir: str = "workspace",
    env: dict[str, str] | None = None,
) -> AsyncIterator[Process]:
    """Start a worker in a sandbox on a copy of ``src_dir``, with uv."""
    await sync_dir(box, src_dir, target_dir)
    uv, *args = _uv_args(src_dir)
    cmd = await box.create_command(
        uv,
        [*args, "--base64"],
        cwd=target_dir,
        env=env,
        stdin=subprocess.PIPE,
    )
    assert cmd.stdin is not None and cmd.stdout is not None
    stdout = Base64ByteReceiveStream(cmd.stdout)
    try:
        yield _CommandProcess(cmd.stdin, stdout, cmd)
    except BaseException:
        await protected(cmd.kill())
        raise
    try:
        await cmd.stdin.aclose()
        await cmd.wait()
    except Exception as exc:
        if not _sandbox_stopped(exc):
            raise
    await stdout.aclose()
    if cmd.stderr is not None:
        await cmd.stderr.aclose()


def sandbox_worker(
    box: Sandbox,
    main_path: str | None = None,
    *,
    src_dir: StrPath = ".",
    target_dir: str = "workspace",
    env: dict[str, str] | None = None,
) -> Worker:
    """A worker in a sandbox, running a copy of ``src_dir`` with ``uv run``.

    ``main_path`` is a local path inside ``src_dir``.
    """
    process = sandbox_process(
        box, src_dir=src_dir, target_dir=target_dir, env=env
    )
    return Worker(process, _remote_main(main_path, src_dir))


def _sandbox_stopped(exc: BaseException | None) -> bool:
    """Whether ``exc`` (or its cause) says the sandbox is already gone."""
    while exc is not None:
        if getattr(exc, "code", None) == "sandbox_stopped":
            return True
        exc = exc.__cause__
    return False


_READY = b"RUN_SYNC_READY\n"


@dataclasses.dataclass
class _PtyProcess:
    stdin: ByteSendStream
    stdout: ByteReceiveStream
    pty: InteractiveSession
    box: Sandbox
    stderr_path: str

    async def exit_message(self) -> str:
        returncode = await self.pty.wait()
        stderr = await self.box.fs.read_text(self.stderr_path)
        return f"worker exited with {returncode}\n{stderr}"


@asynccontextmanager
async def pty_sandbox_process(
    box: Sandbox, *, src_dir: StrPath = ".", target_dir: str = "workspace"
) -> AsyncIterator[Process]:
    """Like ``sandbox_process``, but on a PTY in raw mode, so replies are raw.

    The worker's stderr goes to a file in the sandbox instead of the terminal.
    """
    await sync_dir(box, src_dir, target_dir)
    stderr_path = f"/tmp/run_sync-{os.urandom(4).hex()}.err"
    script = (
        f"cd {shlex.quote(target_dir)} && stty raw -echo"
        f" && printf {shlex.quote(_READY.decode())}"
        f" && exec {shlex.join(_uv_args(src_dir))} 2>{stderr_path}"
    )
    stack = AsyncExitStack()
    try:
        pty = await stack.enter_async_context(
            box.open_interactive("sh", ["-c", script])
        )
        stdout = BufferedByteReceiveStream(pty.stream)
        await stdout.receive_until(_READY, 65536)
        yield _PtyProcess(pty.stream, stdout, pty, box, stderr_path)
    except BaseException:
        await protected(stack.aclose())
        raise
    await stack.aclose()


def pty_sandbox_worker(
    box: Sandbox,
    main_path: str | None = None,
    *,
    src_dir: StrPath = ".",
    target_dir: str = "workspace",
) -> Worker:
    """Like ``sandbox_worker``, but with ``pty_sandbox_process``."""
    process = pty_sandbox_process(box, src_dir=src_dir, target_dir=target_dir)
    return Worker(process, _remote_main(main_path, src_dir))
