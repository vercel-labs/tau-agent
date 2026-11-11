"""Run functions in a Vercel sandbox, using a ``run_sandbox`` worker."""

from __future__ import annotations

import os
import shlex
import stat
import subprocess
import sys
from collections.abc import Iterable
from contextlib import AsyncExitStack
from os import PathLike
from pathlib import Path
from typing import Self

from anyio.streams.buffered import BufferedByteReceiveStream
from vercel.sandbox import Command, InteractiveSession, Sandbox

from tau import run_sandbox
from tau.run_sandbox import Base64ByteReceiveStream, BaseWorker, protected

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


class SandboxWorker(BaseWorker):
    """A worker in a sandbox, running a copy of ``src_dir`` with ``uv run``.

    ``main_path`` is a local path inside ``src_dir``.
    """

    def __init__(
        self,
        box: Sandbox,
        main_path: str | None = None,
        *,
        src_dir: StrPath = ".",
        target_dir: str = "workspace",
        env: dict[str, str] | None = None,
    ) -> None:
        if main_path is None:
            main_path = sys.modules["__main__"].__file__
            assert main_path is not None
        remote_main = os.path.relpath(main_path, src_dir)
        if remote_main.startswith(os.pardir):
            raise ValueError(f"{main_path} is not inside {src_dir}")
        super().__init__(Path(remote_main).as_posix())
        self._box = box
        self._src_dir = src_dir
        self._target_dir = target_dir
        self._env = env

    def _uv_args(self) -> list[str]:
        python = f"{sys.version_info.major}.{sys.version_info.minor}"
        script = Path(
            os.path.relpath(run_sandbox.__file__, self._src_dir)
        ).as_posix()
        return ["uv", "run", "--python", python, script]

    async def __aenter__(self) -> Self:
        await sync_dir(self._box, self._src_dir, self._target_dir)
        uv, *args = self._uv_args()
        self._cmd: Command = await self._box.create_command(
            uv,
            [*args, "--base64"],
            cwd=self._target_dir,
            env=self._env,
            stdin=subprocess.PIPE,
        )
        assert self._cmd.stdin is not None and self._cmd.stdout is not None
        try:
            await self._start(
                self._cmd.stdin, Base64ByteReceiveStream(self._cmd.stdout)
            )
        except BaseException:
            await protected(self._cmd.kill())
            raise
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._stop()
        try:
            assert self._cmd.stdin is not None
            await self._cmd.stdin.aclose()
            await self._cmd.wait()
        except Exception as exc:
            if not _sandbox_stopped(exc):
                raise
        await self._stdout.aclose()
        if self._cmd.stderr is not None:
            await self._cmd.stderr.aclose()

    async def _closed_message(self) -> str:
        returncode = await self._cmd.wait()
        stderr = await self._cmd.stderr.read() if self._cmd.stderr else ""
        return f"worker exited with {returncode}\n{stderr}"


def _sandbox_stopped(exc: BaseException | None) -> bool:
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

    async def __aenter__(self) -> Self:
        await sync_dir(self._box, self._src_dir, self._target_dir)
        self._stderr_path = f"/tmp/run_sync-{os.urandom(4).hex()}.err"
        script = (
            f"cd {shlex.quote(self._target_dir)} && stty raw -echo"
            f" && printf {shlex.quote(self._READY.decode())}"
            f" && exec {shlex.join(self._uv_args())} 2>{self._stderr_path}"
        )
        self._stack = AsyncExitStack()
        try:
            self._pty: InteractiveSession = (
                await self._stack.enter_async_context(
                    self._box.open_interactive("sh", ["-c", script])
                )
            )
            stdout = BufferedByteReceiveStream(self._pty.stream)
            await stdout.receive_until(self._READY, 65536)
            await self._start(self._pty.stream, stdout)
        except BaseException:
            await protected(self._stack.aclose())
            raise
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._stop()
        await self._stack.aclose()

    async def _closed_message(self) -> str:
        returncode = await self._pty.wait()
        stderr = await self._box.fs.read_text(self._stderr_path)
        return f"worker exited with {returncode}\n{stderr}"
