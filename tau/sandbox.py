"""Vercel sandboxes for ``tau --sandbox``."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

from ai.contrib.sandbox import vercel
from ai.contrib.sandbox.util import protected
from vercel.sandbox import Sandbox, create_sandbox, get_or_create_sandbox


@asynccontextmanager
async def open_sandbox(
    name: str | None = None, *, execution_time_limit: timedelta | None = None
) -> AsyncIterator[tuple[Sandbox, bool]]:
    """Yield a sandbox, and whether it was just created.

    With no ``name``, a fresh sandbox that is destroyed on exit.  With
    one, the persistent sandbox of that name, created if need be and
    stopped on exit; stopping snapshots its filesystem, which comes back
    the next time it's opened.  ``execution_time_limit`` applies to a
    reopened sandbox too.
    """
    if name is None:
        async with create_sandbox(
            persistent=False, execution_time_limit=execution_time_limit
        ) as box:
            yield box, True
        return
    box, created = await get_or_create_sandbox(
        name=name, persistent=True, execution_time_limit=execution_time_limit
    )
    try:
        if not created and execution_time_limit is not None:
            await box.update(execution_time_limit=execution_time_limit)
        yield box, created
    finally:
        await protected(box.stop())


async def sync_project(box: Sandbox, src_dir: str, target_dir: str) -> None:
    """Copy a project to a sandbox: the files ``project_paths`` lists."""
    paths = vercel.project_paths(src_dir)
    await vercel.sync_paths(box, src_dir, target_dir, paths)
