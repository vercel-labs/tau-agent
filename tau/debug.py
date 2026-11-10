"""Debug spew that stays out of the TUI.

    from tau.debug import dprint
    dprint("got here", x)

then ``tail -f /tmp/tau-debug.log`` (or ``$TAU_DEBUG_LOG``).
"""

from __future__ import annotations

import os
import time
from typing import Any

PATH = os.environ.get("TAU_DEBUG_LOG", "/tmp/tau-debug.log")


def dprint(*args: Any, sep: str = " ", end: str = "\n") -> None:
    with open(PATH, "a") as f:
        prefix = f"{time.strftime('%H:%M:%S')} [{os.getpid()}]"
        print(prefix, *args, sep=sep, end=end, file=f)
