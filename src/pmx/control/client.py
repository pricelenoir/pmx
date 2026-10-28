"""Talking to a running engine's control socket."""

from __future__ import annotations

import asyncio
from pathlib import Path

from pmx.control import protocol


class NotRunning(ConnectionError):
    """Nothing answers on the control socket."""


async def request(sock: Path, req: protocol.Request, *, timeout: float = 15.0) -> protocol.Response:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(str(sock)), min(timeout, 2.0))
    except (OSError, TimeoutError) as e:
        raise NotRunning(f"no engine on {sock} ({e.__class__.__name__}); start one with `pmx up`") \
            from None
    try:
        writer.write(protocol.encode(req) + b"\n")
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout)
        if not line:
            raise NotRunning(f"the engine closed {sock} without answering")
        return protocol.responses.decode(line)
    finally:
        writer.close()


def call(sock: Path, req: protocol.Request, *, timeout: float = 15.0) -> protocol.Response:
    return asyncio.run(request(sock, req, timeout=timeout))
