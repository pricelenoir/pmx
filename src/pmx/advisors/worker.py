"""The advisor worker and the engine's side of it.

`AdvisorLink` (engine process, started by `pmx run` when `[[advisor]]`s are configured) starts the
worker as a child over a socketpair and turns what it sends into engine inputs: each `Advice` is
recorded raw (`e`, source "advisors") first and then queued, so a recorded session replays its
advice exactly; the engine validates it as it dispatches (`engine/events.py:advice_problem`). The
worker can send only `advice` and `log` lines — anything else is dropped and logged. It is restarted
with back-off if it dies; the engine never waits on it.

The worker (`python -m pmx.advisors.worker`) runs at lower OS priority, keeps a `FeedView` from the
viewer feed, and runs every advisor in an `AdvisorRunner`: each its own asyncio task, an advisor that
raises restarted with back-off while the others carry on. It exits when the engine goes away."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import socket
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

import msgspec

from pmx.advisors.base import AdvisorEnv, AdvisorSpec, Paused, build_advisor
from pmx.advisors.view import FeedView, follow
from pmx.engine.events import Advice, LogEvent

log = logging.getLogger(__name__)


class AdviceLine(msgspec.Struct, tag="advice", tag_field="t"):
    advice: Advice


class LogLine(msgspec.Struct, tag="log", tag_field="t"):
    advisor: str
    level: str  # info | warn | error
    text: str


type WorkerLine = AdviceLine | LogLine
_encode = msgspec.json.Encoder().encode
_decode = msgspec.json.Decoder(WorkerLine).decode


class _Sink:
    """One advisor's `AdviceSink`: what it says goes out under its own name only."""

    def __init__(self, name: str, out: Callable[[WorkerLine], None]) -> None:
        self.name, self.out, self.sent = name, out, 0

    def advise(self, a: Advice) -> None:
        if a.advisor != self.name:
            a = msgspec.structs.replace(a, advisor=self.name)
        self.sent += 1
        self.out(AdviceLine(a))


class AdvisorRunner:
    """Every configured advisor as its own task. One that raises is logged and restarted after
    `backoff` seconds (doubling to `max_backoff`; back to the start after a minute of running); one
    whose `run` returns is done. Others are never affected."""

    def __init__(self, specs: Sequence[AdvisorSpec], view: Any, out: Callable[[WorkerLine], None], *,
                 clock: Callable[[], int] = lambda: time.time_ns() // 1_000_000, backoff: float = 1.0,
                 max_backoff: float = 60.0) -> None:
        self.view, self.out, self.backoff, self.max_backoff = view, out, backoff, max_backoff
        self.stop = asyncio.Event()
        self.restarts: dict[str, int] = {}
        self.advisors: dict[str, tuple[Any, AdvisorEnv, _Sink]] = {}
        for spec in specs:
            if spec.name in self.advisors:
                raise ValueError(f"duplicate advisor name {spec.name!r}")
            note = self._noter(spec.name)
            adv, env = build_advisor(spec, lambda name, params, note=note: AdvisorEnv(
                name, params, clock=clock, note=note, stop=self.stop))
            self.advisors[spec.name] = (adv, env, _Sink(spec.name, out))
            self.restarts[spec.name] = 0

    def _noter(self, name: str) -> Callable[[str, str], None]:
        def note(level: str, text: str) -> None:
            getattr(log, "warning" if level == "warn" else level, log.info)("advisor %s: %s", name, text)
            self.out(LogLine(name, level, text))
        return note

    async def run(self, stop: asyncio.Event) -> None:
        relay = asyncio.ensure_future(self._relay(stop))
        try:
            await asyncio.gather(*(self._keep(name) for name in self.advisors))
        finally:
            relay.cancel()

    async def _relay(self, stop: asyncio.Event) -> None:
        await stop.wait()
        self.stop.set()

    async def _keep(self, name: str) -> None:
        adv, env, sink = self.advisors[name]
        delay = self.backoff
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                await adv.run(self.view, sink, self.stop)
                if not self.stop.is_set():
                    env.note("info", "finished")
                return
            except Paused:
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.restarts[name] += 1
                if time.monotonic() - started > 60:
                    delay = self.backoff
                env.note("error", f"raised {type(e).__name__}: {e}; restarting in {delay:g}s")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), delay)
                delay = min(delay * 2, self.max_backoff)


# ---- the worker process -------------------------------------------------------------------------------


async def serve_worker(conn: socket.socket, feed: Path, specs: Sequence[AdvisorSpec]) -> None:
    from pmx.plugins import load_builtin_plugins

    with contextlib.suppress(OSError):
        os.nice(10)  # a local model must never starve the engine
    load_builtin_plugins(("pmx.advisors",), domains=False)  # what advisors need, not every model's stack
    reader, writer = await asyncio.open_unix_connection(sock=conn)
    stop = asyncio.Event()

    def out(line: WorkerLine) -> None:
        if not writer.is_closing():
            writer.write(_encode(line) + b"\n")

    view = FeedView()
    runner = AdvisorRunner(specs, view, out)

    async def engine_gone() -> None:
        await reader.read()  # the engine never writes; EOF = it's gone
        stop.set()

    async def advise() -> None:  # advisors start on a whole view, not a half-received one
        synced = asyncio.ensure_future(view.synced.wait())
        await asyncio.wait({synced, asyncio.ensure_future(stop.wait())}, return_when=asyncio.FIRST_COMPLETED)
        if synced.done():
            await runner.run(stop)

    await asyncio.gather(engine_gone(), follow(view, feed, stop), advise())
    writer.close()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="pmx.advisors.worker", description="the advisor worker (started by the engine)")
    p.add_argument("--fd", type=int, required=True, help="the engine's end of the link (an inherited socket)")
    p.add_argument("--feed", required=True, help="the viewer feed socket the view is built from")
    p.add_argument("--specs", required=True, help="JSON list of [[advisor]] tables")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s[advisors]: %(message)s")
    specs = msgspec.json.decode(args.specs, type=list[AdvisorSpec])
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve_worker(socket.socket(fileno=args.fd), Path(args.feed), specs))


# ---- the engine's side ----------------------------------------------------------------------------------


class AdvisorLink:
    """Starts and supervises the worker; `sink` is the engine source advice arrives through
    (`engine.source("advisors")`): `record` raw first, then `emit`."""

    SOURCE = "advisors"

    def __init__(self, specs: Iterable[AdvisorSpec], sink: Any, feed: Path, *, clock: Callable[[], int],
                 argv: list[str] | None = None) -> None:
        self.specs, self.sink, self.feed, self.clock = list(specs), sink, feed, clock
        self.argv = argv or [sys.executable, "-m", "pmx.advisors.worker"]
        self.received = 0
        self.refused = 0  # lines that weren't advice or a log
        self.restarts = 0
        self.proc: asyncio.subprocess.Process | None = None

    def handle(self, raw: bytes) -> None:
        """One line from the worker."""
        from pmx.recorder.rawlog import encode_event

        try:
            line = _decode(raw)
        except msgspec.DecodeError as e:
            self.refused += 1
            self.sink.emit(LogEvent("warn", self.SOURCE, f"dropped a line from the advisor worker: {e}"[:300],
                                    self.clock()))
            return
        match line:
            case AdviceLine(advice=a):
                self.received += 1
                self.sink.record(self.SOURCE, "e", encode_event(a))  # raw first: replays reproduce it
                self.sink.emit(a)
            case LogLine(advisor=name, level=level, text=text):
                lvl = level if level in ("info", "warn", "error") else "info"
                self.sink.emit(LogEvent(lvl, f"advisor:{name}", text[:500], self.clock()))

    async def run(self, stop: asyncio.Event) -> None:
        from pmx.sources.base import sleep_or_stop

        if not self.specs:
            return
        backoff = 0.5
        specs = msgspec.json.encode(self.specs).decode()
        while not stop.is_set():
            started = time.monotonic()
            parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                proc = self.proc = await asyncio.create_subprocess_exec(
                    *self.argv, "--fd", str(child.fileno()), "--feed", str(self.feed), "--specs", specs,
                    pass_fds=(child.fileno(),), start_new_session=True)
            finally:
                child.close()
            reader, writer = await asyncio.open_unix_connection(sock=parent, limit=1 << 20)
            stopping = asyncio.ensure_future(stop.wait())
            try:
                while True:
                    line = asyncio.ensure_future(reader.readline())
                    await asyncio.wait({line, stopping}, return_when=asyncio.FIRST_COMPLETED)
                    if stopping.done():
                        line.cancel()
                        break
                    try:
                        raw = line.result()
                    except (ValueError, ConnectionError):  # an over-long line, or the worker died
                        raw = b""
                    if not raw:
                        break
                    self.handle(raw)
            finally:
                stopping.cancel()
                writer.close()  # the worker sees EOF and exits
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(proc.wait(), 5)
                if proc.returncode is None:
                    proc.kill()
                    await proc.wait()
            if stop.is_set():
                return
            self.restarts += 1
            if time.monotonic() - started > 60:
                backoff = 0.5
            self.sink.emit(LogEvent("error", self.SOURCE, f"advisor worker exited ({proc.returncode}); "
                                                          f"restarting in {backoff:g}s", self.clock()))
            await sleep_or_stop(stop, backoff)
            backoff = min(backoff * 2, 30.0)


if __name__ == "__main__":
    main()
