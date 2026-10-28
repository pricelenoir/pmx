"""The engine's control socket: `data/pmx-ctl.sock`, mode 0600, beside the read-only feed socket.

Orders, cancels, halts, and resumes become `Command` inputs on the engine queue — the order manager handles
them in turn, through risk, the scheduler, the broker, and the ledger, like a strategy's intents — and each
request's answer waits for its `CommandResult` plus the order updates, fills, and rejects that came of it (a
live order: until the venue has answered, at most `wait_s`). Manual advice goes in as `Advice(advisor="manual")`
(recorded first, so replays have it); `status` and `positions` read the engine's state between dispatches.
`observe` is the engine side: the run's publisher hands it every event, and it only looks at a few types."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any

import msgspec

from pmx.control import protocol
from pmx.engine.events import Advice, Command, CommandResult, OrderUpdate, RiskReject, SlotFill
from pmx.recorder.rawlog import encode_event

log = logging.getLogger(__name__)

_WAITING = frozenset({"queued", "pending"})


def control_socket(feed_sock: Path) -> Path:
    """The control socket that goes with a feed socket: `data/pmx.sock` → `data/pmx-ctl.sock`."""
    return feed_sock.with_name(feed_sock.stem + "-ctl.sock")


class _Pending:
    __slots__ = ("result", "done", "coids", "orders", "fills", "rejects")

    def __init__(self) -> None:
        self.result: CommandResult | None = None
        self.done = asyncio.Event()
        self.coids: set[str] = set()
        self.orders: list[OrderUpdate] = []
        self.fills: list[SlotFill] = []
        self.rejects: list[RiskReject] = []


class ControlServer:
    wait_s = 3.0  # a live order's answer waits this long for the venue's ack

    def __init__(self, path: Path, system: Any) -> None:
        self.path, self.system = path, system
        self.requests = 0
        self._refs = itertools.count(1)
        self._pending: dict[int, _Pending] = {}
        self._by_coid: dict[str, _Pending] = {}
        self._current: _Pending | None = None  # the command being dispatched (its rejects come before its result)

    # ---- engine side (inside dispatch: cheap) --------------------------------------------------------------------

    def observe(self, ev: Any, now_ms: int) -> None:
        t = type(ev)
        if t is Command:
            self._current = self._pending.get(ev.ref)
        elif t is CommandResult:
            p = self._pending.get(ev.ref)
            if p is not None:
                p.result = ev
                p.coids.update(ev.client_order_ids)
                for c in ev.client_order_ids:
                    self._by_coid[c] = p
                p.done.set()
            self._current = None
        elif t is RiskReject:
            if self._current is not None:
                self._current.rejects.append(ev)
        elif t is OrderUpdate:
            if (p := self._by_coid.get(ev.client_order_id)) is not None:
                p.orders.append(ev)
        elif t is SlotFill:
            if (p := self._by_coid.get(ev.fill.client_order_id)) is not None:
                p.fills.append(ev)

    # ---- the socket ---------------------------------------------------------------------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        self.path.unlink(missing_ok=True)
        server = await asyncio.start_unix_server(self._client, path=str(self.path))
        os.chmod(self.path, 0o600)
        try:
            await stop.wait()
        finally:
            server.close()
            with contextlib.suppress(Exception):
                await server.wait_closed()
            self.path.unlink(missing_ok=True)

    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while line := await reader.readline():
                try:
                    req = protocol.requests.decode(line)
                except msgspec.DecodeError as e:
                    resp = protocol.Response(False, f"bad request: {e}")
                else:
                    self.requests += 1
                    try:
                        resp = await self.handle(req)
                    except Exception as e:  # a bug here mustn't take the engine down
                        log.exception("control request failed: %r", req)
                        resp = protocol.Response(False, f"{type(e).__name__}: {e}")
                writer.write(protocol.encode(resp) + b"\n")
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async def handle(self, req: protocol.Request) -> protocol.Response:
        t = type(req)
        if t is protocol.Status:
            return protocol.Response(True, data=self.status())
        if t is protocol.Positions:
            return protocol.Response(True, data={"positions": self.positions(req.mode)})
        if t is protocol.Advise:
            return self._advise(req)
        return await self._command(req)

    async def _command(self, req: msgspec.Struct) -> protocol.Response:
        s = self.system
        ref = next(self._refs)
        p = self._pending[ref] = _Pending()
        try:
            s.engine.emit(Command(ref, req, s.clock.now_ms()), "control")
            await asyncio.wait_for(p.done.wait(), 10.0)
            if type(req) is protocol.Order and getattr(req, "mode", "paper") == "live":
                deadline = time.monotonic() + self.wait_s
                while time.monotonic() < deadline and not self._answered(p):
                    await asyncio.sleep(0.05)
            else:
                await asyncio.sleep(0)  # the rest of the command's dispatch chain
        except TimeoutError:
            return protocol.Response(False, "the engine didn't answer within 10 s")
        finally:
            del self._pending[ref]
            for c in p.coids:
                self._by_coid.pop(c, None)
        assert p.result is not None
        return protocol.Response(p.result.ok, p.result.error,
                                 orders=[msgspec.structs.asdict(u) for u in p.orders],
                                 fills=[{"slot": f.slot, "mode": f.mode, **msgspec.structs.asdict(f.fill)}
                                        for f in p.fills],
                                 rejects=[msgspec.structs.asdict(r) for r in p.rejects])

    @staticmethod
    def _answered(p: _Pending) -> bool:
        latest = {u.client_order_id: u.status for u in p.orders}
        return all(latest.get(c, "pending") not in _WAITING for c in p.coids)

    def _advise(self, req: protocol.Advise) -> protocol.Response:
        if (req.p is None) == (req.direction is None):
            return protocol.Response(False, "give a probability (--p) or a direction, not both")
        s = self.system
        now = s.clock.now_ms()
        a = Advice("manual", req.target, req.p, req.direction, req.confidence, now + round(req.expires_s * 1000),
                   req.note, now)
        if s.recorder is not None:
            s.recorder.record("advisors", "e", encode_event(a))  # raw first: replays reproduce it
        s.engine.emit(a, "advisors")
        return protocol.Response(True, data={"advice": msgspec.structs.asdict(a)})

    # ---- reads ------------------------------------------------------------------------------------------------------

    def status(self) -> dict[str, Any]:
        s = self.system
        orders = s.orders
        slots = Counter(x.status.state if x.status is not None else "starting" for x in s.router.slots.values())
        working = Counter(o.mode for o in orders.orders.values())
        manual = [{"client_order_id": o.req.client_order_id, "slot": o.slot, "mode": o.mode, "side": o.req.side,
                   "price": o.req.price, "qty": o.req.qty, "filled": o.filled, "status": o.status}
                  for o in orders.orders.values() if o.slot.startswith("manual/")]
        today = {}
        for mode in ("paper", "live"):
            st = s.portfolio.stats("portfolio", mode)
            today[mode] = {"equity": st.equity, "today": st.today, "drawdown": st.drawdown}
        return {"env": s.env, "run_id": s.run_id, "live": s.live is not None, "started_ms": s.started_ms,
                "sources": {n: {"connected": None, "detail": "no status yet"} for n in s.engine.sources}
                | s.runlog.sources(),
                "slots": dict(slots), "working_orders": dict(working), "manual_orders": manual,
                "halts": dict(s.risk.halts), "pnl": today, "dispatched": s.engine.dispatched,
                "queue_depth": s.engine.queue_depth}

    def positions(self, mode: str | None) -> list[dict[str, Any]]:
        out = []
        for (m, slot, mid), p in sorted(self.system.orders._pos.items()):
            if p.qty and (mode is None or m == mode):
                out.append({"slot": slot, "mode": m, "ticker": mid, "qty": p.qty, "cost": p.cost,
                            "realized": p.realized, "fees": p.fees})
        return out
