"""`pmx run`: one config → the whole system (ARCHITECTURE *Config*, *Dispatch*, *Lifecycle*).

`System` builds the engine and everything around it from a `RunConfig` — the Kalshi market source (universe
= every strategy selector ∪ `[watch]`), the account source with an API key, the timer, every `[[source]]`,
the models, the strategy slots, risk, the order manager with the paper broker (and the live broker with `--live`)
and scheduler, the ledger, the recorder, the advisor worker, the feed process on the viewer socket, and the control
socket. `run` serves until `stop` (reconciling with the venue first when trading live), then shuts down in
order: halt everything → cancel working orders (waiting for the venue's acks) → save checkpoints → flush the
ledger and the recorder → close sources and workers → remove the sockets. `pmx serve` is the same with no strategies."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import hashlib
import heapq
import inspect
import logging
import os
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from pmx import config, lifecycle, plugins
from pmx.advisors.worker import AdvisorLink
from pmx.analytics.publish import PerfPublisher
from pmx.control.server import ControlServer, control_socket
from pmx.core.units import cents, fmt_qty
from pmx.engine.clock import Clock, LiveClock
from pmx.engine.config import MANUAL, RunConfig
from pmx.engine.engine import Engine, Tap
from pmx.engine.events import LogEvent, OrderUpdate, RiskReject, SlotFill, SourceStatus, Timer
from pmx.engine.slots import Router
from pmx.engine.state import State
from pmx.execution.broker import Brokers
from pmx.execution.live import LiveBroker
from pmx.execution.orders import OrderManager
from pmx.execution.paper import PaperBroker
from pmx.execution.risk import RiskManager
from pmx.feed import proto
from pmx.feed.server import EngineLink, TapPublisher, market_snapshot
from pmx.ledger.db import Attribution, Ledger
from pmx.models.base import ModelEnv
from pmx.models.runtime import ProcessModel, build_model
from pmx.ops.alerts import Alerts
from pmx.ops.awake import on_battery, stay_awake
from pmx.ops.guard import Heartbeat, guard_pidfile, heartbeat_path
from pmx.ops.health import engine_health, health_path
from pmx.ops.health import write as write_health
from pmx.portfolio.scenarios import OutcomeMap
from pmx.portfolio.tracker import PortfolioTracker
from pmx.recorder.sink import RecordingSink
from pmx.sources.base import SourceEnv, build_source, sleep_or_stop
from pmx.sources.kalshi import KalshiAccount, KalshiMarketData
from pmx.sources.timer import TimerSource

log = logging.getLogger(__name__)


class RunLog:
    """The engine's publisher in `pmx run`: everything goes to the tap; the events a run log should show — logs,
    rejects, fills, rejected orders, sources connecting or dropping — also go to Python logging (whose handlers
    `pmx run` puts on a background thread). A source logging more than `per_s` lines in an engine second is cut
    off for that second and told how many were dropped."""

    per_s = 20

    def __init__(self, tap: Tap) -> None:
        self.tap = tap
        self.log = logging.getLogger("pmx.events")
        self.listeners: list[Callable[[Any, int], None]] = []  # e.g. the control server's `observe`
        self._connected: dict[str, bool] = {}
        self._detail: dict[str, str] = {}
        self._second: dict[str, tuple[int, int]] = {}  # source -> (engine second, lines in it)
        self._rejects: dict[tuple[str, str], int] = {}  # (slot, rule) -> last logged ms

    def append(self, ev: Any, now_ms: int) -> None:
        self.tap.append(ev, now_ms)
        for listen in self.listeners:
            listen(ev, now_ms)
        t = type(ev)
        if t is LogEvent:
            if self._allowed(ev.source, now_ms):
                self.log.log(_LEVELS[ev.level], "%s: %s", ev.source, ev.text)
        elif t is SlotFill:
            f = ev.fill
            self.log.info("fill %s %s %s %s @ %s (%s, fee $%.4f)", ev.slot, ev.mode, f.side, fmt_qty(f.qty),
                          cents(f.price), "taker" if f.is_taker else "maker", f.fee / 1e6)
        elif t is RiskReject:
            key = (ev.slot, ev.rule)
            if now_ms - self._rejects.get(key, -60_000) >= 60_000:  # one line per slot and rule a minute
                self._rejects[key] = now_ms
                self.log.info("reject %s %s %s @ %s: %s — %s", ev.slot, ev.side, fmt_qty(ev.qty), cents(ev.price),
                              ev.rule, ev.detail)
        elif t is OrderUpdate:
            if ev.status == "rejected":
                self.log.warning("order %s rejected by the broker: %s", ev.slot, ev.reason)
        elif t is SourceStatus:
            self._detail[ev.source] = ev.detail
            was = self._connected.get(ev.source)
            if was != ev.connected:
                self._connected[ev.source] = ev.connected
                lost = was is True and not ev.connected  # a source still starting isn't a problem yet
                self.log.log(logging.WARNING if lost else logging.INFO, "source %s %s: %s", ev.source,
                             "up" if ev.connected else "down" if lost else "starting", ev.detail)

    def sources(self) -> dict[str, dict[str, Any]]:
        """Each source's last reported health."""
        return {s: {"connected": c, "detail": self._detail.get(s, "")} for s, c in sorted(self._connected.items())}

    def _allowed(self, source: str, now_ms: int) -> bool:
        sec = now_ms // 1000
        was, n = self._second.get(source, (sec, 0))
        if was != sec:
            if n > self.per_s:
                self.log.warning("%s: %d more log lines in that second dropped", source, n - self.per_s)
            n = 0
        self._second[source] = (sec, n + 1)
        return n < self.per_s


_LEVELS = {"info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}


class StartRefused(Exception):
    """The run must not start (live without both switches, without a key, or on prod before `pmx guard`)."""


def code_version(root: Path = config.PROJECT) -> tuple[str, bool]:
    """(git sha, dirty) of the checkout; ("unknown", True) outside one."""
    def git(*args: str) -> str:
        return subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True, timeout=10,
                              check=True).stdout.strip()

    try:
        return git("rev-parse", "--short=12", "HEAD"), bool(git("status", "--porcelain"))
    except (OSError, subprocess.SubprocessError):
        return "unknown", True


def strategy_version(cls: type, sha: str, dirty: bool) -> str:
    """The git sha; with uncommitted changes, `+dirty.` and a hash of the strategy's own source file, so
    editing a strategy changes its version (checkpoints and performance split by it)."""
    if not dirty:
        return sha
    try:
        src = Path(inspect.getsourcefile(cls) or "").read_bytes()
    except (OSError, TypeError):
        src = b""
    return f"{sha}+dirty.{hashlib.blake2b(src, digest_size=3).hexdigest()}"


def instance_id(data: Path) -> str:
    """This data directory's engine instance, in every client order id; made once."""
    path = data / "instance"
    with contextlib.suppress(FileNotFoundError):
        if iid := path.read_text().strip():
            return iid
    iid = os.urandom(3).hex()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(iid + "\n")
    return iid


class Core:
    """The engine and everything it dispatches to — state with every linker, models, strategy slots, risk, the
    order manager with the paper broker (and `live`, if given) and the order scheduler, the portfolio tracker — as
    `pmx run` builds them from a `RunConfig`. The backtest runner builds the same on a `SimClock`: models
    inline with their `latency_ms`, slots' CPU budgets off (wall time would make results machine-dependent), no
    kill-switch file."""

    def __init__(self, cfg: RunConfig, *, env: str, clock: Clock, ledger: Ledger, instance: str, run_id: str,
                 sha: str, dirty: bool, data: Path, publisher: Any = None, recorder: Any = None,
                 backtest: bool = False, halt_file: Path | None = None, live: LiveBroker | None = None) -> None:
        self.cfg, self.env, self.clock, self.ledger, self.run_id = cfg, env, clock, ledger, run_id
        self.attribution = {s.id: Attribution(run_id, strategy_version(plugins.STRATEGIES[s.kind], sha, dirty),
                                              cfg.params_hash[s.id]) for s in cfg.strategies}
        self.attribution[MANUAL] = Attribution(run_id, MANUAL, "")
        self.state = State(plugins.LINKERS)
        self.outcomes = OutcomeMap(self.state)
        self.router = Router(cfg.strategies, linkers=plugins.LINKERS, outcomes=self.outcomes,
                             **({"cpu_clock": lambda: 0} if backtest else {}))
        self.portfolio = PortfolioTracker(slots=self.router.slots, outcomes=self.outcomes, ledger=ledger)
        self.paper, self.live = PaperBroker(), live
        self.orders = OrderManager(intents=self.router.take_intents, slots=self.router.slots,
                                   brokers={"paper": self.paper} | ({"live": live} if live is not None else {}),
                                   ledger=ledger, instance=instance, paper_cash=cfg.paper_cash,
                                   attribution=self.attribution)
        if live is not None:
            live.positions = lambda mid: self.orders.position("", mid, "live")
        self.risk = RiskManager(positions=self.orders, balance=self.orders, slots=self.router.slots,
                                global_limits=cfg.risk, strategy_limits={MANUAL: cfg.manual},
                                portfolio=self.portfolio, halt_file=halt_file)
        self.orders.risk = self.portfolio.risk = self.risk
        self.risk.on_cancel.append(self.orders.cancel_scope)
        self.router.orders, self.router.portfolio = self.orders, self.portfolio
        self.models = [build_model(m, ModelEnv(data), backtest=backtest, linkers=plugins.LINKERS) for m in cfg.models]
        self.engine = Engine(clock, self.state, models=self.models, router=self.router, order_manager=self.orders,
                             broker=self.paper if live is None else Brokers(self.paper, live),
                             portfolio=self.portfolio, publisher=publisher, recorder=recorder)

    def restore(self) -> int:
        """Hand slots their last checkpoint when their strategy's version is unchanged."""
        versions = {sid: a.strategy_version for sid, a in self.attribution.items()}
        saved = self.ledger.checkpoints()
        blobs = {slot: blob for slot, (v, blob) in saved.items() if versions.get(slot.split("/", 1)[0]) == v}
        if saved:
            log.info("checkpoints: %d restored, %d from other strategy versions ignored", len(blobs),
                     len(saved) - len(blobs))
        self.router.restore(blobs)
        return len(blobs)

    def save_checkpoints(self, now_ms: int) -> None:
        blobs = self.router.checkpoint()
        if blobs:
            self.ledger.save_checkpoints({slot: (self.attribution[slot.split("/", 1)[0]].strategy_version, blob)
                                          for slot, blob in blobs.items()}, self.run_id, now_ms)


class System:
    shutdown_s = 2.0  # paper cancels still queued for write capacity after this are canceled in the ledger
    live_shutdown_s = 10.0  # how long shutdown waits for the venue to acknowledge live cancels
    close_s = 10.0  # sources, the feed process, and workers get this long to stop

    def __init__(self, cfg: RunConfig, *, env: str, sock: Path, venue: Any = None, data: Path | None = None,
                 live: bool = False, record: bool | None = None, source_mode: str = "auto", trades_s: float = 2.0,
                 market: Any = None, clock: Clock | None = None, link_argv: list[str] | None = None,
                 guard: bool = True) -> None:
        """`venue` is the Kalshi client (None in tests, which pass their own `market` source); `record`
        overrides `[engine] record`; `source_mode` is the market source's (auto | live | poll); `guard=False`
        (tests, demo only) runs live without `pmx guard`."""
        if cfg.live and not live:
            raise StartRefused(f"live strategies ({', '.join(cfg.live)}) need `pmx run --live` too")
        if live and getattr(venue, "signer", None) is None:
            raise StartRefused(f"live trading needs a Kalshi API key for {env} (pmx setkey --env {env})")
        if live and env == "prod" and not guard:
            raise StartRefused("live trading on prod needs `pmx guard` to cancel resting orders if the engine dies")
        if live and cfg.risk.max_loss is None:
            raise StartRefused("a live run needs a global max_loss in [risk]")
        plugins.load_builtin_plugins()
        self.cfg, self.env, self.sock = cfg, env, sock
        self.data = data or config.data_root()
        self.clock = clock or LiveClock()
        self.started_ms = self.clock.now_ms()
        self.run_id = time.strftime("%Y%m%d-%H%M%S", time.gmtime(self.started_ms / 1000)) + "-" + os.urandom(2).hex()
        self.sha, self.dirty = code_version()
        self.instance = instance_id(self.data)
        self.ledger = Ledger(self.data / "ledger.sqlite", env=env, durable=bool(cfg.live))
        recording = cfg.engine.record if record is None else record
        self.recorder = RecordingSink(self.data) if recording else None
        mode = "live" if source_mode == "live" or (source_mode == "auto" and getattr(venue, "signer", None)) \
            else "poll"
        self.link = EngineLink(sock, proto.Hello(cfg.engine.venue, env, mode),
                               snapshot=lambda: market_snapshot(self.state), clock=self.clock.now_ms,
                               tickers_of=lambda entity: self.state.tickers_of.get(entity, ()), argv=link_argv)
        self.tap = self.link.tap = Tap(self.link.deliver)
        self.live = LiveBroker(venue, instance=self.instance, emit=lambda ev: self.engine.emit(ev, "live"),
                               clock=self.clock.now_ms, ledger=self.ledger, live={*cfg.live, MANUAL}) if live else None
        if self.live is not None:
            log.warning("LIVE on %s: %s%s", env, ", ".join([*cfg.live, "manual orders"]),
                        "" if guard else " — without `pmx guard`: resting orders outlive a crashed engine")
        self.guard = guard and self.live is not None
        self.heartbeat = Heartbeat(heartbeat_path(sock), instance=self.instance, env=env, run_id=self.run_id) \
            if self.live is not None else None
        self.runlog = RunLog(self.tap)
        self.alerts = Alerts(cfg.ops)
        self.runlog.listeners.append(self.alerts.observe)
        self.core = core = Core(cfg, env=env, clock=self.clock, ledger=self.ledger, instance=self.instance,
                                run_id=self.run_id, sha=self.sha, dirty=self.dirty, data=self.data,
                                publisher=self.runlog, recorder=self.recorder, halt_file=self.data / "HALT",
                                live=self.live)
        self.state, self.router, self.orders, self.risk = core.state, core.router, core.orders, core.risk
        self.paper, self.portfolio, self.models, self.engine = core.paper, core.portfolio, core.models, core.engine
        self.attribution = core.attribution

        series, tickers = cfg.universe()
        self.market = market or KalshiMarketData(
            "kalshi", KalshiMarketData.Params(series=series, tickers=tickers, mode=mode, trades_s=trades_s),
            venue=venue)
        self.engine.on_sleep_gap.append(lambda gap_ms: self.market.resync())
        self.engine.on_sleep_gap.append(lambda gap_ms: self.alerts.sleep_gap(gap_ms, self.clock.now_ms()))
        eng = self.engine
        self.sources: list[tuple[Any, Any]] = [
            (self.market, eng.source("kalshi", max=100_000, policy="reset", venue=cfg.engine.venue)),
            (TimerSource("timer"), eng.source("timer", policy="drop_oldest"))]
        if getattr(venue, "signer", None) is not None:
            account = KalshiAccount("account", venue=venue)
            # Fills and order updates must never be dropped: the queue only overflows past a million.
            self.sources.append((account, eng.source("account", max=1_000_000, policy="drop_oldest")))
            if self.live is not None:
                account.on_connect.append(self.live.reconcile_soon)
                venue.rest.on_429.append(lambda: self.orders.scheduler.rate_limited(self.clock.now_ms()))
        senv = SourceEnv(env, {"kalshi": venue} if venue is not None else {})
        for s in cfg.sources:
            self.sources.append((build_source(s.spec, senv), eng.source(s.name, max=s.queue.max,
                                                                         policy=s.queue.policy)))
        self.advisors = (AdvisorLink(cfg.advisors, eng.source(AdvisorLink.SOURCE), sock, clock=self.clock.now_ms)
                         if cfg.advisors else None)
        self.control = ControlServer(control_socket(sock), self)
        self.runlog.listeners.append(self.control.observe)
        self.health_path = health_path(sock)
        self._awake: Any = None
        self.perf = PerfPublisher(self.data / "ledger.sqlite", env, [*(s.id for s in cfg.strategies), MANUAL],
                                  lambda m: self.tap.append(m, self.clock.now_ms()))
        self.runlog.listeners.append(self.perf.observe)
        self.restored = core.restore()
        self.warmed = 0  # recorded events replayed through warming slots at start (4d)

    # ---- running ------------------------------------------------------------------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        """Serve until `stop`, then shut down in order. A component task that ends early stops the run."""
        now = self.clock.now_ms()
        self.ledger.start_run(self.run_id, instance=self.instance, started_ms=self.started_ms, git_sha=self.sha,
                              dirty=self.dirty, config_hash=self.cfg.hash, config_toml=self.cfg.text)
        TapPublisher(self.tap, self.clock.now_ms).set_models([m.name for m in self.models])
        for m in self.models:
            if isinstance(m, ProcessModel):
                m.attach(self.engine.source(m.name))
        for ev in self.orders.start(now):
            self.engine.emit(ev)
        if self.cfg.ops.stay_awake and isinstance(self.clock, LiveClock):
            self._awake = stay_awake()
            if on_battery():
                self.alerts.alert("battery", "pmx is running on battery: the Mac may still sleep (lid, low power)",
                                  now)
        if self.heartbeat is not None:
            self.heartbeat.beat()
            if self.guard and not self._start_guard():
                log.error("live: `pmx guard` didn't start (see data/logs/guard.log); stopping")
                stop.set()
        if self.live is not None and not stop.is_set():
            try:
                await self.live.reconcile("start")
            except Exception as e:
                log.error("live: couldn't reconcile with the venue at start (%s); stopping", e)
                stop.set()
        if self.cfg.engine.warm_from_recording:
            self.warmed = self._warm(now)
        for scope, mode, points in self.portfolio.start(now):  # viewers' equity curves carry on from the ledger
            high, series = None, []
            for ts, eq in points:
                high = eq if high is None else max(high, eq)
                series.append((ts, eq, high - eq))
            self.tap.append(proto.EquityMsg(scope, mode, series, replace=True), now)
        gc.collect()
        gc.freeze()  # start-up objects live for the run: the collector stops rescanning them (tap 0.45 → 0.13 µs)
        self.engine.emit(LogEvent("info", "engine", f"run {self.run_id}: {len(self.cfg.strategies)} strategies, "
                                                    f"{len(self.models)} models, {len(self.sources)} sources", now))
        io = asyncio.Event()  # sources, the feed, workers: stopped after the shutdown sequence
        dispatching = asyncio.Event()
        dispatch = asyncio.create_task(self.engine.run(dispatching), name="engine")
        work: list[Awaitable[Any]] = [self.tap.run(io), self.link.run(io), self._checkpoints(io), self.control.run(io),
                                      self._ops(io), self.perf.run(io),
                                      *([self.live.run(io)] if self.live is not None else []),
                                      *([self.heartbeat.run(io)] if self.heartbeat is not None else []),
                                      *(src.run(sink, io) for src, sink in self.sources)]
        if self.advisors is not None:
            work.append(self.advisors.run(io))
        tasks = [asyncio.ensure_future(w) for w in work]
        for t in (dispatch, *tasks):
            t.add_done_callback(self._ended(stop, io))
        try:
            await stop.wait()
            dispatching.set()
            await dispatch
            await self._shutdown()
        finally:
            io.set()
            _, pending = await asyncio.wait(tasks, timeout=self.close_s)
            for t in pending:
                log.warning("still running after %.0f s, canceled: %s", self.close_s, t.get_name())
                t.cancel()
            self._close()

    def _warm(self, now_ms: int) -> int:
        """Replay recorded data so the slots start warm (4d): the day's records before the last `warmup_s` (the
        longest any strategy asks for) only rebuild state — books from their last snapshot, contracts, entities —
        then that window runs through the whole engine, with every order the slots ask for discarded and nothing
        sent to viewers. Returns how many events the window held."""
        from datetime import UTC, datetime

        from pmx.backtest.raw_source import sources_for

        warm_ms = max((round(1000 * (s.warmup_s if s.warmup_s is not None else plugins.STRATEGIES[s.kind].warmup_s))
                       for s in self.cfg.strategies), default=0)
        if warm_ms <= 0:
            log.info("warm_from_recording: no strategy has a warm-up; nothing to replay")
            return 0
        since = now_ms - warm_ms
        first = datetime.fromtimestamp(since / 1000, UTC).date()
        last = datetime.fromtimestamp(now_ms / 1000, UTC).date()
        def tagged(i: int, src: Any) -> Any:
            for k, (ms, ev) in enumerate(src.events()):
                yield ms, i, k, src.name, ev

        merged = heapq.merge(*(tagged(i, src) for i, src in enumerate(sources_for(self.cfg, self.data, first, last))),
                             key=lambda e: e[:3])
        engine, orders, state = self.engine, self.orders, self.state
        publisher, intents = engine.publisher, orders._intents
        engine.publisher = None
        orders._intents = lambda: (self.router.take_intents(), [])[1]  # nothing reaches risk or a broker
        n = 0
        try:
            started = False
            for ms, _, _, name, ev in merged:
                if ms > now_ms:
                    break
                if ms < since:
                    state.apply(ev, ms, name)
                    continue
                if not started:  # the window opens: slots start on every market known so far
                    started = True
                    for c in list(state.contracts.values()):
                        engine.dispatch(c, since)
                engine.dispatch(ev, ms, name)
                n += 1
        finally:
            engine.publisher, orders._intents = publisher, intents
            self.router.take_intents()
        log.info("warm_from_recording: %d recorded events from the last %.0f s replayed through %d slots",
                 n, warm_ms / 1000, len(self.router.slots))
        return n

    def _ended(self, stop: asyncio.Event, io: asyncio.Event) -> Callable[[asyncio.Task], None]:
        def done(t: asyncio.Task) -> None:
            if t.cancelled() or io.is_set() or stop.is_set():
                return
            e = t.exception()
            log.error("%s ended early%s; stopping", t.get_name(), f": {e!r}" if e else "", exc_info=e)
            stop.set()
        return done

    async def _shutdown(self) -> None:
        """Halt everything, cancel working orders, save checkpoints. Inputs still queued are dispatched first
        (halted slots trade nothing)."""
        now = self.clock.now_ms()
        self._save_checkpoints(now)
        self.risk.halt("pmx stopping", None, now, level="info")
        slots = len(self.router.slots)
        for ev in self.router.stop_all(now):
            self.engine.emit(ev)
        self.engine.emit(Timer(now))  # carries the halt, the cancels, and the slots' last intents through
        await self.engine.dispatch_all()
        deadline = time.monotonic() + (self.live_shutdown_s if self.live is not None else self.shutdown_s)
        while self.orders.orders and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
            self.engine.emit(Timer(self.clock.now_ms()))
            await self.engine.dispatch_all()
        left = self.ledger.cancel_open("paper", "pmx stopped", self.clock.now_ms())
        if left:
            log.info("shutdown: %d paper orders still queued were canceled in the ledger", left)
        if self.live is not None:
            await self.live.drain(2.0)
            await self.engine.dispatch_all()
            if working := self.live.working():
                log.error("shutdown: %d live orders may still be working at the venue: %s%s", len(working),
                          ", ".join(f"{o.slot} {o.req.client_order_id} ({o.status})" for o in working),
                          " — `pmx guard` cancels them once this process exits" if self.guard else "")
            elif self.heartbeat is not None:
                self.heartbeat.remove()  # a clean stop: the guard exits
        log.info("shutdown: halted, %d slots stopped, working orders canceled", slots)

    async def _checkpoints(self, stop: asyncio.Event) -> None:
        while not await sleep_or_stop(stop, self.cfg.engine.checkpoint_s):
            self._save_checkpoints(self.clock.now_ms())

    def _save_checkpoints(self, now_ms: int) -> None:
        self.core.save_checkpoints(now_ms)

    def _start_guard(self) -> bool:
        """`pmx guard` in its own session (it must outlive this process); True once it's running."""
        pidfile = guard_pidfile(self.sock)
        if lifecycle.read_pid(pidfile) is not None:
            return True  # one is still watching this socket's engine (an earlier run that didn't stop cleanly)
        argv = [sys.executable, "-m", "pmx", "guard", "--env", self.env, "--socket", str(self.sock),
                "--instance", self.instance, "--guard-s", f"{self.cfg.ops.guard_s:g}"]
        proc = lifecycle.start_detached(argv, self.data / "logs" / "guard.log")
        return lifecycle.wait_for(lambda: proc.poll() is None and lifecycle.read_pid(pidfile) is not None, 10.0)

    async def _ops(self, stop: asyncio.Event) -> None:
        """The health file and the periodic alerts, every `[ops] health_s`."""
        while not await sleep_or_stop(stop, self.cfg.ops.health_s):
            try:
                write_health(self.health_path, engine_health(self))
                self.alerts.check(self.clock.now_ms(), data=self.data, queued_writes=len(self.orders.scheduler),
                                  dropped=self.link.total_dropped())
            except Exception:
                log.exception("ops: health or alerts failed")

    def _close(self) -> None:
        if self._awake is not None:
            self._awake.terminate()
        for m in self.models:
            if isinstance(m, ProcessModel):
                m.close()
        if self.recorder is not None:
            self.recorder.close()
        self.ledger.end_run(self.run_id, self.clock.now_ms())
        self.ledger.close()
        self.sock.unlink(missing_ok=True)
