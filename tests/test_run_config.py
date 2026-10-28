"""`config/strategies.toml` → `RunConfig`, `pmx config check`, `pmx run`'s wiring and shutdown, `pmx halt`."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import msgspec
import pytest

from pmx import plugins
from pmx.cli import main
from pmx.core.types import BookSnapshot, Contract
from pmx.engine.config import ConfigError, load, plan
from pmx.engine.run import StartRefused, System, strategy_version
from pmx.execution.risk import clear_halt, read_halts, write_halt
from pmx.ledger.db import Ledger
from pmx.strategies.base import MarketContext, Strategy

BASE = """
[engine]
record = false
[watch]
series = ["KXFED"]
tickers = ["KXCPI-26OCT-T0.3"]
[risk]
max_loss = "100.00"
[[source]]
name = "espn"
kind = "espn_nfl"
[[model]]
name = "mid"
kind = "market_mid"
"""


def _cfg(extra: str = "", base: str = BASE):
    return load(text=base + extra)


def _fails(extra: str, match: str, base: str = BASE) -> None:
    with pytest.raises(ConfigError, match=match):
        _cfg(extra, base)


# ---- the config ---------------------------------------------------------------------------------------------


def test_the_default_config_checks_and_prints_its_plan(capsys):
    main(["config", "check"])
    out = capsys.readouterr().out
    assert "config/strategies.toml" in out and out.rstrip().endswith("config is valid")
    for line in ("smoke ", "scripted · paper", "nfl-watch", "watch · paper · model espn", "espn_wp", "market_mid",
                 "series KXNFLGAME", "max_loss $250.00", "max_position 5 · max_loss $5.00", "espn_nfl"):
        assert line in out, line
    cfg = load()
    assert [s.id for s in cfg.strategies] == ["smoke", "nfl-watch"] and not cfg.live
    assert cfg.risk.max_loss == 250_000_000 and cfg.manual.max_position == 2_000, "money and qty in integer units"


def test_a_broken_config_fails_check_with_the_reason(tmp_path, capsys):
    bad = tmp_path / "bad.toml"
    bad.write_text(BASE + '[[model]]\nname = "x"\nkind = "nope"\n')
    with pytest.raises(SystemExit, match=r"invalid config: .*bad.toml: model 'x': unknown kind 'nope' \(known: "):
        main(["config", "check", "--config", str(bad)])


def test_unknown_plugin_kind():
    _fails('[[strategy]]\nid = "s"\nkind = "nope"\nmarkets = { series = ["KXFED"] }\n',
           "strategy 's': unknown kind 'nope'")
    _fails('[[source]]\nname = "w"\nkind = "nope"\n', "source 'w': unknown kind 'nope'")


def test_unknown_param():
    _fails('[[strategy]]\nid = "s"\nkind = "watch"\nmarkets = { series = ["KXFED"] }\nparams = { treshold = 0.1 }\n',
           "strategy 's': strategy 'watch': unknown params key 'treshold'")
    _fails('[[model]]\nname = "m2"\nkind = "market_mid"\nparams = { min_chnage = 1 }\n',
           "unknown params key 'min_chnage'")


def test_missing_model():
    _fails('[[strategy]]\nid = "s"\nkind = "watch"\nmodel = "wp"\nmarkets = { series = ["KXFED"] }\n',
           r"strategy 's': model 'wp' isn't a configured \[\[model\]\] \(configured: mid\)")


def test_live_without_a_global_max_loss():
    live = '[[strategy]]\nid = "s"\nkind = "noop"\nmode = "live"\nmarkets = { series = ["KXFED"] }\n'
    _fails(live, r"live strategies \(s\) need a global max_loss in \[risk\]",
           base=BASE.replace('max_loss = "100.00"', ""))
    assert _cfg(live).live == ["s"]


def test_inputs_advisors_and_names_must_resolve():
    strat = '[[strategy]]\nid = "s"\nkind = "noop"\nmarkets = { series = ["KXFED"] }\n'
    _fails(strat + 'inputs = ["coinbase"]\n', "input 'coinbase' isn't a source")
    _fails(strat + 'advisors = ["news"]\n', r"advisor 'news' isn't a configured \[\[advisor\]\] \(configured: manual\)")
    assert _cfg(strat + 'inputs = ["espn", "timer"]\nadvisors = ["manual"]\n').strategies[0].inputs == ["espn", "timer"]
    _fails(strat + strat, "duplicate strategy id 's'")
    _fails('[[source]]\nname = "timer"\nkind = "timer"\n', "source 'timer': name already used")
    _fails('[[model]]\nname = "mid"\nkind = "market_mid"\n', "model 'mid' is configured twice")
    _fails('[[strategy]]\nid = "s"\nkind = "noop"\n', "`markets` selects nothing")
    _fails('[[strategy]]\nid = "s"\nkind = "noop"\nmode = "real"\nmarkets = { series = ["KXFED"] }\n',
           r"Expected `str` matching .*\$\.mode|mode")


def test_risk_values_are_checked_and_parsed():
    strat = '[[strategy]]\nid = "s"\nkind = "noop"\nmarkets = { series = ["KXFED"] }\n'
    _fails(strat + 'risk = { max_postion = "5" }\n', "strategy 's': risk: unknown limit 'max_postion'")
    _fails(strat + 'risk = { max_loss = 5.5 }\n', 'max_loss must be a string like "25.00"')
    _fails("[engine]\nturbo = true\n", "unknown field `turbo`", base="")
    s = _cfg(strat + 'risk = { max_position = "2.5", stale_book_ms = 5000 }\n').strategies[0]
    assert (s.risk.max_position, s.risk.stale_book_ms) == (250, 5000)


def test_secrets_are_refused():
    _fails('[[source]]\nname = "w"\nkind = "timer"\nparams = { api_key = "abc" }\n', "looks like a secret")
    _fails('[watch]\nseries = ["4f1c2a9e-1b2c-4d5e-8f90-123456789abc"]\n', "looks like a secret", base="")
    _fails('[watch]\nseries = ["-----BEGIN PRIVATE KEY-----"]\n', "looks like a secret", base="")


def test_universe_is_every_selector_plus_watch():
    cfg = _cfg('[[strategy]]\nid = "s"\nkind = "noop"\n'
               'markets = { series = ["KXNFLGAME"], events = ["KXHIGHNY-26OCT18"], tickers = ["KXFED-X"] }\n')
    assert cfg.universe() == (["KXFED", "KXNFLGAME", "KXHIGHNY"], ["KXCPI-26OCT-T0.3", "KXFED-X"])
    assert cfg.watching(["KXBTC"], []).universe()[0] == ["KXBTC", "KXNFLGAME", "KXHIGHNY"]
    assert not cfg.without_strategies().strategies
    assert [m.name for m in cfg.with_models(["market_mid", "espn_wp"]).models] == ["mid", "espn_wp"]


def test_hashes_follow_content_not_formatting():
    strat = '[[strategy]]\nid = "s"\nkind = "watch"\nmarkets = { series = ["KXFED"] }\nparams = { threshold = 0.05 }\n'
    a, b = _cfg(strat), _cfg("# a comment\n" + strat.replace(" = ", "="))
    assert a.hash == b.hash and a.params_hash == b.params_hash
    c = _cfg(strat.replace("0.05", "0.06"))
    assert c.params_hash["s"] != a.params_hash["s"] and c.hash != a.hash
    assert _cfg(strat.replace("params = { threshold = 0.05 }\n", "")).params_hash["s"] == a.params_hash["s"], \
        "the decoded params: an explicit default hashes like the default"


def test_plan_names_live_strategies():
    cfg = _cfg('[[strategy]]\nid = "s"\nkind = "noop"\nmode = "live"\nmarkets = { series = ["KXFED"] }\n')
    assert "LIVE: s" in plan(cfg)


# ---- the kill-switch file ------------------------------------------------------------------------------------


def test_halt_file_holds_one_halt_per_scope(tmp_path):
    f = tmp_path / "HALT"
    assert read_halts(f) == {}
    write_halt(f, "smoke/T-1", "looks wrong")
    write_halt(f, "", "going home")
    write_halt(f, "smoke/T-1", "still wrong")
    assert read_halts(f) == {"smoke/T-1": "still wrong", "": "going home"}
    assert clear_halt(f, "") and read_halts(f) == {"smoke/T-1": "still wrong"}
    assert not clear_halt(f, "nfl-watch")
    assert clear_halt(f, "smoke/T-1") and not f.exists()
    f.write_text("by hand\n")
    assert read_halts(f) == {"": "by hand"}
    f.write_text("")
    assert read_halts(f) == {"": "kill switch"}, "`touch data/HALT` halts everything"


def test_halt_and_resume_commands(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("PMX_DATA", str(tmp_path))
    main(["halt", "smoke", "--reason", "odd fills"])
    main(["halt", "nfl-watch/KXFED-X"])
    assert read_halts(tmp_path / "HALT") == {"smoke": "odd fills", "nfl-watch/KXFED-X": "pmx halt"}
    assert "halted strategy smoke: odd fills" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="is not a slot ID"):
        main(["halt", "--scope", "slot", "smoke"])
    main(["resume", "smoke"])
    assert read_halts(tmp_path / "HALT") == {"nfl-watch/KXFED-X": "pmx halt"}
    main(["resume"])
    assert not (tmp_path / "HALT").exists()


# ---- pmx run -------------------------------------------------------------------------------------------------


@plugins.strategy("test_run_counter")
class Counter(Strategy):
    """Counts timers; checkpoints the count."""

    def __init__(self, params) -> None:
        super().__init__(params)
        self.n = 0
        self.loaded: bytes | None = None

    def on_timer(self, m: MarketContext) -> None:
        self.n += 1

    def on_save(self) -> bytes:
        return str(self.n).encode()

    def on_load(self, blob: bytes) -> None:
        self.loaded = blob


class FakeMarket:
    """Stands in for `kalshi_market`: two open markets with books, then quiet."""

    name = "kalshi"

    def __init__(self) -> None:
        self.resyncs = 0

    def resync(self) -> None:
        self.resyncs += 1

    async def run(self, sink, stop: asyncio.Event) -> None:
        now = int(time.time() * 1000)
        for t in ("KXFED-A", "KXFED-B"):
            sink.emit(Contract("kalshi", t, "KXFED-EV", t, "", "open", None, series_ticker="KXFED"))
            sink.emit(BookSnapshot("kalshi", t, ((4_000, 1_000),), ((4_600, 1_000),), now))
        await stop.wait()


RUN = BASE.replace("[[source]]\nname = \"espn\"\nkind = \"espn_nfl\"\n", "") + """
[[strategy]]
id = "smoke"
kind = "scripted"
markets = { series = ["KXFED"] }
params = { steps = [{ at = "+0s", action = "quote", side = "bid", price = "mid-0.05", qty = "1" }] }
risk = { max_position = "5" }

[[strategy]]
id = "count"
kind = "test_run_counter"
markets = { series = ["KXFED"] }
"""


async def _until(check, timeout_s: float, what: str) -> float:
    t0 = time.monotonic()
    while not check():
        assert time.monotonic() - t0 < timeout_s, what
        await asyncio.sleep(0.05)
    return time.monotonic() - t0


async def _portfolio_from(sock: Path):
    """The first `portfolio` message a viewer of `sock` gets (what `pmx risk` prints)."""
    from pmx.feed import proto

    reader, writer = await asyncio.open_unix_connection(str(sock), limit=1 << 24)
    try:
        while line := await asyncio.wait_for(reader.readline(), 5):
            m = proto.decoder.decode(line)
            if isinstance(m, proto.PortfolioMsg):
                return m
    finally:
        writer.close()
    raise AssertionError("no portfolio message")


def _system(data: Path, text: str = RUN, **kw) -> System:
    return System(load(text=text), env="demo", sock=Path("r.sock"), data=data, market=FakeMarket(), **kw)


def test_run_trades_paper_halts_from_the_file_and_shuts_down_clean(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # AF_UNIX paths are short
    data = tmp_path / "data"

    async def run() -> tuple[System, float, float]:
        s = _system(data)
        stop = asyncio.Event()
        task = asyncio.create_task(s.run(stop))
        await _until(lambda: Path("r.sock").exists() and len(s.orders.orders) == 2, 10, "both slots rest a bid")
        portfolio = await _portfolio_from(Path("r.sock"))
        assert {"portfolio", "strategy:smoke"} <= {x.scope for x in portfolio.scopes}
        assert any(u.limit == "max_loss" and u.cap == 100_000_000 for u in portfolio.limits), "[risk] max_loss"
        write_halt(data / "HALT", "smoke", "test")
        halted = await _until(lambda: not s.orders.orders and "smoke" in s.risk.halts, 5, "halt cancels the bids")
        clear_halt(data / "HALT", "smoke")
        resumed = await _until(lambda: "smoke" not in s.risk.halts, 5, "resume")
        stop.set()
        await task
        return s, halted, resumed

    s, halted, resumed = asyncio.run(run())
    assert halted < 2.0 and resumed < 2.0, (halted, resumed)
    assert not Path("r.sock").exists() and not (Path("r.pid")).exists()
    led = Ledger(data / "ledger.sqlite", env="demo")
    assert led.open_orders("paper") == []
    rows = led.db.execute("SELECT strategy, status, run_id, strategy_version, params_hash FROM orders").fetchall()
    assert {r[0] for r in rows} == {"smoke/KXFED-A", "smoke/KXFED-B"}
    assert {r[2] for r in rows} == {s.run_id} and all(r[3] and r[4] == s.cfg.params_hash["smoke"] for r in rows)
    (run,) = led.runs()
    assert run["run_id"] == s.run_id and run["ended_ms"] and run["config_hash"] == s.cfg.hash
    assert "test_run_counter" in run["config_toml"] and run["instance"] == (data / "instance").read_text().strip()
    saved = led.checkpoints()
    assert set(saved) == {"count/KXFED-A", "count/KXFED-B"} and int(saved["count/KXFED-A"][1]) > 0

    async def again() -> tuple[System, Counter]:  # the next run restores checkpoints of the same strategy version
        s2 = _system(data)
        assert s2.restored == 2
        stop = asyncio.Event()
        task = asyncio.create_task(s2.run(stop))
        await _until(lambda: "count/KXFED-A" in s2.router.slots and s2.router.slots["count/KXFED-A"].instance
                     is not None and len(s2.orders.orders) == 2, 10, "the slots start and rest bids again")
        counter = s2.router.slots["count/KXFED-A"].instance
        await _until(lambda: s2.paper.resting, 5, "paper orders rest")
        stop.set()
        await task
        return s2, counter

    s2, counter = asyncio.run(again())
    assert counter.loaded == saved["count/KXFED-A"][1]
    led = Ledger(data / "ledger.sqlite", env="demo")
    assert len(led.runs()) == 2 and led.open_orders("paper") == []
    ids = [r[0] for r in led.db.execute("SELECT client_order_id FROM orders WHERE run_id=?", (s2.run_id,))]
    assert len(ids) == 2, "the second run's orders made it into the ledger: ids carry on from the first run's"


def test_checkpoints_of_another_strategy_version_are_ignored(tmp_path):
    led = Ledger(tmp_path / "ledger.sqlite", env="demo")
    led.save_checkpoints({"count/KXFED-A": ("old-version", b"7")}, "r0", 1)
    led.close()
    s = _system(tmp_path)
    assert s.restored == 0
    assert strategy_version(Counter, "abc", False) == "abc"
    v = strategy_version(Counter, "abc", True)
    assert v.startswith("abc+dirty.") and v == strategy_version(Counter, "abc", True)
    s.ledger.close()


def test_live_needs_both_switches_a_key_and_on_prod_pmx_guard(tmp_path):
    from types import SimpleNamespace

    live = RUN.replace('id = "smoke"\nkind = "scripted"\n', 'id = "smoke"\nkind = "scripted"\nmode = "live"\n')
    with pytest.raises(StartRefused, match="need `pmx run --live` too"):
        _system(tmp_path, live)
    with pytest.raises(StartRefused, match="live trading needs a Kalshi API key for demo"):
        _system(tmp_path, live, live=True)
    keyed = SimpleNamespace(signer=object())
    with pytest.raises(StartRefused, match="on prod needs `pmx guard`"):
        System(load(text=live), env="prod", sock=Path("r.sock"), data=tmp_path, market=FakeMarket(), venue=keyed,
               live=True, guard=False)


def test_shutdown_cancels_orders_still_waiting_for_write_capacity(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    data = tmp_path / "data"

    async def run() -> System:
        s = _system(data)
        s.orders.scheduler.tokens = 0.0
        s.orders.scheduler.rate = 0.0  # nothing is ever sent: orders stay queued
        s.shutdown_s = 0.2
        stop = asyncio.Event()
        task = asyncio.create_task(s.run(stop))
        await _until(lambda: len(s.orders.orders) == 2, 10, "two queued bids")
        stop.set()
        await task
        return s

    asyncio.run(run())
    assert Ledger(data / "ledger.sqlite", env="demo").open_orders("paper") == []


def test_serve_is_run_without_strategies_or_recording():
    cfg = load(text=RUN).without_strategies()
    assert msgspec.structs.asdict(cfg)["strategies"] == []


@plugins.strategy("test_run_warmer")
class Warmer(Strategy):
    """Wants to quote on every book and print; remembers what it saw."""

    warmup_s = 60

    def __init__(self, params) -> None:
        super().__init__(params)
        self.seen: list[str] = []

    def on_event(self, ev, m: MarketContext) -> None:
        self.seen.append(type(ev).__name__)
        if m.book is not None and m.book.best_bid() is not None:
            m.quote(bid=(m.book.best_bid()[0], 100), ask=None)


def test_warm_from_recording_starts_slots_warm_without_trading_on_the_replay(tmp_path, monkeypatch):
    from tests.conftest import frame, snapshot

    from pmx.recorder.rawlog import RawLogWriter

    monkeypatch.chdir(tmp_path)
    data = tmp_path / "data"
    now = time.time_ns()
    w = RawLogWriter(data, "kalshi")
    w.write_contracts([Contract("kalshi", "KXFED-A", "KXFED-EV", "A", "", "open", None, series_ticker="KXFED")],
                      now - 120 * 10**9)
    w.write_line(now - 100 * 10**9, "f", snapshot("KXFED-A", [["0.3500", "10.00"]], [["0.6000", "10.00"]]))  # before
    w.write_line(now - 30 * 10**9, "f", frame("trade", {  # inside the minute the slot warms over
        "trade_id": "t1", "market_ticker": "KXFED-A", "yes_price_dollars": "0.3500", "no_price_dollars": "0.6500",
        "count_fp": "1.00", "taker_side": "no", "ts": (now - 30 * 10**9) // 10**9,
        "ts_ms": (now - 30 * 10**9) // 10**6}, sid=3, seq=1))
    w.close()
    text = BASE.replace("record = false", "record = false\nwarm_from_recording = true") + """
[[strategy]]
id = "warm"
kind = "test_run_warmer"
markets = { tickers = ["KXFED-A"] }
"""

    async def run() -> tuple[System, Warmer, int]:
        s = _system(data, text)
        stop = asyncio.Event()
        task = asyncio.create_task(s.run(stop))
        await _until(lambda: s.warmed > 0 and "warm/KXFED-A" in s.router.slots, 10, "replayed")
        slot = s.router.slots["warm/KXFED-A"]
        warmer = slot.instance
        assert slot.ready, "a minute of recorded data counted toward its warm-up"
        assert "Trade" in warmer.seen
        early = s.ledger.db.execute("SELECT COUNT(*) FROM orders WHERE created_ms < ?", (s.started_ms,)).fetchone()[0]
        assert early == 0, "nothing it asked for during the replay became an order"
        await _until(lambda: len(s.orders.orders) >= 1, 10, "live data: it quotes at once")
        stop.set()
        await task
        return s, warmer, s.warmed

    s, warmer, n = asyncio.run(run())
    assert n == 1, "the print inside the window; the earlier snapshot only rebuilt the book"
