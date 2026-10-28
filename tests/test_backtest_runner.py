"""Recorded raw logs (Kalshi frames + ESPN payloads + advice) through the live configuration, deterministically."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import msgspec
import pytest
from tests.conftest import frame, snapshot

from pmx import plugins
from pmx.backtest.raw_source import RawLogSource
from pmx.backtest.runner import Runner, run, walk_forward
from pmx.core.types import Contract
from pmx.engine.config import load
from pmx.engine.events import Advice
from pmx.recorder.rawlog import RawLogWriter, encode_event, envelope
from pmx.strategies.base import MarketContext, Strategy

FIX = Path(__file__).parent / "fixtures" / "espn"
DAY = date(2026, 10, 18)
T0 = 1_792_330_200_000_000_000  # 2026-10-18 13:30:00Z, ns
S = 1_000_000_000
HOU, JAC = "KXNFLGAME-26OCT18HOUJAC-HOU", "KXNFLGAME-26OCT18HOUJAC-JAC"

SEEN: list[tuple[str, int, str]] = []  # (slot, engine ms, what) from the probe


@plugins.strategy("test_bt_probe")
class Probe(Strategy):
    advisors = ()

    def on_event(self, ev, m: MarketContext) -> None:
        SEEN.append((m.slot, m.now_ms, type(ev).__name__))
        super().on_event(ev, m)


CONFIG = """
[engine]
record = false
[[source]]
name = "espn"
kind = "espn_nfl"
[[model]]
name = "espn"
kind = "espn_wp"
[[advisor]]
name = "news"
kind = "scripted_advice"
params = { steps = [{ at = "+0s", series = "KXNFLGAME", p = 0.5 }] }
[[strategy]]
id = "smoke"
kind = "scripted"
markets = { series = ["KXNFLGAME"], linker = "nfl_game" }
params = { steps = [{ at = "+0s", action = "quote", side = "bid", price = "bid", qty = "1" },
                    { at = "+5s", action = "take", side = "bid", price = "ask", qty = "1" }] }
[[strategy]]
id = "probe"
kind = "test_bt_probe"
model = "espn"
advisors = ["news"]
markets = { tickers = ["KXNFLGAME-26OCT18HOUJAC-HOU"] }
"""


def _trade(ticker: str, price: str, count: str, taker: str, ns: int, n: int) -> str:
    no = f"{1 - float(price):.4f}"
    return frame("trade", {"trade_id": f"t{n}", "market_ticker": ticker, "yes_price_dollars": price,
                           "no_price_dollars": no, "count_fp": count, "taker_side": taker,
                           "ts": ns // S, "ts_ms": ns // 1_000_000}, sid=3, seq=n)


def write_day(root: Path, *, shuffle: bool = False, listed_ns: int | None = None) -> None:
    """A recorded morning: two NFL team markets over the websocket, ESPN's scoreboard, one advice. With `listed_ns`,
    the contracts' metadata was recorded then (a whole-universe listing), not on the day."""
    k = RawLogWriter(root, "kalshi")
    contracts = [Contract("kalshi", t, "KXNFLGAME-26OCT18HOUJAC", t, "", "open", None, series_ticker="KXNFLGAME")
                 for t in (HOU, JAC)]
    if listed_ns is None:
        k.write_contracts(contracts, T0)
    else:
        k.write_line(listed_ns, "c", '{"event":"universe","contracts":2}')
        k.write_contracts(contracts, listed_ns)
    k.write_line(T0 + 1 * S, "f", snapshot(HOU, [["0.4000", "10.00"]], [["0.5600", "10.00"]], sid=1, seq=1))
    k.write_line(T0 + 1 * S, "f", snapshot(JAC, [["0.5500", "10.00"]], [["0.4100", "10.00"]], sid=2, seq=1))
    for n, (at, price, count) in enumerate([(3, "0.4100", "1.00"), (8, "0.4000", "20.00"), (12, "0.4400", "2.00")],
                                           start=1):
        k.write_line(T0 + at * S, "f", _trade(HOU, price, count, "no" if price == "0.4000" else "yes", T0 + at * S, n))
    k.close()
    e = RawLogWriter(root, "espn")
    body = (FIX / "scoreboard_live.json").read_text()
    e.write_line(T0 + 2 * S, "f", envelope("https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
                                           200, (T0 + 2 * S) // 1_000_000, body))
    e.close()
    a = RawLogWriter(root, "advisors")
    a.write_line(T0 + 4 * S, "e", encode_event(Advice("news", HOU, 0.6, None, 0.9, (T0 + 600 * S) // 1_000_000,
                                                      "recorded", (T0 + 4 * S) // 1_000_000)))
    if shuffle:  # recorded out of order (a wall-clock step): dispatched at the engine's time, never before it
        a.write_line(T0 + 3 * S, "e", encode_event(Advice("news", HOU, 0.7, None, 0.9, (T0 + 600 * S) // 1_000_000,
                                                          "late", (T0 + 3 * S) // 1_000_000)))
    a.close()


def test_a_recorded_day_runs_the_live_configuration(tmp_path):
    write_day(tmp_path)
    SEEN.clear()
    cfg = load(text=CONFIG)
    s = run(cfg, start=DAY, end=DAY, root=tmp_path, run_dir=tmp_path / "bt")
    assert s.sources["kalshi"] == 7 and s.sources["advisors"] == 1, "2 contracts + 2 books + 3 prints; 1 advice"
    assert s.sources["espn"] == 2, "the scoreboard's two games"
    assert s.inputs > 0 and s.first_ms == T0 // 1_000_000 and s.clamped == 0
    assert s.timers == 12, "a Timer each whole second from 13:30:01 to 13:30:12"
    # smoke: a joined bid on each team, a take of 1 at the ask after 5 s on each; HOU's bid fills when 20 print
    # at 0.40 (10 ahead of it in the queue).
    assert (s.strategies["smoke"].orders, s.strategies["smoke"].fills) == (4, 3)
    assert s.orders == 4 and s.fills == 3
    kinds = {what for slot, _, what in SEEN if slot == "probe/" + HOU}
    assert {"BookSnapshot", "Trade", "Advice", "Forecast"} <= kinds, "ESPN → espn_wp forecasts; recorded advice"
    bt = tmp_path / "bt"
    assert (bt / "config.toml").read_text() == CONFIG
    assert json.loads((bt / "summary.json").read_text())["fills"] == 3


def test_contracts_listed_before_the_start_day_are_known_from_its_first_record(tmp_path):
    day = 86_400 * S
    write_day(tmp_path, listed_ns=T0 - day)
    k = RawLogWriter(tmp_path, "kalshi")
    old = Contract("kalshi", "KXNFLGAME-26OCT11AB-A", "KXNFLGAME-26OCT11AB", "", "", "open", None,
                   series_ticker="KXNFLGAME")
    k.write_contracts([old], T0 - 2 * day)  # before a whole-universe listing: not read
    k.write_contracts([msgspec.structs.replace(old, ticker="KXNFLGAME-26OCT17CD-C", status="settled")], T0 - day + S)
    k.close()
    src = RawLogSource("kalshi", tmp_path, DAY, DAY, prime=True)
    assert [c.ticker for c in src.primed()] == [HOU, JAC], "open ones, newest first, back to the listing"
    SEEN.clear()
    s = run(load(text=CONFIG), start=DAY, end=DAY, root=tmp_path, run_dir=tmp_path / "bt")
    assert s.sources["kalshi"] == 7 and s.first_ms == (T0 + S) // 1_000_000
    assert (s.strategies["smoke"].orders, s.strategies["smoke"].fills) == (4, 3)


def test_a_session_started_mid_day_finds_books_already_built(tmp_path):
    write_day(tmp_path)
    SEEN.clear()
    start, end = (T0 + 9 * S) // 1_000_000, (T0 + 20 * S) // 1_000_000
    s = run(load(text=CONFIG), start=DAY, end=DAY, root=tmp_path, run_dir=tmp_path / "bt", start_ms=start,
            end_ms=end)
    assert s.first_ms == start and s.last_ms == end and s.start.endswith("13:30:09.000Z")
    assert min(ms for _, ms, _ in SEEN) == start, "nothing reached a slot before the start"
    # smoke joins the 0.40 / 0.55 bids from books snapshotted at 13:30:01; the 0.40 print at :08 came before it,
    # so nothing fills the quotes; the takes at +5 s (:14, after the last record) still happen: Timers run to the end.
    assert (s.strategies["smoke"].orders, s.strategies["smoke"].fills) == (4, 2)
    assert s.timers == 11, "13:30:10 .. 13:30:20"


def _ledger_rows(path: Path) -> list:
    import sqlite3

    db = sqlite3.connect(path)
    try:
        return list(db.iterdump())
    finally:
        db.close()


def test_running_twice_gives_identical_ledgers_and_summaries(tmp_path):
    write_day(tmp_path)
    cfg = load(text=CONFIG)
    a = run(cfg, start=DAY, end=DAY, root=tmp_path, run_dir=tmp_path / "a")
    b = run(cfg, start=DAY, end=DAY, root=tmp_path, run_dir=tmp_path / "b")
    wall = {"wall_s", "inputs_per_s"}

    def same(s) -> dict:
        return {k: v for k, v in msgspec.to_builtins(s).items() if k not in wall}

    assert same(a) == same(b)
    assert (tmp_path / "a" / "ledger.sqlite").read_bytes() == (tmp_path / "b" / "ledger.sqlite").read_bytes()
    # Another process with another hash seed: no set iteration order leaks into the result.
    code = ("import sys; from pathlib import Path; from datetime import date; sys.path.insert(0, '.');"
            "import tests.test_backtest_runner as t; from pmx.engine.config import load;"
            "from pmx.backtest.runner import run;"
            f"run(load(text=t.CONFIG), start=date(2026,10,18), end=date(2026,10,18), root=Path({str(tmp_path)!r}),"
            f" run_dir=Path({str(tmp_path / 'c')!r}))")
    env = {**os.environ, "PYTHONHASHSEED": "12345"}
    subprocess.run([sys.executable, "-c", code], check=True, env=env, cwd=Path(__file__).parents[1],
                   capture_output=True, timeout=120)
    assert _ledger_rows(tmp_path / "a" / "ledger.sqlite") == _ledger_rows(tmp_path / "c" / "ledger.sqlite")
    assert (tmp_path / "a" / "ledger.sqlite").read_bytes() == (tmp_path / "c" / "ledger.sqlite").read_bytes()


def test_engine_time_never_goes_backwards(tmp_path):
    write_day(tmp_path, shuffle=True)
    SEEN.clear()
    s = run(load(text=CONFIG), start=DAY, end=DAY, root=tmp_path, run_dir=tmp_path / "bt")
    assert s.clamped == 1
    times = [ms for _, ms, _ in SEEN]
    assert times == sorted(times)


def test_step_and_reset_drive_the_same_engine(tmp_path):
    write_day(tmp_path)
    r = Runner(load(text=CONFIG), start=DAY, end=DAY, root=tmp_path, run_dir=tmp_path / "bt")
    assert r.step(3) == 3 and r.inputs == 3
    while r.step(1):
        pass
    first = r.finish()
    r.reset()
    assert r.inputs == 0
    again = r.run()
    assert (again.inputs, again.fills, again.timers) == (first.inputs, first.fills, first.timers)
    with pytest.raises(RuntimeError, match="already finished"):
        r.finish()


def test_live_strategies_run_as_paper_and_sources_can_be_limited(tmp_path):
    write_day(tmp_path)
    smoke = 'id = "smoke"\nkind = "scripted"\n'
    live = load(text=CONFIG.replace(smoke, smoke + 'mode = "live"\n') + '[risk]\nmax_loss = "10.00"\n')
    assert live.live == ["smoke"]
    s = run(live, start=DAY, end=DAY, root=tmp_path, run_dir=tmp_path / "bt", sources=["kalshi"])
    assert set(s.sources) == {"kalshi"} and s.fills == 3, "paper fills; no ESPN, no advice"


def test_walk_forward_reports_both_windows_and_refuses_overlap(tmp_path):
    write_day(tmp_path)
    cfg = load(text=CONFIG)
    with pytest.raises(ValueError, match="must start after the training window"):
        walk_forward({"a": cfg}, train=(DAY, DAY), evaluate=(DAY, DAY), out_dir=tmp_path / "sw", root=tmp_path)
    rows = walk_forward({"a": cfg}, train=(DAY, DAY), evaluate=(date(2026, 10, 19), date(2026, 10, 19)),
                        out_dir=tmp_path / "sw", root=tmp_path)
    assert rows[0]["train"].fills == 3 and rows[0]["evaluate"].inputs == 0


def test_pmx_backtest_writes_its_run_directory(tmp_path, monkeypatch, capsys):
    from pmx.cli import main

    write_day(tmp_path)
    (tmp_path / "c.toml").write_text(CONFIG)
    monkeypatch.setenv("PMX_DATA", str(tmp_path))
    main(["backtest", "--config", str(tmp_path / "c.toml"), "--from", "2026-10-18", "--run-id", "t1"])
    out = capsys.readouterr().out
    assert "3 fills" in out and "smoke" in out
    assert (tmp_path / "backtests" / "t1" / "ledger.sqlite").exists()
    with pytest.raises(SystemExit, match="nothing recorded"):
        main(["backtest", "--config", str(tmp_path / "c.toml"), "--from", "2026-09-01"])


def test_idle_gaps_tick_slower_and_inputs_on_the_timer_grid_still_advance(tmp_path):
    """Two inputs a day apart, the second exactly on the 5 s grid: idle ticks are a minute apart, and the runner
    doesn't stall on an input that lands where the next Timer would."""
    class Two:
        name, raw, count = "kalshi", "two", 0

        def events(self):
            c = Contract("kalshi", HOU, "KXNFLGAME-26OCT18HOUJAC", HOU, "", "open", None, series_ticker="KXNFLGAME")
            yield 1_000_000, c
            yield 1_000_000 + 86_400_000, c

    r = Runner(load(text="[engine]\nrecord = false\n"), start=DAY, end=DAY, root=tmp_path, run_dir=tmp_path / "bt",
               history=lambda: [Two()], timer_ms=5_000, idle_after_s=120, idle_step_s=60)
    s = r.run()
    assert s.inputs == 2 and s.timers < 120 // 5 + 86_400 // 60 + 5
