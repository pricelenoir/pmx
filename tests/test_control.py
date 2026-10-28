"""The control channel — manual orders, cancels, halts, status, positions, advice — against a running `System`."""

from __future__ import annotations

import asyncio
import os
import stat
from pathlib import Path

import msgspec
import pytest

from pmx.cli.control import parse_duration_s, parse_order_price
from pmx.control import protocol
from pmx.control.client import NotRunning, request
from pmx.engine.config import load
from pmx.engine.events import Advice
from pmx.engine.run import System
from pmx.ledger.db import Ledger
from test_run_config import BASE, FakeMarket, _until

CONFIG = BASE.replace('[[source]]\nname = "espn"\nkind = "espn_nfl"\n', "") + """
[manual]
risk = { max_order_qty = "10", max_position = "20" }
"""
A = "KXFED-A"  # FakeMarket: bid 40¢ × 10, ask 46¢ × 10
CTL = Path("r-ctl.sock")


def test_requests_and_responses_round_trip():
    for req in (protocol.Order("T", "bid", 4_000, 500), protocol.Cancel(all=True, mode="paper"),
                protocol.Halt("smoke", "x"), protocol.Resume(), protocol.Status(), protocol.Positions("live"),
                protocol.Advise("T", p=0.6, expires_s=60)):
        assert protocol.requests.decode(protocol.encode(req)) == req
    line = protocol.encode(protocol.Order("T", "ask", 4_100, 100, "live", "ioc"))
    assert msgspec.json.decode(line)["type"] == "order"
    resp = protocol.Response(False, "nope", rejects=[{"rule": "halted"}])
    assert protocol.responses.decode(protocol.encode(resp)) == resp


def test_prices_and_durations_parse_like_a_person_writes_them():
    assert parse_order_price("0.40") == parse_order_price("40c") == parse_order_price("$0.40") == 4_000
    assert parse_order_price("40.5¢") == 4_050
    assert parse_duration_s("90s") == 90 and parse_duration_s("30m") == 1_800 and parse_duration_s("2h") == 7_200
    with pytest.raises(SystemExit, match="dollars"):
        parse_order_price("forty")


async def _running(tmp_path: Path, *, text: str = CONFIG, **kw):
    s = System(load(text=text), env="demo", sock=Path("r.sock"), data=tmp_path / "data", market=FakeMarket(), **kw)
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop))
    await _until(lambda: CTL.exists() and A in s.state.contracts and s.state.book(A) is not None, 10, "engine up")
    return s, stop, task


def test_manual_paper_orders_rest_fill_cancel_and_are_attributed_to_their_slot(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    async def go() -> System:
        s, stop, task = await _running(tmp_path)
        assert stat.S_IMODE(os.stat(CTL).st_mode) == 0o600
        rest = await request(CTL, protocol.Order(A, "bid", 3_900, 500))
        assert rest.ok and [o["status"] for o in rest.orders][-1] == "open" and rest.orders[0]["slot"] == "manual/" + A
        take = await request(CTL, protocol.Order(A, "bid", 4_600, 200, tif="ioc"))
        assert take.ok and [f["qty"] for f in take.fills] == [200] and take.fills[0]["is_taker"]
        big = await request(CTL, protocol.Order(A, "bid", 4_000, 1_500))
        assert not big.ok and big.error.startswith("rejected: max_order_qty")
        assert [r["rule"] for r in big.rejects] == ["max_order_qty"]
        live = await request(CTL, protocol.Order(A, "bid", 4_000, 100, "live", confirmed=True))
        assert not live.ok and "pmx run --live" in live.error
        st = (await request(CTL, protocol.Status())).data
        assert st["env"] == "demo" and not st["live"] and st["working_orders"] == {"paper": 1}
        assert [o["client_order_id"] for o in st["manual_orders"]] == [rest.orders[0]["client_order_id"]]
        assert {"kalshi", "timer"} <= set(st["sources"])
        pos = (await request(CTL, protocol.Positions())).data["positions"]
        assert [(p["slot"], p["mode"], p["qty"]) for p in pos] == [("manual/" + A, "paper", 200)]
        none = await request(CTL, protocol.Cancel())
        assert not none.ok and "say which" in none.error
        gone = await request(CTL, protocol.Cancel(all=True))
        assert gone.ok and gone.orders[-1]["status"] == "canceled" and s.orders.orders == {}
        stop.set()
        await task
        return s

    s = asyncio.run(go())
    led = Ledger(tmp_path / "data" / "ledger.sqlite", env="demo")
    rows = list(led.db.execute("SELECT strategy, status, run_id, strategy_version FROM orders ORDER BY created_ms"))
    assert [(r[0], r[1]) for r in rows] == [("manual/" + A, "canceled"), ("manual/" + A, "filled")]
    assert {r[2] for r in rows} == {s.run_id} and {r[3] for r in rows} == {"manual"}
    assert [slot for slot, _ in led.fills("paper")] == ["manual/" + A]


def test_halts_and_resumes_apply_through_the_socket_and_advice_reaches_the_engine(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    seen: list[Advice] = []

    async def go() -> None:
        s, stop, task = await _running(tmp_path)
        s.runlog.listeners.append(lambda ev, now: seen.append(ev) if type(ev) is Advice else None)
        await request(CTL, protocol.Order(A, "bid", 3_900, 100))
        h = await request(CTL, protocol.Halt("", "hands off"))
        assert h.ok and s.risk.halts == {"": "hands off"} and s.orders.orders == {}, "halted at once, orders pulled"
        refused = await request(CTL, protocol.Order(A, "bid", 3_900, 100))
        assert not refused.ok and "halted" in refused.error
        assert (await request(CTL, protocol.Resume(""))).ok and s.risk.halts == {}
        assert (await request(CTL, protocol.Order(A, "bid", 3_900, 100))).ok
        adv = await request(CTL, protocol.Advise(A, p=0.62, confidence=0.8, expires_s=600, note="hunch"))
        assert adv.ok and not (await request(CTL, protocol.Advise(A))).ok, "a probability or a direction"
        await _until(lambda: seen, 5, "advice dispatched")
        stop.set()
        await task

    asyncio.run(go())
    assert (seen[0].advisor, seen[0].target, seen[0].p, seen[0].rationale) == ("manual", A, 0.62, "hunch")
    assert not CTL.exists(), "the socket goes at shutdown"


def test_pmx_halt_falls_back_to_the_kill_switch_file_and_clients_say_when_nothing_runs(tmp_path, monkeypatch, capsys):
    from pmx.cli import main
    from pmx.execution.risk import read_halts

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PMX_DATA", str(tmp_path / "data"))
    main(["halt", "smoke", "--reason", "lunch"])
    assert read_halts(tmp_path / "data" / "HALT") == {"smoke": "lunch"}
    assert "applies as soon as it starts" in capsys.readouterr().out
    with pytest.raises(NotRunning, match="start one with `pmx up`"):
        asyncio.run(request(tmp_path / "nothing-ctl.sock", protocol.Status()))
    main(["status"])
    assert "the engine is not responding" in capsys.readouterr().out, "then whatever the health files say"
