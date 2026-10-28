"""Backtest reports over a ledger with known answers; comparison; sweeps; the view socket."""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
from pathlib import Path

import msgspec

sys.path.insert(0, str(Path(__file__).parent / "fixtures" / "ledger"))
from build import ENV, build  # noqa: E402

from pmx.backtest.report import compare, markdown, report, serve_view, view_messages, write  # noqa: E402
from pmx.feed import proto  # noqa: E402


def _run(tmp: Path, name: str, **summary) -> Path:
    d = tmp / name
    d.mkdir(parents=True)
    build(d / "ledger.sqlite")
    (d / "summary.json").write_text(json.dumps({"run_id": name, "start": "2026-10-18", "end": "2026-10-19",
                                                "fidelity": "book", **summary}))
    return d


def test_a_report_has_every_strategy_slot_and_total_from_the_analytics(tmp_path):
    r = report(_run(tmp_path, "bt-a"), env=ENV)
    by = {s.strategy: s for s in r.strategies}
    assert set(by) == {"win3", "luck", "loser", "flip", "edges"}
    assert by["win3"].stats.net == 18_000_000 and by["win3"].verdict.verdict == "inconclusive"
    assert by["loser"].verdict.verdict == "unprofitable" and by["luck"].verdict.verdict == "profitable"
    slot = next(x for x in by["flip"].slots if x.slot == "flip/SER-FLIP-A")
    assert (slot.net, slot.fills, slot.max_position) == (1_250_000, 3, 1_000), "peak |position| 10 contracts"
    assert r.total.net == sum(s.stats.net for s in r.strategies) and r.total.pnl_ci is not None
    md = markdown(r)
    assert "# Backtest report — bt-a" in md and "| loser | unprofitable |" in md and "flip/SER-FLIP-A" in md
    write(r, tmp_path / "bt-a")
    assert json.loads((tmp_path / "bt-a" / "report.json").read_text())["title"] == "bt-a"
    assert (tmp_path / "bt-a" / "report.md").read_text() == md


def test_candle_fidelity_is_labeled_and_runs_compare_side_by_side(tmp_path):
    a = report(_run(tmp_path, "book"), env=ENV)
    b = report(_run(tmp_path, "candle", fidelity="candle"), env=ENV)
    assert not a.warnings and "candle fidelity" in b.warnings[0] and "> candle fidelity" in markdown(b)
    table = compare(a, b)
    assert "| strategy | metric | book | candle |" in table and "| win3 | net | +$18.00 | +$18.00 |" in table


def test_a_sweep_run_says_how_many_variants_were_tried_and_shows_its_other_window(tmp_path):
    out = tmp_path / "sweep"
    for variant in ("fast", "slow", "mid"):
        for window in ("train", "evaluate"):
            _run(out / variant, window)
    r = report(out / "mid" / "evaluate", env=ENV)
    assert r.sweep == {"variant": "mid", "variants": 3, "window": "evaluate", "other_window": "train",
                       "other_net": r.total.net}
    assert "one of 3 tried" in markdown(r)


def test_view_serves_the_run_like_a_live_session(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # AF_UNIX paths are short
    d = _run(tmp_path, "bt-v")
    msgs = view_messages(d, env=ENV)
    assert isinstance(msgs[0], proto.Hello) and msgs[0].source == "backtest" and msgs[0].run == "bt-v"
    assert isinstance(msgs[-1], proto.Synced)
    assert {m.scope for m in msgs if isinstance(m, proto.EquityMsg)} == {"strategy:win3"}
    perf = {m.strategy: m for m in msgs if isinstance(m, proto.PerfMsg)}
    assert set(perf) == {"win3", "luck", "loser", "flip", "edges"} and perf["win3"].stats.net == 18_000_000
    sock = Path("v.sock")

    async def go() -> list[dict]:
        stop = asyncio.Event()
        task = asyncio.create_task(serve_view(msgs, sock, stop))
        for _ in range(50):
            if sock.exists():
                break
            await asyncio.sleep(0.02)
        r, w = await asyncio.open_unix_connection(str(sock), limit=1 << 22)
        got = []
        while True:
            m = msgspec.json.decode(await r.readline())
            got.append(m)
            if m["t"] == "synced":
                break
        w.close()
        stop.set()
        await task
        return got

    got = asyncio.run(go())
    assert [m["t"] for m in got][:2] == ["hello", "equity"] and len(got) == len(msgs)
    assert not sock.exists()


def test_pmx_backtest_report_and_compare(tmp_path, monkeypatch, capsys):
    from pmx.cli import main

    monkeypatch.setenv("PMX_DATA", str(tmp_path))
    _run(tmp_path / "backtests", "r1")
    shutil.copytree(tmp_path / "backtests" / "r1", tmp_path / "backtests" / "r2")
    main(["backtest", "report", "r1", "--env", ENV])
    assert "# Backtest report — r1" in capsys.readouterr().out
    assert (tmp_path / "backtests" / "r1" / "report.md").exists()
    main(["backtest", "compare", "r1", "r2", "--env", ENV])
    assert "| **total** | net |" in capsys.readouterr().out
    shutil.copy(tmp_path / "backtests" / "r1" / "ledger.sqlite", tmp_path / "ledger.sqlite")
    main(["backtest", "report", "--paper", "--from", "2026-10-18", "--to", "2026-10-19", "--env", ENV])
    assert "paper 2026-10-18..2026-10-19" in capsys.readouterr().out
