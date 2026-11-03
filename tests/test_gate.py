"""The promotion gate: live only after paper trading with the same params, long enough, and not unprofitable."""

from __future__ import annotations

from pathlib import Path

import pytest

from pmx.analytics import gate
from pmx.analytics.stats import ReadOnly
from pmx.core.types import Fill, OrderRequest
from pmx.ledger.db import Attribution, Ledger

DAY = 86_400_000
T0 = 1_792_300_000_000


def _ledger(path: Path, trades: list[tuple[str, str, int, bool]]) -> ReadOnly:
    """trades: (strategy, params hash, day, wins) — each a 10-contract buy at 50¢ that settles the next hour."""
    led = Ledger(path, env="prod")
    for n, (strategy, params, day, win) in enumerate(trades):
        ts, ticker = T0 + day * DAY, f"SER-G{n}-A"
        a = Attribution("run", "sha", params)
        coid = f"pmx-t-{n}"
        req = OrderRequest("kalshi", ticker, "bid", 5_000, 1_000, "ioc", False, coid, f"{strategy}/{ticker}")
        led.record_order(req, "paper", ts, attribution=a)
        led.record_fill(Fill("kalshi", f"F{n}", f"V{n}", ticker, "bid", 5_000, 1_000, 0, True, ts, coid), "paper",
                        attribution=a)
        led.record_settlement("kalshi", ticker, "yes" if win else "no", "paper", ts + 3_600_000)
    led.close()
    return ReadOnly(path, "prod")


def test_no_paper_record_or_too_short_a_one_is_refused(tmp_path):
    led = _ledger(tmp_path / "l.sqlite", [("s", "h1", d, d % 2 == 0) for d in range(4)])
    (r,) = gate.check(led, {"s": "h1"}, min_days=10)
    assert not r.ok and r.fills == 4 and "3.0 of the 10 days" in r.why
    (r,) = gate.check(led, {"s": "h2"}, min_days=10)  # new params: the paper record starts over
    assert not r.ok and r.fills == 0 and "no paper trading yet" in r.why
    assert not gate.check(None, {"s": "h1"}, min_days=10)[0].ok


def test_a_long_enough_record_passes_unless_its_verdict_is_unprofitable(tmp_path):
    winner = _ledger(tmp_path / "w.sqlite", [("s", "h1", d, d % 3 != 0) for d in range(15)])
    (r,) = gate.check(winner, {"s": "h1"}, min_days=10)
    assert r.ok and r.days == 14 and r.call in ("profitable", "inconclusive")
    loser = _ledger(tmp_path / "x.sqlite", [("s", "h1", d, False) for d in range(15)])
    (r,) = gate.check(loser, {"s": "h1"}, min_days=10)
    assert not r.ok and r.call == "unprofitable"


def test_pmx_run_live_refuses_before_starting_anything(tmp_path, monkeypatch, capsys):
    from pmx.cli import main

    cfg = tmp_path / "s.toml"
    cfg.write_text('[engine]\nenv = "demo"\n\n[risk]\nmax_loss = "10.00"\n\n[[strategy]]\nid = "s"\nkind = "noop"\n'
                   'mode = "live"\nmarkets = { series = ["KXNFLGAME"] }\nrisk = { max_loss = "5.00" }\n')
    monkeypatch.setenv("PMX_DATA", str(tmp_path / "data"))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PMX_SOCK", "pmx.sock")  # pytest's tmp paths are too long for a Unix socket
    with pytest.raises(SystemExit, match="live trading refused for s"):
        main(["run", "--env", "demo", "--live", "--config", str(cfg)])
    out = capsys.readouterr().out
    assert "promotion gate" in out and "✗ s" in out and "no ledger yet" in out
