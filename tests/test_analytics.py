"""Performance analytics on fixture ledgers with known answers; clustering; speed; `pmx perf`, `pmx export`."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "fixtures" / "ledger"))
from build import DAY, ENV, H, _Book, build  # noqa: E402

from pmx.analytics.breakdowns import breakdowns, edge_spearman  # noqa: E402
from pmx.analytics.publish import PerfPublisher, reports  # noqa: E402
from pmx.analytics.stats import ReadOnly, bootstrap, compute, load, strategy_stats  # noqa: E402
from pmx.analytics.verdict import verdict  # noqa: E402
from pmx.ledger.db import Ledger  # noqa: E402


@pytest.fixture(scope="module")
def led(tmp_path_factory):
    path = build(tmp_path_factory.mktemp("ledger") / "perf.sqlite")
    r = ReadOnly(path, ENV)
    yield r
    r.close()


def _all(led, strategy):
    d = load(led, mode="paper", strategy=strategy)
    s = compute(d)
    rows = breakdowns(d)
    return d, s, rows, verdict(s, rows)


def test_a_three_game_winner_is_inconclusive_with_every_stat_known(led):
    _, s, rows, v = _all(led, "win3")
    assert (s.episodes, s.fills, s.contracts, s.entities, s.markets) == (3, 3, 3_000, 3, 3)
    assert (s.net, s.realized, s.open, s.fees) == (18_000_000, 18_000_000, 0, 0)
    assert (s.win_rate, s.avg_win, s.avg_loss, s.profit_factor) == (1.0, 6_000_000, None, None)
    assert s.expectancy_c == 60.0 and s.best_episode == s.worst_episode == 6_000_000
    assert (s.edge_entry_c, s.edge_realized_c, s.capture) == (10.0, 60.0, 6.0)
    assert (s.brier_model, s.brier_market) == (0.25, 0.3364)
    assert (s.max_drawdown, s.time_under_water_ms, s.max_capital_at_risk) == (4_000_000, 2 * H, 4_000_000)
    assert [p for _, p in s.daily] == [6_000_000, 12_000_000] and s.days == 2 and s.pct_days_positive == 1.0
    assert s.sharpe_day == 2.121, "mean 9M / sd 4.24M over two days"
    assert v.verdict == "inconclusive" and "3 entities" in v.reason and "at least 10" in v.needed
    assert [r.key for r in rows["phase"]] == ["Q1"] and rows["edge"][0].key == ">10¢"


def test_positive_pnl_with_adverse_markouts_is_flagged_as_luck(led):
    _, s, _, v = _all(led, "luck")
    assert s.net == 50_000_000 and s.markouts_c == {"1m": -2.0} and round(s.expectancy_c, 2) == 41.67
    assert v.verdict == "profitable" and s.expectancy_ci[0] > 0
    assert any("luck" in f for f in v.flags)


def test_a_steady_loser_is_unprofitable_and_fees_count(led):
    _, s, _, v = _all(led, "loser")
    assert (s.episodes, s.fees, s.net) == (12, 240_000, 12 * -70_000)
    assert s.expectancy_c == -3.5 and s.maker_share == 0.0 and s.fill_rate is None
    assert v.verdict == "unprofitable" and s.p_nonpositive == 1.0


def test_fees_paid_on_open_positions_count_in_net(tmp_path):
    led = Ledger(tmp_path / "open.sqlite", env=ENV)
    b = _Book(led)
    b.trade("open1/SER-OPEN-A", "SER-OPEN-A", "bid", 4_000, 1_000, DAY, fee=70_000, entity="game:o")
    led.record_equity([(DAY + H, "paper", "strategy:open1", 930_000, 0, 1_000_000, 70_000, 4_000_000)])
    led.close()
    r = ReadOnly(tmp_path / "open.sqlite", ENV)
    try:
        s = compute(load(r, mode="paper", strategy="open1"))
    finally:
        r.close()
    assert (s.episodes, s.open, s.fees, s.realized) == (0, 1_000_000, 70_000, 0)
    assert s.net == 930_000 and s.gross == 1_000_000, "marked at 50¢, less the fee already paid: the equity row"


def test_a_flip_through_flat_is_two_episodes(led):
    d, s, _, _ = _all(led, "flip")
    assert [(e.pnl, e.contracts) for e in d.episodes] == [(1_000_000, 2_000), (250_000, 1_000)]
    assert s.net == 1_250_000


def test_edges_that_dont_pay_and_one_game_carrying_it_are_flagged(led):
    d, s, rows, v = _all(led, "edges")
    assert [(r.key, r.fills) for r in rows["edge"]] == [("2–4¢", 2), ("6–10¢", 2)]
    assert rows["edge"][0].cpc == 60.0 and rows["edge"][1].cpc == -40.0
    assert any("not monotone" in f for f in v.flags) and any("game:e0 carries" in f for f in v.flags)
    assert edge_spearman(d) < 0


def test_the_bootstrap_is_seeded_and_clustered_by_entity(tmp_path):
    """Ten games' results, then the same results ten times over within each game, then spread over 100 games:
    duplicating within a game mustn't narrow the interval the way independent games do."""
    def ledger(name: str, games: int, per_game: int) -> ReadOnly:
        path = tmp_path / f"{name}.sqlite"
        led = Ledger(path, env=ENV)
        b = _Book(led)
        for g in range(games):
            for k in range(per_game):
                t = f"SER-{name}{g}x{k}-A"
                win = (g + k * 3) % 10 < 6 if games == 100 else g % 10 < 6
                b.trade(f"s/{t}", t, "bid", 5_000, 100, DAY + (g * per_game + k) * 1_000, entity=f"game:{g}")
                led.record_settlement("kalshi", t, "yes" if win else "no", "paper", DAY + H)
        led.close()
        return ReadOnly(path, ENV)

    widths = {}
    for name, games, per in (("ten", 10, 1), ("dup", 10, 10), ("wide", 100, 1)):
        d = load(ledger(name, games, per), mode="paper", strategy="s")
        _, ci, _ = bootstrap(d.episodes)
        widths[name] = ci[1] - ci[0]
        assert bootstrap(d.episodes) == bootstrap(d.episodes), "seeded"
    assert abs(widths["dup"] - widths["ten"]) <= 0.2 * widths["ten"]
    assert widths["wide"] < 0.6 * widths["dup"]


def test_a_hundred_thousand_fills_take_under_five_seconds(tmp_path):
    path = tmp_path / "big.sqlite"
    led = Ledger(path, env=ENV)
    rows, orders = [], []
    for i in range(100_000):
        t = f"SER-B{i // 20}-{'A' if i % 2 else 'B'}"
        coid = f"pmx-big-{i:x}"
        side = "bid" if (i // 2) % 2 == 0 else "ask"
        orders.append((coid, "kalshi", t, side, 5_000, 100, "gtc", 0, f"s/{t}", "paper", f"V{i}", "filled", 100,
                       None, DAY + i, DAY + i, ENV, "", "", ""))
        rows.append(("kalshi", "paper", f"F{i}", f"V{i}", coid, t, side, 5_000 + (i % 7) * 100, 100, 1_000, i % 3 == 0,
                     DAY + i * 100, ENV, "", "", ""))
    led.db.execute("BEGIN")
    led.db.executemany("INSERT INTO orders (client_order_id, venue, ticker, side, price, qty, tif, post_only, "
                       "strategy, mode, order_id, status, filled, error, created_ms, updated_ms, env, run_id, "
                       "strategy_version, params_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", orders)
    led.db.executemany("INSERT INTO fills (venue, mode, fill_id, order_id, client_order_id, ticker, side, price, qty, "
                       "fee, is_taker, ts_ms, env, run_id, strategy_version, params_hash) "
                       "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    led.db.execute("COMMIT")
    led.close()
    t0 = time.perf_counter()
    r = ReadOnly(path, ENV)
    d = load(r, mode="paper", strategy="s")
    s = compute(d)
    verdict(s, breakdowns(d))
    took = time.perf_counter() - t0
    assert s.fills == 100_000 and s.episodes == 50_000 and took < 5.0, f"{took:.2f} s"


def test_pmx_perf_and_export(led, tmp_path, capsys):
    from pmx.cli import main

    path = led.db.execute("PRAGMA database_list").fetchone()[2]
    main(["perf", "--ledger", path, "--env", ENV, "--strategy", "win3"])
    out = capsys.readouterr().out
    assert "INCONCLUSIVE: +60.00¢/contract" in out and "max drawdown $4.00" in out
    main(["perf", "--ledger", path, "--env", ENV, "--slot", "loser/SER-LOSE0-A", "--json"])
    j = json.loads(capsys.readouterr().out)
    assert j["stats"]["net"] == -70_000 and j["verdict"]["verdict"] == "inconclusive"
    with pytest.raises(SystemExit, match="no live fills"):
        main(["perf", "--ledger", path, "--env", ENV, "--mode", "live"])
    main(["export", "--year", "2026", "--ledger", path, "--env", ENV])
    rows = list(csv.reader(io.StringIO(capsys.readouterr().out)))
    assert rows[0][:3] == ["time", "type", "mode"] and sum(r[1] == "fill" for r in rows) == 3 + 12 + 24 + 3 + 4
    settled = [r for r in rows if r[1] == "settlement"]
    assert len(settled) == 3 + 12 + 4 and {r[4] for r in settled} >= {"SER-WIN0-A"}


def test_live_engines_publish_perf_off_the_dispatch_path(led):
    path = Path(led.db.execute("PRAGMA database_list").fetchone()[2])
    msgs = reports(path, ENV, ["win3", "loser", "nothing"], DAY + 30 * H)
    assert {(m.strategy, m.range) for m in msgs} == {(s, r) for s in ("win3", "loser")
                                                     for r in ("today", "7d", "30d", "all")}
    sent = []

    async def go() -> None:
        pub = PerfPublisher(path, ENV, ["win3"], sent.append, every_s=0.05, clock=lambda: DAY + 30 * H)
        stop = asyncio.Event()
        task = asyncio.create_task(pub.run(stop))
        for _ in range(100):
            if sent:
                break
            await asyncio.sleep(0.02)
        stop.set()
        pub.soon()
        await task

    asyncio.run(go())
    assert sent and sent[0].strategy == "win3" and sent[-1].stats.net == 18_000_000


def test_stats_respect_env_and_ranges(led):
    assert strategy_stats(led, mode="paper", strategy="win3", start_ms=DAY + 90 * 60_000).episodes == 2
    other = ReadOnly(led.db.execute("PRAGMA database_list").fetchone()[2], "demo")
    assert strategy_stats(other, mode="paper", strategy="win3").fills == 0, "envs never mix"
