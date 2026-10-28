from __future__ import annotations

import pytest

from pmx.core.types import Fill, OrderAck, OrderRequest
from pmx.ledger.db import Ledger


def _fill(fid, side, price, qty, ticker="A", fee=0):
    return Fill("kalshi", fid, "o1", ticker, side, price, qty, fee, True, 1)


@pytest.fixture
def ledger():
    led = Ledger(":memory:")
    yield led
    led.close()


def test_fills_are_idempotent_and_positions_derive(ledger):
    assert ledger.record_fill(_fill("f1", "bid", 4000, 1000, fee=5000), "paper")
    assert not ledger.record_fill(_fill("f1", "bid", 4000, 1000, fee=5000), "paper")
    ledger.record_fill(_fill("f2", "ask", 4500, 400), "paper")
    [p] = ledger.positions("paper").values()
    assert p.qty == 600 and p.cash == -4000 * 1000 + 4500 * 400 and p.fees == 5000
    # marked at 50¢: -4.00 + 1.80 + 6 * 0.50 - 0.005 = +0.795
    assert p.pnl(5000) == 795_000
    assert ledger.positions("live") == {}
    with pytest.raises(ValueError):
        p.pnl()


def test_settlement_pays_longs_and_shorts(ledger):
    ledger.record_fill(_fill("f1", "bid", 3000, 200, ticker="L"), "paper")  # long 2 YES @ 30¢
    ledger.record_fill(_fill("f2", "ask", 3000, 200, ticker="S"), "paper")  # short 2 YES (long NO) @ 30¢
    ledger.record_settlement("kalshi", "L", "yes", "paper")
    ledger.record_settlement("kalshi", "S", "yes", "paper")
    pos = ledger.positions("paper", include_flat=True)
    assert pos[("kalshi", "L")].pnl() == 1_400_000  # +$1.40
    assert pos[("kalshi", "S")].pnl() == -1_400_000
    assert ledger.positions("paper") == {}
    assert ledger.realized_pnl("paper") == 0


def test_order_lifecycle(ledger):
    req = OrderRequest("kalshi", "A", "bid", 5600, 1000, strategy="test")
    ledger.record_order(req, "paper", now_ms=1)
    assert ledger.open_orders("paper")[0]["status"] == "pending"
    ledger.ack_order(OrderAck("kalshi", "oid", req.client_order_id, 400, 600, 2))
    [o] = ledger.open_orders("paper")
    assert (o["status"], o["order_id"], o["filled"]) == ("open", "oid", 400)
    ledger.set_order_status(req.client_order_id, "canceled")
    assert ledger.open_orders("paper") == []


def test_an_old_file_gains_the_new_columns_and_keeps_its_rows(tmp_path):
    import sqlite3

    path = tmp_path / "old.sqlite"
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE fills (venue TEXT NOT NULL, mode TEXT NOT NULL, fill_id TEXT NOT NULL, order_id TEXT NOT NULL,
            client_order_id TEXT NOT NULL, ticker TEXT NOT NULL, side TEXT NOT NULL, price INTEGER NOT NULL,
            qty INTEGER NOT NULL, fee INTEGER NOT NULL, is_taker INTEGER NOT NULL, ts_ms INTEGER NOT NULL,
            PRIMARY KEY (venue, mode, fill_id));
        INSERT INTO fills VALUES ('kalshi', 'paper', 'f0', 'o', 'c', 'A', 'bid', 4000, 100, 0, 1, 1);
    """)
    db.commit()
    db.close()
    led = Ledger(path)
    cols = {r[1] for r in led.db.execute("PRAGMA table_info(fills)")}
    assert {"env", "run_id", "strategy_version", "params_hash"} <= cols
    assert led.positions("paper")[("kalshi", "A")].qty == 100  # env '' rows read by an env-less ledger


def test_envs_never_mix_and_slots_attribute_through_orders(tmp_path):
    from pmx.ledger.db import Attribution

    demo, prod = Ledger(tmp_path / "l.sqlite", env="demo"), Ledger(tmp_path / "l.sqlite", env="prod", durable=True)
    req = OrderRequest("kalshi", "A", "bid", 4000, 300, client_order_id="c1", strategy="s/A")
    demo.record_order(req, "paper", 1, attribution=Attribution("run7", "deadbee", "h1"))
    demo.record_fill(Fill("kalshi", "f1", "o1", "A", "bid", 4000, 300, 0, True, 2, "c1"), "paper")
    demo.record_fill(Fill("kalshi", "f2", "o2", "A", "ask", 4500, 100, 0, True, 3, "nobody"), "paper")
    assert {k: p.qty for k, p in demo.positions("paper", by="slot").items()} == {("s/A", "kalshi", "A"): 300,
                                                                                ("", "kalshi", "A"): -100}
    assert prod.positions("paper") == {} and prod.open_orders("paper") == []
    assert demo.db.execute("SELECT run_id, strategy_version, params_hash, env FROM orders").fetchone() == (
        "run7", "deadbee", "h1", "demo")
    assert prod.db.execute("PRAGMA synchronous").fetchone()[0] == 2  # FULL for live sessions
    assert demo.cancel_open("paper", "restart") == 1 and demo.open_orders("paper") == []


def test_pnl_series_realizes_trades_and_settlements_including_a_tie(ledger):
    ledger.record_fill(_fill("f1", "bid", 4000, 300, ticker="A", fee=1000), "paper")  # long 3 @ 40¢
    ledger.record_fill(_fill("f2", "ask", 4500, 100, ticker="A"), "paper")  # sell 1 @ 45¢: +$0.05
    ledger.record_settlement("kalshi", "A", 5000, "paper", ts_ms=10)  # a tie: the last 2 pay 50¢: +$0.20
    assert ledger.pnl_series("paper") == [(1, -1000), (1, 49_000), (10, 249_000)]
    [p] = ledger.positions("paper", include_flat=True).values()
    assert (p.qty, p.settled, p.settled_price, p.pnl()) == (0, None, 5000, 249_000)
