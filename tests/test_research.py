"""Research layer: windows and the catalog cut, panel/asof without lookahead, costs, evaluation, specs, and the
experiment lifecycle (approval lock, amendments, one holdout, trials)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from pmx.research import costs
from pmx.research import kalshi_data as kd
from pmx.research.catalog import Catalog
from pmx.research.evaluate import evaluate
from pmx.research.panel import asof, duration_ms, panel
from pmx.research.runner import run_stage
from pmx.research.spec import parse
from pmx.research.windows import Windows
from pmx.research.workspace import ResearchError, Workspace

H = 3_600_000
WIN = Windows(date(2026, 1, 1), date(2026, 7, 1))
SERIES = "KXTEST"


def ms(y: int, m: int, d: int, h: int = 0) -> int:
    return int(datetime(y, m, d, h, tzinfo=UTC).timestamp() * 1000)


def _days() -> list[int]:
    """Close times: 30 events in 2025 (discovery), 12 in 2026H1 (validation), 6 after (holdout)."""
    return ([ms(2025, 1 + i % 12, 1 + i // 12 * 7, 23) for i in range(30)]
            + [ms(2026, 1 + i % 6, 3 + i // 6 * 9, 23) for i in range(12)]
            + [ms(2026, 7 + i % 3, 5 + i // 3 * 9, 23) for i in range(6)])


def build_data(root: Path) -> None:
    """Each event: a longshot bucket (bid 8¢, ask 10¢; settles NO) and a favourite (bid 88¢, ask 90¢; settles YES).
    In each market's last 2 hours the quotes jump to where it settles — what a lookahead would see."""
    d = kd.series_dir(root, SERIES)
    (d / "candles_1h").mkdir(parents=True)
    (d / "series.json").write_text(json.dumps({"series": SERIES, "taker_e4": 700, "maker_e4": 0}))
    markets, = [[]]
    for i, close in enumerate(_days()):
        ev = f"{SERIES}-E{i:03d}"
        rows = []
        for suffix, result, bid, ask in (("LO", "no", 800, 1000), ("HI", "yes", 8800, 9000)):
            t = f"{ev}-{suffix}"
            markets.append({"ticker": t, "event_ticker": ev, "series": SERIES, "title": "", "yes_sub_title": suffix,
                            "open_ms": close - 24 * H, "close_ms": close, "expiration_ms": close, "result": result,
                            "settlement": 10_000 if result == "yes" else 0, "strike_type": "between",
                            "floor_strike": 1.0, "cap_strike": 2.0, "volume": 100_00, "source": "historical"})
            final = 10_000 if result == "yes" else 0
            for k in range(1, 25):
                end = close - 24 * H + k * H
                late = end > close - 2 * H
                b, a = (final - 100, final) if late and final else ((0, 100) if late else (bid, ask))
                rows.append({"ticker": t, "end_ms": end, "bid_open": b, "bid_low": b, "bid_high": b, "bid_close": b,
                             "ask_open": a, "ask_low": a, "ask_high": a, "ask_close": a, "price_open": a,
                             "price_high": a, "price_low": a, "price_close": a, "volume": 10_00, "oi": None})
        pl.DataFrame(rows, schema=kd.CANDLE_SCHEMA).write_parquet(d / "candles_1h" / f"{ev}.parquet")
    pl.DataFrame(markets, schema=kd.MARKET_SCHEMA).write_parquet(d / "markets.parquet")


@pytest.fixture
def data(tmp_path: Path) -> Path:
    root = tmp_path / "data"
    build_data(root)
    return root


# ---- windows · catalog · panel ------------------------------------------------------------------------------------

def test_windows_file_must_hold_dates(tmp_path):
    f = tmp_path / "windows.toml"
    f.write_text('discovery_end = "2026-01-01"\nvalidation_end = 2026-07-01\n')
    with pytest.raises(ValueError, match="unquoted"):
        Windows.load(f)
    f.write_text("discovery_end = 2026-01-01\nvalidation_end = 2026-07-01\n")
    assert Windows.load(f) == WIN


def test_windows_order_and_ranges():
    with pytest.raises(ValueError):
        Windows(date(2026, 7, 1), date(2026, 1, 1))
    assert WIN.visible_end_ms("discovery") == ms(2026, 1, 1) and WIN.visible_end_ms("holdout") is None
    assert WIN.judged_ms("validation") == (ms(2026, 1, 1), ms(2026, 7, 1))


def test_catalog_hides_markets_after_the_stage(data):
    n = {s: Catalog(data, WIN, s).markets(SERIES).height for s in ("discovery", "validation", "holdout")}
    assert n == {"discovery": 60, "validation": 84, "holdout": 96}
    c = Catalog(data, WIN, "discovery").candles(SERIES)
    assert c["end_ms"].max() < ms(2026, 1, 1) and c["close_ms"].max() < ms(2026, 1, 1)
    assert Catalog(data, WIN).series("KX*") == [SERIES]
    with pytest.raises(FileNotFoundError, match="pmx data kalshi"):
        Catalog(data, WIN).series("NOPE")


def test_outside_tables_need_known_ms_and_are_cut(data):
    pl.DataFrame({"known_ms": [ms(2025, 6, 1), ms(2026, 3, 1)], "v": [1, 2]}).write_parquet(data / "obs.parquet")
    assert Catalog(data, WIN, "discovery").table("obs.parquet")["v"].to_list() == [1]
    pl.DataFrame({"v": [1]}).write_parquet(data / "bad.parquet")
    with pytest.raises(ValueError, match="known_ms"):
        Catalog(data, WIN).table("bad.parquet")


def test_panel_sees_only_quotes_known_at_decision_time(data):
    cat = Catalog(data, WIN, "discovery")
    p = panel(cat, SERIES, at="T-3h")
    assert p.height == 60
    lo = p.filter(pl.col("ticker").str.ends_with("-LO"))
    assert lo["bid"].unique().to_list() == [0.08] and lo["ask"].unique().to_list() == [0.10]
    assert (p["t_ms"] < p["close_ms"]).all() and (p["quote_age_s"] >= 0).all()
    late = panel(cat, SERIES, at="T-1h").filter(pl.col("ticker").str.ends_with("-LO"))
    assert late["ask"].unique().to_list() == [0.01]  # in the last 2 hours: the jump is visible, as it was live
    assert late["bid"].null_count() == late.height  # a 0 bid is no bid, not a price
    every = panel(cat, SERIES, at="every")
    assert every.height == 60 * 23 and (every["t_ms"] < every["close_ms"]).all()
    with pytest.raises(ValueError):
        panel(cat, SERIES, at="noon")


def test_asof_never_joins_the_future():
    left = pl.DataFrame({"k": ["a", "a"], "t_ms": [100, 200]})
    right = pl.DataFrame({"k": ["a", "a", "a"], "known_ms": [50, 150, 250], "v": [1, 2, 3]})
    assert asof(left, right, by="k")["v"].to_list() == [1, 2]
    with pytest.raises(ValueError):
        asof(left, right.drop("known_ms"))


def test_duration():
    assert duration_ms("6h") == 6 * H and duration_ms("90s") == 90_000 and duration_ms("1.5d") == 36 * H
    with pytest.raises(ValueError):
        duration_ms("6 hours")


# ---- costs · evaluate ----------------------------------------------------------------------------------------------

def test_costs_net_of_the_series_fee(data):
    cat = Catalog(data, WIN, "discovery")
    p = costs.with_fees(panel(cat, SERIES, at="T-3h"), cat)
    lo = p.filter(pl.col("ticker").str.ends_with("-LO")).with_columns(s=costs.sell("bid"), b=costs.buy("ask"))
    fee = 0.07 * 0.08 * 0.92
    assert lo["s"][0] == pytest.approx(0.08 - 0.0 - fee)
    assert lo["b"][0] == pytest.approx(0.0 - 0.10 - 0.07 * 0.10 * 0.90)
    nv = costs.next_volume(cat, p, horizon="2h")
    assert nv["next_volume"].unique().to_list() == [20.0]  # two hourly candles of 10 contracts after t


def _trades(n_events: int, pnl: float, start: int = ms(2025, 1, 1)) -> pl.DataFrame:
    return pl.DataFrame({"event": [f"e{i}" for i in range(n_events) for _ in range(2)],
                         "close_ms": [start + i * 7 * 86_400_000 for i in range(n_events) for _ in range(2)],
                         "pnl": [pnl + (0.01 if j else -0.01) for i in range(n_events) for j in range(2)],
                         "capacity": [5.0] * (2 * n_events)})


def test_evaluate_edge_ci_capacity_and_checks():
    r = evaluate(_trades(40, 0.02), pass_={"min_events": 30, "ci_low_c": 0.0, "recent_quarters_positive": 2,
                                           "min_usd_per_day": 0.02})
    assert r.events == 40 and r.trades == 80 and r.edge_c == pytest.approx(2.0)
    assert r.ci_c[0] <= 2.0 <= r.ci_c[1] and r.p_le0 == 0.0
    assert r.days == 39 * 7 + 1  # calendar span, not just days with trades
    assert r.capacity_per_day == pytest.approx(400 / r.days) and r.usd_per_day == pytest.approx(0.02 * 400 / r.days)
    assert r.passed and [c.name for c in r.checks if c.ok] == ["min_events", "ci_low_c", "recent_quarters_positive",
                                                                  "min_usd_per_day"]
    bad = evaluate(_trades(40, -0.02), pass_={"ci_low_c": 0.0})
    assert bad.passed is False and bad.edge_c == pytest.approx(-2.0)
    with pytest.raises(ValueError, match="missing"):
        evaluate(pl.DataFrame({"event": ["a"], "pnl": [0.1]}))
    with pytest.raises(ValueError, match="unknown pass"):
        evaluate(_trades(3, 0.0), pass_={"sharpe": 1})


def test_evaluate_flags_concentration_and_calibrates():
    t = _trades(10, 0.0).with_columns(pnl=pl.when(pl.col("event") == "e0").then(5.0).otherwise(0.0),
                                      p_model=pl.lit(0.6), p_market=pl.lit(0.5), y=pl.lit(1.0))
    r = evaluate(t)
    assert any("one event" in w for w in r.warnings)
    assert r.calibration.brier_model == pytest.approx(0.16) and r.calibration.brier_market == pytest.approx(0.25)


# ---- specs --------------------------------------------------------------------------------------------------------

SPEC = """+++
series = ["KXTEST"]
variants_max = 4
params = {SIDE}
grid = { band = [[0.05, 0.15], [0.80, 0.95]], at = ["T-3h", "T-6h"] }
pass = { min_events = 5, ci_low_c = 0.0 }
note = "anything else reaches run.py"
+++
# Longshots are overpriced
## Mechanism
Lottery buyers.
"""

RUN = '''
import polars as pl
from pmx.research import costs

def run(ctx, params):
    p = costs.with_fees(ctx.panel(at=params["at"]), ctx.catalog)
    lo, hi = params["band"]
    p = p.filter(pl.col("bid").is_between(lo, hi))
    return p.select(event=pl.col("event_ticker"), close_ms=pl.col("close_ms"), t_ms=pl.col("t_ms"),
                    pnl=costs.sell("bid"))
'''


def test_spec_variants_and_validation():
    s = parse(SPEC.replace("{SIDE}", '{ side = "sell" }'))
    assert s.title == "Longshots are overpriced" and s.config == {"note": "anything else reaches run.py"}
    assert len(s.variants("discovery")) == 4 and s.variants("validation") == [{"side": "sell"}]
    with pytest.raises(ValueError, match="variants_max"):
        parse(SPEC.replace("{SIDE}", "{}").replace("variants_max = 4", "variants_max = 3"))
    with pytest.raises(ValueError, match="unknown pass"):
        parse(SPEC.replace("{SIDE}", "{}").replace("min_events", "sharpe"))
    with pytest.raises(ValueError, match="front matter"):
        parse("# no front matter")
    for bad, msg in (("params = 3", "params"), ("series = [1]", "series"), ("variants_max = 0", "variants_max")):
        with pytest.raises(ValueError, match=msg):
            parse(f"+++\n{bad}\n+++\n# T\n")
    with pytest.raises(ValueError, match="Title"):
        parse("+++\n+++\nno title")


# ---- lifecycle -----------------------------------------------------------------------------------------------------

@pytest.fixture
def ws(tmp_path: Path) -> Workspace:
    w = Workspace(tmp_path / "research")
    w.init()
    (w.root / "windows.toml").write_text("discovery_end = 2026-01-01\nvalidation_end = 2026-07-01\n")
    return w


def _draft(ws: Workspace, slug: str = "longshot", run: str = RUN):
    e = ws.new(slug, today=date(2026, 11, 1))
    e.spec_path.write_text(SPEC.replace("{SIDE}", "{}"))
    e.run_path.write_text(run)
    return e


def test_workspace_init_and_templates(ws):
    for f in ("CLAUDE.md", "AGENDA.md", "FINDINGS.md", "README.md", "windows.toml", "trials.csv"):
        assert (ws.root / f).exists()
    with pytest.raises(ResearchError, match="already"):
        ws.init()
    e = ws.new("weather-longshot", today=date(2026, 11, 1))
    assert e.name == "2026-11-01-weather-longshot" and e.status == "proposed"
    assert "{{" not in e.spec_path.read_text() and e.spec().variants_max == 4  # the template parses as is
    assert ws.find("weather-longshot").name == e.name
    with pytest.raises(ResearchError):
        ws.new("Bad Slug")


def test_lifecycle_locks_stages_and_counts_trials(ws, data):
    e = _draft(ws)
    with pytest.raises(ResearchError, match="needs approval"):
        run_stage(ws, e, data)
    ws.approve(e)
    e.run_path.write_text(RUN + "\n# tweak\n")  # changed after approval
    with pytest.raises(ResearchError, match="changed since discovery"):
        run_stage(ws, e, data)
    ws.amend(e, "comment only")
    runs, text = run_stage(ws, e, data)
    assert len(runs) == 4 and e.status == "discovery" and "Best of 4" in text and "amendments 1" in text
    longshot = next(v for v in runs if v.params == {"band": [0.05, 0.15], "at": "T-3h"})
    assert longshot.result.events == 30 and longshot.result.edge_c > 0  # judged on discovery-era events only
    assert ws.trial_count(e.name) == 4
    run_stage(ws, e, data)  # same lock, same variants: not new trials
    assert ws.trial_count(e.name) == 4 and len(ws.trials()) == 8

    with pytest.raises(ResearchError, match="params"):
        ws.advance(e)  # validation needs the chosen variant
    e.spec_path.write_text(SPEC.replace("{SIDE}", '{ band = [0.05, 0.15], at = "T-3h" }'))
    assert ws.advance(e) == "validation"
    with pytest.raises(ResearchError, match="at validation"):
        run_stage(ws, e, data, stage="discovery")
    (v,), text = run_stage(ws, e, data)
    assert v.result.events == 12 and v.outside == 30  # validation-era events scored; discovery-era rows dropped
    assert v.result.passed and "✓ ci_low_c" in text

    assert ws.advance(e) == "holdout"
    (h,), _ = run_stage(ws, e, data)
    assert h.result.events == 6
    with pytest.raises(ResearchError, match="runs once"):
        run_stage(ws, e, data)
    _, text = run_stage(ws, e, data, force=True)
    assert "no longer out of sample" in text
    assert ws.trial_count(e.name) == 6  # 4 discovery variants + validation + holdout (reruns aren't new trials)

    ws.close(e, "promoted", "sells longshots")
    findings = (ws.root / "FINDINGS.md").read_text()
    assert e.name in findings and "promoted" in findings and "trials: 6" in findings
    with pytest.raises(ResearchError, match="closed"):
        run_stage(ws, e, data)


def test_promotion_needs_a_holdout_and_dead_is_a_result(ws):
    e = _draft(ws)
    ws.approve(e)
    with pytest.raises(ResearchError, match="holdout"):
        ws.close(e, "promoted", "")
    ws.close(e, "dead", "no edge after fees")
    assert e.status == "dead" and "no edge after fees" in (ws.root / "FINDINGS.md").read_text()


def test_run_refuses_lookahead_and_bad_output(ws, data):
    e = _draft(ws, "peek", RUN.replace('t_ms=pl.col("t_ms")', 't_ms=pl.col("close_ms")'))
    ws.approve(e)
    with pytest.raises(ResearchError, match="lookahead"):
        run_stage(ws, e, data)
    e2 = _draft(ws, "nothing", "def run(ctx, params):\n    return 1\n")
    ws.approve(e2)
    with pytest.raises(ResearchError, match="DataFrame"):
        run_stage(ws, e2, data)


def test_amend_needs_a_change_and_a_reason(ws):
    e = _draft(ws)
    with pytest.raises(ResearchError, match="nothing is locked"):
        ws.amend(e, "x")
    ws.approve(e)
    with pytest.raises(ResearchError, match="reason"):
        ws.amend(e, " ")
    with pytest.raises(ResearchError, match="nothing changed"):
        ws.amend(e, "x")
    assert any(p.name.startswith("discovery-") for p in e.locks.iterdir())


def test_approve_and_promote_need_a_terminal(ws, monkeypatch, capsys):
    from pmx.cli import main

    monkeypatch.setenv("PMX_RESEARCH", str(ws.root))
    e = _draft(ws)
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    with pytest.raises(SystemExit, match="in a terminal"):
        main(["research", "approve", e.name])
    assert e.status == "proposed"
    with pytest.raises(SystemExit, match="in a terminal"):
        main(["research", "close", e.name, "--as", "promoted", "--force"])
    assert e.status == "proposed"
    main(["research", "status"])
    assert e.name in capsys.readouterr().out


def test_evaluate_edge_cases():
    zero_qty = _trades(4, 0.01).with_columns(qty=pl.lit(0.0))
    assert all(q.edge_c is None for q in evaluate(zero_qty).quarters)  # no NaN reaches results JSON
    ints = pl.DataFrame({"event": ["a", "b"], "close_ms": [0, 86_400_000], "pnl": [1, -1]})
    assert evaluate(ints).edge_c == 0.0 and evaluate(ints).days == 2
    empty = evaluate(ints.clear(), pass_={"min_events": 1})
    assert (empty.trades, empty.edge_c, empty.ci_c, empty.passed) == (0, None, None, False)
    assert [q.quarter for q in evaluate(_trades(20, 0.01)).quarters] == ["2025Q1", "2025Q2"]
