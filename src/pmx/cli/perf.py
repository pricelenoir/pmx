"""Performance: `pmx perf` and `pmx export --year`."""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

from pmx import config
from pmx.cli.common import die


def _usd(m: int | None, sign: bool = True) -> str:
    if m is None:
        return "-"
    return f"{'+' if sign and m > 0 else '-' if m < 0 else ''}${abs(m) / 1e6:,.2f}"


def _pct(x: float | None) -> str:
    return "-" if x is None else f"{x:.0%}"


def _num(x: float | None, fmt: str = "{:+.2f}") -> str:
    return "-" if x is None else fmt.format(x)


def _ms(day: str | None, *, end: bool = False) -> int | None:
    if not day:
        return None
    from pmx.analytics.stats import TZ

    d = date.fromisoformat(day)
    t = datetime(d.year, d.month, d.day, tzinfo=TZ).timestamp() * 1000
    return int(t + (86_400_000 if end else 0))


def _env(args: argparse.Namespace) -> str:
    return args.env or os.environ.get("KALSHI_ENV") or "prod"


def text(name: str, mode: str, stats, rows, v, rho) -> str:  # noqa: ANN001 - proto structs
    s = stats
    ci = f"[{_usd(s.pnl_ci[0])}, {_usd(s.pnl_ci[1])}]" if s.pnl_ci else "-"
    marks = " · ".join(f"{h} {x:+.2f}¢" for h, x in s.markouts_c.items()) or "-"
    lines = [f"pmx perf — {name}, {mode}", "",
             f"verdict   {v.verdict.upper()}: {v.reason}" + (f"  — {v.needed}" if v.needed else "")]
    lines += [f"          ! {f}" for f in v.flags]
    lines += [
        "",
        f"pnl       net {_usd(s.net)} = realized {_usd(s.realized)} + open {_usd(s.open)} − fees "
        f"{_usd(s.fees, False)}   (95% CI of the total {ci})",
        f"activity  {s.orders} orders · {s.fills} fills · {s.contracts / 100:,.0f} contracts · maker "
        f"{_pct(s.maker_share)} · fill rate {_pct(s.fill_rate)} · {s.episodes} episodes in {s.markets} markets, "
        f"{s.entities} entities",
        f"outcomes  win rate {_pct(s.win_rate)} · avg win {_usd(s.avg_win)} · avg loss {_usd(s.avg_loss)} · "
        f"profit factor {_num(s.profit_factor, '{:.2f}')}",
        f"          expectancy {_num(s.expectancy_c)}¢/contract (P(≤0) {_num(s.p_nonpositive, '{:.0%}')}) · "
        f"best {_usd(s.best_episode)} · worst {_usd(s.worst_episode)}",
        f"edge      at entry {_num(s.edge_entry_c)}¢ · realized {_num(s.edge_realized_c)}¢ · capture "
        f"{_num(s.capture, '{:.2f}')} · markouts {marks} · edge↔result ρ {_num(rho, '{:+.2f}')}",
        f"risk      max drawdown {_usd(s.max_drawdown, False)} · longest under water "
        f"{s.time_under_water_ms / 3_600_000:.1f} h · max capital at risk {_usd(s.max_capital_at_risk, False)} · "
        f"return on risk {_pct(s.return_on_risk)}",
        f"days      {s.days} · Sharpe/day {_num(s.sharpe_day, '{:.2f}')} · positive {_pct(s.pct_days_positive)}",
        f"model     Brier at entry {_num(s.brier_model, '{:.4f}')} vs market {_num(s.brier_market, '{:.4f}')}"]
    for key, label in (("series", "by series"), ("entity", "by entity (top 10)"), ("edge", "by predicted edge"),
                       ("phase", "by phase"), ("day", "by day")):
        if not rows.get(key):
            continue
        lines += ["", f"{label}:"]
        for r in rows[key][:10]:
            edge = f"  edge {r.edge_c:+.2f}¢" if r.edge_c is not None else ""
            lines.append(f"  {r.key[:40]:<40} {r.fills:>5} ep {r.contracts / 100:>8,.0f} ct {_usd(r.pnl):>10} "
                         f"{_num(r.cpc):>7}¢/ct{edge}")
    return "\n".join(lines)


def cmd_perf(args: argparse.Namespace) -> None:
    import msgspec

    from pmx.analytics.breakdowns import breakdowns, edge_spearman
    from pmx.analytics.stats import ReadOnly, compute, load
    from pmx.analytics.verdict import verdict

    path = Path(args.ledger) if args.ledger else config.data_root() / "ledger.sqlite"
    if not path.exists():
        die(f"no ledger at {path}")
    env = _env(args)
    led = ReadOnly(path, env)
    try:
        d = load(led, mode=args.mode, strategy=args.strategy, slot=args.slot, start_ms=_ms(args.from_),
                 end_ms=_ms(args.to, end=True))
        if not d.fills and not d.episodes:
            envs = [r[0] for r in led.db.execute("SELECT DISTINCT env || ' ' || mode FROM fills")]
            die(f"no {args.mode} fills for {env} in that scope (the ledger has: {', '.join(envs) or 'none'})")
        stats = compute(d)
        rows = breakdowns(d)
        v = verdict(stats, rows)
        rho = edge_spearman(d)
    finally:
        led.close()
    if args.json:
        print(msgspec.json.encode({"stats": stats, "breakdowns": rows, "verdict": v, "edge_spearman": rho}).decode())
        return
    print(text(args.slot or args.strategy or "all strategies", f"{env} {args.mode}", stats, rows, v, rho))


def cmd_export(args: argparse.Namespace) -> None:
    """Every fill and settlement of a year, per contract, for taxes and to check against Kalshi's statements."""
    import sqlite3

    path = Path(args.ledger) if args.ledger else config.data_root() / "ledger.sqlite"
    env = _env(args)
    db = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    start, end = _ms(f"{args.year}-01-01"), _ms(f"{args.year}-12-31", end=True)
    out = open(args.out, "w", newline="") if args.out else sys.stdout  # noqa: SIM115
    try:
        w = csv.writer(out)
        w.writerow(["time", "type", "mode", "venue", "ticker", "side", "contracts", "price", "fee", "cash", "maker",
                    "fill_id", "order_id", "slot"])
        rows = []
        for (ts, mode, venue, ticker, side, qty, price, fee, taker, fid, oid, slot) in db.execute(
                "SELECT f.ts_ms, f.mode, f.venue, f.ticker, f.side, f.qty, f.price, f.fee, f.is_taker, f.fill_id, "
                "f.order_id, COALESCE(o.strategy, '') FROM fills f LEFT JOIN orders o "
                "ON o.client_order_id = f.client_order_id WHERE f.env=? AND f.ts_ms >= ? AND f.ts_ms < ?",
                (env, start, end)):
            signed = qty if side == "bid" else -qty
            rows.append((ts, ["fill", mode, venue, ticker, "buy YES" if side == "bid" else "sell YES",
                              f"{qty / 100:.2f}", f"{price / 10_000:.4f}", f"{fee / 1e6:.6f}",
                              f"{(-signed * price - fee) / 1e6:.6f}", "no" if taker else "yes", fid, oid, slot]))
        held: dict[tuple[str, str, str], int] = {}
        for mode, venue, ticker, side, qty in db.execute(
                "SELECT mode, venue, ticker, side, qty FROM fills WHERE env=? AND ts_ms < ?", (env, end)):
            held[(mode, venue, ticker)] = held.get((mode, venue, ticker), 0) + (qty if side == "bid" else -qty)
        for venue, mode, ticker, price, ts in db.execute(
                "SELECT venue, mode, ticker, price, ts_ms FROM settled WHERE env=? AND ts_ms >= ? AND ts_ms < ?",
                (env, start, end)):
            q = held.get((mode, venue, ticker), 0)
            if q:
                rows.append((ts, ["settlement", mode, venue, ticker, "settle", f"{q / 100:.2f}",
                                  f"{price / 10_000:.4f}", "0", f"{q * price / 1e6:.6f}", "", "", "", ""]))
        for ts, row in sorted(rows, key=lambda r: r[0]):
            w.writerow([datetime.fromtimestamp(ts / 1000).isoformat(timespec="seconds"), *row])
    finally:
        if args.out:
            out.close()
        db.close()


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    p = sub.add_parser("perf", parents=[common], help="a strategy's performance: stats, breakdowns, verdict")
    p.add_argument("--strategy", help="a strategy id (default: everything)")
    p.add_argument("--slot", help="one slot (<strategy>/<market>)")
    p.add_argument("--mode", choices=["paper", "live"], default="paper")
    p.add_argument("--from", dest="from_", metavar="YYYY-MM-DD", help="local day (America/New_York)")
    p.add_argument("--to", metavar="YYYY-MM-DD")
    p.add_argument("--ledger", help="default data/ledger.sqlite (a backtest: data/backtests/<run>/ledger.sqlite)")
    p.add_argument("--json", action="store_true")
    e = sub.add_parser("export", parents=[common], help="a year's fills and settlements as CSV (taxes, statements)")
    e.add_argument("--year", type=int, required=True)
    e.add_argument("--ledger", help="default data/ledger.sqlite")
    e.add_argument("--out", help="a CSV file (default stdout)")
    return {"perf": cmd_perf, "export": cmd_export}
