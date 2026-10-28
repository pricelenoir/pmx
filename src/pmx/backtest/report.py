"""Backtest reports: one screen per run — does each strategy work, per slot and overall, with costs and fill
quality — and run-vs-run comparisons. Every number comes from `analytics/`; this module only reads the run's
metadata and formats. The same report reads a live paper ledger over a date range, so a paper session and its
backtest compare side by side.

A run inside a sweep (`walk_forward`: `<out>/<variant>/train|evaluate`) says how many variants were tried and shows
its train and evaluation windows apart (multiple testing)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import msgspec

from pmx.analytics.breakdowns import breakdowns
from pmx.analytics.stats import ReadOnly, compute, load
from pmx.analytics.verdict import verdict
from pmx.feed.proto import BucketRow, PerfStats, Verdict


@dataclass
class SlotRow:
    slot: str
    net: int
    gross: int
    fees: int
    orders: int
    fills: int
    fill_rate: float | None
    edge_entry_c: float | None
    markouts_c: dict[str, float]
    win_rate: float | None
    max_drawdown: int
    max_position: int  # |signed YES qty|, Qty


@dataclass
class StrategyReport:
    strategy: str
    stats: PerfStats
    verdict: Verdict
    breakdowns: dict[str, list[BucketRow]]
    slots: list[SlotRow]


@dataclass
class Report:
    title: str
    meta: dict[str, Any]
    mode: str
    total: PerfStats
    total_verdict: Verdict
    strategies: list[StrategyReport]
    sweep: dict[str, Any] | None = None
    warnings: list[str] = field(default_factory=list)


def _max_position(led: ReadOnly, env: str, mode: str, slot: str, start_ms: int | None, end_ms: int | None) -> int:
    pos = peak = 0
    q = ("SELECT f.side, f.qty FROM fills f JOIN orders o ON o.client_order_id = f.client_order_id "
         "WHERE f.env=? AND f.mode=? AND o.strategy=?")
    args: list[Any] = [env, mode, slot]
    if start_ms is not None:
        q, args = q + " AND f.ts_ms >= ?", [*args, start_ms]
    if end_ms is not None:
        q, args = q + " AND f.ts_ms < ?", [*args, end_ms]
    for side, qty in led.db.execute(q + " ORDER BY f.ts_ms, f.rowid", args):
        pos += qty if side == "bid" else -qty
        peak = max(peak, abs(pos))
    return peak


def from_ledger(path: Path, *, env: str, mode: str = "paper", start_ms: int | None = None, end_ms: int | None = None,
                title: str = "", meta: dict[str, Any] | None = None) -> Report:
    led = ReadOnly(path, env)
    try:
        slots_by: dict[str, list[str]] = {}
        q = "SELECT DISTINCT o.strategy FROM fills f JOIN orders o ON o.client_order_id = f.client_order_id " \
            "WHERE f.env=? AND f.mode=?"
        for (slot,) in led.db.execute(q, (env, mode)):
            if slot:
                slots_by.setdefault(slot.split("/", 1)[0], []).append(slot)
        out = []
        for sid in sorted(slots_by):
            d = load(led, mode=mode, strategy=sid, start_ms=start_ms, end_ms=end_ms)
            stats = compute(d)
            rows = breakdowns(d)
            slots = []
            for slot in sorted(slots_by[sid]):
                s = compute(load(led, mode=mode, slot=slot, start_ms=start_ms, end_ms=end_ms))
                slots.append(SlotRow(slot, s.net, s.gross, s.fees, s.orders, s.fills, s.fill_rate, s.edge_entry_c,
                                     s.markouts_c, s.win_rate, s.max_drawdown,
                                     _max_position(led, env, mode, slot, start_ms, end_ms)))
            out.append(StrategyReport(sid, stats, verdict(stats, rows), rows, slots))
        dt = load(led, mode=mode, start_ms=start_ms, end_ms=end_ms)
        total = compute(dt)
        cur = led.db.execute("SELECT git_sha, dirty, config_hash FROM runs WHERE env=? ORDER BY started_ms", (env,))
        runs = [dict(zip(("git_sha", "dirty", "config_hash"), r, strict=True)) for r in cur]
    finally:
        led.close()
    meta = dict(meta or {})
    if runs:
        meta.setdefault("git", f"{runs[-1]['git_sha']}{' (uncommitted changes)' if runs[-1]['dirty'] else ''}")
        meta.setdefault("config_hash", runs[-1]["config_hash"])
    return Report(title or path.parent.name, meta, mode, total, verdict(total, breakdowns(dt)), out)


def report(run_dir: Path, *, env: str = "prod") -> Report:
    """A backtest's report, from `run_dir/ledger.sqlite` and its `summary.json`."""
    summary = json.loads((run_dir / "summary.json").read_text()) if (run_dir / "summary.json").exists() else {}
    meta = {k: summary.get(k) for k in ("run_id", "start", "end", "fidelity", "inputs", "fills") if k in summary}
    r = from_ledger(run_dir / "ledger.sqlite", env=env, title=summary.get("run_id", run_dir.name), meta=meta)
    if summary.get("fidelity") == "candle":
        r.warnings.append("candle fidelity: rebuilt from 1-minute history — resting-order fills are approximate")
    r.sweep = _sweep(run_dir, env)
    return r


def _sweep(run_dir: Path, env: str) -> dict[str, Any] | None:
    if run_dir.name not in ("train", "evaluate"):
        return None
    variant, out = run_dir.parent, run_dir.parent.parent
    variants = sorted(p.name for p in out.iterdir() if (p / "train").is_dir() or (p / "evaluate").is_dir())
    other = variant / ("evaluate" if run_dir.name == "train" else "train")
    other_net = None
    if (other / "ledger.sqlite").exists():
        other_net = from_ledger(other / "ledger.sqlite", env=env).total.net
    return {"variant": variant.name, "variants": len(variants), "window": run_dir.name,
            "other_window": other.name, "other_net": other_net}


def write(r: Report, run_dir: Path) -> None:
    (run_dir / "report.json").write_bytes(msgspec.json.format(msgspec.json.encode(r)))
    (run_dir / "report.md").write_text(markdown(r))


def _usd(m: int | None, sign: bool = True) -> str:
    if m is None:
        return "-"
    return f"{'+' if sign and m > 0 else '-' if m < 0 else ''}${abs(m) / 1e6:,.2f}"


def _pct(x: float | None) -> str:
    return "-" if x is None else f"{x:.0%}"


def _c(x: float | None) -> str:
    return "-" if x is None else f"{x:+.2f}¢"


def markdown(r: Report) -> str:
    m = r.meta
    lines = [f"# Backtest report — {r.title}", ""]
    meta = [f"{k.replace('_', ' ')}: {v}" for k, v in m.items() if v is not None]
    lines += [" · ".join(meta), ""] if meta else []
    lines += [f"> {w}" for w in r.warnings] + ([""] if r.warnings else [])
    if r.sweep:
        s = r.sweep
        lines += [f"> Sweep: variant `{s['variant']}`, one of {s['variants']} tried — this is its **{s['window']}** "
                  f"window; its {s['other_window']} window netted {_usd(s['other_net'])}. Judge on the evaluation "
                  "window: the best of many variants on its training window is selection, not skill.", ""]
    t = r.total
    ci = f" (95% CI {_usd(t.pnl_ci[0])} … {_usd(t.pnl_ci[1])}, by game)" if t.pnl_ci else ""
    lines += [f"**Total ({r.mode}):** net {_usd(t.net)}{ci} · gross {_usd(t.gross)} · fees {_usd(t.fees, False)} · "
              f"{t.orders:,} orders · {t.fills:,} fills · max drawdown {_usd(t.max_drawdown, False)}",
              f"**Verdict:** {r.total_verdict.verdict} — {r.total_verdict.reason}", ""]
    lines += ["| strategy | verdict | net | fees | fills | ¢/contract | 95% CI | edge at entry | 1m markout |",
              "|---|---|---:|---:|---:|---:|---|---:|---:|"]
    for s in r.strategies:
        st, ci2 = s.stats, s.stats.expectancy_ci
        lines.append(f"| {s.strategy} | {s.verdict.verdict} | {_usd(st.net)} | {_usd(st.fees, False)} | {st.fills:,} | "
                     f"{_c(st.expectancy_c)} | {f'[{ci2[0]:+.2f}, {ci2[1]:+.2f}]' if ci2 else '-'} | "
                     f"{_c(st.edge_entry_c)} | {_c(st.markouts_c.get('1m'))} |")
    for s in r.strategies:
        lines += ["", f"## {s.strategy}", "", f"{s.verdict.reason}" + (f" — {s.verdict.needed}" if s.verdict.needed
                                                                         else "")]
        lines += [f"- ⚠ {f}" for f in s.verdict.flags]
        lines += ["", "| slot | net | gross | fees | orders | fills | fill rate | edge | markouts 10s/1m/5m "
                      "| win rate | max dd | max position |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|"]
        for x in s.slots[:50]:
            marks = "/".join(_c(x.markouts_c.get(h)) for h in ("10s", "1m", "5m"))
            lines.append(f"| {x.slot} | {_usd(x.net)} | {_usd(x.gross)} | {_usd(x.fees, False)} | {x.orders} | "
                         f"{x.fills} | {_pct(x.fill_rate)} | {_c(x.edge_entry_c)} | {marks} | {_pct(x.win_rate)} | "
                         f"{_usd(x.max_drawdown, False)} | {x.max_position / 100:g} |")
        if len(s.slots) > 50:
            lines.append(f"| … {len(s.slots) - 50} more slots | | | | | | | | | | | |")
    return "\n".join(lines) + "\n"


def compare(a: Report, b: Report) -> str:
    """Headline metrics per strategy, side by side."""
    names = sorted({s.strategy for s in a.strategies} | {s.strategy for s in b.strategies})
    by_a, by_b = {s.strategy: s for s in a.strategies}, {s.strategy: s for s in b.strategies}
    lines = [f"| strategy | metric | {a.title} | {b.title} |", "|---|---|---:|---:|"]
    metrics = (("net", lambda s: _usd(s.stats.net)), ("fees", lambda s: _usd(s.stats.fees, False)),
               ("orders", lambda s: f"{s.stats.orders:,}"), ("fills", lambda s: f"{s.stats.fills:,}"),
               ("¢/contract", lambda s: _c(s.stats.expectancy_c)),
               ("1m markout", lambda s: _c(s.stats.markouts_c.get("1m"))),
               ("max drawdown", lambda s: _usd(s.stats.max_drawdown, False)),
               ("verdict", lambda s: s.verdict.verdict))
    for n in names:
        for label, f in metrics:
            va = f(by_a[n]) if n in by_a else "-"
            vb = f(by_b[n]) if n in by_b else "-"
            lines.append(f"| {n} | {label} | {va} | {vb} |")
    lines.append(f"| **total** | net | {_usd(a.total.net)} | {_usd(b.total.net)} |")
    return "\n".join(lines)


# ---- pmx backtest view -----------------------------------------------------------------------------------------


def view_messages(run_dir: Path, *, env: str = "prod") -> list[Any]:
    """What a viewer of a finished run gets: `hello(source="backtest")`, every scope's equity curve, each strategy's
    performance for the whole run, then `synced` — the TUI's Portfolio and Strategies views, as for a live session."""
    from pmx.analytics.publish import report as perf_report
    from pmx.feed import proto

    summary = json.loads((run_dir / "summary.json").read_text()) if (run_dir / "summary.json").exists() else {}
    led = ReadOnly(run_dir / "ledger.sqlite", env)
    try:
        msgs: list[Any] = [proto.Hello("kalshi", env, "backtest", run=summary.get("run_id", run_dir.name))]
        scopes = [r[0] for r in led.db.execute("SELECT DISTINCT scope FROM equity WHERE env=? AND mode='paper' "
                                               "ORDER BY scope", (env,))]
        last = 0
        for scope in scopes:
            pts, high = [], None
            for ts, eq in led.db.execute("SELECT ts_ms, equity FROM equity WHERE env=? AND mode='paper' AND scope=? "
                                         "ORDER BY ts_ms", (env, scope)):
                high = eq if high is None else max(high, eq)
                pts.append((ts, eq, high - eq))
                last = max(last, ts)
            msgs.append(proto.EquityMsg(scope, "paper", pts, replace=True))
        for (sid,) in led.db.execute("SELECT DISTINCT substr(o.strategy, 1, instr(o.strategy, '/') - 1) FROM fills f "
                                     "JOIN orders o ON o.client_order_id = f.client_order_id WHERE f.env=?", (env,)):
            if sid:
                msgs.append(perf_report(led, sid, "paper", "all", last))
    finally:
        led.close()
    msgs.append(proto.Synced())
    return msgs


async def serve_view(msgs: list[Any], sock: Path, stop: Any) -> None:
    """Serve `msgs` to every viewer that connects, until `stop`."""
    import asyncio

    from pmx.feed import proto

    async def viewer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            for m in msgs:
                writer.write(proto.encode(m))
            await writer.drain()
            await reader.read()  # until the viewer goes
        except ConnectionError:
            pass
        finally:
            writer.close()

    sock.unlink(missing_ok=True)
    server = await asyncio.start_unix_server(viewer, path=str(sock))
    try:
        await stop.wait()
    finally:
        server.close()
        sock.unlink(missing_ok=True)


def local_day_ms(day: str, *, end: bool = False) -> int:
    from pmx.analytics.stats import TZ

    d = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=TZ)
    return int(d.timestamp() * 1000) + (86_400_000 if end else 0)


def stamp(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%MZ")
