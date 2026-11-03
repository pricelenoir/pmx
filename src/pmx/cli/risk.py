"""Risk: `pmx risk` — the running engine's portfolio from its feed."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable
from datetime import datetime

from pmx.cli import style
from pmx.cli.common import die, socket_for

_MONEY = {"max_loss", "max_capital_at_risk", "max_worst_case", "max_entity_risk", "max_drawdown"}


def _usd(m: int | None, sign: bool = False) -> str:
    return style.usd(m, sign=sign)


def _limit_value(name: str, v: int) -> str:
    if name in _MONEY:
        return f"${v / 1e6:,.2f}"
    return f"{v / 100:g}" if name == "max_position" else str(v)


def _flat(s) -> bool:  # noqa: ANN001 - a proto ScopeStats
    return not (s.equity or s.today or s.capital_at_risk or s.worst_case or s.open_positions or s.drawdown)


def report(hello, msg, mode: str, *, top: int = 10, every: bool = False) -> str:
    """The text `pmx risk` prints for one `portfolio` message; slots with nothing in them only with `every`."""
    when = datetime.fromtimestamp(msg.ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    lines = [style.heading(f"pmx risk · {hello.env} {mode}", when), ""]
    rows = [["scope", "equity", "today", "drawdown", "at risk", "worst case", "EV model", "open"]]
    flat = 0
    for s in msg.scopes:
        if s.mode != mode:
            continue
        if s.scope.startswith("slot:") and _flat(s) and not every:
            flat += 1
            continue
        name = s.scope.removeprefix("slot:")
        name = style.bold(name) if s.scope == "portfolio" else name if s.scope.startswith("strategy:") else \
            "  " + name
        rows.append([name, _usd(s.equity, True), _usd(s.today, True), _usd(s.drawdown), _usd(s.capital_at_risk),
                     _usd(s.worst_case, True), _usd(s.ev_model, True), str(s.open_positions)
                     + (style.yellow("  stale marks") if s.stale else "")])
    lines += style.table(rows, right={1, 2, 3, 4, 5, 6, 7}, indent=0)
    if flat:
        lines.append(style.dim(f"  … {flat} more market{'s' * (flat != 1)} flat (pmx risk --all lists them)"))
    exposures = [e for e in msg.exposures if e.mode == mode]
    lines += ["", style.bold("exposures") + style.dim(f"  worst first, {len(exposures)}")]
    for e in exposures[:top]:
        outcomes = style.dim(" · ").join(f"{k} {_usd(v, True)}" for k, v in e.scenarios.items())
        lines.append(f"  {e.entity}  {style.dim(','.join(e.strategies))}  at risk {_usd(e.capital_at_risk)} · "
                     f"worst {_usd(e.worst_case, True)}   {outcomes}")
    if not exposures:
        lines.append(style.dim("  none"))
    lines += ["", style.bold("limits")]
    rows = []
    for u in msg.limits:
        if u.scope.startswith("slot:") and not u.breached:
            continue  # per slot, only what's breached
        used = f"{_limit_value(u.limit, u.used)} / {_limit_value(u.limit, u.cap)}"
        share = u.used / u.cap if u.cap else 0
        bar = _bar(share)
        rows.append([u.scope, u.limit, used, (style.red(bar + "  BREACHED") if u.breached else
                                               style.yellow(bar) if share >= 0.8 else style.green(bar))])
    lines += style.table(rows, right={2}, header=False) if rows else [style.dim("  none configured")]
    return "\n".join(lines)


def _bar(share: float, width: int = 12) -> str:
    """How much of a limit is used: ▰▰▰▱▱▱…"""
    n = min(width, max(0, round(share * width)))
    return "▰" * n + "▱" * (width - n)


async def cmd_risk(args: argparse.Namespace) -> None:
    from pmx.feed import proto

    sock = socket_for(args)
    try:
        reader, writer = await asyncio.open_unix_connection(str(sock), limit=1 << 26)
    except OSError:
        die(f"pmx is not running on {sock}; start it with `pmx up`")
    hello = None
    try:
        async def first() -> proto.PortfolioMsg | None:
            nonlocal hello
            while line := await reader.readline():
                m = proto.decoder.decode(line)
                if isinstance(m, proto.Hello):
                    hello = m
                elif isinstance(m, proto.PortfolioMsg):
                    return m
            return None

        msg = await asyncio.wait_for(first(), args.wait)
    except TimeoutError:
        die(f"no portfolio from {sock} within {args.wait:g} s; `pmx risk` needs `pmx run` (not `pmx serve`)")
    finally:
        writer.close()
    if msg is None or hello is None:
        die("the feed closed before sending a portfolio")
    print(report(hello, msg, args.mode, every=args.all))


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    r = sub.add_parser("risk", parents=[common], help="the running engine's portfolio: exposures, scenarios, limits")
    r.add_argument("--mode", choices=["paper", "live"], default="paper")
    r.add_argument("--socket", help="default $PMX_SOCK or data/pmx.sock")
    r.add_argument("--wait", type=float, default=5.0, help="seconds to wait for the engine's next snapshot")
    r.add_argument("--all", action="store_true", help="every market, including the ones with nothing in them")
    return {"risk": cmd_risk}
