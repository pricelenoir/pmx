"""Risk: `pmx risk` — the running engine's portfolio from its feed."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable
from datetime import datetime

from pmx.cli.common import die, socket_for

_MONEY = {"max_loss", "max_capital_at_risk", "max_worst_case", "max_entity_risk", "max_drawdown"}


def _usd(m: int | None, sign: bool = False) -> str:
    if m is None:
        return "-"
    return f"{'+' if sign and m > 0 else '-' if m < 0 else ''}${abs(m) / 1e6:,.2f}"


def _limit_value(name: str, v: int) -> str:
    return _usd(v) if name in _MONEY else f"{v / 100:g}" if name == "max_position" else str(v)


def report(hello, msg, mode: str, *, top: int = 10) -> str:
    """The text `pmx risk` prints for one `portfolio` message."""
    when = datetime.fromtimestamp(msg.ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"pmx risk — {hello.env} {mode}, {when}", "",
             f"{'scope':<44} {'equity':>10} {'today':>10} {'drawdown':>9} {'at risk':>10} {'worst case':>11} "
             f"{'EV model':>10} {'open':>4}"]
    for s in msg.scopes:
        if s.mode != mode:
            continue
        lines.append(f"{s.scope[:44]:<44} {_usd(s.equity, True):>10} {_usd(s.today, True):>10} "
                     f"{_usd(s.drawdown):>9} {_usd(s.capital_at_risk):>10} {_usd(s.worst_case, True):>11} "
                     f"{_usd(s.ev_model, True):>10} {s.open_positions:>4}" + ("  (stale marks)" if s.stale else ""))
    exposures = [e for e in msg.exposures if e.mode == mode]
    lines += ["", f"exposures, worst first ({len(exposures)}):"]
    for e in exposures[:top]:
        outcomes = " · ".join(f"{k} {_usd(v, True)}" for k, v in e.scenarios.items())
        lines.append(f"  {e.entity[:40]:<40} {','.join(e.strategies)[:16]:<16} at risk {_usd(e.capital_at_risk):>9}  "
                     f"worst {_usd(e.worst_case, True):>9}   {outcomes}")
    if not exposures:
        lines.append("  none")
    lines += ["", "limits:"]
    for u in msg.limits:
        if u.scope.startswith("slot:") and not u.breached:
            continue  # per slot, only what's breached
        flag = "  BREACHED" if u.breached else ""
        lines.append(f"  {u.scope[:40]:<40} {u.limit:<20} {_limit_value(u.limit, u.used):>10} / "
                     f"{_limit_value(u.limit, u.cap):<10}{flag}")
    if not msg.limits:
        lines.append("  none configured")
    return "\n".join(lines)


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
    print(report(hello, msg, args.mode))


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    r = sub.add_parser("risk", parents=[common], help="the running engine's portfolio: exposures, scenarios, limits")
    r.add_argument("--mode", choices=["paper", "live"], default="paper")
    r.add_argument("--socket", help="default $PMX_SOCK or data/pmx.sock")
    r.add_argument("--wait", type=float, default=5.0, help="seconds to wait for the engine's next snapshot")
    return {"risk": cmd_risk}
