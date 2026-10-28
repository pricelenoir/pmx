"""Control channel: `pmx order`, `cancel`, `status`, `positions`, `advise` against the running engine (`pmx halt` /
`resume` live in `cli/run.py`: the kill-switch file, applied at once through the socket when it answers)."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import msgspec

from pmx.cli.common import die, socket_for
from pmx.control import protocol
from pmx.control.client import NotRunning, call
from pmx.control.server import control_socket
from pmx.core.units import cents, fmt_qty, parse_price, parse_qty

_DURATION = re.compile(r"(\d+(?:\.\d+)?)\s*(s|m|h)?")


def parse_order_price(text: str) -> int:
    """`0.40`, `$0.40`, `40c`, or `40¢` → price units (1e-4 $)."""
    t = text.strip().lstrip("$")
    try:
        if t.endswith(("c", "¢")):
            return parse_price(str(Decimal(t[:-1]) / 100))
        return parse_price(t)
    except (InvalidOperation, ValueError):
        die(f"price {text!r}: give dollars (0.40) or cents (40c)")


def parse_duration_s(text: str) -> float:
    m = _DURATION.fullmatch(text.strip())
    if not m:
        die(f"duration {text!r}: use e.g. 90s, 30m, 2h")
    return float(m[1]) * {"s": 1, "m": 60, "h": 3600}[m[2] or "s"]


def _sock(args: argparse.Namespace) -> Path:
    return control_socket(socket_for(args))


def _call(args: argparse.Namespace, req: protocol.Request) -> protocol.Response:
    try:
        return call(_sock(args), req)
    except NotRunning as e:
        die(str(e))


def _fmt_order(o: dict[str, Any]) -> str:
    reason = f" ({o['reason']})" if o.get("reason") else ""
    return (f"  {o['status']:<9} {o['client_order_id']}  {o['side']} {fmt_qty(o['qty'])} @ {cents(o['price'])}  "
            f"filled {fmt_qty(o['filled'])}{reason}")


def report(resp: protocol.Response) -> str:
    lines = ["ok" if resp.ok else f"refused: {resp.error}"]
    if resp.ok and resp.error:
        lines.append(resp.error)
    if resp.orders:
        latest: dict[str, dict] = {}
        for o in resp.orders:
            latest[o["client_order_id"]] = o
        lines += [_fmt_order(o) for o in latest.values()]
    for f in resp.fills:
        lines.append(f"  fill {f['side']} {fmt_qty(f['qty'])} @ {cents(f['price'])} "
                     f"({'taker' if f['is_taker'] else 'maker'}, fee ${f['fee'] / 1e6:.4f})")
    for r in resp.rejects:
        lines.append(f"  reject {r['rule']}: {r['detail']}")
    return "\n".join(lines)


def cmd_order(args: argparse.Namespace) -> None:
    parts = [p for p in args.price if p != "@"]
    if len(parts) != 1:
        die("usage: pmx order buy|sell TICKER QTY @ PRICE")
    req = protocol.Order(args.ticker, "bid" if args.side == "buy" else "ask", parse_order_price(parts[0]),
                         parse_qty(args.qty), args.mode, "ioc" if args.ioc else "gtc", args.post_only)
    if args.mode == "live":
        st = _call(args, protocol.Status())
        env = st.data.get("env", "?")
        if not st.data.get("live"):
            die("the engine was not started with --live")
        print(f"LIVE on Kalshi {env}: {args.side} {args.qty} {args.ticker} @ {cents(req.price)} "
              f"({'IOC' if args.ioc else 'GTC'}{', post-only' if args.post_only else ''})")
        if args.yes:
            if env == "prod":
                die("--yes is not accepted on prod; confirm by typing yes")
        elif not sys.stdin.isatty() or input("type yes to send it: ").strip() != "yes":
            die("order not sent")
        req = msgspec.structs.replace(req, confirmed=True)
    resp = _call(args, req)
    print(report(resp))
    if not resp.ok:
        sys.exit(1)


def cmd_cancel(args: argparse.Namespace) -> None:
    if not (args.order_id or args.ticker or args.all):
        die("say what to cancel: an order ID, --ticker TICKER, or --all (manual orders)")
    resp = _call(args, protocol.Cancel(args.order_id or "", args.ticker or "", args.all, args.mode))
    print(report(resp))
    if resp.ok and not resp.orders:
        print("  nothing working to cancel")


def _usd(m: int, sign: bool = True) -> str:
    return f"{'+' if sign and m > 0 else '-' if m < 0 else ''}${abs(m) / 1e6:,.2f}"


def cmd_status(args: argparse.Namespace) -> None:
    try:
        d = call(_sock(args), protocol.Status(), timeout=5.0).data
    except (NotRunning, TimeoutError) as e:
        print(f"the engine is not responding ({e})")
        _status_from_files(args)
        return
    print(_status_text(d))
    _status_from_files(args, engine=False)


def _status_from_files(args: argparse.Namespace, *, engine: bool = True) -> None:
    """What the health files last said: the engine's when it doesn't answer, and the recorder's."""
    import time

    from pmx import config
    from pmx.ops.health import health_path, read

    files = ([health_path(socket_for(args))] if engine else []) + [config.data_root() / "health-record.json"]
    for path in files:
        h = read(path)
        if h is None:
            continue
        age = time.time() - h["ts_ms"] / 1000
        print(f"\n{path.name}, {age:.0f} s old" + (" (stale: the process may have stopped)" if age > 60 else ""))
        if h.get("kind") == "run":
            print(_status_text(h))
        else:
            for name, s in h.get("sources", {}).items():
                last = f"{(time.time() * 1000 - s['last_ms']) / 1000:.0f} s ago" if s.get("last_ms") else "never"
                print(f"  {name:<14} {s['records']:>10,} records, last {last}")
            print(f"  sleep gaps {h.get('sleep_gaps', 0)}, disk free {h.get('disk_free_gb', '?')} GB")


def _status_text(d: dict[str, Any]) -> str:
    started = datetime.fromtimestamp(d["started_ms"] / 1000).strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"pmx {d['env']}{' LIVE' if d['live'] else ''} — run {d['run_id']}, since {started}; "
             f"{d['dispatched']:,} events dispatched, {d['queue_depth']} queued", "", "sources"]
    lines += [f"  {'up  ' if s['connected'] else '?   ' if s['connected'] is None else 'DOWN'} {name:<12} {s['detail']}"
              for name, s in d["sources"].items()]
    lines += ["", "slots    " + (" · ".join(f"{k} {v}" for k, v in sorted(d["slots"].items())) or "none"),
              "orders   " + (" · ".join(f"{k} {v} working" for k, v in sorted(d["working_orders"].items())) or "none"),
              "halts    " + (" · ".join(f"{k or 'everything'}: {v}" for k, v in d["halts"].items()) or "none"),
              "pnl      " + " · ".join(f"{m} equity {_usd(p['equity'])} (today {_usd(p['today'])})"
                                       for m, p in d["pnl"].items())]
    if d["manual_orders"]:
        lines += ["", "manual orders"] + [f"  {o['mode']:<5} {o['status']:<8} {o['client_order_id']}  {o['slot']}  "
                                          f"{o['side']} {fmt_qty(o['qty'])} @ {cents(o['price'])}  filled "
                                          f"{fmt_qty(o['filled'])}" for o in d["manual_orders"]]
    return "\n".join(lines)


def cmd_positions(args: argparse.Namespace) -> None:
    rows = _call(args, protocol.Positions(args.mode)).data["positions"]
    if not rows:
        print("no open positions")
        return
    print(f"{'mode':<6} {'slot':<46} {'qty':>8} {'avg':>7} {'realized':>10} {'fees':>8}")
    for r in rows:
        avg = r["cost"] / r["qty"] / 100 if r["qty"] else 0  # micros per 1e-2 contract → ¢
        print(f"{r['mode']:<6} {r['slot'][:46]:<46} {fmt_qty(r['qty']):>8} {avg:>6.2f}¢ {_usd(r['realized']):>10} "
              f"{_usd(r['fees'], False):>8}")


def cmd_advise(args: argparse.Namespace) -> None:
    req = protocol.Advise(args.target, args.p, args.direction, args.confidence, parse_duration_s(args.expires),
                          args.note or "")
    resp = _call(args, req)
    if not resp.ok:
        die(f"refused: {resp.error}")
    a = resp.data["advice"]
    what = f"p {a['p']:.3f}" if a["p"] is not None else a["direction"]
    print(f"advice sent: {a['target']} {what} (confidence {a['confidence']:g}, expires in {args.expires}); "
          "strategies that list `manual` receive it")


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    target = argparse.ArgumentParser(add_help=False)
    target.add_argument("--socket", help=argparse.SUPPRESS)
    target.add_argument("--demo", action="store_true", help=argparse.SUPPRESS)
    o = sub.add_parser("order", parents=[common, target], help="place an order by hand (slot manual/<ticker>)")
    o.add_argument("side", choices=["buy", "sell"], help="buy = bid YES, sell = ask YES")
    o.add_argument("ticker")
    o.add_argument("qty", help="contracts")
    o.add_argument("price", nargs="+", metavar="@ PRICE", help="dollars (0.40) or cents (40c)")
    o.add_argument("--mode", choices=["paper", "live"], default="paper")
    o.add_argument("--ioc", action="store_true", help="immediate-or-cancel (default: rests, good till canceled)")
    o.add_argument("--post-only", action="store_true")
    o.add_argument("--yes", action="store_true", help="live without the typed confirmation (refused on prod)")
    c = sub.add_parser("cancel", parents=[common, target], help="cancel an order, or manual orders")
    c.add_argument("order_id", nargs="?", help="a client order id (any slot)")
    c.add_argument("--ticker", help="manual orders on this market")
    c.add_argument("--all", action="store_true", help="every manual order")
    c.add_argument("--mode", choices=["paper", "live"])
    sub.add_parser("status", parents=[common, target], help="the running engine: sources, slots, orders, halts, PnL")
    p = sub.add_parser("positions", parents=[common, target], help="open positions per slot")
    p.add_argument("--mode", choices=["paper", "live"])
    a = sub.add_parser("advise", parents=[common, target], help="manual advice for strategies that list `manual`")
    a.add_argument("target", help="a ticker, or an entity key")
    g = a.add_mutually_exclusive_group(required=True)
    g.add_argument("--p", type=float, help="P(YES)")
    g.add_argument("--direction", choices=["buy", "sell", "flat"])
    a.add_argument("--confidence", type=float, default=1.0)
    a.add_argument("--expires", default="30m", help="e.g. 90s, 30m, 2h")
    a.add_argument("--note")
    return {"order": cmd_order, "cancel": cmd_cancel, "status": cmd_status, "positions": cmd_positions,
            "advise": cmd_advise}
