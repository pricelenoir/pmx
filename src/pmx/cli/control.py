"""Control channel: `pmx order`, `cancel`, `status`, `positions`, `advise` against the running engine (`pmx halt` /
`resume` live in `cli/run.py`: the kill-switch file, applied at once through the socket when it answers)."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import msgspec

from pmx.cli import style
from pmx.cli.common import die, socket_for
from pmx.control import protocol
from pmx.control.client import NotRunning, call
from pmx.control.server import control_socket
from pmx.core.units import cents, parse_price, parse_qty

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


def _qty(q: int) -> str:
    return f"{q / 100:,.2f}".rstrip("0").rstrip(".")


def _side(side: str) -> str:
    return "buy" if side == "bid" else "sell"


def _fmt_order(o: dict[str, Any]) -> str:
    reason = style.dim(f"  ({o['reason']})") if o.get("reason") else ""
    return (f"  {style.dot(o['status'])} {style.pad(style.word(o['status']), 9)} "
            f"{_side(o['side'])} {_qty(o['qty'])} @ {cents(o['price'])}  {style.dim('filled')} {_qty(o['filled'])}  "
            f"{style.dim(o['client_order_id'])}{reason}")


def report(resp: protocol.Response, done: str = "done") -> str:
    lines = [f"{style.mark(True)} {done}" if resp.ok else f"{style.mark(False)} {style.red('refused')}: {resp.error}"]
    if resp.ok and resp.error:
        lines.append(resp.error)
    if resp.orders:
        latest: dict[str, dict] = {}
        for o in resp.orders:
            latest[o["client_order_id"]] = o
        lines += [_fmt_order(o) for o in latest.values()]
    for f in resp.fills:
        how = f"{'taker' if f['is_taker'] else 'maker'}, fee ${f['fee'] / 1e6:.4f}"
        lines.append(f"  {style.green('▸ filled')} {_side(f['side'])} {_qty(f['qty'])} @ {cents(f['price'])}  "
                     f"{style.dim(how)}")
    for r in resp.rejects:
        lines.append(f"  {style.red('✗ rejected')} {r['rule']}: {r['detail']}")
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
    print(report(resp, "order sent"))
    if not resp.ok:
        sys.exit(1)


def cmd_cancel(args: argparse.Namespace) -> None:
    if not (args.order_id or args.ticker or args.all):
        die("say what to cancel: an order ID, --ticker TICKER, or --all (manual orders)")
    resp = _call(args, protocol.Cancel(args.order_id or "", args.ticker or "", args.all, args.mode))
    if resp.ok and not resp.orders:
        print(style.dim("nothing working to cancel"))
        return
    n = len({o["client_order_id"] for o in resp.orders})
    print(report(resp, f"canceled {n} order{'s' * (n != 1)}"))


def _usd(m: int, sign: bool = True) -> str:
    return style.usd(m, sign=sign)


def _ago(ms: float) -> str:
    s = max(0, ms / 1000)
    return f"{s:.0f}s" if s < 90 else f"{s / 60:.0f}m" if s < 5400 else f"{s / 3600:.1f}h" if s < 172_800 else \
        f"{s / 86_400:.0f}d"


def cmd_status(args: argparse.Namespace) -> None:
    try:
        d = call(_sock(args), protocol.Status(), timeout=5.0).data
    except (NotRunning, TimeoutError):
        hint = style.dim("start it: pmx up --env …")
        print(f"{style.dot('idle')} {style.yellow("the engine isn't running")}  {hint}")
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
        age = time.time() * 1000 - h["ts_ms"]
        stale = age > 60_000
        what = "the engine" if h.get("kind") == "run" else "the recorder"
        print("\n" + style.heading(f"{what}, last report {_ago(age)} ago", path.name) +
              (f"  {style.yellow('stale: it may have stopped')}" if stale else ""))
        if h.get("kind") == "run":
            print(_status_text(h))
        else:
            rows = [[f"{style.dot('down' if stale else 'up')} {name}", f"{s['records']:,} records",
                     style.dim(f"last {_ago(time.time() * 1000 - s['last_ms'])} ago" if s.get("last_ms") else "never")]
                    for name, s in h.get("sources", {}).items()]
            print("\n".join(style.table(rows, right={1}, header=False)))
            print(style.dim(f"  sleep gaps {h.get('sleep_gaps', 0)} · disk free {h.get('disk_free_gb', '?')} GB"))


def _status_text(d: dict[str, Any]) -> str:
    import time

    up = _ago(time.time() * 1000 - d["started_ms"])
    mode = style.red(style.bold("LIVE")) if d["live"] else style.dim("paper only")
    lines = [f"{style.dot('running')} {style.bold('pmx')} {style.cyan(d['env'])} {mode}   "
             + style.dim(f"run {d['run_id']} · up {up} · {d['dispatched']:,} events"
                         + (f" · {d['queue_depth']} queued" if d["queue_depth"] else "")), "", style.bold("sources")]
    rows = [[f"{style.dot({True: 'up', False: 'down'}.get(s['connected'], 'unknown'))} {name}",
             s["detail"] if s["connected"] is not False else style.red(s["detail"] or "down")]
            for name, s in d["sources"].items()]
    lines += style.table(rows, header=False)
    slots = " · ".join(f"{v} {k}" for k, v in sorted(d["slots"].items()))
    orders = " · ".join(f"{v} {k} working" for k, v in sorted(d["working_orders"].items()))
    halts = " · ".join(style.yellow(f"⏸ {k or 'everything'}: {v}") for k, v in d["halts"].items())
    pnl = "   ".join(f"{m} {_usd(p['equity'])} {style.dim('today')} {_usd(p['today'])}" for m, p in d["pnl"].items())
    lines += ["",
              f"{style.bold('slots ')}   {slots or style.dim('none')}",
              f"{style.bold('orders')}   {orders or style.dim('none working')}",
              f"{style.bold('halts ')}   {halts or style.dim('none')}",
              f"{style.bold('pnl   ')}   {pnl}"]
    if d["manual_orders"]:
        lines += ["", style.bold("manual orders")] + [
            f"  {style.dot(o['status'])} {o['mode']:<5} {_side(o['side'])} {_qty(o['qty'])} @ {cents(o['price'])}  "
            f"{style.dim('filled')} {_qty(o['filled'])}  {style.dim(o['slot'])}" for o in d["manual_orders"]]
    return "\n".join(lines)


def cmd_positions(args: argparse.Namespace) -> None:
    rows = _call(args, protocol.Positions(args.mode)).data["positions"]
    if not rows:
        print(style.dim("no open positions"))
        return
    table = [["mode", "strategy / market", "qty", "avg", "realized", "fees"]]
    for r in rows:
        avg = r["cost"] / r["qty"] / 100 if r["qty"] else 0  # micros per 1e-2 contract → ¢
        qty = _qty(r["qty"])
        table.append([style.red(r["mode"]) if r["mode"] == "live" else r["mode"], r["slot"],
                      style.green(qty) if r["qty"] > 0 else style.red(qty), f"{avg:.2f}¢", _usd(r["realized"]),
                      style.dim(_usd(r["fees"], False))])
    print("\n".join(style.table(table, right={2, 3, 4, 5}, indent=0)))


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
