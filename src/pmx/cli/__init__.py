"""pmx — prediction-market trading: market data, recording, the engine (strategies, risk, paper and live
brokers), backtests, and the dashboard.

  pmx help CMD      one command's options (or pmx CMD -h)
  pmx help --all    every command's options
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
from collections.abc import Callable

from pmx import config
from pmx.cli import account, backtest, bench, control, data, feed, market, ops, perf, record, risk, run
from pmx.cli.common import common_parser

# Each area module owns its commands: `register(sub, common) -> {name: fn}`. Order = `pmx --help` order.
AREAS = (market, record, account, feed, data, run, backtest, risk, perf, ops, control, bench)


def _build() -> tuple[argparse.ArgumentParser, dict[str, Callable]]:
    p = argparse.ArgumentParser(prog="pmx", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True, title="commands", metavar="COMMAND")
    common = common_parser()
    commands: dict[str, Callable] = {}
    for area in AREAS:
        commands |= area.register(sub, common)
    h = sub.add_parser("help", parents=[common], help="this list; pmx help CMD: one command; --all: every command")
    h.add_argument("command", nargs="*", help="a command (and subcommand), e.g. backtest, bench feed")
    h.add_argument("--all", action="store_true", help="every command's options")
    commands["help"] = lambda args: print(_help(p, args.command, every=args.all), end="")
    return p, commands


def _parsers(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    sub = next((a for a in parser._actions if isinstance(a, argparse._SubParsersAction)), None)
    return dict(sub.choices) if sub is not None else {}


def _help(top: argparse.ArgumentParser, path: list[str], *, every: bool = False) -> str:
    """`pmx help [CMD [SUB]]`: that parser's help; `every`: it and everything under it, depth first."""
    parser = top
    for name in path:
        parser = _parsers(parser).get(name) or top.error(f"no command {' '.join(path)!r} (pmx help)")
    if not every:
        return parser.format_help()
    out, todo = [], [parser]
    while todo:
        q = todo.pop(0)
        out.append(q.format_help())
        todo[:0] = [c for n, c in _parsers(q).items() if n != "help"]
    rule = "\n" + "─" * 80 + "\n\n"
    return rule.join(out)


def build_parser() -> argparse.ArgumentParser:
    return _build()[0]


COMMANDS = _build()[1]


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    config.load_env()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.DEBUG if args.verbose else logging.WARNING)
    fn = COMMANDS[args.cmd]
    if inspect.iscoroutinefunction(fn):
        asyncio.run(fn(args))
    else:
        fn(args)
