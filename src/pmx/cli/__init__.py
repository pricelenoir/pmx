"""pmx — trade prediction markets (Kalshi): record data, research ideas, paper trade, trade live.

  pmx help            where to start, by task
  pmx help GUIDE      a walkthrough: start, run, trade, research, server, live, …
  pmx help COMMAND    what a command does, its options, examples (same as pmx COMMAND -h)
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import inspect
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
from collections.abc import Callable

from pmx import config
from pmx.cli import (
    account,
    backtest,
    bench,
    control,
    data,
    feed,
    manual,
    market,
    ops,
    perf,
    record,
    remote,
    research,
    risk,
    run,
)
from pmx.cli.common import common_parser

# Each area module owns its commands: `register(sub, common) -> {name: fn}`. Order = `pmx --help` order.
AREAS = (market, record, account, feed, data, research, run, backtest, risk, perf, ops, remote, control, bench)


def _build() -> tuple[argparse.ArgumentParser, dict[str, Callable]]:
    p = argparse.ArgumentParser(prog="pmx", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", title="commands", metavar="COMMAND")
    common = common_parser()
    commands: dict[str, Callable] = {}
    for area in AREAS:
        commands |= area.register(sub, common)
    h = sub.add_parser("help", parents=[common], help="where to start; pmx help GUIDE or COMMAND; --all")
    h.add_argument("command", nargs="*", help="a guide (start, run, live, …) or a command (and subcommand)")
    h.add_argument("--all", action="store_true", help="every command's options")
    commands["help"] = lambda args: _page(_help(p, args.command, every=args.all))
    blurbs = {a.dest: a.help for a in sub._choices_actions}
    for name, cp in _parsers(p).items():  # `pmx CMD -h`: what it does, options, examples, the guide
        cp.formatter_class = argparse.RawDescriptionHelpFormatter
        cp.description = cp.description or blurbs.get(name)
        cp.epilog = _epilog(name)
    return p, commands


def _epilog(name: str) -> str:
    out = []
    if name in manual.EXAMPLES:
        out.append("examples:\n" + "\n".join("  " + line for line in manual.EXAMPLES[name].splitlines()))
    if name in manual.SEE_ALSO:
        out.append(f"guide: pmx help {manual.SEE_ALSO[name]}")
    return "\n\n".join(out)


def _parsers(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    sub = next((a for a in parser._actions if isinstance(a, argparse._SubParsersAction)), None)
    return dict(sub.choices) if sub is not None else {}


def _help(top: argparse.ArgumentParser, path: list[str], *, every: bool = False) -> str:
    """`pmx help`: the overview; `pmx help GUIDE`: a guide; `pmx help CMD [SUB]`: that command's page;
    `every`: every command's options."""
    if not path and not every:
        return manual.OVERVIEW
    if len(path) == 1 and path[0] in manual.GUIDES and path[0] not in _parsers(top):
        return manual.GUIDES[path[0]][1]
    parser = top
    for name in path:
        nxt = _parsers(parser).get(name)
        if nxt is None:
            known = [*manual.GUIDES, *_parsers(top)]
            close = difflib.get_close_matches(name, known, n=3)
            hint = f"; did you mean {' or '.join(close)}?" if close else ""
            top.exit(2, f"pmx help: no guide or command {' '.join(path)!r}{hint} (pmx help lists them)\n")
        parser = nxt
    if not every:
        return parser.format_help()
    out, todo = [], [parser]
    while todo:
        q = todo.pop(0)
        out.append(q.format_help())
        todo[:0] = [c for n, c in _parsers(q).items() if n != "help"]
    rule = "\n" + "─" * 80 + "\n\n"
    return rule.join(out)


_HEADING = re.compile(r"^(?:[A-Z][A-Z0-9 ()/,'-]+|[a-z]+:)$")


def _page(text: str) -> None:
    """Print, through a pager (like man) when it doesn't fit the terminal; headings in bold on a terminal."""
    if not sys.stdout.isatty():
        print(text, end="")
        return
    text = "\n".join(f"\033[1m{ln}\033[0m" if _HEADING.match(ln.strip()) and ln.strip() else ln
                     for ln in text.splitlines()) + "\n"
    rows = shutil.get_terminal_size().lines
    pager = os.environ.get("PAGER") or ("less" if shutil.which("less") else "")
    if text.count("\n") < rows - 1 or not pager:
        print(text, end="")
        return
    env = os.environ | ({"LESS": "FRX"} if "LESS" not in os.environ else {})
    try:
        subprocess.run(shlex.split(pager), input=text, text=True, env=env, check=False)
    except (OSError, KeyboardInterrupt):
        print(text, end="")


def build_parser() -> argparse.ArgumentParser:
    return _build()[0]


COMMANDS = _build()[1]


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    words = sys.argv[1:] if argv is None else argv
    if words and not words[0].startswith("-") and words[0] not in COMMANDS:  # a typo: suggest, don't list all 34
        close = [f"pmx {c}" for c in difflib.get_close_matches(words[0], list(COMMANDS), n=2)] or \
            [f"pmx help {g}" for g in difflib.get_close_matches(words[0], list(manual.GUIDES), n=1)]
        hint = f"; did you mean {' or '.join(close)}?" if close else ""
        parser.exit(2, f"pmx: no command {words[0]!r}{hint} (pmx help lists them)\n")
    args = parser.parse_args(argv)
    if args.cmd is None:  # plain `pmx`: where to start
        _page(_help(parser, []))
        return
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
