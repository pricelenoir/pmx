"""Shared by every command module: the `--env/-v` parent parser, the venue, signals, formatting."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn

from pmx import config, lifecycle
from pmx.connectors.kalshi.config import KalshiConfig
from pmx.connectors.kalshi.venue import KalshiVenue
from pmx.core.book import OrderBook
from pmx.core.units import cents, fmt_qty


def die(message: str) -> NoReturn:
    """Exit with status 1 and `error: <message>` on stderr (what went wrong; how to fix it after a semicolon)."""
    sys.exit(f"error: {message}")


def common_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--env", choices=["demo", "prod"], help="Kalshi environment (default $KALSHI_ENV or demo)")
    common.add_argument("-v", "--verbose", action="store_true")
    return common


def engine_flags() -> argparse.ArgumentParser:
    """What the engine runs (`run`, `serve`, `up`, `tui`)."""
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--config", help="default config/strategies.toml")
    p.add_argument("--source", choices=["live", "poll"],
                   help="market data: default live with an API key, else REST poll")
    p.add_argument("--series", nargs="+", help="stream these series instead of the config's [watch]")
    p.add_argument("--tickers", nargs="+")
    p.add_argument("--poll", type=float, default=None, help="REST poll: tape interval, seconds (default 2)")
    p.add_argument("--models", help="comma-separated model kinds to add, e.g. market_mid,espn_wp")
    return p


def socket_for(args: argparse.Namespace) -> Path:
    if getattr(args, "socket", None):
        return Path(args.socket)
    # The demo gets its own socket so it can never be mistaken for a real feed.
    return config.data_root() / "pmx-demo.sock" if getattr(args, "demo", False) else config.socket_path()


@contextlib.contextmanager
def engine_pidfile(sock: Path) -> Iterator[None]:
    """This process is the engine: it holds the pidfile; its feed process serves `sock`."""
    try:
        with lifecycle.pidfile(lifecycle.pidfile_for(sock)):
            yield
    finally:
        sock.unlink(missing_ok=True)


def venue(args: argparse.Namespace) -> KalshiVenue:
    return KalshiVenue(KalshiConfig.from_env(args.env))


def on_signal(fn) -> None:
    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        loop.add_signal_handler(s, fn)


def fmt_ts(ms: int | None) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d %H:%MZ") if ms else "-"


def top(book: OrderBook) -> str:
    b, a = book.best_bid(), book.best_ask()
    bid = f"{fmt_qty(b[1]):>10} @ {cents(b[0]):>6}" if b else f"{'-':>19}"
    ask = f"{cents(a[0]):<6} x {fmt_qty(a[1]):<10}" if a else "-"
    return f"{bid}  |  {ask}"
