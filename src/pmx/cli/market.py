"""Market data on demand: markets, book, watch."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Callable

from pmx.cli.common import fmt_ts, on_signal, top, venue
from pmx.core.book import BookTracker, OrderBook
from pmx.core.units import cents, fmt_qty, parse_price


async def cmd_markets(args: argparse.Namespace) -> None:
    v = venue(args)
    try:
        ms = await v.rest.markets(series_ticker=args.series, event_ticker=args.event, status=args.status)
        for m in sorted(ms, key=lambda m: (m.close_time or "", m.ticker)):
            bid = cents(parse_price(m.yes_bid_dollars)) if m.yes_bid_dollars else "-"
            ask = cents(parse_price(m.yes_ask_dollars)) if m.yes_ask_dollars else "-"
            close = (m.close_time or "-")[:16].replace("T", " ")
            print(f"{m.ticker:<36} {bid:>6} / {ask:<6} {close:<17} {m.yes_sub_title}")
        print(f"{len(ms)} markets", file=sys.stderr)
    finally:
        await v.aclose()


async def cmd_book(args: argparse.Namespace) -> None:
    v = venue(args)
    try:
        snap = await v.fetch_book(args.ticker)
    finally:
        await v.aclose()
    book = OrderBook(args.ticker)
    book.apply(snap)
    print(f"{args.ticker}   spread {cents(book.spread()) if book.spread() is not None else '-'}")
    for p, q in reversed(book.levels("ask", args.depth)):
        print(f"{'':>12}   {cents(p):>7}   {fmt_qty(q):<12}")
    print(f"{'':>12}   {'-------':>7}")
    for p, q in book.levels("bid", args.depth):
        print(f"{fmt_qty(q):>12}   {cents(p):>7}")


async def cmd_watch(args: argparse.Namespace) -> None:
    v = venue(args)
    stream = v.stream(args.tickers, trades=True)
    decode = v.decoder()
    tracker = BookTracker()
    on_signal(lambda: asyncio.ensure_future(stream.close()))
    try:
        async for item in stream:
            for ev in decode(item):
                book = tracker.on_event(ev)
                if book is not None:
                    print(f"{fmt_ts(book.ts_ms)}  {book.ticker:<36} {top(book)}")
                elif type(ev).__name__ in ("Trade", "StreamReset"):
                    print(f"  {ev}")
    finally:
        await v.aclose()


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    m = sub.add_parser("markets", parents=[common], help="contracts with top of book (REST)")
    m.add_argument("--series")
    m.add_argument("--event")
    m.add_argument("--status", default="open")

    b = sub.add_parser("book", parents=[common], help="one market's order book ladder (REST)")
    b.add_argument("ticker")
    b.add_argument("--depth", type=int, default=10)

    w = sub.add_parser("watch", parents=[common], help="live top of book from the websocket (needs a key)")
    w.add_argument("tickers", nargs="+")
    return {"markets": cmd_markets, "book": cmd_book, "watch": cmd_watch}
