"""A counting viewer for `pmx bench feed`, run as its own process like a real one:
`python -m pmx.bench.viewer SOCK --seconds S --warmup W` prints one JSON line of what it received
after the first `W` seconds — messages and bytes per second by tag, and the busiest market's
`book` rate, split by whether a slot trades it."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter


async def watch(sock: str, seconds: float, warmup: float) -> dict:
    t0 = time.monotonic()
    while True:
        try:
            reader, writer = await asyncio.open_unix_connection(sock, limit=1 << 24)
            break
        except OSError:
            if time.monotonic() - t0 > 10:
                raise
            await asyncio.sleep(0.05)
    tags: Counter[str] = Counter()
    books: Counter[str] = Counter()
    traded: set[str] = set()
    nbytes, start, end = 0, t0 + warmup, t0 + warmup + seconds
    hellos = 0
    while (now := time.monotonic()) < end:
        try:
            line = await asyncio.wait_for(reader.readline(), end - now)
        except TimeoutError:
            break
        if not line:
            break
        m = json.loads(line)
        t = m["t"]
        hellos += t == "hello"
        if t == "slot":
            traded.add(m["ticker"])
        if now < start:  # the snapshot and warm-up don't count
            continue
        tags[t] += 1
        nbytes += len(line)
        if t == "book":
            books[m["ticker"]] += 1
    writer.close()
    span = max(1e-9, min(time.monotonic(), end) - start)
    quiet = [n for t, n in books.items() if t not in traded]
    busy = [n for t, n in books.items() if t in traded]
    return {
        "seconds": round(span, 2), "msgs_per_s": round(sum(tags.values()) / span), "bytes_per_s": round(nbytes / span),
        "by_tag_per_s": {k: round(v / span, 1) for k, v in tags.most_common()},
        "markets_with_books": len(books),
        "max_books_per_s": round(max(quiet, default=0) / span, 2),
        "max_books_per_s_traded": round(max(busy, default=0) / span, 2),
        "hellos": hellos,
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="pmx.bench.viewer")
    p.add_argument("socket")
    p.add_argument("--seconds", type=float, default=10.0)
    p.add_argument("--warmup", type=float, default=2.0)
    a = p.parse_args(argv)
    json.dump(asyncio.run(watch(a.socket, a.seconds, a.warmup)), sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
