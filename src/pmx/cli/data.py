"""Data sync: `pmx data kalshi` (any series, for research), `pmx data kalshi-history` (NFL games),
`pmx data nfl` (nflverse play-by-play)."""

from __future__ import annotations

import argparse
from collections.abc import Callable

from pmx import config


async def cmd_data(args: argparse.Namespace) -> None:
    if args.what == "nfl":
        from pmx.sports.nfl.data import download

        seasons = [int(s) for s in args.seasons.split(",")]
        current = max(seasons)  # the season in progress grows each week: always fetched again
        for path in download(config.data_root(), [s for s in seasons if s != current], force=args.force) + \
                download(config.data_root(), [current], force=True):
            print(f"  {path}")
        return
    if args.what == "kalshi":
        await _kalshi(args)
        return
    import polars as pl

    from pmx.cli.common import die, venue
    from pmx.connectors.kalshi import history
    from pmx.harness import kalshi_history as kh

    args.series = args.series[0] if args.series else "KXNFLGAME"
    root = config.data_root()
    seasons = [int(s) for s in args.seasons.split(",")]
    v = venue(argparse.Namespace(env="prod"))  # history is prod's: demo has none worth keeping
    try:
        cutoff = await history.cutoff_ms(v.rest)
        markets = await kh.sync_markets(root, v.rest, args.series)
        links = kh.link_games(root, markets, seasons)
        print(f"markets: {len(markets):,} settled ({markets.filter(pl.col('source') == 'historical').height:,} "
              f"archived before {kh.kickoff_utc(cutoff)})")
        g = links.games
        if g.is_empty():
            die("no games linked; fetch the play-by-play first with `pmx data nfl` "
                "(data/nflverse/pbp_<season>.parquet)")
        reg = g.filter(pl.col("season_type").is_in(["REG", "POST"])).height
        print(f"games: {g.height} linked ({reg} REG+POST), {len(links.unmatched)} events unmatched "
              f"({len(links.unmatched) / max(1, g.height + len(links.unmatched)):.1%}), {links.outside} outside the "
              f"nflverse seasons {args.seasons} (preseason, or not downloaded yet), {links.duplicates} duplicates")
        for e in links.unmatched:
            print(f"  unmatched {e}")
        fetched, cached = await kh.sync_candles(root, v.rest, g, cutoff_ms=cutoff, force=args.force,
                                               series=args.series)
        print(f"candles: {fetched} tickers fetched, {cached} already cached")
    finally:
        await v.aclose()
    c = kh.check(root, g)
    print(f"check: {c.with_candles}/{c.games} games have home candles; last candle agrees with the result for "
          f"{c.consistent}/{c.with_candles} ({c.consistent / max(1, c.with_candles):.1%})")
    for bad in c.inconsistent[:20]:
        print(f"  disagrees: {bad}")
    missing = [r["game_id"] for r in g.iter_rows(named=True)
               if not r["home_ticker"] or not kh.candle_path(root, r["home_ticker"]).exists()
               or pl.read_parquet(kh.candle_path(root, r["home_ticker"])).is_empty()]
    for m in missing:
        print(f"  no home candles: {m}")
    if args.spot:
        game = g.filter((pl.col("home_ticker") == args.spot) | (pl.col("away_ticker") == args.spot))
        if not game.is_empty():
            ko = game["kickoff_ms"][0]
            print(f"\n{args.spot} around kickoff ({kh.kickoff_utc(ko)}):")
            print(kh.spot_check(root, args.spot, ko).select(
                "end_utc", "bid_close", "ask_close", "price_close", "volume", "oi"))


async def _kalshi(args: argparse.Namespace) -> None:
    import httpx

    from pmx.cli.common import die, venue
    from pmx.connectors.kalshi.rest import KalshiError
    from pmx.research import kalshi_data as kd
    from pmx.research.windows import day_ms

    if not args.series and not args.category:
        die("which series? --series KXHIGHNY … and/or --category \"Climate and Weather\"")
    since = None
    if args.since:
        from datetime import date

        since = day_ms(date.fromisoformat(args.since))
    v = venue(argparse.Namespace(env="prod"))  # history is prod's
    try:
        series = list(args.series or [])
        for cat in args.category or []:
            found = await kd.series_in_category(v.rest, cat)
            print(f"{cat}: {len(found)} series")
            series += found
        failed = 0
        for s in dict.fromkeys(series):
            try:
                r = await kd.sync_series(config.data_root(), v.rest, s, period=args.period, trades=args.trades,
                                         since_ms=since, force=args.force, progress=print)
            except (KalshiError, httpx.HTTPError) as e:
                print(f"{s}: failed: {e}")
                failed += 1
                continue
            failed += bool(r.failed)
            print(f"{s}: {r.markets:,} settled markets, {r.events:,} events → {r.fetched} files fetched, "
                  f"{r.cached} already cached" + (f", {len(r.failed)} failed" if r.failed else ""))
            if r.capped:
                print(f"  warning: the archive listed {kd.LISTING_CAP:,} markets for {s}, its listing limit; "
                      f"older markets are missing")
            for f in r.failed[:10]:
                print(f"  failed: {f}")
    finally:
        await v.aclose()
    if failed:
        die(f"{failed} series incomplete; run the same command again to fetch what's missing")


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    d = sub.add_parser("data", parents=[common], help="download history: kalshi (any series), kalshi-history, nfl")
    d.add_argument("what", choices=["kalshi", "kalshi-history", "nfl"])
    d.add_argument("--series", nargs="+", help="kalshi: series tickers · kalshi-history: one (default KXNFLGAME)")
    d.add_argument("--category", nargs="+", help='kalshi: every series in a category, e.g. "Climate and Weather"')
    d.add_argument("--period", default="1h", choices=["1m", "1h", "1d"], help="kalshi: candle period")
    d.add_argument("--trades", action="store_true", help="kalshi: also every public trade (large for busy series)")
    d.add_argument("--since", help="kalshi: only fetch candles/trades for markets closing on or after YYYY-MM-DD")
    d.add_argument("--seasons", default="2025,2026", help="nflverse seasons to link against")
    d.add_argument("--force", action="store_true", help="fetch again even when cached (candles, past seasons)")
    d.add_argument("--spot", help="print this ticker's candles around kickoff")
    return {"data": cmd_data}
