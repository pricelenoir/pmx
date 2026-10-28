"""Data sync: `pmx data nfl` (nflverse play-by-play), `pmx data kalshi-history`."""

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
    import polars as pl

    from pmx.cli.common import die, venue
    from pmx.connectors.kalshi import history
    from pmx.harness import kalshi_history as kh

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


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    d = sub.add_parser("data", parents=[common], help="download history: kalshi-history, nfl")
    d.add_argument("what", choices=["kalshi-history", "nfl"])
    d.add_argument("--series", default="KXNFLGAME")
    d.add_argument("--seasons", default="2025,2026", help="nflverse seasons to link against")
    d.add_argument("--force", action="store_true", help="fetch again even when cached (candles, past seasons)")
    d.add_argument("--spot", help="print this ticker's candles around kickoff")
    return {"data": cmd_data}
