"""nflverse play-by-play: download, trim to what the WP model needs, and turn each play
into the pre-play game state plus the wall-clock time that state became known."""

from __future__ import annotations

import logging
from pathlib import Path

import httpx
import numpy as np
import polars as pl
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

PBP_URL = "https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{season}.parquet"
COLUMNS = [
    "game_id", "season", "season_type", "week", "game_date", "home_team", "away_team", "posteam", "play_id",
    "qtr", "game_seconds_remaining", "half_seconds_remaining", "down", "ydstogo", "yardline_100",
    "total_home_score", "total_away_score", "home_timeouts_remaining", "away_timeouts_remaining",
    "home_opening_kickoff", "spread_line", "result", "vegas_home_wp", "time_of_day", "end_clock_time", "desc",
]
# Result of a play is known roughly this long after the snap when the feed has no end time.
PLAY_SECONDS = 6


def pbp_path(root: Path, season: int) -> Path:
    return root / "nflverse" / f"pbp_{season}.parquet"


def download(root: Path, seasons: list[int], *, force: bool = False) -> list[Path]:
    out = []
    with httpx.Client(follow_redirects=True, timeout=120) as http:
        for season in seasons:
            path = pbp_path(root, season)
            if path.exists() and not force:
                out.append(path)
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".download")
            r = http.get(PBP_URL.format(season=season))
            r.raise_for_status()
            tmp.write_bytes(r.content)
            have = set(pq.read_schema(tmp).names)
            pq.write_table(pq.read_table(tmp, columns=[c for c in COLUMNS if c in have]), path)
            tmp.unlink()
            log.info("nflverse pbp %d -> %s", season, path)
            out.append(path)
    return out


def _ts(col: str) -> pl.Expr:
    return pl.col(col).str.to_datetime(time_zone="UTC", strict=False).dt.epoch("ms")


def states(root: Path, seasons: list[int], *, season_types: tuple[str, ...] = ("REG", "POST")) -> pl.DataFrame:
    """One row per play: the state *before* the play (home perspective) and `ts_ms`, when
    that state became known (end of the previous play, else its snap + `PLAY_SECONDS`)."""
    frames = [pl.read_parquet(pbp_path(root, s)) for s in seasons]
    df = pl.concat(frames, how="diagonal_relaxed").filter(pl.col("season_type").is_in(season_types))
    df = df.filter(pl.col("game_seconds_remaining").is_not_null() & pl.col("qtr").is_not_null())
    df = df.sort(["game_id", "play_id"])
    snap = _ts("time_of_day")
    end = pl.coalesce(_ts("end_clock_time"), snap + PLAY_SECONDS * 1000)
    g = pl.col("game_id")
    half = pl.when(pl.col("qtr") <= 2).then(1).when(pl.col("qtr") <= 4).then(2).otherwise(3)

    def before(col: str) -> pl.Expr:
        """The previous play's value: nflverse has scores and timeouts after the play."""
        return pl.col(col).shift(1).over(g)

    poss = (
        pl.when(pl.col("posteam") == pl.col("home_team")).then(1)
        .when(pl.col("posteam") == pl.col("away_team")).then(-1)
        .otherwise(0)
    )
    df = df.with_columns(
        home=pl.col("home_team"),
        away=pl.col("away_team"),
        ts_ms=pl.coalesce(end.shift(1).over(g), snap),
        snap_ms=snap,
        home_score=before("total_home_score").fill_null(0),
        away_score=before("total_away_score").fill_null(0),
        # Timeouts reset at the half and in overtime, so a half's first play keeps its own count.
        home_to=pl.when(half.shift(1).over(g) == half).then(before("home_timeouts_remaining"))
        .otherwise(pl.col("home_timeouts_remaining")),
        away_to=pl.when(half.shift(1).over(g) == half).then(before("away_timeouts_remaining"))
        .otherwise(pl.col("away_timeouts_remaining")),
        poss=poss,
        is_ot=(pl.col("qtr") > 4).cast(pl.Float64),
        home_rec_2h=pl.when(pl.col("qtr") <= 2).then(pl.col("home_opening_kickoff").cast(pl.Float64)),
        home_win=(pl.col("result") > 0).cast(pl.Int8),
        final_home=pl.col("total_home_score").last().over(g),
        final_away=pl.col("total_away_score").last().over(g),
    )
    return df.select(
        "game_id", "season", "season_type", "week", "game_date", "home", "away", "play_id", "ts_ms", "snap_ms",
        "qtr", pl.col("game_seconds_remaining").alias("secs_left"),
        pl.col("half_seconds_remaining").alias("half_secs_left"),
        "home_score", "away_score", (pl.col("home_score") - pl.col("away_score")).alias("score_diff"),
        "poss", "down", "ydstogo", pl.col("yardline_100").alias("yte"), "home_to", "away_to",
        "home_rec_2h", "is_ot", "spread_line", "result", "home_win", "final_home", "final_away",
        pl.col("vegas_home_wp").alias("nflfastr_wp"), "desc",
    )


def feature_inputs(df: pl.DataFrame, prior_logit: np.ndarray) -> dict[str, np.ndarray]:
    """Keyword arguments for `features.build` from a `states()` frame."""
    poss = df["poss"].to_numpy().astype(float)
    has_ball = poss != 0
    def col(name: str) -> np.ndarray:
        return df[name].cast(pl.Float64).fill_null(np.nan).to_numpy()
    return {
        "prior_logit": prior_logit,
        "score_diff": col("score_diff"),
        "secs_left": col("secs_left"),
        "half_secs_left": col("half_secs_left"),
        "poss": poss,
        "down": np.where(has_ball, col("down"), np.nan),
        "ydstogo": np.where(has_ball & ~np.isnan(col("down")), col("ydstogo"), np.nan),
        "yte": np.where(has_ball, col("yte"), np.nan),
        "home_to": col("home_to"),
        "away_to": col("away_to"),
        "home_rec_2h": col("home_rec_2h"),
        "is_ot": col("is_ot"),
    }
