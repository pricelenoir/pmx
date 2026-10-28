"""Cross-check: ESPN summaries (fetched live) vs nflverse play-by-play for the same games.

    uv run python tests/fixtures/espn/crosscheck_nflverse.py [--season 2026] [--weeks 1-6]

Aligns ESPN's plays with nflverse's by description, then compares their pre-play states
(`summary_states` vs `data.states`) on period, score, possession, down, distance, and yards to the end
zone, over plays where nflverse has a possession. Needs network and
data/nflverse/pbp_<season>.parquet."""

from __future__ import annotations

import argparse
import difflib
import re
import time
from collections import defaultdict
from datetime import date

import httpx
import polars as pl

from pmx import config
from pmx.sources.espn_nfl import SCOREBOARD, summary_url
from pmx.sports.nfl import data, espn

_CLOCK = re.compile(r"^\s*\(\d*:\d\d\)\s*")
_FORMATION = re.compile(r"^\s*\((?:no huddle|shotgun|punt formation|field goal formation|run formation|pass formation)"
                        r"[^)]*\)\s*", re.IGNORECASE)
_JERSEY = re.compile(r"\b\d{1,2}-(?=[A-Z])")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--weeks", default="1-6")
    args = ap.parse_args()
    lo, _, hi = args.weeks.partition("-")
    nfl = data.states(config.data_root(), [args.season], season_types=("REG",))
    games: dict[tuple, str] = {}
    for gid, day, home, away in nfl.select("game_id", "game_date", "home", "away").unique().iter_rows():
        games[(home, away)] = games.get((home, away)) or gid
        games[(date.fromisoformat(day), home, away)] = gid
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    per_game = []
    with httpx.Client(timeout=30) as http:
        for week in range(int(lo), int(hi or lo) + 1):
            board = http.get(SCOREBOARD, params={"seasontype": 2, "week": week, "dates": args.season}).json()
            for ev in board.get("events", ()):
                h = espn.game_header(ev)
                if h.status != "post":
                    continue
                d = date.fromisoformat(h.date)
                gid = next((games[k] for k in [(d, h.home, h.away)] if k in games), None)
                if gid is None:
                    print(f"week {week}: {h.away}@{h.home} {h.date} not in nflverse")
                    continue
                obj = http.get(summary_url(h.game_id)).json()
                time.sleep(0.3)
                plays = [p for d in (obj.get("drives") or {}).get("previous", ()) for p in d.get("plays", ())]
                stats = compare(espn.summary_states(obj), plays, nfl.filter(pl.col("game_id") == gid))
                per_game.append((gid, stats))
                for k, (ok, n) in stats.items():
                    totals[k][0] += ok
                    totals[k][1] += n
    print(f"{len(per_game)} games")
    for k, (ok, n) in totals.items():
        print(f"{k:16s} {ok:6d}/{n:6d}  {ok / n:6.1%}" if n else f"{k:16s} -")
    worst = sorted(per_game, key=lambda g: g[1]["aligned"][0] / max(1, g[1]["aligned"][1]))[:3]
    for gid, st in worst:
        print("lowest alignment:", gid, {k: f"{a}/{b}" for k, (a, b) in st.items()})


def _key(text: str) -> str:
    """A play's description, comparable across feeds: no clock prefix, formation, or jersey numbers."""
    t = _CLOCK.sub("", text or "")
    t = _FORMATION.sub("", t)
    t = _JERSEY.sub("", t)
    return " ".join(t.lower().split())[:40]


def compare(states: list, plays: list[dict], nfl: pl.DataFrame) -> dict[str, tuple[int, int]]:
    """Align the two play sequences by description (difflib), then compare the pre-play states of
    matched plays. ESPN stamps scoring plays with the clock after the play and folds the extra point
    into the touchdown, so (period, clock) pairing drifts; descriptions don't."""
    rows = list(nfl.iter_rows(named=True))
    a = [_key(r["desc"]) for r in rows]
    b = [_key(p.get("text", "")) for p in plays]
    out = {k: [0, 0] for k in ("aligned", "period", "score", "possession", "down", "distance", "yards_to_endzone")}
    out["aligned"][1] = sum(1 for r in rows if r["poss"] != 0)
    blocks = difflib.SequenceMatcher(None, a, b, autojunk=False).get_matching_blocks()
    for block in blocks:
        for i in range(block.size):
            row, s = rows[block.a + i], states[block.b + i]
            if row["poss"] == 0:  # timeouts, period ends: no play to compare
                continue
            out["aligned"][0] += 1
            _tally(out, "period", s.period == int(row["qtr"]))
            _tally(out, "score", (s.home_score, s.away_score) == (int(row["home_score"]), int(row["away_score"])))
            _tally(out, "possession", s.possession == ("home" if row["poss"] == 1 else "away"))
            if row["down"] is not None:
                _tally(out, "down", s.down == int(row["down"]))
                _tally(out, "distance", s.distance == int(row["ydstogo"]))
            if row["yte"] is not None:
                _tally(out, "yards_to_endzone", s.yards_to_endzone == int(row["yte"]))
    return {k: (a_, b_) for k, (a_, b_) in out.items()}


def _tally(out: dict, key: str, ok: bool) -> None:
    out[key][0] += ok
    out[key][1] += 1


if __name__ == "__main__":
    main()
