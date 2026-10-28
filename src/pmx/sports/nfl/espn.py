"""ESPN's public NFL JSON → `GameState`s. Pure functions plus a stateful decoder; no I/O.

Endpoints are unofficial and change without notice, so everything here tolerates missing fields, and
the raw responses are recorded so a parser fix can be replayed over them. Payload facts relied on
(verified on 401872995 SEA@DEN, 2026-10-15):
- `events[].date` / `competitions[0].date` is the UTC kickoff; `GameState.date` is the US/Eastern date.
- A play belongs to a drive whose `team` is the offense (on kickoffs: the receiving team, as nflverse
  has it); `start.team` on a kickoff is the kicking team, and `yardsToEndzone` is 0 on timeouts and from
  the wrong side on punts — so field position comes from `possessionText` ("DEN 12", "50"), else
  `yardLine`, which is measured from the home goal line.
- `clock.displayValue` is the game clock at the snap; `wallclock` is the play's UTC time.
- The team of the first drive received the opening kickoff."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import msgspec

from pmx.connectors.base import Frame, StreamItem
from pmx.sports.nfl.state import REG_PERIOD_S, GameState, GameStatus, Side
from pmx.sports.nfl.teams import canon

EASTERN = ZoneInfo("America/New_York")
_TIMEOUT = re.compile(r"timeout #\d+ by ([A-Za-z]{2,3})", re.IGNORECASE)
_NOT_PLAYS = ("timeout", "end period", "end of half", "end of regulation", "end of game", "two-minute warning")


class Header(msgspec.Struct, frozen=True):
    game_id: str
    date: str  # US/Eastern local date of the kickoff
    home: str
    away: str
    home_id: str
    away_id: str
    kickoff_ms: int | None
    status: GameStatus
    home_score: int
    away_score: int
    winner: Side | None


def _ms(iso: str | None) -> int | None:
    if not iso:
        return None
    try:
        return round(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def _clock_s(display: str | None) -> int | None:
    if not display or ":" not in display:
        return None
    m, _, s = display.partition(":")
    try:
        return int(m) * 60 + int(float(s))
    except ValueError:
        return None


def _int(v: Any) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _status(status: dict) -> GameStatus:
    t = status.get("type", {})
    if t.get("name") == "STATUS_HALFTIME":
        return "half"
    state = t.get("state")
    return state if state in ("pre", "in", "post") else "pre"


def _competition(obj: dict) -> tuple[str, dict]:
    """(game id, competition) from a summary or a scoreboard event."""
    if "header" in obj:
        h = obj["header"]
        return str(h["id"]), h["competitions"][0]
    return str(obj["id"]), obj["competitions"][0]


def game_header(obj: dict) -> Header:
    """Who, when, and the score/status, from a summary or one scoreboard event."""
    gid, comp = _competition(obj)
    sides = {c["homeAway"]: c for c in comp["competitors"]}
    home, away = sides["home"], sides["away"]
    kickoff = _ms(comp.get("date") or obj.get("date"))
    date = datetime.fromtimestamp(kickoff / 1000, EASTERN).date().isoformat() if kickoff else ""
    status = _status(comp.get("status", {}))
    hs, as_ = _int(home.get("score")) or 0, _int(away.get("score")) or 0
    winner: Side | None = None
    if status == "post":
        winner = "home" if home.get("winner") or hs > as_ else "away" if away.get("winner") or as_ > hs else None
    return Header(gid, date, canon(home["team"]["abbreviation"]), canon(away["team"]["abbreviation"]),
                  str(home["team"]["id"]), str(away["team"]["id"]), kickoff, status, hs, as_, winner)


def yards_to_endzone(spot: dict, offense: str, offense_home: bool) -> int | None:
    """Field position from the offense's view: `possessionText` ("DEN 12": the DEN 12-yard line), else
    `yardLine` (from the home goal line), else ESPN's own `yardsToEndzone`."""
    text = (spot.get("possessionText") or "").strip()
    if text == "50":
        return 50
    if text:
        abbr, _, n = text.rpartition(" ")
        yards = _int(n)
        try:
            side = canon(abbr)
        except ValueError:
            side = None
        if yards is not None and side is not None and 0 < yards <= 50:
            return 100 - yards if side == offense else yards
    yl = _int(spot.get("yardLine"))
    if yl is not None and 0 < yl < 100:
        return 100 - yl if offense_home else yl
    yte = _int(spot.get("yardsToEndzone"))
    return yte if yte is not None and 0 < yte < 100 else None


def _plays(obj: dict) -> list[tuple[dict, dict]]:
    """(drive, play) in game order: finished drives, then the one in progress."""
    drives = obj.get("drives") or {}
    out, seen = [], set()
    for d in [*drives.get("previous", ()), *([drives["current"]] if drives.get("current") else ())]:
        for p in d.get("plays", ()):
            pid = p.get("id")
            if pid not in seen:
                seen.add(pid)
                out.append((d, p))
    return out


def _receives_2h(plays: list[tuple[dict, dict]], h: Header) -> bool | None:
    if not plays:
        return None
    first = str(plays[0][0].get("team", {}).get("id"))
    return None if first not in (h.home_id, h.away_id) else first != h.home_id


def _half(period: int) -> int:
    return 1 if period <= 2 else 2 if period <= 4 else 3


def _timeouts_left(used: dict[tuple[int, str], int], period: int, side: str) -> int:
    half = _half(period)
    return max(0, (3 if half < 3 else 2) - used.get((half, side), 0))


def _timeout_by(play: dict, h: Header) -> str | None:
    """'home' / 'away' for a team timeout (not an official one), else None."""
    if play.get("type", {}).get("text", "").lower() != "timeout":
        return None
    m = _TIMEOUT.search(play.get("text", ""))
    if m is None:
        return None
    try:
        team = canon(m.group(1))
    except ValueError:
        return None
    return "home" if team == h.home else "away" if team == h.away else None


def _side(team_id: Any, h: Header) -> Side | None:
    tid = str(team_id) if team_id is not None else None
    return "home" if tid == h.home_id else "away" if tid == h.away_id else None


def summary_states(obj: dict) -> list[GameState]:
    """One state per play, as it stood *before* the play (score before it, the snap's clock, down,
    distance, field position) — the shape of nflverse's rows — stamped with the play's wallclock (never
    earlier than the previous stamp). For history and backtests."""
    h = game_header(obj)
    plays = _plays(obj)
    receives = _receives_2h(plays, h)
    wp_after = {str(w.get("playId")): w.get("homeWinPercentage") for w in obj.get("winprobability") or ()}
    used: dict[tuple[int, str], int] = {}
    out: list[GameState] = []
    prev: dict | None = None
    last_ts = h.kickoff_ms or 0
    for drive, p in plays:
        period = _int(p.get("period", {}).get("number")) or 1
        clock = _clock_s(p.get("clock", {}).get("displayValue"))
        possession = _side(drive.get("team", {}).get("id"), h)
        start = p.get("start") or {}
        down = _int(start.get("down"))
        down = down if down is not None and 1 <= down <= 4 else None
        yte = None
        if possession is not None:
            offense = h.home if possession == "home" else h.away
            yte = yards_to_endzone(start, offense, possession == "home")
        last_ts = max(last_ts, _ms(p.get("wallclock")) or last_ts)
        out.append(GameState(
            h.game_id, last_ts, h.date, h.home, h.away, "in", period, REG_PERIOD_S if clock is None else clock,
            _int(prev.get("homeScore")) or 0 if prev else 0, _int(prev.get("awayScore")) or 0 if prev else 0,
            possession, down, _int(start.get("distance")) if down else None, yte,
            _timeouts_left(used, period, "home"), _timeouts_left(used, period, "away"), receives, h.kickoff_ms,
            (prev.get("text") or "").strip()[:160] if prev else "", wp_after.get(str(prev.get("id"))) if prev else None,
        ))
        if (side := _timeout_by(p, h)) is not None:
            used[(_half(period), side)] = used.get((_half(period), side), 0) + 1
        prev = p
    return out


def parse_summary(obj: dict, now_ms: int) -> GameState:
    """The game as it stands after its latest play: score, clock, who has the ball, down and distance,
    timeouts (a `situation` block's when there is one, else counted from timeout plays), the receiving
    team, and ESPN's own win probability. Stamped with the latest play's wallclock, else `now_ms`."""
    h = game_header(obj)
    _, comp = _competition(obj)
    status_obj = comp.get("status", {})
    plays = _plays(obj)
    receives = _receives_2h(plays, h)
    used: dict[tuple[int, str], int] = {}
    for _, p in plays:
        if (side := _timeout_by(p, h)) is not None:
            period = _int(p.get("period", {}).get("number")) or 1
            used[(_half(period), side)] = used.get((_half(period), side), 0) + 1
    if not plays:
        return GameState(h.game_id, now_ms, h.date, h.home, h.away, h.status, home_score=h.home_score,
                         away_score=h.away_score, kickoff_ms=h.kickoff_ms, winner=h.winner)
    drive, last = plays[-1]
    period = _int(status_obj.get("period")) or _int(last.get("period", {}).get("number")) or 1
    clock = _int(status_obj.get("clock"))
    if clock is None:  # a live summary's status has only `displayClock`
        clock = _clock_s(status_obj.get("displayClock") or last.get("clock", {}).get("displayValue"))
    wp = obj.get("winprobability") or ()
    situation = obj.get("situation") or {}
    possession = down = distance = yte = None
    if h.status == "in":
        end = last.get("end") or {}
        team = (situation.get("possession") or (end.get("team") or {}).get("id") or drive.get("team", {}).get("id"))
        possession = _side(team, h)
        spot = situation if situation.get("possessionText") or situation.get("yardLine") is not None else end
        down = _int(spot.get("down"))
        down = down if down is not None and 1 <= down <= 4 else None
        distance = _int(spot.get("distance")) if down else None
        if possession is not None:
            yte = yards_to_endzone(spot, h.home if possession == "home" else h.away, possession == "home")
    hto = _int(situation.get("homeTimeouts"))
    ato = _int(situation.get("awayTimeouts"))
    return GameState(
        h.game_id, _ms(last.get("wallclock")) or now_ms, h.date, h.home, h.away, h.status, period,
        REG_PERIOD_S if clock is None else clock,
        h.home_score if h.home_score or h.away_score else _int(last.get("homeScore")) or 0,
        h.away_score if h.home_score or h.away_score else _int(last.get("awayScore")) or 0,
        possession, down, distance, yte,
        _timeouts_left(used, period, "home") if hto is None else hto,
        _timeouts_left(used, period, "away") if ato is None else ato,
        receives, h.kickoff_ms, (last.get("text") or "").strip()[:160],
        wp[-1].get("homeWinPercentage") if wp else None, h.winner,
    )


def parse_scoreboard(obj: dict, now_ms: int) -> list[GameState]:
    """One state per game on the board, stamped `now_ms` (the board carries no per-game time). Live games
    use the `situation` block; the receiving team isn't on the board (None)."""
    out = []
    for ev in obj.get("events") or ():
        h = game_header(ev)
        comp = ev["competitions"][0]
        status = comp.get("status", {})
        sit = comp.get("situation") or {}
        period = _int(status.get("period")) or 0
        clock = _int(status.get("clock"))
        possession = down = distance = yte = None
        last = sit.get("lastPlay") or {}
        if h.status == "in" and sit:
            possession = _side(sit.get("possession"), h)
            down = _int(sit.get("down"))
            down = down if down is not None and 1 <= down <= 4 else None
            distance = _int(sit.get("distance")) if down else None
            if possession is not None:
                yte = yards_to_endzone(sit, h.home if possession == "home" else h.away, possession == "home")
        wp = (last.get("probability") or {}).get("homeWinPercentage")
        out.append(GameState(
            h.game_id, now_ms, h.date, h.home, h.away, h.status, period,
            REG_PERIOD_S if h.status == "pre" or clock is None else clock, h.home_score, h.away_score,
            possession, down, distance, yte, _int(sit.get("homeTimeouts")) if "homeTimeouts" in sit else 3,
            _int(sit.get("awayTimeouts")) if "awayTimeouts" in sit else 3, None, h.kickoff_ms,
            (last.get("text") or "").strip()[:160], wp, h.winner,
        ))
    return out


def _same(a: GameState, b: GameState) -> bool:
    return msgspec.structs.replace(a, ts_ms=b.ts_ms) == b


class EspnDecoder:
    """Recorded or live ESPN responses (the recorder's envelopes) → the `GameState`s worth emitting: a game's state
    only when it changed (ignoring `ts_ms`). Summaries speak for games in progress; the scoreboard covers
    the rest and borrows the receiving team from the game's last summary. The same object runs live
    (`sources/espn_nfl.py`) and over recorded logs, so both see identical events."""

    def __init__(self) -> None:
        self.last: dict[str, GameState] = {}
        self.receives: dict[str, bool] = {}
        self.summarized: set[str] = set()

    def __call__(self, item: StreamItem) -> list[GameState]:
        if not isinstance(item, Frame):
            return []
        env = json.loads(item.text)
        return self.on_response(env.get("url", ""), env.get("status", 0), env.get("ms", 0), env.get("body"))

    def on_response(self, url: str, status: int, ms: int, body: Any) -> list[GameState]:
        if status != 200 or not isinstance(body, dict):
            return []
        if "/summary" in url:
            s = parse_summary(body, ms)
            self.summarized.add(s.game_id)
            if s.home_receives_2h is not None:
                self.receives[s.game_id] = s.home_receives_2h
            states = [s]
        elif "/scoreboard" in url:
            states = []
            for s in parse_scoreboard(body, ms):
                if s.status in ("in", "half") and s.game_id in self.summarized:
                    continue  # its summaries are fresher and richer
                if s.game_id in self.receives:
                    s = msgspec.structs.replace(s, home_receives_2h=self.receives[s.game_id])
                states.append(s)
        else:
            return []
        out = []
        for s in states:
            prev = self.last.get(s.game_id)
            if prev is None or not _same(prev, s):
                self.last[s.game_id] = s
                out.append(s)
        return out
