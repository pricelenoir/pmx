"""`espn_wp` (NFL domain): a pass-through of ESPN's own win probability (`GameState.espn_home_wp`) to the
game's linked team markets — the home team's gets p, the away team's 1 − p. Once final: 1 for the
winner, 0 for the loser, 0.5 for both on a tie (Kalshi's NFL game rules settle a tie at $0.50)."""

from __future__ import annotations

from typing import Any

from pmx.engine.events import Event
from pmx.engine.state import State
from pmx.models.base import forecast
from pmx.plugins import model
from pmx.sports.nfl.catalog import parse_ticker
from pmx.sports.nfl.state import GameState

VERSION = "espn_wp/1"


@model("espn_wp")
class EspnWp:
    event_types = (GameState,)

    def __init__(self, name: str, params: Any = None, env: Any = None) -> None:
        self.name = name

    def wants(self, ev: Event) -> bool:
        return type(ev) is GameState

    def on_event(self, ev: GameState, state: State, now_ms: int) -> list[Event]:
        out = []
        for ticker in sorted(state.tickers_of.get(ev.entity, ())):
            c = parse_ticker(state.contracts[ticker].ticker)
            if c is None or c.team not in (ev.home, ev.away):
                continue
            side = "home" if c.team == ev.home else "away"
            if ev.status == "post":
                p = 0.5 if ev.winner is None else 1.0 if ev.winner == side else 0.0
            elif ev.espn_home_wp is None:
                continue
            else:
                p = ev.espn_home_wp if side == "home" else 1 - ev.espn_home_wp
            out.append(forecast(self.name, ticker, p, now_ms, ev.ts_ms, version=VERSION))
        return out
