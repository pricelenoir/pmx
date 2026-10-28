"""`espn_nfl`: live NFL game state from ESPN's public scoreboard and game summaries (no auth).

Polls on the games' cadence (`EspnPoller`), records every new response raw (`sink.record(name, "f",
envelope)`, so replays run the same decoder), and emits a `GameState` per game only when it changed.
Errors and parse failures are reported as `SourceStatus` and never stop it. Unofficial endpoints that
can change without notice — another reason everything is kept raw."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import Callable
from typing import Any

import httpx
import msgspec

from pmx.engine.events import SourceStatus
from pmx.plugins import source
from pmx.recorder.rawlog import envelope
from pmx.sources.base import Sink
from pmx.sports.nfl.espn import EspnDecoder

log = logging.getLogger(__name__)


BASE = "https://site.api.espn.com/apis/site/v2/sports/football/nfl"
SCOREBOARD = f"{BASE}/scoreboard"


def summary_url(game_id: str) -> str:
    return f"{BASE}/summary?event={game_id}"


def games(scoreboard: dict) -> dict[str, tuple]:
    """game id -> what changes when a play happens (state, period, clock, scores, last play)."""
    out = {}
    for ev in scoreboard.get("events", ()):
        comp = (ev.get("competitions") or [{}])[0]
        status = comp.get("status", {})
        scores = tuple(c.get("score") for c in comp.get("competitors", ()))
        last = (comp.get("situation") or {}).get("lastPlay", {}).get("id")
        out[str(ev.get("id"))] = (status.get("type", {}).get("state"), status.get("period"), status.get("clock"),
                                  scores, last)
    return out


class EspnPoller:
    """Polls on the cadence ESPN's state calls for: the scoreboard every `idle_s` (every `live_s` while
    any game is in progress); each live game's summary every `summary_s`, at once when the scoreboard
    shows a new play, and once when a game starts or ends. `on_response(url, status, ms, body)` gets
    every response that differs from the previous one for its URL."""

    def __init__(
        self,
        on_response: Callable[[str, int, int, str], None],
        *,
        on_error: Callable[[str, str], None] | None = None,
        http: httpx.AsyncClient | None = None,
        idle_s: float = 60.0,
        live_s: float = 5.0,
        summary_s: float = 15.0,
        max_backoff_s: float = 300.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.on_response, self.on_error = on_response, on_error
        self.http = http or httpx.AsyncClient(timeout=20, headers={"User-Agent": "pmx-recorder"})
        self.idle_s, self.live_s, self.summary_s, self.max_backoff_s = idle_s, live_s, summary_s, max_backoff_s
        self.clock = clock
        self.games: dict[str, tuple] = {}  # last scoreboard signature per game
        self.live: set[str] = set()
        self.next_summary: dict[str, float] = {}
        self.errors = 0
        self.responses = 0
        self.skipped = 0  # identical to the previous response for the URL
        self._last: dict[str, bytes] = {}

    async def fetch(self, url: str) -> tuple[int, str] | None:
        """GET; every response (any status) goes to `on_response` unless it repeats the last one."""
        try:
            r = await self.http.get(url)
        except httpx.HTTPError as e:
            self.errors += 1
            log.warning("espn %s: %s", url, type(e).__name__)
            if self.on_error is not None:
                self.on_error(url, type(e).__name__)
            return None
        body = r.text
        digest = hashlib.blake2b(f"{r.status_code}\n{body}".encode(), digest_size=16).digest()
        if self._last.get(url) == digest:
            self.skipped += 1
        else:
            self._last[url] = digest
            self.responses += 1
            self.on_response(url, r.status_code, int(self.clock() * 1000), body)
        if r.status_code >= 400:
            self.errors += 1
            if self.on_error is not None:
                self.on_error(url, f"HTTP {r.status_code}")
            return None
        return r.status_code, body

    async def poll_once(self) -> float:
        """One scoreboard poll plus the summaries it calls for; returns seconds until the next one."""
        got = await self.fetch(SCOREBOARD)
        if got is None:
            return min(self.max_backoff_s, self.live_s * 2 ** min(self.errors, 6))
        try:
            now_games = games(json.loads(got[1]))
        except ValueError:
            log.warning("espn scoreboard isn't JSON")
            return self.idle_s
        self.errors = 0
        now = self.clock()
        due = []
        for gid, sig in now_games.items():
            state, prev = sig[0], self.games.get(gid)
            changed = prev is not None and prev != sig
            if state == "in":
                if gid not in self.live or changed or now >= self.next_summary.get(gid, 0):
                    due.append(gid)
                self.live.add(gid)
            elif gid in self.live or (prev is not None and prev[0] != state and state == "post"):
                due.append(gid)  # the final summary
                self.live.discard(gid)
        self.games = now_games
        sem = asyncio.Semaphore(4)

        async def summary(gid: str) -> None:
            async with sem:
                await self.fetch(summary_url(gid))
            self.next_summary[gid] = self.clock() + self.summary_s

        await asyncio.gather(*(summary(g) for g in due))
        return self.live_s if self.live else self.idle_s

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    wait = await self.poll_once()
                except Exception:  # never let a surprise in ESPN's payload end the capture
                    log.exception("espn poll failed")
                    wait = self.idle_s
                try:
                    await asyncio.wait_for(stop.wait(), timeout=wait)
                except TimeoutError:
                    pass
        finally:
            await self.http.aclose()


@source("espn_nfl")
class EspnNfl:
    @staticmethod
    def raw_logs(name: str) -> dict[str, Any]:
        """What this source records, by raw source, and the decoder a replay reads it with."""
        return {name: EspnDecoder}

    class Params(msgspec.Struct, frozen=True):
        idle_s: float = 30.0  # scoreboard poll with no game on
        live_s: float = 5.0  # ... while any game is in progress
        summary_s: float = 5.0  # each live game's summary

    def __init__(self, name: str, params: Params | None = None, env: Any = None, *,
                 http: httpx.AsyncClient | None = None) -> None:
        self.name, self.p = name, params or self.Params()
        self.decoder = EspnDecoder()
        self.healthy: bool | None = None
        self._http = http

    async def run(self, sink: Sink, stop: asyncio.Event) -> None:
        def status(ok: bool, detail: str) -> None:
            if ok != self.healthy:
                self.healthy = ok
                sink.emit(SourceStatus(self.name, ok, detail, int(time.time() * 1000)))

        def on_response(url: str, code: int, ms: int, body: str) -> None:
            sink.record(self.name, "f", envelope(url, code, ms, body))
            if code != 200:
                return
            try:
                states = self.decoder.on_response(url, code, ms, json.loads(body))
            except Exception as e:  # schema drift: keep recording raw, say so, carry on
                log.exception("espn: can't parse %s", url)
                status(False, f"parse error on {url.rsplit('/', 1)[-1]}: {type(e).__name__}: {e}")
                return
            status(True, "ok")
            for s in states:
                sink.emit(s)

        poller = EspnPoller(on_response, on_error=lambda url, why: status(False, f"{why}: {url}"), http=self._http,
                            idle_s=self.p.idle_s, live_s=self.p.live_s, summary_s=self.p.summary_s)
        await poller.run(stop)
