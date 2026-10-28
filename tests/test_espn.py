from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path

import httpx
import msgspec
import pytest

from pmx.connectors.base import Control, Frame
from pmx.engine.events import SourceStatus
from pmx.recorder.rawlog import envelope
from pmx.sources.espn_nfl import SCOREBOARD, EspnNfl, EspnPoller, summary_url
from pmx.sports.nfl import espn
from pmx.sports.nfl.state import GameState

FIX = Path(__file__).parent / "fixtures" / "espn"


def load(name: str) -> dict:
    return json.loads((FIX / name).read_text())


@pytest.fixture
def final() -> dict:
    return load("summary_final_401872995.json")


@pytest.fixture
def board() -> dict:
    return load("scoreboard_pre_post.json")


@pytest.fixture
def live_board() -> dict:
    return load("scoreboard_live.json")


def test_header_maps_teams_scores_and_the_eastern_date(final, board):
    h = espn.game_header(final)
    assert (h.game_id, h.home, h.away, h.home_id, h.away_id) == ("401872995", "DEN", "SEA", "7", "26")
    assert (h.date, h.status, h.home_score, h.away_score, h.winner) == ("2026-10-15", "post", 17, 14, "home")
    assert h.kickoff_ms == 1_792_109_700_000  # 2026-10-16T00:15Z: an evening game on the 15th in New York
    pre = espn.game_header(board["events"][0])
    assert (pre.home, pre.away, pre.status, pre.date, pre.winner) == ("JAX", "HOU", "pre", "2026-10-18", None)
    wsh = copy.deepcopy(board["events"][0])
    wsh["competitions"][0]["competitors"][0]["team"]["abbreviation"] = "WSH"
    assert {espn.game_header(wsh).home, espn.game_header(wsh).away} == {"WAS", "HOU"}


@pytest.mark.parametrize(("spot", "offense", "home", "yte"), [
    ({"possessionText": "SEA 29"}, "SEA", False, 71),  # own territory
    ({"possessionText": "DEN 42"}, "SEA", False, 42),  # opponent's
    ({"possessionText": "50"}, "DEN", True, 50),
    ({"possessionText": "JAX 20"}, "JAX", True, 80),  # ESPN's code canonicalized
    ({"yardLine": 10}, "DEN", True, 90),  # yardLine is from the home goal line
    ({"yardLine": 71}, "SEA", False, 71),
    ({"yardLine": 35, "yardsToEndzone": 65}, "SEA", False, 35),  # a kickoff: yardsToEndzone is the kicker's
    ({"yardsToEndzone": 12}, "DEN", True, 12),
    ({"yardsToEndzone": 0}, "DEN", True, None),  # timeouts carry a bogus 0
])
def test_yard_line_conversion_for_both_possessions(spot, offense, home, yte):
    assert espn.yards_to_endzone(spot, offense, home) == yte


def test_summary_states_are_pre_play_states_in_time_order(final):
    states = espn.summary_states(final)
    assert len(states) == 201
    assert all(a.ts_ms <= b.ts_ms for a, b in zip(states, states[1:], strict=False))
    kickoff, first = states[0], states[1]
    assert (kickoff.possession, kickoff.down, kickoff.yards_to_endzone, kickoff.last_play) == ("away", None, 35, "")
    assert (first.possession, first.down, first.distance, first.yards_to_endzone) == ("away", 1, 10, 71)
    assert first.last_play.startswith("W.Lutz kicks") and first.espn_home_wp == pytest.approx(0.4464)
    assert all(s.home_receives_2h is True for s in states)  # SEA had the first drive: DEN receives the 2nd half
    scores = [(s.away_score, s.home_score) for s in states]
    assert scores[0] == (0, 0) and scores[-2:] == [(14, 14), (14, 17)]  # the winning field goal, then End of Game
    assert all(s.period in range(1, 6) and 0 <= s.clock_s <= 900 for s in states)


def test_timeouts_are_counted_per_half(final):
    states = espn.summary_states(final)
    q1 = [s for s in states if s.period == 1]
    after = next(i for i, s in enumerate(q1) if s.last_play.startswith("Timeout #1 by SEA"))
    assert (q1[after - 1].away_timeouts, q1[after].away_timeouts) == (3, 2)
    second_half = [s for s in states if s.period == 3]
    assert (second_half[0].home_timeouts, second_half[0].away_timeouts) == (3, 3)
    ot = [s for s in states if s.period == 5]
    assert ot and ot[0].home_timeouts <= 2 and ot[0].away_timeouts <= 2


def test_parse_summary_final(final):
    s = espn.parse_summary(final, now_ms=0)
    assert (s.status, s.home_score, s.away_score, s.winner, s.period) == ("post", 17, 14, "home", 5)
    assert s.possession is None and s.down is None and s.espn_home_wp == 1.0
    assert s.ts_ms == espn.summary_states(final)[-1].ts_ms and s.home_receives_2h is True
    assert s.entity == "nfl:2026-10-15:DEN-SEA"  # teams in alphabetical order


def live_summary(final: dict, plays: int) -> dict:
    """The final summary cut back to its first `plays` plays, as if the game were in progress."""
    obj = copy.deepcopy(final)
    kept, n = [], 0
    for d in obj["drives"]["previous"]:
        if n >= plays:
            break
        d["plays"] = d["plays"][: plays - n]
        n += len(d["plays"])
        kept.append(d)
    obj["drives"] = {"previous": kept[:-1], "current": kept[-1]}
    comp = obj["header"]["competitions"][0]
    comp["status"] = {"type": {"state": "in", "name": "STATUS_IN_PROGRESS"}, "period": 2, "clock": 312.0}
    last = kept[-1]["plays"][-1]
    for c in comp["competitors"]:
        c["score"] = str(last["homeScore"] if c["homeAway"] == "home" else last["awayScore"])
        c.pop("winner", None)
    obj["winprobability"] = obj["winprobability"][:plays]
    return obj


def test_parse_summary_live_takes_the_state_after_the_latest_play(final):
    obj = live_summary(final, 60)
    s = espn.parse_summary(obj, now_ms=0)
    last = obj["drives"]["current"]["plays"][-1]
    assert (s.status, s.period, s.clock_s) == ("in", 2, 312)
    assert (s.home_score, s.away_score) == (last["homeScore"], last["awayScore"])
    end = last["end"]
    assert s.down == (end["down"] if 1 <= end["down"] <= 4 else None) and s.possession in ("home", "away")
    assert s.away_timeouts == 2 and s.home_timeouts == 3  # SEA's first-half timeout, counted
    assert s.last_play == last["text"].strip()[:160]
    assert s.espn_home_wp == obj["winprobability"][-1]["homeWinPercentage"]
    obj["situation"] = {"homeTimeouts": 1, "awayTimeouts": 0}
    s2 = espn.parse_summary(obj, now_ms=0)
    assert (s2.home_timeouts, s2.away_timeouts) == (1, 0)  # a situation block wins over counting


def test_parse_scoreboard_pre_post_and_live(board, live_board):
    pre, post = espn.parse_scoreboard(board, now_ms=5)
    assert (pre.status, pre.period, pre.clock_s, pre.ts_ms, pre.possession) == ("pre", 0, 900, 5, None)
    assert (post.status, post.home_score, post.away_score, post.winner) == ("post", 17, 14, "home")
    s, waiting = espn.parse_scoreboard(live_board, now_ms=7)  # captured live, HOU @ JAX 2026-10-18 13:41Z
    assert (s.game_id, s.status, s.period, s.clock_s, s.ts_ms) == ("401872996", "in", 1, 668, 7)
    assert (s.possession, s.down, s.distance, s.yards_to_endzone) == ("home", 1, 10, 54)  # JAX ball at HOU 46
    assert (s.home_timeouts, s.away_timeouts, s.espn_home_wp) == (3, 3, 0.6332)
    assert s.last_play.startswith("K.Kroeger punts") and s.home_receives_2h is None
    assert waiting.status == "pre" and waiting.possession is None


def test_parse_summary_of_a_live_game():
    obj = load("summary_live_401872996.json")  # captured with the board above, one play behind it
    s = espn.parse_summary(obj, now_ms=0)
    assert (s.status, s.period, s.clock_s) == ("in", 1, 719)  # from the status's displayClock
    assert (s.possession, s.down, s.distance, s.yards_to_endzone) == ("away", 4, 7, 91)  # HOU 4th & 7 at its 9
    assert (s.home_timeouts, s.away_timeouts, s.home_receives_2h) == (3, 3, False)  # JAX received first
    assert s.ts_ms > s.kickoff_ms and s.espn_home_wp == 0.6419


def frame(url: str, body: dict, status: int = 200, ms: int = 1) -> Frame:
    return Frame(0, envelope(url, status, ms, json.dumps(body)))


def test_decoder_emits_only_changes_and_prefers_summaries_for_live_games(board, final, live_board):
    d = espn.EspnDecoder()
    first = d(frame(SCOREBOARD, board))
    assert [s.status for s in first] == ["pre", "post"]
    assert d(frame(SCOREBOARD, board, ms=99)) == []  # same states (ts ignored)
    assert d(Control(0, "connected")) == [] and d(frame(SCOREBOARD, board, status=503)) == []
    live_id = next(s.game_id for s in espn.parse_scoreboard(live_board, 0) if s.status == "in")
    summary = live_summary(final, 60)
    summary["header"]["id"] = live_id
    from_summary = d(frame(summary_url(live_id), summary))
    assert len(from_summary) == 1 and from_summary[0].home_receives_2h is True
    again = d(frame(SCOREBOARD, live_board))
    assert live_id not in {s.game_id for s in again}  # its summaries speak for it while it's live
    ended = copy.deepcopy(live_board)
    for ev in ended["events"]:
        if ev["id"] == live_id:
            ev["competitions"][0]["status"] = {"type": {"state": "post", "name": "STATUS_FINAL"}, "period": 4,
                                               "clock": 0.0}
    final_states = [s for s in d(frame(SCOREBOARD, ended)) if s.game_id == live_id]
    assert final_states[0].status == "post" and final_states[0].home_receives_2h is True


# ---- the live source ---------------------------------------------------------------------------------


class FakeSink:
    def __init__(self) -> None:
        self.events: list = []
        self.records: list[tuple[str, str, str]] = []

    def emit(self, ev) -> None:
        self.events.append(ev)

    def record(self, source: str, kind: str, payload: str) -> None:
        self.records.append((source, kind, payload))


def test_source_records_raw_emits_changes_and_reports_health(board, final):
    stop = asyncio.Event()
    replies = iter([("board", 200), ("board", 503), ("board", 200), ("broken", 200), ("board", 200)])
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(str(req.url))
        try:
            what, code = next(replies)
        except StopIteration:
            stop.set()
            return httpx.Response(200, json=board)
        if what == "broken":
            body = copy.deepcopy(board)
            del body["events"][0]["competitions"][0]["competitors"]  # schema drift
            return httpx.Response(code, json=body)
        return httpx.Response(code, json=board) if code == 200 else httpx.Response(code, text="busy")

    src = EspnNfl("espn", EspnNfl.Params(idle_s=0.001, live_s=0.001),
                  http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    sink = FakeSink()
    asyncio.run(asyncio.wait_for(src.run(sink, stop), timeout=10))
    games = [e for e in sink.events if isinstance(e, GameState)]
    status = [(e.connected, e.detail.split(":")[0]) for e in sink.events if isinstance(e, SourceStatus)]
    assert [g.status for g in games] == ["pre", "post"]  # emitted once, though the board came back 4 times
    assert status == [(True, "ok"), (False, "HTTP 503"), (True, "ok"), (False, "parse error on scoreboard"),
                      (True, "ok")]
    assert all(r[0] == "espn" and r[1] == "f" for r in sink.records)
    bodies = [json.loads(r[2]) for r in sink.records]
    assert [b["status"] for b in bodies] == [200, 503, 200, 200, 200]  # every new response, raw (repeats skipped)
    assert bodies[0]["body"] == board and bodies[0]["url"] == SCOREBOARD


def test_source_is_a_registered_plugin_with_params():
    from pmx import plugins

    assert plugins.SOURCES["espn_nfl"] is EspnNfl
    p = plugins.build_params(EspnNfl, {"live_s": 2})
    assert p == EspnNfl.Params(live_s=2.0) and msgspec.structs.asdict(p)["idle_s"] == 30.0


# ---- polling ---------------------------------------------------------------------------------------


def scoreboard(*games: tuple[str, str, str]) -> dict:
    """(id, state, last play id) per game."""
    return {"events": [{"id": gid, "competitions": [{
        "status": {"type": {"state": state}, "period": 2, "clock": 300.0},
        "competitors": [{"score": "7"}, {"score": "3"}],
        **({"situation": {"lastPlay": {"id": last}}} if last else {}),
    }]} for gid, state, last in games]}


def test_espn_poller_fetches_live_summaries_on_change_and_skips_repeats():
    board = {"value": scoreboard(("1", "pre", ""), ("2", "in", "p1"))}
    hits: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        url = str(req.url)
        hits.append(url)
        if url == SCOREBOARD:
            return httpx.Response(200, json=board["value"])
        return httpx.Response(200, text=json.dumps({"event": req.url.params["event"], "n": len(hits)}))

    now = [1_000.0]
    got: list[tuple] = []
    poller = EspnPoller(lambda *a: got.append(a), http=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
                        clock=lambda: now[0])

    async def run() -> list[float]:
        waits = [await poller.poll_once()]  # game 2 is live: its summary
        now[0] += 5
        waits.append(await poller.poll_once())  # nothing changed, summary not due yet
        board["value"] = scoreboard(("1", "pre", ""), ("2", "in", "p2"))
        now[0] += 5
        waits.append(await poller.poll_once())  # a new play: summary at once
        now[0] += 16
        waits.append(await poller.poll_once())  # same scoreboard (skipped), summary due again
        board["value"] = scoreboard(("1", "in", ""), ("2", "post", "p9"))
        now[0] += 5
        waits.append(await poller.poll_once())  # game 1 starts, game 2's final summary
        board["value"] = scoreboard(("1", "post", ""), ("2", "post", "p9"))
        waits.append(await poller.poll_once())
        await poller.http.aclose()
        return waits

    waits = asyncio.run(run())
    assert waits == [5.0, 5.0, 5.0, 5.0, 5.0, 60.0]
    summaries = [h.rsplit("=", 1)[1] for h in hits if h != SCOREBOARD]
    assert summaries == ["2", "2", "2", "1", "2", "1"]
    assert [a[0] for a in got].count(SCOREBOARD) == 4 and poller.skipped == 2  # two repeated scoreboards
    line = envelope(*got[0])
    assert json.loads(line) == {"url": SCOREBOARD, "status": 200, "ms": 1_000_000, "body": board_at_start()}
    assert "\n" not in line and json.loads(envelope("u", 502, 1, "<html>bad gateway</html>"))["body"] == \
        "<html>bad gateway</html>"


def board_at_start() -> dict:
    return scoreboard(("1", "pre", ""), ("2", "in", "p1"))


def test_espn_poller_backs_off_on_errors():
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="busy")

    got: list = []
    poller = EspnPoller(lambda *a: got.append(a), http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))

    async def run() -> list[float]:
        out = [await poller.poll_once() for _ in range(3)]
        await poller.http.aclose()
        return out

    assert asyncio.run(run()) == [10.0, 20.0, 40.0]
    assert len(got) == 1 and got[0][1] == 503  # recorded once, then repeats skipped
