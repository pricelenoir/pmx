from __future__ import annotations

import asyncio
import json
from datetime import date

import msgspec
import pyarrow.parquet as pq
import pytest

from conftest import delta, snapshot
from pmx import plugins
from pmx.connectors.base import Control, Frame
from pmx.connectors.kalshi.decode import KalshiDecoder
from pmx.core.types import BookSnapshot, Contract, StreamReset
from pmx.engine.events import Advice
from pmx.plugins import PluginError
from pmx.recorder import retention
from pmx.recorder.compact import compact_day
from pmx.recorder.rawlog import (
    RawLogWriter,
    decode_event,
    encode_event,
    log_files,
    read_records,
    register_event_type,
    sources,
)
from pmx.recorder.replay import replay, replay_merged
from pmx.recorder.sink import RecordingSink, looks_secret
from pmx.sources.base import SourceSpec, build_source

T0 = 1_791_590_400_000_000_000  # 2026-10-10T00:00:00Z in ns
HOUR = 3_600_000_000_000


def _contract(ticker="A") -> Contract:
    return Contract("kalshi", ticker, "E-1", "Title", "Sub", "open", None, series_ticker="E")


def test_rawlog_round_trip_and_hourly_rotation(tmp_path):
    w = RawLogWriter(tmp_path, "kalshi")
    w.write_contracts([_contract()], T0)
    w.write(Control(T0 + 1, "connected", {"tickers": 1}))
    w.write(Frame(T0 + 2, '{"type":\n"x"}'))
    w.write(Frame(T0 + HOUR, '{"type":"y"}'))
    w.close()
    files = log_files(tmp_path, "kalshi")
    assert [f.name.split("-")[0] for f in files] == ["00", "01"]
    recs = [r for f in files for r in read_records(f)]
    assert [r[1] for r in recs] == ["m", "c", "f", "f"]
    assert recs[2][2] == '{"type": "x"}'  # raw newline flattened, JSON unchanged
    assert log_files(tmp_path, "kalshi", start=date(2026, 10, 11)) == []


def test_truncated_file_reads_up_to_last_flushed_frame(tmp_path):
    w = RawLogWriter(tmp_path, "kalshi")
    w.write(Frame(T0, '{"type":"a"}'))
    w.flush()
    path = log_files(tmp_path, "kalshi")[0]
    first = path.stat().st_size
    w.write(Frame(T0 + 1, '{"type":"b"}'))
    w.flush()
    second = path.stat().st_size
    w.close()
    path.write_bytes(path.read_bytes()[: (first + second) // 2])  # killed mid-frame
    assert [r[2] for r in read_records(path)] == ['{"type":"a"}']


def _record_session(tmp_path):
    w = RawLogWriter(tmp_path, "kalshi")
    w.write_contracts([_contract()], T0)
    w.write(Control(T0 + 1, "connected"))
    w.write(Frame(T0 + 2_000_000, snapshot("A", [["0.4000", "10.00"]], [["0.5500", "5.00"]], seq=1)))
    w.write(Frame(T0 + 3_000_000, delta("A", "0.4100", "2.00", "yes", seq=2)))
    w.write(Frame(T0 + 4_000_000, delta("A", "0.3000", "1.00", "yes", seq=3)))  # below top: no bbo row
    w.write(Frame(T0 + 5_000_000, delta("A", "0.4100", "1.00", "yes", seq=9)))  # gap
    w.write(Frame(T0 + 6_000_000, delta("A", "0.4200", "1.00", "yes", seq=10)))  # ignored until snapshot
    w.write(Control(T0 + 7_000_000, "connected"))
    w.write(Frame(T0 + 8_000_000, snapshot("A", [["0.4300", "1.00"]], [], seq=1)))
    w.close()


def test_replay_matches_live_decoding(tmp_path):
    _record_session(tmp_path)
    events = [ev for _, ev in replay(tmp_path, "kalshi", KalshiDecoder())]
    kinds = [type(e).__name__ for e in events]
    assert kinds == ["Contract", "StreamReset", "BookSnapshot", "BookDelta", "BookDelta", "StreamReset",
                     "BookDelta", "StreamReset", "BookSnapshot"]
    assert isinstance(events[2], BookSnapshot) and events[2].asks == ((4500, 500),)
    assert isinstance(events[5], StreamReset) and "gap" in events[5].reason


def test_compact_writes_bbo_and_contracts(tmp_path):
    _record_session(tmp_path)
    counts = compact_day(tmp_path, "kalshi", date(2026, 10, 10), KalshiDecoder())
    bbo = pq.read_table(tmp_path / "parquet/kalshi/bbo/2026-10-10.parquet").to_pylist()
    assert [(r["bid"], r["bid_qty"], r["ask"]) for r in bbo] == [
        (4000, 1000, 4500), (4100, 200, 4500), (None, None, None), (4300, 100, None)
    ]
    assert counts["contracts"] == 1
    assert pq.read_table(tmp_path / "parquet/kalshi/contracts.parquet").column("ticker").to_pylist() == ["A"]


# ---- any source ----------------------------------------------------------------------------------


class Clocks:
    def __init__(self, wall_ns: int = T0) -> None:
        self.wall, self.mono = wall_ns, 0

    def tick(self, s: float, *, slept: float = 0) -> None:
        self.wall += int((s + slept) * 1e9)
        self.mono += int(s * 1e9)


def sink_with(tmp_path, clocks: Clocks) -> RecordingSink:
    return RecordingSink(tmp_path, wall_ns=lambda: clocks.wall, mono_ns=lambda: clocks.mono)


def test_sink_writes_one_log_per_source_and_rotates_each_hourly(tmp_path):
    clocks = Clocks()
    sink = sink_with(tmp_path, clocks)
    sink.record("espn", "f", '{"a":1}')
    sink.write("kalshi", Frame(clocks.wall, '{"type":"x"}'))
    clocks.tick(3_600)
    sink.record("espn", "f", '{"a":2}')
    sink.close()
    assert sources(tmp_path) == ["espn", "kalshi"]
    assert [p.name[:2] for p in log_files(tmp_path, "espn")] == ["00", "01"]
    assert [r[1:] for p in log_files(tmp_path, "espn") for r in read_records(p)] == [("f", '{"a":1}'), ("f", '{"a":2}')]
    assert sink.records == {"espn": 2, "kalshi": 1}


def test_e_records_round_trip_engine_and_domain_events(tmp_path):
    @register_event_type
    class Prior(msgspec.Struct, frozen=True):
        entity: str
        logit: float

    clocks = Clocks()
    sink = sink_with(tmp_path, clocks)
    advice = Advice("news", "T-1", 0.6, "buy", 0.8, 9_000, "why", 1_000)
    sink.record_event("advisors", advice)
    clocks.tick(1)
    sink.record_event("advisors", Prior("nfl:x", 0.25))
    sink.close()
    events = [ev for _, ev in replay(tmp_path, "advisors", KalshiDecoder())]
    assert events == [advice, Prior("nfl:x", 0.25)]
    assert decode_event(encode_event(advice)) == advice
    with pytest.raises(ValueError, match="unknown recorded event type"):
        decode_event('{"type":"Nope","event":{}}')

    class Unregistered(msgspec.Struct):
        x: int

    with pytest.raises(ValueError, match="isn't a registered event type"):
        encode_event(Unregistered(1))


def test_replay_merges_sources_by_receive_time(tmp_path):
    k = RawLogWriter(tmp_path, "kalshi")
    k.write(Frame(T0 + 10, snapshot("A", [["0.4000", "1.00"]], [], seq=1)))
    k.write(Frame(T0 + 30, delta("A", "0.4000", "1.00", "yes", seq=2)))
    k.close()
    a = RawLogWriter(tmp_path, "advisors")
    a.write_line(T0 + 20, "e", encode_event(Advice("x", "A", 0.5, None, 1.0, 9, "", 0)))
    a.write_line(T0 + 30, "e", encode_event(Advice("x", "A", 0.6, None, 1.0, 9, "", 0)))
    a.close()
    merged = [(ns - T0, src, type(ev).__name__) for ns, src, ev in
              replay_merged(tmp_path, {"kalshi": KalshiDecoder(), "advisors": None})]
    assert merged == [(10, "kalshi", "BookSnapshot"), (20, "advisors", "Advice"), (30, "kalshi", "BookDelta"),
                      (30, "advisors", "Advice")]


def test_credentials_are_never_recorded(tmp_path):
    sink = sink_with(tmp_path, Clocks())
    sink.record("espn", "f", '{"headers":{"KALSHI-ACCESS-SIGNATURE":"abc"}}')
    sink.record("espn", "f", '{"url":"https://x/y?api_key=0123456789abcdef"}')
    sink.record("espn", "f", '{"ok":"a secret plan uses a token bucket"}')
    sink.close()
    recs = [r for p in log_files(tmp_path, "espn") for r in read_records(p)]
    assert [r[1] for r in recs] == ["c", "c", "f"] and sink.redacted == 2
    assert "abc" not in recs[0][2] and json.loads(recs[0][2])["event"] == "redacted"
    assert looks_secret("-----BEGIN OPENSSH PRIVATE KEY-----") and looks_secret("Authorization: Bearer x")
    assert looks_secret("https://a.espncdn.com/i/teamlogos/nfl/500/hou.png") is None


def test_a_sleep_writes_a_gap_that_compact_and_replay_surface(tmp_path):
    clocks = Clocks()
    sink = sink_with(tmp_path, clocks)
    sink.write_contracts("kalshi", [_contract()])
    sink.write("kalshi", Frame(clocks.wall, snapshot("A", [["0.4000", "10.00"]], [["0.5500", "5.00"]], seq=1)))
    clocks.tick(40)  # 40 s apart but awake: not a gap
    sink.record("espn", "f", '{"a":1}')
    clocks.tick(1, slept=300)  # the lid closed for 5 minutes
    sink.write("kalshi", Frame(clocks.wall, delta("A", "0.4100", "2.00", "yes", seq=2)))
    sink.close()
    assert sink.sleep_gaps == 1
    for source in ("kalshi", "espn"):
        kinds = [(r[1], r[2]) for p in log_files(tmp_path, source) for r in read_records(p) if r[1] == "c"]
        assert kinds == [("c", '{"event":"sleep_gap","gap_ms":300000}')]
    events = [ev for _, ev in replay(tmp_path, "kalshi", KalshiDecoder())]
    assert [type(e).__name__ for e in events] == ["Contract", "BookSnapshot", "StreamReset", "BookDelta"]
    assert events[2].reason == "sleep_gap"
    assert compact_day(tmp_path, "kalshi", date(2026, 10, 10), KalshiDecoder())["sleep_gaps"] == 1


def test_retention_deletes_only_old_compacted_days(tmp_path):
    for source, day in (("kalshi", "2026-08-01"), ("kalshi", "2026-10-01"), ("espn", "2026-08-01")):
        d = tmp_path / "raw" / source / day
        d.mkdir(parents=True)
        (d / "00-1.log.zst").write_bytes(b"x")
    (tmp_path / "parquet" / "kalshi" / "bbo").mkdir(parents=True)
    (tmp_path / "parquet" / "kalshi" / "bbo" / "2026-08-01.parquet").write_bytes(b"x")
    expired = retention.plan(tmp_path, 30, date(2026, 10, 18))
    assert [(e.source, str(e.day), e.compacted) for e in expired] == [("espn", "2026-08-01", False),
                                                                       ("kalshi", "2026-08-01", True)]
    assert retention.apply(expired) == 1
    assert not (tmp_path / "raw/kalshi/2026-08-01").exists() and (tmp_path / "raw/espn/2026-08-01").exists()
    assert (tmp_path / "raw/kalshi/2026-10-01").exists()


# ---- recording any configured source ------------------------------------------------------------------


@plugins.source("r1_fake")
class FakeFeed:
    class Params(msgspec.Struct, frozen=True):
        n: int = 2

    def __init__(self, name, params, env=None) -> None:
        self.name, self.p = name, params

    async def run(self, sink, stop) -> None:
        for i in range(self.p.n):
            sink.record(self.name, "f", json.dumps({"tick": i}))
            sink.emit(Advice("x", "T", 0.5, None, 1.0, 9, "", 0))  # a parsed event: not the recorder's business


def test_any_configured_source_records_raw_through_the_sink(tmp_path):
    src = build_source(SourceSpec("feed", "r1_fake", {"n": 3}))
    sink = RecordingSink(tmp_path)
    asyncio.run(src.run(sink, asyncio.Event()))
    sink.close()
    assert [r[2] for p in log_files(tmp_path, "feed") for r in read_records(p)] == [
        '{"tick": 0}', '{"tick": 1}', '{"tick": 2}']
    assert sink.emitted == 3
    with pytest.raises(PluginError, match="unknown kind 'nope'"):
        build_source(SourceSpec("x", "nope"))
    with pytest.raises(PluginError, match="unknown params key 'm'"):
        build_source(SourceSpec("x", "r1_fake", {"m": 1}))
