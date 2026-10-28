"""Writes `v3.ndjson`: what a viewer receives from the real publisher + hub over a scripted session,
with one of every v3 message. pytest (`test_feed.py`) checks the file still matches this script's
output and cargo test parses it, so the two sides can't drift. Run from the repo root:

    uv run python tests/fixtures/feed/make_v3.py
"""

from __future__ import annotations

from pathlib import Path

from pmx.core.types import BookDelta, BookSnapshot, Contract, Fill, StreamReset, Trade
from pmx.engine.events import (
    AccountSnapshot,
    Advice,
    Exposure,
    Forecast,
    Halt,
    LimitUsage,
    LogEvent,
    OrderUpdate,
    PortfolioSnapshot,
    RiskReject,
    ScopeStats,
    SlotFill,
    SlotStatus,
    SourceStatus,
)
from pmx.feed import proto
from pmx.feed.hub import FeedHub
from pmx.feed.publisher import Publisher

T = 1_792_000_000_000
HOU, JAC, B68 = "DEMO-NFL-26OCT18HOUJAC-HOU", "DEMO-NFL-26OCT18HOUJAC-JAC", "DEMO-HIGHNY-26OCT18-B68"
PATH = Path(__file__).with_name("v3.ndjson")


def session() -> list[bytes]:
    h = FeedHub(proto.Hello("kalshi", "prod", "engine"), clock=lambda: T)
    h.status = proto.Status(True, "websocket connected", T)
    pub = Publisher(h)
    viewer = h.attach()
    at = iter(range(T + 1, T + 10_000))

    def ev(e: object) -> None:
        pub.on_event(e, next(at))

    h.set_status(True, "engine connected", "engine", T, dropped=1234)
    ev(SourceStatus("weather", False, "poll timeout", T))
    ev(AccountSnapshot(25_000_000_000, [], T))
    pub.set_models(["fair", "mid"])
    for ticker, event, title, sub in ((HOU, "DEMO-NFL-26OCT18HOUJAC", "Houston at Jacksonville: Houston", "Houston"),
                                      (JAC, "DEMO-NFL-26OCT18HOUJAC", "Houston at Jacksonville: Jacksonville",
                                       "Jacksonville"),
                                      (B68, "DEMO-HIGHNY-26OCT18", "NYC high temp on Oct 18: 68-69°F", "68° to 69°")):
        ev(Contract("kalshi", ticker, event, title, sub, "open", T + 86_400_000, series_ticker=event.rsplit("-", 1)[0],
                    event_ts_ms=T + 3_600_000))
    ev(BookSnapshot("kalshi", HOU, tuple((5500 - i * 100, 10_000 if i == 0 else 2_500) for i in range(12)),
                    ((5700, 400), (5800, 12_000)), T))  # 12 bid levels: viewers get the top 10
    ev(BookSnapshot("kalshi", JAC, ((4200, 3_000),), ((4400, 5_000),), T))
    ev(BookSnapshot("kalshi", B68, (), ((3300, 1_200),), T))  # one-sided, and no model covers it
    ev(BookDelta("kalshi", HOU, "ask", 5700, -100, T))  # held back: conflated into the next book
    ev(Trade("kalshi", "t1", HOU, 5600, 300, "bid", T))
    ev(SlotFill(f"mm/{HOU}", "paper", Fill("kalshi", "f1", "o1", HOU, "bid", 5500, 500, 4_331, False, T)))
    pub.apply(proto.EntityMsg("demo:nfl:HOU-JAC", "demo_sports", [HOU, JAC], "Q3 4:12 HOU 17-14 JAC",
                              [("score", "HOU 17 - 14 JAC"), ("possession", "JAC, 2nd & 6")], T), T)
    pub.apply(proto.EntityMsg("wx:KNYC:2026-10-18", "demo_weather", [B68], "KNYC 64°F · high so far 66°F",
                              [("now", "64°F"), ("high so far", "66°F")], T), T)
    ev(Advice("news", HOU, 0.61, "buy", 0.8, T + 60_000, "injury report: JAC QB out", T))
    ev(Forecast("fair", HOU, 0.582, T, T - 1))
    ev(Forecast("mid", HOU, 0.56, T, T))
    ev(SlotStatus(f"mm/{HOU}", "mm", HOU, "paper", "active", 500, 0, 25_000, 4_331, 1, "quote 55¢ / 57¢", T))
    ev(SlotStatus(f"smoke/{B68}", "smoke", B68, "live", "halted", 0, -120_000, 0, 9_000, 0, "halted: max_loss", T))
    ev(OrderUpdate(f"mm/{HOU}", "paper", "c1", "p-1", HOU, "bid", 5500, 500, 0, "open", T))
    ev(OrderUpdate(f"mm/{HOU}", "paper", "c0", "p-0", HOU, "ask", 5800, 500, 500, "filled", T))
    ev(RiskReject(f"smoke/{B68}", B68, "bid", 3200, 1_000, "max_loss", "day loss $1.20 >= $1.00", T))
    ev(LogEvent("warn", f"smoke/{B68}", "slot halted: max_loss", T))
    ev(Halt(f"smoke/{B68}", "max_loss", True, T))
    scopes = [ScopeStats("portfolio", "paper", 1_250_000, 25_000, 74_331, 1_200_669, 310_000, 1_500_000, 299_331,
                         2_800_000, -2_750_000, 410_000, 1_900_000, 2_800_000, 2_750_000, 1)]
    ev(PortfolioSnapshot(T, scopes, [Exposure("demo:nfl:HOU-JAC", "paper", [HOU], ["mm"], 2_800_000,
                                              {"HOU": 2_175_669, "JAC": -2_750_000, "tie": -2_750_000}, -2_750_000,
                                              410_000, 300_000)],
                         [LimitUsage("global", "max_capital_at_risk", 2_800_000, 500_000_000, False)]))
    pub.seed_equity("portfolio", "paper", [(T - 2_000, 1_000_000, 0), (T - 1_000, 1_500_000, 0)])
    pub.publish_perf(proto.PerfMsg("mm", "paper", "7d", T, proto.PerfStats(net=1_200_669, fills=12, contracts=600),
                                   {"market": [proto.BucketRow(HOU, 12, 600, 1_200_669)]},
                                   proto.Verdict("inconclusive", "12 fills over 1 day: too little to say")))
    pub.flush(T + 1_000)  # what was held back goes out
    ev(BookDelta("kalshi", B68, "ask", 3300, -5_000, T))  # impossible: the book is stale until a snapshot
    ev(StreamReset("kalshi", "seq gap", T))
    out = []
    while not viewer.queue.empty():
        out.append(viewer.queue.get_nowait())
    return out


if __name__ == "__main__":
    PATH.write_bytes(b"".join(session()))
    print(f"wrote {PATH}")
