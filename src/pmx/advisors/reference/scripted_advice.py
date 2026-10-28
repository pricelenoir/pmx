"""`scripted_advice`: sends advice from a script on a schedule, so system tests and demos of the advice
path are deterministic. Same step times as the `scripted` strategy:

    loop = false
    [[steps]]
    at = "+10s"                 # after the advisor starts (or the loop restarts)
    target = "KXNFLGAME-…-HOU"  # a ticker or an entity key; or `series = "KXNFLGAME"` for every open market
    p = 0.62                    # and/or direction = "buy" | "sell" | "flat"
    confidence = 0.8
    expires = "30s"             # from when it's sent
    rationale = "scripted"

For system tests of the advisor path, `think_s` makes each step a stand-in model call through
`env.call` first (so timeouts and budgets apply), and `fail` raises at the end of every pass (so the
worker restarts it). It reads nothing it doesn't need, is deterministic, and has learned nothing
(`trained_until` is the epoch): backtests may run it."""

from __future__ import annotations

import asyncio
import contextlib
import re
import tomllib
from typing import ClassVar, Literal

import msgspec

from pmx.advisors.base import AdviceSink, AdvisorEnv, AdvisorParams, StateView
from pmx.engine.events import Advice
from pmx.plugins import advisor

_SPAN = re.compile(r"\+?(\d+(?:\.\d+)?)(ms|s|m|h)")
_UNIT_MS = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000}


def _ms(text: str) -> int:
    m = _SPAN.fullmatch(text.strip())
    if m is None:
        raise ValueError(f"{text!r}: expected a span like '+10s', '30s', '2m'")
    return round(float(m[1]) * _UNIT_MS[m[2]])


class Step(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    at: str
    target: str = ""
    series: str = ""
    p: float | None = None
    direction: Literal["buy", "sell", "flat"] | None = None
    confidence: float = 1.0
    expires: str = "60s"
    rationale: str = "scripted"

    def __post_init__(self) -> None:
        _ms(self.at)
        _ms(self.expires)
        if bool(self.target) == bool(self.series):
            raise ValueError(f"step at {self.at!r}: give `target` or `series`")
        if self.p is None and self.direction is None:
            raise ValueError(f"step at {self.at!r}: give p and/or direction")


@advisor("scripted_advice")
class ScriptedAdvice:
    deterministic: ClassVar[bool] = True
    trained_until: ClassVar[str] = "1970-01-01"

    class Params(AdvisorParams, frozen=True, kw_only=True):
        script: str = ""  # a TOML file of steps
        steps: list[Step] = []  # or the steps inline
        loop: bool = False
        think_s: float = 0.0  # a stand-in model call per step, through `env.call`
        fail: str = ""  # raise this at the end of every pass

        def __post_init__(self) -> None:
            if self.script:
                if self.steps:
                    raise ValueError("give `script` or `steps`, not both")
                with open(self.script, "rb") as f:
                    raw = tomllib.load(f)
                msgspec.structs.force_setattr(self, "steps", msgspec.convert(raw.get("steps", []), list[Step]))
                msgspec.structs.force_setattr(self, "loop", self.loop or raw.get("loop", False))
            if not self.steps:
                raise ValueError("no steps: give `script` (a TOML file) or `steps`")

    def __init__(self, name: str, params: Params, env: AdvisorEnv) -> None:
        self.name, self.p, self.env = name, params, env

    async def run(self, view: StateView, sink: AdviceSink, stop: asyncio.Event) -> None:
        while not stop.is_set():
            t0 = self.env.now_ms()
            for step in self.p.steps:
                wait = t0 + _ms(step.at) - self.env.now_ms()
                if wait > 0:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), wait / 1000)
                    if stop.is_set():
                        return
                if self.p.think_s:
                    await self.env.call(asyncio.sleep(self.p.think_s), what="think")
                now = self.env.now_ms()
                targets = [step.target] if step.target else view.tickers(series=step.series)
                for target in targets:
                    sink.advise(Advice(self.name, target, step.p, step.direction, step.confidence,
                                       now + _ms(step.expires), step.rationale, now))
            if self.p.fail:
                raise RuntimeError(self.p.fail)
            if not self.p.loop:
                return
