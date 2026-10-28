"""Models (ARCHITECTURE *Models*): turn events into `Forecast`s for linked markets. Registered with
`@model(kind)`, built per config instance as `cls(name, params, env)`, run by the engine (or a worker
process, `models/runtime.py`). Stateful and incremental; no I/O in `on_event`; deterministic."""

from __future__ import annotations

import logging
from pathlib import Path

from pmx.engine.engine import Model
from pmx.engine.events import Forecast

__all__ = ["Model", "ModelEnv", "forecast"]


class ModelEnv:
    """What a model gets besides its params: where artifacts live, and a logger for load-time messages."""

    def __init__(self, data_root: Path | None = None, log: logging.Logger | None = None) -> None:
        self.data_root = data_root or Path("data")
        self.log = log or logging.getLogger("pmx.models")


def forecast(name: str, ticker: str, p: float, now_ms: int, data_ts_ms: int, *, version: str = "",
             meta: dict[str, float] | None = None) -> Forecast:
    """A `Forecast` with `p` clamped to [0, 1]."""
    return Forecast(name, ticker, min(max(float(p), 0.0), 1.0), now_ms, data_ts_ms, meta or {}, version)
