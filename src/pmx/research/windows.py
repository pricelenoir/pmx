"""Data windows: which markets each research stage may see and is judged on, by close time (when the outcome
became known), from the workspace's `windows.toml`.

  discovery   sees markets closing before `discovery_end`; judged on them
  validation  sees everything before `validation_end` (fit on discovery-era data);
              judged on [discovery_end, validation_end)
  holdout     sees everything; judged on markets closing on or after `validation_end` — once per experiment

Exploration (notebooks) gets the discovery view."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

Stage = Literal["discovery", "validation", "holdout"]
STAGES: tuple[Stage, ...] = ("discovery", "validation", "holdout")


def day_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp() * 1000)


@dataclass(frozen=True)
class Windows:
    discovery_end: date
    validation_end: date

    def __post_init__(self) -> None:
        if not self.discovery_end < self.validation_end:
            raise ValueError("windows: discovery_end must come before validation_end")

    @classmethod
    def load(cls, path: Path) -> Windows:
        raw = tomllib.loads(path.read_text())
        ends = []
        for key in ("discovery_end", "validation_end"):
            v = raw.get(key)
            if type(v) is not date:  # a TOML date (2026-01-01), not a string or a datetime
                raise ValueError(f"{path}: {key} must be a date like 2026-01-01 (unquoted)")
            ends.append(v)
        return cls(*ends)

    def visible_end_ms(self, stage: Stage) -> int | None:
        """Markets closing at or after this are hidden from the stage (None: nothing hidden)."""
        return {"discovery": day_ms(self.discovery_end), "validation": day_ms(self.validation_end),
                "holdout": None}[stage]

    def judged_ms(self, stage: Stage) -> tuple[int | None, int | None]:
        """[start, end) of close times a stage is evaluated on."""
        d, v = day_ms(self.discovery_end), day_ms(self.validation_end)
        return {"discovery": (None, d), "validation": (d, v), "holdout": (v, None)}[stage]

    def describe(self, stage: Stage) -> str:
        return {"discovery": f"markets closing before {self.discovery_end}",
                "validation": f"markets closing {self.discovery_end} → {self.validation_end}",
                "holdout": f"markets closing on or after {self.validation_end}"}[stage]
