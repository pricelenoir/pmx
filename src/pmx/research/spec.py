"""An experiment's spec: `spec.md` = TOML front matter between `+++` lines, then the hypothesis in prose.

  +++
  series = ["KXHIGH*"]
  variants_max = 6
  params = { side = "sell" }                         # base parameters (validation/holdout: the frozen choice)
  grid = { band = [[0.03, 0.10], [0.05, 0.15]], at = ["T-6h", "T-2h"] }   # discovery tries every combination
  pass = { ci_low_c = 0.0, min_events = 100 }        # evaluate.CRITERIA
  +++
  # Title
  ## Mechanism …

Any other front-matter key is passed to `run.py` as `spec.config`."""

from __future__ import annotations

import itertools
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pmx.research.evaluate import CRITERIA
from pmx.research.windows import Stage

FENCE = "+++"
_KNOWN = {"series", "params", "grid", "variants_max", "pass"}


@dataclass
class Spec:
    title: str
    body: str
    series: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    grid: dict[str, list[Any]] = field(default_factory=dict)
    variants_max: int = 1
    pass_: dict[str, Any] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)

    def variants(self, stage: Stage) -> list[dict[str, Any]]:
        """Discovery: every grid combination over `params`; later stages: `params` alone."""
        if stage != "discovery" or not self.grid:
            return [dict(self.params)]
        keys = list(self.grid)
        return [self.params | dict(zip(keys, combo, strict=True))
                for combo in itertools.product(*(self.grid[k] for k in keys))]


def parse(text: str, where: str = "spec.md") -> Spec:
    lines = text.splitlines()
    if not lines or lines[0].strip() != FENCE:
        raise ValueError(f"{where}: must start with a {FENCE} line (TOML front matter)")
    try:
        end = next(i for i in range(1, len(lines)) if lines[i].strip() == FENCE)
    except StopIteration:
        raise ValueError(f"{where}: front matter has no closing {FENCE}") from None
    try:
        front = tomllib.loads("\n".join(lines[1:end]))
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"{where}: front matter: {e}") from None
    body = "\n".join(lines[end + 1:]).strip()
    title = next((ln[2:].strip() for ln in body.splitlines() if ln.startswith("# ")), "")
    if not title:
        raise ValueError(f"{where}: needs a '# Title' line after the front matter")
    grid, params, series = front.get("grid", {}), front.get("params", {}), front.get("series", [])
    if not isinstance(grid, dict) or any(not isinstance(v, list) or not v for v in grid.values()):
        raise ValueError(f"{where}: grid must map each parameter to a non-empty list of values")
    if not isinstance(params, dict):
        raise ValueError(f"{where}: params must be a table, e.g. params = {{ at = \"T-6h\" }}")
    series = [series] if isinstance(series, str) else series
    if not isinstance(series, list) or not all(isinstance(x, str) for x in series):
        raise ValueError(f"{where}: series must be a list of series tickers or globs")
    variants_max = front.get("variants_max", 1)
    if type(variants_max) is not int or variants_max < 1:
        raise ValueError(f"{where}: variants_max must be a positive integer")
    pass_ = front.get("pass", {})
    unknown = set(pass_) - set(CRITERIA)
    if unknown:
        raise ValueError(f"{where}: unknown pass criteria {', '.join(sorted(unknown))}; known: {', '.join(CRITERIA)}")
    spec = Spec(title, body, list(series), dict(params), grid, variants_max, dict(pass_),
                {k: v for k, v in front.items() if k not in _KNOWN})
    n = len(spec.variants("discovery"))
    if n > spec.variants_max:
        raise ValueError(f"{where}: the grid has {n} variants, more than variants_max = {spec.variants_max}")
    return spec


def load(path: Path) -> Spec:
    return parse(path.read_text(), str(path))
