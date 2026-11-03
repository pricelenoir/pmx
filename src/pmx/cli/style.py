"""How pmx's terminal output looks: colour for meaning (green good, red bad, yellow needs attention), dim for
detail, bold for what to read first, a few plain symbols (● ○ ✓ ✗ ▲ ▼). Only on a terminal: piped or redirected
output stays plain text, and `NO_COLOR` turns it off (`FORCE_COLOR` turns it on)."""

from __future__ import annotations

import os
import re
import sys
from typing import TextIO

_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def enabled(stream: TextIO | None = None) -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    stream = stream or sys.stdout
    return hasattr(stream, "isatty") and stream.isatty() and os.environ.get("TERM") != "dumb"


def _paint(code: str, text: str, stream: TextIO | None = None) -> str:
    return f"\x1b[{code}m{text}\x1b[0m" if text and enabled(stream) else text


def bold(t: str) -> str:
    return _paint("1", t)


def dim(t: str) -> str:
    return _paint("2", t)


def green(t: str) -> str:
    return _paint("32", t)


def red(t: str) -> str:
    return _paint("31", t)


def yellow(t: str) -> str:
    return _paint("33", t)


def cyan(t: str) -> str:
    return _paint("36", t)


def err(t: str) -> str:
    """For stderr: red and bold when stderr is a terminal."""
    return _paint("1;31", t, sys.stderr)


def visible(t: str) -> int:
    """Width on screen, without colour codes."""
    return len(_ANSI.sub("", t))


def pad(t: str, n: int, *, right: bool = False) -> str:
    gap = " " * max(0, n - visible(t))
    return gap + t if right else t + gap


def usd(micros: int | None, *, sign: bool = True, color: bool = True) -> str:
    """Money: +$1.25 green, -$0.40 red, $0.00 dim."""
    if micros is None:
        return dim("-")
    s = f"{'+' if sign and micros > 0 else '-' if micros < 0 else ''}${abs(micros) / 1e6:,.2f}"
    if not color or not sign:
        return s
    return green(s) if micros > 0 else red(s) if micros < 0 else dim(s)


_STATES = {
    "up": ("●", green), "active": ("●", green), "running": ("●", green), "ok": ("●", green), "open": ("●", green),
    "filled": ("●", green), "down": ("●", red), "failed": ("●", red), "error": ("●", red), "rejected": ("●", red),
    "halted": ("●", yellow), "warming": ("●", yellow), "queued": ("●", yellow), "pending": ("●", yellow),
    "activating": ("●", yellow), "unknown": ("○", dim), "idle": ("○", dim), "inactive": ("○", dim),
    "canceled": ("○", dim), "not loaded": ("○", dim), "not running": ("○", dim),
}


def dot(state: str) -> str:
    """A coloured dot for a state word: ● green up/active, ● red down/failed, ● yellow halted, ○ dim idle."""
    sym, color = _STATES.get(state, ("○", dim))
    return color(sym)


def word(state: str) -> str:
    """The state word in its colour."""
    _, color = _STATES.get(state, ("○", dim))
    return color(state)


def mark(ok: bool | None) -> str:
    return green("✓") if ok else red("✗") if ok is False else yellow("?")


def heading(title: str, detail: str = "") -> str:
    """`title` in bold, then `detail` dimmed: the first line of a command's output."""
    return bold(title) + (f"  {dim(detail)}" if detail else "")


def table(rows: list[list[str]], *, right: set[int] = frozenset(), header: bool = True, indent: int = 2) -> list[str]:
    """Aligned columns (by visible width); the first row dimmed as a header; `right` aligns those columns right."""
    if not rows:
        return []
    widths = [max(visible(r[i]) for r in rows if i < len(r)) for i in range(max(len(r) for r in rows))]
    out = []
    for n, r in enumerate(rows):
        cells = [pad(c, widths[i], right=i in right) for i, c in enumerate(r)]
        line = " " * indent + "  ".join(cells).rstrip()
        out.append(dim(line) if header and n == 0 else line)
    return out
