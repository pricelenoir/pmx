"""Terminal styling: colour only on a terminal (NO_COLOR / FORCE_COLOR respected), alignment by visible width."""

from __future__ import annotations

import io

import pytest

from pmx.cli import style


class Tty(io.StringIO):
    def isatty(self) -> bool:
        return True


@pytest.fixture
def tty(monkeypatch):
    """Colour on (pytest owns sys.stdout, so force it rather than fake a terminal)."""
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("FORCE_COLOR", "1")


def test_plain_when_piped(monkeypatch):
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    monkeypatch.setattr(style.sys, "stdout", io.StringIO())
    assert style.green("ok") == "ok" and style.usd(-1_500_000) == "-$1.50" and style.mark(True) == "✓"


def test_colour_on_a_terminal_and_the_switches(tty, monkeypatch):
    assert style.green("ok") == "\x1b[32mok\x1b[0m"
    assert style.usd(1_250_000) == "\x1b[32m+$1.25\x1b[0m" and style.usd(-400_000) == "\x1b[31m-$0.40\x1b[0m"
    assert style.usd(0) == "\x1b[2m$0.00\x1b[0m" and style.usd(5, sign=False) == "$0.00"
    monkeypatch.setenv("NO_COLOR", "1")  # wins over FORCE_COLOR
    assert style.green("ok") == "ok"
    monkeypatch.delenv("NO_COLOR")
    monkeypatch.delenv("FORCE_COLOR")
    assert style.enabled(Tty()) and not style.enabled(io.StringIO())
    monkeypatch.setenv("TERM", "dumb")
    assert not style.enabled(Tty())


def test_tables_align_by_what_you_see(tty):
    rows = [["name", "pnl"], [style.bold("portfolio"), style.usd(1_000_000)], ["s", style.usd(-12_340_000)]]
    lines = style.table(rows, right={1}, indent=0)
    assert len({style.visible(line) for line in lines}) == 1  # every row the same width on screen
    assert style.visible(style.pad(style.red("ab"), 5)) == 5


def test_states_have_a_dot_and_a_colour(tty):
    assert style.dot("up").startswith("\x1b[32m") and style.dot("failed").startswith("\x1b[31m")
    assert style.dot("halted").startswith("\x1b[33m") and "○" in style.dot("something new")
