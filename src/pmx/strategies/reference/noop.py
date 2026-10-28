"""`noop`: does nothing. Exercises slot lifecycle (start, routing, stop) in system tests."""

from __future__ import annotations

from pmx.plugins import strategy
from pmx.strategies.base import Strategy


@strategy("noop")
class Noop(Strategy):
    pass
