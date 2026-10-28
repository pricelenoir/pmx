from __future__ import annotations

import argparse

import pytest

from pmx import cli
from pmx.cli import AREAS, COMMANDS, build_parser
from pmx.cli.common import common_parser


@pytest.mark.parametrize("argv", [
    ["markets", "--series", "KXNFLGAME"], ["book", "T", "--depth", "3"], ["watch", "A", "B"],
    ["record", "--series", "KXHIGHNY"], ["compact", "--day", "2026-10-10"], ["account"],
    ["run", "--config", "c.toml", "--live"], ["config", "check"], ["halt", "smoke/T", "--reason", "x"], ["resume"],
    ["order", "buy", "T", "5", "@", "0.40"], ["order", "sell", "T", "1", "40c", "--mode", "live", "--ioc"],
    ["cancel", "--all"], ["cancel", "pmx-a1-b2-3"], ["status"], ["positions", "--mode", "paper"],
    ["advise", "T", "--p", "0.62", "--expires", "10m"], ["guard", "--guard-s", "5"], ["doctor"],
    ["ops", "install", "--record", "--backup"], ["ops", "uninstall"], ["ops", "backup", "--to", "/tmp/x"],
    ["data", "kalshi-history", "--seasons", "2025"], ["backtest", "report", "r1"], ["backtest", "compare", "a", "b"],
    ["backtest", "view", "r1"], ["backtest", "--nfl-history", "--seasons", "2026"],
])
def test_every_command_parses(argv):
    args = build_parser().parse_args(argv)
    assert args.cmd in COMMANDS


def test_env_flag_after_subcommand():
    assert build_parser().parse_args(["book", "T", "--env", "prod"]).env == "prod"


def test_commands_are_exactly_the_registered_subparsers_in_help_order():
    sub = next(a for a in build_parser()._actions if isinstance(a, argparse._SubParsersAction))
    names = ["markets", "book", "watch", "record", "compact", "account", "keygen", "setkey", "check",
             "serve", "up", "down", "tui", "data", "run", "config", "halt", "resume", "backtest", "risk", "perf",
             "export",
             "guard", "doctor", "ops", "order", "cancel", "status", "positions", "advise", "bench", "help"]
    assert list(sub.choices) == names
    assert list(COMMANDS) == names


def test_help_describes_every_command_and_all_prints_each(capsys):
    sub = next(a for a in build_parser()._actions if isinstance(a, argparse._SubParsersAction))
    assert all(a.help for a in sub._choices_actions), "every command has a one-line description"
    cli.main(["help", "--all"])
    out = capsys.readouterr().out
    for name in sub.choices:
        if name != "help":
            assert f"usage: pmx {name} " in out, name
    assert "usage: pmx bench soak " in out, "subcommands too"
    cli.main(["help", "bench", "feed"])
    assert capsys.readouterr().out.startswith("usage: pmx bench feed ")
    with pytest.raises(SystemExit):
        cli.main(["help", "nope"])


def test_each_area_registers_only_its_own_commands():
    owned = {}
    for area in AREAS:
        sub = argparse.ArgumentParser().add_subparsers()
        fns = area.register(sub, common_parser())
        assert set(fns) == set(sub.choices), area.__name__
        owned |= {name: area.__name__ for name in fns}
    assert owned["book"] == "pmx.cli.market" and owned["compact"] == "pmx.cli.record"
    assert owned["check"] == "pmx.cli.account" and owned["tui"] == "pmx.cli.feed" and owned["bench"] == "pmx.cli.bench"
    assert owned["run"] == owned["config"] == owned["halt"] == "pmx.cli.run"
    assert owned["risk"] == "pmx.cli.risk"
    assert owned["backtest"] == "pmx.cli.backtest"
    assert owned["order"] == owned["status"] == owned["advise"] == "pmx.cli.control"
    assert owned["guard"] == owned["doctor"] == owned["ops"] == "pmx.cli.ops"
    assert owned["perf"] == owned["export"] == "pmx.cli.perf"
    assert owned["data"] == "pmx.cli.data"


def test_main_runs_async_commands_in_a_loop_and_sync_ones_directly(monkeypatch):
    calls = []

    async def fake_async(args):
        calls.append(("async", args.cmd))

    def fake_sync(args):
        calls.append(("sync", args.cmd))

    monkeypatch.setattr(cli.config, "load_env", lambda: None)
    monkeypatch.setitem(COMMANDS, "account", fake_async)
    monkeypatch.setitem(COMMANDS, "compact", fake_sync)
    cli.main(["account"])
    cli.main(["compact"])
    assert calls == [("async", "account"), ("sync", "compact")]
