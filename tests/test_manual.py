"""The manual stays true: every command has examples, every example parses, every `pmx …` it names exists."""

from __future__ import annotations

import re
import shlex

import pytest

from pmx.cli import COMMANDS, build_parser, main, manual


def _texts() -> list[str]:
    return [manual.OVERVIEW, *(text for _, text in manual.GUIDES.values()), *manual.EXAMPLES.values()]


def _example_lines() -> list[str]:
    """Command lines in examples and guides: `pmx …` up to a two-space comment."""
    out = []
    for text in _texts():
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("pmx ") and not line.startswith("pmx —"):
                out.append(re.split(r"\s{2,}", line)[0])
    return out


def test_every_command_has_examples_and_a_real_guide():
    assert set(manual.EXAMPLES) == set(COMMANDS)
    assert set(manual.SEE_ALSO.values()) <= set(manual.GUIDES)
    for name in manual.GUIDES:
        assert f"pmx help {name}" in manual.OVERVIEW, name


def test_every_command_and_guide_the_manual_names_exists():
    commands = {shlex.split(line)[1] for line in _example_lines()}
    assert commands <= set(COMMANDS), commands - set(COMMANDS)
    guides = {m for text in _texts() for m in re.findall(r"\bpmx help ([a-z]+)", text)}
    assert guides <= set(manual.GUIDES) | set(COMMANDS), guides - set(manual.GUIDES) - set(COMMANDS)


@pytest.mark.parametrize("line", _example_lines())
def test_every_example_parses(line):
    if any(x in line for x in ("…", "...", " / ", ";", "|")):
        pytest.skip("prose, not a command")
    args = build_parser().parse_args(shlex.split(line)[1:])
    assert args.cmd in COMMANDS


def test_pmx_alone_and_help_show_the_overview_and_guides(capsys):
    main([])
    assert "WHAT DO YOU WANT TO DO?" in capsys.readouterr().out
    main(["help", "live"])
    assert "GOING LIVE" in capsys.readouterr().out
    main(["help", "order"])
    out = capsys.readouterr().out
    assert "examples:" in out and "guide: pmx help trade" in out and "place an order by hand" in out
    with pytest.raises(SystemExit) as e:
        main(["help", "lve"])
    assert e.value.code == 2 and "did you mean live" in capsys.readouterr().err
