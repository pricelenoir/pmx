"""Research: `pmx research init | new | approve | run | advance | amend | close | status` (docs/RESEARCH.md)."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from pmx import config


def _confirm(name: str, what: str) -> None:
    """A person's decisions (approve, promote): typed in a terminal, so an agent's shell can't take them."""
    from pmx.cli.common import die

    if not sys.stdin.isatty():
        die(f"{what} is a decision for a person: run it yourself in a terminal (it asks you to type the experiment "
            "name)")
    try:
        typed = input(f"{what} {name}? type its name to confirm: ").strip()
    except EOFError:
        typed = ""
    if typed != name:
        die("not confirmed")


def cmd_research(args: argparse.Namespace) -> None:
    from pmx.cli.common import die
    from pmx.research import report
    from pmx.research.workspace import ResearchError, Workspace

    ws = Workspace(config.research_root())
    try:
        if args.action == "init":
            for p in ws.init():
                print(f"  {p}")
            print(f"research workspace ready at {ws.root} (git-ignored; `pmx ops backup` copies it)")
            return
        ws.require()
        if args.action == "status":
            print(report.format_status(ws), end="")
            return
        if args.action == "new":
            e = ws.new(args.name, title=args.title or "")
            print(f"{e.path}\n  draft spec.md and run.py, then get it approved: pmx research approve {e.name}")
            return
        e = ws.find(args.name)
        if args.action == "approve":
            _confirm(e.name, "approve")
            ws.approve(e)
            print(f"{e.name} approved; spec.md + run.py locked. Next: pmx research run {e.name}")
        elif args.action == "run":
            from pmx.research.runner import run_stage

            _, text = run_stage(ws, e, config.data_root(), stage=args.stage, force=args.force)
            print(text, end="")
        elif args.action == "advance":
            stage = ws.advance(e)
            print(f"{e.name} → {stage} (locked). Next: pmx research run {e.name}")
        elif args.action == "amend":
            fp = ws.amend(e, args.reason or "")
            print(f"{e.name}: re-locked at {fp[:12]}; amendment recorded")
        elif args.action == "close":
            if args.verdict == "promoted":
                _confirm(e.name, "promote")
            ws.close(e, args.verdict, args.note or "", force=args.force)
            print(f"{e.name} closed as {args.verdict}; FINDINGS.md updated")
    except (ResearchError, ValueError, FileNotFoundError) as err:
        die(str(err))


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    r = sub.add_parser("research", parents=[common],
                       help="pre-registered experiments: init, new, approve, run, advance, amend, close, status")
    r.add_argument("action", choices=["init", "new", "approve", "run", "advance", "amend", "close", "status"])
    r.add_argument("name", nargs="?", help="experiment (new: a slug like weather-longshot; others: name or slug)")
    r.add_argument("--title", help="new: the hypothesis title")
    r.add_argument("--stage", choices=["discovery", "validation", "holdout"], help="run: default the current stage")
    r.add_argument("--reason", help="amend: what changed and why")
    r.add_argument("--as", dest="verdict", choices=["promoted", "dead"], help="close: the verdict")
    r.add_argument("--note", help="close: what it means (goes to FINDINGS.md)")
    r.add_argument("--force", action="store_true", help="run: rerun the holdout · close: promote without one")

    def check(args: argparse.Namespace) -> None:
        if args.action not in ("init", "status") and not args.name:
            r.error(f"{args.action} needs an experiment name")
        if args.action == "close" and not args.verdict:
            r.error("close needs --as promoted|dead")
        cmd_research(args)

    return {"research": check}
