"""The engine: `pmx run`, `pmx config check`, `pmx halt` / `pmx resume`."""

from __future__ import annotations

import argparse
import asyncio
import logging
import logging.handlers
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path

from pmx import config, lifecycle
from pmx.cli.common import die, engine_flags, engine_pidfile, on_signal, socket_for


async def run_engine(args: argparse.Namespace, *, strategies: bool = True) -> None:
    """`pmx run`; `pmx serve` is the same with no strategies and nothing recorded."""
    from pmx.connectors.kalshi.config import KalshiConfig
    from pmx.connectors.kalshi.rest import KalshiRest
    from pmx.connectors.kalshi.venue import KalshiVenue
    from pmx.engine.config import ConfigError, load
    from pmx.engine.run import StartRefused, System
    from pmx.feed.hub import socket_alive

    try:
        cfg = load(args.config)
        if args.series or args.tickers:
            cfg = cfg.watching(args.series or [], args.tickers or [])
        if args.models:
            cfg = cfg.with_models([k for k in args.models.split(",") if k])
    except (ConfigError, ValueError) as e:
        die(str(e))
    if not strategies:
        cfg = cfg.without_strategies()
    sock = socket_for(args)
    pid = lifecycle.read_pid(lifecycle.pidfile_for(sock))
    if pid is not None or await socket_alive(sock):
        die(f"pmx is already running on {sock}" + (f" (pid {pid})" if pid else "") + "; stop it with `pmx down`")
    kcfg = KalshiConfig.from_env(args.env or cfg.engine.env)
    if getattr(args, "live", False) and kcfg.env == "prod" and not confirmed(cfg, args.confirm):
        die("live trading on prod needs confirmation; type `prod` when asked, or pass --confirm prod")
    mode = args.source or ("live" if kcfg.key_id else "poll")
    if mode == "live" and not kcfg.key_id:
        die(f"--source live needs an API key for {kcfg.env}; set one up with `pmx setkey --env {kcfg.env}`")
    # Polling leans on REST alone; the source paces itself under the public limit.
    v = KalshiVenue(kcfg, KalshiRest(kcfg.rest_base, kcfg.signer(), read=(100.0, 100.0)) if mode == "poll" else None)
    log = logging.getLogger("pmx.run")
    listener = _log_off_thread(config.data_root() / "logs" / f"run-{date.today()}.log" if strategies else None)
    stop = asyncio.Event()
    on_signal(stop.set)
    try:
        system = System(cfg, env=kcfg.env, sock=sock, venue=v, live=getattr(args, "live", False),
                        record=None if strategies else False, source_mode=mode, trades_s=args.poll or 2.0)
    except StartRefused as e:
        await v.aclose()
        die(f"cannot start: {e}")
    try:
        with engine_pidfile(sock):
            log.info("%s %s on %s (market data: %s; orders: %s; run %s, config %s)",
                     "running" if strategies else "serving", kcfg.env, sock,
                     "websocket" if mode == "live" else "REST poll",
                     "live allowed" if getattr(args, "live", False) else "paper only", system.run_id, cfg.hash)
            await system.run(stop)
    finally:
        await v.aclose()
        log.info("stopped")
        listener.stop()


def confirmed(cfg: object, confirm: str | None, *, ask: Callable[[str], str] = input) -> bool:
    """Real money: the plan — live strategies and their limits — then `prod` typed once per run; headless
    (`pmx up`), `--confirm prod` instead."""
    from pmx.engine.config import plan

    if confirm is not None:
        return confirm == "prod"
    print(plan(cfg, env="prod"))  # type: ignore[arg-type]
    if not sys.stdin.isatty():
        return False
    try:
        return ask("\nThese strategies trade real money on Kalshi prod. Type prod to go on: ").strip() == "prod"
    except EOFError:
        return False


def _log_off_thread(path: Path | None) -> logging.handlers.QueueListener:
    """The engine never waits on a log write: its handlers (stderr, and `path`) run on a listener thread."""
    import logging.handlers
    import queue

    root = logging.getLogger()
    handlers = list(root.handlers)
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        f = logging.handlers.RotatingFileHandler(path, maxBytes=50 << 20, backupCount=5)
        f.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        handlers.append(f)
    q: queue.SimpleQueue = queue.SimpleQueue()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(logging.handlers.QueueHandler(q))
    listener = logging.handlers.QueueListener(q, *handlers, respect_handler_level=True)
    listener.start()
    return listener


async def cmd_run(args: argparse.Namespace) -> None:
    await run_engine(args, strategies=True)


def cmd_config(args: argparse.Namespace) -> None:
    from pmx.engine.config import ConfigError, load, plan

    try:
        cfg = load(args.config)
    except ConfigError as e:
        die(f"invalid config: {e}")
    print(plan(cfg, env=args.env))
    print("\nconfig is valid")


def _halt_scope(args: argparse.Namespace) -> tuple[str, str]:
    """(scope for the kill-switch file, what it is in words)."""
    what = args.scope or ("all" if not args.id else "slot" if "/" in args.id else "strategy")
    if what == "all":
        if args.id:
            die("--scope all takes no ID")
        return "", "everything"
    if not args.id:
        die(f"--scope {what} needs an ID")
    if (what == "slot") != ("/" in args.id):
        die(f"{args.id!r} is not a {what} ID (slot IDs are <strategy>/<ticker>)")
    return args.id, f"{what} {args.id}"


def _halt_file() -> Path:
    return config.data_root() / "HALT"


def _tell_engine(args: argparse.Namespace, req: object) -> bool:
    """Apply a halt or resume in the running engine at once (its control socket); False if it doesn't answer in 2 s."""
    from pmx.control.client import NotRunning, call
    from pmx.control.server import control_socket

    try:
        return call(control_socket(socket_for(args)), req, timeout=2.0).ok  # type: ignore[arg-type]
    except (NotRunning, TimeoutError):
        return False


def cmd_halt(args: argparse.Namespace) -> None:
    from pmx.control import protocol
    from pmx.execution.risk import write_halt

    scope, what = _halt_scope(args)
    reason = args.reason or "pmx halt"
    write_halt(_halt_file(), scope, reason)  # durable, and the fallback when the engine doesn't answer
    now = _tell_engine(args, protocol.Halt(scope, f"kill switch: {reason}"))  # the reason the file would give
    running = now or lifecycle.read_pid(lifecycle.pidfile_for(socket_for(args))) is not None
    print(f"halted {what}: {reason}")
    if now:
        print(f"  the engine stopped trading and is canceling working orders; undo: pmx resume {scope}".rstrip())
    elif running:
        print(f"  the engine stops trading within 2 s and cancels working orders; undo: pmx resume {scope}".rstrip())
    else:
        print("  pmx isn't running: the halt applies as soon as it starts (pmx resume to clear it)")


def cmd_resume(args: argparse.Namespace) -> None:
    from pmx.control import protocol
    from pmx.execution.risk import clear_halt, read_halts

    if args.scope is None and not args.id:
        halts = read_halts(_halt_file())
        if not clear_halt(_halt_file()):
            print("nothing was halted by pmx halt")
            return
        for scope in halts:
            _tell_engine(args, protocol.Resume(scope))
        print(f"resumed: {', '.join(k or 'everything' for k in halts)}")
        return
    scope, what = _halt_scope(args)
    _tell_engine(args, protocol.Resume(scope))
    if not clear_halt(_halt_file(), scope):
        die(f"{what} was not halted by pmx halt (halted: "
            f"{', '.join(k or 'everything' for k in read_halts(_halt_file())) or 'nothing'})")
    print(f"resumed {what}")


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    r = sub.add_parser("run", parents=[common, engine_flags()],
                       help="the engine with config/strategies.toml, in the foreground (pmx up: in the background)")
    r.add_argument("--socket", help="default $PMX_SOCK or data/pmx.sock")
    r.add_argument("--live", action="store_true", help="allow strategies with mode = \"live\"")
    r.add_argument("--confirm", metavar="ENV", help="live on prod without the typed confirmation (pmx up)")
    c = sub.add_parser("config", parents=[common], help="pmx config check: validate the config, print the plan")
    c.add_argument("action", choices=["check"])
    c.add_argument("--config", help="default config/strategies.toml")
    scope = argparse.ArgumentParser(add_help=False)
    scope.add_argument("id", nargs="?", help="a strategy id or a slot id (<strategy>/<market>); none = everything")
    scope.add_argument("--scope", choices=["slot", "strategy", "all"], help="default: from the id")
    scope.add_argument("--socket", help=argparse.SUPPRESS)
    h = sub.add_parser("halt", parents=[common, scope], help="stop trading (kill-switch file data/HALT)")
    h.add_argument("--reason", help="shown in the log and the TUI")
    sub.add_parser("resume", parents=[common, scope], help="undo pmx halt")
    return {"run": cmd_run, "config": cmd_config, "halt": cmd_halt, "resume": cmd_resume}
