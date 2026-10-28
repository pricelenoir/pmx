"""Account and keys: account, keygen, setkey, check."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from pathlib import Path

from pmx.cli.common import die, venue
from pmx.connectors.kalshi.config import KalshiConfig
from pmx.core.units import fmt_micros, fmt_qty


async def cmd_account(args: argparse.Namespace) -> None:
    v = venue(args)
    try:
        print(f"[{v.cfg.env}] balance ${fmt_micros(await v.balance())}")
        for p in await v.positions():
            print(f"  {p.ticker:<36} {fmt_qty(p.qty):>10}  realized ${fmt_micros(p.realized_pnl)}"
                  f"  fees ${fmt_micros(p.fees)}")
    finally:
        await v.aclose()


def cmd_keygen(args: argparse.Namespace) -> None:
    from pmx.setup import keygen

    env = args.env or "demo"
    path, public = keygen(env)
    host = "demo.kalshi.co" if env == "demo" else "kalshi.com"
    print(f"private key: {path} (mode 0600; its path is saved in .env)\n")
    print(public)
    print(f"Paste the public key above at https://{host}/account/profile → API Keys → Create New API Key,")
    print(f"then: pmx setkey --env {env} <KEY_ID>")


def _ask(prompt: str) -> str:
    try:
        return input(prompt).strip()
    except EOFError:
        return ""


async def cmd_setkey(args: argparse.Namespace) -> None:
    """Everything after making the key on Kalshi's site, in one go: the key id, the private key file (moved under
    ~/.kalshi/, 0600), .env, then `pmx check`. Asks for whatever isn't on the command line."""
    import shlex

    from pmx import setup
    from pmx.setup import check, install_key, set_key_id

    interactive = sys.stdin.isatty()
    env = args.env or (_ask("Kalshi environment — prod (kalshi.com) or demo (demo.kalshi.co)? [prod] ") or "prod"
                       if interactive else "demo")
    if env not in ("prod", "demo"):
        die(f"unknown environment {env!r}; use demo or prod")
    key_id = args.key_id or (_ask("API key ID (the short id Kalshi shows, not the long private key): ")
                             if interactive else "")
    if not key_id:
        die("missing the API key ID; usage: pmx setkey --env prod KEY_ID [--key FILE]")
    if "PRIVATE KEY" in key_id or len(key_id) > 100:
        die("that looks like a private key, not a key ID; pass the private key as a file with --key FILE")
    key = args.key
    current = KalshiConfig.from_env(env).private_key_path
    if key is None and interactive:
        keep = f"; Enter keeps {current}" if current and Path(current).expanduser().exists() else ""
        answer = _ask(f"Private key file Kalshi gave you (drag it into this window{keep}): ")
        key = shlex.split(answer)[0] if answer else None  # a dragged path arrives shell-escaped
    if key:
        try:
            path, kind = install_key(env, Path(key), setup.KEY_DIR, keep=args.keep)
        except (OSError, ValueError) as e:
            die(f"private key: {e}")
        print(f"private key ({kind}): {path} (mode 0600)" + ("" if args.keep else f"; removed {key}"))
    elif not (current and Path(current).expanduser().exists()):
        die(f"no private key for {env}; run pmx setkey --env {env} KEY_ID --key FILE (the file Kalshi gave you)")
    set_key_id(env, key_id)
    print(f"key ID saved (KALSHI_{env.upper()}_API_KEY_ID in .env)\n")
    if args.no_check:
        return
    os.environ[f"KALSHI_{env.upper()}_API_KEY_ID"] = key_id.strip()
    if key:
        os.environ[f"KALSHI_{env.upper()}_PRIVATE_KEY_PATH"] = str(path)
    if not await check(env):
        sys.exit(1)


async def cmd_check(args: argparse.Namespace) -> None:
    from pmx.setup import check

    if not await check(args.env or KalshiConfig.from_env().env, order=args.order):
        sys.exit(1)


def register(sub: argparse._SubParsersAction, common: argparse.ArgumentParser) -> dict[str, Callable]:
    sub.add_parser("account", parents=[common], help="Kalshi balance and venue positions")
    sub.add_parser("keygen", parents=[common], help="make an Ed25519 key pair to register on Kalshi")
    k = sub.add_parser("setkey", parents=[common], help="install a Kalshi API key (asks for what's missing)")
    k.add_argument("key_id", nargs="?", help="the API key ID from Kalshi")
    k.add_argument("--key", metavar="FILE", help="the private key file Kalshi gave you (moved to ~/.kalshi/)")
    k.add_argument("--keep", action="store_true", help="copy the key file instead of moving it")
    k.add_argument("--no-check", action="store_true", help="don't run pmx check afterwards")
    ck = sub.add_parser("check", parents=[common], help="verify auth, REST, websocket (--order: a 1¢ demo order)")
    ck.add_argument("--order", action="store_true", help="also place + cancel a 1¢ post-only order (demo only)")
    return {"account": cmd_account, "keygen": cmd_keygen, "setkey": cmd_setkey, "check": cmd_check}
