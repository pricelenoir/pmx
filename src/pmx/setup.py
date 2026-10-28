"""`pmx keygen`, `pmx setkey`, `pmx check`: a Kalshi key — made here (`keygen`, then register the public
half) or made by Kalshi (`setkey --key <downloaded file>`) — installed under ~/.kalshi/, then the whole
path verified (auth, REST, websocket, and on demo an order round-trip)."""

from __future__ import annotations

import asyncio
import os
import re
import time
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from dotenv import set_key

from pmx import config
from pmx.connectors.kalshi.config import KalshiConfig, KalshiEnv
from pmx.connectors.kalshi.rest import KalshiError
from pmx.connectors.kalshi.venue import KalshiVenue
from pmx.core.book import BookTracker
from pmx.core.types import BookSnapshot, OrderRequest
from pmx.core.units import cents, fmt_micros, parse_price

KEY_DIR = Path.home() / ".kalshi"


def keygen(env: KalshiEnv, key_dir: Path = KEY_DIR, env_file: Path | None = None) -> tuple[Path, str]:
    """Ed25519 key pair; private key written 0600 and never printed. Returns (path, public PEM)."""
    path = key_dir / f"pmx-{env}.key"
    if path.exists():
        raise FileExistsError(f"{path} already exists — delete it first to rotate")
    key = Ed25519PrivateKey.generate()
    key_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(pem)
    env_file = env_file or config.PROJECT / ".env"
    env_file.touch(mode=0o600, exist_ok=True)
    set_key(str(env_file), f"KALSHI_{env.upper()}_PRIVATE_KEY_PATH", str(path), quote_mode="never")
    public = key.public_key().public_bytes(serialization.Encoding.PEM,
                                           serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    return path, public


_PEM = re.compile(rb"-----BEGIN ([A-Z ]*)PRIVATE KEY-----.+?-----END \1PRIVATE KEY-----", re.DOTALL)


def install_key(env: KalshiEnv, source: Path, key_dir: Path = KEY_DIR, env_file: Path | None = None, *,
                keep: bool = False) -> tuple[Path, str]:
    """A private key file Kalshi gave you → `~/.kalshi/pmx-<env>.key` (0600), its path into .env, and the
    original deleted unless `keep`. Text around the key (a note, the key id) is dropped; a key already there is
    kept as `pmx-<env>.key.old`. Returns (path, key type). The key is never printed."""
    source = source.expanduser()
    m = _PEM.search(source.read_bytes())
    if m is None:
        raise ValueError(f"{source} has no private key in it (looking for -----BEGIN … PRIVATE KEY-----)")
    pem = b"\n".join(line.strip() for line in m.group(0).splitlines() if line.strip()) + b"\n"
    try:
        key = serialization.load_pem_private_key(pem, password=None)
    except (ValueError, TypeError) as e:
        raise ValueError(f"{source}: the private key doesn't load ({type(e).__name__}) — copied in full?") from None
    if not isinstance(key, RSAPrivateKey | Ed25519PrivateKey):
        raise ValueError(f"{source}: a {type(key).__name__}, but Kalshi keys are RSA or Ed25519")
    dest = key_dir / f"pmx-{env}.key"
    key_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    same = dest.exists() and dest.resolve() == source.resolve()
    if dest.exists() and not same and dest.read_bytes().strip() != pem.strip():
        os.replace(dest, dest.with_name(dest.name + ".old"))
    tmp = dest.with_name(dest.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(pem)
    os.replace(tmp, dest)
    env_file = env_file or config.PROJECT / ".env"
    env_file.touch(mode=0o600, exist_ok=True)
    set_key(str(env_file), f"KALSHI_{env.upper()}_PRIVATE_KEY_PATH", str(dest), quote_mode="never")
    if not keep and not same:
        source.unlink()
    return dest, "rsa" if isinstance(key, RSAPrivateKey) else "ed25519"


def set_key_id(env: KalshiEnv, key_id: str, env_file: Path | None = None) -> None:
    env_file = env_file or config.PROJECT / ".env"
    env_file.touch(mode=0o600, exist_ok=True)
    set_key(str(env_file), f"KALSHI_{env.upper()}_API_KEY_ID", key_id.strip(), quote_mode="never")


class Check:
    def __init__(self) -> None:
        self.failed = False

    def ok(self, msg: str) -> None:
        print(f"  ✓ {msg}")

    def fail(self, msg: str) -> None:
        print(f"  ✗ {msg}")
        self.failed = True


async def check(env: KalshiEnv, *, order: bool = False, timeout: float = 15.0) -> bool:
    c = Check()
    cfg = KalshiConfig.from_env(env)
    print(f"Kalshi [{env}]  {cfg.rest_base}")

    if not cfg.private_key_path or not Path(cfg.private_key_path).expanduser().exists():
        c.fail(f"private key: set KALSHI_{env.upper()}_PRIVATE_KEY_PATH (run `pmx keygen --env {env}`)")
        return False
    if not cfg.key_id:
        c.fail(f"key id: set KALSHI_{env.upper()}_API_KEY_ID (`pmx setkey --env {env} <KEY_ID>`)")
        return False
    try:
        signer = cfg.signer()
        assert signer is not None
        c.ok(f"key loads ({signer.key_type})")
    except Exception as e:
        c.fail(f"key does not load: {e}")
        return False

    v = KalshiVenue(cfg)
    try:
        markets = await v.rest.markets(status="open", mve_filter="exclude", max_pages=1)
        liquid = [m for m in markets if m.yes_bid_dollars and m.yes_ask_dollars
                  and 0 < parse_price(m.yes_bid_dollars) < parse_price(m.yes_ask_dollars)]
        if not liquid:
            c.fail(f"public REST: {len(markets)} open markets, none with a two-sided book")
            return False
        target = liquid[0]
        c.ok(f"public REST: {len(markets)} open markets; test market {target.ticker}")

        try:
            c.ok(f"authenticated REST: balance ${fmt_micros(await v.balance())}")
        except KalshiError as e:
            c.fail(f"authenticated REST: {e} — key id wrong, key not registered, or wrong environment?")
            return False

        stream = v.stream([target.ticker], trades=False)
        decode, books = v.decoder(), BookTracker()

        async def first_snapshot() -> BookSnapshot | None:
            async for item in stream:
                for ev in decode(item):
                    books.on_event(ev)
                    if isinstance(ev, BookSnapshot):
                        return ev
            return None

        try:
            t0 = time.monotonic()
            snap = await asyncio.wait_for(first_snapshot(), timeout)
            book = books.get("kalshi", target.ticker)
            top = f"{cents(book.best_bid()[0]) if book and book.best_bid() else '-'} / " \
                  f"{cents(book.best_ask()[0]) if book and book.best_ask() else '-'}"
            c.ok(f"websocket: snapshot in {time.monotonic() - t0:.2f}s, {len(snap.bids)}+{len(snap.asks)} "
                 f"levels, top {top}")
        except TimeoutError:
            c.fail(f"websocket: no snapshot within {timeout:.0f}s")
        finally:
            await stream.close()

        if order:
            if env != "demo":
                c.fail("order round-trip runs on demo only")
            else:
                await _order_roundtrip(c, v, target.ticker)
    finally:
        await v.aclose()
    print("all checks passed" if not c.failed else "some checks failed")
    return not c.failed


async def _order_roundtrip(c: Check, v: KalshiVenue, ticker: str) -> None:
    """Post-only bid for 1 contract at 1¢ — rests far from the market — then cancel it."""
    req = OrderRequest("kalshi", ticker, "bid", 100, 100, post_only=True, strategy="pmx-check")
    try:
        ack = await v.place(req)
    except KalshiError as e:
        c.fail(f"place order: {e}")
        return
    if ack.filled:
        c.fail(f"order unexpectedly filled {ack.filled} — check demo positions")
    else:
        c.ok(f"placed post-only bid 1¢ x1 ({ack.order_id[:8]}…, resting {ack.remaining})")
    try:
        reduced = await v.cancel(ack.order_id, ticker)
        c.ok(f"canceled (reduced_by {reduced})")
    except KalshiError as e:
        c.fail(f"cancel: {e} — cancel {ack.order_id} manually on demo.kalshi.co")
