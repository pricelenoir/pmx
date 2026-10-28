"""The control channel's wire format (ARCHITECTURE *Control channel*): NDJSON over `data/pmx-ctl.sock`, one
request per line, one `Response` per request. Integer units and YES terms, as everywhere: `price` in 1e-4 $,
`qty` in 1e-2 contracts; `bid` buys YES, `ask` sells YES."""

from __future__ import annotations

from typing import Any, Literal

import msgspec

from pmx.engine.events import Mode


class Order(msgspec.Struct, frozen=True, tag="order"):
    """A manual order; it runs as slot `manual/<ticker>` under `[manual]` risk limits."""

    ticker: str
    side: Literal["bid", "ask"]
    price: int
    qty: int
    mode: Mode = "paper"
    tif: Literal["gtc", "ioc"] = "gtc"
    post_only: bool = False
    confirmed: bool = False  # live: the CLI showed the order and the user confirmed it
    venue: str = "kalshi"


class Cancel(msgspec.Struct, frozen=True, tag="cancel"):
    """One order by id (any slot), or the manual orders on a market, or all manual orders."""

    client_order_id: str = ""
    ticker: str = ""
    all: bool = False
    mode: Mode | None = None


class Halt(msgspec.Struct, frozen=True, tag="halt"):
    scope: str = ""  # a slot id, a strategy id, or "" = everything
    reason: str = "pmx halt"


class Resume(msgspec.Struct, frozen=True, tag="resume"):
    scope: str = ""


class Status(msgspec.Struct, frozen=True, tag="status"):
    pass


class Positions(msgspec.Struct, frozen=True, tag="positions"):
    mode: Mode | None = None


class Advise(msgspec.Struct, frozen=True, tag="advise"):
    """Manual advice: `Advice(advisor="manual")` for strategies that list `manual`."""

    target: str
    p: float | None = None
    direction: Literal["buy", "sell", "flat"] | None = None
    confidence: float = 1.0
    expires_s: float = 1_800.0
    note: str = ""


type Request = Order | Cancel | Halt | Resume | Status | Positions | Advise


class Response(msgspec.Struct, frozen=True):
    ok: bool
    error: str = ""
    orders: list[dict[str, Any]] = []  # order updates for this request's orders, in order
    fills: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    data: dict[str, Any] = {}


requests = msgspec.json.Decoder(Request)
responses = msgspec.json.Decoder(Response)
encode = msgspec.json.Encoder().encode
