"""Exact integer units. Venues speak fixed-point strings; floats never touch money.

Price  : 1e-4 dollars  (10_000 == $1.00, a YES contract's payout)
Qty    : 1e-2 contracts (100 == one contract)
Micros : 1e-6 dollars  (Price * Qty lands here exactly)
"""

from __future__ import annotations

type Price = int
type Qty = int
type Micros = int

ONE_DOLLAR: Price = 10_000
ONE_CONTRACT: Qty = 100


def _parse_fixed(s: str, places: int) -> int:
    s = s.strip()
    neg = s.startswith("-")
    if neg or s.startswith("+"):
        s = s[1:]
    whole, _, frac = s.partition(".")
    if len(frac) > places:
        if frac[places:].strip("0"):
            raise ValueError(f"{s!r} has more than {places} decimal places")
        frac = frac[:places]
    if not (whole or frac) or not (whole + frac).isdigit():
        raise ValueError(f"not a fixed-point number: {s!r}")
    v = int(whole or "0") * 10**places + int(frac.ljust(places, "0") or "0")
    return -v if neg else v


def _fmt_fixed(v: int, places: int) -> str:
    sign = "-" if v < 0 else ""
    whole, frac = divmod(abs(v), 10**places)
    return f"{sign}{whole}.{frac:0{places}d}"


def parse_price(s: str) -> Price:
    return _parse_fixed(s, 4)


def parse_qty(s: str) -> Qty:
    return _parse_fixed(s, 2)


def parse_micros(s: str) -> Micros:
    return _parse_fixed(s, 6)


def fmt_price(p: Price) -> str:
    return _fmt_fixed(p, 4)


def fmt_qty(q: Qty) -> str:
    return _fmt_fixed(q, 2)


def fmt_micros(m: Micros) -> str:
    return _fmt_fixed(m, 6)


def notional(price: Price, qty: Qty) -> Micros:
    return price * qty


def complement(p: Price) -> Price:
    """NO price for a YES price (a NO bid at p is a YES ask at 1 - p)."""
    return ONE_DOLLAR - p


def cents(p: Price) -> str:
    """Display form: 56¢, 56.5¢, 0.25¢."""
    whole, frac = divmod(p, 100)
    return f"{whole}¢" if not frac else f"{whole}.{frac:02d}".rstrip("0") + "¢"
