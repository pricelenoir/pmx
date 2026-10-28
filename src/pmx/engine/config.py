"""`config/strategies.toml` → `RunConfig` (ARCHITECTURE *Config*). `load` validates everything `pmx run`
will build without touching the network: plugin kinds and their params (`plugins.build_params`), risk limits
as integer units, names that refer to each other (a strategy's model, inputs, advisors), the live switches,
and no secrets. `plan` is what `pmx config check` prints."""

from __future__ import annotations

import hashlib
import re
import tomllib
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, Literal

import msgspec

from pmx import config, plugins
from pmx.advisors.base import AdvisorSpec
from pmx.core.units import Micros, parse_micros
from pmx.engine.slots import ByLinker, Router, StrategySpec
from pmx.execution.risk import RiskLimits
from pmx.models.runtime import ModelSpec
from pmx.plugins import PluginError, build_params
from pmx.recorder.sink import looks_secret
from pmx.sources.base import SourceSpec

DEFAULT = config.PROJECT / "config" / "strategies.toml"

# Sources `pmx run` always runs, under these names (what strategies list in `inputs`).
BUILTIN_SOURCES = {"kalshi": "Kalshi market data", "timer": "the 1 s heartbeat",
                   "account": "Kalshi fills and balance (needs an API key)", "advisors": "the advisor worker"}
MANUAL = "manual"  # manual advice from `pmx advise`


class ConfigError(ValueError):
    pass


class QueueSpec(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    max: int = 10_000
    policy: Literal["conflate", "drop_oldest", "reset"] = "conflate"


class SourceConfig(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One `[[source]]`."""

    name: str
    kind: str
    params: dict[str, Any] = {}
    queue: QueueSpec = QueueSpec()

    @property
    def spec(self) -> SourceSpec:
        return SourceSpec(self.name, self.kind, self.params)


class EngineSection(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    venue: Literal["kalshi"] = "kalshi"
    env: Literal["demo", "prod"] | None = None  # market data; `--env` wins, then this, then $KALSHI_ENV
    record: bool = True  # raw-record every source while running
    paper_cash: str = "1000.00"  # the paper account's starting balance
    checkpoint_s: float = 60.0  # slot checkpoints (on_save) this often and at shutdown
    warm_from_recording: bool = False


class WatchSection(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    series: list[str] = []
    tickers: list[str] = []


class ManualSection(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    risk: dict[str, Any] = {}


class OpsSection(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Unattended running: alerts, health, the live watchdog, staying awake, backups."""

    alerts: bool = True  # macOS notifications; every alert is logged either way
    alert_every_s: float = 300.0  # the same alert at most this often
    source_down_s: float = 60.0  # a source down this long alerts
    throttled_s: float = 30.0  # the order scheduler holding writes this long alerts
    disk_min_gb: float = 5.0
    health_s: float = 10.0  # data/health.json this often
    guard_s: float = 10.0  # `pmx guard` cancels live orders after this long without a heartbeat
    stay_awake: bool = True  # hold a no-sleep assertion (caffeinate) while running
    backup_dir: str = ""  # `pmx ops backup` copies raw logs and the ledger here
    backup_keep_days: int = 14


class _File(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    engine: EngineSection = EngineSection()
    watch: WatchSection = WatchSection()
    risk: dict[str, Any] = {}
    manual: ManualSection = ManualSection()
    ops: OpsSection = OpsSection()
    source: list[SourceConfig] = []
    model: list[ModelSpec] = []
    advisor: list[AdvisorSpec] = []
    strategy: list[dict[str, Any]] = []


class RunConfig(msgspec.Struct, frozen=True, kw_only=True):
    path: Path | None
    text: str  # as written (the `runs` row keeps it)
    hash: str  # of the parsed config, so formatting doesn't change it
    engine: EngineSection
    watch: WatchSection
    risk: RiskLimits  # global
    manual: RiskLimits
    ops: OpsSection
    paper_cash: Micros
    sources: list[SourceConfig]
    models: list[ModelSpec]
    advisors: list[AdvisorSpec]
    strategies: list[StrategySpec]  # `risk` decoded to RiskLimits
    params_hash: dict[str, str]  # strategy id -> hash of its decoded params

    @property
    def live(self) -> list[str]:
        """Ids of strategies with `mode = "live"`."""
        return [s.id for s in self.strategies if s.mode == "live"]

    def universe(self) -> tuple[list[str], list[str]]:
        """(series, tickers) the Kalshi market source streams: every strategy selector plus `[watch]`. A
        selector's events are discovered through their series."""
        series = dict.fromkeys(self.watch.series)
        tickers = dict.fromkeys(self.watch.tickers)
        for s in self.strategies:
            series.update(dict.fromkeys(s.markets.series))
            series.update(dict.fromkeys(e.split("-", 1)[0] for e in s.markets.events))
            tickers.update(dict.fromkeys(s.markets.tickers))
        return list(series), list(tickers)

    def without_strategies(self) -> RunConfig:
        """`pmx serve`: the same engine, sources, and models, trading nothing."""
        return msgspec.structs.replace(self, strategies=[], params_hash={})

    def watching(self, series: Sequence[str], tickers: Sequence[str]) -> RunConfig:
        """`[watch]` replaced (`--series` / `--tickers`)."""
        return msgspec.structs.replace(self, watch=WatchSection(list(series), list(tickers)))

    def with_models(self, kinds: Sequence[str]) -> RunConfig:
        """Plus a model per kind not already configured (`--models`), named after its kind."""
        have = {m.name for m in self.models} | {m.kind for m in self.models}
        extra = []
        for k in kinds:
            if k not in have:
                _plugin(plugins.MODELS, "model", k, k, {})
                extra.append(ModelSpec(name=k, kind=k))
        return msgspec.structs.replace(self, models=[*self.models, *extra])


def load(path: Path | str | None = None, *, text: str | None = None) -> RunConfig:
    """Parse and validate; raises `ConfigError` with a message naming the file and the problem."""
    p = Path(path) if path is not None else (DEFAULT if text is None else None)
    where = str(p) if p is not None else "config"
    if text is None:
        assert p is not None
        try:
            text = p.read_text()
        except FileNotFoundError:
            raise ConfigError(f"{where}: no such file") from None
    try:
        raw = tomllib.loads(text)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"{where}: {e}") from None
    for key, value in _strings(raw, "$"):
        if _secret(key, value):
            raise ConfigError(f"{where}: {key} looks like a secret — keys live in .env and ~/.kalshi/, never in config")
    try:
        f = msgspec.convert(raw, _File)
        return _resolve(f, raw, text, p)
    except msgspec.ValidationError as e:
        raise ConfigError(f"{where}: {e}") from None
    except (PluginError, ValueError) as e:
        raise ConfigError(f"{where}: {e}") from None


def _resolve(f: _File, raw: dict, text: str, path: Path | None) -> RunConfig:
    plugins.load_builtin_plugins()
    sources: dict[str, str] = dict(BUILTIN_SOURCES)
    for s in f.source:
        if s.name in sources:
            raise ValueError(f"source {s.name!r}: name already used ({sources[s.name]})")
        if s.queue.max <= 0:
            raise ValueError(f"source {s.name!r}: queue max must be positive")
        _plugin(plugins.SOURCES, "source", s.name, s.kind, s.params)
        sources[s.name] = f"source {s.kind}"
    models: set[str] = set()
    for m in f.model:
        if m.name in models:
            raise ValueError(f"model {m.name!r} is configured twice")
        _plugin(plugins.MODELS, "model", m.name, m.kind, m.params)
        models.add(m.name)
    advisors: set[str] = set()
    for a in f.advisor:
        if a.name in advisors or a.name == MANUAL:
            raise ValueError(f"advisor {a.name!r}: name already used" + (" (reserved for `pmx advise`)"
                                                                       if a.name == MANUAL else ""))
        _plugin(plugins.ADVISORS, "advisor", a.name, a.kind, a.params)
        advisors.add(a.name)
    specs = [_strategy(i, t) for i, t in enumerate(f.strategy)]
    for spec in specs:  # kinds, params, linkers — named by id, since kinds repeat
        try:
            Router([spec], linkers=plugins.LINKERS)
        except PluginError as e:
            raise ValueError(str(e) if repr(spec.id) in str(e) else f"strategy {spec.id!r}: {e}") from None
    router = Router(specs, linkers=plugins.LINKERS)  # duplicate ids
    params_hash = {}
    for s in router.strategies.values():
        spec = s.spec
        if spec.model is not None and spec.model not in models:
            raise ValueError(f"strategy {spec.id!r}: model {spec.model!r} isn't a configured [[model]] "
                             f"(configured: {', '.join(sorted(models)) or 'none'})")
        for name in s.inputs:
            if name not in sources:
                raise ValueError(f"strategy {spec.id!r}: input {name!r} isn't a source "
                                 f"(sources: {', '.join(sources)})")
        for name in spec.advisors:
            if name not in advisors and name != MANUAL:
                raise ValueError(f"strategy {spec.id!r}: advisor {name!r} isn't a configured [[advisor]] "
                                 f"(configured: {', '.join(sorted(advisors | {MANUAL}))})")
        sel = spec.markets
        if not (sel.series or sel.tickers or sel.events):
            raise ValueError(f"strategy {spec.id!r}: `markets` selects nothing (give series, events, or tickers)")
        params_hash[spec.id] = _hash([spec.kind, msgspec.to_builtins(s.params)])[:12]
    if f.engine.checkpoint_s <= 0:
        raise ValueError("[engine] checkpoint_s must be positive")
    risk = _limits("[risk]", f.risk)
    live = [s.id for s in specs if s.mode == "live"]
    if live and risk.max_loss is None:
        raise ValueError(f"live strategies ({', '.join(live)}) need a global max_loss in [risk]")
    return RunConfig(path=path, text=text, hash=_hash(raw)[:16], engine=f.engine, watch=f.watch, risk=risk,
                     manual=_limits("[manual] risk", f.manual.risk), ops=f.ops,
                     paper_cash=parse_micros(f.engine.paper_cash), sources=f.source, models=f.model,
                     advisors=f.advisor, strategies=specs, params_hash=params_hash)


def _plugin(registry: dict[str, type], what: str, name: str, kind: str, params: dict) -> None:
    cls = registry.get(kind)
    if cls is None:
        raise ValueError(f"{what} {name!r}: unknown kind {kind!r} (known: {', '.join(sorted(registry)) or 'none'})")
    build_params(cls, params)


def _strategy(i: int, table: dict[str, Any]) -> StrategySpec:
    sid = table.get("id", f"#{i + 1}")
    try:
        limits = RiskLimits.of(table.get("risk"))
        spec = msgspec.convert({k: v for k, v in table.items() if k != "risk"}, StrategySpec)
    except (msgspec.ValidationError, ValueError) as e:
        raise ValueError(f"strategy {sid!r}: {e}") from None
    return msgspec.structs.replace(spec, risk=limits)


def _limits(what: str, table: dict[str, Any]) -> RiskLimits:
    try:
        return RiskLimits.of(table)
    except (msgspec.ValidationError, ValueError) as e:
        raise ValueError(f"{what}: {e}") from None


def _hash(obj: Any) -> str:
    return hashlib.sha256(msgspec.json.encode(obj, order="sorted")).hexdigest()


_SECRET_KEY = re.compile(r"secret|token|password|passwd|api_?key|key_?id|private_?key|credential", re.IGNORECASE)
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)


def _secret(key: str, value: str) -> bool:
    name = key.rsplit(".", 1)[-1]
    return bool(looks_secret(value) or _UUID.search(value) or (_SECRET_KEY.search(name) and value))


def _strings(obj: Any, path: str) -> Iterator[tuple[str, str]]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _strings(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _strings(v, f"{path}[{i}]")
    elif isinstance(obj, str):
        yield path, obj


# ---- the plan `pmx config check` prints ---------------------------------------------------------------------

_MONEY = {"max_loss", "max_capital_at_risk", "max_worst_case", "max_entity_risk", "max_drawdown"}
_QTY = {"max_position", "max_order_qty"}


def _fmt_limits(lim: RiskLimits) -> str:
    out = []
    for name in lim.__struct_fields__:
        v = getattr(lim, name)
        if v is None or v == []:
            continue
        if name in _MONEY:
            v = f"${v / 1e6:,.2f}"
        elif name in _QTY:
            v = f"{v / 100:g}"
        elif name == "price_band":
            v = f"{v / 10_000:g}"
        elif name == "windows":
            v = ", ".join(f"{w.kind} -{w.before_s}s/+{w.after_s}s {w.action}" for w in v)
        out.append(f"{name} {v}")
    return " · ".join(out) or "none"


def _fmt_params(params: dict[str, Any]) -> str:
    return ", ".join(f"{k}={v!r}" if isinstance(v, str) else f"{k}={v}" for k, v in params.items())


def _selector(s: StrategySpec) -> str:
    sel = s.markets
    parts = [f"{k} {', '.join(v)}" for k, v in (("series", sel.series), ("events", sel.events),
                                                 ("tickers", sel.tickers)) if v]
    if sel.linker:
        parts.append(f"linked by {sel.linker}")
    if sel.status:
        parts.append(f"status {sel.status}")
    by = s.slot_by
    slots = (f"one slot per {by.linker} group" if isinstance(by, ByLinker) else "one slot over all of them"
             if by == "all" else f"one slot per {by}")
    return f"{'; '.join(parts)} — {slots}"


def plan(cfg: RunConfig, *, env: str | None = None) -> str:
    e = cfg.engine
    series, tickers = cfg.universe()
    where = cfg.path.relative_to(config.PROJECT) if cfg.path and cfg.path.is_relative_to(config.PROJECT) else cfg.path
    lines = [f"{where or 'config'}  (hash {cfg.hash})", "",
             f"engine    {e.venue} · env {env or e.env or 'from $KALSHI_ENV'} · "
             f"{'records raw' if e.record else 'not recording'} · paper cash ${cfg.paper_cash / 1e6:,.2f} · "
             f"checkpoints every {e.checkpoint_s:g} s" + (" · warms from the recording" if e.warm_from_recording
                                                         else ""),
             "universe  " + " · ".join(p for p in (f"series {', '.join(series)}" if series else "",
                                                     f"tickers {', '.join(tickers)}" if tickers else "") if p),
             f"risk      {_fmt_limits(cfg.risk)}",
             f"manual    {_fmt_limits(cfg.manual)}", "", "sources"]
    for name, what in BUILTIN_SOURCES.items():
        if name != "advisors" or cfg.advisors:
            lines.append(f"  {name:<14} {what}")
    for s in cfg.sources:
        q = s.queue
        lines.append(f"  {s.name:<14} {s.kind}" + (f" ({_fmt_params(s.params)})" if s.params else "")
                     + f" · queue {q.policy} ≤ {q.max:,}")
    lines += ["", "models"]
    lines += [f"  {m.name:<14} {m.kind} · {m.runtime}" + (f" ({_fmt_params(m.params)})" if m.params else "")
              for m in cfg.models] or ["  none"]
    if cfg.advisors:
        lines += ["", "advisors"] + [f"  {a.name:<14} {a.kind}" + (f" ({_fmt_params(a.params)})" if a.params else "")
                                     for a in cfg.advisors]
    lines += ["", "strategies"]
    if not cfg.strategies:
        lines.append("  none")
    for s in cfg.strategies:
        extra = [f"model {s.model}" if s.model else "", f"inputs {', '.join(s.inputs)}" if s.inputs else "",
                 f"advisors {', '.join(s.advisors)}" if s.advisors else "",
                 f"cadence {s.cadence_ms} ms" if s.cadence_ms else "",
                 f"warm-up {s.warmup_s:g} s" if s.warmup_s else ""]
        lines.append(f"  {s.id:<14} " + " · ".join([s.kind, s.mode, *(x for x in extra if x)])
                     + f" (params {cfg.params_hash[s.id]})")
        lines.append(f"  {'':<14} markets: {_selector(s)}")
        if s.params:
            lines.append(f"  {'':<14} params: {_fmt_params(s.params)}")
        lines.append(f"  {'':<14} risk per slot: {_fmt_limits(s.risk)}")
    if cfg.live:
        lines += ["", f"LIVE: {', '.join(cfg.live)} — real money; `pmx run` refuses to start without --live"]
    return "\n".join(lines)
