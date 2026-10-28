"""Plugin registries. Sources, linkers, models, strategies, and advisors register a class
(a function, for linkers) under a `kind`; config creates named instances of them, with `params`
decoded into the plugin's nested `Params` struct by `build_params`.

    @model("market_mid")
    class MarketMid:
        class Params(msgspec.Struct, frozen=True):
            min_change: float = 0.001
"""

from __future__ import annotations

import importlib
import pkgutil
from collections.abc import Callable, Iterator
from typing import Any

import msgspec
import msgspec.inspect as mi

from pmx.core.types import Contract

type Linker = Callable[[Contract], str | None]
# A domain's mutually exclusive outcomes for one entity: {outcome: {ticker: leg}}, a leg being "yes" (settles at
# $1), "no" ($0), or the YES settlement price when it's neither (an NFL tie pays $0.50 on both teams).
type Outcomes = Callable[[str, list[Contract]], dict[str, dict[str, "str | int"]]]

SOURCES: dict[str, type] = {}
LINKERS: dict[str, Linker] = {}
MODELS: dict[str, type] = {}
STRATEGIES: dict[str, type] = {}
ADVISORS: dict[str, type] = {}
OUTCOMES: dict[str, Outcomes] = {}  # by the linker kind whose entities they describe

_REGISTRIES: dict[str, dict[str, Any]] = {
    "source": SOURCES, "linker": LINKERS, "model": MODELS, "strategy": STRATEGIES, "advisor": ADVISORS,
    "outcomes": OUTCOMES,
}

# `load_builtin_plugins` imports every module under these packages, and only `linker` and `outcomes` modules
# under the domain packages (domains keep research code that's slow to import next to them).
PLUGIN_PACKAGES = ("pmx.sources", "pmx.models", "pmx.strategies", "pmx.advisors")
DOMAIN_PACKAGES = ("pmx.sports",)


class PluginError(ValueError):
    """A plugin can't be registered or configured. The message names the plugin."""


def _qualname(obj: Any) -> str:
    return f"{obj.__module__}.{obj.__qualname__}"


def _register(what: str, kind: str) -> Callable[[Any], Any]:
    registry = _REGISTRIES[what]

    def deco(obj: Any) -> Any:
        prev = registry.get(kind)
        # The same definition again (a module reload) replaces; a different one is a clash.
        if prev is not None and _qualname(prev) != _qualname(obj):
            raise PluginError(f"duplicate {what} kind {kind!r}: {_qualname(prev)} and {_qualname(obj)}")
        registry[kind] = obj
        return obj

    return deco


def source(kind: str) -> Callable[[type], type]:
    """Class decorator; the class is built as `cls(name, params, env)`."""
    return _register("source", kind)


def linker(kind: str) -> Callable[[Linker], Linker]:
    """Function decorator; `fn(contract) -> entity key | None`, pure."""
    return _register("linker", kind)


def model(kind: str) -> Callable[[type], type]:
    """Class decorator; the class is built as `cls(name, params, env)`."""
    return _register("model", kind)


def strategy(kind: str) -> Callable[[type], type]:
    """Class decorator; the class is built as `cls(params)`, once per slot."""
    return _register("strategy", kind)


def advisor(kind: str) -> Callable[[type], type]:
    """Class decorator; the class is built as `cls(name, params, env)`."""
    return _register("advisor", kind)


def outcomes(linker_kind: str) -> Callable[[Outcomes], Outcomes]:
    """Function decorator; `fn(entity, its contracts) -> {outcome: {ticker: leg}}` for the entities `linker_kind`
    makes (portfolio scenario risk; group slots' `outcomes`). Pure."""
    return _register("outcomes", linker_kind)


def describe(obj: Any) -> str:
    """`model 'market_mid'`, or the qualified name of something unregistered."""
    for what, registry in _REGISTRIES.items():
        for kind, registered in registry.items():
            if registered is obj:
                return f"{what} {kind!r}"
    return _qualname(obj)


def _modules(package: str) -> Iterator[str]:
    """Every module under `package`; nothing if the package doesn't exist yet."""
    try:
        root = importlib.import_module(package)
    except ModuleNotFoundError as e:
        if e.name is not None and (package == e.name or package.startswith(e.name + ".")):
            return
        raise
    if not hasattr(root, "__path__"):
        return

    def reraise(name: str) -> None:  # pkgutil would otherwise skip a subpackage that fails to import
        raise

    for info in pkgutil.walk_packages(root.__path__, package + ".", onerror=reraise):
        yield info.name


def load_builtin_plugins(packages: tuple[str, ...] | None = None, *, domains: bool = True) -> None:
    """Import every plugin module (of `packages`, default `PLUGIN_PACKAGES`) so its decorators run. A package
    that doesn't exist yet is skipped; a module that fails to import raises (a broken plugin must stop
    `pmx run`, not vanish)."""
    for package in PLUGIN_PACKAGES if packages is None else packages:
        for name in _modules(package):
            importlib.import_module(name)
    for package in DOMAIN_PACKAGES if domains else ():
        for name in _modules(package):
            if name.rsplit(".", 1)[1] in ("linker", "linkers", "outcomes"):
                importlib.import_module(name)


def build_params(cls: type, table: dict[str, Any] | None) -> Any:
    """A config `params` table -> `cls.Params`. Unknown keys (at any depth) and wrong types raise
    `PluginError` naming the plugin; a plugin without `Params` accepts only an empty table."""
    table = table or {}
    params = getattr(cls, "Params", None)
    if params is None:
        if table:
            raise PluginError(f"{describe(cls)} takes no params, got {', '.join(sorted(table))}")
        return None
    _check_keys(mi.type_info(params), table, "params", cls)
    try:
        return msgspec.convert(table, params)
    except msgspec.ValidationError as e:
        raise PluginError(f"{describe(cls)}: bad params: {e}") from None


def _check_keys(t: mi.Type, value: Any, path: str, cls: type) -> None:
    """msgspec ignores unknown fields unless a struct forbids them; config typos must not pass silently."""
    if isinstance(t, mi.Metadata):
        _check_keys(t.type, value, path, cls)
    elif isinstance(t, mi.StructType) and isinstance(value, dict):
        fields = {f.encode_name: f for f in t.fields}
        unknown = sorted(k for k in value if k not in fields)
        if unknown:
            raise PluginError(f"{describe(cls)}: unknown {path} key {', '.join(repr(k) for k in unknown)}"
                              f" (known: {', '.join(fields) or 'none'})")
        for k, v in value.items():
            _check_keys(fields[k].type, v, f"{path}.{k}", cls)
    elif isinstance(t, mi.UnionType):
        structs = [u for u in t.types if isinstance(u, mi.StructType)]
        if len(structs) == 1:  # Optional[Struct]; tagged unions are msgspec's to resolve
            _check_keys(structs[0], value, path, cls)
        for u in t.types:
            if not isinstance(u, mi.StructType):
                _check_keys(u, value, path, cls)
    elif isinstance(t, mi.CollectionType) and isinstance(value, list):
        for i, v in enumerate(value):
            _check_keys(t.item_type, v, f"{path}[{i}]", cls)
    elif isinstance(t, mi.TupleType) and isinstance(value, list):
        for i, (u, v) in enumerate(zip(t.item_types, value, strict=False)):
            _check_keys(u, v, f"{path}[{i}]", cls)
    elif isinstance(t, mi.DictType) and isinstance(value, dict):
        for k, v in value.items():
            _check_keys(t.value_type, v, f"{path}.{k}", cls)
