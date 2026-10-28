from __future__ import annotations

import os
import tomllib
from pathlib import Path

import msgspec
from dotenv import load_dotenv

from pmx.recorder.daemon import RecorderConfig

PROJECT = Path(__file__).resolve().parents[2]


def load_env() -> None:
    load_dotenv(PROJECT / ".env")


def data_root() -> Path:
    return Path(os.environ.get("PMX_DATA", PROJECT / "data"))


def socket_path() -> Path:
    """Where `pmx serve` listens for viewers."""
    return Path(os.environ.get("PMX_SOCK", data_root() / "pmx.sock"))


class RecorderSettings(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    keep_days: int = 30  # raw logs older than this go once compacted


def _recorder_toml(path: Path | None) -> dict:
    return tomllib.loads((path or PROJECT / "config" / "recorder.toml").read_text())


def recorder_config(path: Path | None = None) -> dict[str, RecorderConfig]:
    """Per-venue recorder universes from config/recorder.toml."""
    raw = _recorder_toml(path)
    return {venue: msgspec.convert(section, RecorderConfig) for venue, section in raw.items()
            if venue not in ("recorder", "source")}


def recorder_sources(path: Path | None = None) -> list:
    """The `[[source]]` plugins `pmx record` captures besides the venue (`SourceSpec`s)."""
    from pmx.sources.base import SourceSpec

    return [msgspec.convert(s, SourceSpec) for s in _recorder_toml(path).get("source", ())]


def recorder_settings(path: Path | None = None) -> RecorderSettings:
    """The `[recorder]` table of config/recorder.toml."""
    return msgspec.convert(_recorder_toml(path).get("recorder", {}), RecorderSettings)
