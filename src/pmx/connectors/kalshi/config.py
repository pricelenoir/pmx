from __future__ import annotations

import os
from typing import Literal

import msgspec

from pmx.connectors.kalshi.auth import KalshiSigner

type KalshiEnv = Literal["demo", "prod"]

HOSTS: dict[KalshiEnv, tuple[str, str]] = {
    "demo": ("https://external-api.demo.kalshi.co", "wss://external-api-ws.demo.kalshi.co"),
    "prod": ("https://external-api.kalshi.com", "wss://external-api-ws.kalshi.com"),
}
REST_ROOT = "/trade-api/v2"
WS_PATH = "/trade-api/ws/v2"


class KalshiConfig(msgspec.Struct, frozen=True):
    env: KalshiEnv = "demo"
    key_id: str | None = None
    private_key_path: str | None = None

    @classmethod
    def from_env(cls, env: KalshiEnv | None = None) -> KalshiConfig:
        """KALSHI_ENV picks demo|prod (default demo). Keys are per environment:
        KALSHI_<ENV>_API_KEY_ID / KALSHI_<ENV>_PRIVATE_KEY_PATH."""
        env = env or os.environ.get("KALSHI_ENV", "demo")  # type: ignore[assignment]
        if env not in HOSTS:
            raise ValueError(f"KALSHI_ENV must be demo or prod, got {env!r}")
        p = f"KALSHI_{env.upper()}_"
        return cls(env, os.environ.get(p + "API_KEY_ID"), os.environ.get(p + "PRIVATE_KEY_PATH"))

    @property
    def rest_base(self) -> str:
        return HOSTS[self.env][0] + REST_ROOT

    @property
    def ws_url(self) -> str:
        return HOSTS[self.env][1] + WS_PATH

    def signer(self) -> KalshiSigner | None:
        if not (self.key_id and self.private_key_path):
            return None
        return KalshiSigner.from_file(self.key_id, self.private_key_path)
