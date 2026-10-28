"""Kalshi request signing: base64(sign(timestamp_ms + METHOD + path)), RSA-PSS/SHA-256
or Ed25519 depending on the key. `path` is from the API root, no query string."""

from __future__ import annotations

import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key


class KalshiSigner:
    def __init__(self, key_id: str, private_key: RSAPrivateKey | Ed25519PrivateKey) -> None:
        self.key_id = key_id
        self._key = private_key

    @classmethod
    def from_file(cls, key_id: str, path: str | Path) -> KalshiSigner:
        key = load_pem_private_key(Path(path).expanduser().read_bytes(), password=None)
        if not isinstance(key, RSAPrivateKey | Ed25519PrivateKey):
            raise TypeError(f"unsupported Kalshi key type: {type(key).__name__}")
        return cls(key_id, key)

    @property
    def key_type(self) -> str:
        return "ed25519" if isinstance(self._key, Ed25519PrivateKey) else "rsa"

    def sign(self, text: str) -> str:
        msg = text.encode()
        if isinstance(self._key, Ed25519PrivateKey):
            sig = self._key.sign(msg)
        else:
            sig = self._key.sign(
                msg,
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256(),
            )
        return base64.b64encode(sig).decode()

    def headers(self, method: str, path: str, *, now_ms: int | None = None) -> dict[str, str]:
        ts = str(now_ms if now_ms is not None else int(time.time() * 1000))
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": self.sign(ts + method.upper() + path.split("?")[0]),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }
