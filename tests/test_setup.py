from __future__ import annotations

import asyncio
import stat

import pytest
from dotenv import dotenv_values

from pmx.connectors.kalshi.auth import KalshiSigner
from pmx.setup import check, install_key, keygen, set_key_id


def test_keygen_writes_private_key_0600_and_env(tmp_path):
    env_file = tmp_path / ".env"
    path, public = keygen("demo", key_dir=tmp_path / "keys", env_file=env_file)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert public.startswith("-----BEGIN PUBLIC KEY-----")
    assert "PRIVATE" not in public
    set_key_id("demo", " abc-123 \n", env_file=env_file)
    vals = dotenv_values(env_file)
    assert vals["KALSHI_DEMO_PRIVATE_KEY_PATH"] == str(path) and vals["KALSHI_DEMO_API_KEY_ID"] == "abc-123"
    assert KalshiSigner.from_file("abc-123", path).key_type == "ed25519"
    with pytest.raises(FileExistsError):
        keygen("demo", key_dir=tmp_path / "keys", env_file=env_file)


def test_check_reports_missing_key(monkeypatch, capsys):
    monkeypatch.delenv("KALSHI_DEMO_PRIVATE_KEY_PATH", raising=False)
    monkeypatch.delenv("KALSHI_DEMO_API_KEY_ID", raising=False)
    assert asyncio.run(check("demo")) is False
    assert "pmx keygen --env demo" in capsys.readouterr().out


def _rsa_pem() -> bytes:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                             serialization.NoEncryption())


def test_install_key_moves_kalshis_file_into_place(tmp_path):
    keys, env_file = tmp_path / "keys", tmp_path / ".env"
    download = tmp_path / "Downloads" / "kalshi key.txt"
    download.parent.mkdir()
    pem = _rsa_pem()
    download.write_bytes(b"API key id: abc\r\n\r\n" + pem.replace(b"\n", b"\r\n  ") + b"\r\nthanks\r\n")
    path, kind = install_key("prod", download, key_dir=keys, env_file=env_file)
    assert kind == "rsa" and path == keys / "pmx-prod.key" and not download.exists(), "moved, not copied"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600 and path.read_bytes() == pem
    assert dotenv_values(env_file)["KALSHI_PROD_PRIVATE_KEY_PATH"] == str(path)
    assert KalshiSigner.from_file("abc", path).key_type == "rsa"
    other = tmp_path / "second.pem"
    other.write_bytes(_rsa_pem())
    install_key("prod", other, key_dir=keys, env_file=env_file, keep=True)
    assert other.exists() and (keys / "pmx-prod.key.old").read_bytes() == pem, "the previous key is kept aside"


def test_install_key_refuses_what_isnt_a_private_key(tmp_path):
    note = tmp_path / "note.txt"
    note.write_text("just the key id: 1234")
    with pytest.raises(ValueError, match="has no private key in it"):
        install_key("demo", note, key_dir=tmp_path / "keys", env_file=tmp_path / ".env")
    broken = tmp_path / "broken.txt"
    broken.write_bytes(b"\n".join(_rsa_pem().splitlines()[:5] + [b"-----END RSA PRIVATE KEY-----"]))
    with pytest.raises(ValueError, match="doesn't load"):
        install_key("demo", broken, key_dir=tmp_path / "keys", env_file=tmp_path / ".env")
    assert note.exists() and broken.exists() and not (tmp_path / "keys" / "pmx-demo.key").exists()


def test_setkey_refuses_a_private_key_pasted_as_the_id(tmp_path, monkeypatch):
    from pmx.cli import main

    with pytest.raises(SystemExit, match="looks like a private key"):
        main(["setkey", "--env", "prod", "-----BEGIN RSA PRIVATE KEY-----", "--no-check"])


def test_setkey_asks_for_what_is_missing_and_installs_a_dragged_file(tmp_path, monkeypatch, capsys):
    import sys

    from pmx import config, setup
    from pmx.cli import main

    monkeypatch.setattr(setup, "KEY_DIR", tmp_path / "keys")
    monkeypatch.setattr(config, "PROJECT", tmp_path)
    monkeypatch.setattr(config, "load_env", lambda: None)
    monkeypatch.delenv("KALSHI_PROD_PRIVATE_KEY_PATH", raising=False)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    download = tmp_path / "My Key.txt"
    download.write_bytes(_rsa_pem())
    answers = iter(["", "4f1c2a9e-1b2c-4d5e-8f90-123456789abc", str(download).replace(" ", "\\ ") + " "])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(answers))
    main(["setkey", "--no-check"])
    vals = dotenv_values(tmp_path / ".env")
    assert vals["KALSHI_PROD_API_KEY_ID"] == "4f1c2a9e-1b2c-4d5e-8f90-123456789abc", "prod is the default when asked"
    assert vals["KALSHI_PROD_PRIVATE_KEY_PATH"] == str(tmp_path / "keys" / "pmx-prod.key")
    assert not download.exists()
    assert "BEGIN" not in capsys.readouterr().out, "the key is never printed"
