"""The X token helper edits .env.local safely and never prints the token."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "set_x_token.py"
TOKEN = "0123456789abcdef0123456789abcdef01234567"
spec = importlib.util.spec_from_file_location("set_x_token", SCRIPT)
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


def test_update_replaces_active_lines_and_appends_the_rest():
    text = "A=1\r\n# X_AUTH_TOKEN=commented\r\nX_AUTH_TOKEN=old\r\nB=2\r\nX_AUTH_TOKEN=older\r\n"
    out = helper.update_env_text(text, {"X_AUTH_TOKEN": "new", "NEWS_ENABLE_X_ACCOUNTS": "true"})
    assert out == ("A=1\r\n# X_AUTH_TOKEN=commented\r\nX_AUTH_TOKEN=new\r\nB=2\r\nX_AUTH_TOKEN=new\r\n"
                   "NEWS_ENABLE_X_ACCOUNTS=true\r\n")
    assert helper.update_env_text("", {"A": "1"}) == "A=1\n"


def test_write_keeps_bom_and_other_lines(tmp_path):
    path = tmp_path / ".env.local"
    path.write_bytes(b"\xef\xbb\xbfKEEP=1\r\nTRADING_MODE=paper\r\n")
    helper.write_env_file(path, {"X_AUTH_TOKEN": TOKEN})
    assert path.read_bytes() == b"\xef\xbb\xbfKEEP=1\r\nTRADING_MODE=paper\r\nX_AUTH_TOKEN=" + TOKEN.encode() + b"\r\n"
    assert not list(tmp_path.glob("*.tmp"))


def test_token_normalization():
    assert helper.normalize_token(f"  auth_token={TOKEN}\n") == TOKEN
    assert helper.normalize_token(f'"{TOKEN}"') == TOKEN
    for bad in ("", "short", f"{TOKEN} extra", f"{TOKEN}#x", "a=b" * 10):
        with pytest.raises(ValueError):
            helper.normalize_token(bad)


def test_main_writes_the_file_and_never_prints_the_token(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(helper, "ROOT", tmp_path)
    monkeypatch.setattr(helper, "_is_gitignored", lambda: True)
    monkeypatch.setattr(helper.getpass, "getpass", lambda _prompt: TOKEN)
    assert helper.main() == 0
    assert f"X_AUTH_TOKEN={TOKEN}" in (tmp_path / ".env.local").read_text(encoding="utf-8")
    assert "NEWS_ENABLE_X_ACCOUNTS=true" in (tmp_path / ".env.local").read_text(encoding="utf-8")
    shown = capsys.readouterr()
    assert TOKEN not in shown.out + shown.err


def test_main_refuses_when_the_file_could_be_committed(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(helper, "ROOT", tmp_path)
    monkeypatch.setattr(helper, "_is_gitignored", lambda: False)
    monkeypatch.setattr(helper.getpass, "getpass", lambda _prompt: pytest.fail("must not prompt"))
    assert helper.main() == 1
    assert not (tmp_path / ".env.local").exists()
    assert "gitignored" in capsys.readouterr().err
