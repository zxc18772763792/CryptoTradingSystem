from __future__ import annotations

import json

from scripts import selfcheck_opennews


def test_opennews_selfcheck_non_strict_allows_missing_config(monkeypatch, capsys) -> None:
    monkeypatch.delenv("NEWS_ENABLE_OPENNEWS", raising=False)
    monkeypatch.delenv("OPENNEWS_TOKEN", raising=False)
    monkeypatch.setattr(selfcheck_opennews, "load_dotenv", lambda *args, **kwargs: False)
    monkeypatch.setattr("sys.argv", ["selfcheck_opennews.py"])

    code = selfcheck_opennews.main()

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["ok"] is True
    assert payload["configured"] is False
    assert payload["token_present"] is False


def test_opennews_selfcheck_strict_fails_missing_config(monkeypatch, capsys) -> None:
    monkeypatch.delenv("NEWS_ENABLE_OPENNEWS", raising=False)
    monkeypatch.delenv("OPENNEWS_TOKEN", raising=False)
    monkeypatch.setattr(selfcheck_opennews, "load_dotenv", lambda *args, **kwargs: False)
    monkeypatch.setattr("sys.argv", ["selfcheck_opennews.py", "--strict"])

    code = selfcheck_opennews.main()

    payload = json.loads(capsys.readouterr().out)
    assert code == 2
    assert payload["ok"] is False
    assert payload["configured"] is False


def test_opennews_selfcheck_strict_output_is_clean_json(monkeypatch, capsys) -> None:
    monkeypatch.delenv("NEWS_ENABLE_OPENNEWS", raising=False)
    monkeypatch.delenv("OPENNEWS_TOKEN", raising=False)
    monkeypatch.setattr(selfcheck_opennews, "load_dotenv", lambda *args, **kwargs: False)
    monkeypatch.setattr("sys.argv", ["selfcheck_opennews.py", "--strict"])

    selfcheck_opennews.main()

    output = capsys.readouterr().out.strip()
    assert output.startswith("{")
    assert output.endswith("}")
    json.loads(output)
