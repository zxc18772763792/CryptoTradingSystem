from __future__ import annotations

import os

import main


def test_run_web_syncs_cli_trading_mode_into_global_settings(monkeypatch):
    captured = {}

    def fake_uvicorn_run(app, **kwargs):
        captured["app"] = app
        captured["kwargs"] = kwargs

    monkeypatch.setattr(main.uvicorn, "run", fake_uvicorn_run)
    monkeypatch.setattr(main.settings, "TRADING_MODE", "paper", raising=False)

    run_config = main.RunConfig(
        mode="web",
        trading_mode="live",
        web_host="127.0.0.1",
        web_port=8000,
        data_storage_path=main.Path("data"),
        cache_path=main.Path("cache"),
        log_path=main.Path("logs"),
        log_level="INFO",
        log_rotation="10 MB",
        log_retention="30 days",
    )

    main.run_web(run_config)

    assert os.environ["TRADING_MODE"] == "live"
    assert main.settings.TRADING_MODE == "live"
    assert captured["app"] == "web.main:app"
