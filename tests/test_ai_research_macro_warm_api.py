from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock


def test_warm_macro_cache_endpoint_returns_snapshot(tmp_path, monkeypatch):
    from web.api import ai_research as ai_module

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    monkeypatch.setattr(ai_module, "ensure_ai_research_runtime_state", lambda app: None)
    monkeypatch.setattr(
        "core.data.macro_collector.update_macro_cache",
        AsyncMock(return_value={"fed_rate": 1, "cn_cpi_yoy": 1, "ppi_cpi_gap": 1}),
    )
    monkeypatch.setattr(
        "core.data.macro_collector.load_macro_snapshot",
        lambda: {
            "fed_rate": 3.64,
            "cpi_yoy": 2.8,
            "ppi_yoy": 1.2,
            "ppi_cpi_gap": -1.6,
            "cn_cpi_yoy": 1.0,
            "cn_ppi_yoy": 0.5,
            "cn_ppi_cpi_gap": -0.5,
        },
    )
    monkeypatch.setattr(
        "core.data.macro_collector.group_macro_snapshot",
        lambda snapshot: {
            "market": {"vix": None, "dxy": None, "tnx_10y": None},
            "us": {"fed_rate": snapshot["fed_rate"], "ppi_cpi_gap": snapshot["ppi_cpi_gap"]},
            "china": {"cn_cpi_yoy": snapshot["cn_cpi_yoy"], "cn_ppi_cpi_gap": snapshot["cn_ppi_cpi_gap"]},
        },
    )

    monkeypatch.chdir(tmp_path)
    macro_dir = tmp_path / "data" / "macro"
    macro_dir.mkdir(parents=True, exist_ok=True)
    for name in ("fed_rate", "ppi_cpi_gap", "cn_cpi_yoy"):
        (macro_dir / f"{name}.parquet").write_text("placeholder", encoding="utf-8")

    result = asyncio.run(ai_module.warm_ai_macro_cache(request))

    assert result["warmed"] is True
    assert result["macro"]["updated_count"] == 3
    assert "ppi_cpi_gap" in result["macro"]["active_series"]
    assert result["macro"]["regions"]["china"]["cn_ppi_cpi_gap"] == -0.5
    assert result["macro"]["last_updated"] is not None
