from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd


_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "standalone_strategy_miner.py"
_SPEC = importlib.util.spec_from_file_location("standalone_strategy_miner", _SCRIPT_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC is not None and _SPEC.loader is not None
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


def test_aggregate_candidates_prefers_stable_trade_count() -> None:
    df = pd.DataFrame(
        [
            {
                "phase": "base",
                "symbol": "BTC/USDT",
                "timeframe": "10s",
                "strategy": "VWAPReversionStrategy",
                "total_return": 12.0,
                "sharpe_ratio": 2.5,
                "max_drawdown": 3.0,
                "win_rate": 70.0,
                "total_trades": 18,
                "anomaly_bar_ratio": 0.0,
                "quality_flag": "ok",
                "score": _MODULE._score_run(
                    {
                        "total_return": 12.0,
                        "sharpe_ratio": 2.5,
                        "max_drawdown": 3.0,
                        "win_rate": 70.0,
                        "total_trades": 18,
                        "anomaly_bar_ratio": 0.0,
                        "quality_flag": "ok",
                    }
                ),
                "error": "",
            },
            {
                "phase": "recent",
                "symbol": "ETH/USDT",
                "timeframe": "10s",
                "strategy": "VWAPReversionStrategy",
                "total_return": 9.0,
                "sharpe_ratio": 2.2,
                "max_drawdown": 4.0,
                "win_rate": 66.0,
                "total_trades": 12,
                "anomaly_bar_ratio": 0.0,
                "quality_flag": "ok",
                "score": _MODULE._score_run(
                    {
                        "total_return": 9.0,
                        "sharpe_ratio": 2.2,
                        "max_drawdown": 4.0,
                        "win_rate": 66.0,
                        "total_trades": 12,
                        "anomaly_bar_ratio": 0.0,
                        "quality_flag": "ok",
                    }
                ),
                "error": "",
            },
            {
                "phase": "base",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "strategy": "ADXTrendStrategy",
                "total_return": 15.0,
                "sharpe_ratio": 3.5,
                "max_drawdown": 2.0,
                "win_rate": 100.0,
                "total_trades": 1,
                "anomaly_bar_ratio": 0.0,
                "quality_flag": "ok",
                "score": _MODULE._score_run(
                    {
                        "total_return": 15.0,
                        "sharpe_ratio": 3.5,
                        "max_drawdown": 2.0,
                        "win_rate": 100.0,
                        "total_trades": 1,
                        "anomaly_bar_ratio": 0.0,
                        "quality_flag": "ok",
                    }
                ),
                "error": "",
            },
            {
                "phase": "recent",
                "symbol": "BTC/USDT",
                "timeframe": "15m",
                "strategy": "ADXTrendStrategy",
                "total_return": 8.0,
                "sharpe_ratio": 2.0,
                "max_drawdown": 1.5,
                "win_rate": 100.0,
                "total_trades": 1,
                "anomaly_bar_ratio": 0.0,
                "quality_flag": "ok",
                "score": _MODULE._score_run(
                    {
                        "total_return": 8.0,
                        "sharpe_ratio": 2.0,
                        "max_drawdown": 1.5,
                        "win_rate": 100.0,
                        "total_trades": 1,
                        "anomaly_bar_ratio": 0.0,
                        "quality_flag": "ok",
                    }
                ),
                "error": "",
            },
        ]
    )

    summary = _MODULE._aggregate_candidates(df)
    leaders = _MODULE._pick_leaders(summary)

    assert not summary.empty
    assert leaders["recommended"]["strategy"] == "VWAPReversionStrategy"


def test_discover_symbols_requires_all_needed_timeframes(tmp_path: Path) -> None:
    base = tmp_path / "historical" / "binance" / "BTC_USDT"
    base.mkdir(parents=True)
    (base / "1m.parquet").write_text("", encoding="utf-8")
    (base / "1h.parquet").write_text("", encoding="utf-8")

    eth = tmp_path / "historical" / "binance" / "ETH_USDT"
    eth.mkdir(parents=True)
    (eth / "1h.parquet").write_text("", encoding="utf-8")

    found = _MODULE._discover_symbols(
        tmp_path,
        "binance",
        ["BTC/USDT", "ETH/USDT"],
        required_timeframes=["1m", "1h"],
        max_count=5,
    )

    assert found == ["BTC/USDT"]
