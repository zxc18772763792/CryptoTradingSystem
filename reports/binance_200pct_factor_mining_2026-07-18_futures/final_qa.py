from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


REPORT_DIR = Path(__file__).resolve().parent
AUDIT_DIR = REPORT_DIR.parent / "binance_200pct_futures_universe_audit_2026-07-18"


def load_json(name: str) -> dict:
    return json.loads((REPORT_DIR / name).read_text(encoding="utf-8"))


def main() -> None:
    runups = pd.read_csv(REPORT_DIR / "current_30d_runups.csv")
    watch = pd.read_csv(REPORT_DIR / "current_watchlist.csv")
    universe = pd.read_csv(AUDIT_DIR / "futures_universe.csv")
    verification = load_json("verification_summary.json")
    data_quality = load_json("data_quality.json")
    model_metrics = load_json("model_metrics.json")
    oi_summary = load_json("oi_factor_summary_30d.json")
    combo_summary = load_json("oi_price_combo_summary_30d.json")

    assert len(runups) == 19
    assert (runups["classification"] == "close-confirmed").sum() == 17
    assert (runups["classification"] == "wick-only").sum() == 2
    assert {"AKEUSDT", "BTWUSDT"}.issubset(set(runups["symbol"]))

    joined = runups[["symbol"]].merge(
        universe[["symbol", "spot_listed_usdt"]],
        on="symbol",
        how="left",
        validate="one_to_one",
    )
    assert joined["spot_listed_usdt"].notna().all()
    assert int((~joined["spot_listed_usdt"]).sum()) == 12

    comparison = verification["comparison"]
    assert comparison["symbol_set_exact_match"] is True
    assert comparison["classification_exact_match"] is True
    assert comparison["event_times_exact_match"] is True
    assert comparison["maximum_absolute_return_difference"] < 1e-12

    assert data_quality["source_market"] == "futures"
    assert data_quality["requested_usdt_perpetual_symbols"] == 530
    assert data_quality["loaded_symbols"] == 530
    assert not data_quality["source_errors"]
    assert data_quality["duplicate_symbol_time_rows"] == 0
    assert data_quality["invalid_ohlc_rows"] == 0

    preferred = model_metrics["preferred_ranking"]
    assert preferred["method"] == "full_factor_model"
    assert preferred["full_model_test_top_2pct_lift"] > preferred[
        "simple_fragility_test_top_2pct_lift"
    ]
    assert watch["ranking_method"].eq("full_factor_model").all()
    assert watch["ranking_score"].is_monotonic_decreasing

    assert oi_summary["coverage"]["fetch_errors"] == 0
    assert oi_summary["current_close_confirmed_events"]["count"] == 17
    assert combo_summary["warning"]

    report_text = (REPORT_DIR / "REPORT.md").read_text(encoding="utf-8")
    for required in [
        "旧结论“只有 5 个收盘确认币”应撤回",
        "17 个收盘确认、2 个仅影线上穿",
        "AKEUSDT",
        "BTWUSDT",
        "单笔中位收益为 -17.53%",
    ]:
        assert required in report_text, required

    result = {
        "status": "passed",
        "events": {
            "total": 19,
            "close_confirmed": 17,
            "wick_only": 2,
            "futures_only": 12,
            "AKEUSDT_present": True,
            "BTWUSDT_present": True,
        },
        "data_quality": {
            "requested_symbols": 530,
            "loaded_symbols": 530,
            "source_errors": 0,
            "duplicate_symbol_time_rows": 0,
            "invalid_ohlc_rows": 0,
        },
        "model": {
            "preferred_ranking": preferred["method"],
            "test_top_2pct_lift": model_metrics["test"]["top_2pct_lift"],
            "independent_events_captured_top_2pct": model_metrics[
                "test_independent_event_capture"
            ]["captured_any_top_2pct"],
        },
        "limitations_asserted": {
            "oi_combo_not_independently_validated": True,
            "strategy_not_approved_for_live_trading": True,
        },
    }
    (REPORT_DIR / "final_qa_summary.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
