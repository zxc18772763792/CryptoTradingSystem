from datetime import datetime, timedelta, timezone
import json

import pytest

from scripts import backtest_binance_chat_sentiment as bt


BASE = datetime(2026, 10, 1, tzinfo=timezone.utc)


def view(i, at, side=-1):
    return {"message_id": i, "published_at": at.isoformat(), "available_at": at.isoformat(),
            "author": f"author-{i}", "text_sha": f"text-{i}", "side": side}


def test_coverage_gap_resets_batches_but_relaxed_case_is_explicit():
    views = [view(i, BASE + timedelta(minutes=i)) for i in range(5)]
    views += [view(i, BASE + timedelta(hours=1, minutes=i)) for i in range(5, 10)]
    data = {"views": views, "coverage": [
        [BASE.isoformat(), (BASE + timedelta(minutes=5)).isoformat()],
        [(BASE + timedelta(hours=1)).isoformat(), (BASE + timedelta(hours=2)).isoformat()]]}
    features, funnel = bt.make_features(data)
    assert not [f for f in features if f["case"] == "strict"]
    relaxed = [f for f in features if f["case"] == "coverage_relaxed"]
    assert len(relaxed) == 1 and relaxed[0]["qualified"]
    assert relaxed[0]["coverage_certified"] is False
    strict10 = next(v for v in funnel if v["case"] == "strict" and v["size"] == 10)
    assert strict10["incomplete_batch_views"] == 10


def test_long_windows_do_not_qualify_when_coverage_is_relaxed():
    views = [view(i, BASE + timedelta(hours=i * 3)) for i in range(10)]
    features, _ = bt.make_features({"views": views, "coverage": []})
    assert len(features) == 1 and features[0]["status"] == "long_window"
    assert not bt.make_events(features)[0]


def candidate(at=BASE, size=10, feature_id="first"):
    return {"qualified": True, "available_at": at.isoformat(), "feature_id": feature_id,
            "case": "coverage_relaxed", "size": size, "score": -60, "authors": 10,
            "span_hours": 2, "coverage_certified": False}


@pytest.mark.parametrize("seconds,minutes", [(239, 5), (240, 10)])
def test_entry_is_strictly_after_freeze_plus_sixty_seconds(seconds, minutes):
    events, skipped = bt.make_events([candidate(BASE + timedelta(seconds=seconds))])
    assert not skipped and len(events) == 2
    assert {bt.utc(e["entry_at"]) for e in events} == {BASE + timedelta(minutes=minutes)}
    assert {e["side"] for e in events} == {-1, 1}


def test_overlap_is_per_book_and_equal_exit_boundary_can_reenter():
    features = [candidate(), candidate(BASE + timedelta(hours=2), feature_id="overlap"),
                candidate(BASE + timedelta(hours=2), size=20, feature_id="other-size"),
                candidate(BASE + timedelta(hours=6), feature_id="next")]
    events, skipped = bt.make_events(features)
    assert len(skipped) == 2
    assert len(events) == 6
    assert {e["feature_id"] for e in events} == {"first", "other-size", "next"}


def market():
    return {"bars": [{"at": BASE.isoformat(), "open": 100},
                     {"at": (BASE + timedelta(hours=6)).isoformat(), "open": 102}],
            "funding": [{"at": BASE.isoformat(), "rate": .5, "mark": 100},
                        {"at": (BASE + timedelta(hours=6)).isoformat(), "rate": .01, "mark": 110}]}


def event(side=1):
    return {"side": side, "entry_at": BASE.isoformat(),
            "exit_at": (BASE + timedelta(hours=6)).isoformat()}


def test_cash_funding_excludes_entry_includes_exit_and_uses_settlement_mark():
    long = bt.settle(event(), market(), BASE + timedelta(days=1))
    short = bt.settle(event(-1), market(), BASE + timedelta(days=1))
    assert long["status"] == short["status"] == "settled"
    assert long["funding"] == pytest.approx(-.011)
    assert long["net"] == pytest.approx(.007)
    assert short["net"] == pytest.approx(-.011)
    assert long["net"] + short["net"] == pytest.approx(-.004)
    assert long["net"] - long["net_stress"] == pytest.approx(.002)


@pytest.mark.parametrize("missing,status", [("prices", "pending_price"),
                                           ("funding", "pending_funding"),
                                           ("mark", "funding_mark_missing")])
def test_missing_market_inputs_do_not_become_zero_funding_or_zero_profit(missing, status):
    data = market()
    if missing == "prices":
        data["bars"] = []
    elif missing == "funding":
        data["funding"] = []
    else:
        data["funding"][1]["mark"] = None
    result = bt.settle(event(), data, BASE + timedelta(days=1))
    assert result["status"] == status and "net" not in result


def test_future_exit_remains_immature_even_when_later_prices_exist():
    assert bt.settle(event(), market(), BASE + timedelta(hours=5)) == {"status": "immature_exit"}


def test_empty_samples_and_leave_best_out_preserve_null():
    assert bt.metrics([])["mean_net_pct"] is None
    sample = {"outcome": bt.settle(event(), market(), BASE + timedelta(days=1))}
    result = bt.metrics([sample])
    assert result["settled"] == 1 and result["mean_without_best_pct"] is None


def receipts(tmp_path):
    quotes = {"tool": "get_futures_usds_kline_candlestick_data", "request": {"symbol": "BTCUSDT", "interval": "5m"},
              "response": {"result": [[int(BASE.timestamp() * 1000), "100", "100", "100", "100"]]}}
    funds = {"tool": "get_futures_usds_funding_rate_history", "request": {"symbol": "BTCUSDT", "limit": 16},
             "response": {"result": [{"symbol": "BTCUSDT", "fundingTime": int(BASE.timestamp() * 1000),
                                      "fundingRate": "0.001", "markPrice": "100"}]}}
    for name, value in (("binance_klines.json", quotes), ("binance_funding.json", funds)):
        (tmp_path / name).write_text(json.dumps(value), encoding="utf-8")
    return quotes, funds


@pytest.mark.parametrize("problem", ["duplicate-funding", "spot", "invalid-price", "capped-funding"])
def test_market_receipt_validation(problem, tmp_path):
    quotes, funds = receipts(tmp_path)
    if problem == "duplicate-funding":
        funds["response"]["result"] *= 2
    elif problem == "spot":
        quotes["tool"] = "get_spot_kline_candlestick_data"
    elif problem == "invalid-price":
        quotes["response"]["result"][0][1] = "NaN"
    else:
        funds["request"]["limit"] = 1
    (tmp_path / "binance_klines.json").write_text(json.dumps(quotes), encoding="utf-8")
    (tmp_path / "binance_funding.json").write_text(json.dumps(funds), encoding="utf-8")
    with pytest.raises(ValueError):
        bt.market_snapshot(tmp_path)


@pytest.mark.parametrize("problem", ["future-label", "duplicate-message", "checksum"])
def test_frozen_input_validation(problem, tmp_path):
    data = {"metadata": {"activation_at": BASE.isoformat()},
            "source_checkpoint": (BASE + timedelta(hours=1)).isoformat(),
            "views": [view(1, BASE + timedelta(minutes=1))]}
    if problem == "future-label":
        data["views"][0]["available_at"] = (BASE + timedelta(hours=2)).isoformat()
    elif problem == "duplicate-message":
        data["views"] *= 2
    data["snapshot_sha"] = bt.digest(data)
    if problem == "checksum":
        data["views"][0]["side"] = 1
    (tmp_path / "inputs.json").write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ValueError):
        bt.read_inputs(tmp_path)
