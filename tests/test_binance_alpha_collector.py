import json
from datetime import datetime, timezone

from core.data.binance_alpha_collector import (
    AlphaCollectorConfig,
    _SQLiteStore,
    _parse_kline_rows,
    _parse_orderbook_row,
    _parse_trade_rows,
)


def test_collector_config_keeps_research_intervals():
    config = AlphaCollectorConfig(kline_intervals=("1m", "1h"))

    assert config.kline_intervals == ("1m", "1h")
    assert config.max_tokens == 400
    assert config.as_dict()["kline_intervals"] == ["1m", "1h"]


def test_parsers_normalize_alpha_market_payloads():
    klines = _parse_kline_rows(
        "ALPHA_175",
        "ALPHA_175USDT",
        "1m",
        [[1788668220000, "1.0", "1.2", "0.9", "1.1", "12", 1788668279999, "13.2", 7, "6", "6.6", "0"]],
        captured_at="2026-09-06T00:00:00+00:00",
    )
    trades = _parse_trade_rows(
        "ALPHA_175",
        "ALPHA_175USDT",
        [{"a": 7, "p": "1.1", "q": "2", "f": 7, "l": 7, "T": 1788668279000, "m": False}],
        captured_at="2026-09-06T00:00:00+00:00",
    )
    orderbook = _parse_orderbook_row(
        "ALPHA_175",
        "ALPHA_175USDT",
        {"lastUpdateId": 9, "E": 10, "T": 11, "bids": [["1.0", "2"]], "asks": [["1.1", "3"]]},
        captured_at="2026-09-06T00:00:00+00:00",
    )

    assert klines[0]["close"] == 1.1
    assert klines[0]["trade_count"] == 7
    assert trades[0]["trade_id"] == 7
    assert orderbook["last_update_id"] == 9
    assert json.loads(orderbook["bids_json"])[0] == ["1.0", "2"]


def test_sqlite_store_upserts_klines_and_keeps_auxiliary_rows(tmp_path):
    store = _SQLiteStore(tmp_path / "alpha_market.db")
    klines = _parse_kline_rows(
        "ALPHA_175",
        "ALPHA_175USDT",
        "1m",
        [
            [1000, "1", "2", "0.5", "1.5", "4", 5999, "6", 2, "2", "3", "0"],
            [60000, "1.5", "2", "1", "1.8", "5", 119999, "9", 3, "2", "3", "0"],
        ],
        captured_at="2026-09-06T00:00:00+00:00",
    )
    trade = _parse_trade_rows(
        "ALPHA_175",
        "ALPHA_175USDT",
        [{"a": 12, "p": "1.5", "q": "2", "f": 12, "l": 12, "T": 2000, "m": True}],
        captured_at="2026-09-06T00:00:00+00:00",
    )[0]
    book = _parse_orderbook_row(
        "ALPHA_175",
        "ALPHA_175USDT",
        {"lastUpdateId": 1, "E": 2, "T": 2, "bids": [], "asks": []},
        captured_at="2026-09-06T00:00:00+00:00",
    )

    store.persist_batch(
        captured_at="2026-09-06T00:00:00+00:00",
        tokens=[{"alphaId": "ALPHA_175", "price": "1.5", "volume24h": "10"}],
        klines=klines,
        trades=[trade],
        orderbooks=[book],
        run_id="run-1",
        started_at="2026-09-06T00:00:00+00:00",
        finished_at="2026-09-06T00:00:01+00:00",
        run_status="ok",
        summary={"kline_rows": 2, "trade_rows": 1, "orderbook_rows": 1},
    )

    assert store.latest_kline_times("1m") == {"ALPHA_175": 60000}
    assert store.latest_trade_ids() == {"ALPHA_175": 12}
    assert store.path.exists()

    deleted = store.prune(
        AlphaCollectorConfig(
            kline_retention_days=1,
            kline_min_bars=1,
            trade_retention_hours=1,
            orderbook_retention_hours=1,
            market_retention_hours=1,
            run_retention_days=1,
        ),
        now=datetime(2026, 9, 8, tzinfo=timezone.utc),
    )

    assert deleted == {
        "market_snapshots": 1,
        "alpha_agg_trades": 1,
        "alpha_orderbook_snapshots": 1,
        "collector_runs": 1,
        "alpha_klines": 1,
    }
    assert store.counts() == {
        "market_snapshots": 0,
        "klines": 1,
        "agg_trades": 0,
        "orderbook_snapshots": 0,
        "collector_runs": 0,
    }


def test_retention_keeps_minimum_bars_for_slow_timeframes(tmp_path):
    store = _SQLiteStore(tmp_path / "slow_timeframe.db")
    klines = _parse_kline_rows(
        "ALPHA_175",
        "ALPHA_175USDT",
        "1d",
        [
            [1577836800000, "1", "2", "0.5", "1.5", "4", 1577923199999, "6", 2, "2", "3", "0"],
            [1577923200000, "1.5", "2", "1", "1.8", "5", 1578009599999, "9", 3, "2", "3", "0"],
        ],
        captured_at="2020-01-02T00:00:00+00:00",
    )
    store.persist_batch(
        captured_at="2020-01-02T00:00:00+00:00",
        tokens=[],
        klines=klines,
        trades=[],
        orderbooks=[],
        run_id="slow-run",
        started_at="2020-01-02T00:00:00+00:00",
        finished_at="2020-01-02T00:00:01+00:00",
        run_status="ok",
        summary={},
    )

    deleted = store.prune(
        AlphaCollectorConfig(kline_retention_days=14, kline_min_bars=1200),
        now=datetime(2026, 9, 8, tzinfo=timezone.utc),
    )

    assert deleted["alpha_klines"] == 0
    assert store.counts()["klines"] == 2
