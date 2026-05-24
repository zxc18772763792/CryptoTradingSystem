from datetime import datetime, timezone

from core.exchange_adapters.ccxt_adapter import _to_dt_ms


_EXPECTED = datetime(2024, 5, 4, 14, 18, 16, tzinfo=timezone.utc)


def test_to_dt_ms_keeps_normal_millisecond_timestamp() -> None:
    result = _to_dt_ms(1_714_832_296_000)
    assert result == _EXPECTED
    assert result.tzinfo is timezone.utc


def test_to_dt_ms_accepts_seconds_microseconds_and_nanoseconds() -> None:
    assert _to_dt_ms(1_714_832_296) == _EXPECTED
    assert _to_dt_ms(1_714_832_296_000_000) == _EXPECTED
    assert _to_dt_ms(1_714_832_296_000_000_000) == _EXPECTED


def test_to_dt_ms_rejects_non_positive_and_none() -> None:
    assert _to_dt_ms(None) is None
    assert _to_dt_ms(0) is None
    assert _to_dt_ms(-1) is None
    assert _to_dt_ms("not a number") is None
