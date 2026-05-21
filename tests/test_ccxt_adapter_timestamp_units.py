from datetime import datetime

from core.exchange_adapters.ccxt_adapter import _to_dt_ms


def test_to_dt_ms_keeps_normal_millisecond_timestamp() -> None:
    assert _to_dt_ms(1_714_832_296_000) == datetime(2024, 5, 4, 14, 18, 16)


def test_to_dt_ms_accepts_seconds_microseconds_and_nanoseconds() -> None:
    expected = datetime(2024, 5, 4, 14, 18, 16)

    assert _to_dt_ms(1_714_832_296) == expected
    assert _to_dt_ms(1_714_832_296_000_000) == expected
    assert _to_dt_ms(1_714_832_296_000_000_000) == expected
