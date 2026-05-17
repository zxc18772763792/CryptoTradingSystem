from datetime import timezone

from prediction_markets.polymarket.utils import format_exception, parse_ts_any


def test_parse_ts_any_accepts_epoch_milliseconds():
    dt = parse_ts_any("1772469869953")

    assert dt.tzinfo == timezone.utc
    assert dt.year == 2026
    assert dt.month == 3
    assert dt.day == 2


def test_parse_ts_any_accepts_epoch_microseconds():
    dt = parse_ts_any(1772469869953000)

    assert dt.year == 2026
    assert dt.month == 3
    assert dt.day == 2


def test_format_exception_keeps_type_when_message_is_empty():
    assert format_exception(TimeoutError()) == "TimeoutError"
