from datetime import datetime, timedelta, timezone
import json

import pytest

from scripts.probe_safew_export import MAX_BYTES, import_export, parse_export


NOW = datetime(2026, 10, 30, 14, tzinfo=timezone.utc)


def document(text="Login code: 012345. Device: example-private-device"):
    return {"id": 777000, "name": "SafeW", "type": "personal_chat", "messages": [{
        "id": 1, "type": "message", "from_id": "user777000",
        "date": "2026-10-30T20:00:00", "date_unixtime": str(int((NOW - timedelta(hours=2)).timestamp())),
        "text": text,
    }]}


def raw(value):
    return json.dumps(value, ensure_ascii=False).encode()


def test_live_file_import_deduplicates_without_retaining_security_notice(tmp_path):
    source = tmp_path / "export.json"
    source.write_bytes(raw(document()))
    output = tmp_path / "probe"
    first = import_export(source, output, observed_at=NOW)
    second = import_export(source, output, observed_at=NOW + timedelta(minutes=1))
    assert first["new_versions"] == 1
    assert second["new_versions"] == 0
    assert second["duplicate_versions"] == 1
    assert second["total_unique_messages"] == 1
    assert second["live_connection_verified"] is False
    saved = (output / "status.json").read_text()
    assert "012345" not in saved and "example-private-device" not in saved
    envelope = next(iter(json.loads(saved)["versions"].values()))
    assert envelope["published_at_utc"] == "2026-10-30T12:00:00+00:00"
    assert envelope["first_observed_at_utc"] == NOW.isoformat()
    assert envelope["strategy_eligible"] is False
    assert envelope["provenance"] == "historical_desktop_export"


def test_rich_text_equals_plain_text_but_changed_content_is_a_new_version(tmp_path):
    source, output = tmp_path / "export.json", tmp_path / "probe"
    source.write_bytes(raw(document("official notice")))
    import_export(source, output, observed_at=NOW)
    source.write_bytes(raw(document(["official ", {"type": "bold", "text": "notice"}])))
    assert import_export(source, output, observed_at=NOW)["new_versions"] == 0
    source.write_bytes(raw(document("updated notice")))
    result = import_export(source, output, observed_at=NOW)
    assert result["new_versions"] == 1 and result["total_versions"] == 2
    assert result["total_unique_messages"] == 1


@pytest.mark.parametrize("field,value", [("id", 123456), ("id", True), ("name", "Other"), ("type", "channel")])
def test_rejects_other_sources(field, value):
    export = document()
    export[field] = value
    with pytest.raises(ValueError):
        parse_export(raw(export), NOW)


@pytest.mark.parametrize("change", [
    {"date_unixtime": "bad"}, {"date_unixtime": True}, {"from_id": "user123"},
    {"date_unixtime": str(int((NOW + timedelta(seconds=1)).timestamp()))},
    {"edited_unixtime": str(int((NOW - timedelta(hours=3)).timestamp()))},
    {"text": [{"type": "link", "url": "not-a-text-segment"}]},
])
def test_invalid_record_fails_before_creating_output(tmp_path, change):
    export = document()
    export["messages"][0].update(change)
    source, output = tmp_path / "export.json", tmp_path / "probe"
    source.write_bytes(raw(export))
    with pytest.raises(ValueError):
        import_export(source, output, observed_at=NOW)
    assert not output.exists()


def test_rejects_duplicate_ids_empty_and_oversized_exports():
    export = document()
    export["messages"] *= 2
    with pytest.raises(ValueError):
        parse_export(raw(export), NOW)
    export["messages"] = []
    with pytest.raises(ValueError):
        parse_export(raw(export), NOW)
    with pytest.raises(ValueError):
        parse_export(b" " * (MAX_BYTES + 1), NOW)


def test_timezone_is_required():
    with pytest.raises(ValueError):
        parse_export(raw(document()), NOW.replace(tzinfo=None))
