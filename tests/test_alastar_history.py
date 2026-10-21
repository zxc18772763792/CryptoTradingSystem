from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import requests

from scripts.collect_alastar_history import ENDPOINT, HistoryCollectionError, _fetch_page, _previous_url, collect_history
from core.news.collectors.common import BeautifulSoup


def _message(message_id: int, text: str = "#BTC entry zone update", date: str | None = "2026-10-01T01:02:03Z", photo: bool = False) -> str:
    content = f'<div class="tgme_widget_message_text js-message_text">{text}</div>' if text else ""
    timestamp = f'<time datetime="{date}"></time>' if date else ""
    image = '<a class="tgme_widget_message_photo_wrap"></a>' if photo else ""
    return f'<div class="tgme_widget_message" data-post="AlastarTrades/{message_id}">{image}{content}<span class="tgme_widget_message_meta"><a class="tgme_widget_message_date">{timestamp}</a></span></div>'


def _page(*messages: str, before: int | None = None) -> str:
    previous = f'<link rel="prev" href="/s/alastartrades?before={before}">' if before else ""
    return f"<html><head>{previous}</head><body>{''.join(messages)}</body></html>"


class _Response:
    def __init__(self, html: str, status: int = 200, headers: dict | None = None):
        self.status_code = status
        self.headers = headers or {}
        self.body = html.encode("utf-8")
        self.closed = False

    def iter_content(self, chunk_size: int):
        yield self.body

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, responses: list):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


def test_archive_follows_public_pagination_deduplicates_and_preserves_audit_evidence(tmp_path):
    first = _page(_message(5), _message(4), before=4)
    second = _page(_message(4), _message(2, date="2026-09-01T01:02:03+00:00"))
    session = _Session([_Response(first), _Response(second)])
    waits = []

    manifest = collect_history(tmp_path, session=session, sleep_fn=waits.append)
    messages = [json.loads(line) for line in (tmp_path / "messages.jsonl").read_text(encoding="utf-8").splitlines()]

    assert manifest["status"] == "complete"
    assert manifest["stop_reason"] == "public_history_boundary"
    assert manifest["complete_public_preview_pagination"] is True
    assert manifest["complete_channel_history"] is False
    assert manifest["page_count"] == 2
    assert manifest["parsed_message_count"] == 3
    assert manifest["oldest_observed_message_id"] == 2
    assert manifest["not_visible_id_ranges"] == [{"start": 3, "end": 3, "count": 1}]
    assert [message["payload"]["message_id"] for message in messages] == [2, 4, 5]
    assert messages[0]["published_at"] == "2026-09-01T01:02:03+00:00"
    assert messages[0]["collection"]["original_published_at"] == messages[0]["published_at"]
    assert messages[0]["collected_at"]
    assert waits == [1.8]
    assert [call[0] for call in session.calls] == [ENDPOINT, ENDPOINT + "?before=4"]
    assert session.calls[0][1]["allow_redirects"] is False
    page_file = tmp_path / manifest["pages"][0]["file"]
    assert page_file.read_bytes() == first.encode("utf-8")
    assert hashlib.sha256(page_file.read_bytes()).hexdigest() == manifest["pages"][0]["sha256"]
    assert json.loads((tmp_path / "collection_manifest.json").read_text(encoding="utf-8"))["messages_sha256"] == hashlib.sha256((tmp_path / "messages.jsonl").read_bytes()).hexdigest()


def test_image_only_and_undated_posts_are_inventoried_without_fabricating_text_or_dates(tmp_path):
    html = _page(_message(1), _message(2, text="", photo=True), _message(3, date=None))
    manifest = collect_history(tmp_path, session=_Session([_Response(html)]), sleep_fn=lambda _: None)

    assert manifest["observed_message_count"] == 3
    assert manifest["parsed_message_count"] == 1
    skipped = {item["message_id"]: item for item in manifest["unparsed_messages"]}
    assert skipped[2]["reason"] == "media_without_text_or_caption"
    assert skipped[2]["has_photo"] is True
    assert skipped[3]["reason"] == "missing_or_invalid_publication_time"


def test_page_limit_is_reported_as_incomplete_and_keeps_next_cursor(tmp_path):
    manifest = collect_history(tmp_path, max_pages=1, session=_Session([_Response(_page(_message(4), before=4))]), sleep_fn=lambda _: None)

    assert manifest["status"] == "incomplete"
    assert manifest["stop_reason"] == "max_pages"
    assert manifest["next_page_url"] == ENDPOINT + "?before=4"


def test_http_failure_preserves_earlier_pages_and_reports_incomplete_archive(tmp_path):
    session = _Session([_Response(_page(_message(4), before=4)), _Response("private", status=403)])
    manifest = collect_history(tmp_path, session=session, sleep_fn=lambda _: None)

    assert manifest["status"] == "incomplete"
    assert manifest["parsed_message_count"] == 1
    assert "HTTP 403" in manifest["error"]
    assert len(session.calls) == 2
    assert (tmp_path / "messages.jsonl").exists()


def test_rate_limit_retry_obeys_retry_after_and_closes_response():
    throttled = _Response("wait", status=429, headers={"Retry-After": "7"})
    session = _Session([throttled, _Response("ok")])
    waits = []

    assert _fetch_page(session, ENDPOINT, waits.append) == b"ok"
    assert waits == [7.0]
    assert throttled.closed is True


def test_long_provider_cooldown_stops_without_retrying_early():
    session = _Session([_Response("wait", status=429, headers={"Retry-After": "120"})])
    waits = []

    with pytest.raises(HistoryCollectionError, match="provider cooldown"):
        _fetch_page(session, ENDPOINT, waits.append)
    assert waits == []
    assert len(session.calls) == 1


def test_transient_failure_has_bounded_backoff():
    session = _Session([requests.Timeout("sensitive proxy address"), _Response("bad", status=503), _Response("ok")])
    waits = []

    assert _fetch_page(session, ENDPOINT, waits.append) == b"ok"
    assert waits == [5.0, 10.0]


@pytest.mark.parametrize("href", ["https://example.com/s/alastartrades?before=4", "/s/private?before=4", "/s/alastartrades?before=4&token=secret", "/s/alastartrades?before=not-an-id", "/s/alastartrades?before=5"])
def test_unsafe_or_nonprogressing_previous_links_are_rejected(href):
    soup = BeautifulSoup(f'<link rel="prev" href="{href}">', "html.parser")
    with pytest.raises(HistoryCollectionError):
        _previous_url(soup, ENDPOINT + "?before=5")


def test_more_link_is_supported_when_rel_prev_is_absent():
    soup = BeautifulSoup('<a class="tme_messages_more" href="/s/alastartrades?before=4"></a>', "html.parser")
    assert _previous_url(soup, ENDPOINT) == ENDPOINT + "?before=4"


def test_oldest_page_newer_after_link_is_not_followed_as_older_history():
    soup = BeautifulSoup('<a class="tme_messages_more" href="/s/alastartrades?after=3"></a>', "html.parser")
    assert _previous_url(soup, ENDPOINT + "?before=4") is None


def test_cryptoquant_channel_uses_its_own_identity_and_page_boundary(tmp_path):
    html = _page(_message(4)).replace("AlastarTrades", "cryptoquant_alert")
    result = collect_history(tmp_path, channel="cryptoquant_alert", session=_Session([_Response(html)]), sleep_fn=lambda _: None)
    messages = [json.loads(line) for line in (tmp_path / "messages.jsonl").read_text(encoding="utf-8").splitlines()]
    assert result["channel"] == "cryptoquant_alert"
    assert messages[0]["source_id"] == "telegram:cryptoquant_alert:4"
    assert messages[0]["source"] == "telegram_cryptoquant_alert"


def test_empty_preview_does_not_claim_a_verified_history_boundary(tmp_path):
    manifest = collect_history(tmp_path, session=_Session([_Response("<html>View in Telegram</html>")]), sleep_fn=lambda _: None)
    assert manifest["status"] == "incomplete"
    assert manifest["complete_public_preview_pagination"] is False
    assert "boundary is unverified" in manifest["error"]


def test_existing_research_archive_is_not_overwritten(tmp_path):
    (tmp_path / "messages.jsonl").write_text("existing", encoding="utf-8")
    with pytest.raises(FileExistsError):
        collect_history(tmp_path, session=_Session([]), sleep_fn=lambda _: None)
    assert (tmp_path / "messages.jsonl").read_text(encoding="utf-8") == "existing"


@pytest.mark.parametrize("kwargs", [{"max_pages": 0}, {"max_pages": 201}, {"delay_seconds": 0.1}])
def test_request_and_page_budgets_are_bounded(tmp_path, kwargs):
    with pytest.raises(ValueError):
        collect_history(tmp_path, **kwargs)
