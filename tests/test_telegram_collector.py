from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import requests

from core.news.collectors.telegram import TelegramPublicChannelCollector


def _message(
    text: str = "#BTC update<br><br>Whale longs increased while $ETH and #Q diverged.",
    *,
    message_id: int = 453,
    date: str | None = "2026-10-13T14:37:26+08:00",
    channel: str = "AlastarTrades",
    reply: str = "",
    edited: bool = False,
    photo: bool = False,
) -> str:
    # Same selectors and structure as the live public Alastar preview: reply
    # snippets reuse tgme_widget_message_text but use js-message_reply_text.
    quoted = (
        '<a class="tgme_widget_message_reply" href="https://t.me/AlastarTrades/448">'
        f'<div class="tgme_widget_message_text js-message_reply_text">{reply}</div></a>'
    ) if reply else ""
    timestamp = f'<time datetime="{date}">06:37</time>' if date is not None else ""
    image = '<a class="tgme_widget_message_photo_wrap" href="https://t.me/AlastarTrades/453"></a>' if photo else ""
    return (
        f'<div class="tgme_widget_message js-widget_message" data-post="{channel}/{message_id}">'
        f'<div class="tgme_widget_message_bubble">{quoted}{image}'
        f'<div class="tgme_widget_message_text js-message_text">{text}</div>'
        '<div class="tgme_widget_message_footer"><span class="tgme_widget_message_meta">'
        f'{"edited " if edited else ""}<a class="tgme_widget_message_date">{timestamp}</a>'
        '</span></div></div></div>'
    )


@pytest.fixture
def collector():
    instance = TelegramPublicChannelCollector()
    yield instance
    instance.close()


def test_public_message_has_utc_time_stable_identity_and_commentary_provenance(collector):
    item = collector._from_html(_message(edited=True))[0]

    assert item["source"] == "telegram_alastar"
    assert item["source_id"] == "telegram:alastartrades:453"
    assert item["published_at"] == "2026-10-13T06:37:26+00:00"
    assert item["url"] == "https://t.me/alastartrades/453"
    assert item["symbols"] == ["BTCUSDT", "ETHUSDT", "QUSDT"]
    assert item["payload"]["channel"] == "alastartrades"
    assert item["payload"]["message_id"] == 453
    assert item["payload"]["provenance"] == "telegram_public_preview"
    assert item["payload"]["source_kind"] == "third_party_commentary"
    assert item["payload"]["is_third_party_commentary"] is True
    assert item["payload"]["telegram_edited"] is True
    assert item["title"].startswith("#BTC update Whale longs")


def test_photo_caption_excludes_reply_text_and_preserves_visible_text(collector):
    item = collector._from_html(
        _message("#BTW <b>update</b><br><br>New &amp; distinct caption.", reply="#SOL stale duplicated text", photo=True)
    )[0]

    assert item["content"] == "#BTW update\n\nNew & distinct caption."
    assert item["payload"]["original_text"] == item["content"]
    assert item["payload"]["has_photo"] is True
    assert item["symbols"] == ["BTWUSDT"]
    assert "duplicated" not in item["title"]
    assert item["payload"]["telegram_edited"] is False


@pytest.mark.parametrize("date", [None, "", "invalid", "2026-10-13T06:37:26", "2026-02-30T06:37:26Z",
                                 "9999-12-31T23:59:59-01:00", "0001-01-01T00:00:00+01:00"])
def test_missing_or_invalid_publication_is_skipped_instead_of_dated_now(collector, date):
    assert collector._from_html(_message(date=date)) == []


def test_only_allowlisted_channel_posts_are_returned_and_duplicate_widgets_are_removed(collector):
    valid = _message()
    items = collector._from_html(valid + _message(channel="OtherChannel") + valid)

    assert len(items) == 1
    assert items[0]["payload"]["channel"] == "alastartrades"
    with pytest.raises(RuntimeError, match="does not match"):
        collector._from_html(_message(channel="OtherChannel"))


@pytest.mark.parametrize("channels", [["https://t.me/s/alastartrades"], ["../private"], ["+invite"], [], ["user?before=1"]])
def test_configuration_rejects_urls_invites_and_empty_allowlists(channels):
    with pytest.raises(ValueError, match="public|usernames"):
        TelegramPublicChannelCollector({"defaults": {"telegram_alastar_channels": channels}})


def test_html_and_text_limits_are_enforced(collector):
    with pytest.raises(RuntimeError, match="HTML size limit"):
        collector._from_html("x" * (collector.MAX_HTML_BYTES + 1))
    collector.max_text_chars = 64
    item = collector._from_html(_message("#BTC " + "x" * 100))[0]
    assert len(item["content"]) == 64
    assert item["payload"]["text_truncated"] is True
    assert len(item["payload"]["original_text"]) == 64


def test_nonmessage_or_login_page_is_an_explicit_source_failure(collector):
    with pytest.raises(RuntimeError, match="unavailable"):
        collector._from_html('<html><a href="tg://resolve?domain=private">View in Telegram</a></html>')


def _response(body: str, *, url: str = "https://t.me/s/alastartrades", status: int = 200):
    response = requests.Response()
    response.status_code = status
    response.url = url
    response.encoding = "utf-8"
    response._content = body.encode("utf-8")
    return response


def test_pull_filters_by_original_date_sorts_latest_and_uses_only_public_preview(collector, monkeypatch):
    now = datetime.now(timezone.utc)
    body = (
        _message(message_id=450, date=(now - timedelta(minutes=90)).isoformat())
        + _message(message_id=451, date=(now - timedelta(minutes=10)).isoformat())
        + _message(message_id=452, date=(now - timedelta(minutes=5)).isoformat())
    )
    calls: list[str] = []

    def request(url: str, **kwargs: Any):
        calls.append(url)
        return _response(body)

    monkeypatch.setattr(collector, "_request", request)
    items = collector.pull_latest(max_records=1, since_minutes=30)

    assert calls == ["https://t.me/s/alastartrades"]
    assert [item["payload"]["message_id"] for item in items] == [452]


def test_http_failure_propagates_to_manager_instead_of_empty_success(collector, monkeypatch):
    collector.retry_count = 0
    monkeypatch.setattr(collector._session, "request", lambda **kwargs: _response("unavailable", status=503))

    with pytest.raises(RuntimeError, match="request failed"):
        collector.pull_latest()


def test_redirect_to_login_or_another_site_is_rejected(collector, monkeypatch):
    monkeypatch.setattr(collector, "_request", lambda *args, **kwargs: _response("", url="https://t.me/alastartrades"))

    with pytest.raises(RuntimeError, match="redirected"):
        collector.pull_latest()


def test_incremental_overlap_keeps_posts_with_equal_timestamps(collector, monkeypatch):
    published = (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(microsecond=0)
    body = _message(message_id=453, date=published.isoformat()) + _message(message_id=454, date=published.isoformat())
    monkeypatch.setattr(collector, "_request", lambda *args, **kwargs: _response(body))
    items, cursor = collector.pull_incremental(cursor=str(published.timestamp()), since_minutes=30)

    assert {item["payload"]["message_id"] for item in items} == {453, 454}
    assert float(cursor) == published.timestamp()


def test_observation_archive_receives_page_before_age_and_result_limits(collector, monkeypatch):
    from core.news import alastar_observation
    collector.observation_archive_enabled = True
    now = datetime.now(timezone.utc)
    body = _message(message_id=450, date=(now - timedelta(days=7)).isoformat()) + _message(message_id=451, date=now.isoformat())
    calls = []
    monkeypatch.setattr(collector, "_request", lambda *args, **kwargs: _response(body))
    monkeypatch.setattr(alastar_observation, "observe_records", lambda records, **kwargs: calls.append((records, kwargs)))
    returned = collector.pull_latest(since_minutes=30, max_records=1)
    assert len(returned) == 1
    assert len(calls) == 1 and len(calls[0][0]) == 2
    assert calls[0][1]["mode"] == "live_public_preview"
    assert calls[0][1]["raw_preview"] == body.encode("utf-8")
    assert calls[0][1]["observed_at"] >= now


def test_observation_preserves_full_text_before_news_display_truncation(collector, monkeypatch):
    from core.news import alastar_observation
    collector.observation_archive_enabled = True
    collector.max_text_chars = 64
    original = "#BTC " + "word " * 25
    body = _message(original, date=datetime.now(timezone.utc).isoformat())
    calls = []
    monkeypatch.setattr(collector, "_request", lambda *args, **kwargs: _response(body))
    monkeypatch.setattr(alastar_observation, "observe_records", lambda records, **kwargs: calls.append(records))
    returned = collector.pull_latest()
    assert len(returned[0]["content"]) == 64
    assert calls[0][0]["content"] == original.strip()
    assert calls[0][0]["payload"]["text_truncated"] is False


def test_observation_failure_is_visible_and_does_not_drop_news(collector, monkeypatch, caplog):
    from core.news import alastar_observation
    collector.observation_archive_enabled = True
    body = _message(date=datetime.now(timezone.utc).isoformat())
    monkeypatch.setattr(collector, "_request", lambda *args, **kwargs: _response(body))
    def fail(*args, **kwargs):
        raise OSError("synthetic archive write failure")
    monkeypatch.setattr(alastar_observation, "observe_records", fail)
    returned = collector.pull_latest()
    assert len(returned) == 1
    assert returned[0]["payload"]["observation_archive_status"] == "failed"
    assert "research observation archive failed" in caplog.text


def test_unseen_ids_between_visible_posts_are_recorded_but_album_ids_are_not(collector):
    album = (
        '<div class="tgme_widget_message js-widget_message" data-post="AlastarTrades/17">'
        '<div class="tgme_widget_message_grouped_wrap">'
        '<a class="tgme_widget_message_photo_wrap"></a><a class="tgme_widget_message_photo_wrap"></a></div>'
        '<div class="tgme_widget_message_text js-message_text">#LAB album</div>'
        '<span class="tgme_widget_message_meta"><a class="tgme_widget_message_date">'
        '<time datetime="2026-10-13T06:00:00+00:00">06:00</time></a></span></div>'
    )
    page = album + _message("#LAB next", message_id=19) + _message("#LAB later", message_id=23)
    items = {item["payload"]["message_id"]: item for item in collector._from_html(page)}

    assert items[17]["payload"]["unseen_message_ids_before"] is None
    assert items[19]["payload"]["unseen_message_ids_before"] == []  # 17,18 = album
    assert items[23]["payload"]["unseen_message_ids_before"] == [20, 21, 22]


def test_large_id_gaps_keep_diagnostic_memory_bounded():
    import tracemalloc
    from core.news.collectors.common import BeautifulSoup
    from core.news.collectors.telegram import _unseen_ids_before

    # A deleted/private stretch must not allocate every absent ID just to keep
    # the first 100 diagnostic IDs. Construct widgets before measuring memory.
    soup = BeautifulSoup(_message(message_id=1) + _message(message_id=1_000_000), "html.parser")
    widgets = soup.select(".tgme_widget_message[data-post]")
    tracemalloc.start()
    try:
        gaps = _unseen_ids_before(widgets, "alastartrades")
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert gaps[1_000_000] == list(range(2, 102))
    assert peak < 1024 * 1024


def test_promotional_recap_is_flagged_without_dropping_symbol_tags(collector):
    recap = collector._from_html(_message("This week's top catches<br>#AIOT +463% #D +395%"))[0]
    view = collector._from_html(_message("#LAB whales are adding shorts."))[0]

    assert recap["payload"]["is_promotional_recap"] is True
    assert recap["symbols"] == ["AIOTUSDT", "DUSDT"]
    assert view["payload"]["is_promotional_recap"] is False


def test_picture_only_post_skips_news_but_reaches_the_archive_as_media_only(collector):
    body = _message("", photo=True, message_id=460) + _message(message_id=461)
    news = collector._from_html(body)
    assert [item["payload"]["message_id"] for item in news] == [461]
    archived = collector._from_html(body, preserve_full_text=True, include_media_only=True)
    media = next(item for item in archived if item["payload"]["message_id"] == 460)
    assert media["content"] == "" and media["payload"]["media_only"] is True
    assert media["payload"]["has_photo"] is True and media["payload"]["photo_count"] == 1
    assert next(i for i in archived if i["payload"]["message_id"] == 461)["payload"]["media_only"] is False
    # A caption-less post without any picture is still not a record at all.
    assert collector._from_html(_message("", message_id=462), include_media_only=True) == []


def test_one_failing_channel_does_not_drop_the_others(monkeypatch):
    collector = TelegramPublicChannelCollector({"defaults": {"telegram_alastar_channels": ["alastartrades", "otherchan"]}})
    now = datetime.now(timezone.utc).isoformat()

    def fake_request(endpoint, *args, **kwargs):
        if endpoint.endswith("/otherchan"):
            raise requests.ConnectionError("blocked")
        return _response(_message(date=now))

    monkeypatch.setattr(collector, "_request", fake_request)
    items = collector.pull_latest()
    assert [item["payload"]["channel"] for item in items] == ["alastartrades"]
    assert set(collector.last_channel_errors) == {"otherchan"}

    monkeypatch.setattr(collector, "_request", lambda *a, **k: (_ for _ in ()).throw(requests.ConnectionError("down")))
    with pytest.raises(RuntimeError, match="All Telegram channels failed"):
        collector.pull_latest()
    collector.close()


def test_each_configured_channel_is_archived_in_its_own_directory(monkeypatch, tmp_path):
    from core.news import alastar_observation

    collector = TelegramPublicChannelCollector({"defaults": {
        "telegram_alastar_channels": ["alastartrades", "otherchan"],
        "telegram_observation_archive_channels": ["alastartrades", "otherchan", "notcollected"],
        "telegram_alastar_observation_archive": True}})
    assert collector.archive_channels == ["alastartrades", "otherchan"]  # only channels actually pulled
    now = datetime.now(timezone.utc).isoformat()
    bodies = {"alastartrades": _message(date=now), "otherchan": _message(date=now, channel="OtherChan")}
    monkeypatch.setattr(collector, "_request", lambda endpoint, *a, **k: _response(
        bodies[endpoint.rsplit("/", 1)[1]], url=endpoint))
    calls = []
    monkeypatch.setattr(alastar_observation, "observe_records", lambda records, **kwargs: calls.append(kwargs))
    collector.pull_latest()
    assert [(c["channel"], c["output"]) for c in calls] == [
        ("alastartrades", alastar_observation.DEFAULT_OUTPUT),
        ("otherchan", alastar_observation.OTHER_CHANNELS_ROOT / "otherchan")]
    collector.close()
