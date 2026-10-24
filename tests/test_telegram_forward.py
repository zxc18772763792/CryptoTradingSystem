from datetime import datetime, timedelta, timezone
import json

import pytest

from core.research.telegram_forward import Store, features, normalize, outcome, rule_kind


def message(source="cryptoquant_official", *, mode="forward", author="one", number=1,
            published="2026-10-24T12:30:00+00:00", received="2026-10-24T12:31:00+00:00", edited=False):
    return normalize({"source_id":f"telegram:{source}:{number}","channel":source,"message_id":number,
                      "mode":mode,"author_hash":author,"published_at":published,"received_at":received,
                      "first_received_at":received,"content":"BTC will rise", "edited":edited,
                      "forwarded":False,"sender_is_bot":False})


def label(row, *, side=1, at="2026-10-24T12:32:00+00:00"):
    return {"source_id":row["source_id"],"text_sha256":row["text_sha256"],"labelled_at":at,
            "kind":"directional","events":[{"symbol":"BTCUSDT","side":side,"confidence":.9,"quote":"BTC will rise"}]}


def test_backfilled_history_never_becomes_forward_signal():
    row=message(mode="historical")
    assert features([row],{row["source_id"]:label(row)},forward=True,
                    activation="2026-10-24T12:00:00+00:00",now="2026-10-24T12:40:00+00:00")==[]


def test_first_observed_revision_is_available_only_after_actual_label():
    row=message(edited=True)
    kwargs={"forward":True,"activation":"2026-10-24T12:00:00+00:00"}
    assert features([row],{row["source_id"]:label(row)},now="2026-10-24T12:31:59+00:00",**kwargs)==[]
    result=features([row],{row["source_id"]:label(row)},now="2026-10-24T12:40:00+00:00",**kwargs)
    assert result[0]["available_at"]=="2026-10-24T12:32:00+00:00"
    row["revision_observed"]=True
    assert features([row],{row["source_id"]:label(row)},now="2026-10-24T12:40:00+00:00",**kwargs)==[]


def test_stale_receipt_and_slow_classification_are_not_live_signals():
    row=message(received="2026-10-24T13:01:00+00:00")
    result=features([row],{row["source_id"]:label(row,at="2026-10-24T13:02:00+00:00")},forward=True,
                    activation="2026-10-24T12:00:00+00:00",now="2026-10-24T13:03:00+00:00")
    assert result==[]
    row=message()
    assert features([row],{row["source_id"]:label(row,at="2026-10-24T13:02:00+00:00")},forward=True,
                    activation="2026-10-24T12:00:00+00:00",now="2026-10-24T13:03:00+00:00")==[]


def test_chat_votes_are_author_balanced_and_wait_for_closed_hour():
    rows=[message("BinanceChinese",author="frequent",number=i) for i in range(100)]
    rows += [message("BinanceChinese",author=f"bear{i}",number=100+i) for i in range(4)]
    labels={r["source_id"]:label(r,side=1 if r["author_hash"]=="frequent" else -1) for r in rows}
    assert features(rows,labels,now="2026-10-24T12:59:59+00:00")==[]
    result=features(rows,labels,now="2026-10-24T13:00:00+00:00")
    assert len(result)==2
    assert all(r["directional_authors"]==5 and r["imbalance"]==pytest.approx(-.6) for r in result)
    assert {r["policy"]:r["side"] for r in result}=={"chat_follow":-1,"chat_contrarian":1}


def test_api_support_messages_cannot_generate_price_direction():
    row=message("Binance_api_Chinese")
    assert features([row],{row["source_id"]:label(row)},now="2026-10-24T13:00:00+00:00")==[]
    assert rule_kind(dict(row,content="BTC API 下单报错"))=="technical"
    assert rule_kind(dict(row,content="Bitcoin capital allocation is bullish"))=="needs_model"


def test_journal_prevents_overlap_and_detects_mutated_decision(tmp_path):
    store=Store(tmp_path)
    candidate={"policy":"chat_follow","source":"BinanceChinese","symbol":"BTCUSDT", "side":1,
               "holding_hours":6,"feature_id":"first","available_at":"2026-10-24T12:00:00+00:00"}
    assert store.freeze(candidate,"2026-10-24T12:00:00+00:00")
    assert not store.freeze(candidate,"2026-10-24T12:01:00+00:00")
    assert not store.freeze(dict(candidate,feature_id="second"),"2026-10-24T12:01:00+00:00")
    assert store.verify_journal()["records"]==1
    event_id,blob=store.db.execute("SELECT event_id,payload_json FROM decisions").fetchone()
    row=json.loads(blob)
    assert row["entry_at"]=="2026-10-24T12:05:00+00:00"
    row["side"]=-1
    store.db.execute("UPDATE decisions SET payload_json=? WHERE event_id=?",(json.dumps(row),event_id))
    store.db.commit()
    with pytest.raises(RuntimeError,match="committed journal"):
        store.verify_journal()


def test_new_revision_preserves_first_receipt_and_historical_mode(tmp_path):
    store=Store(tmp_path)
    original=message(mode="historical")
    store.ingest(original)
    revised=dict(original,content="BTC will fall",mode="forward",first_received_at="2026-10-25T12:00:00+00:00")
    store.ingest(revised)
    current=store.rows()[0]
    assert current["first_received_at"]==original["first_received_at"]
    assert current["mode"]=="historical" and current["revision_observed"]


def test_missing_funding_is_unavailable_and_mark_price_scales_cost():
    event={"entry_at":"2026-10-24T00:00:00+00:00","exit_at":"2026-10-24T06:00:00+00:00","side":1}
    bars=[{"at":event["entry_at"],"open":100},{"at":event["exit_at"],"open":110}]
    assert outcome(event,bars,[])["status"]=="pending_funding"
    rates=[{"at":f"2026-10-24T{hour:02d}:00:00+00:00","rate":.001,"mark":110} for hour in (0,4,8)]
    result=outcome(event,bars,rates)
    assert result["status"]=="settled"
    assert result["funding"]==pytest.approx(-.0011)
    assert result["net"]==pytest.approx(.1-.002-.0011)
    rates[1]["mark"]=None
    assert outcome(event,bars,rates)["status"]=="funding_mark_missing"


def test_nonfinite_prices_do_not_settle_and_personal_fields_are_redacted():
    event={"entry_at":"2026-10-24T00:00:00+00:00","exit_at":"2026-10-24T06:00:00+00:00","side":1}
    bars=[{"at":event["entry_at"],"open":float("nan")},{"at":event["exit_at"],"open":110}]
    assert outcome(event,bars,[])["status"]=="invalid_price"
    row=normalize(dict(message(),content="contact fake@example.test api_key=syntheticsecret"))
    assert "fake@example.test" not in row["content"] and "syntheticsecret" not in row["content"]


def test_six_hour_exploratory_feature_uses_distinct_policy_and_closed_window():
    rows=[message("BinanceChinese",author=f"a{i}",number=i,
                  published=f"2026-10-24T{12+i:02d}:10:00+00:00",
                  received=f"2026-10-24T{12+i:02d}:11:00+00:00") for i in range(5)]
    labels={r["source_id"]:label(r,at=f"2026-10-24T{12+i:02d}:12:00+00:00") for i,r in enumerate(rows)}
    assert features(rows,labels,chat_bucket_hours=6,now="2026-10-24T17:59:59+00:00")==[]
    result=features(rows,labels,chat_bucket_hours=6,now="2026-10-24T18:00:00+00:00")
    assert {r["policy"] for r in result}=={"chat_6h_follow","chat_6h_contrarian"}
    assert all(r["directional_authors"]==5 and r["bucket_hours"]==6 for r in result)


def test_text_retention_clears_body_revisions_and_quote(tmp_path):
    store=Store(tmp_path)
    row=message()
    store.ingest(row)
    store.save_label(row,label(row))
    assert store.expire_text("2026-11-25T12:00:00+00:00")==1
    assert store.rows()[0]["content"]==""
    assert store.db.execute("SELECT count(*) FROM revisions").fetchone()[0]==0
    assert "quote" not in store.labels()[row["source_id"]]["events"][0]


def test_feature_observation_is_immutable_and_zero_views_is_not_neutral(tmp_path):
    store=Store(tmp_path)
    row={"feature_id":"one","imbalance":None,"directional_authors":0,"qualified":False,"reason":"no_directional_views"}
    assert store.observe(row)
    assert not store.observe(dict(row,imbalance=.5))
    assert store.verify_journal()["records"]==1
    saved=json.loads(store.db.execute("SELECT payload_json FROM observations").fetchone()[0])
    assert saved["imbalance"] is None
