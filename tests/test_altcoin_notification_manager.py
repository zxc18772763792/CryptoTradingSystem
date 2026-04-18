from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from core.notifications.notification_manager import AlertRule, NotificationManager


def _altcoin_context():
    return {
        "altcoin": {
            "scans": {
                "cfg-1": {
                    "rows": [
                        {
                            "symbol": "AAA/USDT",
                            "layout_score": 0.81,
                            "alert_score": 0.66,
                            "anomaly_score": 0.61,
                            "accumulation_score": 0.78,
                            "control_score": 0.49,
                            "signal_state": "布局吸筹",
                            "rank": 2,
                        },
                        {
                            "symbol": "BBB/USDT",
                            "layout_score": 0.64,
                            "alert_score": 0.88,
                            "anomaly_score": 0.91,
                            "accumulation_score": 0.42,
                            "control_score": 0.63,
                            "signal_state": "异动启动",
                            "rank": 1,
                        },
                    ],
                    "sort_indexes": {
                        "layout": {"AAA/USDT": 2, "BBB/USDT": 5},
                        "alert": {"AAA/USDT": 3, "BBB/USDT": 1},
                        "control": {"AAA/USDT": 4, "BBB/USDT": 2},
                    },
                }
            }
        }
    }


def _rule(rule_type: str, params: dict) -> AlertRule:
    now = datetime.now(timezone.utc)
    return AlertRule(
        id="rule-1",
        name="demo",
        rule_type=rule_type,
        params=params,
        created_at=now,
        updated_at=now,
    )


def test_eval_altcoin_score_above_uses_prefetched_scan_context():
    manager = NotificationManager()
    rule = _rule(
        "altcoin_score_above",
        {
            "config_key": "cfg-1",
            "symbol": "BBB/USDT",
            "score_key": "anomaly",
            "threshold": 0.72,
        },
    )

    reason = manager._eval_rule(rule, _altcoin_context())

    assert reason is not None
    assert "BBB/USDT" in reason
    assert "anomaly" in reason
    assert "0.9100" in reason


def test_eval_altcoin_rank_top_n_uses_sort_index_snapshot():
    manager = NotificationManager()
    rule = _rule(
        "altcoin_rank_top_n",
        {
            "config_key": "cfg-1",
            "symbol": "BBB/USDT",
            "sort_by": "alert",
            "rank_n": 3,
        },
    )

    reason = manager._eval_rule(rule, _altcoin_context())

    assert reason is not None
    assert "BBB/USDT" in reason
    assert "前 3" in reason
    assert "当前排名 1" in reason


def test_cooldown_ok_handles_naive_last_triggered_datetime():
    manager = NotificationManager()
    rule = _rule(
        "altcoin_score_above",
        {"config_key": "cfg-1", "symbol": "AAA/USDT", "threshold": 0.5},
    )
    rule.cooldown_seconds = 300
    rule.last_triggered_at = datetime.utcnow()

    assert manager._cooldown_ok(rule) is False
    assert rule.last_triggered_at is not None
    assert rule.last_triggered_at.tzinfo is not None
    assert rule.to_dict()["last_triggered_at"].endswith("+00:00")


def test_eval_altcoin_score_above_skips_benchmark_excluded_row():
    manager = NotificationManager()
    rule = _rule(
        "altcoin_score_above",
        {
            "config_key": "cfg-1",
            "symbol": "BTC/USDT",
            "score_key": "anomaly",
            "threshold": 0.72,
        },
    )
    context = {
        "altcoin": {
            "scans": {
                "cfg-1": {
                    "rows": [
                        {
                            "symbol": "BTC/USDT",
                            "alt_eligible": False,
                            "anomaly_score": 0.96,
                            "signal_state": "",
                        }
                    ]
                }
            }
        }
    }

    reason = manager._eval_rule(rule, context)

    assert reason is None


def test_evaluate_rules_only_triggers_altcoin_alert_when_condition_crosses_up(monkeypatch):
    manager = NotificationManager()
    manager._loaded = True
    rule = _rule(
        "altcoin_score_above",
        {
            "config_key": "cfg-1",
            "symbol": "AVAX/USDT",
            "score_key": "anomaly",
            "threshold": 0.72,
            "channels": ["feishu"],
        },
    )
    manager._rules = {rule.id: rule}

    async def fake_upsert(_rule):
        return None

    sent_messages = []

    async def fake_send_message(title, message, channels):
        sent_messages.append((title, message, tuple(channels)))
        return {"feishu": True}

    monkeypatch.setattr(manager, "_upsert_rule", fake_upsert)
    monkeypatch.setattr(manager, "send_message", fake_send_message)

    warm_context = {
        "altcoin": {
            "scans": {
                "cfg-1": {
                    "rows": [
                        {
                            "symbol": "AVAX/USDT",
                            "alt_eligible": True,
                            "anomaly_score": 0.81,
                            "signal_state": "异动启动",
                            "rank": 1,
                        }
                    ]
                }
            }
        }
    }
    cool_context = {
        "altcoin": {
            "scans": {
                "cfg-1": {
                    "rows": [
                        {
                            "symbol": "AVAX/USDT",
                            "alt_eligible": True,
                            "anomaly_score": 0.42,
                            "signal_state": "",
                            "rank": 3,
                        }
                    ]
                }
            }
        }
    }

    first = asyncio.run(manager.evaluate_rules(warm_context))
    second = asyncio.run(manager.evaluate_rules(warm_context))
    cooled = asyncio.run(manager.evaluate_rules(cool_context))
    third = asyncio.run(manager.evaluate_rules(warm_context))

    assert first["triggered_count"] == 0
    assert second["triggered_count"] == 0
    assert cooled["triggered_count"] == 0
    assert third["triggered_count"] == 1
    assert len(sent_messages) == 1
    assert "AVAX/USDT" in third["triggered"][0]["reason"]
