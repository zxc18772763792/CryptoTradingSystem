import json
from datetime import datetime, timezone

from scripts import audit_exit_reasons as audit
from scripts import audit_close_reason_coverage as coverage
from scripts import verify_post_change_live_exits as live_gate


def test_audit_live_counts_reasons_and_reason_gaps(tmp_path, monkeypatch):
    live_journal = tmp_path / "strategy_trade_journal.jsonl"
    risk_history = tmp_path / "risk_trade_history_live.json"
    rows = [
        {
            "timestamp": "2026-05-20T00:00:00+00:00",
            "action": "open_or_add",
            "signal": {"metadata": {"exit_template": "SignalPlusTimeStop"}},
        },
        {
            "timestamp": "2026-05-20T00:01:00+00:00",
            "action": "open_or_add",
            "signal": {"metadata": {}},
        },
        {
            "timestamp": "2026-05-20T00:02:00+00:00",
            "action": "close",
            "signal_type": "close_long",
            "pnl": 1.0,
            "gross_pnl_usd": 1.0,
            "net_pnl_usd": 0.8,
            "close_order_mode": "limit_first",
            "signal": {"metadata": {"close_reason": "take_profit"}},
        },
        {
            "timestamp": "2026-05-20T00:03:00+00:00",
            "action": "close",
            "signal_type": "close_long",
            "pnl": -0.5,
            "gross_pnl_usd": 0.2,
            "net_pnl_usd": -0.5,
            "slippage_bps": 12.5,
            "signal": {"metadata": {}},
        },
    ]
    live_journal.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    risk_history.write_text(
        json.dumps(
            {
                "trade_history": [
                    {"timestamp": "2026-05-20T00:04:00+00:00", "action": "manual_order"},
                    {
                        "timestamp": "2026-05-20T00:05:00+00:00",
                        "action": "manual_order",
                        "close_reason": "stop_loss",
                        "close_order_mode": "market_fallback",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(audit, "LIVE_JOURNAL_PATH", live_journal)
    monkeypatch.setattr(audit, "RISK_HISTORY_PATHS", (risk_history,))

    summary = audit.audit_live(days=0, now=datetime(2026, 5, 20, tzinfo=timezone.utc))
    report = audit.render_markdown(summary)

    assert summary.opens == 2
    assert summary.closes == 2
    assert summary.losing_closes == 1
    assert summary.open_exit_templates["SignalPlusTimeStop"] == 1
    assert summary.open_exit_templates["NONE"] == 1
    assert summary.exit_reasons["take_profit"] == 1
    assert summary.exit_reasons["signal_close"] == 1
    assert summary.close_order_modes["limit_first"] == 1
    assert summary.close_order_modes["unknown"] == 1
    assert summary.risk_close_order_modes["market_fallback"] == 1
    assert summary.missing_close_reason == 1
    assert summary.risk_missing_close_reason == 1
    assert len(summary.gross_win_net_loss) == 1
    assert "`take_profit`" in report
    assert "Close Order Modes" in report


def test_close_reason_coverage_wrapper_reports_gates(tmp_path, monkeypatch):
    live_journal = tmp_path / "strategy_trade_journal.jsonl"
    risk_history = tmp_path / "risk_trade_history_live.json"
    rows = [
        {
            "timestamp": "2026-05-20T00:02:00+00:00",
            "action": "close",
            "strategy": "VWAPReversionStrategy",
            "signal_type": "close_long",
            "close_reason": "vwap_mean_reversion_completed",
            "close_order_mode": "limit_first",
        },
        {
            "timestamp": "2026-05-20T00:03:00+00:00",
            "action": "close",
            "strategy": "BollingerBandsStrategy",
            "signal_type": "close_long",
            "close_reason": "bollinger_middle_reversion",
            "close_order_mode": "market_fallback",
        },
    ]
    live_journal.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    risk_history.write_text(
        json.dumps(
            {
                "trade_history": [
                    {
                        "timestamp": "2026-05-20T00:05:00+00:00",
                        "action": "manual_order",
                        "side": "long",
                        "entry_price": 100.0,
                        "take_profit": 104.0,
                        "close_reason": "take_profit",
                        "close_order_mode": "limit_first",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(audit, "LIVE_JOURNAL_PATH", live_journal)
    monkeypatch.setattr(audit, "RISK_HISTORY_PATHS", (risk_history,))

    summary = coverage.audit_close_reason_coverage(
        days=0,
        now=datetime(2026, 5, 20, tzinfo=timezone.utc),
    )
    report = coverage.render_markdown(summary)

    assert summary.journal_coverage == 1.0
    assert summary.risk_coverage == 1.0
    assert summary.vwap_reason_rows == 1
    assert summary.vwap_sell_misclassified_rows == 0
    assert summary.long_invalid_take_profit_rows == 0
    assert "Journal close_reason coverage" in report
    assert "PASS" in report


def test_post_change_live_gate_passes_when_samples_satisfy_targets(tmp_path, monkeypatch):
    live_journal = tmp_path / "strategy_trade_journal.jsonl"
    risk_history = tmp_path / "risk_trade_history_live.json"
    rows = [
        {
            "timestamp": "2026-05-20T23:59:00+00:00",
            "action": "close",
            "strategy": "BollingerBandsStrategy",
            "signal_type": "close_long",
            "close_reason": "bollinger_middle_reversion",
            "close_order_mode": "market",
            "net_pnl_usd": -99.0,
            "slippage_bps": 20.0,
        },
        {
            "timestamp": "2026-05-21T00:01:00+00:00",
            "action": "close",
            "strategy": "BollingerBandsStrategy",
            "signal_type": "close_long",
            "close_reason": "bollinger_middle_reversion",
            "close_order_mode": "limit_first",
            "net_pnl_usd": 1.0,
            "slippage_bps": 1.0,
        },
        {
            "timestamp": "2026-05-21T00:02:00+00:00",
            "action": "close",
            "strategy": "BollingerBandsStrategy",
            "signal_type": "close_long",
            "close_reason": "bollinger_middle_reversion",
            "close_order_mode": "limit_first",
            "net_pnl_usd": -1.0,
            "slippage_bps": 2.0,
        },
        {
            "timestamp": "2026-05-21T00:03:00+00:00",
            "action": "close",
            "strategy": "VWAPReversionStrategy",
            "signal_type": "close_long",
            "close_reason": "vwap_mean_reversion_completed",
            "close_order_mode": "limit_first",
            "net_pnl_usd": 0.5,
            "slippage_bps": 1.5,
        },
    ]
    live_journal.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    risk_history.write_text(json.dumps({"trade_history": []}), encoding="utf-8")
    monkeypatch.setattr(audit, "LIVE_JOURNAL_PATH", live_journal)
    monkeypatch.setattr(audit, "RISK_HISTORY_PATHS", (risk_history,))

    async def _fake_bollinger_win_rate(days: int):
        return 0.55

    monkeypatch.setattr(live_gate, "_bollinger_backtest_win_rate", _fake_bollinger_win_rate)

    summary = live_gate.build_summary(
        days=0,
        since=datetime(2026, 5, 21, tzinfo=timezone.utc),
        min_samples=2,
        max_win_rate_delta=0.10,
    )
    report = live_gate.render_markdown(summary)

    statuses = {row.gate: row.status for row in summary.rows}
    assert summary.journal_closes == 3
    assert statuses["Journal close_reason coverage"] == "PASS"
    assert statuses["VWAP journal close semantics"] == "PASS"
    assert statuses["Active strategy close share"] == "PASS"
    assert statuses["LIMIT close order share"] == "PASS"
    assert statuses["Average close slippage"] == "PASS"
    assert statuses["Bollinger live/backtest win-rate delta"] == "PASS"
    assert "Post-Change Live Exit Gate" in report


def test_post_change_live_gate_marks_sample_dependent_checks_pending(tmp_path, monkeypatch):
    live_journal = tmp_path / "strategy_trade_journal.jsonl"
    risk_history = tmp_path / "risk_trade_history_live.json"
    live_journal.write_text("", encoding="utf-8")
    risk_history.write_text(json.dumps({"trade_history": []}), encoding="utf-8")
    monkeypatch.setattr(audit, "LIVE_JOURNAL_PATH", live_journal)
    monkeypatch.setattr(audit, "RISK_HISTORY_PATHS", (risk_history,))

    async def _fake_bollinger_win_rate(days: int):
        return 0.55

    monkeypatch.setattr(live_gate, "_bollinger_backtest_win_rate", _fake_bollinger_win_rate)

    summary = live_gate.build_summary(days=0, min_samples=2)

    assert summary.journal_closes == 0
    assert {row.status for row in summary.rows} == {"PENDING"}
