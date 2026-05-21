"""Tests for scripts/backfill_close_reasons.py.

The script reads strategy_trade_journal.jsonl and risk_trade_history_*.json,
infers close_reason from available fields, and writes back. Tests verify:

- inference rules (priority order matches docstring)
- _is_close_row detection across action/signal_type variants
- dry-run does not modify files
- apply mode creates .bak when requested
- nested risk-history payload (`{"trade_history": [...]}`) is handled
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

# Import the script module by path.
import importlib.util
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts/backfill_close_reasons.py"
_spec = importlib.util.spec_from_file_location("backfill_close_reasons", SCRIPT_PATH)
backfill = importlib.util.module_from_spec(_spec)
# Register in sys.modules BEFORE exec so dataclass decorators inside the
# module can resolve their containing module via cls.__module__.
sys.modules["backfill_close_reasons"] = backfill
_spec.loader.exec_module(backfill)


# ───────────────────────────── _is_close_row ─────────────────────────────


class TestIsCloseRow:
    def test_action_close(self):
        assert backfill._is_close_row({"action": "close"}) is True

    def test_action_protective_close(self):
        assert backfill._is_close_row({"action": "protective_close"}) is True

    def test_action_open_or_add(self):
        assert backfill._is_close_row({"action": "open_or_add"}) is False

    def test_manual_order_with_close_long_signal(self):
        assert backfill._is_close_row({"action": "manual_order", "signal_type": "close_long"}) is True
        assert backfill._is_close_row({"action": "manual_order", "signal_type": "close_short"}) is True

    def test_manual_order_with_reduce_only_metadata(self):
        assert backfill._is_close_row(
            {"action": "manual_order", "metadata": {"reduce_only": True}}
        ) is True

    def test_manual_order_open(self):
        # Regular manual buy — not a close.
        assert backfill._is_close_row({"action": "manual_order", "signal_type": "buy"}) is False


# ───────────────────────────── infer_close_reason ─────────────────────────────


class TestInferCloseReason:
    def test_already_has_reason_returns_none(self):
        assert backfill.infer_close_reason({"close_reason": "rsi_long_exit", "action": "close"}) is None

    def test_nested_signal_metadata_reason_wins(self):
        row = {"action": "close", "signal": {"metadata": {"close_reason": "vwap_target_reached"}}}
        assert backfill.infer_close_reason(row) == "vwap_target_reached"

    def test_protective_close_with_trigger_reason(self):
        row = {
            "action": "protective_close",
            "signal": {"metadata": {"trigger_reason": "stop_loss"}},
        }
        assert backfill.infer_close_reason(row) == "stop_loss"

    def test_protective_close_generic_fallback(self):
        row = {"action": "protective_close"}
        assert backfill.infer_close_reason(row) == "protective_close_legacy"

    def test_close_order_mode_limit_first_with_signal(self):
        row = {"action": "close", "close_order_mode": "limit_first", "signal_type": "close_long"}
        assert backfill.infer_close_reason(row) == "signal_close_legacy"

    def test_signal_type_close_long(self):
        row = {"action": "close", "signal_type": "close_long"}
        assert backfill.infer_close_reason(row) == "signal_close_legacy"

    def test_manual_order_default(self):
        row = {"action": "manual_order"}
        assert backfill.infer_close_reason(row) == "manual_order"

    def test_unknown_terminal(self):
        row = {"action": "close"}  # nothing else to infer from
        assert backfill.infer_close_reason(row) == backfill.UNKNOWN_TERMINAL


# ───────────────────────────── file IO + dry-run vs apply ─────────────────────────────


class TestBackfillIO:
    def _write_jsonl(self, path: Path, rows):
        with path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")

    def test_jsonl_dry_run_does_not_modify(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        rows = [
            {"action": "open_or_add"},
            {"action": "close", "signal_type": "close_long"},
        ]
        self._write_jsonl(path, rows)
        original = path.read_text(encoding="utf-8")
        stats = backfill.backfill_jsonl(path, apply=False, backup=False)
        assert path.read_text(encoding="utf-8") == original
        assert stats.rewrote is False
        assert stats.total_close_rows == 1
        assert stats.inferred["signal_close_legacy"] == 1

    def test_jsonl_apply_writes_inferred_field(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        self._write_jsonl(
            path,
            [
                {"action": "close", "signal_type": "close_long"},
                {"action": "close", "close_reason": "already_set"},
            ],
        )
        stats = backfill.backfill_jsonl(path, apply=True, backup=False)
        assert stats.rewrote is True
        out_rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
        assert out_rows[0]["close_reason"] == "signal_close_legacy"
        assert out_rows[0]["close_reason_source"] == backfill.BACKFILL_MARKER_SOURCE
        # Pre-existing reason untouched.
        assert out_rows[1]["close_reason"] == "already_set"
        assert "close_reason_source" not in out_rows[1]

    def test_apply_creates_backup_when_requested(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        self._write_jsonl(path, [{"action": "close", "signal_type": "close_long"}])
        backfill.backfill_jsonl(path, apply=True, backup=True)
        assert path.with_suffix(".jsonl.bak").exists()

    def test_apply_skips_backup_when_disabled(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        self._write_jsonl(path, [{"action": "close", "signal_type": "close_long"}])
        backfill.backfill_jsonl(path, apply=True, backup=False)
        assert not path.with_suffix(".jsonl.bak").exists()

    def test_nested_risk_history_payload(self, tmp_path):
        path = tmp_path / "risk.json"
        payload = {
            "scope": "live",
            "trade_history": [
                {"action": "close", "signal_type": "close_long"},
                {"action": "open_or_add"},
            ],
        }
        path.write_text(json.dumps(payload), encoding="utf-8")
        stats = backfill.backfill_json_history(path, apply=True, backup=False)
        assert stats.rewrote is True
        loaded = json.loads(path.read_text(encoding="utf-8"))
        # Top-level scope preserved
        assert loaded["scope"] == "live"
        assert loaded["trade_history"][0]["close_reason"] == "signal_close_legacy"
        assert loaded["trade_history"][1].get("close_reason") is None  # open row untouched

    def test_idempotent_apply(self, tmp_path):
        path = tmp_path / "journal.jsonl"
        self._write_jsonl(path, [{"action": "close", "signal_type": "close_long"}])
        backfill.backfill_jsonl(path, apply=True, backup=False)
        first = path.read_text(encoding="utf-8")
        # Second run on already-tagged data: should not modify.
        stats = backfill.backfill_jsonl(path, apply=True, backup=False)
        assert stats.rewrote is False
        assert path.read_text(encoding="utf-8") == first


def test_unknown_terminal_marker_distinct_from_signal_close():
    """Confirm the two terminals don't collide — important for audit reporting."""
    assert backfill.UNKNOWN_TERMINAL != "signal_close_legacy"
    assert backfill.UNKNOWN_TERMINAL != "manual_order"
