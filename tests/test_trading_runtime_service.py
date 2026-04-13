import asyncio

import pytest
from fastapi import HTTPException

from web.services import trading_runtime_service


def test_list_pending_mode_switches_hides_tokens_by_default():
    trading_runtime_service._mode_switch_pending.clear()
    try:
        result = trading_runtime_service.request_mode_switch(
            target_mode="live",
            current_mode="paper",
            reason="verify",
        )

        pending = trading_runtime_service.list_pending_mode_switches()

        assert result["token"]
        assert pending
        assert pending[0]["target_mode"] == "live"
        assert "token" not in pending[0]
    finally:
        trading_runtime_service._mode_switch_pending.clear()


def test_list_pending_mode_switches_can_include_tokens_when_requested():
    trading_runtime_service._mode_switch_pending.clear()
    try:
        result = trading_runtime_service.request_mode_switch(
            target_mode="live",
            current_mode="paper",
            reason="verify",
        )

        pending = trading_runtime_service.list_pending_mode_switches(include_token=True)

        assert pending
        assert pending[0]["token"] == result["token"]
    finally:
        trading_runtime_service._mode_switch_pending.clear()


def test_request_mode_switch_uses_clear_text_for_already_target_mode():
    result = trading_runtime_service.request_mode_switch(
        target_mode="paper",
        current_mode="paper",
        reason="verify",
    )

    assert result["message"] == "Already in target mode."


def test_request_mode_switch_uses_clear_warning_text():
    result = trading_runtime_service.request_mode_switch(
        target_mode="live",
        current_mode="paper",
        reason="verify",
    )

    assert result["warning"] == "Switching to live trading is high risk. Verify API permissions and risk settings."
    assert result["confirm_text"] == trading_runtime_service.get_mode_confirm_text()


def test_ensure_trading_mode_started_defaults_risk_scope_to_mode(monkeypatch):
    monkeypatch.setattr(trading_runtime_service.risk_manager, "get_risk_report", lambda: {"equity": {}, "limits": {}, "alerts": []})
    monkeypatch.setattr(trading_runtime_service.execution_engine, "_running", False, raising=False)
    monkeypatch.setattr(trading_runtime_service.execution_engine, "set_paper_trading", lambda *args, **kwargs: None)
    monkeypatch.setattr(trading_runtime_service.account_manager, "set_mode_for_all", lambda *args, **kwargs: 0)
    monkeypatch.setattr(trading_runtime_service.runtime_state, "clear_registered_caches", lambda *args, **kwargs: {})

    async def _start():
        trading_runtime_service.execution_engine._running = True

    monkeypatch.setattr(trading_runtime_service.execution_engine, "start", _start)
    monkeypatch.setattr(trading_runtime_service.execution_engine, "get_trading_mode", lambda: "paper")
    monkeypatch.setattr(trading_runtime_service.execution_engine, "get_queue_size", lambda: 0)

    result = asyncio.run(trading_runtime_service.ensure_trading_mode_started("paper"))

    assert result["running"] is True
    assert result["mode"] == "paper"
    assert result["risk_scope"] == "paper"


def test_switch_trading_mode_rejects_missing_token_with_clear_text():
    trading_runtime_service._mode_switch_pending.clear()

    async def _call():
        await trading_runtime_service.switch_trading_mode(
            token="missing-token",
            confirm_text=trading_runtime_service.get_mode_confirm_text(),
            app=None,
            reason="verify",
        )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(_call())

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Mode switch token not found"


def test_switch_trading_mode_rejects_bad_confirm_text_with_clear_text():
    trading_runtime_service._mode_switch_pending.clear()
    pending = trading_runtime_service.request_mode_switch(
        target_mode="live",
        current_mode="paper",
        reason="verify",
    )

    async def _call():
        await trading_runtime_service.switch_trading_mode(
            token=pending["token"],
            confirm_text="wrong",
            app=None,
            reason="verify",
        )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(_call())

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Confirmation text mismatch"
