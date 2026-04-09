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
