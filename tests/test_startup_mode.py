from web.startup_mode import resolve_startup_trading_mode


def test_persisted_live_restore_falls_back_to_paper_by_default():
    decision = resolve_startup_trading_mode(
        configured_mode="paper",
        persisted_account={"mode": "live"},
        allow_persisted_live_mode_start=False,
    )

    assert decision.configured_mode == "paper"
    assert decision.persisted_mode == "live"
    assert decision.effective_mode == "paper"
    assert decision.source == "guarded_configured"
    assert decision.blocked_persisted_live_restore is True


def test_persisted_live_restore_can_be_explicitly_enabled():
    decision = resolve_startup_trading_mode(
        configured_mode="paper",
        persisted_account={"mode": "live"},
        allow_persisted_live_mode_start=True,
    )

    assert decision.effective_mode == "live"
    assert decision.source == "persisted"
    assert decision.blocked_persisted_live_restore is False


def test_explicit_live_configuration_wins_over_persisted_paper():
    decision = resolve_startup_trading_mode(
        configured_mode="live",
        persisted_account={"mode": "paper"},
        allow_persisted_live_mode_start=False,
    )

    assert decision.effective_mode == "live"
    assert decision.source == "configured"
    assert decision.blocked_persisted_live_restore is False
