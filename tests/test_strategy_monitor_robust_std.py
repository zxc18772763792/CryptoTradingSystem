from core.monitoring.strategy_monitor import CUSUMMonitor, detect_strategy_decay


def test_cusum_robust_std_detects_fat_tail_decay():
    returns = [0.001, -0.001] * 10 + [0.25] + [-0.003] * 30

    result = detect_strategy_decay(returns, target_return=0.0, h=2.0, k=0.5, min_bars=20)

    assert result["triggered"] is True
    assert result["trigger_idx"] is not None
    assert result["std"] < 1.0
    assert result["decay_pct"] < 0


def test_stateful_cusum_monitor_uses_robust_scale_after_outlier():
    monitor = CUSUMMonitor(strategy_name="fat-tail", h=2.0, k=0.5, min_bars=20)

    statuses = [monitor.update(value) for value in ([0.001, -0.001] * 10 + [0.25] + [-0.003] * 30)]

    assert any(status["triggered"] for status in statuses)
    assert max(status["std_pct"] for status in statuses) < 1.0


def test_stateful_cusum_monitor_suppresses_immediate_retrigger_during_cooldown():
    monitor = CUSUMMonitor(
        strategy_name="cooldown",
        h=1.0,
        k=0.0,
        min_bars=2,
        cooldown_bars=3,
    )

    first = monitor.update(-0.001)
    second = monitor.update(-0.001)
    third = monitor.update(-0.001)

    assert first["triggered"] is False
    assert second["triggered"] is True
    assert second["cooldown_bars_remaining"] == 3
    assert third["triggered"] is False
    assert third["trigger_count"] == 1
    assert third["cooldown_bars_remaining"] == 2
