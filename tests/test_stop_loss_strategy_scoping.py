from core.risk.stop_loss import StopLossConfig, StopLossManager, StopLossType, TakeProfitManager
from core.trading.position_manager import PositionSide


def test_stop_loss_info_is_scoped_by_strategy():
    manager = StopLossManager()

    manager.set_stop_loss(
        "binance",
        "BTC/USDT",
        StopLossConfig(type=StopLossType.FIXED, value=5.0),
        strategy="alpha_strategy",
    )
    manager.set_stop_loss(
        "binance",
        "BTC/USDT",
        StopLossConfig(type=StopLossType.FIXED, value=10.0),
        strategy="beta_strategy",
    )

    alpha_info = manager.get_stop_loss_info("binance", "BTC/USDT", strategy="alpha_strategy")
    beta_info = manager.get_stop_loss_info("binance", "BTC/USDT", strategy="beta_strategy")

    assert alpha_info is not None
    assert alpha_info["value"] == 5.0
    assert beta_info is not None
    assert beta_info["value"] == 10.0
    assert manager.get_stop_loss_info("binance", "BTC/USDT") is None


def test_take_profit_targets_are_scoped_by_strategy():
    manager = TakeProfitManager()

    manager.set_take_profit(
        "binance",
        "BTC/USDT",
        [{"price": 105.0, "quantity_pct": 0.5}],
        strategy="alpha_strategy",
    )
    manager.set_take_profit(
        "binance",
        "BTC/USDT",
        [{"price": 110.0, "quantity_pct": 1.0}],
        strategy="beta_strategy",
    )

    alpha_target = manager.check_take_profit(
        "binance",
        "BTC/USDT",
        current_price=106.0,
        position_side=PositionSide.LONG,
        strategy="alpha_strategy",
    )
    beta_before = manager.check_take_profit(
        "binance",
        "BTC/USDT",
        current_price=106.0,
        position_side=PositionSide.LONG,
        strategy="beta_strategy",
    )
    beta_target = manager.check_take_profit(
        "binance",
        "BTC/USDT",
        current_price=111.0,
        position_side=PositionSide.LONG,
        strategy="beta_strategy",
    )

    assert alpha_target is not None
    assert alpha_target["price"] == 105.0
    assert beta_before is None
    assert beta_target is not None
    assert beta_target["price"] == 110.0
