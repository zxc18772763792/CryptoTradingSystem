from datetime import datetime, timezone

from core.structural.context import StructuralMarketContext
from core.structural.onchain_flow import evaluate_onchain_regime


def test_onchain_accumulation_regime_scales_longs_up_and_shorts_down():
    now = datetime(2026, 5, 20, tzinfo=timezone.utc)
    ctx = StructuralMarketContext.from_row(
        {
            "symbol": "ETH/USDT",
            "exchange_netflow_z": -3.0,
            "exchange_balance_change_z": -2.0,
            "stablecoin_balance_change_z": 3.0,
            "whale_outflow_score": 1.0,
            "onchain_as_of": now,
        },
        timestamp=now,
    )

    scalar = evaluate_onchain_regime(ctx, now=now)

    assert scalar.metadata["onchain_regime"] == "accumulation"
    assert scalar.long_scalar > 1.0
    assert scalar.short_scalar < 1.0
    assert "onchain_accumulation_regime" in scalar.reason_codes


def test_onchain_distribution_regime_reduces_longs():
    now = datetime(2026, 5, 20, tzinfo=timezone.utc)
    ctx = StructuralMarketContext.from_row(
        {
            "symbol": "ETH/USDT",
            "exchange_netflow_z": 3.0,
            "exchange_balance_change_z": 2.5,
            "stablecoin_balance_change_z": -3.0,
            "whale_inflow_score": 1.0,
            "onchain_as_of": now,
        },
        timestamp=now,
    )

    scalar = evaluate_onchain_regime(ctx, now=now)

    assert scalar.metadata["onchain_regime"] == "distribution"
    assert scalar.long_scalar < 1.0
    assert scalar.short_scalar > 1.0
    assert "onchain_distribution_regime" in scalar.reason_codes
