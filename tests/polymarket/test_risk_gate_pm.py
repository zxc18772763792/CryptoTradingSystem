from core.ai.risk_gate import RiskGate


def test_risk_gate_blocks_on_high_pm_global_risk():
    gate = RiskGate({"polymarket": {"enable": True, "global_risk_high": 0.5}})
    signal, reasons = gate.evaluate(
        symbol="BTCUSDT",
        proposed_signal="LONG",
        market_features={"pm_global_risk": 0.8, "spread": 0.001, "vol_1h": 0.02},
    )
    assert signal == "FLAT"
    assert any("pm_global_risk" in reason for reason in reasons)
