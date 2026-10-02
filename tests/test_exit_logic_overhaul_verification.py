from scripts.verify_exit_logic_overhaul import build_report


def test_exit_logic_overhaul_verification_report_passes_core_offline_checks(monkeypatch):
    import pandas as pd
    monkeypatch.setattr("scripts.verify_exit_logic_overhaul._load_local_btc_1h", lambda days: pd.DataFrame())
    report = build_report()

    assert "Trade count delta" in report
    assert "Acceptance `>=30%`: `PASS`" in report
    assert "Required reasons present: `PASS`" in report
    assert "Acceptance 1x/1.5x/2x ATR defaults: `PASS`" in report
    assert "Acceptance active close share `>=20%`: `UNVERIFIED_NO_LOCAL_DATA`" in report
    assert "Acceptance stop_loss share `<=30%`: `UNVERIFIED_NO_LOCAL_DATA`" in report
    assert "Acceptance SL distance about `1.5x ATR`: `PASS`" in report
    assert "Phase 5 Live Maker/Fallback Audit" in report
