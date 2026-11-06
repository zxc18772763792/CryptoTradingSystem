"""Replay the archived CoinGlass development benchmarks without fetching or rewriting research data."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd()))
from scripts import coinglass_combination_study as combo, coinglass_wide_study as wide

single = combo.single
frames = single.build_panel(single.load_cached())
panel = combo.add_features(single.build_signals(frames), frames)
test = panel["fwd_1d"].index >= combo.SPLIT
perps = list(panel["fwd_1d"].columns)
cache = json.loads(combo.FUNDING_PATH.read_text(encoding="utf-8"))
score = combo.composite(panel, combo.training_weights(panel, 7))[test]
funding = combo.daily_funding(cache, panel["fwd_1d"].index, perps)[test]
baseline = {}
for cost in (combo.COST, 0.002):
    res = combo.funded_books(score, panel["fwd_7d"][test], panel["fwd_1d"][test], funding, 7, cost)
    baseline[str(cost)] = {"net_pct_per_book": res["plain_net"] * 700,
                           "positive_quarters": sum(v > 0 for v in res["plain_by_quarter"].values())}
wide_frames = wide.wide_frames()
wide_panel = wide.wide_panel(wide_frames)
wide_index = wide_panel["fwd_1d"].index
wide_result = wide.test_set(wide_panel, list(wide_panel["fwd_1d"].columns), wide.ct.SHADOW_WEIGHTS,
                            wide_index >= single.SPLIT, wide.funding_cache(), reps=0)
out = {"main": baseline, "wide_equal": {"coins": wide_result["coins"], "net_pct_per_book": wide_result["net_per_book"] * 100}}
print(out)
(Path("reports/tracker_data_audit_20261105") / "coinglass_backtest_replay.json").write_text(json.dumps(out, indent=2, allow_nan=False), encoding="utf-8")
