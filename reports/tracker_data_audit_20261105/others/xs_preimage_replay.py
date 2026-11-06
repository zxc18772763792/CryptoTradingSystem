"""Isolate the impact of the retained pre-timezone-repair spot files."""
import hashlib
import json
from pathlib import Path

source_path = Path("reports/tracker_data_audit_20261105/others/xs_venue_replay.py")
prefix = source_path.read_text(encoding="utf-8").split("\nresults = ", 1)[0]
exec(compile(prefix, str(source_path), "exec"))

first_date, last_date = registered.index.min(), registered.index.max()
rollback = json.loads((RESEARCH / "timezone_repair_20261020/rollback_runs.json").read_text(encoding="utf-8"))
preimages = {}
for run in rollback["runs"]:
    for f in run["files"]:
        path = Path(f["path"])
        if path.name == "1h.parquet" and path.parent.parent.name == "binance" and path.parent.name.endswith("_USDT"):
            base = path.parent.name.removesuffix("_USDT")
            if base in bases:
                old = Path(f["backup"])
                assert hashlib.sha256(old.read_bytes()).hexdigest() == f["pre_hash"], old
                preimages[base] = old
spot = panel_for("binance", include_parts=False)
old_series = {s: spot[s].dropna() for s in spot}
for base, path in preimages.items():
    close = pd.read_parquet(path, columns=["close"])["close"]
    close.index = pd.to_datetime(close.index, utc=True).tz_convert(None)
    old_series[base] = close[~close.index.duplicated(keep="last")].sort_index()
replays = {"spot_monolith": replay(spot).gross_pct, "spot_pre_timezone_monolith": replay(pd.DataFrame(old_series).sort_index()).gross_pct,
           "futures_monolith": replay(panel_for("binance_futures", include_parts=False)).gross_pct}
registered_gross = registered.gross_spread * 50
out = {"verified_preimages": len(preimages)}
for k, values in replays.items():
    out[k] = comparison(values, registered_gross)
    print(k, out[k], flush=True)
out["pre_vs_post_spot"] = comparison(replays["spot_pre_timezone_monolith"], replays["spot_monolith"])
pd.concat({"registered": registered_gross, **replays}, axis=1).to_csv(OUT / "xs_preimage_replay_daily.csv", index_label="date")
(OUT / "xs_preimage_replay.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
