"""Independent standard-library verification of one official pre-entry audit.

Does not import the production null/price/journal functions or call any API.
"""
from datetime import datetime, timedelta, timezone
import csv
import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/research/alastar_paper_v2"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    audit_raw = (OUT / "official_null_audit.json").read_bytes()
    audit = json.loads(audit_raw)
    entry = datetime.fromisoformat(audit["entry_at"])
    assert entry.utcoffset() == timedelta(0)
    end = int(entry.timestamp()*1000)
    start = end-30*24*60*60*1000
    direction = audit["direction"]
    assert type(direction) is int and direction in (-1, 1)
    bars, hashes = {}, []
    for ref in audit["evidence"]:
        path = (OUT/ref["file"]).resolve()
        assert path.is_relative_to(OUT.resolve())
        raw = path.read_bytes()
        assert sha(raw) == ref["sha256"]
        part = json.loads(raw)
        p = part["params"]
        assert part["endpoint"] == "https://fapi.binance.com/fapi/v1/klines"
        assert p["symbol"] == audit["symbol"] and p["interval"] == "5m"
        assert start <= p["startTime"] <= p["endTime"] <= end
        assert len(part["rows"]) <= p["limit"] <= 1500
        stamps = [int(r[0]) for r in part["rows"]]
        assert stamps == sorted(stamps) and len(set(stamps)) == len(stamps)
        for row in part["rows"]:
            t = int(row[0])
            assert p["startTime"] <= t <= p["endTime"] and t % 300000 == 0
            values = tuple(map(float, row[1:5]))
            assert all(math.isfinite(v) and v > 0 for v in values)
            o,h,lo,c = values
            assert lo <= min(o,c) <= max(o,c) <= h
            if len(row) >= 9:
                assert float(row[5]) >= 0 and float(row[8]) >= 0
                if float(row[5]) == 0 or float(row[8]) == 0: continue
            assert t not in bars or bars[t] == values
            bars[t] = values
        hashes.append(ref["sha256"])
    values = []
    for t in range(start, end-3600000+1, 300000):
        if t not in bars: continue
        price = bars[t][0]
        valid, liquidated = True, False
        for j in range(12):
            stamp = t+j*300000
            if stamp not in bars:
                valid = False; break
            _,h,lo,_ = bars[stamp]
            if (h >= 1.99*price if direction == -1 else lo <= .005*price):
                liquidated = True; break
        if not valid: continue
        if liquidated: values.append(-1.)
        elif t+3600000 in bars:
            values.append(max(-1., direction*(bars[t+3600000][0]/price-1)-.002))
    mean = math.fsum(values)/len(values)
    difference = mean-audit["null"]["mean_net_price_only"]
    assert len(values) == audit["null"]["n"] >= 2000 and abs(difference) < 1e-12

    # A separately downloaded first chronological fresh-call anchor should
    # reproduce the round4 d1 entry/exit before checking any strategy claim.
    assert audit["symbol"] == "PIPPINUSDT" and audit["entry_at"] == "2026-03-13T06:45:00+00:00"
    original_bars = {}
    for path in (ROOT/"data/research/alastar_response_20261021/official_5m").glob("PIPPINUSDT_*.json"):
        part = json.loads(path.read_bytes())
        assert part["status"] == "ok"
        for r in part["rows"]:
            original_bars[int(r[0])] = tuple(map(float,r[1:5]))
    assert original_bars[end][0] == bars[end][0]
    real = max(-1., direction*(original_bars[end+3600000][0]/original_bars[end][0]-1)-.002)
    with (ROOT/"data/research/alastar_selection_timing_20261021/delay_decay_5m.csv").open(encoding="utf-8-sig") as stream:
        anchor = next(r for r in csv.DictReader(stream) if r["signal_id"] == "telegram:alastartrades:4:e1")
    assert abs(real-float(anchor["d1_60m_net"])) < 1e-12
    result = {"ok": True, "verified_at_utc": datetime.now(timezone.utc).isoformat(),
              "audit_sha256": sha(audit_raw), "source_hashes": hashes, "official_rows": len(bars),
              "valid_null_starts": len(values), "independent_mean_net_price_only": mean,
              "difference_from_production": difference, "first_chronological_anchor_matches_round4_d1": True,
              "historical_anchor_net": real, "historical_anchor_excess_price_only": real-mean,
              "clock_check_passed": audit["clock_check"]["ok"],
              "scope": "one implementation audit, not43new strategy trials or forward validation"}
    target = OUT/"official_null_validation.json"
    if target.exists():
        old = target.read_bytes()
        (OUT/"evidence"/f"{sha(old)}.json").write_bytes(old)
    target.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(result,ensure_ascii=False))


if __name__ == "__main__":
    main()
