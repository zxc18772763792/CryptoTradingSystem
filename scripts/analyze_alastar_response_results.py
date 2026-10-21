"""Concentration, matched-event and timing diagnostics; no parameter selection."""
from collections import defaultdict
import hashlib
import json

import numpy as np
import pandas as pd

from scripts.research_alastar_responses import OUT, ROOT, HOLDS, DELAYS, Market, evaluate, utc, write


def main():
    result = json.loads((OUT/"response_results.json").read_text(encoding="utf-8"))
    outcomes = [json.loads(l) for l in (OUT/"response_outcomes.jsonl").read_text(encoding="utf-8").splitlines()]
    admitted = [json.loads(l) for l in (OUT/"admitted_trades.jsonl").read_text(encoding="utf-8").splitlines()]
    groups = defaultdict(list)
    for r in admitted: groups[(r["scenario"],r["analysis_phase"])].append(r)
    concentration = []
    for (scenario,phase),rows in groups.items():
        coin = defaultdict(float)
        for r in rows: coin[r["symbol"]] += r["net_return"]
        best_coin = max(coin,key=coin.get)
        rest = [r["net_return"] for r in rows if r["symbol"] != best_coin]
        concentration.append({"scenario":scenario,"phase":phase,"n":len(rows),"best_coin":best_coin,
                              "mean_without_best_coin":float(np.mean(rest)) if rest else None,
                              "best_coin_net_sum":coin[best_coin],
                              "best_coin_share_of_total_net":coin[best_coin]/sum(coin.values()) if sum(coin.values())>0 else None})
    # Same event IDs across horizons, independent of book overlap filtering. This
    # isolates response-cycle effects; it is not an investable portfolio.
    common = []
    for family in result["pools"]:
        for phase in ("all","earlier","later"):
            parts={h:{r["event_id"]:r for r in outcomes if r["scenario"]==f"{family}|d15|h{h}"
                      and r["status"]=="ok" and (phase=="all" or r["phase"]==phase)} for h in HOLDS}
            ids=set.intersection(*(set(x) for x in parts.values()))
            for h,part in parts.items():
                values=[part[k]["net_return"] for k in sorted(ids)]
                common.append({"family":family,"phase":phase,"hours":h,"n":len(ids),
                               "mean":float(np.mean(values)) if values else None,
                               "event_ids":sorted(ids)})
    events=json.loads((OUT/"resolved_response_events.json").read_text(encoding="utf-8"))
    taxonomy=json.loads((OUT/"taxonomy.json").read_text(encoding="utf-8"))
    unique={e["source_id"]:e for e in taxonomy["records"]}
    timing=[]
    for delay in DELAYS:
        for resolution in ("5min","h"):
            lags=[((utc(e["published_at"])+pd.Timedelta(minutes=delay)).ceil(resolution)-utc(e["published_at"])).total_seconds()/60 for e in unique.values()]
            timing.append({"delay_minutes":delay,"price_resolution":resolution,"n":len(lags),
                           "median_actual_wait_minutes":float(np.median(lags)),
                           "min_actual_wait_minutes":min(lags),"max_actual_wait_minutes":max(lags)})
    # Old40 labels under the SAME new5m/funding/collateral model. Only the entry
    # rounding resolution changes, so this does not confound text-pool expansion.
    legacy=json.loads((ROOT/"data/research/alastar_followup_20261021/expanded_frozen_signals.json").read_text(encoding="utf-8"))
    market=Market(); legacy_cases=[]; legacy_summary=[]
    for hours in (1,4,24,72):
        pairs=[]
        for signal in legacy:
            e={**signal,"event_id":signal["source_id"]+":legacy", "reaction_type":"immediate_view"}
            five=evaluate(e,market,hours,1)
            target=(utc(e["published_at"])+pd.Timedelta(minutes=1)).ceil("h")
            shifted={**e,"published_at":str(target-pd.Timedelta(minutes=1))}
            hour=evaluate(shifted,market,hours,1)
            if five["status"]==hour["status"]=="ok":
                pairs.append((five,hour))
                legacy_cases.append({"source_id":signal["source_id"],"hours":hours,
                                     "publication":e["published_at"],"entry_5m":five["entry_at"],"entry_1h":hour["entry_at"],
                                     "net_5m":five["net_return"],"net_1h":hour["net_return"]})
        legacy_summary.append({"hours":hours,"n_matched":len(pairs),
                               "mean_5m":float(np.mean([a["net_return"] for a,b in pairs])) if pairs else None,
                               "mean_1h":float(np.mean([b["net_return"] for a,b in pairs])) if pairs else None})
    totals={"semantic_information_count":json.loads((OUT/"taxonomy.json").read_text(encoding="utf-8"))["summary"]["information_messages"],
            "resolved_events":len(events),"successful_scenario_events":sum(r["status"]=="ok" for r in outcomes),
            "scenario_event_statuses":dict(pd.Series([r["status"] for r in outcomes]).value_counts().astype(int).to_dict()),
            "input_hashes":{name:hashlib.sha256((OUT/name).read_bytes()).hexdigest() for name in
                            ("response_results.json","response_outcomes.jsonl","admitted_trades.jsonl","resolved_response_events.json")}}
    write(OUT/"response_diagnostics.json",{"totals":totals,"concentration":concentration,
                                           "matched_event_horizons":common,"timestamp_rounding":timing,
                                           "legacy40_timing_comparison":legacy_summary,
                                           "legacy40_timing_cases":legacy_cases,
                                           "legacy_market_sources":market.sources,
                                           "caveat":"Matched-event diagnostics ignore portfolio overlap; not returns of another deployable account. Historical timing availability is assumed, not observed."})
    pd.DataFrame([{k:v for k,v in s.items() if not isinstance(v,(dict,list))} for s in result["summary"]]).to_csv(OUT/"all_scenarios.csv",index=False)
    print(json.dumps({"totals":totals,"legacy40_timing_comparison":legacy_summary,"timestamp_rounding":timing},ensure_ascii=True,indent=2))


if __name__=="__main__": main()
