"""Same-coin price-only factor diagnostic; does not replace funded strategy PnL."""
from datetime import datetime,timezone
import hashlib,json,math,statistics
from pathlib import Path
from scripts.research_korea_composite import DAY,FEE,month_ci,timestamp

ROOT=Path(__file__).resolve().parents[1]


def matched_price_reference(history,day):
    required=[history.get(str(day-i*DAY)) for i in range(1,38)]
    if any(r is None or min(r[0],r[1])<=0 or not all(math.isfinite(float(x)) for x in r[:2]) for r in required):
        return None
    returns=[1-history[str(day-j*DAY)][0]/history[str(day-(j+7)*DAY)][0]-FEE for j in range(1,31)]
    return {'reference_mean':statistics.mean(returns),'reference_n':len(returns),
            'latest_reference_bar_open_ms':day-DAY,'earliest_reference_bar_open_ms':day-37*DAY}


def main():
    folder=ROOT/'data/research/korea_factor_selection_20261029';out=folder/'benchmark_run_01';out.mkdir(exist_ok=False)
    paths=[folder/'run_02/analysis.json',ROOT/'data/research/upbit/perp_daily_cache.json',
           ROOT/'data/research/upbit/perp_cache.json',folder/'benchmark_protocol.json']
    blobs={str(p):p.read_bytes() for p in paths};prior=json.loads(blobs[str(paths[0])]);daily=json.loads(blobs[str(paths[1])]);cache=json.loads(blobs[str(paths[2])]);rows=[]
    for r in prior['events']:
        if r['cohort']!='legacy':continue
        day=timestamp(r['date']);base={'key':r['key'],'kind':r['kind'],'token':r['token'],'date':r['date'],'reference':None,'event_price_only':None,'excess_pp':None}
        bars=cache['v2|'+r['symbol']+'|'+r['date'][:10]]['klines']
        reference=matched_price_reference(daily.get(r['symbol'],{}),day)
        if reference:
            actual=1-float(bars[7][4])/float(bars[0][4])-FEE
            base.update(reference=reference['reference_mean'],event_price_only=actual,
                        excess_pp=(actual-reference['reference_mean'])*100,reference_details=reference)
        rows.append(base)
    summary={}
    for family in ['caution','warning_urged','listing_krw']:
        allrows=[r for r in rows if r['kind']==family];used=[r for r in allrows if r['excess_pp'] is not None]
        values=[r['excess_pp']/100 for r in used]
        summary[family]={'all_events':len(allrows),'covered':len(used),'missing':len(allrows)-len(used),
                         'event_price_only_mean_pct':statistics.mean(r['event_price_only'] for r in used)*100 if used else None,
                         'same_coin_prior_mean_pct':statistics.mean(r['reference'] for r in used)*100 if used else None,
                         'excess_mean_pp':statistics.mean(r['excess_pp'] for r in used) if used else None,
                         'excess_month_bootstrap90_pp':month_ci(used,values),
                         'positive_excess_n':sum(r['excess_pp']>0 for r in used)}
    report={'generated_at_utc':datetime.now(timezone.utc).isoformat(),'protocol':json.loads(blobs[str(paths[3])]),
            'summary':summary,'events':rows,'implementation_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'manifest':{p:hashlib.sha256(b).hexdigest() for p,b in blobs.items()}}
    with (out/'analysis.json').open('x',encoding='utf-8') as f:json.dump(report,f,indent=2,ensure_ascii=False,allow_nan=False)
    print(json.dumps(summary,indent=2))


if __name__=='__main__':main()
