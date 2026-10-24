"""Bounded offline horizon study; frozen labels and local public-market evidence only.

Same-entry paired comparisons use one common signal cohort and common pre-entry
null starts across all five horizons. No AI, network, accounts or orders.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))
from scripts.review_telegram_signals import FIXED_CUTOFF, entry_hour, load_signals
from scripts.research_alastar_images import build_signals
from scripts.research_alastar_responses import Market, funding_range
from scripts.research_telegram_signal_strategy import LocalMarket

OUT = ROOT / "data/research/alastar_horizons_20261022"
REPORT = ROOT / "reports/alastar_horizons_20261022"
HORIZONS = (1, 4, 24, 72, 168)
MIN_NULL = 100  # hourly starts, not v2's 2,000 five-minute starts
DRAWS = 5000
SEED = 20261022


def digest(raw): return hashlib.sha256(raw).hexdigest()


def save_bytes(path,raw):
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists() and path.read_bytes()!=raw:
        old=path.read_bytes(); history=path.parent/'history'/f'{path.stem}_{digest(old)}{path.suffix}'
        history.parent.mkdir(exist_ok=True)
        if not history.exists(): history.write_bytes(old)
    path.write_bytes(raw)


def write(path, obj):
    save_bytes(path,json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False, default=str).encode('utf-8'))


def net_path(frame, start, hours, side, step="h", cost=.002):
    """Only pre-exit highs/lows count; known liquidation survives later gaps."""
    end = start+pd.Timedelta(hours=hours)
    path = frame.reindex(pd.date_range(start, end, freq=step))
    first = path.iloc[0]
    if not np.isfinite(first["open"]) or first["open"] <= 0 or first["volume"] <= 0: return None
    p0 = float(first["open"])
    risk = path.iloc[:-1]
    bad = (~np.isfinite(risk[["open", "high", "low", "close", "volume"]]).all(axis=1)
           | (risk[["open", "high", "low", "close"]] <= 0).any(axis=1) | (risk.volume <= 0)).to_numpy()
    hits = (risk.high >= 1.99*p0).to_numpy() if side == -1 else (risk.low <= .005*p0).to_numpy()
    bad_at = int(np.flatnonzero(bad)[0]) if bad.any() else len(risk)
    hit_at = int(np.flatnonzero(hits)[0]) if hits.any() else len(risk)
    if hit_at < bad_at:
        return {"net": -1., "liquidated": True, "entry_price": p0,
                "last_exposure_at": risk.index[hit_at]+pd.Timedelta(step if step != "h" else "1h")}
    last = path.iloc[-1]
    if bad_at < len(risk) or not np.isfinite(last["open"]) or last["open"] <= 0 or last["volume"] <= 0: return None
    return {"net": max(-1., side*(float(last["open"])/p0-1)-cost), "liquidated": False,
            "entry_price": p0, "last_exposure_at": end}


class Panel:
    def __init__(self, frame):
        self.frame = frame.reindex(pd.date_range(frame.index.min(), frame.index.max(), freq="h")) if len(frame) else frame
        self.cache = {}

    def outcomes(self, hours, side):
        key = hours, side
        if key in self.cache: return self.cache[key]
        f = self.frame; n = len(f); out = np.full(n, np.nan)
        if n <= hours: return out
        o, high = f.open.to_numpy(float), f.high.to_numpy(float)
        good = np.isfinite(f[["open", "high", "low", "close", "volume"]]).all(axis=1).to_numpy()
        good &= (f[["open", "high", "low", "close", "volume"]] > 0).all(axis=1).to_numpy()
        size = n-hours; alive = good[:size].copy(); hit = np.zeros(size, bool)
        for k in range(hours):
            alive &= good[k:k+size]
            hit |= alive & ((high[k:k+size] >= 1.99*o[:size]) if side == -1 else
                            (f.low.to_numpy(float)[k:k+size] <= .005*o[:size]))
        complete = alive & np.isfinite(o[hours:]) & (o[hours:] > 0) & (f.volume.to_numpy()[hours:] > 0)
        with np.errstate(divide="ignore", invalid="ignore"):
            raw = np.maximum(-1., side*(o[hours:]/o[:size]-1)-.002)
        out[:size] = np.where(hit, -1., np.where(complete, raw, np.nan))
        self.cache[key] = out
        return out

    def positions(self, at, side, common=True, hours=168):
        if len(self.frame) == 0 or at not in self.frame.index: return None, np.array([], dtype=int)
        p = int(self.frame.index.get_loc(at))
        idx = np.arange(max(0, p-720), p-(max(HORIZONS) if common else hours)+1)
        for h in HORIZONS if common else (hours,):
            idx = idx[np.isfinite(self.outcomes(h, side)[idx])]
        return p, idx


def nonoverlap(rows, hours):
    kept, until = [], {}
    for r in sorted(rows, key=lambda x: (x["entry_at"], x["signal_id"])):
        if r["entry_at"] < until.get(r["symbol"], r["entry_at"]): continue
        kept.append(r); until[r["symbol"]] = r["entry_at"]+pd.Timedelta(hours=hours)
    return kept


def week_interval(values, stamps):
    groups = {}
    for v, at in zip(values, stamps):
        week = str((at-pd.Timedelta(days=at.weekday())).date())
        groups.setdefault(week, []).append(v)
    if len(groups) < 8: return None, len(groups)
    sums = np.array([sum(v) for v in groups.values()]); sizes = np.array([len(v) for v in groups.values()])
    pick = np.random.default_rng(SEED).integers(0, len(groups), (DRAWS, len(groups)))
    return np.quantile(sums[pick].sum(1)/sizes[pick].sum(1), [.025, .975]).tolist(), len(groups)


def cells_for(rows, cohort, mode):
    cells = []
    for phase in ("all", "earlier", "later"):
        base = [r for r in rows if phase == "all" or ((r["entry_at"] >= FIXED_CUTOFF) == (phase == "later"))]
        if not base: continue
        for hi, h in enumerate(HORIZONS):
            values = np.array([r["net"][hi] for r in base]); excess = np.array([r["excess"][hi] for r in base])
            delta = excess-np.array([r["excess"][0] for r in base]); stamps = [r["entry_at"] for r in base]
            ci, weeks = week_interval(excess, stamps); dci, _ = week_interval(delta, stamps)
            totals = Counter()
            for r, v in zip(base, excess): totals[r["symbol"]] += v
            best_symbol = max(totals, key=totals.get)
            without_symbol = [v for r, v in zip(base, excess) if r["symbol"] != best_symbol]
            funded = [r["funded"][hi] for r in base if r["funded"][hi] is not None]
            cells.append({"cohort": cohort, "mode": mode, "phase": phase, "hours": h, "n": len(base), "weeks": weeks,
                          "mean_net": float(values.mean()), "median_net": float(np.median(values)),
                          "mean_net_80bps": float(np.maximum(-1., values-.006).mean()),
                          "mean_excess": float(excess.mean()), "median_excess": float(np.median(excess)), "excess_ci95": ci,
                          "mean_excess_horizon_specific_null": float(np.mean([r['specific_excess'][hi] for r in base])),
                          "paired_gain_horizon_specific_null": float(np.mean([r['specific_excess'][hi]-r['specific_excess'][0] for r in base])),
                          "excess_without_best_trade": float(np.delete(excess, excess.argmax()).mean()) if len(base)>1 else None,
                          "excess_without_best_symbol": float(np.mean(without_symbol)) if without_symbol else None,
                          "best_symbol": best_symbol, "paired_excess_gain_vs_1h": float(delta.mean()), "paired_gain_ci95": dci,
                          "paired_net_gain_vs_1h": float((values-np.array([r["net"][0] for r in base])).mean()),
                          "funded_n": len(funded), "mean_funded_net": float(np.mean(funded)) if funded else None,
                          "mean_vs_48h_reversal": float(np.mean([r["vs_reversal"][hi] for r in base if r["vs_reversal"][hi] is not None]))
                          if any(r["vs_reversal"][hi] is not None for r in base) else None})
    return cells


def max_t(rows_by_cohort, cells):
    """Exploratory week sign-flip max-|t|; all periods and overlapping groups share signs."""
    eligible = [c for c in cells if c["mode"]=="hour_common" and c["weeks"]>=8]
    weeks = sorted({str((r["entry_at"]-pd.Timedelta(days=r["entry_at"].weekday())).date())
                    for rows in rows_by_cohort.values() for r in rows})
    rng = np.random.default_rng(SEED+5)
    signs = rng.choice([-1., 1.], size=(DRAWS, len(weeks)))
    wmap = {w:i for i,w in enumerate(weeks)}; scores, null_scores = [], []
    for c in eligible:
        raw = np.zeros(len(weeks)); d = np.zeros(len(weeks)); hidx=HORIZONS.index(c["hours"])
        for r in rows_by_cohort[c["cohort"]]:
            if c["phase"] != "all" and ((r["entry_at"]>=FIXED_CUTOFF) != (c["phase"]=="later")): continue
            w = str((r["entry_at"]-pd.Timedelta(days=r["entry_at"].weekday())).date())
            raw[wmap[w]] += r["excess"][hidx]
            d[wmap[w]] += r["excess"][hidx]-r["excess"][0]
        for field, a in (("excess",raw), ("gain",d)):
            den = np.sqrt(np.square(a).sum())
            if den == 0: continue
            score = a.sum()/den; pseudo=(signs*a).sum(axis=1)/den
            scores.append((c,field,score)); null_scores.append(pseudo)
    if not scores: return
    maximum = np.max(np.abs(np.vstack(null_scores)),axis=0)
    for (c,field,score), pseudo in zip(scores,null_scores):
        c[field+"_p_week_signflip_two_sided"] = float((1+(np.abs(pseudo)>=abs(score)-1e-12).sum())/(DRAWS+1))
        c[field+"_p_family_max_t"] = float((1+(maximum>=abs(score)-1e-12).sum())/(DRAWS+1))


def run():
    OUT.mkdir(parents=True,exist_ok=True); REPORT.mkdir(parents=True,exist_ok=True)
    text, notes = load_signals(); text=[s for s in text if s["channel"]=="alastartrades"]
    image, image_notes=build_signals()
    cohorts={"text_fresh": [s for s in text if s["kind"]=="fresh_call"], "text_broad": text,
             "image_short": [s for s in image if s["kind"]=="image_first_leg" and s["side"]==-1]}
    plan={"created_at_utc":datetime.now(timezone.utc).isoformat(),"horizons":HORIZONS,"cohort_counts":{k:len(v) for k,v in cohorts.items()},
          "historical_exploratory_only":True,"holding_comparison":"same signal and entry; common168h nonoverlap and price/null availability",
          "null":"hourly starts in [entry-30d, entry-168h], common valid starts for all horizons; price-only20bps both sides",
          "minimum_hourly_null_starts":MIN_NULL,"phases_cutoff":str(FIXED_CUTOFF),"draws":DRAWS,"seed":SEED,
          "models":["hour_common","5m_common_sensitivity"],"notes":notes,"image_notes":image_notes}
    plan_path=OUT/"analysis_plan.json"
    if not plan_path.exists(): write(plan_path,plan)
    market, official=LocalMarket(), Market(); panels={}; coverage=[]; sets={}; sensitivity_sets={}; cells=[]; independent=[]; excluded_rows=[]
    sources={"frozen_text":digest((ROOT/'data/research/alastar_response_20261021/resolved_response_events.json').read_bytes())}
    for cohort, signals in cohorts.items():
        rows=[]; sensitive=[]; exclusions=Counter(); available={h:[] for h in HORIZONS}
        for s in signals:
            sym, side=s["symbol"],s["side"]
            if sym not in panels: panels[sym]=Panel(market.frame(sym))
            p=panels[sym]; at=entry_hour(s["published_at"])
            specific=[]
            for h in HORIZONS:
                own_pos, own_idx=p.positions(at,side,common=False,hours=h)
                valid=own_pos is not None and np.isfinite(p.outcomes(h,side)[own_pos]) and len(own_idx)>=MIN_NULL
                if valid:
                    actual_net=float(p.outcomes(h,side)[own_pos]); own_null=float(p.outcomes(h,side)[own_idx].mean())
                    available[h].append({**s,'entry_at':at,'net':actual_net,'excess':actual_net-own_null})
                    specific.append(own_null)
                else: specific.append(None)
            pos, idx=p.positions(at,side)
            if pos is None or not all(np.isfinite(p.outcomes(h,side)[pos]) for h in HORIZONS):
                reason='missing_common_actual_path'; exclusions[reason]+=1
                excluded_rows.append({'cohort':cohort,**s,'reason':reason}); continue
            if len(idx)<MIN_NULL:
                reason='insufficient_common_preentry_null'; exclusions[reason]+=1
                excluded_rows.append({'cohort':cohort,**s,'reason':reason}); continue
            net=np.array([p.outcomes(h,side)[pos] for h in HORIZONS]); null=np.array([p.outcomes(h,side)[idx].mean() for h in HORIZONS])
            fund_frame=official.funding_for(sym)
            def funded(start,h,ret):
                end=ret["last_exposure_at"]
                if not official.funding_coverage_ok(sym,start-pd.Timedelta(seconds=1),end+pd.Timedelta(seconds=1)): return None
                f=funding_range(fund_frame,start,end,ret['entry_price'],side,complete_query=True)
                return max(-1.,ret['net']+f['lower']) if f is not None else None
            actual=[net_path(p.frame,at,h,side) for h in HORIZONS]
            assert all(a is not None and abs(a['net']-v)<1e-10 for a,v in zip(actual,net))
            rev=None
            if pos>=48 and np.isfinite(p.frame.open.iloc[pos-48]) and p.frame.open.iloc[pos-48]>0:
                rev=-int(np.sign(p.frame.open.iloc[pos]/p.frame.open.iloc[pos-48]-1))
            reversal=[float(net[i]-p.outcomes(h,rev)[pos]) if rev in(-1,1) and np.isfinite(p.outcomes(h,rev)[pos]) else None for i,h in enumerate(HORIZONS)]
            base={**s,'entry_at':at,'net':net.tolist(),'excess':(net-null).tolist(),'null_mean':null.tolist(),'null_n':len(idx),
                  'specific_excess':[float(v-n) for v,n in zip(net,specific)],
                  'funded':[funded(at,h,a) for h,a in zip(HORIZONS,actual)],'vs_reversal':reversal}
            rows.append(base)
            f5=official.prices_for(sym); at5=(pd.Timestamp(s['published_at'])+pd.Timedelta(seconds=60)).ceil('5min')
            values5=[net_path(f5,at5,h,side,step='5min') for h in HORIZONS] if len(f5) else []
            if values5 and all(a is not None for a in values5):
                v5=np.array([a['net'] for a in values5])
                idx5=idx[p.frame.index[idx+max(HORIZONS)]<=at5]
                if len(idx5)<MIN_NULL: continue
                null5=np.array([p.outcomes(h,side)[idx5].mean() for h in HORIZONS])
                specific5=[]
                for i,h in enumerate(HORIZONS):
                    _, ih=p.positions(at,side,common=False,hours=h)
                    ih=ih[p.frame.index[ih+h]<=at5]
                    specific5.append(float(v5[i]-p.outcomes(h,side)[ih].mean()))
                sensitive.append({**base,'entry_at':at5,'net':v5.tolist(),'excess':(v5-null5).tolist(),'null_mean':null5.tolist(),'null_n':len(idx5),
                                  'specific_excess':specific5,
                                  'funded':[funded(at5,h,a) for h,a in zip(HORIZONS,values5)],'vs_reversal':[None]*len(HORIZONS)})
        kept=nonoverlap(rows,max(HORIZONS)); sets[cohort]=kept
        # Keep hourly comparison cohort fixed for sensitivity; missing5m stays explicit.
        ids={r['signal_id'] for r in kept}; sensitive=[r for r in sensitive if r['signal_id'] in ids]; sensitivity_sets[cohort]=sensitive
        coverage.append({'cohort':cohort,'signals':len(signals),'common_paths_and_null':len(rows),'common_nonoverlap':len(kept),
                         'overlap_removed':len(rows)-len(kept),'5m_sensitivity':len(sensitive),'excluded':dict(exclusions)})
        cells.extend(cells_for(kept,cohort,'hour_common')); cells.extend(cells_for(sensitive,cohort,'5m_common_sensitivity'))
        for h, available_rows in available.items():
            own_all=nonoverlap(available_rows,h)
            for phase in ('all','later'):
                own=[r for r in own_all if phase=='all' or r['entry_at']>=FIXED_CUTOFF]
                independent.append({'cohort':cohort,'phase':phase,'hours':h,'n':len(own),
                                    'mean_net':float(np.mean([r['net'] for r in own])) if own else None,
                                    'mean_excess_horizon_specific_null':float(np.mean([r['excess'] for r in own])) if own else None})
        print(cohort,coverage[-1],flush=True)
    max_t(sets,cells)
    # Persist inspectable per-signal values; no rounded inputs enter the summary.
    write(OUT/'paired_signals.json',sets); write(OUT/'paired_signals_5m.json',sensitivity_sets)
    write(OUT/'summary.json',{'coverage':coverage,'cells':cells})
    write(OUT/'excluded_signals.json',excluded_rows)
    sources.update(market.sources); sources.update(official.sources)
    for path in sorted((ROOT/'data/research/alastar_images_20261021/labels').glob('*.json')): sources[str(path.relative_to(ROOT))]={'sha256':digest(path.read_bytes())}
    write(OUT/'source_manifest.json',sources)
    for name,data in (('HORIZON_COMPARISON',cells),('COHORT_COVERAGE',coverage),('INDEPENDENT_HORIZONS',independent)):
        save_bytes(REPORT/f'{name}.csv',pd.DataFrame(data).to_csv(index=False).encode('utf-8-sig'))
    write(OUT/'run_receipt.json',{'completed_at_utc':datetime.now(timezone.utc).isoformat(),'script_sha256':digest(Path(__file__).read_bytes()),
                               'plan_sha256':digest(plan_path.read_bytes()),'summary_sha256':digest((OUT/'summary.json').read_bytes()),
                               'risk_model':'v2 approximate1x; shorthigh>=1.99*entry or longlow<=.005*entry; known loss survives subsequent gaps',
                               '5m_null_proxy':'hourly starts whose longest exit is no later than the actual5m entry; no future endpoint'})
    print(json.dumps({'report':str(REPORT),'cells':len(cells),'coverage':coverage},ensure_ascii=True),flush=True)
    return cells


if __name__=='__main__': run()
