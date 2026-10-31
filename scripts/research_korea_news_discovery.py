"""Read-only, bounded public-news discovery for fixed Korean-caution episodes.

This does not collect messages, classify with an external model, trade, or
rewrite old books. Discovery output is frozen before new execution-path data.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
DAY = 86_400_000
SOURCES = ('chaincatcher','cointelegraph','coindesk','CoinDesk','decrypt',
           'Crypto Briefing','BeInCrypto','Binance','okx')
ALIASES = {
    'IOTX':r'\b(?:IOTX|IoTeX)\b', 'RESOLV':r'\bRESOLV\b',
    'DOT':r'\b(?:DOT|Polkadot)\b|波卡', 'KERNEL':r'\b(?:KERNEL|KernelDAO|Kernel DAO)\b',
    'BONK':r'\bBONK\b','STORJ':r'\bSTORJ\b','JASMY':r'\bJASMY\b',
    'ZIL':r'\b(?:ZIL|Zilliqa)\b','SNX':r'\b(?:SNX|Synthetix)\b',
    'RVN':r'\b(?:RVN|Ravencoin)\b','MANTRA':r'\bMANTRA\b',
    'SAND':r'\b(?:SAND|The Sandbox|Sandbox)\b',
    'EGLD':r'\b(?:EGLD|MultiversX)\b','SOPH':r'\b(?:SOPH|Sophon)\b',
    'XAI':r'\bXAI\b','BLUR':r'\bBLUR\b',
    'FET':r'\b(?:FET|Fetch\.ai|FetchAI|Artificial Superintelligence Alliance)\b',
    'INIT':r'\b(?:INIT|Initia)\b','INJ':r'\b(?:INJ|Injective)\b',
}
RECOVERY = re.compile(r'\bfix\w*|\bpatch\w*|\brestor\w*|\bresum\w*|\brecover\w*|'
                      r'\breimburs\w*|\bcompensat\w*|\bbacking\b|\baudit\w*|'
                      r'\bresolved?\b|\brepay\w*|修复|补丁|重启|恢复|赔付|补足|'
                      r'弥补|兜底|垫付|追回|审计|补偿|赎回|修正', re.I)
PUBLIC_DOMAINS = ('news.google.com','chaincatcher.com','coindesk.com','cointelegraph.com',
                  'decrypt.co','cryptonews.net','cryptoslate.com','theblock.co','panewslab.com',
                  'odaily.news','blockbeats.info','foresightnews.pro','cryptobriefing.com',
                  'beincrypto.com','cryptorank.io','cryptopolitan.com','coinpedia.org',
                  'bitcoinworld.co.in','cryptodaily.co.uk','tradingview.com','finance.yahoo.com')
RECOVERY_WIDE = re.compile(RECOVERY.pattern+r'|collateral gap|reserve shortfall|cover.{0,35}shortfall|fill.{0,35}gap', re.I)


def utc_ms(value):
    t = datetime.fromisoformat(str(value).replace('Z','+00:00'))
    # News storage explicitly declares its naive datetimes as UTC.
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return int(t.timestamp()*1000)


def db_time(ms):
    return datetime.fromtimestamp(ms/1000,timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')


def write_new(path, value):
    with path.open('x',encoding='utf-8') as stream:
        json.dump(value,stream,ensure_ascii=False,indent=2,allow_nan=False)


def discover(folder, *, wide_public=False):
    folder.mkdir(parents=True,exist_ok=False)
    p = ROOT/'data/research/korea_composite_20261029/run_04/analysis.json'
    original = p.read_bytes()
    prior = json.loads(original)
    pooled = json.loads((p.parent/'current_pooled_tracker_snapshot.json').read_bytes())['trades']
    upbit = json.loads((p.parent/'current_upbit_tracker_snapshot.json').read_bytes())['trades']
    cutoff = int(datetime.now(timezone.utc).timestamp()*1000)
    protocol = {'version':'korea-news-exit-discovery-20261029-v2' if wide_public else 'korea-news-exit-discovery-20261029-v1','created_at_utc':datetime.now(timezone.utc).isoformat(),
                'cutoff_ms':cutoff,'development_outcomes_known':True,'sources':SOURCES,
                'additional_public_domains':PUBLIC_DOMAINS if wide_public else [],
                'lexicon':RECOVERY_WIDE.pattern if wide_public else RECOVERY.pattern,
                'implementation_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'scope':'same pre-existing 42 observations; no outcome-driven selection of coins',
                'discovery':'asset mention plus fixed recovery lexicon; candidates require manual subject/incident review',
                'window':'first_received after entry and before original exit; currently open entries capped at observation cutoff',
                'freshness':'publication <= first receipt; publication lag <= 6h for eligible triggers; invalid and late retained',
                'next_stage':'freeze classifications before fetching new execution paths; source receipt + at least60s then next 5m open; do not replace a stop already hit',
                'coverage':'records present in the named public news feeds only; absence is not proof no announcement existed',
                'side_effects':'new research output only; no trading, state, live collector or scheduler changes',
                'cohort_source_sha256':hashlib.sha256(original).hexdigest()}
    write_new(folder/'protocol.json',protocol)
    c = sqlite3.connect((ROOT/'data/news.db').resolve().as_uri()+'?mode=ro',uri=True)
    c.row_factory = sqlite3.Row
    c.execute('BEGIN')
    placeholders = ','.join('?' for _ in SOURCES)
    where,params = 'source IN ('+placeholders+')',list(SOURCES)
    if wide_public:
        patterns = [prefix+domain+'/%' for domain in PUBLIC_DOMAINS for prefix in ['https://','https://www.']]
        where = '('+where+' OR '+' OR '.join('url LIKE ?' for _ in patterns)+')'
        params += patterns
    source_bounds = [dict(r) for r in c.execute(f'SELECT source,min(published_at) first_published,max(published_at) last_published,min(fetched_at) first_received,max(fetched_at) last_received,count(*) records FROM news_raw WHERE {where} GROUP BY source',params)]
    global_start = min(utc_ms(r['first_received']) for r in source_bounds)
    events, receipts, candidates = [], {}, []
    for e in prior['events']:
        event = {k:e.get(k) for k in ['key','token','symbol','entry_ms','cohort','status','short_return','hedged_return']}
        old = pooled.get(e['key']) or upbit.get(e['key']) or {}
        end = e.get('exit_ms') or old.get('exit_at') or e['entry_ms']+7*DAY
        event.update(exit_ms=int(end),query_end_ms=min(int(end),cutoff),
                     settled=e.get('short_return') is not None,
                     nominal_archive_window_covered=e['entry_ms']>=global_start and end<=cutoff)
        pattern = ALIASES.get(e['token'])
        if pattern is None:
            event.update(status_of_news='asset_alias_not_defined',matched_news=0,candidate_n=0)
            events.append(event)
            continue
        if event['query_end_ms']<global_start:
            event.update(status_of_news='before_archive_window',matched_news=0,candidate_n=0)
            events.append(event)
            continue
        rx = re.compile(pattern,re.I)
        rows = c.execute(f'SELECT id,source,title,content,url,published_at,fetched_at FROM news_raw WHERE {where} AND fetched_at>? AND fetched_at<=? ORDER BY fetched_at,id',
                         (*params,db_time(e['entry_ms']),db_time(event['query_end_ms'])))
        matched, found = 0,0
        for source in rows:
            r = dict(source)
            text = str(r['title'] or '')+'\n'+str(r['content'] or '')
            if not rx.search(text):
                continue
            matched += 1
            r['text_sha256'] = hashlib.sha256(text.encode()).hexdigest()
            receipts[r['id']] = r
            if not (RECOVERY_WIDE if wide_public else RECOVERY).search(text):
                continue
            first, published = utc_ms(r['fetched_at']),utc_ms(r['published_at'])
            lag = (first-published)/1000
            candidates.append({'event_key':e['key'],'token':e['token'],'news_id':r['id'],
                               'source':r['source'],'url':r['url'],'title':r['title'],
                               'published_ms':published,'first_received_ms':first,
                               'publication_lag_seconds':lag,'timestamp_eligible':0<=lag<=6*3600,
                               'text_sha256':r['text_sha256'],'review':'pending'})
            found += 1
        event.update(status_of_news='bounded_search_complete',matched_news=matched,candidate_n=found)
        events.append(event)
    c.rollback()
    c.close()
    write_new(folder/'source_bounds.json',source_bounds)
    write_new(folder/'events.json',events)
    write_new(folder/'public_news_receipts.json',list(receipts.values()))
    write_new(folder/'candidates_before_price_paths.json',candidates)
    print(json.dumps({'events':len(events),'nominal_window_covered':sum(r['nominal_archive_window_covered'] for r in events),
                      'asset_matched_news':len(receipts),'recovery_candidates':len(candidates),
                      'candidate_events':len({r['event_key'] for r in candidates}),
                      'source_count':len(source_bounds)},ensure_ascii=False,indent=2))


if __name__=='__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,required=True)
    parser.add_argument('--wide-public',action='store_true')
    args = parser.parse_args()
    discover(args.output_dir,wide_public=args.wide_public)
