"""One bounded INIT public-data watch cycle; scheduled by this chat's heartbeat.

Writes only data/research/init_risk_watch. Never changes strategies, ledgers,
account settings or the existing news collector configuration.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# Loading the pure module directly avoids core.research.__init__ importing the
# strategy research package and its unrelated runtime configuration.
_spec = importlib.util.spec_from_file_location('init_watch_rules', ROOT/'core/research/init_risk_watch.py')
_rules = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_rules)
VERSION, HOUR = _rules.VERSION, _rules.HOUR
features, conditions, transition, milliseconds = _rules.features, _rules.conditions, _rules.transition, _rules.milliseconds
from core.utils.file_lock import advisory_file_lock

DEFAULT_OUTPUT = ROOT/'data/research/init_risk_watch'


def write_new(path, value):
    blob = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False).encode('utf-8')
    with path.open('xb') as stream:
        stream.write(blob)
        stream.flush()
        os.fsync(stream.fileno())
    return hashlib.sha256(blob).hexdigest()


def replace_state(path, value):
    stage = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    write_new(stage, value)
    stage.replace(path)


async def collect():
    import httpx
    from config.settings import settings
    from core.utils.shared_ssl import get_shared_ssl_context
    args = {'timeout': 20, 'verify': get_shared_ssl_context()}
    if settings.HTTPS_PROXY or settings.HTTP_PROXY:
        args['proxy'] = settings.HTTPS_PROXY or settings.HTTP_PROXY
    base = 'https://fapi.binance.com'
    closed_hour_end = int(datetime.now(timezone.utc).timestamp()*1000)//HOUR*HOUR-1
    specs = {
        'klines': (base+'/fapi/v1/klines', {'symbol':'INITUSDT','interval':'1h','limit':26,'endTime':closed_hour_end}),
        'oi': (base+'/futures/data/openInterestHist', {'symbol':'INITUSDT','period':'1h','limit':8}),
        'top_positions': (base+'/futures/data/topLongShortPositionRatio', {'symbol':'INITUSDT','period':'1h','limit':8}),
        'accounts': (base+'/futures/data/globalLongShortAccountRatio', {'symbol':'INITUSDT','period':'1h','limit':8}),
        'taker': (base+'/futures/data/takerlongshortRatio', {'symbol':'INITUSDT','period':'1h','limit':8}),
        'mark': (base+'/fapi/v1/premiumIndex', {'symbol':'INITUSDT'}),
        'rpc': ('https://rpc.initia.xyz/status', {}),
        'rest': ('https://rest.initia.xyz/cosmos/base/tendermint/v1beta1/blocks/latest', {}),
        'upbit_notice': ('https://api-manager.upbit.com/api/v1/announcements/6708', {}),
        'upbit_wallet_notices': ('https://api-manager.upbit.com/api/v1/announcements', {'os':'web','page':1,'per_page':20,'category':'wallet'}),
        'upbit_trade_notices': ('https://api-manager.upbit.com/api/v1/announcements', {'os':'web','page':1,'per_page':20,'category':'trade'}),
    }
    async with httpx.AsyncClient(**args) as client:
        async def get(name, url, params):
            rec = {'url':url, 'params':params, 'attempts':[]}
            # One same-endpoint retry for transport/5xx only. Refusals/429 are
            # final. Keep both attempts, never conceal an unavailable source.
            for attempt in range(2):
                evidence = {'started_at_utc':datetime.now(timezone.utc).isoformat()}
                retry = False
                try:
                    response = await client.get(url, params=params)
                    evidence['http_status'] = response.status_code
                    retry = response.status_code >= 500
                    response.raise_for_status()
                    rec['body'] = response.json()
                    rec.pop('error_type', None)
                except Exception as exc:
                    rec['error_type'] = evidence['error_type'] = type(exc).__name__
                    retry = retry or isinstance(exc, httpx.TransportError)
                rec['attempts'].append(evidence)
                if not retry or attempt==1:
                    break
                await asyncio.sleep(1)
            rec['received_at_utc'] = datetime.now(timezone.utc).isoformat()
            return name, rec
        return dict(await asyncio.gather(*(get(k, *v) for k, v in specs.items())))


def collect_x():
    # Existing authenticated reader, shared request budget/refusal handling.
    # Bounded latest-five coverage; never claims complete timelines.
    from config.settings import settings
    from core.utils.proxy_env import ensure_proxy_env
    from core.news.collectors.x_accounts import XAccountsCollector
    ensure_proxy_env(settings)
    client = XAccountsCollector({'defaults':{
        'x_accounts_handles':['initia','ItsAlwaysZonny','spreekaway'],
        'x_accounts_tweets_per_account':5,
    }})
    try:
        items = client.pull_latest(max_records=15, since_minutes=14*1440)
        return {'received_at_utc':datetime.now(timezone.utc).isoformat(),
                'failed_handles':sorted(client.last_account_errors),
                'coverage':'latest five fetched posts per handle; collector filters replies/retweets; not complete',
                'posts':[{'handle':r['payload']['handle'], 'url':r['url'],
                          'published_at':r['published_at'], 'text':r['content']} for r in items]}
    finally:
        client.close()


def run(folder, *, skip_x=False):
    folder.mkdir(parents=True, exist_ok=True)
    state_path = folder/'state.json'
    with advisory_file_lock(state_path):
        state = json.loads(state_path.read_bytes()) if state_path.exists() else {}
        started = datetime.now(timezone.utc).isoformat()
        activated = state.get('activated_at_utc', started)
        receipts = asyncio.run(collect())
        if not state.get('x_last_attempt_failed') and 'last_x_attempt_ms' in state:
            state['next_x_attempt_ms'] = min(state.get('next_x_attempt_ms', 10**20), state['last_x_attempt_ms']+HOUR)
        if not skip_x and milliseconds(started) >= state.get('next_x_attempt_ms', 0):
            try:
                x = collect_x()
            except Exception as exc:
                x = {'error_type':type(exc).__name__, 'received_at_utc':datetime.now(timezone.utc).isoformat()}
            receipts['x'] = x
            state['last_x_attempt_ms'] = milliseconds(x['received_at_utc'])
            if x.get('error_type') or x.get('failed_handles'):
                state['x_consecutive_failures'] = state.get('x_consecutive_failures', 0)+1
                state['x_last_attempt_failed'] = True
                state['next_x_attempt_ms'] = state['last_x_attempt_ms']+6*HOUR
            else:
                state['x_consecutive_failures'] = 0
                state['x_last_attempt_failed'] = False
                state['next_x_attempt_ms'] = state['last_x_attempt_ms']+HOUR
        now = datetime.now(timezone.utc).isoformat()
        at = milliseconds(now)
        bodies = {k:r['body'] for k,r in receipts.items() if 'body' in r and not r.get('error_type')}
        if 'rpc' not in bodies and 'rest' in bodies:
            try:
                header = bodies['rest']['block']['header']
                bodies['rpc'] = {'result':{'node_info':{'network':header['chain_id']},
                                          'sync_info':{'latest_block_time':header['time'], 'latest_block_height':header['height']}}}
            except (KeyError, TypeError):
                pass
        source_available = {k:milliseconds(r['received_at_utc']) for k,r in receipts.items() if r.get('received_at_utc')}
        derived, issues = features(bodies, at, source_available)
        if 'upbit_notice' not in bodies:
            issues.append('upbit_notice_unavailable')
        elif not isinstance(bodies['upbit_notice'].get('data', {}).get('body'), str) or not bodies['upbit_notice']['data']['body']:
            issues.append('upbit_notice_invalid_body')
        for name in ['upbit_wallet_notices', 'upbit_trade_notices']:
            if not isinstance(bodies.get(name, {}).get('data', {}).get('notices'), list):
                issues.append(name+'_unavailable')
        if state.get('x_last_attempt_failed'):
            issues.append('x_last_attempt_failed_backoff')
        current = conditions(derived)
        if issues:
            state['consecutive_degraded'] = state.get('consecutive_degraded', 0)+1
            if state['consecutive_degraded'] >= 3:
                current['data_degraded'] = '连续3次数据不完整：'+', '.join(issues)
        else:
            state['consecutive_degraded'] = 0
        next_conditions, alerts = transition(state.get('conditions', {}), current, at)
        if not issues and 'data_degraded' in state.get('conditions', {}).get('active', []):
            alerts.append({'kind':'data_recovered','message':'此前报告的数据采集异常已恢复；不代表链状态或市场风险恢复正常'})
        # A changed public notice is a review trigger, not proof wallets reopened.
        if 'upbit_notice' in bodies:
            notice = bodies['upbit_notice'].get('data', {})
            body = notice.get('body')
            if not isinstance(body, str) or not body:
                pass
            else:
                sha = hashlib.sha256(body.encode()).hexdigest()
                if state.get('notice_sha256') and sha != state['notice_sha256']:
                    alerts.append({'kind':'notice_changed', 'message':'Upbit INIT原警示正文变更，需核实充提和警示结论',
                                   'url':'https://www.upbit.com/service_center/notice?id=1504353355'})
                state['notice_sha256'] = sha
        old_tip = state.get('last_chain_tip')
        if 'chain_height' in derived:
            tip = {k:derived[k] for k in ['chain_height','chain_tip_ms','chain_endpoint_state']}
            if old_tip and old_tip['chain_endpoint_state']=='stale' and tip['chain_endpoint_state']=='fresh' and tip['chain_height']>old_tip['chain_height']:
                alerts.append({'kind':'node_resumed', 'message':'官方公共节点出现新鲜更高区块；须再核实官方复链、其它节点及交易所充提'})
            state['last_chain_tip'] = tip
        notice_versions = set(state.get('seen_notice_versions', []))
        for name in ['upbit_wallet_notices', 'upbit_trade_notices']:
            rows = bodies.get(name, {}).get('data', {}).get('notices', [])
            if not isinstance(rows, list):
                continue
            for row in rows:
                title = str(row.get('title') or '')
                if not re.search(r'(?i)\bINIT\b|이니시아', title):
                    continue
                # Seed pre-existing notices. Metadata updates are review triggers;
                # only title/identity/time below define an announcement version.
                key = hashlib.sha256(json.dumps([row.get('id'),title,row.get('listed_at')],ensure_ascii=False).encode()).hexdigest()
                try:
                    pub = milliseconds(row['listed_at'])
                except (KeyError, ValueError, TypeError):
                    issues.append(name+'_invalid_timestamp')
                    continue
                if key not in notice_versions and milliseconds(activated) <= pub <= at:
                    alerts.append({'kind':'exchange_notice','message':'新发现Upbit INIT公告，标题不等于正文已核验',
                                   'title':title,'published_at':row['listed_at'],
                                   'url':row.get('share_url') or 'https://api-manager.upbit.com/api/v1/announcements/'+str(row['id'])})
                notice_versions.add(key)
        state['seen_notice_versions'] = sorted(notice_versions)
        x = receipts.get('x', {})
        seen = set(state.get('seen_x_versions', []))
        for post in x.get('posts', []):
            key = hashlib.sha256((post['url']+'\n'+post['text']).encode()).hexdigest()
            relevant = post['handle']=='initia' or re.search(r'(?i)\bINIT(?:USDT)?\b|initia|MoveVM', post['text'])
            if key not in seen and relevant and milliseconds(activated) <= milliseconds(post['published_at']) <= at:
                alerts.append({'kind':'source_update','message':'新发现来源更新；原文断言待核实，不作交易指令',**post,
                               'receipt_lag_seconds':(at-milliseconds(post['published_at']))/1000})
            seen.add(key)
        state['seen_x_versions'] = sorted(seen)
        state.update(version=VERSION, activated_at_utc=activated, updated_at_utc=now,
                     conditions=next_conditions, latest_features=derived, latest_issues=issues)
        run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'_'+uuid.uuid4().hex[:8]
        evidence = folder/'runs'/run_id
        evidence.mkdir(parents=True)
        sha = write_new(evidence/'receipts.json', receipts)
        result = {'version':VERSION,'started_at_utc':started,'observed_at_utc':now,
                  'activated_at_utc':activated,'features':derived,'issues':issues,'alerts':alerts,
                  'receipts_path':str(evidence/'receipts.json'),'receipts_sha256':sha,
                  'limits':['no individual CEX positions','no verified native INIT wallet labels/flows',
                            'RPC and REST are one operator; no independent global restart confirmation',
                            'Upbit latest 20 wallet and 20 trade notices per cycle; not a complete archive',
                            'X latest five posts every hour; not real-time or complete'],
                  'monitor_only':True,'trading_enabled':False}
        result_sha = write_new(evidence/'result.json', result)
        state['latest_result_path'] = str(evidence/'result.json')
        state['latest_result_sha256'] = result_sha
        replace_state(state_path, state)
        return result


if __name__ == '__main__':
    if hasattr(sys.stdout,'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument('--skip-x', action='store_true', help='public market/chain only; does not consume X requests')
    parser.add_argument('--status', action='store_true', help='read existing state without network or writes')
    args = parser.parse_args()
    if args.status:
        print((args.output_dir/'state.json').read_text('utf-8') if (args.output_dir/'state.json').exists() else '{}')
    else:
        print(json.dumps(run(args.output_dir, skip_x=args.skip_x), ensure_ascii=False, indent=2))
