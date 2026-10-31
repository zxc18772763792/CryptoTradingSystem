"""Public-data INIT observations. No trading or paper-ledger dependencies.

Thresholds are operational notifications, frozen prospectively on 2026-10-29;
they are not a backtested entry signal. Unknown and stale inputs stay unknown.
"""
from __future__ import annotations

from datetime import datetime, timezone
import math
import statistics

VERSION = 'init-risk-watch-v1-20261029'
HOUR = 3_600_000


def milliseconds(value):
    d = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if d.tzinfo is None:
        raise ValueError('timezone required')
    return int(d.timestamp() * 1000)


def number(value):
    n = float(value)
    if not math.isfinite(n):
        raise ValueError('nonfinite input')
    return n


def ordered(rows, timestamp, now_ms):
    found = {}
    for row in rows:
        t = int(timestamp(row))
        if t > now_ms:
            continue
        if t in found and found[t] != row:
            raise ValueError('conflicting duplicate observation')
        found[t] = row
    return sorted(found.items())


def features(bodies, now_ms, available_ms=None):
    """Use complete UTC hours only; require exact windows and fresh endpoints."""
    f, issues = {}, []
    available_ms = available_ms or {}
    def cutoff(name):
        return min(now_ms, int(available_ms.get(name, now_ms)))
    try:
        bars = ordered(bodies['klines'], lambda r: r[0], cutoff('klines'))
        # A response captured before the close cannot become a complete bar
        # merely because another request or the X poll finished after it.
        bars = [(t, r) for t, r in bars if int(r[6]) < cutoff('klines')]
        tail = bars[-25:]
        if len(tail) != 25 or any(b[0]-a[0] != HOUR for a, b in zip(tail, tail[1:])):
            raise ValueError('incomplete price window')
        last = tail[-1][1]
        if not 0 <= now_ms-(int(last[6])+1) <= HOUR+300_000:
            raise ValueError('stale price')
        before = number(tail[-2][1][4])
        baseline = statistics.median(number(r[7]) for _, r in tail[:-1])
        if before <= 0 or baseline <= 0:
            raise ValueError('invalid price/volume')
        f.update(price=number(last[4]), price_1h_pct=(number(last[4])/before-1)*100,
                 volume_multiple_24h_median=number(last[7])/baseline,
                 price_window_end_ms=int(last[6])+1)
    except (KeyError, ValueError, TypeError, IndexError):
        issues.append('price_window_unavailable')
    for name, field, output, scale in [
        ('oi', 'sumOpenInterest', 'oi_1h_pct', 100),
        ('top_positions', 'longAccount', 'top_long_change_pp', 100),
    ]:
        try:
            rows = ordered(bodies[name], lambda r: r['timestamp'], cutoff(name))
            t, row = rows[-1]
            if not 0 <= now_ms-t <= 90*60_000:
                raise ValueError('stale position statistics')
            previous = dict(rows).get(t-HOUR)
            if previous is None:
                raise ValueError('missing exact hour')
            a, b = number(previous[field]), number(row[field])
            if name == 'oi':
                if min(a, b) <= 0:
                    raise ValueError('invalid OI')
                change = (b/a-1)*scale
                f['oi_contract_units'] = b
            else:
                if not 0 <= min(a, b) <= max(a, b) <= 1:
                    raise ValueError('invalid share')
                change = (b-a)*scale
                f['top_long_share_pct'] = b*100
            f[output] = change
            f[name+'_at_ms'] = t
        except (KeyError, ValueError, TypeError, IndexError):
            issues.append(name+'_unavailable')
    try:
        m = bodies['mark']
        if m['symbol'] != 'INITUSDT' or not 0 <= now_ms-int(m['time']) <= 300_000:
            raise ValueError('stale mark')
        f['indicated_funding_pct'] = number(m['lastFundingRate'])*100
        f['funding_next_ms'] = int(m['nextFundingTime'])
    except (KeyError, ValueError, TypeError):
        issues.append('mark_unavailable')
    try:
        r = bodies['rpc']['result']
        if r['node_info']['network'] != 'interwoven-1':
            raise ValueError('wrong network')
        tip = milliseconds(r['sync_info']['latest_block_time'])
        height = int(r['sync_info']['latest_block_height'])
        if tip > now_ms or height <= 0:
            raise ValueError('invalid chain tip')
        f.update(chain_height=height, chain_tip_ms=tip,
                 chain_tip_age_seconds=(now_ms-tip)/1000,
                 chain_endpoint_state='fresh' if now_ms-tip <= 600_000 else 'stale')
    except (KeyError, ValueError, TypeError):
        issues.append('chain_endpoint_unavailable')
    return f, issues


def conditions(f):
    result = {}
    if all(k in f for k in ['price_1h_pct', 'volume_multiple_24h_median']):
        if abs(f['price_1h_pct']) >= 10 and f['volume_multiple_24h_median'] >= 2:
            result['price_volume'] = '完整1小时涨跌≥10%，成交额≥前24小时中位数2倍'
    if f.get('oi_1h_pct', -math.inf) >= 10:
        result['oi_expansion'] = '合约张数OI在1小时增加≥10%；不能识别具体大户'
    if abs(f.get('top_long_change_pp', 0)) >= 5:
        result['top_position_shift'] = '大户群体多仓占比1小时变化≥5个百分点'
    if f.get('indicated_funding_pct', 0) <= -0.1 and f.get('price_1h_pct', -math.inf) >= 5:
        result['squeeze_risk'] = '指示资金费率≤-0.1%且1小时上涨≥5%；仅为挤空风险线索'
    if f.get('chain_endpoint_state') == 'stale':
        result['stale_chain_endpoint'] = '官方公共节点返回旧块；单一服务商，不代表全网独立确认'
    return result


def transition(previous, current, now_ms, *, cooldown_ms=6*HOUR):
    """Alert on a new episode, at most once per condition per six hours."""
    emitted = dict(previous.get('last_emitted_ms', {}))
    active = set(previous.get('active', []))
    alerts = []
    for key, message in sorted(current.items()):
        if key not in active and now_ms-emitted.get(key, -10**20) >= cooldown_ms:
            alerts.append({'kind': key, 'message': message})
            emitted[key] = now_ms
    return {'active': sorted(current), 'last_emitted_ms': emitted}, alerts
