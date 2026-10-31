"""Pure Korean research annotations; no orders, paper trades or runtime activation.

The complete policy has no claimed historical performance: old reason/wallet
and classification clocks are incomplete. The price-only precursor is separate.
"""
from datetime import datetime,timedelta,timezone
import math

VERSION='korea-factor-research-policy-v1-20261029'


def utc(value):
    d=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('timezone_required')
    return d.astimezone(timezone.utc)


def assess(c,decision_at_utc):
    def out(state,reason,**extra):
        return {'version':VERSION,'mode':'shadow_research_only','state':state,'reason':reason,
                'creates_order':False,'creates_paper_trade':False,'activates_strategy':False,
                'performance_claim':None,**extra}
    if c.get('provenance')!='live':
        return out('retrospective_only','Reconstructed evidence is not a new forward decision.')
    if c.get('venue') not in ['upbit','bithumb'] or c.get('kind') not in ['caution','listing_krw']:
        return out('observe','Unsupported event family.')
    if c.get('asset_mapping_verified') is not True:return out('defer','Asset mapping unverified.')
    try:
        now=utc(decision_at_utc);pub=utc(c['published_at_utc']);received=utc(c['received_at_utc'])
        classified=utc(c['classified_at_utc']);market=utc(c['market_received_at_utc']);end=utc(c['market_window_end_utc'])
    except (KeyError,ValueError,TypeError):return out('defer','Source or classification clocks missing/naive.')
    planned=pub.replace(hour=0,minute=0,second=0,microsecond=0)+timedelta(days=1,minutes=5)
    close=planned-timedelta(minutes=5)
    if max(pub,received,classified,market,end)>now or received>classified:
        return out('defer','Future evidence or inconsistent chronology.')
    if now<planned:return out('observe','Wait for the common delayed decision window.')
    if now>planned+timedelta(minutes=5):return out('missed','Entry window missed; no backfill.')
    if end!=close or end>market or now-market>timedelta(minutes=10):
        return out('defer','Notice-day completed market data not freshly received.')
    if c.get('same_episode_position_open'):return out('observe','No same-episode stacking.')
    if c.get('pre_entry_funding_observed') is not True or c.get('fee_inputs_known') is not True:
        return out('defer','Pre-entry cost inputs missing; future funding is not known.')
    try:fund_received=utc(c['pre_entry_funding_received_at_utc'])
    except (KeyError,ValueError,TypeError):return out('defer','Pre-entry funding receipt clock missing.')
    if fund_received>now or now-fund_received>timedelta(minutes=10):
        return out('defer','Pre-entry funding receipt future/stale.')
    for claim in c.get('updates',[]):
        if not (claim.get('same_asset') is True and claim.get('same_incident') is True
                and claim.get('actor_role') in ['issuer','foundation']):continue
        try:at=utc(claim['received_at_utc']);reviewed=utc(claim['reviewed_at_utc'])
        except (KeyError,ValueError,TypeError):return out('defer','Material update review clocks missing.')
        if max(at,reviewed)>now or reviewed<at:return out('defer','Material update unavailable at decision.')
        if claim.get('stage') in ['patch_drafted','audit_pending','backing_commitment','audit_completed',
                                 'backing_transferred','chain_restarted','wallet_resumed']:
            return out('risk_review','Direct progress changes the thesis; no automatic exit.',update_stage=claim['stage'])
    if c['kind']=='listing_krw':return out('reference_only','Separate KRW fade reference; added heat/cooling filters failed.')
    if c.get('reason') in ['security_incident','network_or_technical']:
        return out('incident_observation','Security/network cases require separate evidence review.')
    if c.get('reason') not in ['disclosure_or_supply','project_or_team_issue']:
        return out('defer','No timely reviewed business/disclosure reason.')
    if c.get('transfer_status') not in ['working','deposit_only_suspended']:
        return out('defer','Wallet state unknown or withdrawals/both directions restricted.')
    try:wallet_received=utc(c['transfer_status_received_at_utc']);wallet_event=utc(c['transfer_status_event_at_utc'])
    except (KeyError,ValueError,TypeError):return out('defer','Wallet observation clocks missing.')
    if max(wallet_received,wallet_event)>now or min(wallet_received,wallet_event)<now-timedelta(minutes=10):
        return out('defer','Wallet evidence is future/stale; old announcement receipt is not current status.')
    try:
        value=float(c['relative_notice_day_return'])
        if not math.isfinite(value):raise ValueError('nonfinite')
    except (KeyError,TypeError,ValueError):return out('defer','Relative coin/BTC feature missing.')
    if value>=0:return out('observe','No relative weakness confirmation.')
    return out('candidate_for_shadow','Business/disclosure caution with relative weakness.',
               family='korea_caution_relative_weakness',direction_hypothesis='short',
               carry_sign_is_not_an_entry_filter=True)
