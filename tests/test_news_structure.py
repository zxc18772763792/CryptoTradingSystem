from datetime import datetime,timezone
import sqlite3
import pytest
from core.research.news_structure import Extractor,LibraryWriter,SCHEMA,clocks,utc,digest

@pytest.fixture
def extractor():return Extractor(['BTC','ETH','SOL','ABC','FOO','SKYAI','BANK','OPEN'])

def test_conditional_liquidation_is_not_an_observed_event(extractor):
    c=extractor.extract('If BTC falls below $60,000, longs could be liquidated.','news')['claims']
    assert next(x for x in c if x['event_type']=='realized_liquidation')['assertion']=='conditional'

def test_explicit_denial_is_preserved_not_a_hack_signal(extractor):
    c=extractor.extract('SOL 并未被攻击，网络运行正常。','news')['claims'][0]
    assert c['assertion']=='negated' and c['phase']=='denied_or_not_completed'

def test_scheduled_unlock_keeps_event_date_separate(extractor):
    r=extractor.extract('$ABC 将于2026-11-01 08:00 UTC解锁。','news')
    assert r['claims'][0]['phase']=='scheduled'
    assert any(x['resolved_utc']=='2026-11-01T08:00:00.000000Z' for x in r['temporal_mentions'])

def test_publisher_footer_does_not_make_incident_a_bitcoin_event(extractor):
    c=extractor.extract('$ABC was hacked - Bitcoin World','news')['claims'][0]
    assert {x['symbol'] for x in c['assets']}=={'ABC'}

def test_withdrawal_suspension_is_not_a_whale_withdrawal(extractor):
    r=extractor.extract('Binance suspends SOL withdrawals while wallet maintenance runs.','news')
    assert 'exchange_flow' not in {c['event_type'] for c in r['claims']}
    assert 'exchange_operation' in {c['event_type'] for c in r['claims']}

def test_dollar_numbers_and_quote_currency_are_not_assets(extractor):
    assert extractor.assets('$100M in USDT')==[]
    assert {x['symbol'] for x in extractor.assets('SOL/USDT')}=={'SOL'}

def test_asset_evidence_does_not_cross_clauses(extractor):
    r=extractor.extract('BTC is quiet; $ABC was hacked.','news')
    assert {a['symbol'] for a in r['claims'][0]['assets']}=={'ABC'}

def test_multiline_posts_keep_first_line_and_unicode_offsets(extractor):
    text='🟢 $ABC was hacked\n新的消息：SOL将上线Binance'
    r=extractor.extract(text,'x')
    assert any(c['event_type']=='security_incident' for c in r['claims'])
    for c in r['claims']:
        assert text[c['start']:c['end']]==c['evidence']
        assert text[c['trigger_start']:c['trigger_end']]==c['trigger']
        for a in c['assets']:assert text[a['start']:a['end']]==a['evidence']

@pytest.mark.parametrize('text,value,unit',[('$2.5M','2500000.0','USD'),('200万美元','2000000','USD'),('10 BTC','10','BTC'),('0.25%','0.25','%')])
def test_units_keep_currency_and_exact_decimal(extractor,text,value,unit):
    q=extractor.extract(text,'news')['quantities'][0]
    assert q['value_decimal']==value and q['unit']==unit

def test_no_timezone_guess_or_eight_hour_shift():
    assert utc('2026-10-25 08:00:00') is None
    assert utc('2026-10-25T08:00:00+08:00')=='2026-10-25T00:00:00.000000Z'
    assert utc('2026-10-25 08:00:00',declared_naive_utc=True)=='2026-10-25T08:00:00.000000Z'

def test_new_extraction_never_becomes_old_forward_evidence():
    r=clocks({'published_at':'2026-01-01T00:00:00Z','seen_at':'2026-01-01T00:01:00Z','edited_at':'2026-01-02T00:00:00Z'},'2026-10-25T00:00:00Z')
    assert r['decision_available_at']=='2026-10-25T00:00:00.000000Z'
    assert r['retrospective_observed_at']=='2026-01-02T00:00:00.000000Z'

def test_backfill_without_receipt_has_only_retrospective_proxy():
    r=clocks({'published_at':'2026-01-01T00:00:00Z'},'2026-10-25T00:00:00Z')
    assert r['retrospective_observed_at'] is None and r['historical_proxy_at']

def test_unknown_cashtag_is_retained_with_resolution_status(extractor):
    a=extractor.assets('$UNKNOWNNEW was hacked')
    assert a[0]['symbol']=='UNKNOWNNEW' and a[0]['kind']=='cashtag_unresolved'

def test_scaled_token_is_not_misread_as_dollar_amount():
    e=Extractor(['1000PEPE']);r=e.extract('$1000PEPE was hacked','news')
    assert r['claims'][0]['assets'][0]['symbol']=='1000PEPE'
    assert r['quantities']==[]

def test_currency_prefix_wins_over_following_position_asset(extractor):
    q=extractor.extract('$70 million BTC long position was liquidated','news')['quantities'][0]
    assert q['unit']=='USD' and q['value_decimal']=='70000000'

def test_hacked_social_account_is_not_protocol_security(extractor):
    c=extractor.extract('Instagram account hacked to promote a Solana token','news')['claims'][0]
    assert c['asset_role']=='lure_or_social_network_context'

def test_weekly_and_daily_etf_disclosures_are_separate(extractor):
    c=extractor.extract('Bitcoin spot ETF net inflow last week was $100M','news')['claims'][0]
    assert c['reporting_window']=='weekly'

def test_duplicates_and_changed_versions_preserve_occurrence_lineage(extractor):
    db=sqlite3.connect(':memory:');db.executescript(SCHEMA)
    w=LibraryWriter(db,extractor,'2026-10-25T00:00:00Z')
    r={'id':'x:one:1','platform':'x','source':'one','text':'$ABC was hacked','published_at':'2026-10-01T00:00:00Z'}
    a=w.add(r,origin='snapshot-a');b=w.add(r,origin='snapshot-b')
    changed={**r,'text':'$ABC was not hacked'};c=w.add(changed,origin='snapshot-c')
    assert a==b and a!=c
    assert db.execute('select count(*) from documents').fetchone()[0]==2
    assert db.execute('select count(*) from occurrences').fetchone()[0]==3
    assert w.counts['repeated_versions']==1
    assert not db.execute('pragma foreign_key_check').fetchall()
    assert db.execute('select text_sha256 from documents where version_id=?',(a,)).fetchone()[0]==digest(r['text'])
    db.close()
