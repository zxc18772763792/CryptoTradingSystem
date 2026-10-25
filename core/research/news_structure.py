"""Evidence-linked news extraction. Pure local rules; no network or trading hooks.

Offsets are Unicode code points in the unmodified stored text. Machine extraction
is not human verification. Event dates, publication, reception and extraction are
separate clocks. Unknown facts stay unknown.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

VERSION = "evidence-news-v3"
UTC = timezone.utc
MAJORS = {"BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "LINK", "AVAX", "TRX", "DOT", "LTC", "BCH", "SUI", "TON", "XMR", "ZEC", "DASH", "UNI", "AAVE", "NEAR", "APT", "ARB", "OP", "FIL", "ATOM", "ETC"}
STABLES = {"USD", "USDT", "USDC", "BUSD", "DAI", "EUR", "GBP", "CNY", "RMB"}
NAMES = {"Bitcoin": "BTC", "Ethereum": "ETH", "Solana": "SOL", "Dogecoin": "DOGE", "Chainlink": "LINK", "比特币": "BTC", "以太坊": "ETH", "索拉纳": "SOL", "狗狗币": "DOGE", "波场": "TRX"}
VENUES = re.compile(r"\b(?:binance|coinbase|upbit|bithumb|okx|bybit|bitget|kraken|gate(?:\.io)?)\b|币安|欧易|交易所", re.I)
WHALER = re.compile(r"\b(?:whale|wallet|address|transfer\w*)\b|鲸鱼|巨鲸|地址|钱包|转入|转出|转账", re.I)
PROMOTION = re.compile(r"presale|price prediction|sponsored|join.{0,20}(?:vip|premium)|预售|推广|广告|价格预测", re.I)
MODAL = re.compile(r"\b(?:could|might|may|rumou?r\w*|alleg\w*|unconfirmed|reportedly)\b|传闻|据传|网传|未经证实|可能|有望", re.I)
CONDITIONAL = re.compile(r"\bif\b|\bunless\b|如果|若(?:是|跌|涨|突破|未|不)|假设|清算强度|爆仓强度|liquidation intensity|liquidation price", re.I)
NEGATION = re.compile(r"\b(?:not|never|denied|denies|false|refuted|no evidence)\b|否认|不实|并未|未曾|尚未|未获|没有|未被|并非", re.I)
PLAN = re.compile(r"\b(?:will|plans?|planning|scheduled|upcoming|propos\w*|expected|set to|to be)\b|\bto\s+(?:list|delist|unlock|launch|burn|buy|suspend|resume)\b|将于|将会|将(?:上|下|解|开|启|增|推|停|恢)|计划|拟于|拟将|预计|即将|定于", re.I)
COMPLETE = re.compile(r"\b(?:has|have|had|was|were|completed|launched|bought|purchased|acquired|deposited|withdrawn|withdrew|approved|unlocked|hacked|exploited|liquidated|burned)\b|已(?:经|完成|于|被|上线|转|解锁|获批|购|买|销毁)|遭到|被盗|完成|获批|批准|累计|净流入|净流出", re.I)
RECAP = re.compile(r"\byears? ago\b|last year|recap|retrospect|此前.{0,8}(?:喊|分享)|回顾|复盘|多年前|一年前|we (?:called|shared).{0,80}(?:now|profit)|tp.{0,8}hit|获利|止盈达成", re.I)
RULES = {
    "listing": r"\blist(?:s|ed|ing)?\b|上线|上币|新增.{0,8}交易",
    "delisting": r"delist\w*|下架|退市|摘牌|终止交易",
    "security_incident": r"\bhack\w*|\bexploit\w*|security breach|攻击|被盗|漏洞|失窃",
    "token_unlock": r"\bunlock\w*|解锁",
    "supply_reduction": r"\bburn(?:ed|s|ing)?\b|\bbuyback\b|\brepurchase\w*|回购|销毁",
    "treasury_purchase": r"\b(?:bought|purchas\w*|acquir\w*|accumulat\w*)\b|买入|增持|购入|加仓",
    "exchange_flow": r"\b(?:deposit\w*|withdraw\w*|withdrew|transfer\w*)\b|转入|转出|转账|充值|提币",
    "etf_flow": r"\binflow\w*|\boutflow\w*|净流入|净流出|资金流入|资金流出",
    "etf_decision": r"\bapprov\w*|\breject\w*|\bdenied\b|获批|批准|拒绝",
    "whale_position": r"\b(?:open\w*|add\w*|increase\w*).{0,25}\b(?:long|short)\b|开.{0,5}[多空]|做多|做空",
    "realized_liquidation": r"\bliquidat\w*|爆仓|清算",
    "technology": r"\bmainnet\b|\bhard fork\b|\bupgrade\b|主网|升级|硬分叉",
    "macro_policy": r"rate cut|rate hike|federal reserve|\bFOMC\b|\bCPI\b|美联储|降息|加息|通胀",
    "exchange_operation": r"suspend\w*|halt\w*|resum\w*|暂停|恢复.{0,8}(?:充值|提币|交易)",
}
COMPILED = {k: re.compile(v, re.I) for k, v in RULES.items()}
QUANTITY = re.compile(r"(?P<currency>US\$|\$|USD\s*|CNY\s*|RMB\s*)?(?P<number>\d[\d,]*(?:\.\d+)?)\s*(?P<scale>billion|million|thousand|bn|trillion|[kmbt](?![A-Za-z])|万亿|亿|万)?\s*(?P<unit>美元|美金|人民币|元|USDT|USDC|USD|%|％|BTC|ETH)?", re.I)
SCALES = {"k": Decimal(1000), "thousand": Decimal(1000), "m": Decimal(10)**6, "million": Decimal(10)**6, "b": Decimal(10)**9, "bn": Decimal(10)**9, "billion": Decimal(10)**9, "t": Decimal(10)**12, "trillion": Decimal(10)**12, "万": Decimal(10000), "亿": Decimal(10)**8, "万亿": Decimal(10)**12}


def digest(value: str | bytes) -> str:
    return hashlib.sha256(value.encode("utf-8") if isinstance(value, str) else value).hexdigest()


def utc(value: Any, *, declared_naive_utc: bool = False) -> str | None:
    if not value:
        return None
    try:
        d = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if d.tzinfo is None:
            if not declared_naive_utc:
                return None
            d = d.replace(tzinfo=UTC)
        return d.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except (ValueError, TypeError, OverflowError):
        return None


def clocks(record: dict, extracted_at: str) -> dict:
    pub, seen, edit = (utc(record.get(k)) for k in ("published_at", "seen_at", "edited_at"))
    actual = max([t for t in (pub, seen, edit, utc(extracted_at)) if t], default=None)
    historical = max([t for t in (pub, seen, edit) if t], default=None) if pub and seen else None
    proxy = max([t for t in (pub, edit) if t], default=None) if pub else None
    lag = (datetime.fromisoformat(seen.replace("Z", "+00:00")) - datetime.fromisoformat(pub.replace("Z", "+00:00"))).total_seconds() if pub and seen else None
    return {"published_at": pub, "seen_at": seen, "edited_at": edit, "decision_available_at": actual,
            "retrospective_observed_at": historical, "historical_proxy_at": proxy, "receipt_lag_seconds": lag}


def units(text: str, platform: str) -> Iterable[tuple[int, int, str]]:
    # News headlines are the scope of the claim; avoid body link lists and footers.
    end = text.find("\n") if platform == "news" and "\n" in text else len(text)
    selected = text[:end]
    if platform == "news":
        footer = re.search(r"\s+[-|–]\s+(?:Bitcoin World|Cointelegraph|CoinDesk|Yahoo Finance)\s*$", selected, re.I)
        if footer:
            selected = selected[:footer.start()]
    seen = set()
    for m in re.finditer(r"[^\n。！？!?；;，]+[。！？!?；;，]?", selected):
        a, b = m.span()
        while a < b and selected[a].isspace():
            a += 1
        clause = selected[a:b]
        key = re.sub(r"\s+", " ", clause).strip().casefold()
        if key and key not in seen:
            seen.add(key)
            yield a, b, clause


def claim_scope(kind: str, text: str, assets: list[dict]) -> dict:
    """Textual roles, not a verified causal link to a tradable token."""
    actor='unspecified'
    if re.search(r'\bETF\b|交易所交易基金',text,re.I):actor='etf_fund'
    elif re.search(r'treasury|company|corporat|Metaplanet|Strive|BitMine|Strategy.{0,12}(?:acquir|bought|buy)|公司|财库|微策略',text,re.I):actor='corporate_or_treasury'
    elif WHALER.search(text):actor='whale_or_wallet'
    window='unspecified'
    if re.search(r'yesterday|\bdaily\b|昨日|昨天|单日',text,re.I):window='daily'
    elif re.search(r'last week|this week|\bweekly\b|上周|本周',text,re.I):window='weekly'
    elif re.search(r'last month|this month|\bmonthly\b|上月|本月',text,re.I):window='monthly'
    role='mentioned_asset_unverified'
    if kind=='etf_flow':role='fund_underlying_mention'
    elif kind=='exchange_flow':role='transferred_asset_mention'
    elif kind=='treasury_purchase':role='purchase_asset_mention_unverified'
    elif kind=='security_incident':
        if re.search(r'instagram|social.media|accounts? hacked|phishing|promote|钓鱼|社交账号',text,re.I):role='lure_or_social_network_context'
        elif re.search(r'bridge|intents|跨链桥',text,re.I):role='bridge_or_network_context'
        elif re.search(r'stolen|took away|盗走|转走',text,re.I):role='stolen_or_recovered_currency'
        elif any(re.search(re.escape(a['evidence'])+r'.{0,20}(?:protocol|platform|协议|平台).{0,20}(?:hacked|exploited|被攻击|被盗)',text,re.I) for a in assets):role='explicit_protocol_mention_unverified_identity'
    elif kind=='realized_liquidation':
        direct=any(re.search(re.escape(a['evidence'])+r'.{0,20}(?:long|short|多头|空头)|(?:long|short).{0,20}(?:of|in).{0,20}'+re.escape(a['evidence']),text,re.I) for a in assets)
        role='position_asset_mention' if direct else 'market_or_price_context'
        if re.search(r'crypto (?:longs|shorts)|in crypto|全网|全市场',text,re.I):role='market_aggregate_context'
    return {'actor_role':actor,'asset_role':role,'reporting_window':window,'role_rule':'text_scope_v1_not_identity_verification'}


class Extractor:
    def __init__(self, symbols: Iterable[str]):
        self.symbols = {str(s).upper().removesuffix("USDT") for s in symbols} | MAJORS
        self.symbols -= STABLES
        names = "|".join(re.escape(k) for k in NAMES)
        major = "|".join(sorted(MAJORS, key=len, reverse=True))
        self.names_re = re.compile(names, re.I)
        self.major_re = re.compile(r"(?<![A-Za-z_$])(?:" + major + r")(?![A-Za-z_])")

    def assets(self, text: str, offset: int = 0) -> list[dict]:
        mentions = []
        for pattern, kind in [(re.compile(r"(?<!\w)\$((?:[A-Za-z][A-Za-z0-9]{0,18}|1000[A-Za-z][A-Za-z0-9]{1,12}))(?!\w)"), "cashtag"),
                              (re.compile(r"(?<!\w)([A-Za-z0-9]{1,19})\s*/?\s*USDT\b", re.I), "pair")]:
            for m in pattern.finditer(text):
                symbol = m[1].upper()
                if kind=='pair' and '/' not in m[0] and any(ch.isspace() for ch in m[0]):
                    if symbol in {'IN','TO','OF','AND','WITH','ON','FOR','AT','THE','BY','ALL','IS','AN'} or (symbol not in self.symbols and not m[1].isupper()):continue
                if symbol not in STABLES:
                    mentions.append({"symbol": symbol, "start": offset+m.start(), "end": offset+m.end(), "evidence": m[0], "kind": kind if symbol in self.symbols else kind+'_unresolved'})
        for m in self.names_re.finditer(text):
            if m[0].isascii() and ((m.start() and text[m.start()-1].isalnum()) or (m.end()<len(text) and text[m.end()].isalnum())):
                continue
            symbol = next(v for k,v in NAMES.items() if k.casefold()==m[0].casefold())
            mentions.append({"symbol": symbol, "start": offset+m.start(), "end": offset+m.end(), "evidence": m[0], "kind": "literal_name"})
        for m in self.major_re.finditer(text):
            mentions.append({"symbol":m[0],"start":offset+m.start(),"end":offset+m.end(),"evidence":m[0],"kind":"major_ticker"})
        unique = {(m["symbol"],m["start"],m["end"]):m for m in mentions}
        return sorted(unique.values(),key=lambda m:(m["start"],m["symbol"]))

    def extract(self, text: str, platform: str) -> dict:
        claims, quantities, dates = [], [], []
        for start, end, clause in units(text, platform):
            mentions = self.assets(clause,start)
            asset_set = sorted({m["symbol"] for m in mentions})
            for kind, regex in COMPILED.items():
                match = regex.search(clause)
                if not match:
                    continue
                if kind == "listing" and (COMPILED['delisting'].search(clause) or not VENUES.search(clause)):
                    continue
                if kind=='token_unlock' and re.search(r'unlock.{0,12}wallet|解锁.{0,8}钱包|钱包.{0,8}解锁',clause,re.I):continue
                if kind == "treasury_purchase" and not re.search(r"treasur|institution|corporat|strategy|metaplanet|whale|财库|机构|公司|微策略|巨鲸",clause,re.I):
                    continue
                if kind == "exchange_flow" and (not WHALER.search(clause) or not VENUES.search(clause) or COMPILED['exchange_operation'].search(clause)):
                    continue
                if kind.startswith("etf_") and not re.search(r"\bETF\b|交易所交易基金",clause,re.I):
                    continue
                if kind == "whale_position" and not WHALER.search(clause):
                    continue
                assertion = "affirmed"
                if PROMOTION.search(clause): assertion = "promotion"
                elif CONDITIONAL.search(clause): assertion = "conditional"
                elif NEGATION.search(clause): assertion = "negated"
                elif MODAL.search(clause): assertion = "uncertain"
                elif RECAP.search(clause): assertion = "recap"
                if clause.rstrip().endswith(("?","？")): assertion = "question"
                phase = "scheduled" if PLAN.search(clause) else ("completed" if COMPLETE.search(clause) else "reported")
                if kind=='exchange_flow' and re.search(r'\b(?:transferred|transfers|deposits|withdraws)\b|刚刚.{0,20}(?:转入|转出)|已转',clause,re.I) and not PLAN.search(clause):phase='completed'
                if kind=='etf_flow' and re.search(r'net inflow|net outflow|净流入|净流出',clause,re.I) and not PLAN.search(clause):phase='completed'
                if kind=='security_incident' and not re.search(r'hacked|exploited|stolen|suffered.{0,12}attack|hack.{0,12}occurred|被盗|遭.{0,8}攻击|失窃',clause,re.I):phase='reported'
                if kind=='treasury_purchase' and re.search(r'\b(?:bought|purchased|purchases|acquired|acquires)\b|买入|购入|增持',clause,re.I) and not PLAN.search(clause):phase='completed'
                if kind=='whale_position' and re.search(r'\bopened\b|已.{0,8}开|新开',clause,re.I) and not PLAN.search(clause):phase='completed'
                if assertion == "negated": phase = "denied_or_not_completed"
                if assertion == "conditional":phase='conditional_not_realized'
                if assertion == "uncertain":phase='unconfirmed'
                # Link only assets within the event's own clause. No inherited article asset.
                local = [m for m in mentions if max(m['start']-(start+match.end()),(start+match.start())-m['end'],0)<=80]
                claims.append({"event_type":kind,"assertion":assertion,"phase":phase,"start":start,"end":end,"evidence":clause,
                               "trigger_start":start+match.start(),"trigger_end":start+match.end(),"trigger":match[0],
                               "assets":local,"asset_count":len({m['symbol'] for m in local}),
                               "venues":sorted({m[0].lower() for m in VENUES.finditer(clause)}),
                               "exact_story_key":digest(re.sub(r"\s+"," ",clause).strip().casefold()),
                               "extractor":VERSION,"review_status":"machine_extracted",**claim_scope(kind,clause,local)})
            for m in QUANTITY.finditer(clause):
                if not (m['currency'] or m['unit']): continue
                if any(a['kind'].startswith('cashtag') and start+m.start()<a['end'] and start+m.end()>a['start'] for a in mentions):continue
                unit = (m['unit'] or ('USD' if '$' in (m['currency'] or '') or 'USD' in (m['currency'] or '') else 'CNY')).upper()
                unit = {'美元':'USD','美金':'USD','人民币':'CNY','元':'CNY','％':'%'}.get(unit,unit)
                if m['currency'] and ('$' in m['currency'] or 'USD' in m['currency']):unit='USD'
                number = Decimal(m['number'].replace(',','')) * SCALES.get((m['scale'] or '').lower(),Decimal(1))
                role = 'percent' if unit=='%' else 'amount_unspecified'
                if re.search(r'\bTP\b|take.profit|target|目标|止盈',clause,re.I):role='target_or_claimed_performance'
                if re.search(r'liquidat|爆仓|清算',clause,re.I):role='liquidation_amount'
                elif re.search(r'inflow|outflow|流入|流出|deposit|withdraw|转入|转出|转账',clause,re.I):role='flow_amount'
                quantities.append({'start':start+m.start(),'end':start+m.end(),'evidence':m[0],'value_decimal':str(number),'unit':unit,'role':role})
            for m in re.finditer(r'20\d{2}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}月\d{1,2}日|\btomorrow\b|明日|明天|今日|今天',clause,re.I):
                dates.append({'start':start+m.start(),'end':start+m.end(),'evidence':m[0],'resolved_utc':None,'resolution':'calendar_or_relative_text_timezone_unproven'})
            for m in re.finditer(r'20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?\s*(?:UTC|Z|[+-]\d{2}:\d{2})',clause):
                exact=utc(re.sub(r'\s*UTC$', '+00:00',m[0]))
                if exact:dates.append({'start':start+m.start(),'end':start+m.end(),'evidence':m[0],'resolved_utc':exact,'resolution':'explicit_timezone'})
        return {'claims':claims,'quantities':quantities,'temporal_mentions':dates}


SCHEMA = '''
PRAGMA foreign_keys=ON;
CREATE TABLE documents(version_id TEXT PRIMARY KEY, document_key TEXT NOT NULL, platform TEXT NOT NULL, source TEXT, provider TEXT,
 published_at TEXT, first_seen_at TEXT, edited_at TEXT, provenance TEXT, author_key TEXT, text TEXT NOT NULL, text_sha256 TEXT NOT NULL,
 legacy_story_key TEXT, meta_json TEXT, extracted_at TEXT NOT NULL, extractor_version TEXT NOT NULL,
 decision_available_at TEXT, retrospective_observed_at TEXT, historical_proxy_at TEXT, receipt_lag_seconds REAL,
 UNIQUE(document_key,text_sha256));
CREATE TABLE occurrences(id INTEGER PRIMARY KEY,version_id TEXT NOT NULL REFERENCES documents(version_id),origin TEXT NOT NULL,
 origin_key TEXT NOT NULL, received_at TEXT, provenance TEXT, source_text_sha TEXT,published_at TEXT,edited_at TEXT,meta_json TEXT, UNIQUE(origin,origin_key,source_text_sha));
CREATE TABLE claims(claim_id TEXT PRIMARY KEY, version_id TEXT NOT NULL REFERENCES documents(version_id),event_type TEXT,assertion TEXT,phase TEXT,
 start_offset INTEGER,end_offset INTEGER,evidence TEXT,trigger_start INTEGER,trigger_end INTEGER,trigger TEXT,asset_count INTEGER,
 venues_json TEXT,exact_story_key TEXT,review_status TEXT);
CREATE TABLE claim_assets(claim_id TEXT NOT NULL REFERENCES claims(claim_id),symbol TEXT,start_offset INTEGER,end_offset INTEGER,evidence TEXT,mention_kind TEXT,
 PRIMARY KEY(claim_id,symbol,start_offset,end_offset));
CREATE TABLE claim_scopes(claim_id TEXT PRIMARY KEY REFERENCES claims(claim_id),actor_role TEXT,asset_role TEXT,reporting_window TEXT,role_rule TEXT);
CREATE TABLE quantities(version_id TEXT NOT NULL REFERENCES documents(version_id),start_offset INTEGER,end_offset INTEGER,evidence TEXT,value_decimal TEXT,unit TEXT,numeric_role TEXT);
CREATE TABLE temporal_mentions(version_id TEXT NOT NULL REFERENCES documents(version_id),start_offset INTEGER,end_offset INTEGER,evidence TEXT,resolved_utc TEXT,resolution TEXT);
CREATE TABLE contract_mentions(version_id TEXT NOT NULL REFERENCES documents(version_id),start_offset INTEGER,end_offset INTEGER,address TEXT,network_hint TEXT,address_role TEXT);
CREATE TABLE symbol_map(symbol TEXT PRIMARY KEY,native_symbol TEXT,status TEXT,mapping_source TEXT);
CREATE TABLE legacy_labels(id TEXT PRIMARY KEY,document_key TEXT,symbol TEXT,event_type TEXT,event_ts TEXT,labelled_at TEXT,evidence_json TEXT,model_source TEXT,asset_grounding TEXT);
CREATE TABLE reviewed_plans(version_id TEXT REFERENCES documents(version_id),origin TEXT,kind TEXT,symbol TEXT,side INTEGER,entry_limit REAL,stop REAL,targets_json TEXT,edited INTEGER,label_sha256 TEXT);
CREATE TABLE quality_issues(id INTEGER PRIMARY KEY,version_id TEXT,code TEXT,detail TEXT);
CREATE TABLE build_meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);
CREATE INDEX document_source_time ON documents(source,published_at);
CREATE INDEX document_key_versions ON documents(document_key,extracted_at);
CREATE INDEX claim_type_stage ON claims(event_type,assertion,phase);
CREATE INDEX asset_symbol ON claim_assets(symbol,claim_id);
CREATE INDEX story_claim ON claims(exact_story_key);
CREATE INDEX issue_code ON quality_issues(code);
CREATE VIRTUAL TABLE document_fts USING fts5(version_id UNINDEXED,text,tokenize='trigram');
CREATE VIEW research_claims AS SELECT c.*,d.document_key,d.platform,d.source,d.provider,d.published_at,d.first_seen_at,d.edited_at,d.provenance,
 d.decision_available_at,d.retrospective_observed_at,d.historical_proxy_at,d.receipt_lag_seconds,s.actor_role,s.asset_role,s.reporting_window,
 a.symbol,a.evidence AS asset_evidence,a.start_offset AS asset_start,a.end_offset AS asset_end
 FROM claims c JOIN documents d USING(version_id) JOIN claim_assets a USING(claim_id) LEFT JOIN claim_scopes s USING(claim_id);
'''


class LibraryWriter:
    def __init__(self, connection: sqlite3.Connection, extractor: Extractor, extracted_at: str):
        self.db, self.extractor, self.extracted_at = connection, extractor, utc(extracted_at)
        if not self.extracted_at: raise ValueError('Extraction time needs a timezone')
        self.counts = {'input_occurrences':0,'new_versions':0,'repeated_versions':0,'duplicate_input_rows':0}

    def add(self, record: dict, *, origin: str, origin_key: str | None = None) -> str:
        self.counts['input_occurrences'] += 1
        text = str(record.get('text') or '')
        text_sha = digest(text)
        version_id = digest(str(record['id'])+'\0'+text_sha)
        time = clocks(record,self.extracted_at)
        values = [version_id,record['id'],record['platform'],record.get('source'),record.get('provider'),time['published_at'],time['seen_at'],time['edited_at'],record.get('provenance'),record.get('author_key'),text,text_sha,record.get('story_key'),record.get('meta_json') or '{}',self.extracted_at,VERSION,time['decision_available_at'],time['retrospective_observed_at'],time['historical_proxy_at'],time['receipt_lag_seconds']]
        fresh = self.db.execute('INSERT OR IGNORE INTO documents VALUES ('+','.join('?' for _ in values)+')',values).rowcount == 1
        self.counts['new_versions' if fresh else 'repeated_versions'] += 1
        inserted = self.db.execute('INSERT OR IGNORE INTO occurrences(version_id,origin,origin_key,received_at,provenance,source_text_sha,published_at,edited_at,meta_json) VALUES(?,?,?,?,?,?,?,?,?)',
                         (version_id,origin,origin_key or record['id'],time['seen_at'],record.get('provenance'),text_sha,time['published_at'],time['edited_at'],record.get('meta_json') or '{}')).rowcount
        self.counts['duplicate_input_rows'] += int(not inserted)
        if not fresh:return version_id
        self.db.execute('INSERT INTO document_fts VALUES(?,?)',(version_id,text))
        def issue(code,detail):self.db.execute('INSERT INTO quality_issues(version_id,code,detail) VALUES(?,?,?)',(version_id,code,detail))
        if record.get('text_sha') and record['text_sha']!=text_sha:issue('input_text_hash_mismatch','Original declared hash differs; preserved as occurrence metadata.')
        if not time['published_at']:issue('missing_or_naive_publication','No publication-time historical proxy is allowed.')
        if not time['seen_at']:issue('missing_first_receipt','Historical snapshot only; not forward evidence.')
        if time['receipt_lag_seconds'] is not None and not -60<=time['receipt_lag_seconds']<=21600:issue('late_or_inconsistent_receipt','Retrospective observed-clock research must exclude or explicitly study this lag.')
        if not text.strip():issue('empty_visible_text','No text event is inferred from media.')
        if self.db.execute('SELECT count(*) FROM documents WHERE document_key=?',(record['id'],)).fetchone()[0]>1:issue('multiple_text_versions','Do not replace older text or backdate the new version.')
        self.extract_version(version_id,text,record['platform'])
        return version_id

    def extract_version(self,version_id: str,text: str,platform: str) -> None:
        """Populate only derived tables; useful for an isolated new extractor snapshot."""
        extracted = self.extractor.extract(text,platform)
        for n,c in enumerate(extracted['claims']):
            claim_id=digest(version_id+'\0'+str(n)+'\0'+c['event_type'])
            self.db.execute('INSERT INTO claims VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(claim_id,version_id,c['event_type'],c['assertion'],c['phase'],c['start'],c['end'],c['evidence'],c['trigger_start'],c['trigger_end'],c['trigger'],c['asset_count'],json.dumps(c['venues']),c['exact_story_key'],c['review_status']))
            self.db.execute('INSERT INTO claim_scopes VALUES(?,?,?,?,?)',(claim_id,c['actor_role'],c['asset_role'],c['reporting_window'],c['role_rule']))
            for a in c['assets']:
                self.db.execute('INSERT INTO claim_assets VALUES(?,?,?,?,?,?)',(claim_id,a['symbol'],a['start'],a['end'],a['evidence'],a['kind']))
            if not c['assets']:self.db.execute('INSERT INTO quality_issues(version_id,code,detail) VALUES(?,?,?)',(version_id,'event_without_grounded_asset',c['event_type']))
        for q in extracted['quantities']:
            self.db.execute('INSERT INTO quantities VALUES(?,?,?,?,?,?,?)',(version_id,q['start'],q['end'],q['evidence'],q['value_decimal'],q['unit'],q['role']))
        for t in extracted['temporal_mentions']:
            self.db.execute('INSERT INTO temporal_mentions VALUES(?,?,?,?,?,?)',(version_id,t['start'],t['end'],t['evidence'],t['resolved_utc'],t['resolution']))
        for m in re.finditer(r'(?<![A-Za-z0-9])0x[0-9a-fA-F]{40}(?![A-Za-z0-9])|(?<![A-Za-z0-9])[1-9A-HJ-NP-Za-km-z]{32,44}(?![A-Za-z0-9])',text):
            network='evm_network_unresolved' if m[0].startswith('0x') else 'base58_network_unresolved'
            self.db.execute('INSERT INTO contract_mentions VALUES(?,?,?,?,?,?)',(version_id,m.start(),m.end(),m[0],network,'address_not_proven_token_or_wallet'))


def read_only(path: Path) -> sqlite3.Connection:
    connection=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=30)
    connection.row_factory=sqlite3.Row
    return connection
