"""Build, inspect and search isolated evidence-linked local news snapshots.

Does not collect from Telegram/X, call models, mutate news databases, schedule
jobs or interact with trading. Existing output snapshots are never overwritten.
"""
from __future__ import annotations
import argparse
import json
import os
import sqlite3
import sys
import uuid
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from core.research.news_structure import Extractor,LibraryWriter,SCHEMA,VERSION,digest,utc,read_only,units,COMPILED

def dump(path,value):
    with path.open('x',encoding='utf-8') as stream:json.dump(value,stream,indent=2,ensure_ascii=False,allow_nan=False)

def protect_output(folder):
    if os.name!='nt':
        folder.mkdir(parents=True,exist_ok=True);folder.chmod(0o700);return
    import ctypes
    drive=Path(folder.resolve().anchor)
    fs=ctypes.create_unicode_buffer(64)
    ok=ctypes.windll.kernel32.GetVolumeInformationW(str(drive),None,0,None,None,None,fs,len(fs))
    if not ok or fs.value.upper()!='NTFS':raise RuntimeError('Private research output requires NTFS on Windows')
    from scripts.telegram_desktop_bridge import protect_directory
    protect_directory(folder)

def build(args):
    folder=args.output_dir.resolve();protect_output(folder)
    target=folder/'library.sqlite'
    if target.exists():raise RuntimeError('Snapshot exists; use a new output directory')
    lock=folder/'build.lock'
    with lock.open('x',encoding='utf-8') as stream:stream.write(str(os.getpid()))
    stage=folder/('library.building.'+uuid.uuid4().hex+'.sqlite')
    at=datetime.now(timezone.utc).isoformat()
    universe=json.loads(args.market_manifest.read_text(encoding='utf-8'))
    extractor=Extractor([r['symbol'] for r in universe])
    db=sqlite3.connect(stage);db.executescript(SCHEMA)
    writer=LibraryWriter(db,extractor,at)
    manifest={'started_at':at,'extractor':VERSION,'code_sha256':digest((ROOT/'core/research/news_structure.py').read_bytes()),'inputs':[],'deferred_tails':[]}
    def add(record,origin,key=None):
        result=writer.add(record,origin=origin,origin_key=key)
        if writer.counts['input_occurrences']%10000==0:
            db.commit();print(json.dumps(writer.counts),flush=True)
        return result
    try:
        source=read_only(args.base_corpus);source.execute('BEGIN')
        n=0
        for r in source.execute('SELECT * FROM messages ORDER BY id'):
            add(dict(r),str(args.base_corpus));n+=1
        manifest['inputs'].append({'file':str(args.base_corpus),'sha256':digest(args.base_corpus.read_bytes()),'rows':n,'kind':'frozen_canonical_snapshot'})
        # Legacy labels stay separate, with explicit grounding and linkage limits.
        for r in source.execute('SELECT * FROM structured ORDER BY id'):
            parent=db.execute('SELECT text,platform FROM documents WHERE document_key=? ORDER BY rowid LIMIT 1',(r['message_id'],)).fetchone()
            symbol=str(r['symbol'] or '').upper().removesuffix('USDT')
            if not parent:grounding='no_parent_reference'
            else:
                scope=next(iter(units(parent[0],parent[1])),(0,0,''))[2] if parent[1]=='news' else parent[0]
                grounding='literal_asset_evidence' if symbol in {a['symbol'] for a in extractor.assets(scope)} else ('macro_mapping_requires_review' if symbol=='BTC' and COMPILED['macro_policy'].search(scope) else 'unsupported_asset_in_visible_scope')
            db.execute('INSERT INTO legacy_labels VALUES(?,?,?,?,?,?,?,?,?)',(r['id'],r['message_id'],r['symbol'],r['event_type'],r['event_ts'],r['labelled_at'],r['evidence_json'],r['model_source'],grounding))
        source.rollback();source.close();db.commit()
        if args.news_db:
            source=read_only(args.news_db);source.execute('BEGIN');n=0
            for r in source.execute('SELECT id,source,title,content,url,published_at,fetched_at,payload FROM news_raw ORDER BY id'):
                payload=json.loads(r['payload'] or '{}');text=str(r['title'] or '')+'\n'+str(r['content'] or '')
                from_snapshot={'id':'news:'+str(r['id']),'platform':'news','source':r['source'],'provider':payload.get('provider') or 'unknown',
                               'published_at':utc(r['published_at'],declared_naive_utc=True),'seen_at':utc(r['fetched_at'],declared_naive_utc=True),
                               'edited_at':None,'provenance':'observed_news','text':text,'meta_json':json.dumps({'title':r['title'],'public_url':r['url'],'raw_id':r['id'],'naive_timezone_declared_by_storage':'UTC'},ensure_ascii=False)}
                # Credentials in any raw payload are never imported. Keep visible text as-is.
                add(from_snapshot,str(args.news_db),str(r['id']));n+=1
            source.rollback();source.close();manifest['inputs'].append({'file':str(args.news_db),'rows':n,'kind':'one_read_transaction_live_news'})
        if args.channel_snapshot:
            for p in sorted(args.channel_snapshot.glob('*_history.json')):
                blob=p.read_bytes();archive=json.loads(blob)
                for r in archive['posts']:
                    add({'id':f'tgstudy:{r["source"]}:{r["message_id"]}','platform':'telegram_channel','source':r['source'],'provider':'telegram',
                         'published_at':r['published_at'],'seen_at':archive['coverage'].get('collected_until'),'edited_at':r.get('edited_at'),
                         'provenance':'historical_snapshot','author_key':r.get('author_key'),'text':r['text'],'text_sha':r['text_sha256'],
                         'meta_json':json.dumps({k:r.get(k) for k in ['forwarded','role','has_media','reply_to_id']})},str(p),str(r['message_id']))
                manifest['inputs'].append({'file':str(p),'sha256':digest(blob),'rows':len(archive['posts']),'kind':'joined_channel_surviving_history'})
            p=args.channel_snapshot/'labels_before_prices.json'
            if p.exists():
                blob=p.read_bytes();count=0;unmatched=0
                for r in json.loads(blob):
                    if r['kind'] not in ['market_call','limit_call']:continue
                    key=f'tgstudy:{r["source"]}:{r["message_id"]}'
                    v=db.execute('SELECT version_id FROM documents WHERE document_key=? AND text_sha256=?',(key,r['text_sha256'])).fetchone()
                    if not v:unmatched+=1;continue
                    db.execute('INSERT INTO reviewed_plans VALUES(?,?,?,?,?,?,?,?,?,?)',(v[0],str(p),r['kind'],r['symbols'][0],r['side'],r['entry_limit'],r['stop'],json.dumps(r['targets']),int(bool(r['edited_at'])),digest(json.dumps(r,sort_keys=True,ensure_ascii=False))))
                    count+=1
                manifest['reviewed_signal_labels']={'matched':count,'unmatched':unmatched,'sha256':digest(blob)}
        if args.x_archive_dir:
            for p in sorted(args.x_archive_dir.glob('*.jsonl')):
                blob=p.read_bytes();lines=blob.splitlines();n=0
                for i,line in enumerate(lines):
                    if not line.strip():continue
                    try:r=json.loads(line)
                    except ValueError:
                        if i==len(lines)-1 and not blob.endswith(b'\n'):
                            manifest['deferred_tails'].append({'file':str(p),'line':i+1});continue
                        raise RuntimeError('Malformed archive line; refusing silent data loss') from None
                    add({'id':f'x:{r.get("handle")}:{r.get("tweet_id")}','platform':'x','source':r.get('handle'),'provider':'x','published_at':r.get('created_at'),
                         'seen_at':r.get('first_seen_at'),'edited_at':None,'provenance':r.get('provenance','unknown'),'author_key':digest(str(r.get('handle')))[:20],
                         'text':r.get('text',''),'meta_json':json.dumps({k:r.get(k) for k in ['is_retweet','is_quote','in_reply_to_user','author','image_count','has_video']})},str(p),str(i+1));n+=1
                manifest['inputs'].append({'file':str(p),'sha256':digest(blob),'rows':n,'kind':'existing_x_archive_no_network'})
        db.commit()
        # A batch is only usable when extraction has completed; never backdate
        # today's labels to the beginning of a potentially long build.
        completed=utc(datetime.now(timezone.utc).isoformat())
        db.execute("UPDATE documents SET extracted_at=?,decision_available_at=MAX(COALESCE(published_at,''),COALESCE(first_seen_at,''),COALESCE(edited_at,''),?)",(completed,completed))
        manifest['extraction_completed_at']=completed
        manifest['writer_counts']=writer.counts
        for row in universe:db.execute('INSERT OR REPLACE INTO symbol_map VALUES(?,?,?,?)',(row['symbol'],row.get('native_symbol'),row.get('status'),str(args.market_manifest)))
        manifest['tables']={name:db.execute('SELECT count(*) FROM '+name).fetchone()[0] for name in ['documents','occurrences','claims','claim_assets','claim_scopes','quantities','temporal_mentions','contract_mentions','symbol_map','legacy_labels','reviewed_plans','quality_issues']}
        manifest['platforms']=[list(r) for r in db.execute('SELECT platform,count(*),avg(length(text)) FROM documents GROUP BY platform')]
        manifest['claims_by_type']=[list(r) for r in db.execute('SELECT event_type,assertion,phase,count(*) FROM claims GROUP BY event_type,assertion,phase')]
        manifest['legacy_grounding']=[list(r) for r in db.execute('SELECT asset_grounding,count(*) FROM legacy_labels GROUP BY asset_grounding')]
        manifest['quality_counts']=[list(r) for r in db.execute('SELECT code,count(*) FROM quality_issues GROUP BY code')]
        manifest['completed_at']=datetime.now(timezone.utc).isoformat()
        db.execute('INSERT INTO build_meta VALUES(?,?)',('manifest',json.dumps(manifest,ensure_ascii=False)));db.commit()
        assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
        assert not db.execute('PRAGMA foreign_key_check').fetchall()
        db.close()
        if target.exists():raise RuntimeError('Output appeared concurrently; preserving staged snapshot')
        os.replace(stage,target)
        manifest['database_sha256']=digest(target.read_bytes());dump(folder/'build_manifest.json',manifest)
        print(json.dumps({'status':'complete','tables':manifest['tables'],'writer_counts':writer.counts}),flush=True)
    finally:
        try:db.close()
        finally:lock.unlink(missing_ok=True)

def inspect(args):
    db=read_only(args.db)
    try:
        if args.command=='stats':
            manifest=json.loads(db.execute('SELECT value_json FROM build_meta WHERE key=?',('manifest',)).fetchone()[0])
            print(json.dumps({k:manifest[k] for k in ['tables','platforms','legacy_grounding','quality_counts'] if k in manifest},ensure_ascii=False,indent=2));return
        params=[];filters=[]
        if args.symbol:filters.append('a.symbol=?');params.append(args.symbol.upper())
        if args.event_type:filters.append('c.event_type=?');params.append(args.event_type)
        if args.phase:filters.append('c.phase=?');params.append(args.phase)
        if args.assertion:filters.append('c.assertion=?');params.append(args.assertion)
        fts=''
        if args.text:
            if len(args.text)>=3:
                fts=' JOIN document_fts ON document_fts.version_id=d.version_id';filters.append('document_fts MATCH ?');params.append('"'+args.text.replace('"','""')+'"')
            else:filters.append('d.text LIKE ?');params.append('%'+args.text+'%')
        if args.asset_role:filters.append('s.asset_role=?');params.append(args.asset_role)
        if args.reporting_window:filters.append('s.reporting_window=?');params.append(args.reporting_window)
        query='SELECT DISTINCT d.document_key,d.source,d.platform,d.published_at,d.first_seen_at,d.provenance,c.event_type,c.assertion,c.phase,a.symbol,s.actor_role,s.asset_role,s.reporting_window,c.evidence,substr(d.text,1,400) AS text_preview FROM documents d'+fts+' LEFT JOIN claims c ON c.version_id=d.version_id LEFT JOIN claim_assets a ON a.claim_id=c.claim_id LEFT JOIN claim_scopes s ON s.claim_id=c.claim_id'
        if filters:query+=' WHERE '+' AND '.join(filters)
        query+=' ORDER BY d.published_at DESC LIMIT ?';params.append(max(1,min(args.limit,100)))
        for row in db.execute(query,params):
            value=dict(row)
            if args.metadata_only:value.pop('evidence',None);value.pop('text_preview',None)
            print(json.dumps(value,ensure_ascii=False))
    finally:db.close()

def reextract(args):
    """Create a new derived snapshot; keep the input and every text version intact."""
    folder=args.output_dir.resolve();protect_output(folder);target=folder/'library.sqlite'
    if target.exists():raise RuntimeError('Snapshot exists; use a new output directory')
    lock=folder/'build.lock'
    with lock.open('x',encoding='utf-8') as stream:stream.write(str(os.getpid()))
    stage=folder/('library.building.'+uuid.uuid4().hex+'.sqlite')
    source=read_only(args.input_db);db=sqlite3.connect(stage)
    try:
        source.backup(db);source.close();db.execute('PRAGMA foreign_keys=ON')
        db.executescript('CREATE TABLE IF NOT EXISTS contract_mentions(version_id TEXT REFERENCES documents(version_id),start_offset INTEGER,end_offset INTEGER,address TEXT,network_hint TEXT,address_role TEXT);CREATE TABLE IF NOT EXISTS symbol_map(symbol TEXT PRIMARY KEY,native_symbol TEXT,status TEXT,mapping_source TEXT);')
        db.execute('CREATE TABLE IF NOT EXISTS claim_scopes(claim_id TEXT PRIMARY KEY REFERENCES claims(claim_id),actor_role TEXT,asset_role TEXT,reporting_window TEXT,role_rule TEXT)')
        db.executescript("DROP VIEW research_claims;CREATE VIEW research_claims AS SELECT c.*,d.document_key,d.platform,d.source,d.provider,d.published_at,d.first_seen_at,d.edited_at,d.provenance,d.decision_available_at,d.retrospective_observed_at,d.historical_proxy_at,d.receipt_lag_seconds,a.symbol,a.evidence AS asset_evidence,a.start_offset AS asset_start,a.end_offset AS asset_end,s.actor_role,s.asset_role,s.reporting_window FROM claims c JOIN documents d USING(version_id) JOIN claim_assets a USING(claim_id) LEFT JOIN claim_scopes s USING(claim_id);")
        for name in ['claim_assets','claim_scopes','claims','quantities','temporal_mentions','contract_mentions']:db.execute('DELETE FROM '+name)
        db.execute("DELETE FROM quality_issues WHERE code='event_without_grounded_asset'")
        universe=json.loads(args.market_manifest.read_text(encoding='utf-8'));extractor=Extractor([x['symbol'] for x in universe])
        started=datetime.now(timezone.utc).isoformat();writer=LibraryWriter(db,extractor,started);n=0
        original_manifest=json.loads(db.execute('SELECT value_json FROM build_meta WHERE key=?',('manifest',)).fetchone()[0])
        for version,text,platform in db.execute('SELECT version_id,text,platform FROM documents ORDER BY rowid'):
            writer.extract_version(version,text,platform);n+=1
            if n%10000==0:db.commit();print(json.dumps({'reextracted_versions':n}),flush=True)
        for row in universe:db.execute('INSERT OR REPLACE INTO symbol_map VALUES(?,?,?,?)',(row['symbol'],row.get('native_symbol'),row.get('status'),str(args.market_manifest)))
        completed=utc(datetime.now(timezone.utc).isoformat())
        db.execute("UPDATE documents SET extractor_version=?,extracted_at=?,decision_available_at=MAX(COALESCE(published_at,''),COALESCE(first_seen_at,''),COALESCE(edited_at,''),?)",(VERSION,completed,completed))
        manifest={'started_at':started,'completed_at':completed,'extraction_completed_at':completed,'extractor':VERSION,
                  'input_database':str(args.input_db),'input_database_sha256':digest(args.input_db.read_bytes()),
                  'code_sha256':digest((ROOT/'core/research/news_structure.py').read_bytes()),'original_manifest':original_manifest,
                  'raw_versions_and_occurrences_preserved':True,
                  'tables':{t:db.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in ['documents','occurrences','claims','claim_assets','claim_scopes','quantities','temporal_mentions','contract_mentions','symbol_map','legacy_labels','reviewed_plans','quality_issues']},
                  'platforms':[list(r) for r in db.execute('SELECT platform,count(*),avg(length(text)) FROM documents GROUP BY platform')],
                  'legacy_grounding':[list(r) for r in db.execute('SELECT asset_grounding,count(*) FROM legacy_labels GROUP BY asset_grounding')],
                  'quality_counts':[list(r) for r in db.execute('SELECT code,count(*) FROM quality_issues GROUP BY code')]}
        db.execute('INSERT OR REPLACE INTO build_meta VALUES(?,?)',('manifest',json.dumps(manifest,ensure_ascii=False)));db.commit()
        assert db.execute('PRAGMA integrity_check').fetchone()[0]=='ok' and not db.execute('PRAGMA foreign_key_check').fetchall()
        db.close()
        if target.exists():raise RuntimeError('Output appeared concurrently; preserving staged snapshot')
        os.replace(stage,target);manifest['database_sha256']=digest(target.read_bytes());dump(folder/'build_manifest.json',manifest)
        print(json.dumps({'status':'complete','tables':manifest['tables']}),flush=True)
    finally:
        source.close();db.close();lock.unlink(missing_ok=True)

def main():
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8')
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('build');p.add_argument('--base-corpus',type=Path,required=True);p.add_argument('--market-manifest',type=Path,required=True);p.add_argument('--output-dir',type=Path,required=True)
    p.add_argument('--news-db',type=Path);p.add_argument('--channel-snapshot',type=Path);p.add_argument('--x-archive-dir',type=Path)
    p=sub.add_parser('reextract');p.add_argument('--input-db',type=Path,required=True);p.add_argument('--market-manifest',type=Path,required=True);p.add_argument('--output-dir',type=Path,required=True)
    for name in ['stats','search']:
        p=sub.add_parser(name);p.add_argument('--db',type=Path,required=True)
        if name=='search':
            for option in ['symbol','event-type','phase','assertion','text','asset-role','reporting-window']:p.add_argument('--'+option)
            p.add_argument('--limit',type=int,default=10);p.add_argument('--metadata-only',action='store_true')
    args=parser.parse_args()
    if args.command=='build':build(args)
    elif args.command=='reextract':reextract(args)
    else:inspect(args)

if __name__=='__main__':main()
