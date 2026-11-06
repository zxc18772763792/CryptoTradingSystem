from collections import Counter
import asyncio,json,sys
from pathlib import Path
import httpx
sys.path.insert(0,str(Path('reports/tracker_data_audit_20261105')))
import coinglass_exchange_replay as a

async def main():
 p=a.REPORT/'coinglass_exchange_replay.json'
 d=json.loads(p.read_text())
 states={k:json.loads((Path('data/research')/v/'tracker.json').read_text(encoding='utf-8-sig')) for k,v in a.TRACKERS.items()}
 fund=d['funding_api']
 prices={tuple(row['key']): row['bars'] for row in d['price_api']}
 jobs={}
 for symbol,value in fund.items():
  if isinstance(value,dict):
   rows=[r for r in d['funding_checks'] if r['symbol']==symbol]
   jobs[symbol]=('/fundingRate',{'symbol':symbol,'startTime':min(r['from_ms'] for r in rows)+1,'endTime':max(r['to_ms'] for r in rows),'limit':1000})
 for row in d['price_checks']:
  if row['status'] not in ('same minute','nearby minute'):
   key=tuple(row[k] for k in ('day','side','symbol','field'))
   minute=row['timestamp_ms']//60000*60000
   jobs[key]=('/klines',{'symbol':row['symbol'],'interval':'1m','startTime':minute-1800000,'endTime':minute+1800000,'limit':61})
 async with httpx.AsyncClient(timeout=httpx.Timeout(12,connect=8)) as client:
  replies=await a.query_all(client,jobs)
 for key,value in replies.items():
  if isinstance(value,list):
   if isinstance(key,str): fund[key]=value
   else: prices[key]=value
 price_checks=a.verify_prices(a.price_jobs(states),prices)
 for row in price_checks:
  key=tuple(row[k] for k in ('day','side','symbol','field'))
  if key not in jobs or not isinstance(prices[key],list): continue
  matches=[b[0] for b in prices[key] if float(b[3])<=row['ledger_price']<=float(b[2])]
  delta=min((round((t-row['timestamp_ms'])/60000,2) for t in matches),key=abs,default=None)
  row['nearest_match_minute_from_timestamp']=delta
  if row['status']=='within six minutes' and abs(delta or 0)>6:
   row['status']='within thirty minutes'
  print(row)
 d['funding_checks'],d['marked_books']=a.verify_funding(states,fund)
 d['price_checks']=price_checks
 d['price_api']=[{'key':list(k),'bars':v} for k,v in prices.items()]
 p.write_text(json.dumps(d,indent=2,allow_nan=False),encoding='utf-8')
 print('funding',Counter(r['status'] for r in d['funding_checks']))
 print('prices',Counter(r['status'] for r in d['price_checks']))
 for key in ('main','shadow'):
  books=[v for k,v in d['marked_books'].items() if k.endswith('/'+key)]
  print(key,len(books),{k:a.stats.mean(b[k] for b in books) for k in books[0]})

asyncio.run(main())
