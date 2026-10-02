import ast, asyncio, json, builtins
results=[]
def print(*items,**kwargs):
    results.append({'probe':str(items[0]),'observed':list(items[1:])})
from pathlib import Path
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from types import SimpleNamespace
logger = SimpleNamespace(**{k:(lambda *a,**kw:None) for k in ('debug','info','warning','error','critical')})
_log_task_exception=lambda _:None
def defs(path,names,methods=None):
    nodes=[]
    for n in ast.parse(Path(path).read_text(encoding='utf-8-sig')).body:
        if isinstance(n,(ast.ClassDef,ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in names:
            if methods is not None and isinstance(n,ast.ClassDef):
                n.body=[x for x in n.body if isinstance(x,(ast.FunctionDef,ast.AsyncFunctionDef)) and x.name in methods];n.bases=[]
            nodes.append(n)
    exec('from __future__ import annotations\n'+ast.unparse(ast.Module(body=nodes,type_ignores=[])),globals())
async def nothing(*a,**kw):return None
defs('core/exchanges/base_exchange.py',{'OrderSide','OrderType','OrderStatus','Order'})
defs('core/trading/order_manager.py',{'OrderRequest'})
defs('core/trading/position_manager.py',{'PositionSide','AmbiguousPositionError','Position','PositionManager'})
pm=PositionManager.__new__(PositionManager)
pm._positions={};pm._position_history=[];pm._scope='paper';pm._dirty=False
pm._persist_scope_state=lambda **kw:None;pm._trim_position_history=lambda:None;pm._notify_callbacks=nothing
position_manager=pm
defs('core/risk/risk_manager.py',{'RiskManager'},{'pre_trade_check','_is_daily_loss_halt_reason'})
rm=RiskManager.__new__(RiskManager)
rm._check_new_day=lambda:None;rm._add_alert=lambda **kw:None
rm._trading_halted=False;rm._daily_trades=0;rm.max_daily_trades=100
rm.max_open_positions=20;rm.max_leverage=3;rm._current_equity=1000
rm.max_position_size=.1;rm.max_gross_exposure_ratio=1
_position_manager=lambda:pm;risk_manager=rm
defs('core/trading/execution_engine.py',{'ExecutionEngine'},{'_execute_manual_order_single_in_active_mode','_strategy_lookup_value','_resolved_order_fill_qty','_order_status_value','_safe_nonnegative_float','_reconcile_local_positions_with_exchange','_sync_local_position_from_exchange','_canonical_symbol','_close_position_in_active_mode'})
e=ExecutionEngine.__new__(ExecutionEngine);e._current_trading_mode=lambda:'paper'
async def context(exchange,symbol,quantity,preferred_price):return 100,quantity*100
async def equity(**kw):return 1000
e._resolve_order_context=context;e._get_account_equity=equity;e._notify_callbacks=nothing
e._normalize_protection_levels=lambda **kw:(None,None)
account_manager=SimpleNamespace(is_enabled=lambda _:True,resolve_exchange=lambda *a:'binance')
seen=[]
async def governance(**kw):
    seen.append(('governance_allow_close',kw['allow_close']))
    return SimpleNamespace(allowed=True,trace_id='audit-only')
decision_engine=SimpleNamespace(evaluate_order_intent=governance)
async def capture(request):
    seen.append(('submitted_amount',request.amount,request.reduce_only,request.params['leverage']))
    return None
order_manager=SimpleNamespace(create_order=capture,get_last_error=lambda:'')
pm.open_position('binance','BTC/USDT',PositionSide.LONG,100,1,strategy='manual')
async def probe_manual():
    await e._execute_manual_order_single_in_active_mode(exchange='binance',symbol='BTC/USDT',side='sell',order_type='market',amount=100,price=100,leverage=100,stop_loss=None,take_profit=None,trailing_stop_pct=None,trailing_stop_distance=None,trigger_price=None,order_mode='normal',iceberg_parts=1,algo_slices=1,algo_interval_sec=0,account_id='main',reduce_only=False,strategy='manual')
asyncio.run(probe_manual())
print('REVERSE_BYPASS',json.dumps(seen))
print('SAME_ORDER_AS_ENTRY_ALLOWED',rm.pre_trade_check('BTC/USDT','sell','manual',1000,10000,100,1,False))
pm._positions={};pm._position_history=[]
a=pm.open_position('binance','BTC/USDT',PositionSide.LONG,100,1,strategy='A')
b=pm.open_position('binance','BTC/USDT',PositionSide.LONG,100,2,strategy='B')
e._current_trading_mode=lambda:'live';e.list_pending_live_order_intents=lambda **kw:[]
e._last_live_reconcile_at=None;e._live_reconcile_interval_seconds=12
e._live_reconcile_absence_counts={};e._live_reconcile_grace_seconds=20
e._live_reconcile_absence_min_age_seconds=600;e._live_reconcile_absence_threshold=3
async def positions():return [SimpleNamespace(symbol='BTC/USDT:USDT',amount=3,side='long',entry_price=100,current_price=100,unrealized_pnl=0,leverage=1)]
connector=SimpleNamespace(config=SimpleNamespace(default_type='swap'),get_positions=positions)
async def ensure(*a,**kw):return connector
e._ensure_exchange_connector=ensure
asyncio.run(e._reconcile_local_positions_with_exchange())
print('RECONCILE_DUPLICATION',a.quantity,b.quantity,'local_total',sum(p.quantity for p in pm.get_all_positions()),'venue_total',3)
pm._positions={};pm._position_history=[]
p=pm.open_position('binance','ETH/USDT',PositionSide.LONG,100,2,strategy='S')
first=pm.close_position('binance','ETH/USDT',110,1,'main','S').realized_pnl
second=pm.close_position('binance','ETH/USDT',120,1,'main','S').realized_pnl
print('CUMULATIVE_CLOSE',first,second,'sum_if_logged_verbatim',first+second,'correct_total',30)
ns={'__name__':'__main__'}
exec(compile(Path('core/accounting/pnl_decomposer.py').read_text(encoding='utf-8-sig'),'core/accounting/pnl_decomposer.py','exec'),ns)
pnl=ns['PnLDecomposer']();pnl.on_fill('BTC/USDT','buy',1,100,fee=2,slippage_cost=3);pnl.on_fill('BTC/USDT','sell',1,110,fee=1,slippage_cost=1)
print('ENTRY_COST_OMITTED',pnl.portfolio_breakdown(),'correct_net',3)
# Actual CLOSE signal method records cumulative realized PnL.
pm._positions={};pm._position_history=[]
p=pm.open_position('binance','ETH/USDT',PositionSide.LONG,100,2,strategy='S')
pm.close_position('binance','ETH/USDT',110,1,'main','S')
e._current_trading_mode=lambda:'paper'
e._resolve_strategy_trade_policy=lambda *a:{}
e._requires_strategy_position_isolation=lambda *a:True
async def existing(**kw):return pm.get_position(kw['exchange'],kw['symbol'],kw['account_id'],kw.get('strategy_name'))
e._resolve_existing_position=existing
e._close_limit_first_enabled=lambda *a:False
e._consume_paper_order_cost=lambda *a:{}
e._resolve_signal_trading_mode=lambda *a:'paper'
e._record_live_strategy_trade=nothing
async def costs(**kw):return {'fee_usd':0,'slippage_cost_usd':0}
e._resolve_execution_costs=costs
strategy_manager=SimpleNamespace(get_strategy_allocation=lambda *a:1)
recorded=[];rm.record_trade=lambda row:recorded.append(row)
async def filled(request):
    return Order(id='audit-close',symbol=request.symbol,side=request.side,type=request.order_type,price=120,amount=request.amount,filled=request.amount,remaining=0,cost=120*request.amount,status=OrderStatus.CLOSED,timestamp=datetime.now(timezone.utc),exchange='binance')
order_manager=SimpleNamespace(create_order=filled)
audit_logger=SimpleNamespace(log=nothing)
signal=SimpleNamespace(metadata={'account_id':'main'},symbol='ETH/USDT',strategy_name='S',price=120,strength=1,signal_type=SimpleNamespace(value='close_long'),stop_loss=None,take_profit=None,to_dict=lambda:{})
asyncio.run(e._close_position_in_active_mode(signal,PositionSide.LONG))
print('ENGINE_CLOSE_LOGGED_PNL',recorded[0]['pnl'],'correct_this_fill',20)

# Real-submit method, connectors and governance replaced entirely by in-memory stubs.
import re, time, uuid, math
defs('core/trading/order_manager.py',{'OrderManager'})
om=OrderManager();om._resolve_request_mode=lambda _: 'live'
async def om_gov(*a,**kw):return SimpleNamespace(allowed=True,trace_id='audit-only',reduce_only=False)
om._evaluate_order_governance=om_gov
om._governance_rejection_reason=lambda *a:''
async def create_fake(**kw):
    return Order(id='42',symbol=kw['symbol'],side=kw['side'],type=kw['order_type'],price=100,amount=kw['amount'],filled=0,remaining=kw['amount'],cost=0,status=OrderStatus.OPEN,timestamp=datetime.now(timezone.utc),exchange='binance')
cancelled=[]
def makeconnector(aid):
    async def cancel(order_id,symbol):cancelled.append((aid,order_id,symbol));return True
    return SimpleNamespace(create_order=create_fake,cancel_order=cancel)
conns={aid:makeconnector(aid) for aid in ('A','B')}
async def ensure_om(exchange_name,account_id=None):return conns[account_id]
om._ensure_exchange_connector=ensure_om
om._resolve_cached_exchange=lambda exchange_name,account_id=None:conns[account_id]
async def orders_and_cancel():
    for aid in ('A','B'):
        await om._create_real_order(OrderRequest(symbol='BTC/USDT',side=OrderSide.BUY,order_type=OrderType.LIMIT,amount=1,price=100,account_id=aid,params={'trading_mode':'live'}))
    await om.cancel_order('42','BTC/USDT','binance','live')
asyncio.run(orders_and_cancel())
print('ORDER_ID_COLLISION',len(om._orders),'stored_account',om._order_meta['42']['account_id'],'cancelled',cancelled)

# Non-unit contractSize is never used by the real OKX connector.
defs('core/exchanges/okx_connector.py',{'OKXConnector'},{'create_order','_format_order_precision','get_positions'})
okx=OKXConnector();submitted=[]
async def client_create(**kw):submitted.append(kw);return {'id':'audit-contract'}
async def client_positions():return [{'symbol':'SOL/USDT:USDT','contracts':10,'contractSize':.1,'side':'long','entryPrice':100,'markPrice':110,'unrealizedPnl':10,'leverage':1}]
client=SimpleNamespace(create_order=client_create,fetch_positions=client_positions,market=lambda _:{'contract':True,'contractSize':.1})
async def okx_client():return client
okx._ensure_client=okx_client;okx._parse_order=lambda row:row
defs('core/exchanges/base_exchange.py',{'Position'})
async def contract_probe():
    await okx.create_order('SOL/USDT:USDT',OrderSide.BUY,OrderType.MARKET,1)
    positions=await okx.get_positions()
    print('CONTRACT_UNITS','sent_contracts',submitted[0]['amount'],'correct_contracts',10,'reported_base_qty',positions[0].amount,'correct_base_qty',1)
asyncio.run(contract_probe())


# Binance signed REST isolated at HTTP boundary. Fake testnet credentials only.
import hashlib, hmac
from urllib.parse import urlencode
_BINANCE_REST_TIMEOUT_SEC=8.0
defs('core/trading/binance_rest.py',{'_binance_credentials','binance_signed_request'})
account_manager=SimpleNamespace(get_exchange_credentials=lambda *a: {'api_key':'AUDIT_FAKE_KEY','api_secret':'AUDIT_FAKE_SECRET','sandbox':True,'proxy':''})
settings=SimpleNamespace(HTTP_PROXY='',HTTPS_PROXY='')
_BINANCE_REST_TIMEOUT_SEC=8.0
_BINANCE_RECV_WINDOW=5000
_BINANCE_TIME_OFFSET_MS={}
_refresh_binance_time_offset=nothing
_apply_httpx_proxy_kw=lambda *a:None
requested_urls=[]
class FakeHTTPClient:
    def __init__(self,**kw):pass
    async def __aenter__(self):return self
    async def __aexit__(self,*args):pass
    async def post(self,url,**kw):
        requested_urls.append(url)
        return SimpleNamespace(status_code=200,text='',json=lambda:{'orderId':'sandbox-probe'})
    get=post
httpx=SimpleNamespace(AsyncClient=FakeHTTPClient)
asyncio.run(binance_signed_request('POST','/fapi/v1/order',host='fapi',params={'symbol':'BTCUSDT','quantity':1},account_id='testnet'))
print('SANDBOX_REST_MAINNET',requested_urls,_binance_credentials('testnet'))

# Additional isolated observations: pending children must not count as completed fills.
defs('core/trading/execution_engine.py',{'ExecutionEngine'},{'execute_manual_order','_execute_position_partial_take_profit','_strategy_lookup_value'})
algo=ExecutionEngine.__new__(ExecutionEngine);algo._notify_callbacks=nothing
async def unfilled(**kw):return {'order_id':'pending','status':'open','filled':0,'amount':kw['amount'],'price':100}
algo._execute_manual_order_single=unfilled
merged=asyncio.run(algo.execute_manual_order(exchange='gate',symbol='BTC/USDT',side='buy',order_type='limit',amount=2,price=100,order_mode='iceberg',iceberg_parts=2))
print('ALGO_UNFILLED_MERGE',merged['filled'],merged['status'],'actual_filled',0)

# Partial profit-management consumes the marker even when the mocked order remains unfilled.
profit=SimpleNamespace(side=PositionSide.LONG,exchange='gate',symbol='ETH/USDT',account_id='main',strategy='S',quantity=2,leverage=1,take_profit=120,metadata={})
profit.update_price=lambda _:None
algo._effective_profit_management_metadata=lambda p: dict(p.metadata,partial_take_profit_fraction=.5)
algo._calculate_partial_take_profit_quantity=lambda *a,**kw:(1,'')
algo._current_trading_mode=lambda:'live'
algo._safe_protective_pct=lambda _:None
position_manager=SimpleNamespace(get_position=lambda *a,**kw:profit)
partial=asyncio.run(algo._execute_position_partial_take_profit(profit,current_price=120))
print('PARTIAL_ZERO_FILL_MARKED_DONE',partial['applied'],profit.metadata['partial_take_profit_done'],profit.take_profit,profit.quantity)

obs={r['probe']:r['observed'] for r in results}
checks={
 'manual_reverse_risk_bypass': json.loads(obs['REVERSE_BYPASS'][0])[1][1]==100,
 'reconcile_duplicates_aggregate': obs['RECONCILE_DUPLICATION'][0:2]==[3.0,3.0],
 'close_pnl_cumulative': obs['ENGINE_CLOSE_LOGGED_PNL'][0]==30,
 'order_id_account_collision':obs['ORDER_ID_COLLISION'][0]==1 and obs['ORDER_ID_COLLISION'][2]=='B',
 'contract_size_mismatch':obs['CONTRACT_UNITS'][1]==1 and obs['CONTRACT_UNITS'][5]==10.0,
 'sandbox_rest_mainnet':requested_urls==['https://fapi.binance.com/fapi/v1/order'],
 'algo_zero_fill_counted':merged['filled']==2 and merged['status']=='closed',
 'partial_zero_fill_marker':partial['applied'] and profit.quantity==2 and profit.take_profit is None,
}
builtins.print(json.dumps({'revision':'2fecd05dea8c5ee438b4071e50548b828b7411af','method':'AST-extracted functions; project modules never imported; all IO and venues mocked; no persistence','checks':checks,'observations':results},ensure_ascii=False,indent=2))
assert all(checks.values()),checks

