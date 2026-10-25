"""Audit counterexamples: expected CURRENT bad behavior, not passing safety gates.

Executed only inside the credential-free snapshot. All roles, keys, quotes,
accounts and managers are synthetic; no provider, order endpoint or daemon runs.
"""
import asyncio
import fnmatch
import importlib
import json
import math
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from core.ops.service.auth import OpsAuthContext
from core.governance.rbac import has_permission


def record(name,**proof):
    with (Path(__file__).resolve().parents[1]/'audit_observations.jsonl').open('a',encoding='utf-8') as f:
        f.write(json.dumps({'case':name,**proof},ensure_ascii=False)+'\n')


@pytest.fixture
def api(monkeypatch,tmp_path):
    import web.api.auth as auth
    from web.api import trading_accounts,trading_runtime,ai_agent
    from core.trading.account_manager import AccountManager
    from config.settings import settings
    role=['OPERATOR']
    async def fake_auth(request):
        ctx=OpsAuthContext(actor='audit_synthetic_user',role=role[0],token_present=False,api_key_present=True,client_ip='testclient')
        request.state.ops_auth=ctx
        return ctx
    monkeypatch.setattr(auth,'require_ops_auth',fake_auth)
    monkeypatch.setattr(settings,'BASE_DIR',tmp_path/'accounts')
    accounts=AccountManager()
    accounts.create_account('audit_vault','Synthetic account','binance',mode='live',enabled=False,
        metadata={'credentials':{'binance':{'api_key':'audit-synthetic-key','api_secret':'audit-synthetic-secret','sandbox':True}}})
    monkeypatch.setattr(trading_accounts.trading_api,'account_manager',accounts)
    app=FastAPI();app.include_router(trading_accounts.router,prefix='/api/trading');app.include_router(trading_runtime.router,prefix='/api/trading');app.include_router(ai_agent.router,prefix='/api/ai')
    return TestClient(app),role,accounts


def test_readonly_account_api_returns_synthetic_credential_fields(api):
    client,role,_=api;role[0]='AUDITOR'
    result=client.get('/api/trading/accounts')
    assert result.status_code==200
    account=next(x for x in result.json()['accounts'] if x['account_id']=='audit_vault')
    keys=account['metadata']['credentials']['binance']
    assert keys['api_secret']=='audit-synthetic-secret'
    assert not has_permission('AUDITOR','manage_accounts')
    record('SEC-01',role='AUDITOR',status=result.status_code,api_key_exposed=bool(keys['api_key']),api_secret_exposed=bool(keys['api_secret']))


def test_operator_can_enable_and_retarget_existing_live_account_without_approval(api):
    client,_,accounts=api
    assert not has_permission('OPERATOR','approve_live')
    control=client.put('/api/trading/accounts/audit_vault',json={'mode':'live'})
    assert control.status_code==403
    result=client.put('/api/trading/accounts/audit_vault',json={'enabled':True,'metadata':{'credentials':{'binance':{'sandbox':False,'api_key':'audit-only-key','api_secret':'audit-only-secret'}}}})
    assert result.status_code==200
    assert accounts.get_account_mode('audit_vault')=='live'
    assert accounts.is_enabled('audit_vault')
    assert accounts.get_exchange_credentials('audit_vault','binance')['sandbox'] is False
    record('SEC-02',role='OPERATOR',status=200,explicit_live_change_control_status=403,live_account_enabled=True,sandbox_changed_to_false=True)


def test_operator_can_enable_ai_live_gate_without_live_permission(api,monkeypatch):
    client,_,_=api
    from web.api import ai_research
    from config.settings import settings
    agent=ai_research.autonomous_trading_agent
    monkeypatch.setattr(settings,'AI_AUTONOMOUS_AGENT_ALLOW_LIVE',False)
    monkeypatch.setattr(agent,'_override',{})
    monkeypatch.setattr(agent,'_save_overlay',lambda:None)
    result=client.post('/api/ai/runtime-config/autonomous-agent',json={'allow_live':True})
    assert result.status_code==200
    assert result.json()['config']['allow_live'] is True
    assert not has_permission('OPERATOR','approve_live')
    record('SEC-03',role='OPERATOR',status=200,allow_live_enabled=True,real_agent_started=False)


def test_ack_alert_role_can_reset_daily_halt(api,monkeypatch):
    client,_,_=api
    from web.api import trading_runtime as runtime
    reset=Mock()
    monkeypatch.setattr(runtime,'risk_manager',SimpleNamespace(reset_halt=reset))
    monkeypatch.setattr(runtime,'invalidate_trading_stats_cache',lambda:None)
    monkeypatch.setattr(runtime,'_schedule_audit_log',lambda **kw:None)
    monkeypatch.setattr(runtime,'_build_effective_risk_report',AsyncMock(return_value={'trading_halted':False}))
    response=client.post('/api/trading/risk/reset')
    assert response.status_code==200
    assert not has_permission('OPERATOR','approve_risk_change')
    reset.assert_called_once()
    record('SEC-04',role='OPERATOR',status=200,daily_halt_reset_called=True)


def test_operator_ai_route_can_change_shared_risk_thresholds(api,monkeypatch):
    client,_,_=api
    from web.api import ai_research
    risk_module=importlib.import_module('core.risk.risk_manager')
    updates=[]
    monkeypatch.setattr(risk_module.risk_manager,'update_parameters',lambda payload:updates.append(payload))
    monkeypatch.setattr(ai_research,'_build_autonomous_agent_risk_config',lambda:{'audit':True})
    response=client.post('/api/ai/autonomous-agent/risk-config',json={'autonomy_max_drawdown_reduce_only':1.0})
    assert response.status_code==200 and updates==[{'autonomy_max_drawdown_reduce_only':1.0}]
    assert not has_permission('OPERATOR','approve_risk_change')
    record('SEC-05',role='OPERATOR',status=200,shared_risk_update_called=True)


def test_nonfinite_order_schema_and_risk_check_accept_nan(monkeypatch):
    from core.risk.risk_manager import RiskManager
    from web.api.trading import OrderRequest
    risk_module=importlib.import_module('core.risk.risk_manager')
    manager=RiskManager(use_persisted_overlay=False)
    monkeypatch.setattr(manager,'_check_new_day',lambda:None)
    monkeypatch.setattr(risk_module,'_position_manager',lambda:SimpleNamespace(get_position_count=lambda:0,get_all_positions=lambda:[],get_positions_by_strategy=lambda _:[]))
    normal=manager.pre_trade_check('BTC/USDT','buy','audit',1000.,1e12,1.)
    invalid=manager.pre_trade_check('BTC/USDT','buy','audit',float('nan'),1e12,1.)
    request=OrderRequest(exchange='binance',symbol='BTC/USDT',side='buy',order_type='market',amount='NaN')
    assert normal is False and invalid is True and math.isnan(request.amount)
    record('TR-01',normal_oversized_rejected=True,nan_equity_oversized_allowed=True,nan_order_amount_schema_accepted=True)


@pytest.mark.asyncio
async def test_paper_market_fill_reverts_valid_live_quote_to_request(monkeypatch):
    from core.trading.order_manager import OrderManager,OrderRequest
    from core.exchanges.base_exchange import OrderSide,OrderType,OrderStatus
    manager=OrderManager()
    monkeypatch.setattr(manager,'_resolve_cached_exchange',lambda *a,**k:None)
    quote=SimpleNamespace(price=200.,source='synthetic_fresh_quote',age_ms=0,bid=199.,ask=201.)
    monkeypatch.setattr(manager,'_paper_live_price',AsyncMock(return_value=quote))
    monkeypatch.setattr(manager,'_resolve_paper_cost_params',lambda request:(0.,0.))
    monkeypatch.setattr(manager,'_evaluate_order_governance',AsyncMock(return_value=SimpleNamespace(allowed=True,reduce_only=False,trace_id='audit')))
    order=await manager._create_paper_order(OrderRequest(exchange='binance',symbol='BTC/USDT',side=OrderSide.BUY,order_type=OrderType.MARKET,amount=1.,price=100.))
    assert order.status==OrderStatus.CLOSED and order.price==100.
    record('TR-02',fresh_quote=200.,requested_price=100.,paper_filled_price=order.price,source=manager._order_meta[order.id]['paper_fill_source'])


@pytest.mark.asyncio
async def test_paper_order_records_nonfinite_fill_quantity(monkeypatch):
    from core.trading.order_manager import OrderManager,OrderRequest
    from core.exchanges.base_exchange import OrderSide,OrderType,OrderStatus
    manager=OrderManager()
    monkeypatch.setattr(manager,'_resolve_paper_cost_params',lambda request:(0.,0.))
    monkeypatch.setattr(manager,'_evaluate_order_governance',AsyncMock(return_value=SimpleNamespace(allowed=True,reduce_only=False,trace_id='audit')))
    order=await manager._create_paper_order(OrderRequest(exchange='binance',symbol='BTC/USDT',side=OrderSide.BUY,order_type=OrderType.LIMIT,amount=float('nan'),price=100.))
    assert order.status==OrderStatus.CLOSED and math.isnan(order.filled)
    record('TR-01-paper',nan_filled_order_cached=True,real_connector_called=False)


def test_docker_context_includes_git_ignored_env_variants():
    root=Path(__file__).resolve().parents[1]
    patterns=[line.strip() for line in (root/'.dockerignore').read_text(encoding='utf-8-sig').splitlines() if line.strip() and not line.startswith('#')]
    def ignored(name):
        excluded=False
        for pattern in patterns:
            negative=pattern.startswith('!')
            if fnmatch.fnmatchcase(name,pattern.lstrip('!').strip('/')):excluded=not negative
        return excluded
    assert ignored('.env') and ignored('.env.local')
    exposed=[name for name in ('.env.production','.env.backup','.env.test') if not ignored(name)]
    assert len(exposed)==3
    assert 'COPY --chown=cts:cts . /app' in (root/'Dockerfile').read_text(encoding='utf-8-sig')
    record('OPS-01',unexcluded_root_env_names=exposed,real_secret_files_read=False,image_built=False)
