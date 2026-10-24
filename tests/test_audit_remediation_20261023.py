"""Authorization, number and fill-price regressions for the October 22 audit."""
import asyncio
import fnmatch
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
import pytest

from core.ops.service.auth import OpsAuthContext


@pytest.fixture
def api(monkeypatch, tmp_path):
    from web.api import auth, trading_accounts, trading_runtime, ai_agent, ai_research
    from config.settings import settings
    from core.trading.account_manager import AccountManager
    role = ['OPERATOR']
    async def identify(request):
        ctx = OpsAuthContext(actor='synthetic-audit-user', role=role[0], token_present=False,
                             api_key_present=True, client_ip='testclient')
        request.state.ops_auth = ctx
        return ctx
    monkeypatch.setattr(auth, 'require_ops_auth', identify)
    monkeypatch.setattr(settings, 'BASE_DIR', tmp_path)
    accounts = AccountManager()
    accounts.create_account('audit_live', 'Synthetic', 'binance', mode='live', enabled=False,
                            metadata={'credentials': {'binance': {'api_key': 'synthetic-key', 'api_secret': 'synthetic-secret', 'sandbox': True}},
                                      'api_secret': 'synthetic-legacy', 'unexpected': {'api_key': 'synthetic-nested'},
                                      'isolated': True, 'sandbox': True})
    accounts.create_account('audit_paper', 'Synthetic paper', 'binance')
    monkeypatch.setattr(trading_accounts.trading_api, 'account_manager', accounts)
    monkeypatch.setattr(ai_research.autonomous_trading_agent, 'get_runtime_config', lambda: {'allow_live': False})
    app = FastAPI()
    app.include_router(trading_accounts.router, prefix='/api/trading')
    app.include_router(trading_runtime.router, prefix='/api/trading')
    app.include_router(ai_agent.router, prefix='/api/ai')
    return TestClient(app), role, accounts


def assert_no_credentials(payload):
    text = json.dumps(payload)
    assert all(secret not in text for secret in ('synthetic-key', 'synthetic-secret', 'synthetic-legacy', 'synthetic-nested'))
    assert 'credentials' not in text and 'api_secret' not in text


@pytest.mark.parametrize('role', ['AUDITOR', 'ENGINEER', 'OPERATOR', 'RISK_OWNER', 'SYSTEM'])
def test_account_list_and_summary_do_not_return_credentials(api, monkeypatch, role):
    from web.api import trading_accounts
    client, identity, accounts = api
    identity[0] = role
    monkeypatch.setattr(trading_accounts.trading_api.position_manager, 'get_all_positions', lambda: [])
    monkeypatch.setattr(trading_accounts.trading_api.order_manager, 'get_recent_orders', lambda **_: [])
    for endpoint in ('/accounts', '/accounts/summary'):
        response = client.get('/api/trading'+endpoint)
        assert response.status_code == 200
        assert_no_credentials(response.json())
    assert accounts.get_exchange_credentials('audit_live')['api_secret'] == 'synthetic-secret'


@pytest.mark.parametrize('method,payload', [('put', {'enabled': True}), ('put', {'metadata': {'credentials': {'binance': {'sandbox': False}}}}),
                                          ('put', {'mode': 'paper', 'enabled': True}), ('delete', None)])
@pytest.mark.parametrize('role,allowed', [('OPERATOR', False), ('RISK_OWNER', True), ('SYSTEM', True)])
def test_existing_live_account_mutations_require_live_approval(api, role, allowed, method, payload):
    client, identity, accounts = api; identity[0] = role
    if method == 'delete':response = client.delete('/api/trading/accounts/audit_live')
    else:response = client.put('/api/trading/accounts/audit_live', json=payload)
    assert response.status_code == (200 if allowed else 403)
    if not allowed:
        assert accounts.get_account('audit_live')['enabled'] is False
        assert accounts.get_account('audit_live')['mode'] == 'live'
        assert accounts.get_exchange_credentials('audit_live')['sandbox'] is True


def test_account_create_and_update_outputs_are_write_only_and_paper_stays_usable(api):
    client, _, accounts = api
    response = client.post('/api/trading/accounts', json={'account_id': 'paper_new', 'name': 'Paper', 'exchange': 'binance',
        'metadata': {'credentials': {'binance': {'api_secret': 'synthetic-secret'}}}})
    assert response.status_code == 200
    assert_no_credentials(response.json())
    response = client.put('/api/trading/accounts/paper_new', json={'metadata': {'api_secret': 'synthetic-legacy'}})
    assert response.status_code == 200
    assert_no_credentials(response.json())
    assert accounts.get_exchange_credentials('paper_new')['api_secret'] == 'synthetic-secret'
    assert client.put('/api/trading/accounts/audit_paper', json={'mode': 'live'}).status_code == 403


def test_account_authorization_checks_latest_state_under_mutation_lock(api):
    _, _, manager = api
    # A second writer changes the mode before the mutation obtains its lock.
    manager.update_account('audit_paper', {'mode': 'live'})
    denied = Mock(side_effect=PermissionError('live approval required'))
    with pytest.raises(PermissionError):manager.update_account('audit_paper', {'enabled': False}, authorize=denied)
    assert denied.call_args.args[0]['mode'] == 'live'
    assert manager.get_account('audit_paper')['enabled'] is True


@pytest.mark.parametrize('role,status', [('AUDITOR', 403), ('ENGINEER', 403), ('OPERATOR', 403), ('RISK_OWNER', 200), ('SYSTEM', 200)])
@pytest.mark.parametrize('endpoint', ['/api/trading/risk/reset', '/api/ai/autonomous-agent/risk-config'])
def test_risk_owner_is_required_on_both_risk_entrypoints(api, monkeypatch, role, status, endpoint):
    from web.api import trading_runtime, ai_research
    client, identity, _ = api;identity[0] = role
    reset = Mock()
    monkeypatch.setattr(trading_runtime, 'risk_manager', SimpleNamespace(reset_halt=reset))
    monkeypatch.setattr(trading_runtime, 'invalidate_trading_stats_cache', lambda: None)
    monkeypatch.setattr(trading_runtime, '_schedule_audit_log', lambda **_: None)
    monkeypatch.setattr(trading_runtime, '_build_effective_risk_report', AsyncMock(return_value={}))
    risk = importlib.import_module('core.risk.risk_manager').risk_manager
    update = Mock();monkeypatch.setattr(risk, 'update_parameters', update)
    monkeypatch.setattr(ai_research, '_build_autonomous_agent_risk_config', lambda: {})
    response = client.post(endpoint, json={'autonomy_max_drawdown_reduce_only': .05} if '/api/ai/' in endpoint else None)
    assert response.status_code == status
    assert reset.called == (status == 200 and '/api/trading/' in endpoint)
    assert update.called == (status == 200 and '/api/ai/' in endpoint)


@pytest.mark.parametrize('role,status', [('OPERATOR', 403), ('SYSTEM', 200)])
def test_enabling_ai_live_gate_requires_approval_before_update(api, monkeypatch, role, status):
    from web.api import ai_research
    client, identity, _ = api;identity[0] = role
    update = AsyncMock(return_value={'allow_live': True})
    monkeypatch.setattr(ai_research.autonomous_trading_agent, 'update_runtime_config', update)
    response = client.post('/api/ai/runtime-config/autonomous-agent', json={'allow_live': True})
    assert response.status_code == status
    assert update.await_count == int(status == 200)


@pytest.mark.parametrize('endpoint,payload', [('/runtime-config/autonomous-agent', {'symbol': 'ETH/USDT'}),
                                            ('/autonomous-agent/start', {}), ('/autonomous-agent/run-once', {})])
def test_operator_cannot_reconfigure_or_start_an_already_live_capable_agent(api, monkeypatch, endpoint, payload):
    from web.api import ai_research
    client, _, _ = api
    monkeypatch.setattr(ai_research.autonomous_trading_agent, 'get_runtime_config', lambda: {'allow_live': True})
    update, start, once = AsyncMock(), AsyncMock(), AsyncMock()
    for name, stub in [('update_runtime_config', update), ('start', start), ('trigger_run_once', once)]:
        monkeypatch.setattr(ai_research.autonomous_trading_agent, name, stub)
    assert client.post('/api/ai'+endpoint, json=payload).status_code == 403
    assert not any(stub.called for stub in (update,start,once))


@pytest.mark.parametrize('current,changes', [(False, {'symbol': 'ETH/USDT'}), (True, {'allow_live': False}),
                                           (True, {'enabled': False}), (True, {'mode': 'shadow'})])
def test_operator_can_edit_paper_config_and_disarm_live_agent(api, monkeypatch, current, changes):
    from web.api import ai_research
    client, _, _ = api
    monkeypatch.setattr(ai_research.autonomous_trading_agent, 'get_runtime_config', lambda: {'allow_live': current})
    update=AsyncMock(return_value=changes);monkeypatch.setattr(ai_research.autonomous_trading_agent, 'update_runtime_config', update)
    assert client.post('/api/ai/runtime-config/autonomous-agent', json=changes).status_code == 200
    update.assert_awaited_once()


@pytest.mark.parametrize('field', ['amount', 'price', 'leverage', 'stop_loss', 'take_profit', 'trigger_price'])
@pytest.mark.parametrize('value', ['NaN', 'Infinity', '-Infinity'])
def test_order_schema_rejects_nonfinite_fields(field, value):
    from web.api.trading import OrderRequest
    payload={'exchange':'binance','symbol':'BTC/USDT','side':'buy','order_type':'market','amount':1,field:value}
    with pytest.raises(ValidationError):OrderRequest(**payload)


@pytest.mark.parametrize('field', ['account_equity', 'order_value', 'leverage', 'strategy_allocation'])
@pytest.mark.parametrize('value', [float('nan'),float('inf'),-float('inf')])
def test_risk_does_not_allow_nonfinite_numbers(monkeypatch, field, value):
    from core.risk.risk_manager import RiskManager
    module=importlib.import_module('core.risk.risk_manager')
    risk=RiskManager(use_persisted_overlay=False)
    monkeypatch.setattr(risk,'_check_new_day',lambda:None)
    monkeypatch.setattr(module,'_position_manager',lambda:SimpleNamespace(get_position_count=lambda:0,get_all_positions=lambda:[],get_positions_by_strategy=lambda _:[]))
    kwargs=dict(symbol='BTC/USDT',side='buy',strategy_name=None,account_equity=1000,order_value=10,leverage=1,strategy_allocation=1)
    assert risk.pre_trade_check(**kwargs) is True
    kwargs[field]=value
    assert risk.pre_trade_check(**kwargs) is False


@pytest.mark.parametrize('value', [float('nan'),float('inf'),-1,0])
def test_invalid_quantity_cannot_reach_either_order_backend(monkeypatch, value):
    from core.trading.order_manager import OrderManager, OrderRequest
    from core.exchanges.base_exchange import OrderSide,OrderType
    manager=OrderManager();paper,live=AsyncMock(),AsyncMock()
    monkeypatch.setattr(manager,'_create_paper_order',paper);monkeypatch.setattr(manager,'_create_real_order',live)
    request=OrderRequest(symbol='BTC/USDT',side=OrderSide.BUY,order_type=OrderType.MARKET,amount=value)
    assert asyncio.run(manager.create_order(request)) is None
    assert manager.get_last_error()
    assert not paper.called and not live.called


def test_direct_paper_backend_also_rejects_nan_without_recording():
    from core.trading.order_manager import OrderManager,OrderRequest
    from core.exchanges.base_exchange import OrderSide,OrderType
    manager=OrderManager()
    request=OrderRequest(symbol='BTC/USDT',side=OrderSide.BUY,order_type=OrderType.LIMIT,amount=float('nan'),price=100)
    with pytest.raises(ValueError):asyncio.run(manager._create_paper_order(request))
    assert not manager._orders


def test_valid_close_remains_available_when_equity_is_unavailable(monkeypatch):
    from core.risk.risk_manager import RiskManager
    module=importlib.import_module('core.risk.risk_manager')
    risk=RiskManager(use_persisted_overlay=False)
    monkeypatch.setattr(risk,'_check_new_day',lambda:None)
    monkeypatch.setattr(module,'_position_manager',lambda:SimpleNamespace(get_position_count=lambda:0,get_all_positions=lambda:[]))
    assert risk.pre_trade_check('BTC/USDT','sell',None,float('nan'),100.,allow_close=True)
    assert not risk.pre_trade_check('BTC/USDT','sell',None,1000.,float('nan'),allow_close=True)


def test_docker_secret_variants_are_excluded_but_example_is_allowed():
    root=Path(__file__).resolve().parents[1]
    patterns=[s.strip() for s in (root/'.dockerignore').read_text(encoding='utf-8-sig').splitlines() if s.strip() and not s.startswith('#')]
    def ignored(name):
        result=False
        for pattern in patterns:
            if fnmatch.fnmatchcase(name,pattern.lstrip('!')):result=not pattern.startswith('!')
        return result
    assert all(ignored(x) for x in ('.env','.env.local','.env.production','.env.backup','.env.test','.env.production.local'))
    assert not ignored('.env.example')
