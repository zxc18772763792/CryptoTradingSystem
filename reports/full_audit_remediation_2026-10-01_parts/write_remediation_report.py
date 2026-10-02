from pathlib import Path
import json
import re
import subprocess

root = Path(__file__).resolve().parents[2]
out = Path(__file__).resolve().parent
notes = {
'T1': ('反手按完整新单接受风控；纯减仓强制 reduce_only，未完成意图继续阻止新敞口。', 'test_audit_remediation_trading.py::test_oversized_reversal_cannot_use_close_risk_exemption'),
'T2': ('按真实连接账户分组；共享仓位保留策略原分配。总量不符标记 reconciliation_required，并阻止新开仓。', 'test_audit_remediation_trading.py::test_reconciliation_preserves_strategy_allocations'),
'T3': ('缓存键包含账户、交易所、标的和交易所订单号，以 URL 安全摘要对外引用；裸 ID 歧义拒绝查询/取消。', 'test_audit_remediation_trading.py::test_cross_account_venue_id_is_ambiguous_and_scoped_cancel_is_correct'),
'T4': ('四个 CCXT 连接器在提交、成交解析和持仓解析边界换算 contractSize；未知合约单位或反向合约拒绝执行。', 'test_audit_remediation_trading.py::test_derivative_quantities_round_trip_in_base_units'),
'T5': ('专用 CLOSE 记录本次 realized_pnl 增量，避免重复计入之前的部分平仓。', 'test_audit_remediation_trading.py::test_close_signal_records_incremental_realized_pnl'),
'T6': ('sandbox 禁用生产 fast REST；走 sandbox CCXT，并通过该连接器设置开仓杠杆。', 'test_audit_remediation_trading.py::test_sandbox_fast_rest_cannot_create_an_http_client'),
'T7': ('算法父单仅汇总实际 filled；零成交不再回退 amount 或标记 closed。', 'test_audit_remediation_trading.py::test_unfilled_algo_children_do_not_count_as_fills'),
'T8': ('部分止盈保留目标量、已确认量和待成交订单；零/部分成交保留保护，待终态和本地对账后完成。', 'test_audit_remediation_trading.py::test_partial_take_profit_keeps_protection_until_fill'),
'A3': ('FIFO 平仓按消耗比例分摊开仓手续费和滑点，保留余量对应成本。', 'test_audit_remediation_trading.py::test_fifo_partial_close_allocates_entry_cost_exactly_once'),
'WEB-01': ('策略注册/导入/启动/批量启动/参数/配置/分配与模式变更，涉及 live 时同时校验 approve_live。', 'test_audit_remediation_web.py::test_strategy_operator_cannot_mutate_live'),
'WEB-02': ('熔断 reset/evaluate 要求 approve_risk_change。', 'test_audit_remediation_web.py::test_risk_reset_authorization_and_audit'),
'WEB-03': ('允许集合新增成员、替换成员或从有限集合变为空集合均视为增加风险。', 'test_audit_remediation_web.py::test_allowlist_changes_detect_new_permissions'),
'WEB-04': ('治理 kill_switch 与日内停机状态分离；配置变更不清除日内停机，治理开关在报告中独立显示。', 'governance/test_governance_gates.py 中两项 risk_config 停机回归'),
'WEB-05': ('策略名称、类型、最近信号和状态摘要进入 HTML 前转义；更新 app.js 资源版本。', 'xss_fixed.json：恶意名称没有未转义进入 innerHTML'),
'WEB-06': ('HTTP profile 输出限制于 data/profiles/polymarket/*.json；排除目录逃逸和覆盖，写入采用 exclusive create。', 'test_audit_remediation_web.py::test_profile_write_is_confined_and_cannot_overwrite'),
'WEB-07': ('Ops 新闻路由要求 manage_news；审计查询要求 read_audit。', 'test_audit_remediation_web.py::test_ops_secondary_entrypoints_require_capability'),
'WEB-08': ('可选 manual signal 入口同时要求 manage_orders 和 approve_live。', 'test_audit_remediation_web.py::test_ops_secondary_entrypoints_require_capability'),
'WEB-09': ('修正熔断审计 logger 参数；target 放入 details，写入异常记录日志。', 'test_audit_remediation_web.py::test_risk_reset_authorization_and_audit'),
'WEB-10': ('WebSocket hello 发送纳入资源 try/finally；初次发送断连也 unsubscribe。', 'test_audit_remediation_web.py::test_websocket_hello_disconnect_unsubscribes'),
'WEB-11': ('追加、裁剪、outcome 回写使用同一文件锁覆盖读改写全过程。', 'test_audit_remediation_web.py::test_counterfactual_rewrite_does_not_lose_concurrent_appends'),
'WEB-12': ('推进回放改为 POST + manage_data_sources；前端同步改为 POST。', 'test_audit_remediation_web.py::test_replay_next_is_authenticated_post'),
'DR-01': ('价格年龄使用源事件与接收时间的较早者，重播旧行情不能刷新为新鲜。', 'test_audit_remediation_data.py::test_old_source_tick_stays_stale_even_when_repeated'),
'DR-02': ('模型响应必须包含有效 action；无效响应进入 schema 错误分支，默认失败关闭；显式 fail_open 保持既有策略。', 'test_audit_remediation_data.py::test_invalid_ai_response_fails_closed'),
'DR-03': ('整批新闻先入库，游标与原始新闻同事务提交；同时间戳及五分钟迟到窗口重叠去重，游标不回退。', 'test_audit_remediation_data.py 中三项 news delivery/cursor/timestamp 回归'),
'DR-04': ('训练集尾部 purge forward_bars，训练标签使用的未来价格早于测试特征开始。', 'test_audit_remediation_data.py::test_training_labels_end_before_test_features'),
'DR-05': ('因子缓存指纹覆盖全数据、全索引、列名和 dtype，不再抽样或忽略扩展列。', 'test_audit_remediation_data.py::test_factor_cache_invalidates_changed_middle_and_extra_columns'),
'DR-06': ('DSL 源数据仅前向填充，前导缺失不再从未来回填。', 'test_audit_remediation_data.py::test_strategy_data_is_causal_and_missing_identifiers_fail_closed'),
'DR-07': ('缺失 source 或无法解析的 operand 显式报错，禁止静默替换。', 'test_audit_remediation_data.py::test_strategy_data_is_causal_and_missing_identifiers_fail_closed'),
'DR-08': ('持仓保存 entry_fee，平仓净收益扣除双边费用；账户现金不重复扣开仓费。', 'test_audit_remediation_data.py::test_backtest_entry_fee_affects_trade_classification_once'),
'DR-09': ('结果携带权益时间索引；风险指标按实际采样频率年化，Calmar 使用回撤小数；胜负统计仅计已平仓交易。', 'test_audit_remediation_data.py::test_hourly_annualization_and_calmar_fraction_units'),
'DR-10': ('Hub/provider 拒绝 NaN/Infinity，时间戳转换防溢出。', 'test_audit_remediation_data.py::test_nonfinite_market_price_rejected'),
'DR-11': ('账户锁后，在同一事务内重新读取订单、落 fill、更新账户/仓位并终结订单；跨账户访问拒绝。', 'polymarket/test_audit_paper_atomicity.py 并发 8 次成交与注入失败回滚'),
'DR-12': ('在同一账户事务中将 OPEN/PARTIAL 买单未成交量计入现金和仓位预算。', 'polymarket/test_audit_paper_atomicity.py::test_concurrent_pending_orders_reserve_position_budget'),
'DR-13': ('纸盘仅使用同 token、新鲜且有可执行 bid/ask 的报价；历史回放显式传入模拟时间。', 'polymarket/test_audit_paper_atomicity.py 时效、身份、报价和 replay clock 回归'),
'DR-14': ('Gamma 用 token/outcome 数组对应价格，保留源 updatedAt；不伪造 bid/ask，不将降级结果记为 CLOB 成功。', 'polymarket/test_audit_paper_atomicity.py::test_gamma_fallback_maps_tokens_and_preserves_source_time'),
'DR-15': ('CUSUM 满足 min_bars 后才触发；paper→shadow 为合法状态转换，失败时保留当前状态。', 'test_audit_remediation_data.py 中两项 CUSUM/lifecycle 回归'),
'OPS-01': ('启动命令明确带工程入口绝对路径和实例端口；停止前复核身份；计划任务和互斥量名称同时包含工程路径摘要及端口。', 'test_audit_remediation_ops.py::test_windows_managed_process_scope_uses_only_mock_processes'),
'OPS-02': ('Compose worker 改用独立循环心跳探针，不继承 Web HTTP 探针。', 'test_audit_remediation_ops.py::test_news_worker_health_requires_recent_worker_heartbeat；compose 静态解析'),
'QA-01': ('权重/manifest 使用合成 fixture，认证显式 dummy token；历史行情缺失标注未验证，离线测试不再要求本机产物。', 'test_pump_precursor.py、test_research_loop_v2.py、test_pump_watchlist_resilience.py、test_exit_logic_overhaul_verification.py'),
'IS1': ('selfcheck 非零/空输出直接失败，父进程透传 evaluator 退出码。', 'ignored_helpers_results.json：5/1/7/0 四种 mock 退出场景'),
'IS2': ('REST 行情健康仅接受 HTTP 200；WS 隧道可达性继续独立接受 200/400/404。', 'ignored_helpers_results.json：200/401/429/500 mock 矩阵'),
}
assert len(notes) == 41
original = (root / 'reports/full_audit_2026-10-01.md').read_text(encoding='utf-8')
rows = []
for line in original.splitlines():
    if line.startswith('| ') and line.split('|')[1].strip() in notes:
        pieces=line.split('|')
        issue, level = pieces[1].strip(), pieces[2].strip()
        rows.append((issue, level, *notes[issue]))
assert len(rows) == 41
header = '''# 全量审计修复与验证记录 · 2026-10-01

本记录对应 [原始全量审计](full_audit_2026-10-01.md) 的 41 项确认问题。业务代码、接口边界、纸盘持久化、研究统计和运维脚本的修复均在主工程工作区中；原审计报告保持原样。**不代表已部署或实盘验收通过。**

原审计基线为 `2fecd05dea8c5ee438b4071e50548b828b7411af`；最终验证时 HEAD 为 `ddd197f94a12eb624bc026fc53838fe543d1f27d`，其已有的研究功能提交保留并纳入隔离测试。本次没有提交或推送 Git，也没有同步修改 `_radar_refactor`。保留之前的审计产物和已有文件；ignored 的两个历史辅助脚本仅修控制流，未迁移或提交其配置/凭据内容。

## 验证状态

修复已完成代码和离线回归验证，41 项确认问题均有逐项修复记录。

- 全量隔离测试：**2673 passed / 2 skipped / 0 failed**，约 6 分 30 秒。运行参数为 `pytest -q -m "not live"`；[完整日志](full_audit_remediation_2026-10-01_parts/pytest_211755.log)、[JUnit XML](full_audit_remediation_2026-10-01_parts/pytest_211755.xml)、[执行元数据](full_audit_remediation_2026-10-01_parts/run_211755.json)。
- 补强交易回归：**13 passed**，覆盖四个连接器实际提交量、sandbox 杠杆路由、止盈成交确认与对账；[日志](full_audit_remediation_2026-10-01_parts/pytest_212050.log)、[JUnit XML](full_audit_remediation_2026-10-01_parts/pytest_212050.xml)。这些测试与全量套件存在重叠，不累加为独立用例数。
- 静态检查：885 个 Python 文件 AST、JavaScript 语法、Compose 解析、6 个 PowerShell 文件语法均通过；XSS 反例已不能未经转义进入 HTML；两个 ignored 辅助脚本的 mock 验证通过。`git diff --check` 通过。
- 核对 1252 个测试副本来源文件：全部生产代码与全量测试副本一致；唯一变化是补强后的交易回归文件，它与 13 项通过的补充测试副本一致。证据见 [验证清单](full_audit_remediation_2026-10-01_parts/verification_manifest.json)。
- 2 项跳过分别是：`test_ake_btw_and_17_plus_2_baseline_event_regression` 缺少冻结基线；`test_buy_take_profit_above_current_price` 的既有随机样本未触发 BUY crossing。它们未被计为通过。另有 1 条既有 Starlette/httpx 弃用警告，本次未升级依赖。

所有 pytest 在全新临时源代码副本执行，只复制跟踪源码与新增测试/辅助代码；不复制真实环境文件、密钥、账户状态和研究原始数据。子进程清理凭据变量、关闭自动 worker，并通过 guard 禁止访问主工作区/关联工作树及外部网络。测试可以启动自己拥有的 loopback socket，不能连接已有服务。

## 逐项修复清单

| ID | 等级 | 修复后的行为 | 验证依据 |
|---|---|---|---|
'''
lines = [header]
for issue, level, fix, evidence in rows:
    lines.append(f'| {issue} | {level} | {fix} | {evidence} |')
lines.append('''
## 行为变化和上线注意事项

- 交易数量在系统内部统一为基币量。线性合约依赖交易所 market 的 contractSize；不明确单位及反向合约现在失败关闭。不得通过填默认 1 来绕过拒绝。
- 反手按整单名义价值保守风控；共享真实仓位与策略分配不符时保留本地分配并阻止新增敞口，需通过权威成交记录归因后恢复。没有凭聚合快照猜测每个策略的仓位。
- 订单接口的 `id/order_id` 可为账户绑定的不透明引用；`exchange_order_id` 保留交易所原 ID。撤单也支持明确 account_id。重启后未恢复且身份不明确的订单拒绝盲目路由。
- 待成交部分止盈不会消费保护。若成交已确认但本地账本还未完成对账，继续等待，不重复发送减仓单。
- 新闻游标和整批原始新闻同事务提交。五分钟重叠覆盖边界同时间戳及常见迟到；更早的迟到数据仍需要补采流程，不声称任意乱序绝不遗漏。
- 回测交易级净收益含开仓费用，可能改变胜率与 profit factor；现金费用只扣一次。旧 BacktestResult 没有权益时间索引时保留日频兼容解释，新结果按实际时间频率计算。
- 纸盘默认报价最大年龄为 120 秒。Gamma 降级价格仅供指示/展示，不用于成交；历史 replay 使用显式模拟时钟。
- profile 新建限于 `data/profiles/polymarket/*.json`，HTTP 不覆盖已有文件；调用方应使用新的输出文件名。
- 运维入口改用 managed_entry 身份，计划任务及互斥量按工程和端口区分；旧进程无身份标记不会被新脚本接管。首次切换方式见 [STARTUP.md](../STARTUP.md)，本次未停止任何进程或启动任何服务。

## 仍需环境验证的范围

1. 未连接真实交易所或交易账户；没有订单、杠杆、余额读取/修改。真实 schema、部分成交/撤单竞态、跨账户重启恢复仍需无资金沙箱验收。
2. 纸盘并发和回滚在独立 SQLite 上验证；PostgreSQL 路径使用同样的账户行锁和单事务，但本机没有启动 PostgreSQL/多主机压力测试。文件锁竞争验证也不等于长时间压力基准。
3. 未 build/start Docker；worker 健康检查已做代码、配置和时效单元验证。未测试真实 Windows 服务/计划任务启停，全部进程枚举与停止对象为 mock。
4. 未重训或复算真实历史研究数据；没有浏览器真实 DOM/CSP 全流程，XSS 为离线 DOM sink 验证。未添加/升级依赖或声称供应链漏洞已审计。
5. 原报告列出的待验证风险不自动转为已解决问题。本次闭环对象是 41 项确认项；没有以测试通过保证策略收益或实盘资金安全。

## 复查与证据

- [隔离测试执行器](full_audit_remediation_2026-10-01_parts/run_isolated_tests.py)
- [静态检查结果](full_audit_remediation_2026-10-01_parts/static_results.json)
- [PowerShell 语法检查](full_audit_remediation_2026-10-01_parts/powershell_syntax.json)
- [XSS 修复后反例](full_audit_remediation_2026-10-01_parts/xss_fixed.json)
- [历史辅助脚本 mock 验证](full_audit_remediation_2026-10-01_parts/ignored_helpers_results.json)

新增回归测试位于 `tests/test_audit_remediation_*.py`、`tests/polymarket/test_audit_paper_atomicity.py` 及 `tests/helpers/check_managed_process_scope.ps1`。早期失败日志保留用于区分测试前提修正与真正实现回归，不覆盖原审计证据。
''')
(root / 'reports/full_audit_remediation_2026-10-01.md').write_text('\n'.join(lines), encoding='utf-8')
(out / 'closure_checklist.json').write_text(json.dumps([dict(id=i, priority=p, fix=f, evidence=e) for i,p,f,e in rows], ensure_ascii=False, indent=2), encoding='utf-8')
print('Wrote 41-item remediation report and checklist')
