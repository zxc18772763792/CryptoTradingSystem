# 交易与风控审计修复 · 2026-10-23

本分工修复 3 个新增 P2 问题。工作基于当前未提交改动，保留之前的订单有限数检查、报价冲突拒绝和账户授权修复；旧审计 TR-01/TR-02 不计入本轮新增。未改 Alastar 冻结模块、协议或账本，未访问交易账户密钥、提交真实订单、切换运行模式或推送代码。

## 新增问题及修复

| 编号 | 触发与原行为 | 修复与位置 | 回归证据 |
|---|---|---|---|
| TRADING-01 / P2 | 对 CLOSED、REJECTED、EXPIRED 或已 CANCELED 的缓存订单再撤单，paper 路径无条件改为 CANCELED、重复发回调；live 路径仍发出撤单并可能覆盖终态。已成交记录因此可呈现撤单状态。 | `core/trading/order_manager.py:1007` 在路由前要求 OPEN；`:1025` 的 paper 内部入口再次检查 OPEN。正常部分成交撤单保留 filled、remaining、cost。 | `tests/test_team_trading_safety_20261023.py:37` 覆盖 paper/live × 4终态，断言状态和成交值不变、无连接器调用/回调；`:53` 验证 OPEN 撤单只转换一次。 |
| TRADING-02 / P2 | `strategy_allocation=0.0` 被 `or 1.0` 当作全额；字符串 `"0"` 转成0后又跳过额度检查。资金分配为0的策略仍通过新增仓位检查。 | `core/risk/risk_manager.py:1009` 使用已校验的 allocation_value，并明确拒绝0分配新增订单，不让价格误差容限放行极小订单；正常退出单仍可通过。 | `tests/test_team_trading_safety_20261023.py:82` 验证数字/字符串0和10/0.01金额均拒绝；`:90` 验证退出及正常正分配订单。 |
| TRADING-03 / P2 | `update_parameters` 逐项写字段，再转int/归一化；后面的非法字符串或NaN计数字段抛异常时，前面提高的风险阈值已生效，NaN计数也可能残留。Inf等值可静默解除部分阈值。调用失败与实际风控状态不一致。 | `core/risk/risk_manager.py:1532` 先完整转换并校验所有支持参数及别名的有限性，再在状态锁下统一应用；无效输入整包拒绝。原有有限数clamp、别名优先级和0值禁用语义保留。 | `tests/test_team_trading_safety_20261023.py:107` 覆盖16个名称 × NaN/±Inf/非法字符串，断言抛ValueError、所有阈值不变、超限订单仍被拒绝；`:126` 和`:153` 覆盖合法更新、别名、计数转换及16种0值。 |

## 验证

修复前新增测试初次运行：**77 failed, 2 passed**，分别证明终态回退、0资金分配放行和参数部分更新/非有限数接受。之后将测试初始仓位比例固定为0.1，避免本机 settings 覆盖影响风险行为断言；业务修复没有借用本机配置。

最终使用项目解释器 `F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe` 运行下列范围：**212 passed, 1 warning in 17.00s**。其中新增95项，现有订单/审计/风控114项，自治风控配置及持久化3项。warning 为现有 Starlette 对 httpx TestClient 的弃用提醒。

```powershell
& 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' -m pytest `
  tests/test_team_trading_safety_20261023.py `
  tests/test_order_manager_safety.py tests/test_order_manager_mode_routing.py `
  tests/test_audit_remediation_20261023.py tests/test_trade_risk_ml_fixes.py `
  tests/test_audit_remediation_trading.py `
  tests/test_ai_autonomous_agent.py::test_risk_report_uses_configurable_autonomy_thresholds `
  tests/test_ai_autonomous_agent.py::test_risk_manager_clamps_autonomy_daily_stop_buffer_to_daily_loss_limit `
  tests/test_ai_autonomous_agent.py::test_risk_manager_persists_autonomy_threshold_overlay -q --tb=short
```

## 参数兼容与剩余边界

所有支持参数仍接受有限数，保留原有取值归一化：仓位/日损比例限制至[0.001,1]，美元日损最低0，交易/持仓计数截断为整数且最低1，杠杆最低1，波动提醒比例限制至[0.01,1]，autonomy阈值限制至[0,1]且日损缓冲不超过日损比例。美元日损和autonomy阈值0仍为合法禁用/零阈值配置。未知参数及None仍沿用原忽略行为。

`core/` 内唯一 `update_parameters` 调用方为 `core/governance/service.py:238`，传入max_position_size/max_daily_loss_ratio/max_leverage；其合法有限0行为保持不变。API调用方的ValueError映射已反馈API分工：ai_research原有400映射；trading_runtime需对非法参数返回400/422，避免状态安全但HTTP500。

本报告验证了缓存订单已经处于终态时的撤销保护；在途live撤单和交易所最终成交的竞争仍由现有订单查询/成交对账处理，本次没有接真实交易所验证该时序。参数更新对非法输入具备整包失败语义；未把所有风控读取入口改成全局事务快照。未新增声称全项目或实盘端到端验证。
