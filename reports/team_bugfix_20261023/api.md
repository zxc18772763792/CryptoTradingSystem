# API 审计与修复 · 2026-10-23

范围：`web/api/` 的权限、请求数值校验与返回状态；保留现有未提交修改，未修改 Alastar、交易/风控核心或 `web/main.py`。

本轮新增 4 项可复现问题，均已修复。它们不重复旧审计已修复的 SEC-01 至 SEC-05：其中 API-01 是另一个独立写入口，修改的是止损建议策略文件。

## API-01 · P2 · 策略管理权限可写共享止损策略

- 触发：OPERATOR 或 RESEARCH_LEAD 对 `POST /api/trading/analytics/stoploss/policy` 写入任意 policy。
- 原因：`require_sensitive_ops_permissions("approve_risk_change", "manage_strategies")` 按 OR 判定；只有策略管理权限也可通过。
- 修复前：这两种角色均 HTTP 200，写服务被调用；均无 `approve_risk_change` 权限。
- 修复：写入口只接受 `approve_risk_change`，RISK_OWNER/SYSTEM 仍可写；原只读入口不受影响。
- 影响边界：该文件影响止损建议，不据此声称该接口直接改变实盘止损单。
- 文件：`web/api/trading_analytics.py`。

## API-02 · P2 · 显式零资产被旧资产替代，低资产告警可能漏报

- 触发：风险快照资产为 5000，请求 `POST /api/notifications/evaluate` 提交 `total_usd: 0`。
- 修复前：`total_usd or snapshot` 将合法的 0 改回 5000，低于 100 的资产告警因此不匹配。
- 修复：仅在 None/未提供时回退风险快照；保留 0 与负资产。
- 测试：校验遗漏、null、0、负值四种输入；把返回上下文送入真实 `equity_below` 规则判断，验证告警条件。
- 文件：`web/api/notifications.py`。

## API-03 · P2 · 非有限资产进入通知评估并导致响应 500

- 触发：同一评估入口提交字符串 `NaN`、`Infinity` 或 `-Infinity`，Pydantic 原本会转成非有限 float。
- 修复前：调用规则评估后，响应序列化抛出 `ValueError: Out of range float values are not JSON compliant`；即使请求报错，规则副作用也已可能发生。
- 修复：在请求字段约束中拒绝非有限值，返回 HTTP 422，并确认规则评估从未被调用。
- 文件：`web/api/notifications.py`。

## API-04 · P2 · 通知测试事件在滚动缓冲区满后丢失

- 触发：已有 999 或 1000 条事件，`POST /api/notifications/test` 在两个通道产生两条新事件。
- 修复前：通过旧列表长度截取新窗口，分别只返回 1 条或 0 条事件；管理器会自动只保留末尾 1000 条，列表长度不再是游标。
- 修复：保留发送前事件对象引用，使用对象身份识别新窗口中的新增事件，避免旧条目淘汰导致偏移错误。
- 测试：初始事件数为 0、10、999、1000，均返回两个通道完整的新事件。
- 文件：`web/api/notifications.py`。

## 验证

解释器：`F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe`。

- 修复前新回归：`tests/test_team_api_bugfix_20261023.py`：8 failed / 9 passed（证明上述四类问题，失败覆盖参数变体）。
- 初次修复后：新回归 + `tests/web/test_notifications_altcoin_context.py` + `tests/test_sensitive_api_auth.py` + `tests/test_trading_route_modules.py` + `tests/test_market_ws_api_price_paths.py`：94 passed，13.59 秒。
- 增强低资产实际规则断言后再次运行新增回归：17 passed，9.99 秒。
- 交易 agent 的数值原子校验变更落地后，补充下述 API 错误映射回归，最终相同 5 个测试文件合跑：109 passed，12.81 秒；其中本轮 `tests/test_team_api_bugfix_20261023.py` 共 32 项全部通过。
- 已有 1 个 Starlette TestClient/httpx 弃用提示，不影响结果；未更改依赖。
- 所有身份、事件、资产均为合成数据；真实发送、持久化写服务均替换为隔离桩，未实盘下单、使用账户密钥、推送或 stage。

## 交易/风控修复的 API 配套（不重复计为新增发现）

交易 agent 新增 `RiskManager.update_parameters` 对非法数值的整包拒绝。经跨模块复核，`RiskUpdateRequest` 原本允许字符串 NaN/Infinity 转成非有限 float，`POST /api/trading/risk/params` 未捕获新的 ValueError，会响应 500。

已在 `web/api/trading_runtime.py` 将该校验异常映射 HTTP 400，保持 AI 风控入口的同类错误语义；失败请求不会清缓存、审计成功或读取新报告。新增 5 个可更新浮点字段 × 3 个非有限值共 15 项回归，使用真实 RiskManager 校验方法但不构造生产管理器、不触及持久化。修复前首个反例稳定抛 ValueError；修复后均 HTTP 400 且模拟旧杠杆值保持不变。

## 未解决问题及边界

- 本轮修复未更改通知管理器发送/持久化生命周期，未验证外部 Telegram/飞书可用性。
- 通知测试返回的是发送期间新增事件；并发后台通知仍可能一并出现。当前接口没有请求相关 ID，若要严格关联单次发送，需要另行扩展管理器协议。
- 完整项目回归、部署和运行中服务核验由主审统一执行，本报告不表示全 API 已穷尽审查。
