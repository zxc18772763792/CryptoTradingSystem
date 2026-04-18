# 套利页风控台落盘与实施计划

## 摘要
- 计划文档目标落点：`E:\9_Crypto\crypto_trading_system\docs\ARBITRAGE_RISK_CONSOLE_PLAN_2026-04-18.md`
- 实施顺序固定为：先保存计划文档，再做后端风控闸门，再接前端风控台，最后补联调与测试。
- 页面目标仍然是把套利页改造成“先判断能不能做、为什么不能做、亏损主要亏在哪”的风控控制台，而不是继续扩策略入口。
- v1 只主推 `PairsTradingStrategy` 和 `FamaFactorArbitrageStrategy`；`CEX/Triangular/DEX/FlashLoan` 统一降级到“高级 / 仅实时验证”。

## 实施变更
- 第一步：落盘计划文档
  - 将当前方案整理为项目内文档，文件名使用 `ARBITRAGE_RISK_CONSOLE_PLAN_2026-04-18.md`。
  - 文档内容保留：目标、状态卡定义、接口约定、实施顺序、测试清单、默认阈值。
- 第二步：先做后端风控闸门
  - 新增聚合接口 `GET /api/research/arbitrage-readiness`，统一返回 `data_status`、`backtest_status`、`cost_status`、`entry_status`、`recommended_action`。
  - 扩展现有 `pairs-ranking` 返回值，增加 `entry_state`、`entry_ready`、`blocked_reasons`，让前端能明确区分“研究高分”和“当前可执行”。
  - 修复数据路径依赖当前工作目录的问题，改成稳定的项目绝对路径或可注入根路径，避免错误地显示“无数据”。
- 第三步：接前端风控台
  - 套利页首屏新增 4 张状态卡：`数据就绪`、`回测可信度`、`成本压缩`、`当前入场状态`。
  - 将“配对币种扫描榜”拆成 `研究候选` 和 `当前可开仓候选` 两层，只有同时通过闸门的标的进入执行候选区。
  - `Pairs/Fama` 默认展开；`CEX/Triangular/DEX/FlashLoan` 折叠并显示 `仅实时验证`，禁用回测主 CTA。
  - 所有策略卡统一显示状态 badge：`可回测`、`近似回测`、`仅实时验证`、`数据不足`、`成本过高`、`当前仅观察`。
- 第四步：补亏损诊断和 CTA 规则
  - `gross <= 0` 显示“结构边际不足”。
  - `gross > 0 且 net <= 0` 显示“成本吞噬边际”。
  - 样本太少、`quality_flag` 异常或 `anomaly_bar_ratio` 过高时，显示“结果不可信”。
  - 当前 `signal_bias=watch` 或未触发入场阈值时，只允许加入观察，不允许给开仓建议。

## 接口与状态约定
- `arbitrage-readiness` 返回字段：
  - `data_status`: `ready`、`reasons[]`、每条腿 bars、重叠 bars、最新时间戳
  - `backtest_status`: `supported`、`mode`（`spread_approx` / `factor_backtest` / `realtime_only`）、`reason`
  - `cost_status`: `gross_total_return`、`net_total_return`、`cost_drag_return_pct`、`estimated_trade_cost_usd`、`pass|warn|fail|unknown`
  - `entry_status`: `watch|long_spread|short_spread|no_trade`、`entry_ready`、`reasons[]`
  - `recommended_action`: `先补数据` / `先回测` / `加入观察` / `允许开仓`
- 前端统一使用一组闸门状态驱动按钮和文案：
  - `research_candidate`
  - `backtest_required`
  - `live_only`
  - `cost_blocked`
  - `entry_ready`
  - `blocked_reasons[]`

## 测试方案
- API 测试覆盖 4 类结果：`数据不足`、`仅实时验证`、`成本失败`、`允许开仓`。
- 数据路径测试要覆盖不同启动目录，确保都能读到同一批历史数据。
- 前端测试要验证：
  - `Pairs` 下高分但 `watch` 的配对不会被标成可执行。
  - `CEX/Triangular/DEX/FlashLoan` 必须展示为 `仅实时验证`。
  - `gross>0 && net<=0` 时首页诊断直接显示“成本吞噬边际”。
  - 没有回测结果时 `cost_status=unknown`，主 CTA 固定为“先回测”。

## 默认假设
- 计划文档保存路径默认采用项目内 `docs`，不使用工作区根目录 `docs`。
- `entry_ready` 默认要求当前 `abs(z_score) >= entry_z_score`，且 `signal_bias` 不是 `watch`。
- 最小样本默认使用：每条腿至少 `1000` bars、重叠不少于 `500` bars；不足时只能研究，不能给执行建议。
- 成本阈值默认使用：`cost_drag < 30%` 为 `pass`，`30%-60%` 为 `warn`，`>60%` 或 `net<=0` 为 `fail`；若没有最近回测，则一律 `unknown` 并要求先回测。
- 当前仍处于 Plan Mode；真正的“落盘 + 实施”将在退出 Plan Mode 后按上述顺序执行，不再额外改动方案。
