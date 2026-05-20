# Phase 3 · 成本悲观度核实 + 边际敏感性(独立任务)

> 上游计划:`docs/STABILITY_PROFITABILITY_IMPROVEMENT_PLAN_2026-05-20.md`
> 前置条件:Phase 1(数据 UTC)与 Phase 2(回测=实盘)已落地。

## 为什么单独做

盈利在手续费/滑点/资金费的边际上决定生死。回测如果用过于乐观的成本默认,
edge 看起来真实、上线后被费用吃掉。但成本的"合理悲观值"最好用一段**真实
成交日志**校准——而真实日志要 Phase 1/2 上线运行一段时间后才有意义。
因此这阶段适合在 Phase 1/2 上线观察后单开会话完成。

## 目标

1. 把回测的 fee / slippage / funding 默认改成**比实盘悲观**(下偏不下偏看真实
   成交)。
2. 增加"成本敏感性"报告:同策略在成本 ×0.5/×1.0/×1.5/×2.0 下的 Sharpe /
   Return / DSR 是否仍存活;edge 在成本 +50% 下消失的策略需要在 UI 显式标红。
3. 加 CI 断言:任何**新策略候选**晋升 paper/live 前必须 pass 成本敏感性。

## 待审计文件

- `core/backtest/cost_models.py` — `fee_rate`, `slippage_rate(flat|dynamic)`,
  `microstructure_proxies`。
- `core/backtest/backtest_engine.py` — `BacktestConfig` 字段 `fee_model` /
  `slippage_model` 默认。
- `core/research/validation_gate.py` — 晋升时是否检查成本下 edge 是否存活。
- `web/api/backtest.py` — 默认 `commission_rate=0.0004`, `slippage_bps=2.0` 是否
  与真实交易所(Binance Spot taker 0.10%、futures 0.04%/maker、Gate 类似)
  一致;若有特别低的默认需要拉高。

## 第一步:与真实成交校准

需要 Phase 1/2 上线后采样:
- 从 `risk_manager._trade_history` 或订单成交回报里聚合 **realized fee_rate**
  per (exchange, symbol, side, order_type)。
- 滑点 = `|fill_price - signal_price| / signal_price`,按 timeframe 聚合 P95。
- Funding:抓最近 30 天的真实 funding payment / notional。

把 P95(或更悲观的 P99)写回成本模型默认。

## 第二步:成本敏感性报告

在 `core/research/strategy_research.py` 的 backtest 结果对象上加
`cost_sensitivity: {0.5: {...}, 1.0: {...}, 1.5: {...}, 2.0: {...}}`,每一档存
sharpe/return/max_dd/dsr。`web/static/js/ai_research.js` 候选详情面板新增
"成本敏感性"小图(线状),`1.5×` 下 sharpe<0 的策略标红。

## 第三步:晋升闸门

`core/research/validation_gate.py`:promote 时若 `cost_sensitivity[1.5×].sharpe`
< 阈值(例如 0.3)→ 强制 downgrade 到 shadow。

## 风险/回滚

- 拉高默认成本会让所有历史候选 edge 数字下降,**这是预期**——它们之前在虚高。
- 回滚:`settings.BACKTEST_COST_PESSIMISTIC` flag,默认 True;False 走旧值。

## 验证

- 现有 `tests/test_backtest_cost_models.py` 必须仍 pass(或更新基线)。
- 新增 `tests/test_cost_sensitivity_report.py`:同一策略不同成本档下 sharpe
  单调递减;0.5× 比 2.0× 高。
- 至少一个真实候选在 1.5× 下被降级,在测试 fixture 里固化。

## 验收清单

- [ ] 成本默认从真实成交 P95 推导,记入 commit message。
- [ ] 候选 API 响应包含 `cost_sensitivity`。
- [ ] UI 显示并标红高成本敏感策略。
- [ ] validation_gate 拒绝高成本敏感策略晋升 live。
- [ ] 测试覆盖且无回归。
