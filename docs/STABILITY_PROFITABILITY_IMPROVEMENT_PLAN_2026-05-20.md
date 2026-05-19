# 稳定盈利导向 · 系统改进计划 (2026-05-20)

> 指导原则:工程不能"保证盈利",只能保证系统**诚实**——实盘行为 = 已验证的预期,且亏损期不失控。
> 盈利来自策略 edge;工程的职责是不让 edge 被数据/执行/度量的谎言吞掉。
> 严格按阶段顺序执行,不跳阶段。地基不稳时优化上层都是噪声。

## 现状已确认的关键问题

1. **数据时区污染(根因已定位)**:`scripts/maintain_top100_data.py:258` 用
   `datetime.fromtimestamp(ms/1000)`(无 `tz=`)写入**本地时间(UTC+8)**的 K线
   parquet。`_normalize_parquet_frame_index` 当前用启发式(未来时间→减 8h)兜底,
   是补丁叠补丁,且在"按收盘打标签"等场景下可能误平移污染最新数据。
2. **回测 ≠ 实盘**:回测页主路径用 `web/api/backtest.py:_build_positions`
   (按策略名向量化重写指标),实盘用 `strategy.generate_signals`(策略类真实
   逻辑)。实测方向吻合度 RSI 51% / Bollinger 70% / MACD 46% / Momentum 64%。
   → 用回测数字做选策略/分配资金 = 在虚构地基上下注。
3. **信号全局压制**:`_emit_signals` 冲突窗口键为 `(symbol, exchange)`,跨**不同
   策略**丢弃较弱信号,违反策略隔离,可能静默杀掉有效策略入场。
4. **策略静默死亡**:`runtime_limit` 到期自动平仓且不重启,生命周期无监督。
5. **成本悲观度未核实**:盈利在手续费/滑点/资金费的边际上决定生死。

## 已完成(前序提交)

- `d8a57db` 时区启发式归一 + 实时拉取退避
- `f03e14b` 行情缓存跨策略共享(限流队列打爆 → N→1)
- `1d4d160` 回测窗口对齐实盘 + 一致性测试(MACD 背离记 xfail)

这些是止血,本计划做根治。

---

## Phase 1 — 数据完整性根治【已批准,立即执行】

目标:K线 parquet 全链路统一 tz-naive UTC,消除启发式依赖。

### 1.1 修复所有 K线 parquet 写入端为 UTC
- `scripts/maintain_top100_data.py:258` — `datetime.fromtimestamp(r0/1000.0)`
  → `datetime.fromtimestamp(r0/1000.0, tz=timezone.utc)`(或 `pd.to_datetime(unit=ms, utc=True)`)。
- 审计其余 `save_klines_to_parquet` 调用方并逐一确认 UTC:
  `core/data/historical_data.py`(经 connector.get_klines,已 UTC ✓)、
  `core/ai/autonomous_agent.py:2164`、`web/api/data.py:3915`、
  `scripts/research/audit_universe30_local_data.py:260`、
  `core/data/binance_archive.py`(已 `utc=True` ✓)。

### 1.2 一次性存量迁移脚本 `scripts/migrate_parquet_klines_to_utc.py`
- 扫描 `data/historical/**/*_parts/*.parquet`(及其它 K线 parquet 根)。
- 检测本地时间分区:与同 symbol 可信 UTC 源(如 binance_archive 产物)或
  "整帧 max 显著超 now_utc"判定;判定为本地则整体 `- 8h`。
- 安全:`--dry-run` 先报告;写入前 `.bak` 备份(或写入旁路目录再原子替换);
  幂等(已 UTC 不动);输出每文件 before/after 与判定依据。
- 验证:迁移后随机抽样 last bar 与真实 UTC 对齐(gap 在合理范围)。

### 1.3 读取端从"静默兜底"改为"告警+断言"
- `_normalize_parquet_frame_index`:保留 UTC 修正作为安全网,但命中本地→UTC
  平移时 `logger.warning` 并计数,暴露仍在写本地时间的写入端;
  新增 `settings.PARQUET_TZ_STRICT`(默认 False)开启后改为 raise,便于 CI/排障。

### 1.4 验证
- 重载真实 BTC/ETH 1m/15m parquet,断言 last bar 与 `utcnow` gap 合理、幂等。
- 跑 `tests/` 中 data/backtest/strategy 相关全量,无回归。
- 新增 `tests/test_parquet_tz_integrity.py`:写本地时间→读取应得 UTC;已 UTC 不变。

### 回滚
写入端为单行可逆;迁移脚本有 `.bak`;读取端改动纯增告警/可选断言。

---

## Phase 2 — 回测 = 实盘(单一代码路径)【高爆炸半径,需显式 GO/NO-GO】

> ⚠ 此阶段会**作废全部历史回测数值**(旧数值本就失真)。执行前必须用户确认承受该代价。
> 本计划仅记录方案,**不在未确认前执行**。

### 方案
- 回测页主路径从 `_build_positions(name,df)` 切换到真实
  `strategy.generate_signals`(`core/backtest/backtest_engine.py` 已具备该路径,
  且窗口已在 `1d4d160` 对齐实盘尾窗)。
- `_build_positions` 降级为仅"研究态快速向量扫描",并在 UI/响应标注
  "approx, not execution-grade";正式回测/验证/晋升一律走真实路径。
- 一致性测试阈值收紧:移除 50% 糖衣门槛,所有低吻合策略进**显式已知缺陷清单**,
  达 parity 才出清单。
- 性能:真实路径逐 bar replay 较慢 → 加进度/缓存,必要时并行。

### 风险/回滚
历史回测对比失效(不可逆,需用户确认);代码层面切换可 feature-flag 回退。

---

## Phase 3 — 验证现实化【Phase 1 后执行】

- 回测成本必须**比实盘悲观**:核对 `core/backtest/cost_models.py` 的
  fee/slippage/funding 默认,确保 ≥ 实际成交边际;加成本下限断言。
- 复用既有 OOS / purged walk-forward / DSR / validation_gate,但其结论只有在
  Phase 1+2 之后才可信;补"成本敏感性"报告(成本 ±50% 下 edge 是否存活)。

## Phase 4 — 风控接入实盘下单链路【Phase 1 后执行】

- 冲突窗口键改为含策略名 `(strategy, symbol, exchange)`,恢复策略隔离;
  跨策略冲突改为"记录+告警"而非静默丢弃(或可配置)。
- 组合级 + 单策略级回撤熔断接到执行链路(非仅研究端);
  既有 CUSUM 衰减监控 / 相关性过滤确认作用于实盘 candidate。
- `runtime_limit` 生命周期监督:到期事件显式通知 + 可选自动续期策略,
  杜绝"策略悄悄停了仍以为在跑"。

## Phase 5 — 纸盘→实盘对账闸门【Phase 1–4 后执行】

- 持续记录每策略"实现 PnL vs 回测预期",偏差超阈值自动降级 shadow;
- 仅持续吻合的策略可升真实资金;小资金验证,吻合再加码。

---

## 执行节奏

冻结新功能/新策略,先打通"数据→回测→验证"可信链路(Phase 1 必做,Phase 2
需确认)。Phase 1 完成后回报并在 Phase 2 GO/NO-GO 处停下等用户决策。
