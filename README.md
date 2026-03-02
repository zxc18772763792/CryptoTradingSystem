# CryptoTradingSystem

一个面向研究与实盘演进的加密交易系统（当前以 `Binance/Gate/OKX` 接入、`模拟盘` 与 `Web 控制台` 为主），包含：

- 多交易所连接与账户视图
- 策略注册/参数编辑/并行实例运行
- 数据页面（秒级到日级 K 线、缺口自动补数）
- 回测页面（单策略、多策略对比、参数优化）
- 高级研究页面（因子库、相关性、情绪、研究结论）
- 新闻页面（实时新闻流、结构化事件、多颗粒度统计）
- 高频研究主线（5m 永续、多空、成本模型、funding、报告脚本）

## 1. 当前版本定位

本仓库当前版本已经具备较完整的“研究 + 控制台 + 模拟执行”闭环，适合作为：

- 策略研究与筛选工作台
- 模拟盘验证平台
- 实盘接入前的工程化底座

说明：
- 保持了原有 live trading 逻辑接口（增量改造，不推翻）
- 大量增强集中在：回测、研究、新闻、前端控制台、状态稳定性

## 2. Python 与环境要求

- 推荐 Python：`3.11`
- Windows PowerShell 5+（或 PowerShell 7）
- 可选：Conda（推荐）

## 3. 安装

### 3.1 Conda（推荐）

```powershell
conda create -n crypto_trading python=3.11 -y
conda activate crypto_trading
pip install -r requirements.txt
```

### 3.2 venv

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 4. 配置

1. 复制配置模板：

```powershell
Copy-Item .env.example .env
```

2. 在 `.env` 中填写需要的密钥（不要提交到 GitHub）

常见项（按需）：

- 交易所 API Key（Binance / Gate / OKX）
- `ZHIPU_API_KEY`（新闻摘要/结构化事件使用 GLM）
- 新闻源 Key（如 `NEWSAPI_KEY`、`CRYPTOPANIC_TOKEN`，按需启用）

## 5. 启动方式

### 5.1 Web（推荐）

```powershell
python main.py --mode web --trading-mode paper
```

或：

```powershell
python main.py --mode web --trading-mode live
```

默认端口：`8000`

访问地址：

- 主控制台：`http://127.0.0.1:8000/`
- 新闻页：`http://127.0.0.1:8000/news`
- FastAPI 文档：`http://127.0.0.1:8000/docs`

### 5.2 CLI

```powershell
python main.py --mode cli --trading-mode paper
```

### 5.3 Windows 开发脚本

```powershell
.\scripts\dev_web.ps1
.\scripts\test.ps1
```

## 6. 页面功能概览（当前版本）

## 6.1 仪表盘

- 账户总览（真实账户 / 虚拟账户）
- 资产净值曲线与币种分布
- 风险监控（风险等级、熔断、日内总盈亏、浮盈亏、已实现盈亏）
- 交易所连接状态
- 未结构化新闻流（含结构化事件活跃度小图）

备注：
- 状态徽章使用 `/api/status` 缓存与前端独立轮询，避免“状态延迟”误报

## 6.2 策略页面

- 已注册策略（按分类折叠显示、卡片化）
- 策略参数编辑（点击卡片即加载）
- 支持：
  - 启动/停止
  - 复制新实例
  - 删除实例
  - 一键清空
  - 时间周期（`1m/5m/1h...`）配置
  - 运行时长限制（分钟）
- 同一策略可多实例并行（不同参数）

## 6.3 数据页面

- 多粒度 K 线（默认 `1m`）
- 支持秒级周期（`1s/5s/10s/30s`）
- K 线时间轴与右侧刻度显示已修复
- 秒级缺口自动补 `1s` 后再重采样（提升 `10s` 连续性）

## 6.4 回测页面

支持三类核心操作：

1. 普通回测（单策略）
2. 多策略对比
3. 参数优化

已增强：

- 分钟级区间选择（`datetime-local`）
- 区间锁定标识（普通回测 / 对比 / 优化）
- 多策略对比支持：
  - 自选策略集合
  - 预设保存/加载（浏览器本地）
  - 按来源加载（策略库全部 / 已注册策略去重）
  - 预优化后再对比
  - 排行榜可点击预览（联动上方回测图）
  - 一键注册收益第一/前3策略为新实例
- 参数优化支持：
  - 胜率 / 最大回撤 / 收益率联合图
  - 2 参数热力图（参数敏感性）
  - 一键回填最佳参数到策略参数编辑
  - 一键按最佳参数注册新实例（可并行运行）

## 6.5 高级研究页面

- 因子实验室（筛选 / 排序 / 导出）
- 因子相关性矩阵
- 多币种收益相关性矩阵
- 市场情绪仪表盘
- 研究结论与推荐策略卡
- 多粒度、多币种研究（默认已扩展到 30 币种研究宇宙）

说明：
- 因子库已扩展（31 因子）
- 因子标签中 `偏多` 为绿色、`偏空` 为红色

## 6.6 新闻页面 / 新闻 Tab

数据流（当前）：

- `GDELT + Jin10 + RSS`（按可用性组合）
- 可选 GLM 标题摘要/情绪判断（失败自动快速回退）

能力：

- 未结构化新闻流 + 结构化事件流
- 顶部统计支持颗粒度切换（当前列表 / `5m/15m/1h/4h/1d`）
- 结构化事件统计分析（多时间颗粒度）
- 统计口径说明（避免顶部统计与来源统计混淆）

## 7. 高频研究（5m Binance 永续，多空）

本仓库已加入“高频研究主线”（不改 live 下单接口）：

- 时间序列因子库：`core/factors_ts/*`
- 多因子高频策略：`strategies/quantitative/multi_factor_hf.py`
- 高频成本模型：
  - `maker/taker`
  - 动态滑点（`atr_pct / realized_vol / spread_proxy`）
  - 资金费率（可选）
- 研究脚本六件套：`scripts/research/*`

### 7.1 一键运行研究报告（推荐）

```powershell
python scripts/research/all_reports.py --exchange binance --symbol BTC/USDT --timeframe 5m --days 30 --config config/strategy_multi_factor_hf.yaml
```

输出示例目录：

- `data/reports/YYYYMMDD_hf_research/`

### 7.2 预拉取资金费率缓存（推荐）

```powershell
python scripts/research/pull_funding_cache.py --symbols BTC/USDT,ETH/USDT --days 180 --source auto
```

缓存路径：

- `data/funding/binance/*_funding.parquet`

### 7.3 数据覆盖审计（30 币种多粒度）

```powershell
python scripts/research/audit_universe30_local_data.py --exchange binance --timeframes "1m,5m,15m,1h,4h,1d"
```

## 8. 文档（已整理）

- 回测逻辑：`docs/backtest_logic_current.md`
- 因子公式：`docs/factor_formulas.md`
- 适配器架构：`docs/architecture_adapters.md`
- 开源组件选型：`docs/open_source_reference.md`

## 9. 目录结构（核心）

```text
config/        配置
core/          核心模块（交易/风控/回测/新闻/研究/适配器）
strategies/    策略实现（技术/量化/宏观/套利）
web/           FastAPI + 前端模板 + API
scripts/       开发脚本 / 研究脚本 / 数据维护脚本
docs/          项目文档
tests/         测试
data/          本地数据（已加入 .gitignore）
logs/          日志（已加入 .gitignore）
main.py        入口
```

## 10. 测试与开发检查

### 10.1 运行测试

```powershell
pytest -q
```

### 10.2 常用脚本

```powershell
.\scripts\cleanup_repo.ps1
.\scripts\test.ps1
.\scripts\dev_web.ps1
```

## 11. Docker（可选）

```powershell
docker compose build
docker compose up -d
```

## 12. 注意事项与风险提示

1. 本项目用于研究、开发与模拟验证，实盘交易风险自负
2. 服务启动后前几十秒可能出现状态延迟（交易所初始化阶段），属于正常现象
3. 新闻摘要依赖外部 LLM 接口，若超时会自动回退到快速模式
4. `.env`、`keys.txt`、`data/`、`logs/` 已加入 `.gitignore`，请勿手动上传敏感信息

## 13. Git 提交建议（当前仓库已初始化）

```powershell
git add .
git commit -m "your message"
git push
```


## News Worker Notes

Environment variables:

- `NEWS_ENABLE_CHAINCATCHER_FLASH=1`
- `NEWS_ENABLE_BINANCE_ANNOUNCEMENTS=1`
- `NEWS_ENABLE_OKX_ANNOUNCEMENTS=1`
- `NEWS_ENABLE_BYBIT_ANNOUNCEMENTS=1`
- `NEWS_ENABLE_CRYPTOCOMPARE_NEWS=1`
- `CRYPTOCOMPARE_API_KEY=` optional, collector still works without it with stricter rate limit
- `NEWS_PULL_SYNC_LLM=1` keeps `/ingest/pull_now` backward-compatible with sync extraction
- `NEWS_LLM_MIN_IMPORTANCE=35`

Run examples:

```powershell
python -m uvicorn core.news.service.api:app --host 0.0.0.0 --port 8008
python -m core.news.service.worker --once
python -m core.news.service.worker
python -m core.news.service.llm_worker
```

Optional one-click startup with dedicated news worker:

```powershell
$env:START_NEWS_WORKER = '1'
.\_once.ps1
```
