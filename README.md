# Crypto Trading System

多交易所加密交易系统（现货/模拟盘优先），包含数据采集、策略执行、风控、回测与 Web 控制台。

## Python 版本

- 推荐并已验证：`Python 3.11`
- 不建议使用 3.9 运行本仓库（已统一清理 3.9 相关缓存）

## 环境要求

- Windows PowerShell 5+（或 PowerShell 7）
- Python 3.11
- 可选：Conda（推荐）

## 安装依赖

### Conda（推荐）

```powershell
conda create -n crypto_trading python=3.11 -y
conda activate crypto_trading
pip install -r requirements.txt
```

### venv

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 配置

1. 复制模板：

```powershell
Copy-Item .env.example .env
```

2. 按需填写你的 API Key（请勿提交 `.env`）。

## Windows 开发快速启动

### 启动 Web（自动准备环境）

```powershell
.\scripts\dev_web.ps1
```

### 运行测试

```powershell
.\scripts\test.ps1
```

### 清理仓库缓存/临时文件

```powershell
# 仅预览将删除内容
.\scripts\cleanup_repo.ps1 -DryRun

# 实际清理（默认清理 7 天前日志）
.\scripts\cleanup_repo.ps1
```

## 运行

```powershell
# Web 模式
python main.py --mode web

# CLI 模式
python main.py --mode cli

# 指定交易模式（paper/live）
python main.py --mode web --trading-mode paper
python main.py --mode web --trading-mode live
```

## API 文档

服务启动后访问：

- `http://localhost:8000/docs`

## 测试

```powershell
pytest -q
```

## Docker

```powershell
docker compose build
docker compose up -d
```

## 项目结构（核心）

```text
config/        # 配置
core/          # 核心模块
strategies/    # 策略实现
web/           # FastAPI + 前端模板
data/          # 本地数据
logs/          # 日志
tests/         # 测试
scripts/       # 开发脚本
main.py        # 入口
```

## Web 页面布局（当前）

- `http://localhost:8000/`：主控制台（仪表盘/交易/策略/数据/研究/回测）
- `http://localhost:8000/news`：独立新闻页（实时新闻流、事件标签、情绪/影响分数）
- 仪表盘内新增 `实时新闻` 卡片：自动轮询更新，可手动触发拉取

## 新闻数据流（只读，不下单）

- 新闻拉取：`CryptoPanic + GDELT + NewsAPI -> news_raw`（按可用性自动降级）
- 事件抽取：`GLM5(可选) / 规则回退 -> news_events`
- Web 接口：`/api/news/*`（`latest`、`summary`、`events`、`pull_now`）
- 说明：新闻模块仅提供信息与信号素材，不直接触发交易执行

### 消息面环境变量（可选）

- `NEWSAPI_KEY`：NewsAPI key（demo/dev 用）
- `CRYPTOPANIC_TOKEN`：CryptoPanic token
- `CMC_API_KEY`：CoinMarketCap key（可用于后续市场因子增强）
- `NEWS_ENABLE_GDELT/NEWS_ENABLE_NEWSAPI/NEWS_ENABLE_CRYPTOPANIC`：源开关
- `NEWS_PULL_INTERVAL_SEC`：后台拉取间隔（秒）
- `NEWS_PULL_SINCE_MINUTES`：每次拉取回看窗口（分钟）
- `NEWS_PULL_MAX_RECORDS`：每次最大拉取条数

## 风险提示

- 本项目仅用于研究与开发验证。
- 实盘交易有风险，请先在模拟盘充分验证。

## 高频研究（5m Binance 永续，多空）如何跑

说明：
- 本次升级不改 live 下单接口，只增强研究与回测模块。
- 新增 `MultiFactorHFStrategy`（配置驱动）与 `core/factors_ts`（时间序列因子库）。
- 回测引擎支持 `maker/taker`、动态滑点、PnL 成本分解；资金费率默认关闭（可开启）。现在支持通过 `FundingRateProvider` 自动从本地缓存 / Binance 公共 funding 接口补齐。

### 1) 运行 5m 高频研究六件套（建议先本地准备好 Binance 5m 数据）

```powershell
python scripts/research/all_reports.py --exchange binance --symbol BTC/USDT --timeframe 5m --days 30 --config config/strategy_multi_factor_hf.yaml
```

### 1.1) 预先拉取并缓存永续资金费率（推荐）

```bash
python scripts/research/pull_funding_cache.py --symbols BTC/USDT,ETH/USDT --days 180 --source auto
```

- 缓存位置：`data/funding/binance/*_funding.parquet`
- 当 `BacktestConfig(include_funding=True)` 且行情数据缺少 `funding_rate` 列时，回测引擎会尝试使用该缓存自动补齐

输出目录示例：
- `data/reports/YYYYMMDD_hf_research/`

包含（轻量版）：
- `data_quality_report.csv`
- `factor_edge_table.csv`
- `factor_corr_heatmap.png`
- `cost_sensitivity_table.csv`
- `cost_curve.png`
- `leaderboard.csv`
- `walk_forward_stability.png`
- `robustness_report.md`

### 2) 单独跑数据质量 / 成本敏感性 / 因子研究

```powershell
python scripts/research/data_qa.py --exchange binance --symbol BTC/USDT --timeframe 5m --days 30
python scripts/research/cost_sensitivity.py --exchange binance --symbol BTC/USDT --timeframe 5m --days 30
python scripts/research/factor_study.py --exchange binance --symbol BTC/USDT --timeframe 5m --days 30 --horizon-bars 3
```

### 3) 调整多因子组合（只改 YAML）

默认配置文件：
- `config/strategy_multi_factor_hf.yaml`

可调项：
- `factors`：因子组合/权重/transform
- `enter_th / exit_th`：滞回阈值
- `gates`：波动/点差/成交量过滤
- `cooldown_bars`
- `position_sizing`

### 4) 关键限制（当前版本）

- 资金费率结算支持已加入回测引擎，但默认关闭；开启后会优先读取行情数据中的 `funding_rate` 列，若缺失且配置了 `FundingRateProvider` 则自动回填。
- `spread_proxy` 使用 `(high-low)/close`，是无盘口条件下的粗代理，适合研究筛选，不等价于真实盘口点差。
