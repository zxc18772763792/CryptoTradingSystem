# 山寨雷达数据完整性修复整理

日期：2026-06-16

## 背景

山寨雷达页面此前存在一个数据一致性问题：页面价格可能已经被实时 ticker 兜底修正，但后端排序、因子库和多资产相关性仍可能读取过期的本地 K 线。实际排查中，部分本地 4h K 线停留在 2026-02-22，而 Binance public ticker 能返回当前价格。

这会造成两个风险：

- 榜单价格和评分输入不是同一批数据源。
- 旧 K 线结果被缓存后，页面短时间内持续展示 stale local K 数据。

## 代码范围

- `web/api/altcoin.py`
- `core/research/altcoin_radar.py`
- `web/static/js/altcoin_radar.js`
- `web/asset_versions.py`
- `tests/web/test_altcoin_route.py`
- `tests/test_altcoin_radar_derivatives.py`
- `tests/test_altcoin_radar_ui_assets.py`

## 后端修复

1. 扫描入口会过滤 stale local K 线。
   - 超过当前 timeframe 硬阈值的本地 K 线不再参与本轮雷达计算。
   - 如果存在更新的 live market snapshot，则保留该币种，由 snapshot 驱动行情指标。
   - 如果没有 snapshot，则该币种不会继续进入榜单和因子计算。

2. 因子库和多资产相关性只接收新鲜 K 线币种。
   - 对只有 live snapshot 的币种，不再让 `get_factor_library()` 和 `get_multi_assets_overview()` 重新加载旧 K 线。
   - 如果本轮没有新鲜 K 线，因子和相关性输入显式跳过。

3. Binance public ticker 兜底结果显式可观测。
   - CoinGlass market snapshot 不可用时，会尝试 Binance spot/futures 24h ticker。
   - Binance 兜底无数据时会返回明确 warning，不再静默退回旧 K 线。

4. 缓存策略更严格。
   - `refresh=true` 会等待新扫描结果，不再先返回旧缓存。
   - 全部由 stale local K 构成的结果不会写入 5 分钟缓存。

## 返回字段

每一行新增或透出以下数据源字段：

- `freshness.market_source`
- `freshness.market_source_type`
- `freshness.using_market_snapshot`
- `freshness.local_market_as_of`
- `freshness.market_snapshot_as_of`
- `data_quality.market_source`
- `data_quality.market_source_type`
- `data_quality.using_market_snapshot`

这些字段用于区分当前行来自 `local_kline`、`coinglass_coins_markets`、`binance_futures_ticker_24h` 或其它 snapshot 源。

## 页面修复

山寨雷达表格和详情面板会展示 market source：

- 表格新鲜度列显示类似 `100% / 100% · futures ticker`。
- 详情数据质量区域显示 `Market Source` 和 `Market As Of`。
- `web/asset_versions.py` 中 `js/altcoin_radar.js` 升级到 `v=23`，避免浏览器继续使用旧 JS。

## 验证

已执行：

```powershell
node --check web\static\js\altcoin_radar.js
..\.conda\miniforge3\envs\crypto_trading\python.exe -m pytest tests\test_altcoin_radar_ui_assets.py tests\web\test_altcoin_route.py tests\test_altcoin_radar_universe.py tests\test_altcoin_radar_derivatives.py tests\test_altcoin_radar_events.py tests\test_altcoin_radar_narrative.py tests\test_altcoin_notification_manager.py -q
```

结果：

- JS 语法检查通过。
- 山寨雷达相关测试集通过：`93 passed`。
- 真实扫描确认默认 30 币均来自 `binance_futures_ticker_24h`，旧本地 K 线被剔除，页面显示 `futures ticker` 数据源。
