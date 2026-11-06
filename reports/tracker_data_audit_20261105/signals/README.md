# CoinGlass 信号数据抽核 · 2026-11-05

原版 11 月 4 日 UTC 信号于北京时间 11 月 5 日 10:45:35.740 成仓。按单币缓存最后写入时间筛到当时已到齐的 121/131 个合约，用日常评分函数复算，24 多、24 空的代码和全部 48 个五位小数分数与账本逐项相同。复算给每个可用合约同一个正占位价格，成交价格和结算不在本项检查范围内。

14:57:25 时缓存已补齐 131 个合约的六个端点，每端点当天的预期时间戳和最近 31 个日点均有有效值。用该份完整缓存复算是 26 多、26 空。原 24 多全部保留，原 24 空保留 23 个（LTC 出榜），47 个重叠分数均变化。这是因同一时间的横截面候选池变化所致。月度版最新已成仓的 11 月 3 日 UTC 信号是 324/354；现存缓存已有全部 354 个的 31 天窗口，原版较早日期及月度版成仓时的成员与原始文件已被滚动覆盖，不能用现在的文件日期恢复。

## 独立核对 Binance USD-M

Binance 官方 `/futures/data/globalLongShortAccountRatio` 的 `period=1h` 公共数据，目标 `timestamp=1793836800000`（11 月 5 日 00:00 UTC，即 11 月 4 日信号的次日零点）。Binance 的小数乘 100 后与本地 CoinGlass 百分数比较。

| 合约 | CoinGlass 多头账户 % | Binance 多头账户 % |
| --- | ---: | ---: |
| 1000FLOKIUSDT | 56.33 | 56.33 |
| JUPUSDT | 52.29 | 52.29 |
| MOVEUSDT | 68.15 | 68.15 |
| LTCUSDT | 61.83 | 61.83 |
| BTCUSDT | 58.37 | 58.17 |

BTC 的 Binance 5m 同时点亦为 58.17%，00:35 UTC 数据为 58.37%。数值恰好一致无法证明 CoinGlass 的取样规则。将原版 121 币复算的 BTC 当日账户多空百分比从 58.37/41.63 改成 58.17/41.83 后，48 个合约都保留，有两个空头分数变化，最大绝对变化 0.00059。

## 端点与归档

- OI 用聚合美元名义持仓收盘字段，资金费用 OI 加权序列在这里仅作为标准化选币信号。清算、taker 是 Binance、OKX、Bybit 汇总美元流量；全局账户和大户持仓多空比为 Binance 具体永续合约。`frames_from_store` 把后两类的次日 00:00 数据标记为上一信号日，前四类使用信号日 UTC 零点为日线标签。
- 每日采集最新多空比走 `1h` 端点取日界时戳，旧日回补和长期研究归档是 `1d`。原版宇宙中与长期归档相同时间戳的账户多空比 1,174/7,211、大户持仓多空比 1,059/7,211 值有差异。例 2ZUSDT 2026-10-12 00:00 UTC 账户多头百分比，滚动缓存 50.17%，长期归档 49.94%。两份文件并未保留逐行发布时点，无法从现有数据判定每项差异来自周期口径还是提供方修订。样本一致性不能外推到其他来源、币种和字段。
- `1000XECUSDT` 的历史采集日志反复记录清算、taker 为空。工作树现有修复仅为这两个端点使用 `1000XEC`，OI 和资金费仍用 `XEC`。11 月 5 日 15:00 观察该文件六个端点最新仍对应 11 月 3 日信号，无法用于 11 月 4 日排名。该币不在原版 131 币中，月度池含该币。

源资料：[Binance 账户多空比](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Long-Short-Ratio)、[CoinGlass 账户多空比](https://docs.coinglass.com/reference/global-longshort-account-ratio)、[CoinGlass 大户持仓多空比](https://docs.coinglass.com/reference/top-longshort-position-ratio)、[CoinGlass 清算](https://docs.coinglass.com/reference/aggregated-liquidation-history)、[CoinGlass taker](https://docs.coinglass.com/reference/aggregated-taker-buysell-volume-history)。
