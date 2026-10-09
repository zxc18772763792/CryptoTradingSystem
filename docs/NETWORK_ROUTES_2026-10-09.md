# 外部接口线路与代理分流（2026-10-09）

目的：能直连的上游一律不走计流量的本机代理（FlClash，127.0.0.1:7890），只把必须走代理的留给它。

## 一、怎么分流

- `.env` 里的 `PROXY_BYPASS_HOSTS`（逗号分隔的域名后缀）在启动时并入 `NO_PROXY`（`core/utils/proxy_env.py`）。
  所有读环境变量的客户端（httpx、requests、`trust_env=True` 的 aiohttp）对这些域名直连。
- 用独立的名字，是因为系统里如果有 `NO_PROXY`，设置读取时系统变量优先，会把 `.env` 里的同名值覆盖掉。
- 显式传代理的客户端（ccxt 交易所连接）用 `bypasses_proxy()` 判断；目前只有 Gate 在名单里。
- 代理和研究模块的大模型调用、Coinglass 客户端本来就是直连（`trust_env=False` 或 aiohttp 默认）。
- 改名单后要重启网页服务**和两个新闻进程**：先停新闻进程再停网页服务，启动脚本会把三个一起拉起来。

## 二、实测（直连 vs 走代理，各 3 轮）

**直连可用，已加入名单：**

| 用途 | 域名 | 直连 | 走代理 |
|---|---|---|---|
| 大模型 | kuaipao.ai | 1.2s 3/3 | 3.9s 3/3 |
| 大模型 | fast.vpsairobot.com | 1.0s 3/3 | 1.8s 2/3 |
| 大模型 | nowcoding.ai / integrate.api.nvidia.com | 0.4–0.6s 3/3 | 4.5s |
| Coinglass | vip2.coinglass.site | 1.3s 3/3 | 3.8s 2/3 |
| 交易所 | api.gateio.ws | 0.5s 3/3 | 3.5s 1/3 |
| 交易所 | data.binance.vision（历史归档） | 0.4s 3/3 | 1.5s |
| 数据 | api.llama.fi | 1.5s 3/3 | 失败 |
| 数据 | api.alternative.me / hacked.slowmist.io / cryptocompare / FRED | 0.7–1.6s 3/3 | 2–6s，偶有失败 |
| 数据 | blockchain.info | 0.5–1.0s | 失败 |
| 新闻 | cointelegraph / decrypt / cryptopanic / bitcoinist / newsbtc / chaincatcher / 金十 / gov.cn | 0.1–4.5s | 部分失败 |
| 新闻 | api.gdeltproject.org | 能连上，但被限流 429 | 失败 |
| 通知 | open.feishu.cn | 0.07s | 0.07s |

**必须走代理（直连超时或被拒）：**
- 交易所：币安全部 API、行情推送和公告（data.binance.vision 除外）、OKX、Bybit、Upbit、Bithumb。
- 数据：CoinGecko、Polymarket。雅虎财经直连返回 403。
- 新闻：Google News、CoinDesk、CryptoSlate。
- 通知：Telegram。

**两种方式都失败：** 金色财经、巴比特、NewsAPI、Deribit、Bybit 公告页。相关采集目前拿不到数据。

国内 DNS 会把币安域名解析成假地址（Facebook 网段），所以币安只能走代理。

## 三、仍走代理的主要流量

- **币安行情推送**（ccxt `watch_tickers`，最多 16 个币，每币每秒一条完整行情）。
  - 实测 5.9KB/s，约 0.48GB/天、14.5GB/月，没有压缩，是最大的固定开销。
  - 改成精简行情（miniTicker）实测 2.0KB/s（约 5GB/月），但没有买一卖一价。
  - 减少订阅币数，流量按币数线性下降。
- **币安 REST**：K 线补拉（代理只在本地 K 线超过 10–15 分钟未更新时才拉，约 30KB/次）、行情和资金费率采集、公告轮询。
- **新闻**：Google News RSS 轮询。

## 四、同时发现的账户问题（不是线路问题）

- **Coinglass 中转 key 已失效**：10/8 到期，`/api/gateway/account` 返回 401"无效的 API Key"。所有 Coinglass 数据从 10/8 起不可用。
- **vpsairobot（GPT）余额不足**：返回 403 INSUFFICIENT_BALANCE。代理和研究模块已自动退到 kuaipao 的 deepseek-v4.1-flash。

## 五、网络变化后怎么重测

1. 用 `httpx` 分别以 `trust_env=False` 和 `proxy="http://127.0.0.1:7890"` 请求各上游的轻量接口，各测 3 轮。
2. 直连稳定的加进 `PROXY_BYPASS_HOSTS`，直连失败的移出。
3. 按第一节重启三个进程。
