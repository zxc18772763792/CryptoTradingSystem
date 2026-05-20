# Crypto 交易策略调研与策略库缺口清单

更新日期：2026-05-20  
范围：现货、永续、交割合约、期权、链上/DeFi 与事件驱动策略。本文只做策略研究和系统建设建议，不构成投资建议。

## 1. 当前策略库现状

本次检查了 `config/strategy_registry.py`、`strategies/*`、`core/data/*`、`core/market_state/*` 和 Polymarket 子模块。当前注册表里有 46 个 crypto 交易策略，覆盖面已经不窄：

- 技术指标：MA/EMA、MACD、ADX、Aroon、RSI、Stochastic、Bollinger、CCI、Williams %R、Stoch RSI。
- 趋势、动量、反转：Momentum、ROC、Price Acceleration、Donchian、Bollinger Squeeze。
- 均值回归与统计套利：Z-score、Bollinger Reversion、VWAP Reversion、Half-life、Pairs Trading、Hurst。
- 横截面与多因子：FamaFactorArbitrage、MultiFactorHF、MKT/SMB/HML/MOM/REV/VOL/LIQ/QMJ/BAB 等。
- 微观结构雏形：OrderFlowImbalance、TradeIntensity、orderbook collector、market regime classifier。
- 套利框架：CEX 跨所、三角、DEX、闪电贷套利。
- 情绪/宏观/资金流：Fear & Greed、社媒、资金流、鲸鱼活动。
- ML/AI：XGBoost 方向分类、AI research/promotion pipeline。

但有一个明显特征：当前策略库更偏“方向判断”和“单腿/单策略信号”，对 crypto 最有特色的“carry、basis、期权波动率、清算/杠杆拥挤、链上 LP/MEV、事件供给冲击”还没有形成一等策略。

已有但未充分策略化的数据入口：

- `core/data/funding_rate_collector.py` 已支持 Binance、Bybit、OKX、Gate funding rate 与 Binance premium index。
- `core/data/options_collector.py` 已有 Deribit 公共期权摘要，可拿 ATM IV、skew、put/call OI。
- `core/data/coinglass_onchain.py` 已有 exchange netflow、exchange balance、whale transfer、options row normalization。
- `core/data/orderbook/orderbook_collector.py` 已有多交易所 L2 snapshot、spread、depth imbalance。
- `core/market_state/classifier.py` 已能融合 spread、imbalance、long/short、news bias 做 regime。

## 2. 外部资料要点

几个和当前策略缺口最相关的资料结论：

- Funding 和 basis 是 crypto 永续/期货市场的核心状态变量。Binance 官方提供 funding history、funding cap/floor、basis、OI、taker buy/sell、top trader long/short 等公开接口，足够支撑 funding carry、basis carry、拥挤度与清算风险策略的研究数据层。
- 2025 年关于 funding arbitrage 的开放论文专门评估了 CEX/DEX 永续市场中的 funding rate arbitrage 风险收益，2026 年 SSRN 论文也把 funding rate 作为永续合约中的反馈机制，而不是简单的被动现金流。
- Deribit 公共接口能返回全币种衍生品摘要，包含 OI、best bid/ask、mark price、funding、mark IV 等字段，适合做 BTC/ETH 期权 IV/RV、skew、term structure 研究。
- Glassnode 的 Point-in-Time on-chain 指标说明了一个关键点：链上交易策略回测必须使用 PiT 或其他不可回写历史的数据，否则很容易未来函数。
- 2024-2026 年关于 MEV、L2 MEV、Uniswap v3 LP、stablecoin run/arbitrage、market making inventory risk 的资料都指向同一件事：这些策略的 alpha 不是“信号公式”本身，而是执行、风控、拥挤度、成本建模和数据时效。

Binance funding 快照用于现实校验。通过 Binance 公共市场数据在 2026-05-20 08:00 Asia/Shanghai 附近取最近 6 个 8h funding：

| Symbol | 最近 6 次 8h funding 平均值 | 粗略年化 | 最新 8h funding | 说明 |
|---|---:|---:|---:|---|
| BTCUSDT | 0.00529% | 5.79% | 0.00325% | 温和正 funding |
| ETHUSDT | 0.00510% | 5.59% | 0.00552% | 温和正 funding |
| SOLUSDT | -0.00163% | -1.78% | 0.00284% | 最近几次方向切换 |
| DOGEUSDT | -0.00022% | -0.24% | 0.00308% | 基本中性 |
| BNBUSDT | 0.00223% | 2.45% | 0.00762% | 最新一笔较高 |

这不是当前交易建议，只说明 funding 序列仍然有可筛选、可年化、可风控的横截面结构。

## 3. 最值得补进策略库的策略

### P0 - Spot/Perp Funding Carry Strategy

现状：策略库没有 delta-neutral funding carry。虽然已有 funding collector，但还没有把 funding cashflow、spot/perp hedge、手续费、滑点、杠杆、保证金和再平衡统一成策略。

核心思路：

- 当永续 funding 明显为正，做多现货/做空永续，收取 shorts funding。
- 当 funding 明显为负，做空现货或借币/做多永续，收取 longs funding。实际落地时负 funding 方向通常受借币、现货做空和流动性约束，优先级低于正 funding carry。
- 用 mark-index basis、funding 预测、历史 funding 均值/波动、OI、spread、订单簿深度过滤拥挤与执行成本。

入场条件建议：

- expected_funding_annualized > fee_annualized + borrow_cost + slippage_buffer + min_edge。
- spot/perp basis 不处于极端背离，或者背离有回归依据。
- 盘口深度满足目标仓位的 slippage-at-risk。
- 单币 funding 不是由短时挤仓造成的异常尖峰。

回测要点：

- 单独建 funding cashflow ledger，不能只靠 K 线收益。
- 需要模拟 spot leg、perp leg、再平衡误差、手续费、资金占用、保证金率和强平风险。
- 对极端行情做 stress：mark price 偏离、basis 扩大、负 funding 翻转、交易所风控限制。

落地难度：低到中。数据层已有 60% 以上，缺的是组合 PnL 和风控引擎。

### P0 - Cross-Exchange Funding Spread Strategy

现状：已有跨交易所价差套利和多交易所 funding collector，但没有“同一标的不同交易所 funding spread”的 delta-neutral 策略。

核心思路：

- 同一币种在多个交易所 funding、premium、mark-index basis 不一致时，构造低净 delta 的双永续或现货+永续组合。
- 典型方向：在正 funding 更高的一端做空永续，在负 funding 或低 funding 一端做多永续/现货，目标是收 funding spread，而不是赌价格方向。

信号：

- funding_spread_zscore、expected_next_funding_spread、basis_spread、exchange liquidity score。
- 交易所级别风险：提现/转账延迟、API 可用性、费率、保证金模式、ADL 风险。

为什么值得优先做：

- 你现有 `FundingRateCollector.fetch_all()` 天然就是跨所聚合。
- 比简单 CEX 价差套利更不依赖毫秒级搬砖，适合先做 paper/live 小仓验证。

主要风险：

- 两边价格短时脱锚，保证金压力不对称。
- 某交易所 funding cap/floor 或 funding interval 突然调整。
- 资金分布在多个交易所，账户风险和提现风险不能忽略。

### P0 - Calendar Futures Basis Carry Strategy

现状：策略库没有交割合约/季度合约 cash-and-carry。已有 perpetual basis 概念，但还没有对 fixed maturity futures 做年化基差交易。

核心思路：

- 当季度/次季度合约相对 spot 或 index 处于足够 contango，做多现货/做空交割合约，持有至基差收敛或到期。
- 当 backwardation 极端且有库存/借币能力时可反向，但实操门槛更高。

数据：

- Binance basis endpoint 提供 `basisRate`、`annualizedBasisRate`、`futuresPrice`、`indexPrice`。
- 可扩展 CME/OKX/Deribit 到期合约，做跨 venue basis curve。

回测：

- 到期日现金结算逻辑。
- rolling futures 的换仓成本。
- 现货融资成本、借贷利率、稳定币收益率机会成本。

优先级理由：

- 比裸趋势策略低相关。
- 与 funding carry 可以形成统一的 `CarryStrategyFamily`。

### P0 - Liquidation/OI Crowding Strategy

现状：已有 orderbook imbalance、long/short ratio、CoinGlass filters、market regime classifier，但没有专门的“杠杆拥挤/清算瀑布”策略。

核心思路：

- 不是单纯追 liquidation heatmap，而是把 OI 变化、funding 极值、long/short ratio、taker buy/sell imbalance、basis、spread/depth thinning 组合成拥挤度。
- 两种模式：
  1. 防守模式：拥挤度高且盘口变薄时，降低现有方向策略仓位或禁止新开仓。
  2. 交易模式：清算瀑布后 OI 快速下降、funding reset、价格偏离过度时做短线反转；若 OI 扩张伴随趋势确认，则做 breakout continuation。

信号：

- `oi_change_z`、`funding_percentile`、`basis_z`、`taker_buy_sell_ratio`、`top_trader_position_ratio`、`spread_bps`、`depth_imbalance`。

为什么和现有策略不同：

- 现有 OrderFlowImbalance 更像单一微观因子。
- 这个策略关注“杠杆结构”和“清算路径”，更适合做 portfolio gate 与 tail-risk alpha。

主要风险：

- liquidation heatmap 多为估算，不可当真实挂单。
- 高杠杆拥挤可以持续很久，过早逆势容易被趋势碾压。

### P1 - Options IV/RV Volatility Strategy

现状：已有 Deribit options collector，但没有期权策略。当前 collector 还只是 ATM IV、skew、put/call OI 的摘要级信号。

可落地方向：

- IV/RV spread：当 implied vol 低于预测 realized vol，买入跨式/宽跨式并做 delta hedge；反之谨慎卖 vol。
- Skew mean reversion：25d put/call skew 极端时，用 risk reversal 或期权价差表达。
- Term structure：近月/远月 IV 曲线异常时做 calendar spread。
- Gamma scalping：长 gamma 结合高频 delta hedge，依赖执行成本和 realized path。

必要升级：

- 拉取完整 option chain、到期日、strike、mark IV、bid/ask、Greeks、OI、volume。
- 计算 realized vol forecast、vol-of-vol、jump risk、event calendar。
- 回测必须是期权组合回测，不可把 IV 当普通方向因子。

主要风险：

- 期权 bid/ask 很宽，纸面 edge 容易被执行吃掉。
- 卖 vol 策略尾部风险大，必须有仓位上限、vega/gamma/theta limits 和事件黑名单。

### P1 - Token Unlock / Supply Event Strategy

现状：已有新闻、公告、事件采集器，但没有系统化的 unlock/listing/delisting/supply shock 策略。

核心思路：

- 建立事件日历：token unlock、vesting cliff、large emission、exchange listing/delisting、airdrop claim、staking unlock、governance vote execution。
- 对每类事件建立事件窗：T-30/T-14/T-7/T-1/T+1/T+14。
- 结合流动性、FDV/float、unlock size/free float、unlock recipient、OI/funding、CEX/DEX 深度决定交易方向。

可交易模板：

- 大额 team/investor cliff unlock 前，若流动性弱、OI/funding 已偏空，谨慎做空或禁止做多。
- 事件后波动释放、供给压力被吸收且 funding reset 后，寻找反转。
- Listing 只在公告解析、交易开放、充值开放、合约上线之间做精细事件状态机，避免公告后追高。

主要风险：

- 很多 unlock 已被提前定价。
- 小币流动性和借币能力差，回测结果容易失真。
- 事件数据源必须去重、时间戳必须严格按首次可见时间。

### P1 - Inventory-Aware Market Making Strategy

现状：有 orderbook collector 和 trade intensity，但没有真正的双边挂单、库存控制、撤单、队列位置、adverse selection 模型。

核心思路：

- 基于 Avellaneda-Stoikov/online learning 类型框架，围绕 mid price 双边报价。
- quote width 由 volatility、spread、order arrival、inventory、funding、toxicity 决定。
- 当库存偏多时下调 bid 或提高 ask，反之亦然。

落地建议：

- 先做 paper maker，验证 fill model 和 queue model。
- 只选择 BTC/ETH/SOL 等深度好、maker fee/rebate 明确、API 稳定的市场。
- 以“减少交易成本和提高执行质量”为第一目标，不要一开始追求纯做市盈利。

主要风险：

- 聚合盘口没有真实队列位置，fill 假设极易乐观。
- 低延迟、撤单速率、交易所限频、maker/taker fee 决定策略生死。
- 容易在趋势行情中积累逆势库存。

### P1 - On-Chain Exchange Flow Strategy 2.0

现状：WhaleActivityStrategy 已经有鲸鱼概念，但偏事件/阈值；`coinglass_onchain.py` 已经能拿 exchange netflow、balance、stablecoin balance 等更结构化的数据。

升级方向：

- 从“某笔大额转账”升级为“交易所库存与稳定币购买力”的慢变量策略。
- 指标：exchange netflow、exchange balance change、stablecoin exchange balance change、whale outflow、realized profit/loss、active addresses、fee dominance。
- 用 PiT 数据做回测，严格禁止用供应商后修正历史污染训练。

策略模板：

- BTC/ETH 交易所余额下降、稳定币余额上升、价格横盘时，偏多 regime。
- 大额 inflow 到交易所、realized profit 激增、funding 过热时，降低多头风险。
- 与 trend/funding/liquidation 策略做 meta gate，不一定直接单独开仓。

主要风险：

- 地址标签会后修正。
- 交易所内部钱包整理会造成假信号。
- 链上数据慢，不适合短线单独触发。

### P2 - Stablecoin Peg / Depeg Relative Value Strategy

现状：当前库没有稳定币 peg arbitrage 或 depeg 风险策略。

核心思路：

- 监控 USDT/USDC/DAI/USDe/FDUSD 等在 CEX、DEX、Curve/Uniswap 池、跨链桥上的折溢价和池子失衡。
- 小偏离时做 cross-venue mean reversion。
- 大偏离时切换为风险模式，重点判断可赎回性、issuer 风险、链/桥风险、交易所充值提现状态。

落地难度：

- 中到高。需要稳定币交易对、链上池状态、提现/充值状态和发行方/储备事件数据。
- 可先作为风险守门器：当稳定币基准资产异常脱锚时，降低所有策略杠杆或切换计价币。

### P2 - Hedged Uniswap v3 / CLMM LP Strategy

现状：DEXArbitrageStrategy 和 FlashLoanArbitrageStrategy 是“交易套利”，不是“主动 LP 做市”。

核心思路：

- 在 Uniswap v3/v4 或其他 CLMM 上选择价格区间提供流动性，赚取手续费。
- 用 perps/spot 对 LP 的 delta 和 impermanent loss 做部分 hedge。
- 根据 realized vol、volume/TVL、fee tier、range utilization、gas cost 动态调整区间。

主要风险：

- LP 本质上像卖波动率，强趋势中容易跑输持币。
- gas、MEV、重平衡成本和 oracle 延迟必须进入回测。

### P2 - CEX-DEX / MEV-Lite Arbitrage and Liquidation Bot

现状：已有 DEX/FlashLoan 套利类，但缺少现代 MEV 环境下的 bundle/private routing、simulation、gas auction、mempool/intent flow 模块。

核心思路：

- CEX-DEX price gap、DEX-DEX route gap、lending liquidation、oracle update 后 backrun。
- 在主网成熟市场，纯公开 mempool 套利很拥挤；更现实的切入点是长尾池、小链/L2、新池早期、或只做 MEV 防护与执行优化。

落地建议：

- 不建议作为第一批实盘 alpha。
- 先把它作为“链上交易前模拟 + MEV 风险过滤器”，避免自己的 DEX 交易被 sandwich。

## 4. 不建议优先投入的方向

- 裸 grid bot：除非配套 regime filter、库存控制和止损，否则本质上是无保护均值回归，趋势行情容易积累亏损库存。
- 裸高杠杆趋势策略：你的库里已有大量趋势/动量类策略，边际新增价值低，真正缺的是杠杆拥挤和执行风控。
- 只看单一 liquidation heatmap 的策略：估算误差大，应和 OI、funding、basis、orderbook depth 联合使用。
- 主网公开 mempool 三角套利：竞争高度专业化，没有 bundle/private orderflow/仿真环境时胜率很低。
- 盲目卖期权波动率：历史收益可能好看，但尾部风险和保证金跳变会毁掉组合。

## 5. 建议落地路线

第一阶段，优先补 carry 和拥挤度：

1. 新增 `FundingRateCarryStrategy`，支持 spot/perp funding cashflow、basis filter、funding forecast、手续费和保证金模拟。
2. 新增 `CrossExchangeFundingSpreadStrategy`，复用 `FundingRateCollector.fetch_all()`，先 paper 模式。
3. 新增 `LiquidationCrowdingStrategy` 或 `LeverageCrowdingGate`，作为独立策略和全局风控 gate 双用。
4. 为 backtest engine 补充 funding cashflow、双腿组合、保证金占用、借币/融资成本和强平价计算。

第二阶段，补期权和事件：

1. 升级 Deribit options collector 到完整 option chain。
2. 新增 `OptionsVolatilityStrategy`，只先做 BTC/ETH paper，不做裸卖波动率实盘。
3. 新增 `SupplyEventStrategy`，把 unlock/listing/delisting/airdrop 统一成事件 schema。

第三阶段，补链上相对价值和执行：

1. 新增 `OnChainFlowRegimeStrategy`，先作为策略组合 gate。
2. 新增 `StablecoinPegRiskGate`，先做风控，不急着做主动套利。
3. 市场做市和 CLMM LP 只在有真实 fill、gas、fee、queue 回测后再进入小仓实盘。

## 6. 资料来源

- Binance USD-M Futures Funding Rate History: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History
- Binance USD-M Futures Funding Info: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-Info
- Binance USD-M Futures Basis: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Basis
- Binance Open Interest Statistics: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics
- Binance Taker Buy/Sell Volume: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Taker-BuySell-Volume
- Binance Top Trader Long/Short Position Ratio: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Top-Trader-Long-Short-Ratio
- Deribit `public/get_book_summary_by_currency`: https://docs.deribit.com/api-reference/market-data/public-get_book_summary_by_currency
- Deribit `public/get_order_book`: https://docs.deribit.com/api-reference/market-data/public-get_order_book
- Glassnode Point-in-Time exchange/on-chain metrics: https://docs.glassnode.com/basic-api/endpoints/pit
- Glassnode exchange/on-chain metric caveats: https://insights.glassnode.com/exchange-metrics/
- Exploring Risk and Return Profiles of Funding Rate Arbitrage on CEX and DEX, Blockchain: Research and Applications, 2025: https://www.sciencedirect.com/science/article/pii/S2096720925000818
- Funding Rate Mechanism in Perpetual Futures, SSRN, 2026: https://papers.ssrn.com/sol3/Delivery.cfm/6185958.pdf?abstractid=6185958
- Coinbase Institutional, A Primer on Perpetual Futures: https://www.coinbase.com/en-gb/institutional/research-insights/research/market-intelligence/a-primer-on-perpetual-futures
- Price dynamics and volatility jumps in bitcoin options, Financial Innovation, 2024: https://link.springer.com/article/10.1186/s40854-024-00653-z
- Cryptos have rough volatility and correlated jumps, Digital Finance, 2025: https://link.springer.com/article/10.1007/s42521-025-00125-8
- Adaptive Optimal Market Making Strategies with Inventory Liquidation Costs, arXiv, 2024: https://arxiv.org/abs/2405.11444
- Adaptive Market Making with Inventory Constraints via Online Learning, SSRN/AAAI, 2025: https://papers.ssrn.com/sol3/Delivery.cfm/5110826.pdf?abstractid=5110826
- Linking MEV attacks to further maximise attackers' gains, Blockchain: Research and Applications, 2025: https://www.sciencedirect.com/science/article/pii/S2096720925000673
- Optimistic MEV in Ethereum Layer 2s, 2025: https://arxiv.org/abs/2506.14768
- Stablecoin Runs and the Centralization of Arbitrage, SSRN, 2025: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5285654
- Uniswap concentrated liquidity developer docs: https://developers.uniswap.org/concepts/protocol/concentrated-liquidity
- Unified Approach for Hedging Impermanent Loss of Liquidity Provision, arXiv, 2024: https://arxiv.org/abs/2407.05146
- Liquidity provision with tau-reset strategies, arXiv, 2025: https://arxiv.org/abs/2505.15338
- Token unlock market impact reporting based on Keyrock study, 2024: https://crypto.news/token-unlocks-almost-always-negative-for-price-keyrocks-study-reveals/
