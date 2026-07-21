# 链上数据源调研：持仓集中度 + 解锁日历

日期：2026-07-19
动机：三轮策略研究的终局结论——继续提高周度横截面模型上限的方向是
"操盘方选标的时的输入变量"，首选持仓集中度与解锁日历两个特征
（见 `docs/STRATEGY_EXPLORATION_2026-07-19.md` 终局第 3 条）。
方法：不看宣传页，全部用我们宇宙的 110 个币实测 API。

---

## 一、现状盘点

系统内**没有**任何第三方链上 key（.env 仅 Coinglass/交易所/LLM/飞书）。
`core/data/coinglass_onchain.py` 只覆盖交易所余额/净流，两特征均需新接。

## 二、解锁日历：实测结论

### 数据源对比

| 源 | 成本 | 实测结果 | 历史回填 |
|---|---|---|---|
| **DefiLlama datasets 桶**（`defillama-datasets.llama.fi/emissions/{slug}`） | **免费无 key** | ✅ HTTP 200，返回完整 vesting 序列（分类标签 + 过去/未来时间戳 + 数量） | ✅ 日程表天然含全历史 |
| DefiLlama 正式 API（api.llama.fi/emissions） | 付费（Pro ~$300/mo） | 402（2026 年已入付费墙） | ✅ |
| Tokenomist（原 TokenUnlocks） | 企业询价 | 未测（无 key） | ✅ 行业金标准 |
| CryptoRank v2 unlock 端点 | 免费层需注册 / 付费层 | 未测（需用户注册） | 部分 |

### 宇宙覆盖率（110 币实测）

- **37/110 有完整解锁表**（含 slug 映射修正：清单用 DefiLlama 协议 slug
  而非 CoinGecko id——ARB=`arbitrum`、CRV=`curve-finance`、
  MANTRA=`mantra-dao` 等 6 个靠文件探测找回；别名表已存
  `data/research/onchain/unlock_slugs.json`）
- **34/110 流通率 ≥95%**（meme/老币）→ 解锁特征**合法为 0**，非缺失
- **39/110 真缺口**——几乎全是 2026 fresh 妖币（AKE/RAVE/GENIUS/COAI/
  MYX/UB/US...）。它们没有公开 vesting 表，"解锁"=内部人任意抛售，
  本就该由持仓集中度刻画（两特征互补的天然分工）
- 有效赋值率 **71/110 = 65%**，且覆盖的正是解锁风险真实存在的中盘币

### 可派生特征

`unlock_next_7d/30d_pct_mcap`（未来解锁量×价/市值）、`days_to_next_cliff`、
`past_30d_unlocked_pct`。**可回测**：日程是前视已知的确定性日历。

### 风险

datasets 桶是**未文档化的前端数据源**：正式 API 已于 2026 入付费墙，
桶随时可能跟进。缓解：周度缓存全量落盘（一次 ~40 文件）；若失效，
备选 = CryptoRank 免费层（用户注册）或 DefiLlama Pro（$300/mo）。

## 三、持仓集中度：实测结论

### 宇宙链分布（CoinGecko platforms，1 次调用）

**BSC 36 + Solana 29 = 59%（妖币腹地）**，ETH 17，原生链/无合约 14，
Base 3，其它 8。任何单链方案都不及格，必须多链。

### 数据源对比（均以宇宙币实测）

| 源 | 成本 | 实测结果 | 多链 | 历史 |
|---|---|---|---|---|
| **GeckoTerminal `/networks/{net}/tokens/{addr}/info`** | **免费无 key**（~30 req/min） | ✅ 返回**预聚合分布**：`top_10`/`11_20`/`21_40` 占比 + 持有人数 + `developer_holding_percentage` + `mint_authority`/`freeze_authority`/`is_honeypot`（ACT 实测 top10=84.6%） | ✅ SOL/BSC/ETH/Base 全覆盖 | ❌ 仅当前快照 |
| Blockscout 公共实例 `/api/v2/tokens/{addr}/holders` | 免费无 key | ✅ ETH 实测 200（50 holder/页，含数量可自算 top-N） | ❌ ETH/Base 有，**BSC 无实例（404）** | ❌ |
| Solana 公共 RPC `getTokenLargestAccounts` | 免费无 key | ⚠️ 可用但不稳（实测一次空返回），且是 top-20 **账户**（含池子），需去重标注 | 仅 SOL | ❌ |
| Moralis Token Holder Stats | 免费层需注册（40k CU/天） | 未测（无 key）。**唯一提供 EVM 持仓历史时间序列的平价源** | EVM+SOL | ✅ |
| Etherscan V2 `tokenholderlist` | Pro $199/mo | 未测 | EVM | ❌ |
| BscScan Pro / OKLink / Birdeye / Solscan Pro / Holderscan | 付费或需注册 | 未测 | 各有侧重 | 部分 |

### 选型：GeckoTerminal 为主源

理由：唯一同时满足「零成本、零 key、多链全覆盖（含 BSC 缺口）、
预聚合到我们要的粒度（top10 占比）、附赠安全字段（dev 持仓/铸币权/
蜜罐）」。全宇宙覆盖率实测见附录 A（脚本 `snapshot_onchain_features.py`
每周产出）。

已知局限（报告必须诚实）：
1. **无历史** → 特征只能前向积累。对策：**周度快照从今天开始落盘**
   （`data/research/onchain/holder_snapshots/`），8-12 周后即有第一份
   可评估横截面；这也是纸面跟踪期的天然并行任务。
2. top_10 含交易所热钱包与 DEX 池子 → 绝对值偏高，但横截面**排名**
   仍可比（模型用的正是横截面排名）。后续可用 GT 的池子地址接口
   做减法精化。
3. 非官方稳定性：字段/限速可能变动；30 req/min 下全宇宙 4-5 分钟/周，
   压力极小。
4. 纯 CEX 上市、无 DEX 交易的币会缺失（实测缺口见附录 A）。

## 四、接入方案

```
scripts/snapshot_onchain_features.py   （新，周度，与 pump watchlist 同日跑）
  ├─ CG /coins/list?include_platform=true（1 调用，缓存 7 天）→ base→链→合约
  ├─ GeckoTerminal token info × 全宇宙（~110 调用 @2.2s ≈ 4 分钟）
  │    → top10_pct / holders_count / dev_pct / mint_auth / honeypot
  ├─ DefiLlama emissions × 已映射 slug（~40 文件，缓存 7 天）
  │    → unlock_next_7d/30d_pct_mcap / days_to_next_cliff
  └─ 落盘 data/research/onchain/{holder_snapshots,unlocks}/<date>.json
       + latest.json（供雷达/名单读取）
```

- **不立刻进模型**：先作为周度名单的**展示列**（top10%、解锁临近标记），
  积累 8-12 周快照后做横截面提升评估（复用 pump_precursor 面板方法），
  显著才进权重重训。解锁特征因可回溯，可先行单独回测。
- Coinglass 预算零占用；全链路零新增成本、零新增 key。
- 升级路径：若 EVM 持仓**历史**成为刚需（回测验证），用户注册
  Moralis 免费层（唯一平价历史源）；解锁侧若桶失效再评估 CryptoRank。

## 附录 A：GeckoTerminal 全宇宙覆盖实测

全宇宙实测（108 个有 CG 映射的币，逐一调用）：

- **覆盖 85/108（79%）**。缺失 23 个中 **18 个是原生 L1 币**
  （APT/AVAX/BCH/DASH/DOT/ETC/FIL/HBAR/HYPE/LTC/NEAR/SUI/TAO/TIA/
  XLM/XMR/ZEC/1000XEC）——无代币合约，持仓集中度概念本就不适用；
  真缺口仅 AIA/MAGMA/US/XPL/MANTRA 5 个。
  **在特征有意义的币里覆盖率 ≈ 94%。**
- **区分度实测（face validity 极强）**：top10 持仓占比最高 =
  GENIUS 104.7%*、STAR 99.8%、SIREN 99.1%、NEIRO 99.1%、BILL 98.7%、
  RE 97.6%、HOME 97.2%——**2026 妖币全数 95%+**；
  最低 = TURBO 18.1%、1000PEPE 22.0%、GRAM 20.1%、LINK 29.9%、
  FARTCOIN 32.2%——成熟/公平launch币显著分散。
  这正是横截面模型缺失的「操盘方选标的」维度。
  （*>100% 由池子/双计导致——绝对值含交易所与 DEX 池，横截面排名可用，
  精化可后续用 GT 池子地址做减法。）

首次正式快照已于 2026-07-19 落盘 `data/research/onchain/holder_snapshots/`
（此后每周与伏击名单同日采集，历史档案从今天开始累积）。

## 附录 B：解锁特征回测（对抗验证，2026-07-21）

解锁日程前视已知，可**立即回测**（不必等快照积累）。用
`build_unlock_feature_panel.py` 把历史解锁 overhang 重建到既有周度面板
（2213 可分析行 = 910 有真实日程 + 1303 全流通零），
`unlock_candidate_findings.py` 算出候选统计，再用一个 10 agent 的对抗验证
workflow（3 个候选发现 × 3 个独立证伪视角：样本外稳定性 / 伪重复 /
置换显著性 + 综合）逐一攻击。

**结论：三个候选发现全部被三个视角一致证伪——解锁 overhang 既不是特征，
也不是 veto gate，只能做展示列。**

| 发现 | 样本内 | 样本外 | 一票否决的证据 |
|---|---|---|---|
| F1 大解锁（≥5% 市值/30d）抑制拉盘 | 0/99（诱人） | **反转**：大解锁组 5.08% > 小组 4.13% | 全部 3 个大解锁"拉盘"来自**同一个币 WLD**，3 个重叠周窗=1 个真实事件；Fisher p=0.153；按 base 聚类置换 p=0.91 |
| F2 刚解锁的币前向拉盘更少 | 0/94 | **反转** 5.36% > 4.11% | 去掉 WLD → 前向拉盘率归零（0/124） |
| F4 用解锁做 veto gate 提升残余池 | +0.30pp | **−0.08pp（反转）** | gate 删掉的正是未来赢家（样本外大解锁组 5.08% 高于残余池 4.13%） |

根因（三个发现共享）：**伪重复**——30 天前向窗口使同一个拉盘事件被周度
重复计数约 3 次，大解锁 cell 里只有 ~12 个独立币，WLD 一个币贡献了全部
"反例"拉盘。样本外切分 / 去重到独立事件 / 尊重聚类的置换——任何一种做法
下信号都消失或反转。教科书式假阳性，被对抗验证当场击杀。

**落地**：解锁 overhang 仅作为币卡的**上下文标签**（"未来 30 天解锁 X%
市值"），零权重、不做过滤。若将来重访，须在**事件/币粒度**（去重重叠周窗）
且至少 30-40 个独立大解锁事件后才可谈任何结论。持仓集中度特征仍按原计划
积累周度快照，8-12 周后独立评估。

产物：`reports/ambush_modes_2026-07-18/unlock_{feature_panel.parquet,
candidate_findings.json}`；验证 workflow run `wf_16712e56-44a`。

*研究性质，不构成投资建议。*
