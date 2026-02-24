# 因子计算公式说明（当前仓库）

本文说明当前仓库中的两类因子：

1. **横截面因子库（CS）**：`core/data/factor_library.py`
2. **时间序列因子库（TS）**：`core/factors_ts/*`

说明：
- 记号以 bar 为单位，具体 bar 时长由 `timeframe` 决定（如 `1m/5m/1h`）。
- 所有因子实现均应满足“**不使用未来信息**”。

---

## 一、基础记号

对某资产 `i` 在时间 `t`：

- `C_t`: close
- `O_t`: open
- `H_t`: high
- `L_t`: low
- `V_t`: volume
- `r_t = C_t / C_{t-1} - 1`（简单收益）
- `lr_t = ln(C_t / C_{t-1})`（对数收益）
- `DV_t = C_t * V_t`（美元成交额代理）

滚动窗口：
- `mean_n(x)`: 最近 `n` 个 bar 均值
- `std_n(x)`: 最近 `n` 个 bar 标准差

---

## 二、时间序列因子（TS，`core/factors_ts`）

主要用于 5m 高频策略（如 `MultiFactorHFStrategy`）。

## 2.1 `ret_log(n)`

公式：

```text
ret_log(n)_t = ln(C_t / C_{t-n})
```

含义：
- `n` bar 对数收益，常用于短中期动量/反转信号。

## 2.2 `ema_slope(fast, slow)`

先计算 EMA：

```text
EMA_fast,t = EMA(C, fast)
EMA_slow,t = EMA(C, slow)
```

因子：

```text
ema_slope_t = (EMA_fast,t - EMA_slow,t) / C_t
```

含义：
- 价格趋势强度的归一化表达。

## 2.3 `zscore_price(lookback)`

```text
mu_t = mean_lookback(C)
sigma_t = std_lookback(C)
zscore_price_t = (C_t - mu_t) / sigma_t
```

含义：
- 均值回归偏离程度。

## 2.4 `realized_vol(lookback)`

使用对数收益：

```text
realized_vol_t = std_lookback( ln(C_t / C_{t-1}) )
```

含义：
- 最近窗口实现波动率（bar 级）。

## 2.5 `atr_pct(lookback)`

真实波动范围：

```text
TR_t = max(
  H_t - L_t,
  |H_t - C_{t-1}|,
  |L_t - C_{t-1}|
)
ATR_t = mean_lookback(TR)
atr_pct_t = ATR_t / C_t
```

含义：
- ATR 相对价格比例，用于波动 gate / 动态滑点。

## 2.6 `spread_proxy()`

```text
spread_proxy_t = (H_t - L_t) / C_t
```

含义：
- 在无盘口时，用当根振幅近似点差/冲击风险（粗代理）。

## 2.7 `volume_z(lookback)`

```text
mu_t = mean_lookback(V)
sigma_t = std_lookback(V)
volume_z_t = (V_t - mu_t) / sigma_t
```

含义：
- 成交量相对活跃度（用于 gate 过滤）。

---

## 三、横截面因子（CS，`core/data/factor_library.py`）

核心思想：
- 对每个时点 `t`，计算所有资产的某个横截面特征 `metric_{i,t}`
- 将资产按特征排序，分成高分组与低分组
- 因子收益 = `高组平均收益 - 低组平均收益`（或方向相反）

通用形式（`_long_short_factor`）：

```text
Factor_t = mean( r_{i,t} | i in top quantile(metric_t) )
         - mean( r_{i,t} | i in bottom quantile(metric_t) )
```

其中 `quantile` 默认约 0.3（可配置）。

---

## 四、CS 因子对应特征定义（按代码实现）

注：下面是“横截面排序用 metric”的定义；最终因子收益由上面的 long-short 过程得到。

## 4.1 核心风格因子

### `MKT`（市场因子）

```text
MKT_t = mean_i(r_{i,t})
```

即全市场等权收益（不是 long-short）。

### `SMB`（规模）

代码使用 `metric_size = -log( rolling_mean(DV) )`

```text
metric_size_{i,t} = - ln( mean_n(DV_{i}) )
```

含义：
- 成交额越小，metric 越大，更偏“小规模”。

### `HML` / `VAL`（价值）

```text
metric_val_{i,t} = - ( C_{i,t} / mean_n(C_i) - 1 )
```

含义：
- 价格低于长期均线越多，metric 越大（更“便宜”）。

### `MOM`（中期动量）

```text
metric_mom_{i,t} = C_{i,t} / C_{i,t-n_mid} - 1
```

### `REV`（反转）

```text
metric_rev_{i,t} = - r_{i,t-1}
```

含义：
- 前一 bar 越差，当前反转因子 metric 越高。

### `VOL`（低波）

```text
metric_vol_{i,t} = - std_n(r_i)
```

含义：
- 波动越低，metric 越高。

### `LIQ`（流动性）

先用 Amihud 类指标：

```text
amihud_{i,t} = mean_n( |r_{i}| / DV_i )
metric_liq_{i,t} = - amihud_{i,t}
```

### `RMW`（盈利质量代理）

```text
metric_rmw_{i,t} = mean_n(r_i)
```

说明：
- 这里是 crypto 场景下的“收益质量代理”，不是财报口径 RMW。

### `CMA`（投资强度代理）

```text
metric_cma_{i,t} = - pct_change_n( mean_n(DV_i) )
```

说明：
- 用成交额扩张/收缩代理“投资强度”。

### `QMJ`（质量）

```text
metric_qmj_{i,t} = mean_n(r_i) / std_n(r_i)
```

即滚动 Sharpe 代理。

### `BAB`（低贝塔）

先滚动贝塔：

```text
beta_{i,t} = Cov_n(r_i, MKT) / Var_n(MKT)
metric_bab_{i,t} = - beta_{i,t}
```

---

## 4.2 扩展高频/行为风格因子（CS）

### `MOMF`（快动量）

```text
metric_mom_fast_{i,t} = C_{i,t} / C_{i,t-n_fast} - 1
```

### `MOMS`（慢动量）

```text
metric_mom_slow_{i,t} = C_{i,t} / C_{i,t-n_slow} - 1
```

### `REVF`（快反转）

```text
metric_rev_fast_{i,t} = - ( C_{i,t} / C_{i,t-n_rev_fast} - 1 )
```

### `TRND`（趋势强度）

```text
EMA_fast = EMA(C, n_fast)
EMA_slow = EMA(C, n_slow)
metric_trnd_{i,t} = EMA_fast / EMA_slow - 1
```

### `ACC`（动量加速度）

```text
metric_acc_{i,t} = metric_mom_fast_{i,t} - metric_mom_{i,t}
```

### `PERS`（收益持续性）

```text
metric_pers_{i,t} = mean_n( 1[r_i > 0] ) - 0.5
```

### `SKEW`（偏度）

```text
metric_skew_{i,t} = skew_n(r_i)
```

### `KURT`（低峰度 / 低尾部风险）

代码中用负峰度：

```text
metric_kurt_{i,t} = - kurtosis_n(r_i)
```

### `DSV`（下行波动）

令 `down_i = max(-r_i, 0)`：

```text
metric_dsv_{i,t} = - sqrt( mean_n( down_i^2 ) )
```

### `USV`（上行下行比）

```text
up_mean_{i,t} = mean_n( max(r_i, 0) )
down_mean_{i,t} = mean_n( max(-r_i, 0) )
metric_usv_{i,t} = up_mean_{i,t} / down_mean_{i,t}
```

### `IVOL`（特质波动）

先用市场模型残差：

```text
resid_{i,t} = r_{i,t} - beta_{i,t} * MKT_t
metric_ivol_{i,t} = - std_n(resid_i)
```

### `BSTB`（贝塔稳定）

```text
metric_bstb_{i,t} = - std_n(beta_i)
```

### `CORR`（低相关）

```text
corr_{i,t} = Corr_n(r_i, MKT)
metric_corr_{i,t} = - corr_{i,t}
```

### `VOV`（波动率之波动）

```text
vol_short_{i,t} = std_{n_short}(r_i)
metric_vov_{i,t} = - std_n(vol_short_i)
```

### `RVOL`（相对成交量）

```text
DV_short_{i,t} = mean_{n_short}(DV_i)
DV_long_{i,t} = mean_{n_long}(DV_i)
metric_rvol_{i,t} = DV_short_{i,t} / DV_long_{i,t} - 1
```

### `TURN`（换手趋势代理）

```text
metric_turn_{i,t} = pct_change_k( mean_{n_short}(DV_i) )
```

### `FLOW`（量价流向）

代码中：

```text
signed_flow_{i,t} = sign(r_{i,t}) * log(1 + V_{i,t})
metric_flow_{i,t} = mean_n(signed_flow_i)
```

### `MDD`（回撤韧性 / 低回撤）

```text
rolling_peak_{i,t} = max_n(C_i)
metric_mdd_{i,t} = C_{i,t} / rolling_peak_{i,t} - 1
```

说明：
- 值越高（回撤越小）越好。

### `RPOS`（区间位置）

```text
L_n = min_n(C_i)
H_n = max_n(C_i)
metric_rpos_{i,t} = (C_{i,t} - L_n) / (H_n - L_n) - 0.5
```

---

## 五、关于“最终因子值”和“资产打分”

`build_factor_library(...)` 输出两部分：

1. `factors`（时间序列）
- 每列一个因子收益时间序列（如 `MKT`, `SMB`, `MOM`...）

2. `asset_scores`（最新时点横截面打分）
- 对各风格 metric 做 z-score 后按权重汇总得到资产综合分数

示意（代码中的组合权重）：

```text
score
= 0.20 * z(MOM)
+ 0.15 * z(VAL)
+ 0.05 * z(HML)
+ 0.15 * z(QMJ)
+ 0.15 * z(RMW)
+ 0.10 * z(CMA)
+ 0.08 * z(VOL)
+ 0.07 * z(LIQ)
+ 0.05 * z(BAB)
```

---

## 六、限制与注意事项

1. `CS 因子`是 crypto 场景代理定义
- 名称借用 Fama 风格（如 `RMW/CMA`），但不是传统财务报表因子

2. `spread_proxy` 不是盘口点差
- 仅为无盘口数据时的粗代理

3. `timeframe` 会影响滚动窗口长度
- `core/data/factor_library.py` 内部会按粒度自适应窗口（不是固定小时窗口）

4. 所有因子都应避免未来函数
- 新增 `tests/test_no_lookahead_ts_factors.py` 已验证 TS 因子基本无未来函数

