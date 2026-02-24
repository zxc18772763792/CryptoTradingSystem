# 因子计算公式说明（当前仓库实现）

更新时间：2026-02-24

本文档说明当前仓库中两类因子：

1. 横截面因子（CS）：`core/data/factor_library.py`
2. 时间序列因子（TS）：`core/factors_ts/*`

说明：
- 公式用于解释实现思路，个别边界处理（`NaN`、裁剪、窗口预热、异常值处理）以代码为准
- 所有因子均要求在时点 `t` 只使用 `<= t` 的数据（避免未来函数）

## 1. 基础记号

对某资产在时点 `t`：

- `O_t`: 开盘价
- `H_t`: 最高价
- `L_t`: 最低价
- `C_t`: 收盘价
- `V_t`: 成交量
- `r_t = C_t / C_{t-1} - 1`（简单收益）
- `lr_t = ln(C_t / C_{t-1})`（对数收益）
- `DV_t = C_t * V_t`（成交额代理）

滚动运算：

- `mean_n(x)`: 最近 `n` 个 bar 均值
- `std_n(x)`: 最近 `n` 个 bar 标准差
- `max_n(x)`: 最近 `n` 个 bar 最大值
- `min_n(x)`: 最近 `n` 个 bar 最小值

## 2. 时间序列因子（TS, `core/factors_ts`）

主要用于高频/中频单标的策略（例如 `MultiFactorHFStrategy`）。

## 2.1 `ret_log(n)`

```text
ret_log(n)_t = ln(C_t / C_{t-n})
```

含义：
- `n` bar 的对数收益，常用于短中期动量 / 反转特征

## 2.2 `ema_slope(fast, slow)`

先计算指数移动平均：

```text
EMA_fast,t = EMA(C, fast)
EMA_slow,t = EMA(C, slow)
```

因子值（归一化趋势差）：

```text
ema_slope_t = (EMA_fast,t - EMA_slow,t) / C_t
```

含义：
- 趋势强度代理，数值越大表示偏多趋势越强

## 2.3 `zscore_price(lookback)`

```text
mu_t = mean_lookback(C)
sigma_t = std_lookback(C)
zscore_price_t = (C_t - mu_t) / sigma_t
```

含义：
- 价格相对滚动均值的标准化偏离
- 常用于均值回归：绝对值越大，偏离越明显

## 2.4 `realized_vol(lookback)`

使用对数收益计算滚动波动率：

```text
realized_vol_t = std_lookback( lr_t )
```

含义：
- 最近窗口的实现波动率（bar 级）
- 常用于交易 gate、风险过滤、动态滑点

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
- ATR 相对价格的比例化表示
- 常用于高波动过滤与动态成本估计

## 2.6 `spread_proxy()`

```text
spread_proxy_t = (H_t - L_t) / C_t
```

含义：
- 在无盘口（bid/ask）数据时，用单根振幅粗略近似点差/冲击风险
- 仅作研究代理，不等于真实盘口点差

## 2.7 `volume_z(lookback)`

```text
mu_t = mean_lookback(V)
sigma_t = std_lookback(V)
volume_z_t = (V_t - mu_t) / sigma_t
```

含义：
- 当前成交量相对于历史窗口的活跃度
- 可用于过滤低流动性时段

## 3. 横截面因子（CS, `core/data/factor_library.py`）

核心思路：

- 在每个时点 `t`，对一组资产计算横截面特征 `metric_{i,t}`
- 按特征排序，将资产分成高分组和低分组
- 因子收益通常定义为“高分组收益 - 低分组收益”（方向因子除外）

通用 long-short 形式：

```text
Factor_t = mean(r_{i,t} | i ∈ Top(metric_t))
         - mean(r_{i,t} | i ∈ Bottom(metric_t))
```

分组比例通常由 `quantile`（如 `0.3`）控制。

## 4. 主要 CS 因子与其排序特征（metric）

下面给出“排序特征”的定义；最终因子收益来自 long-short 组合。

## 4.1 `MKT`（市场因子）

```text
MKT_t = mean_i(r_{i,t})
```

说明：
- 市场平均收益，不是 long-short 因子

## 4.2 `SMB`（规模，小盘偏好代理）

代码中用成交额代理规模，取负号表示“小规模更高分”：

```text
metric_size_{i,t} = - ln(mean_n(DV_i))
```

含义：
- 成交额越小，metric 越大（越偏“小规模”）

## 4.3 `HML / VAL`（价值代理）

使用价格相对滚动均值的偏离作为“便宜/昂贵”代理：

```text
metric_val_{i,t} = - (C_{i,t} / mean_n(C_i) - 1)
```

含义：
- 价格低于历史均值越多，metric 越大（更“便宜”）

## 4.4 `MOM`（中期动量）

```text
metric_mom_{i,t} = C_{i,t} / C_{i,t-n_mid} - 1
```

## 4.5 `REV`（短期反转）

```text
metric_rev_{i,t} = - r_{i,t-1}
```

含义：
- 上一根跌得越多，当前反转因子得分越高

## 4.6 `VOL`（低波动）

```text
metric_vol_{i,t} = - std_n(r_i)
```

含义：
- 波动越低，得分越高

## 4.7 `LIQ`（流动性）

使用 Amihud 类指标代理非流动性，再取负号：

```text
amihud_{i,t} = mean_n( |r_i| / DV_i )
metric_liq_{i,t} = - amihud_{i,t}
```

含义：
- 非流动性越低（越容易交易），得分越高

## 4.8 `RMW`（收益质量代理）

```text
metric_rmw_{i,t} = mean_n(r_i)
```

说明：
- 这里是 crypto 场景中的收益质量代理，不是传统财务报表口径 `RMW`

## 4.9 `CMA`（投资强度代理）

用成交额趋势作为投资/扩张强度代理：

```text
metric_cma_{i,t} = - pct_change_k(mean_n(DV_i))
```

## 4.10 `QMJ`（质量）

```text
metric_qmj_{i,t} = mean_n(r_i) / std_n(r_i)
```

说明：
- 可视作滚动 Sharpe 代理

## 4.11 `BAB`（低 Beta）

先计算滚动 beta：

```text
beta_{i,t} = Cov_n(r_i, MKT) / Var_n(MKT)
metric_bab_{i,t} = - beta_{i,t}
```

含义：
- beta 越低，得分越高

## 5. 扩展横截面因子（高频/行为风格）

以下名称与 `core/data/factor_library.py` 中的扩展指标一致（或近似命名）。

## 5.1 动量/趋势类

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
metric_rev_fast_{i,t} = - (C_{i,t} / C_{i,t-n_rev_fast} - 1)
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

## 5.2 分布形态/波动结构类

### `PERS`（收益持续性）
```text
metric_pers_{i,t} = mean_n(1[r_i > 0]) - 0.5
```

### `SKEW`（偏度）
```text
metric_skew_{i,t} = skew_n(r_i)
```

### `KURT`（低峰度 / 低尾部风险代理）
```text
metric_kurt_{i,t} = - kurtosis_n(r_i)
```

### `DSV`（下行波动）
```text
down_i = max(-r_i, 0)
metric_dsv_{i,t} = - sqrt(mean_n(down_i^2))
```

### `USV`（上/下行强度比）
```text
up_mean_{i,t} = mean_n(max(r_i, 0))
down_mean_{i,t} = mean_n(max(-r_i, 0))
metric_usv_{i,t} = up_mean_{i,t} / down_mean_{i,t}
```

### `IVOL`（特质波动）
```text
resid_{i,t} = r_{i,t} - beta_{i,t} * MKT_t
metric_ivol_{i,t} = - std_n(resid_i)
```

### `BSTB`（Beta 稳定性）
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

## 5.3 流动性/量价行为类

### `RVOL`（相对成交量 / 成交额活跃）
```text
DV_short_{i,t} = mean_{n_short}(DV_i)
DV_long_{i,t} = mean_{n_long}(DV_i)
metric_rvol_{i,t} = DV_short_{i,t} / DV_long_{i,t} - 1
```

### `TURN`（换手趋势代理）
```text
metric_turn_{i,t} = pct_change_k(mean_{n_short}(DV_i))
```

### `FLOW`（量价流向）
```text
signed_flow_{i,t} = sign(r_{i,t}) * log(1 + V_{i,t})
metric_flow_{i,t} = mean_n(signed_flow_i)
```

## 5.4 回撤/区间位置类

### `MDD`（低回撤 / 回撤韧性代理）
```text
rolling_peak_{i,t} = max_n(C_i)
metric_mdd_{i,t} = C_{i,t} / rolling_peak_{i,t} - 1
```

说明：
- 值越高（越接近 0），表示当前回撤越小

### `RPOS`（区间位置）
```text
L_n = min_n(C_i)
H_n = max_n(C_i)
metric_rpos_{i,t} = (C_{i,t} - L_n) / (H_n - L_n) - 0.5
```

## 6. 因子库输出与资产打分

`build_factor_library(...)` 通常会输出两类结果：

1. `factors`
- 因子收益时间序列（如 `MKT/SMB/HML/MOM/...`）

2. `asset_scores`
- 最新时点的资产横截面综合得分
- 用于高级研究页中的“偏多/偏空”与排序展示

综合打分通常做法：

1. 对多个风格 metric 做标准化（如 z-score）
2. 按固定权重线性组合
3. 输出最终资产分数

示意（非逐字代码）：

```text
score
= w1 * z(MOM)
+ w2 * z(VAL)
+ w3 * z(QMJ)
+ w4 * z(VOL)
+ ...
```

## 7. 与高频策略的关系（当前实现）

`MultiFactorHFStrategy` 使用的是 TS 因子库（`core/factors_ts`），并通过配置指定：

- 因子列表（name + params）
- 权重（weight）
- 变换（transform，如 `zscore/rank/none`）
- gates（`max_rv/max_atr_pct/max_spread_proxy/min_volume_z`）

因此：
- `TS 因子` 主要用于单策略时序决策
- `CS 因子` 主要用于研究页与横截面分析（多币种排序、风格因子）

## 8. 限制与注意事项

1. `CS 因子`是 crypto 代理定义
- 名称借用了传统风格因子（如 `RMW/CMA`），但并非财务报表版 Fama 因子

2. `spread_proxy` 不是真实盘口点差
- 只是无盘口场景下的粗代理

3. `timeframe` 会影响窗口长度解释
- 同样的 `lookback=60` 在 `1m` 和 `1h` 上含义不同

4. 需警惕未来函数
- TS 因子已有测试：`tests/test_no_lookahead_ts_factors.py`
- 新增因子时应保持“只读到当前时点”

