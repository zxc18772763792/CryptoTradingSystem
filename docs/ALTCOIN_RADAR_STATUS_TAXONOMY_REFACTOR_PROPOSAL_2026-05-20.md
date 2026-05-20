# 山寨雷达状态语义整理与展示层归一化建议

日期：2026-05-20

## 1. 背景

当前山寨雷达已经有比较完整的底层评分体系，但 UI 上把三类不同语义混在一起展示：

1. 旧版结构状态：`异动启动`、`布局吸筹`、`高控盘跟踪`、`高控盘警戒`、`派发风险`
2. 新版信号来源：`点火`、`延续`、`末端`、`叙事`、`叙事确认`
3. 事件/预警：`点火穿越`、`排名跃升`、`拥挤风险`、`叙事热度`

这些概念各自合理，但用户在页面上会把它们理解成同一层级的交易信号，导致典型困惑：

- `点火` 和 `异动启动` 有什么区别？
- `末端` 和 `拥挤` 哪个更严重？
- `跃升` 是不是买点？
- `叙事` 是不是等于可以追？
- `末端` 是不是可以开空？

结论：底层算法不建议粗暴合并；真正需要整理的是展示层语义、预警命名和动作建议。

## 2. 当前实现梳理

### 2.1 旧版结构状态：signal_state

位置：`core/research/altcoin_radar.py`

当前 `signal_state` 由 `_signal_state_for_row()` 输出：

| 状态 | 主要条件 | 语义 |
| --- | --- | --- |
| `布局吸筹` | `accumulation >= 0.68`、`control >= 0.55`、`risk_penalty < 0.20` | 偏结构蓄势和吸筹 |
| `异动启动` | `anomaly >= 0.72` 且 `accumulation/control` 有跟随 | 价格、量、波动异常，并且不是孤立跳动 |
| `高控盘跟踪` | `control >= 0.70`、`risk_penalty < 0.25` | 控盘/流动性结构明显，风险不高 |
| `高控盘警戒` | `control >= 0.70`、`risk_penalty >= 0.25` | 控盘强但风险也高 |
| `派发风险` | `anomaly >= 0.70`、`accumulation < 0.35`、`control >= 0.65` | 更像冲高派发或末端风险 |

问题：这些状态是“结构画像”，不一定是最新事件，也不一定是方向信号。

### 2.2 新版信号来源：signal_source

位置：`core/research/altcoin_radar_perp.py`、`core/research/altcoin_radar_narrative.py`、`core/research/altcoin_radar.py`

当前 `signal_source` 输出：

| 前端显示 | 后端值 | 主要条件 | 语义 |
| --- | --- | --- | --- |
| `点火` | `perp_ignition` | `ignition_score >= 0.60` | 合约侧启动，偏 OI、成交、清算、突破 |
| `延续` | `perp_continuation` | `continuation_score >= 0.55` 且 `ignition_score < 0.50` | 行情已经在走，不是最早点火 |
| `末端` | `crowded_late_stage` | `crowding_late_score >= 0.65` | 过度拥挤/后期风险，优先别追 |
| `叙事` | `narrative_ignition` | `narrative_heat_score >= 0.55` | 板块、社区、公告、活跃度升温 |
| `叙事确认` | `narrative_confirmation` | `narrative_heat >= 0.40` 且 `meme_rotation >= 0.35` | 有题材热度和轮动支持 |

当前优先级是：

`末端` > `点火` > `延续` > `叙事`

这个优先级方向是对的，因为当拥挤风险足够高时，风险标签应该压过启动标签。

问题：前端把 `末端` 和 `点火/叙事` 放在同一视觉位置，用户容易把 `末端` 误读为“可以做空”的方向信号，而不是“别追/等回落”的风险信号。

### 2.3 事件与预警

位置：`core/research/altcoin_radar_events.py`、`web/api/altcoin.py`、`core/notifications/notification_manager.py`

当前事件：

| 事件 | 后端值 | 触发条件 | 语义 |
| --- | --- | --- | --- |
| `点火` | `altcoin_ignition_cross_up` | `ignition_score` 从低于 `0.60` 穿越到高于等于 `0.60` | 新近出现合约点火 |
| `跃升` | `altcoin_rank_jump_top_n` | 当前进前 `15`，且排名至少跳升 `3` 名 | 雷达排名突变 |
| `拥挤` | `altcoin_crowding_risk_spike` | `crowding_late_score >= 0.65` | 拥挤风险事件 |
| `叙事热度` | `altcoin_narrative_heat_spike` | `narrative_heat_score >= 0.55` | 题材热度事件 |

问题：`跃升` 是排名事件，不是交易方向；`拥挤` 是风险事件，不是开空信号；`点火` 是穿越事件，不等于持续有效状态。

## 3. 建议目标

目标不是删除分数，而是建立一个更清晰的展示层：

1. 页面上只给用户一个“主判断”
2. 风险与机会分开展示
3. 事件只作为“最近发生了什么”，不混入主状态
4. 预警按钮按用途分组，避免旧版预警和新版事件预警并列造成误读
5. 保留原始分数给高级用户和调试使用

## 4. 建议的新语义模型

### 4.1 三层展示模型

建议前端和 API 详情页统一展示三层：

| 层级 | 建议字段 | 页面标题 | 内容 |
| --- | --- | --- | --- |
| 主判断 | `primary_state` | 当前判断 | 点火观察、延续跟踪、叙事观察、布局吸筹、异动启动、末端风险、派发风险、待观察 |
| 风险标签 | `risk_tags` | 风险提示 | 拥挤、派发、流动性陷阱、数据降级、衍生品过热、清算后脆弱 |
| 最近事件 | `recent_events` | 最近事件 | 点火穿越、排名跃升、叙事升温、拥挤升温 |

### 4.2 主判断枚举

建议新增展示层枚举，不替换底层分数：

| `primary_state` | 中文显示 | 触发优先级 | 建议动作 |
| --- | --- | --- | --- |
| `late_crowded_risk` | 末端风险 | 最高 | 别追，建拥挤预警，等拥挤回落 |
| `distribution_risk` | 派发风险 | 高 | 不追，观察冲高回落和承接失败 |
| `perp_ignition_watch` | 点火观察 | 中高 | 建点火预警，等回踩/延续确认 |
| `perp_continuation_watch` | 延续跟踪 | 中 | 跟踪回踩承接，不当首爆 |
| `narrative_watch` | 叙事观察 | 中 | 加入 Watchlist，等合约侧跟随 |
| `accumulation_setup` | 布局吸筹 | 中 | 观察结构是否继续压缩/抬升 |
| `anomaly_watch` | 异动观察 | 中低 | 查原因，看是否有合约/链上确认 |
| `control_watch` | 控盘跟踪 | 中低 | 跟踪但注意流动性和滑点 |
| `neutral_watch` | 待观察 | 默认 | 不作为明确交易候选 |

### 4.3 推荐优先级

建议用一个专门函数生成展示层状态，例如：

```python
def derive_altcoin_display_state(row: Mapping[str, Any]) -> Dict[str, Any]:
    ...
```

推荐优先级：

1. 如果 `crowding_late_score >= 0.65` 或 `signal_source == crowded_late_stage`
   - `primary_state = late_crowded_risk`
   - `risk_tags += ["拥挤", "末端"]`
2. 否则如果 `signal_state == 派发风险`
   - `primary_state = distribution_risk`
   - `risk_tags += ["派发"]`
3. 否则如果 `signal_source == perp_ignition`
   - `primary_state = perp_ignition_watch`
4. 否则如果 `signal_source == perp_continuation`
   - `primary_state = perp_continuation_watch`
5. 否则如果 `signal_source` 以 `narrative_` 开头
   - `primary_state = narrative_watch`
6. 否则按旧版 `signal_state` 映射：
   - `布局吸筹` -> `accumulation_setup`
   - `异动启动` -> `anomaly_watch`
   - `高控盘跟踪/高控盘警戒` -> `control_watch`
7. 否则：
   - `primary_state = neutral_watch`

这能保留当前 `末端 > 点火 > 延续 > 叙事` 的核心原则，同时让旧版状态有明确兜底位置。

## 5. 页面改造建议

### 5.1 榜单行

当前榜单行建议从“很多标签并列”改成：

- 第一视觉：`primary_state.label`
- 第二视觉：关键分数，例如 `点火 0.63 / 拥挤 0.72 / 叙事 0.58`
- 第三视觉：风险标签，最多展示 2-3 个
- 最近事件用小图标或短文本，不要和主状态混在一起

示例：

```text
ORDI/USDT   末端风险
点火 0.68  拥挤 0.74  叙事 0.22
风险：拥挤 / 衍生品过热
事件：点火穿越、拥挤升温
```

### 5.2 详情页

建议详情页顶部固定四块：

1. 当前判断
   - 显示 `primary_state.label`
   - 一句话解释：例如“合约侧已点火，但拥挤分过高，当前优先按末端风险处理。”
2. 操作建议
   - 来自现有 `_build_action_plan()`，但文案和 `primary_state` 对齐
3. 风险提示
   - 显示 `risk_tags`
   - 明确写出“风险提示不是开仓方向”
4. 最近事件
   - 时间线只放 `recent_events`

### 5.3 筛选器

当前筛选器里同时有旧版状态、新版信号和事件。建议分组：

```text
主判断：
- 全部
- 点火观察
- 延续跟踪
- 叙事观察
- 布局吸筹
- 异动观察
- 末端风险

风险：
- 拥挤
- 派发风险
- 流动性陷阱
- 数据降级

事件：
- 点火穿越
- 排名跃升
- 叙事升温
- 拥挤升温
```

### 5.4 预警按钮

建议默认只展示新版事件预警：

- `点火预警`
- `跃升预警`
- `拥挤预警`
- `叙事预警`

旧版预警移动到“高级预警”：

- `异动预警`
- `吸筹预警`
- `高控盘预警`

并加一句小字说明：

```text
高级预警基于旧版结构分数，适合研究和筛选，不等同于即时事件。
```

## 6. API 改造建议

### 6.1 新增 display_state 字段

建议在每个 row 中新增字段：

```json
{
  "display_state": {
    "primary_state": "late_crowded_risk",
    "label": "末端风险",
    "tone": "danger",
    "summary": "合约侧已过热，优先防止追在情绪末端。",
    "action_hint": "建拥挤预警，等拥挤回落",
    "risk_tags": ["拥挤", "衍生品过热"],
    "opportunity_tags": ["点火曾出现"],
    "event_tags": ["点火穿越", "拥挤升温"]
  }
}
```

兼容性：

- 不删除 `signal_state`
- 不删除 `signal_source`
- 不删除 `event_flags`
- 前端逐步切到 `display_state`

### 6.2 建议落点

新增函数可放在：

- `core/research/altcoin_radar.py`

或者拆成新文件：

- `core/research/altcoin_radar_display.py`

建议拆新文件，原因：

- 当前 `altcoin_radar.py` 已经偏大
- 展示层状态是 UI 语义，不应继续塞进核心评分函数
- 后续文案和枚举会变化，单独文件更容易维护

推荐文件：

```text
core/research/altcoin_radar_display.py
```

推荐函数：

```python
def derive_display_state(row: Mapping[str, Any]) -> Dict[str, Any]:
    ...
```

在 `build_altcoin_rows()` 末尾或者 `sort_rows()` 阶段补充：

```python
row["display_state"] = derive_display_state(row)
```

## 7. 文案建议

### 7.1 统一术语

建议把用户可见术语改成更少、更明确的词：

| 当前词 | 建议词 | 原因 |
| --- | --- | --- |
| `末端` | `末端风险` | 避免被理解为做空按钮 |
| `拥挤` | `拥挤风险` | 明确是风险，不是方向 |
| `点火` | `点火观察` | 避免被理解为立即追 |
| `延续` | `延续跟踪` | 明确不是首爆 |
| `叙事` | `叙事观察` | 明确需要合约/价格确认 |
| `跃升` | `排名跃升` | 避免理解为价格跃升 |
| `异动启动` | `异动观察` | 避免自动等同买点 |

### 7.2 详情页动作文案

建议保留现有 `_build_action_plan()`，但把文案按新主状态改写：

`late_crowded_risk`：

```text
当前按末端风险处理。拥挤分已经越过阈值，榜单靠前不代表适合追。
优先建拥挤预警，等待 funding / 多空比 / 拥挤分回落后再重新评估。
```

`perp_ignition_watch`：

```text
当前是合约点火观察。先看 15m / 1h 是否继续放量、OI 是否持续抬升。
更稳的处理是等第一轮回踩不破，再观察是否二次发力。
```

`rank_jump` 事件：

```text
排名跃升说明雷达因子变化很快，不代表价格已经给出低风险入场点。
先检查跃升来自点火、叙事还是风险项。
```

## 8. 分阶段实施计划

### Phase 1：只加展示层，不改算法

范围：

- 新增 `core/research/altcoin_radar_display.py`
- 给 row 新增 `display_state`
- 前端详情页优先读取 `display_state`
- 保留所有旧字段

验收：

- 旧测试不应大规模改动
- API rows 中新增 `display_state`
- 末端样本显示为 `末端风险`
- 拥挤事件显示在“最近事件”，不覆盖风险标签

### Phase 2：前端重排

范围：

- 榜单行展示主判断、风险标签、最近事件
- 详情页顶部改成“当前判断 / 操作建议 / 风险提示 / 最近事件”
- 筛选器按“主判断 / 风险 / 事件”分组

验收：

- 用户不用知道 `signal_state` 和 `signal_source` 也能读懂页面
- `末端风险` 不再和 `点火观察` 同色同级展示
- `排名跃升` 清楚显示为事件

### Phase 3：预警体系分组

范围：

- 默认预警只展示事件预警
- 旧版 `异动/吸筹/高控盘` 放到高级预警
- 预警创建接口保持兼容

验收：

- 旧规则仍能显示和删除
- 新建规则优先走四类事件预警
- UI 不再把 `异动预警` 和 `点火预警` 当成同一层

### Phase 4：可选的评分解释增强

范围：

- 每个主判断给出 top contributors
- 例如点火来自 OI / 成交 / 清算，拥挤来自 funding / 多空比 / 派发

验收：

- 用户能看到“为什么是这个状态”
- 排查误判更容易

## 9. 测试建议

### 9.1 单元测试

新增：

```text
tests/test_altcoin_radar_display_state.py
```

覆盖：

1. `crowding_late_score >= 0.65` 时输出 `late_crowded_risk`
2. `signal_state == 派发风险` 时输出 `distribution_risk`
3. `signal_source == perp_ignition` 且无拥挤时输出 `perp_ignition_watch`
4. `signal_source == narrative_ignition` 时输出 `narrative_watch`
5. 旧版 `布局吸筹` 能映射到 `accumulation_setup`
6. 旧版 `异动启动` 能映射到 `anomaly_watch`
7. 数据降级能进入 `risk_tags`
8. 事件 flags 能进入 `event_tags`

### 9.2 API 测试

更新：

```text
tests/web/test_altcoin_route.py
```

断言：

- `/api/altcoin/radar/scan` 的每行包含 `display_state`
- `display_state.primary_state`、`label`、`tone` 存在
- 旧字段仍存在，保持兼容

### 9.3 UI 资产测试

更新：

```text
tests/test_altcoin_radar_ui_assets.py
```

断言：

- 前端读取 `display_state`
- 页面包含 `末端风险`、`点火观察`、`排名跃升`
- 旧版高级预警入口存在

## 10. 不建议做的事

1. 不建议删除 `signal_state`
   - 它仍然是有价值的结构画像。

2. 不建议删除 `signal_source`
   - 它是判断来源，不等同最终展示状态。

3. 不建议把 `点火` 和 `异动` 合成一个分数
   - 点火偏合约启动；异动偏价格/成交/波幅异常，两者语义不同。

4. 不建议把 `末端` 改成开空信号
   - 当前公式使用极端 funding、多空比偏离、拥挤风险等，更多是风险识别，不足以单独决定方向。

5. 不建议让 `跃升` 进入主状态
   - 跃升是排名事件，应该提示“变化很快”，不是独立市场状态。

## 11. 推荐结论

建议进行整理，但不要合并底层算法。

最优路径是新增一层 `display_state`：

- 底层继续保留所有分数和旧字段
- API 增加展示层主判断
- 前端按“主判断 / 风险提示 / 最近事件”重排
- 预警分成默认事件预警和高级结构预警

这样既不会破坏当前研究能力，也能显著降低用户误解成本，尤其能避免把 `末端/拥挤` 误读为“可以开空”，或者把 `跃升/异动` 误读为“可以直接追”。

