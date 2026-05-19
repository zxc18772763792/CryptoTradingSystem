# 代码审计报告 — 2026-05-19

> 范围：`E:/9_Crypto/crypto_trading_system` 全量 Python 源码（474 个文件）。
> 本轮为 [`CODE_AUDIT_2026-05-18.md`](CODE_AUDIT_2026-05-18.md) 之后的复核 + 全代码静态/运行时校验。

## 1. 方法

| 手段 | 覆盖 | 结果 |
|---|---|---|
| `python -m compileall .` | 474/474 文件 | ✅ 全部通过，无语法错误 |
| `pytest --collect-only` | 全测试树 | ✅ 1045 用例全部成功收集，无 import 错误 |
| `pyflakes .` 全量静态扫描 | 474 文件 | 见第 3 节 |
| `pytest -q`（全量，排除 integration） | 1045 用例 | ✅ **1045 passed**，170.9s，0 失败 |

## 2. 结论

代码整体健康：语法、导入、运行时测试全绿。上一轮审计（5-18）发现的风控/交易/策略核心问题已在提交 `902e54c` 中落地。本轮：修复 2 个缺失 import 潜伏缺陷；删除 autonomous_agent ~80 行死代码；将未接入实盘的 `stop_loss.py` 收敛标注；F541 f-string 全量修复。F401 未使用 import 自动批量删除经实测**不安全**（循环导入 / monkeypatch 桩点），已放弃并改为单独排期。全量测试保持 1045+ 全绿。

## 3. 本轮发现与处置

### P1 — 已修复（真实潜伏缺陷）

两处缺失 import。因文件头部有 `from __future__ import annotations`，注解被惰性化为字符串，运行时未触发 `NameError`——但属真实缺陷：一旦调用 `typing.get_type_hints()` 或移除 future import 即崩溃。

| 文件 | 问题 | 修复 |
|---|---|---|
| `prediction_markets/polymarket/features.py` | 函数签名用 `datetime` 注解（3 处）但未 import | 增加 `from datetime import datetime` |
| `scripts/maintain_top100_data.py` | 局部变量注解用 `Optional[Exception]`（2 处）但 typing 未导入 `Optional` | typing import 补 `Optional` |

修复后 `pyflakes` undefined-name 计数 5 → **0**；二文件 `py_compile` 通过；全量 1045 测试仍全绿。

### P0（结构性）— `core/risk/stop_loss.py` 未接入实盘 ✅ 已收敛

**发现**: `StopLossManager` / `TakeProfitManager` / `check_take_profit` / `set_take_profit`
/ `calculate_stop_price` / `update_trailing_stop` 在 `core/trading/`、`web/`、
`risk_manager.py` 中**零调用**——仅 tests 与 `core/risk/__init__.py` re-export 引用。
真实实盘 SL/TP 在 `core/trading/execution_engine.py`（`take_profit_pct`、
`partial_take_profit_*`、fixed-stop / 交易所侧保护单）。

**影响**: 提交 902e54c 对 R-3/R-4 的修复只作用于测试面，不改变实盘行为；新增的
`confirm_take_profit`/`release_take_profit` 契约在生产中无调用方。存在两套可能发散的
SL/TP 逻辑，且看似权威的一套实为死路径，易产生虚假信心。

**处置（用户决策：收敛/降级为内部工具）**: 在 `stop_loss.py` 模块 docstring 顶部加明确
标注「NOT ON THE LIVE TRADING PATH」+ 双重平仓风险警告，保留模块（tests 仍依赖），
不引入第二套强平。`CODE_REVIEW_2026-05-19.md` 的 R-3/R-4/T-4 条目已同步对齐。

### P3 — ✅ 已修复（行为正确的代码异味）

- **`core/ai/autonomous_agent.py`**：`_describe_model_feedback_issue_legacy` 与
`_describe_model_feedback_issue` 各有一份约 40 行被有意覆盖的旧实现（约 80 行死代码）。
**已删除**两份旧实现，仅保留委托 `_shared_describe_model_feedback_issue` 的薄包装；
`py_compile` 通过，pyflakes 无 redefinition。

### P3 — 卫生项（本轮部分处理）

| 类别 | 数量 | 处置 |
|---|---|---|
| 无占位符的 f-string (F541) | 18 | ✅ 已用 ruff 全量修复（去掉多余 `f` 前缀，零行为风险） |
| 未使用 import (F401) | 269 | ⚠ **本轮放弃自动批量删除**。ruff `--fix` 实测在本仓引发回归：`core/ops/service/api.py` 循环导入暴露、`web/api/trading.py` 等被测试 `monkeypatch` 的「仅测试引用」import 被误删，导致 4 处采集错误 + 顺序相关测试失败。结论：本仓存在循环导入、`monkeypatch` 桩点、按导入序初始化的单例，**F401 自动删除不安全**，需逐模块人工审查 + CI check-only 守卫，单独排期。 |
| 未使用局部变量 (F841) | 23 | 同上，留待人工/CI |
| 测试内重复 import（AsyncMock） | 3 | `tests/test_ai_research_phase2.py`，无害 |

### 信息级

- 测试期 42 条 `DeprecationWarning`：`datetime.utcnow()` 在 Py3.13 弃用，绝大多数来自第三方（pydantic / sqlalchemy）；本仓内仅 `tests/test_altcoin_notification_manager.py:166` 一处，且为测试夹具刻意构造 naive 时间，可保留。
- 环境提示：`requests` 报 urllib3/charset 版本不完全匹配，不影响测试结果。

## 4. 仍待单独排期（沿用上轮结论）

未在本轮重复处理，详见 [`CODE_AUDIT_2026-05-18.md`](CODE_AUDIT_2026-05-18.md) 第 7 节：
- G-4 巨型单文件拆分（web/api 42K、autonomous_agent、execution_engine）
- G-3 fire-and-forget task 统一治理（76 处）
- web/api 二轮专项深审

## 5. 一句话总结

全量 474 文件编译通过、1045 测试全绿；本轮修复 2 个潜伏 import 缺陷 + 清死代码 + 收敛未接线的 `stop_loss.py` + F541 全量修复；F401 自动清理因回归风险放弃改排期，**当前无阻塞性问题**。
