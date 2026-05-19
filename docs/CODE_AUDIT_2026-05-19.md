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

代码整体健康：语法、导入、运行时测试全绿。上一轮审计（5-18）发现的风控/交易/策略核心问题已在提交 `902e54c Stabilize live trading order routing` 中落地。本轮新发现 2 个真实潜伏缺陷，已修复；其余为低优先级卫生项。

## 3. 本轮发现与处置

### P1 — 已修复（真实潜伏缺陷）

两处缺失 import。因文件头部有 `from __future__ import annotations`，注解被惰性化为字符串，运行时未触发 `NameError`——但属真实缺陷：一旦调用 `typing.get_type_hints()` 或移除 future import 即崩溃。

| 文件 | 问题 | 修复 |
|---|---|---|
| `prediction_markets/polymarket/features.py` | 函数签名用 `datetime` 注解（3 处）但未 import | 增加 `from datetime import datetime` |
| `scripts/maintain_top100_data.py` | 局部变量注解用 `Optional[Exception]`（2 处）但 typing 未导入 `Optional` | typing import 补 `Optional` |

修复后 `pyflakes` undefined-name 计数 5 → **0**；二文件 `py_compile` 通过；全量 1045 测试仍全绿。

### P3 — 记录，未改（行为正确的代码异味）

- **`core/ai/autonomous_agent.py:513 / 551`**：`_describe_model_feedback_issue_legacy` 与 `_describe_model_feedback_issue` 各有一份约 40 行的完整旧实现，随后被 590/594 行的薄包装（委托 `_shared_describe_model_feedback_issue`）**有意覆盖**（源码含注释说明）。运行行为正确（后定义生效），但约 80 行死代码易误导维护者。建议后续清理时直接删除前一份实现，非紧急。

### P3 — 卫生项（低优先级，不影响功能）

| 类别 | 数量 | 说明 |
|---|---|---|
| 未使用 import | 269 | 多集中在 scripts/ 与测试；建议引入 ruff 统一清理 |
| 未使用局部变量 | 23 | 同上 |
| 无占位符的 f-string | 18 | 多余的 `f` 前缀，纯字符串，建议去掉 `f` |
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

全量 474 文件编译通过、1045 测试全绿；本轮修复 2 个缺失 import 的潜伏缺陷，其余均为非功能性卫生项，**当前无阻塞性问题**。
