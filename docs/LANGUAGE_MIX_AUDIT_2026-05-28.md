# 中英文混杂与乱码审计报告（2026-05-28）

范围：`E:\9_Crypto\crypto_trading_system`

本次审计由三个只读子任务并行完成，并由主审复核高风险命中：

- 后端/策略代码：`core/`, `strategies/`, `prediction_markets/`, `web/api/`, `web/services/`
- 前端：`web/templates/`, `web/static/js/`, `web/static/css/`
- 配置/脚本/测试边界：`config/`, `scripts/`, `tests/`, 根目录配置

工作区在审计开始前已有未提交变更，本报告未尝试回滚或修改这些变更。除新增本报告外，本次没有修复业务代码。

后续修复状态：2026-05-28 第一轮已修复 P0 中的 `risk_manager.py` scoped 风险告警乱码、`trading.py` 交易时段乱码、`altcoin_radar.js` Watchlist 错误 toast 乱码，并补充静态/单元测试防回归。旧 `scripts/legacy/fix_mojibake.py` 也已改为默认 dry-run，必须显式传 `--write` 才会写回文件。

## 总体结论

代码库存在三类不同问题，修复时应分开处理：

1. **真实乱码 / 编码污染**：少量当前运行路径仍会返回或展示乱码，优先级最高。
2. **用户可见文案中英混杂**：大量 API、告警、toast、页面标签把中文自然语言和英文枚举/配置键混在同一字段中，需要统一展示层策略。
3. **历史修复残留与测试覆盖不足**：已有 mojibake 修复表和静态测试，但范围过窄，且部分旧脚本仍可写回前端资产。

自动扫描的关键基线（排除 `docs/`, `reports/`, `data/`, `.playwright-cli`, `models` 后）：

- `frontend`: 17 个文件含中文，约 4,956 行中文，4,678 行中英同处一行。
- `web_backend`: 13 个文件含中文，约 658 行中文，611 行中英同处一行。
- `backend_core`: 61 个文件含中文，约 1,069 行中文，656 行中英同处一行。
- `config_scripts`: 23 个文件含中文，约 670 行中文，462 行中英同处一行。
- 复核后的真实 mojibake 热点集中在 5 个文件：`core/risk/risk_manager.py`, `web/api/trading.py`, `web/static/js/altcoin_radar.js`, `web/static/js/ai_research.js`, `scripts/legacy/fix_mojibake.py`。其中 `ai_research.js` 多数命中是历史兼容修复表或注释块，不等同于当前页面可见乱码。

## P0：当前运行路径的真实乱码

### 1. 风险告警 payload 返回乱码

文件：`core/risk/risk_manager.py:581`

证据：

- `direction = "涓婂崌" if change_ratio > 0 else "涓嬮檷"`
- `title = "璐︽埛娉㈠姩棰勮"`
- `message = f"璐︽埛鏉冪泭鐭椂{direction}..."`

影响：这是 scoped equity 更新路径里的告警 payload，可能进入前端、通知或审计输出。该函数旁边的非 scoped 路径已经使用正常中文“上升/下降/账户波动预警”，说明这里是残留编码污染。

建议：直接改为与 `risk_manager.py:520-523` 同义的正常中文，并加静态测试覆盖该文件。

### 2. 交易时段 API 返回乱码

文件：`web/api/trading.py:6121`

证据：

- `return "浜氱洏"`
- `return "娆х洏"`
- `return "缇庣洏"`

影响：`_session_name()` 是 API 侧交易时段展示字段，乱码会直接污染前端或下游数据。

建议：恢复为“亚盘/欧盘/美盘”，并给 `_session_name()` 加最小单元测试。

### 3. 山寨雷达 Watchlist 错误 toast 乱码

文件：`web/static/js/altcoin_radar.js:1818`

证据：

- ``notify(`Watchlist 鏇存柊澶辫触: ${error.message}`, true)``

影响：手动加入 Watchlist 失败时用户可见。相邻代码 `altcoin_radar.js:1801` 已是正常 `Watchlist 更新失败`，说明这是单点遗漏。

建议：改为 `观察列表更新失败` 或统一为 `Watchlist 更新失败`。若产品语言以中文为主，建议使用中文主标签，英文作为括注。

## P1：用户可见 API / UI 文案风格不一致

### 4. 后端 API detail 混入英文模式码

文件：`web/api/ai_research.py:463`

证据：`当前系统处于 {mode} 模式...请先切换到 paper 模式，或改选“实盘候选（live_candidate）”。`

影响：用户看到中文说明时同时暴露 `paper`、`live_candidate` 这类内部/枚举值。类似情况还出现在执行门禁详情中，例如 `web/api/ai_research.py:2641-2650` 的 `enabled=false`, `agent`, `allow_live`, `provider` 等。

建议：API 返回稳定 code，例如 `mode_conflict`、`agent_disabled`，展示文案在前端 i18n 字典中拼装；如果后端必须返回中文文案，英文枚举用括注格式统一展示。

### 5. 下载任务状态同字段中英混用

文件：`web/api/data.py:4277` 和 `web/api/data.py:4322`

证据：

- `status_message = "任务已开始，正在下载历史K线"`
- `timeout_message = "Download task timed out after ... seconds"`

影响：同一个 `status_message/error` 流程里中文和英文交替出现，前端展示体验不一致，也增加自动化断言难度。

建议：统一为中文展示文案，或返回 `status_code/message_key/params`，由前端统一渲染。

### 6. 能力说明 payload 混入英文枚举

文件：`web/api/data.py:5676`

证据：payload 中 `mode = "realtime_only"`、`headline = "仅实时验证"`、`reason = "依赖实时盘口 / 跨场所 / 链上执行，单一 K 线回测会失真"`。

影响：同一 payload 同时承担机器契约和用户展示。长期看会让 API 兼容性与本地化绑死。

建议：保留 `mode` 作为英文稳定枚举，新增或规范 `display.headline/display.reason`，前端只显示 display 字段。

### 7. AI 研究复盘摘要混入内部 action label

文件：`web/api/ai_research.py:2216`

证据：`当前仍持有 {symbol} {action_label}，最新浮盈亏约 ... USDT。`

影响：`action_label` 的语言取决于上游数据，可能出现 `hold`、`close` 等英文动作和中文句子拼接。

建议：在 API 层统一 action 显示映射，或者只返回动作 code，前端统一翻译。

### 8. 前端模板和 JS 的术语未统一

主要范围：

- `web/templates/index.html`
- `web/static/js/app.js`
- `web/static/js/ai_research.js`
- `web/static/js/research_workbench.js`
- `web/static/js/altcoin_radar.js`

常见混用：

- `paper/live` vs `模拟盘/实盘`
- `watchlist` vs `观察列表`
- `close-only` vs `仅减仓`
- `shadow/enforce` vs `只提示/可拦截`
- `timeframe/lookback/params/bar` vs `周期/回看/参数/K线`
- `Funding/Basis/OFI/OI/News` 与中文句子直接混排

建议：建立前端术语表，技术缩写可保留英文，但必须使用固定格式，例如 `资金费率 (Funding)`、`未平仓量 (OI)`。

## P2：历史修复残留和测试覆盖不足

### 9. 旧 mojibake 修复脚本仍会写回运行资产

文件：`scripts/legacy/fix_mojibake.py:209` 和 `scripts/legacy/fix_mojibake.py:248`

证据：脚本直接读取并写回 `web/static/js/ai_research.js`。其中 `PLACEHOLDER_FIXES` 还包含大量 `????` 形态的宽匹配。

影响：虽然位于 `scripts/legacy/`，但仍是可执行写入脚本，误运行可能再次改坏前端资产。

建议：改成默认 dry-run，必须传 `--write` 才写文件；或归档到不可执行说明文档中。若保留，必须加注释说明仅用于历史迁移。

### 10. 静态内容质量测试覆盖过窄

文件：`tests/test_content_quality_static.py:14`

证据：当前只检查：

- `config/database.py`
- `core/data/funding_rate_collector.py`
- `web/static/js/ai_research.js`

且仅覆盖有限 marker。`core/risk/risk_manager.py`、`web/api/trading.py`、`web/static/js/altcoin_radar.js` 都未被该测试挡住。

建议：扩大扫描范围到 `core/`, `web/api/`, `web/static/js/`, `config/`, `scripts/`，并允许显式白名单，例如 `scripts/legacy/fix_mojibake.py` 的历史样本表。

### 11. Playwright 可见乱码检测不覆盖 GBK 形态

文件：`tests/playwright_smoke.spec.js:16`

证据：`hasMojibake()` 主要检测 `å|ç|æ|ä|é|è|ö|Ã|Â` 形态，不覆盖当前实际残留的 `涓/璐/浜/娆/缇/鏇/澶辫触` 等 GBK mojibake。

影响：即使 UI 出现当前类型的中文乱码，smoke 也可能漏报。

建议：补充 GBK mojibake marker，并把 Playwright smoke 纳入发布前检查；普通 `pytest` 不会自动运行 JS smoke。

### 12. 注释和历史兼容代码仍保留乱码样本

文件：

- `web/static/js/ai_research.js:356` 的 `LEGACY_MOJIBAKE_REPAIRS`
- `web/static/js/ai_research.js:2800` 的注释块残留
- `web/static/js/ai_research.js:4452` 的注释掉函数残留
- `web/static/js/ai_research.js:6042` 的乱码注释
- `web/static/js/ai_research_agent.js:219` 的本地 repair 正则
- `strategies/technical/macd_strategy.py:187` 的 `# ????????????`

影响：多数不直接展示给用户，但会让静态扫描噪声变大，也说明历史修复没有收尾。

建议：把历史修复表限定为数据兼容模块并加白名单注释；删除注释块中的乱码；把无意义问号注释改为明确英文或中文说明。

## P3：可接受但应规范的中文元数据

以下不是乱码，但需要产品/API 策略明确：

- `config/strategy_registry.py` 中大量 `category`, `usage`, `description` 为中文。
- `core/research/altcoin_radar.py` 中 `STATE_*` 常量值直接是中文状态。
- `web/api/altcoin.py` 对中文预警名做反向归一化。
- 多个 CLI 脚本输出中文，例如 `scripts/download_data.py`, `scripts/test_system.py`, `scripts/start_web.py`。

如果系统目标是中文操作界面，这些可以保留；但机器契约应避免依赖中文展示值。建议统一规则：

- API 和持久化字段：英文稳定 code。
- 前端展示：中文主文案。
- 技术术语：第一次出现用 `中文 (English/code)`，后续保持同一写法。
- CLI：面向人工的脚本可中文，面向自动化/日志解析的脚本使用英文 code 或 JSON。

## 建议修复顺序

1. 修复 P0 三处当前运行路径乱码：`risk_manager.py`, `trading.py`, `altcoin_radar.js`。
2. 扩展 `tests/test_content_quality_static.py`，确保 P0 类型不会回归。
3. 给 `scripts/legacy/fix_mojibake.py` 加 dry-run / `--write` 保护，或归档为不可写迁移记录。
4. 补强 `tests/playwright_smoke.spec.js` 的 GBK mojibake 检测。
5. 制定术语表并分批迁移用户可见 API/UI 文案。

## 复核命令

审计中使用的主要命令：

```powershell
rg --files core strategies prediction_markets web/api web/services -g "*.py"
rg -n "[\p{Han}]" core strategies prediction_markets web/api web/services -g "*.py"
rg -n "涓|璐|棰|鏉|泭|鐭|崌|嬮|浜氱洏|娆х洏|缇庣洏|鏇存柊澶辫触|[?]{8,}" core strategies web config scripts -g "!**/__pycache__/**"
rg -n "mojibake|乱码|中英|中文|英文|Watchlist|paper|live|close-only|shadow|enforce|timeframe|lookback|params" web core config scripts tests
python -m pytest tests/test_content_quality_static.py tests/test_infra_fixes.py::test_legacy_files_relocated tests/test_openai_responses_migration.py::test_clean_news_text_repairs_utf8_mojibake -q
```

子任务验证结果：配置/脚本审计员运行的 targeted pytest 为 `5 passed in 3.00s`。
