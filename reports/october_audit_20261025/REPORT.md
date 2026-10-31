# 10 月新增功能与研究审计及修复 · 2026-10-25

本轮确认并修复 **16 类代码与研究核算问题**。主要影响研究证据的可信度：开发/前向数据边界、缺失持仓后的收益偏差、资金费归属、研究进程并发，以及采集与页面汇总之间的断链。

主交易系统的隔离非 live 全量回归为 **3800 通过、3 跳过、0 失败**；最终源码的相关回归另为 **169 通过**，其中新增审计反例 25 项。两组有重叠，不相加。前端 Node 测试 13 项通过。未提交或推送代码。

## 范围和方法

- 主项目：`F:\9_Crypto\crypto_trading_system`，审计起点 HEAD `c55b3ca456ac5abc167fd2d98f1f1c1cf5db5b49`；审计期间另有一笔新 KOL 研究提交 `1e9f9c11be123028a38717c72614d5728a801de0`，已补查其脚本和研究表述。以 10 月前最后提交 `2fecd05` 为变更基线，当前 10 月以来核心代码、脚本、配置、前端与测试共有 338 条变更路径；历史报告/二进制产物另计。
- 人工重点：Coinglass 组合及反转跟踪器、公告/韩国警示跟踪器、研究循环 v3 和横截面评价、X 采集/归档/事件及面板研究、Telegram 前向运行器和汇总接线；并回看 10/01、10/22 的审计及现有交易/权限整改。
- 自动覆盖：主项目 Python、JavaScript、JSON、YAML 语法；PowerShell 解析；交易执行、账户隔离、RBAC、风控、数据时区、研究和 Web API 现有回归。
- 测试使用项目 Python 3.11 的独立源码快照；移除真实凭据和运行数据，拦截外网、现有服务连接及原项目访问，写入限于临时目录。未真实采集 Telegram/X、调用模型或提交订单。依赖试算仅联网读取 conda 包索引。
- 父工作区的 `_radar_refactor` 和 `ake_research` 是另外的工作/研究目录，不纳入这份主交易系统结论。未运行其研究或链上采集脚本。

## 已修复问题

| ID | 级别 | 原错误与影响 | 修复 |
|---|---|---|---|
| OCT-01 | P1 | Coinglass 组合到期后，缺失个别持仓仍以剩余持仓的均值结算，改变原始权重并可能制造利润 | 超过宽限期后标记 unresolved；旧账本中 missing_positions 非零的组合也不计入收益和退役样本 |
| OCT-02 | P1 | 反转主组合及清算过滤影子同样会丢掉失联持仓，再以幸存持仓计算收益 | 不完整组合保留为 unresolved；主/影子收益及配对统计排除明确记录为缺仓的历史结果 |
| OCT-03 | P1 | v3 开发期最后 7 天的前瞻标签使用了留出期价格，开发反馈与模型面板摘要泄漏留出信息；旧指标还会继续回灌 | 开发信号截止日清除 7 天重叠，完整标签须在留出期前结束；旧账本中越界或无法证明边界的指标不再反馈给模型 |
| OCT-04 | P1 | 前向验证只按 frozen_panel_end 起算；面板落后时可能把公式冻结前的历史数据当作前向 | 起点改为 UTC 冻结日与冻结面板截止日的较晚者之后一天 |
| OCT-05 | P2 | 前向判断仅等待日历天数；有效打分日不足设定样本量仍可能通过 | 同时要求评价状态有效且实际打分日满足 forward_min_days；不足继续观察 |
| OCT-06 | P2 | v3 配置进程与研究子进程分别读改写状态，原子替换仍会互相覆盖配置/轮次 | 增加同进程及跨进程文件锁，覆盖完整读改写事务；验证两个真实子进程并发累计不丢更新 |
| OCT-07 | P2 | X 存档在磁盘追加成功前就更新内存去重集；瞬时写盘失败后，同一进程会永久跳过尚未保存的帖子 | 追加并关闭成功后才提交去重集；同批重复使用独立待写集合 |
| OCT-08 | P2 | X 共享请求账本压缩会删除 who 来源字段，且与其他进程追加竞争，可能丢请求记录 | 锁住追加/压缩事务，保留完整来源信息，以临时文件原子替换压缩结果 |
| OCT-09 | P1 | X live 面板以发帖时间入场，不检查首次收到帖子是否已经晚于午夜入场；其输出还会套用第 8 节正向、价格收益检验的通过结论 | 事件保留 first_seen_at，实时信号须在入场前已收到，覆盖起点也按接收时间；live 输出明确为描述性，第 9 节反向且含资金费的假设保持 not_assessed，禁止借价格表宣称通过 |
| OCT-10 | P2 | Telegram cycle 持续更新状态和账本，却不刷新页面读取的 forward_summary.json，页面收益长期过期 | 在外围 cycle 包装器内，成功周期结束后、释放研究锁之前刷新派生汇总；冻结 CLI、交易规则及其实现哈希文件不变 |
| OCT-11 | P1 | 公告分钟线内部缺口可能包含止损，但原逻辑仍按随后收盘价记盈利 | 验证从首个持仓完整分钟开始的连续价格路径；缺口先重试，长期不足记 unresolved |
| OCT-12 | P2 | 公告源请求失败时，tick 在已有仓位结算之前退出 | 先结算并落盘已有持仓，再拉新公告；源错误仍原样上报 |
| OCT-13 | P2 | 韩国警示跟踪与回测把恰好入场时点的资金费也计入新仓位，与对冲腿和其他账本的边界不一致 | 统一为入场后、退出时点及以前的结算，即 (entry, exit] |
| OCT-14 | P2 | 已止损的韩国警示回测仍要求计划持有期末的 K 线和资金费，拒绝本来完整的已退出样本 | 先确定实际止损/退出时点，再只验证持仓期间的连续价格和资金费覆盖 |
| OCT-15 | P1 | 新增四频道 Telegram 研究将永续事件、随机对照和计划模拟的缺失资金费补零；共享小时面板还会把持仓窗口内部的 NaN 结算当零，却按扣实际资金费的净收益报告 | 实际事件及随机对照都要求资金费覆盖；面板保留无效结算标记并验证整个持仓窗口，模拟缺失则 unavailable，现货才明确采用零资金费 |
| OCT-16 | P1 | 同一新 KOL 研究中，实际收益有一倍保证金下限，但随机对照仍可低于 -100%，凭空提高实际相对对照的超额 | 对照采用与实际相同的收益下限；合成清算反例证明原来成本会凭空产生正超额 |

OCT-14 同时修复新 KOL 计划模拟要求整个 168 小时未来价格、即使早已触及止损也不结算的问题。新 KOL 报告中关于缺失帖子偏差方向、分钟级执行必然更差、负结果不受频道选择偏差影响的断言也已改正，旧数字明确标为尚未按修复后口径重算。

主要修复入口：

- [研究循环](F:/9_Crypto/crypto_trading_system/core/ai/research_loop_v3.py)、[评价边界](F:/9_Crypto/crypto_trading_system/core/research/xs_return_evaluation.py)、[文件锁](F:/9_Crypto/crypto_trading_system/core/utils/file_lock.py)
- [Coinglass 组合](F:/9_Crypto/crypto_trading_system/core/research/coinglass_composite_tracker.py)、[反转账本](F:/9_Crypto/crypto_trading_system/core/research/xs_reversal_tracker.py)、[配对评价](F:/9_Crypto/crypto_trading_system/core/research/paired_evaluation.py)
- [公告跟踪](F:/9_Crypto/crypto_trading_system/core/research/announcement_short_tracker.py)、[韩国警示跟踪](F:/9_Crypto/crypto_trading_system/core/research/upbit_caution_tracker.py)、[相关回测](F:/9_Crypto/crypto_trading_system/scripts/upbit_short_backtest.py)
- [X 存档](F:/9_Crypto/crypto_trading_system/core/news/x_archive.py)、[请求账本](F:/9_Crypto/crypto_trading_system/core/news/x_pacing.py)、[X 事件](F:/9_Crypto/crypto_trading_system/scripts/research_x_kol.py)、[X 面板](F:/9_Crypto/crypto_trading_system/scripts/research_x_panel.py)
- [Telegram 周期包装器](F:/9_Crypto/crypto_trading_system/scripts/telegram_forward_cycle.py)
- [新 KOL 研究及修订说明](F:/9_Crypto/crypto_trading_system/scripts/research_telegram_new_kols.py)、[研究文档](F:/9_Crypto/crypto_trading_system/docs/TELEGRAM_NEW_KOLS_RESEARCH_20261025.md)
- [共享 Telegram 小时面板](F:/9_Crypto/crypto_trading_system/scripts/review_telegram_signals.py)
- [新增审计反例](F:/9_Crypto/crypto_trading_system/tests/test_october_audit_regressions.py)

## 验证记录

| 验证 | 结果 | 证据 |
|---|---|---|
| 隔离全量 pytest，排除 live，保留 slow | 3800 通过、3 跳过；约 788 秒 | [全量日志](F:/9_Crypto/crypto_trading_system/reports/october_audit_20261025/pytest_201505.log)、run_201505.json、pytest_201505.xml |
| 最终源码的所有相关测试模块 | 169 通过；约 56 秒 | [最终相关回归](F:/9_Crypto/crypto_trading_system/reports/october_audit_20261025/pytest_204552.log)、run_204552.json、pytest_204552.xml |
| 首批修复前反例 | 8 项按预期失败，确认原始错误行为 | pytest_200240.log |
| 后续修复前反例 | 资金费、数据边界、旧反馈等失败均有独立记录 | pytest_200952.log、pytest_201401.log、pytest_201740.log、pytest_202404.log |
| 新 KOL 研究修复前反例 | 5 项按预期失败，确认资金费缺失、退出路径及对照收益下限错误 | pytest_203806.log、pytest_203932.log |
| 共享小时面板修复前反例 | 持仓窗口内部 NaN 资金费被记成零，按预期失败 | pytest_204521.log |
| 前端 Node 展示/调度回归 | 13 项通过 | tests/agent_details_scheduling.test.cjs、research_tracker_diagnostics.test.cjs、risk_panel_display.test.cjs、agent_model_feedback.cjs、dashboard_attribution_coverage.cjs |
| 语法解析 | Python 1058、JS/CJS 25、YAML 7、JSON 2，无解析错误 | static_checks.json |
| PowerShell 解析 | 32 个文件，0 个错误 | powershell_syntax.json |
| 用户原有未提交文件 | 两份文件 SHA 与审计前快照完全相同 | verification_summary.json |

全量源码快照早于最后的训练标签/旧反馈、止损后价格路径及新 KOL 核算补修；这些最终改动由 169 项相关回归覆盖。22 个修改文件的内容哈希与最终受测快照完全一致，见 final_validation_manifest.json。未把先前快照的全量结果冒充最终源码的再次全量执行。

首轮全量有 20 项 Alastar intake 失败，根因是隔离目录把证据文件路径推到 263–265 个字符，触发本机 Windows 长路径限制。缩短独立测试目录后该模块 43 项通过，第二轮全量也通过。未因此修改 Alastar 的冻结研究规则。

全量唯一警告为 Starlette/httpx TestClient 弃用提示；没有为消除警告升级正在运行的环境。浏览器页面没有做完整端到端走查，前端结论来自单元/资产与语法检查。

## 环境差异和运行状态

`scripts/verify_env.py` 确认启动所需及可选模块均可导入；但 `scripts/check_dependency_contract.py --include-dev` 只满足 36/50 项声明，另有以下 14 项版本或安装差异：

| 包 | 当前安装 | 项目要求 |
|---|---|---|
| aiohttp | 3.14.3 | >=3.14.4,<4 |
| web3 | 8.0.0 | >=7.6,<8 |
| schedule | 未安装 | >=1.2,<2 |
| pytest | 8.4.2 | >=9.0.3,<10 |
| pytest-asyncio | 0.26.0 | >=1,<2 |
| pytest-cov | 未安装 | >=6,<7 |
| mypy | 未安装 | >=1.14,<2 |
| types-redis | 未安装 | >=4.6,<5 |
| certifi | 2026.6.17 | >=2026.10.19 |
| urllib3 | 2.7.0 | >=2.8.0,<3 |
| multidict | 6.7.1 | >=6.9.1,<7 |
| PyJWT | 2.13.0 | >=2.15.0,<3 |
| tornado | 6.5.8 | >=6.5.9,<7 |
| Pillow | 12.3.0 | >=13,<14 |

已使用 conda-forge、锁定 Python 3.11 / NumPy 1.26 / pandas 2.3 / PyArrow 24 并保持其他已安装包做 **dry-run**，求解成功，约 53 秒。方案涉及 95 个链接包及 2 个解除链接包；这只是可行性试算，不等于已安装、已完成安全升级或已在升级环境验证。

入口：[依赖试算脚本](F:/9_Crypto/crypto_trading_system/reports/october_audit_20261025/dependency_plan.py)；证据 dependency_plan.json、dependency_plan.log、verification_summary.json。没有降低项目的依赖安全版本要求。

只读状态确认 Web、新闻采集和 LLM worker 正在运行，Web 为 paper，自治交易代理停止。本轮没有停止/重启这些服务，也没有替换其正在使用的原生依赖；因此依赖差异仍待维护升级，Web/worker 内已导入的业务模块要经托管重启后加载修复。Telegram 子进程每周期启动，下一周期会自然读取新的外围包装器。

## 研究证据和文件保留

旧研究报告、原始数据、冻结协议、已登记阈值、原始账本均保留。明确缺仓的历史结果在汇总/统计时被排除，没有改写成虚构的完整收益。

资金费边界和训练标签修正意味着相关旧回测/开发指标不能直接当作已按修复后口径复核的结果；本轮没有重新采集或重算真实研究数据，也没有更新旧报告中的收益数字。X 第 9 节的反向假设仍需独立实现并验证实际资金费，当前输出仅为描述性，不能判为通过。

`scripts/news_library.py` 和 `docs/NEWS_RESEARCH_LIBRARY.md` 是进入审计前已有的用户改动；两者内容哈希均完整保留。其他研究产物没有搬移或删除。所有本轮代码与报告留在本地工作区，供逐文件审阅。
