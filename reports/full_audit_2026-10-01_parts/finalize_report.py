"""Build the audit deliverables; only write dated audit artifacts under reports."""
from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
PARTS = Path(__file__).resolve().parent
BASE = ROOT.as_posix()
HEAD = "2fecd05dea8c5ee438b4071e50548b828b7411af"
INVENTORY = json.loads((PARTS / "inventory.json").read_text(encoding="utf-8"))

def link(path: str, line: int | None = None, label: str | None = None) -> str:
    target = f"{BASE}/{path}" + (f":{line}" if line else "")
    return f"[{label or path}]({target})"

def part(name: str, label: str | None = None) -> str:
    return link(f"reports/{PARTS.name}/{name}", label=label or name)

json_errors = []
json_paths = [r["path"] for r in INVENTORY["files"] if r["path"].endswith(".json")]
for path in json_paths:
    try:
        json.loads((ROOT / path).read_text(encoding="utf-8-sig"))
    except Exception as exc:
        json_errors.append({"path": path, "error": str(exc)})
(PARTS / "json_syntax.json").write_text(json.dumps({"files": len(json_paths), "errors": json_errors}, ensure_ascii=False, indent=2), encoding="utf-8")

ignored = subprocess.check_output(["git", "ls-files", "--others", "--ignored", "--exclude-standard", "--", "*.py", "*.js", "*.cjs", "*.ps1", "*.bat"], cwd=ROOT, text=True).splitlines()
ignored_records = []
for path in ignored:
    data = (ROOT / path).read_bytes()
    text = data.decode("utf-8-sig")
    status = "静态副作用审阅；未执行"
    if path.endswith(".py"):
        ast.parse(text, filename=path)
        status += "；AST通过"
    ignored_records.append({"path": path, "sha256": hashlib.sha256(data).hexdigest(), "lines": len(text.splitlines()), "status": status})
(PARTS / "ignored_source_inventory.json").write_text(json.dumps(ignored_records, ensure_ascii=False, indent=2), encoding="utf-8")

# These names/locations are checked against the detailed, independently reviewed parts.
FINDINGS = [
    ("T1", "P1", "反手新开敞口被整单平仓豁免风控", "core/trading/execution_engine.py", 5805, "反向非reduce-only量超过旧仓；可越过组合敞口/杠杆检查"),
    ("T2", "P1", "聚合持仓复制到每个策略", "core/trading/execution_engine.py", 3774, "同账户同标的同侧多策略；本地持仓重复放大"),
    ("T3", "P1", "订单ID跨账户覆盖", "core/trading/order_manager.py", 802, "两账户返回相同裸ID；取消/查询发往错误账户"),
    ("T4", "P1", "基币量与合约张数混用", "core/exchanges/okx_connector.py", 232, "衍生品contractSize非1；订单与对账数量错倍率"),
    ("T5", "P1", "部分平仓后累计PNL重复入账", "core/trading/execution_engine.py", 5505, "部分平仓后专用CLOSE；统计和日内风控错误"),
    ("T6", "P1", "Binance fast REST丢失sandbox", "core/trading/binance_rest.py", 152, "sandbox账户走fast REST；请求指向生产域名"),
    ("T7", "P2", "算法单把零成交子单计为已成交", "core/trading/execution_engine.py", 6311, "OPEN/filled=0；汇总filled与closed状态错误"),
    ("T8", "P2", "零成交仍标记部分止盈完成", "core/trading/execution_engine.py", 2380, "减仓单接受但未成交；止盈保护被提前消费"),
    ("A3", "P3", "离线PNL分解遗漏开仓成本", "core/accounting/pnl_decomposer.py", 249, "离线lot消费；净收益偏高，未找到生产调用"),
    ("WEB-01", "P1", "策略实盘入口缺approve_live", "web/api/strategies.py", 2815, "有manage_strategies但无实盘批准角色；可设置并启动live"),
    ("WEB-02", "P1", "只读角色可解除熔断", "web/api/risk.py", 63, "合法AUDITOR/ENGINEER身份；无风险管理能力检查"),
    ("WEB-03", "P1", "允许范围清空/替换不走增风险审批", "core/governance/service.py", 124, "集合变化非严格超集；空集合实际变为不限制"),
    ("WEB-04", "P1", "普通风险配置应用清除日内停机", "core/governance/service.py", 234, "kill_switch=false配置应用；重置亏损基线并进入保护宽限"),
    ("WEB-05", "P1", "策略摘要未转义持久化字段", "web/static/js/app.js", 3371, "可写名称/符号进入innerHTML；交易页面脚本执行风险"),
    ("WEB-06", "P2", "profile输出可覆盖仓库业务文件", "core/ops/service/polymarket_routes.py", 170, "有研究管理能力并指定output_path；仅限仓库根不够"),
    ("WEB-07", "P2", "Ops新闻变更/审计读取能力缺口", "core/ops/service/news_routes.py", 13, "有效身份但无manage_news/read_audit；能到达敏感操作"),
    ("WEB-08", "P1", "可选手工信号接口缺订单授权", "core/ops/service/api.py", 1166, "仅OPS_ALLOW_MANUAL_SIGNAL开启时；当前启用状态未读取"),
    ("WEB-09", "P2", "熔断解除审计签名错误被吞", "web/api/risk.py", 77, "成功reset后的log调用；OperationAudit记录缺失"),
    ("WEB-10", "P2", "WS初始发送失败遗留订阅", "web/main.py", 2766, "hello异常发生在try/finally之前；队列与fanout增长"),
    ("WEB-11", "P2", "审计文件重写与追加竞态丢记录", "core/audit/gate_counterfactuals.py", 190, "读快照后并发append再replace；新trace丢失"),
    ("WEB-12", "P2", "未认证GET推进共享回放游标", "web/api/data.py", 6949, "已知回放ID；改变其他使用者回放状态"),
    ("DR-01", "P1", "源旧行情仍通过新鲜度检查", "core/marketdata/hub.py", 376, "旧源时间戳重发；接收时间不断刷新ok"),
    ("DR-02", "P1", "AI enforce无效action默认放行", "core/ai/live_decision_router.py", 761, "缺字段/未知动作；fail_open=false也允许"),
    ("DR-03", "P1", "新闻先提交游标再截断和保存", "core/news/collectors/manager.py", 428, "批量超过max_records或后续保存失败；永久漏数据"),
    ("DR-04", "P2", "单标的ML标签切分无purge", "core/ml/pipeline.py", 401, "forward标签触及测试期；holdout泄漏"),
    ("DR-05", "P2", "因子缓存忽略非close输入", "core/factors_ts/cache.py", 95, "high/low/volume等修订；复用错误因子结果"),
    ("DR-06", "P2", "DSL前导缺失bfill读未来", "core/research/strategy_program.py", 334, "legacy/general source前导NaN；历史信号提前可见"),
    ("DR-07", "P2", "DSL缺少source静默换成close", "core/research/strategy_program.py", 335, "列缺失/变量拼错；策略语义发生变化"),
    ("DR-08", "P2", "回测每笔净收益漏entry fee", "core/backtest/backtest_engine.py", 724, "非零开仓fee；权益已亏但交易被标盈利"),
    ("DR-09", "P2", "分析器频率/Calmar单位错误", "core/backtest/performance_analyzer.py", 138, "非日频或非零回撤；风险调整指标不可比"),
    ("DR-10", "P2", "正无穷报价被认定有效", "core/marketdata/runtime_price_provider.py", 43, "price=inf；缺finite检查，未证明必定真实下单"),
    ("DR-11", "P2", "预测市场纸盘成交不原子", "prediction_markets/polymarket/paper_trading.py", 105, "第三步失败后重试/并发；重复加仓扣款"),
    ("DR-12", "P2", "纸盘持仓上限不含待买预留", "prediction_markets/polymarket/paper_trading.py", 231, "同token多张OPEN BUY；成交后超限"),
    ("DR-13", "P2", "纸盘按任意年龄报价成交", "prediction_markets/polymarket/paper_trading.py", 109, "latest为多年旧价；仍FILLED"),
    ("DR-14", "P2", "Gamma fallback未按outcome映射", "prediction_markets/polymarket/worker.py", 66, "CLOB失败fallback；YES/NO同价且旧快照被标新"),
    ("DR-15", "P2", "CUSUM预热与降级状态机不一致", "core/monitoring/strategy_monitor.py", 122, "不足min_bars可触发；paper→shadow被强制改retired"),
    ("OPS-01", "P2", "启停脚本未隔离工程/端口进程", "scripts/web.ps1", 196, "主工程与工作树/多端口共存；停止或去重波及另一实例"),
    ("OPS-02", "P2", "新闻worker继承Web健康探针", "Dockerfile", 71, "compose news_service运行纯worker；8000/livez不存在"),
    ("QA-01", "P2", "离线测试依赖未跟踪数据与隐式token", "tests/test_pump_precursor.py", 88, "干净克隆/清洁环境；4个数据依赖失败及1个认证前提失败"),
    ("IS1", "P2", "辅助验收失败仍退出成功", "data/_level1_relaxed_run.py", 34, "evaluator非零或watcher无输出；父shell仍见exit0"),
    ("IS2", "P2", "HTTP错误被计为代理行情健康", "data/_ws_proxy_monitor.py", 55, "futures time返回401/429/500但stream可达；计为ok"),
]

def overview_table() -> str:
    rows = ["| ID | 级别 | 问题 | 准确定位 | 触发及影响 |", "|---|---|---|---|---|"]
    for ident, severity, title, path, line, trigger in FINDINGS:
        rows.append(f"| {ident} | {severity} | {title} | {link(path, line)} | {trigger} |")
    return "\n".join(rows)

report = f"""# F:\\9_Crypto 全量审计报告 · 2026-10-01

本次确认 **41项代码/工程问题：P0 0项、P1 15项、P2 25项、P3 1项**。其中WEB-08是代码中已确认的条件启用权限缺口，当前实例是否开启未读取。资金与控制边界有多项高优先缺陷，不能仅凭测试多数通过认定实盘安全。报告没有声称真实损失、账户利用或生产订单已发生。

交付为本报告、{link('reports/full_audit_2026-10-01_coverage.md', label='全量覆盖清单')}及{part('inventory.json', '逐文件机器清单')}。所有新增产物位于带日期的reports路径；**没有自动修复业务代码**。适用AGENTS、README、安全/治理与启动文档、Git状态均已重新读取/核对。真实密钥文件、交易账户、原始研究数据与生产参数未读取或修改；审计未启动服务、worker或发送真实订单。

## 审计基线与工程边界

| 对象 | 当前证据 | 本次处理 |
|---|---|---|
| 主工程 | `F:\\9_Crypto\\crypto_trading_system`，HEAD `{HEAD}`，分支`codex/operating-reality-bug-sweep` | 1239个Git跟踪文件；874个Python文件共300242行；运行非LIVE隔离测试 |
| `_radar_refactor` | HEAD `8a627bf597e1f67c37a15aedfbeb70bce60b9be3`，分支`refactor/radar-modularize` | Git已注册的关联工作树；984文件/774 Python/281583行；AST全量检查，独有7文件补丁深读，不重复旧测试 |
| 工作区父目录 | 独立Git根；仅跟踪`.gitignore`、主工程gitlink、`docs/PROJECT_AGENT_HANDOFF_2026-06-08.md` | 交接文档与边界审阅；不是第二套业务系统 |
| `.conda`、编辑器元数据 | 外部运行环境与`.claude/.vscode`配置 | 环境依赖验证，排除第三方包逐行审计 |
| ignored源码辅助 | 12个`.py/.cjs/.ps1`，位于data、logs、tmp | 只读源码/副作用审阅，逐项单列，不执行历史维护辅助 |
| ignored数据/账户状态 | 原始行情、研究快照、运行日志、秘密配置 | 不覆盖、不复算、不读取真实账户状态；其业务来源和消费代码在范围内 |

`git worktree list --porcelain`证明`_radar_refactor`为活动关联工作树，与AGENTS中的旧描述不一致，以当前Git状态为准。主工程和工作树有194个第一方路径差异（config3、core65、scripts43、tests60、web23），但大多数为主工程更近的演进；工作树独有1个提交，7文件补丁为+373/-3。补丁全文审阅未确认新增安全边界缺陷；旧journal全文件读取及前端版本更新属于待验证性能/发布风险，不能推导工作树整体无缺陷。证据见{part('radar_inventory_summary.json')}、{part('radar_diff_paths.txt')}及本报告Web附录。主工程开始时干净；最终只新增日期审计文件，工作树无修改。

## 优先处理顺序

1. 先收敛所有实盘入口授权（WEB-01/02/08），把反手关闭与新开风险检查拆开（T1），隔离真实账户订单身份（T3）和sandbox路由（T6）。这些边界应先在模拟交易所验证拒绝矩阵，再评估上线。
2. 修复聚合持仓归因、合约单位、每次成交PNL以及熔断独立状态（T2/4/5、WEB-03/04）；对账必须覆盖部分成交、重启恢复和多策略共享账户。
3. 修复源行情时效、AI schema和新闻可靠入库（DR-01/02/03），再修回测/模型泄漏、纸盘原子性及其他P2。修复后运行确定性反例，不应仅重跑现有绿灯测试。

上述是代码证据的优先级，不推断当前账户或服务的实际模式。P0代表立即且广泛的灾难性风险，本次没有足够证据归为P0；P1为资金/关键权限或常规数据完整性高影响；P2为有条件正确性、纸盘/研究/运行可靠性；P3为低影响或离线组件问题。没有在真实账户验证不意味着缺陷可忽略。

## 确认问题索引

下表去重后的41项均在后文给出触发、影响、证据和修复建议。跟踪代码行号对应上述HEAD；ignored脚本对应本次只读快照SHA。并列根因合并计数（DR-09频率/单位、DR-15预热/降级），不同DSL缺源与未来填充分列。A3未找到生产调用，按P3计算。待验证风险不进入计数。

{overview_table()}

## 验证结果与安全隔离

使用AGENTS指定解释器`F:\\9_Crypto\\.conda\\miniforge3\\envs\\crypto_trading\\python.exe`，Python3.11.16。`scripts/verify_env.py --strict`通过，`python -m pip check`无损坏依赖。未安装或更新任何包。

| 检查 | 结果 | 范围/证据 |
|---|---|---|
| Python AST | 主工程874/874、工作树774/774成功 | {part('inventory.json')}、{part('radar_inventory_summary.json')} |
| PowerShell解析 | 21/21通过 | {part('powershell_syntax.json')} |
| JavaScript/CJS语法 | 19/19 `node --check`通过 | {part('javascript_syntax.json')} |
| YAML | 10/10 safe_load通过 | {part('yaml_syntax.json')} |
| 跟踪JSON | {len(json_paths)}个，错误{len(json_errors)} | {part('json_syntax.json')}；只验语法，不等于业务schema验证 |
| 配置契约 | `scripts/check_config_contract.ps1`通过 | 对`.env.example`与源码检查，不读取真实env |
| Git空白/冲突检查 | `git diff --check`通过 | 没有业务代码改动 |
| 定向危险原语/密钥模式 | 跟踪UTF-8文本扫描无命中 | 非穷尽语义安全/CVE审计，不读取秘密文件 |
| 交易边界复现 | 8项关键断言均通过；另复现A3开仓成本漏计 | {part('trading_repro_root_verified.json')} |
| 数据/研究复现 | 13组离线原函数观测成功，覆盖15项问题；关键断言root复核通过 | {part('data_research_root_verified.json')} |
| Web/治理/运维复现 | mock权限、路径、队列、审计并发与进程选择证据成立 | {part('web_security_repro_results.json')}、{part('web_security_xss_results.json')}、{part('ops_process_repro.json')} |

测试副作用先经源码审查；完整pytest在临时目录的**HEAD git archive快照**运行，未导入真实env。子进程只继承基础OS环境，显式paper和worker关闭；sitecustomize guard阻止打开原主工程/工作树路径、阻止外部DNS与连接，仅允许测试自行拥有的loopback服务和已审阅的Python/PowerShell子进程。文件/数据库写入发生在临时快照或pytest临时目录；账户、交易所和敏感操作均为模拟。快照目录为`C:\\Users\\zxc\\AppData\\Local\\Temp\\crypto_full_audit_20261001_01a0f2da\\source`。

完整命令`pytest -q -m "not live" --basetemp=隔离临时目录 --junitxml=隔离临时目录/pytest_results.xml`：**2593通过、2跳过、9失败**，342.42秒。随后对6个疑点定向复测：**5通过、1失败**，14.96秒。完整套件没有宣称全绿，也未把两次计数简单相加。

| 原9个失败分类 | 数量 | 核对后结论 |
|---|---:|---|
| 审计guard对子进程事件参数误判 | 4 | Windows subprocess executable可能为None；修正审计guard从args识别后，乱码修复dry-run及3个PS Unicode模型测试均通过；不是项目业务缺陷 |
| 未显式配置测试OPS_TOKEN | 1 | 无token时保护返回503，原测试期望401；给隔离环境提供合成dummy token后通过，不是未认证放行 |
| 未跟踪研究权重 | 2 | pump precursor、research loop直接依赖`data/research/pump_watchlist/model_weights.json`；隔离干净快照缺失 |
| 未跟踪ambush manifest | 1 | holder universe测试依赖`data/research/ambush_modes/manifest.json`，期望基础universe>50 |
| 未跟踪历史BTC 1h行情 | 1 | exit_logic验证脚本缺`data/historical/binance/BTC_USDT/1h.parquet`，独立复测仍失败；无法验证历史退出占比门槛 |

证据：{part('pytest_full.log')}、{part('pytest_results.xml')}、{part('pytest_focused.log')}、{part('pytest_focused_results.xml')}、{part('test_run_summary.json')}、{part('focused_test_run_summary.json')}、{part('test_isolation_guard.py')}。4个数据依赖属于QA-01的工程可重复性问题，未判断现有真实本地数据缺失或其研究结论错误。没有复制/覆盖原始研究数据来让测试绿灯。初次尝试coverage参数时因pytest_cov不存在而在执行前退出4，随后移除参数；不算上述9失败。pytest_cov/coverage/mypy/ruff不在项目环境，未安装，**本次没有行/分支覆盖率百分比、类型检查或lint全绿结论**。出现一项Starlette/httpx弃用警告；不把第三方兼容警告当项目回归。

## 主审计补充问题

### OPS-01 / P2：进程启停与去重没有工程/端口所有权边界

- 位置：{link('scripts/web.ps1',196)}、175-193、573-638；{link('scripts/supervise_web.ps1',66)}、174-183；{link('_once.ps1',48)}、48-70。
- 触发：同机同时运行主工程与`_radar_refactor`，或两端口Web/同module的worker。Get-ManagedWebProcesses按模块/命令特征匹配，不约束当前repo和目标port；Get-ObservedWorkerProcesses与supervisor worker去重同样只按module。
- 影响：对8000实例执行stop可能停止8001实例；supervisor可能把另一工程worker判为重复并强杀；启动可因发现另一工程worker而错误跳过。属于可造成停机/任务中断的运行维护缺陷，未声称数据损坏已发生。
- 证据：AST提取PowerShell函数并mock全部OS进程操作，请求port8000仍选中8000和8001；同worker module选中main与radar；重复清理停掉模拟radar PID12002。{part('ops_process_repro.ps1')}与{part('ops_process_repro.json')}，未枚举或终止真实进程。
- 建议：启动记录instance/project/port、PID和start_time，操作前核对完整解释器、命令和真实工程身份；worker同样采用实例注册，不以module名全局去重。多工作树/多端口用mock矩阵验证隔离。

### OPS-02 / P2：news_service沿用不存在的Web健康端点

- 位置：{link('Dockerfile',71)}-72；{link('docker-compose.yml',47)}-65；{link('core/news/service/worker.py',730)}-748、760-763。
- 触发：启用compose的news_service profile，其command仅运行`core.news.service.worker`，继承同镜像`curl localhost:8000/livez` HEALTHCHECK，service未覆盖。
- 影响：worker正常工作也会持续unhealthy，误导监控与部署门禁。Docker restart:unless-stopped并不会仅因unhealthy自动重启，本报告不声称必然重启循环。
- 证据：compose、Dockerfile及worker启动全文结构核对；worker只连接新闻DB并执行worker_loop，没有HTTP8000监听。未build/启动容器。
- 建议：为worker独立探测持久心跳/最后成功拉取/处理延迟，或在compose显式覆盖适用健康检查；分别验收web与worker容器状态。

### QA-01 / P2：所谓离线测试依赖本机研究产物与隐式认证环境

- 位置：{link('tests/test_pump_precursor.py',88)}、{link('tests/test_research_loop_v2.py',98)}、{link('tests/test_pump_watchlist_resilience.py',45)}、{link('tests/test_exit_logic_overhaul_verification.py',4)}、{link('scripts/verify_exit_logic_overhaul.py',190)}、{link('tests/test_sensitive_api_auth.py',565)}。research_loop相关权重加载堆栈见完整测试日志。
- 触发：干净克隆/CI、没有ignored研究权重/manifest/真实历史行情；或没有OPS_TOKEN。即使环境所有依赖齐全，4个数据案例仍不能独立执行，另1认证案例依赖未声明环境前提。
- 影响：离线验收无法从代码版本重现，失败混淆算法回归和缺数据；历史阈值测试无法提供稳定门禁。
- 证据：HEAD archive无真实env/ignored数据，完整9失败经隔离guard及dummy token定向排除后剩4个数据依赖；exit_logic单独复测仍失败。证据见验证结果表与日志。
- 建议：确定性合成小fixture、mock权重/manifest/行情loader、每个认证案例显式dummy token；必须依赖历史产物的测试标成清楚的integration并提供受控版本fixture/来源，不放宽生产认证或提交秘密数据。缺数据应明确缺前提，不能生成带PASS措辞的研究结论。

## 架构、覆盖与未验证边界

架构资金主链为策略/AI信号→执行引擎→风险/治理→OrderManager→交易所，账户/模式metadata在多个层级解析；持仓归因和单位语义是本次主要边界缺陷。数据主链为WS/REST→Hub→runtime provider→执行与估值，源时间与接收活性目前混用。研究链为DSL/因子/训练→回测指标→候选验证/晋级；缓存、标签未来区间与交易成本必须统一。新闻和预测市场链的多步持久化缺乏统一可靠交付/事务边界。大型`web/api/trading.py`9881行、`web/api/data.py`7358行、`execution_engine.py`6904行增加跨层语义漂移风险；模块尺寸本身未记为额外缺陷。

全量覆盖指所有已识别第一方文件完成登记、适用语法/模式/依赖结构检查，关键链人工深读并复现；**不等于300242行均逐行深审，也不等于所有函数动态验证**。独立覆盖清单把S（静态）、D（人工重点链）、V（隔离验证）区分；现有测试提供广泛行为证据，但不是实际覆盖率。文档/历史报告/model manifests被登记并核对当前入口和使用边界，未复算历史收益、重训全部模型或断言其统计有效。

以下范围保持待验证，不计入41项：真实交易所/Polymarket schema与订单部分成交/撤单；实盘重启恢复及跨账户完整对账；数据库migration/多worker审批事务及Parquet跨进程锁压力；真实源故障/限流/断连恢复；长期journal/DB/队列/缓存增长与延迟基准；历史研究原始数据、premium时效、模型外推与walk-forward全量复算；浏览器真实DOM/CSP与完整页面端到端；Docker build/容器运行/真实部署健康；外部依赖CVE和供应链来源审计。只有语法与依赖一致性检查，不能称依赖无已知漏洞。Playwright smoke依赖`@playwright/test`及运行Web，当前无项目node_modules且本次不启动服务，未运行；bundled Node用于语法和离线DOM mock。其余风险按模块附录的具体位置记录，避免把推测升级为确认缺陷。

## 逐项详细证据附录

下列直接并入独立模块报告的确认问题和待验证风险，完整触发/建议以对应条目为准。分报告原件也保留，方便单独审阅。

"""

def normalize_links(text: str) -> str:
    def replace(match: re.Match) -> str:
        target = match.group(2)
        if "://" in target or re.match(r"^[A-Za-z]:", target) or target.startswith("/"):
            return match.group(0)
        if (PARTS / target).exists():
            return f"[{match.group(1)}]({(PARTS / target).as_posix()})"
        return match.group(0)
    return re.sub(r"\[([^\]]+)\]\(([^)]+)\)", replace, text)

for filename, title, start, end in [
    ("trading.md", "交易、执行与风控", "## 确认问题", "## 与其他分报告合并边界"),
    ("web_security.md", "Web、安全、治理与审计", "## 已确认问题", "## 覆盖清单"),
    ("data_research.md", "数据、AI、模型、回测与预测市场", "## 已确认问题", "## 覆盖与验证记录"),
]:
    source = (PARTS / filename).read_text(encoding="utf-8")
    fragment = source[source.index(start):source.index(end)]
    report += f"\n## 附录：{title}\n\n原件：{part(filename)}。\n\n" + normalize_links(fragment) + "\n"

radar_part = (PARTS / "web_security.md").read_text(encoding="utf-8").split("## `_radar_refactor` 独有补丁差异审阅", 1)[1]
report += "\n## 附录：工作树独有补丁审阅\n\n" + normalize_links(radar_part) + "\n"
ignored_detail = PARTS / "trading_ignored_scripts.md"
if ignored_detail.exists():
    report += "\n## 附录：ignored运行辅助脚本\n\n" + normalize_links(ignored_detail.read_text(encoding="utf-8")) + "\n"

cov = f"""# 全量审计覆盖清单 · 2026-10-01

基线：主工程HEAD `{HEAD}`。总报告：{link('reports/full_audit_2026-10-01.md')}。

## 覆盖口径

- **S**：登记、文本/AST/语法/模式或配置依赖结构审查，按文件类型适用；不是逐函数正确性证明。
- **D**：模块关键链人工深读，准确范围见模块报告；目录标D不表示目录每一行深读。
- **V**：隔离现有测试或AST/Mock反例，精确案例和覆盖对象见JUnit/复现JSON；不是实盘验证。
- **I**：文档、历史报告、模型/数据产物元信息登记、当前引用/接口契约核对；未复算历史结论。
- **X**：外部依赖或禁止访问的秘密/账户/真实状态；范围理由列明。没有将X伪装成通过。

## 工作区项目边界

| 边界 | 结果 | 去重/验证 |
|---|---|---|
| 主工程 | 1239个Git跟踪文件；874 Python/300242行 | S全部；D重点链；V非LIVE套件与原函数mock |
| `_radar_refactor` | 关联工作树，非独立业务副本；984跟踪/774 Python | S全量AST，D独有7文件补丁；不重复旧套件 |
| 父Git工程 | 3个跟踪条目 | 主工程gitlink、忽略规则、交接文档；无其他主业务 |
| ignored源码 | 12个辅助文件 | S/D副作用审阅，不执行；明细在本文件末尾 |
| `.conda` | 第三方runtime | 环境和pip一致性验证；X包源码逐行/CVE |
| ignored真实数据/日志与密钥 | 不纳入业务验证输入 | X不读取秘密、不覆盖原始数据；消费代码S/D |

## 审计领域检查表

| 领域 | 完成范围 | 层级 | 对应证据/限制 |
|---|---|---|---|
| 架构/模块 | main、config、strategy→risk→execution→order→connector、data/AI/research/web边界 | S/D | 总报告资金/数据/研究链说明 |
| 交易执行/风控 | 账户模式、手动/策略订单、止损止盈、分单、聚合持仓、合约单位、PNL | S/D/V | {part('trading.md')}；88 Python/36227行；真实venue未验证 |
| 持久化/恢复/并发 | position/order state、新闻游标、纸盘事务、registry/audit文件、worker进程 | S/D/V | mock故障/竞态；真实migration/长期压力X |
| 数据时效/完整性 | Hub、runtime价格、历史存储/premium/news/Polymarket | S/D/V | {part('data_research.md')}；193 Python/75354行 |
| 回测/模型验证 | 标签split、DSL因果性、因子缓存、fee统计、年化、晋级与CUSUM | S/D/V | 没有复算真实历史收益/重训全模型 |
| API/安全 | 身份、RBAC/治理、敏感路由、路径、HTML、WS、审计 | S/D/V | {part('web_security.md')}；75源文件109860行/343静态路由 |
| 日志/可观测性 | event bus、decision trace、audit logger/counterfactual、news/worker健康 | S/D/V | 审计签名、队列泄漏与丢更新复现；真实日志内容X |
| 部署/运维 | Docker/compose、PowerShell/bat、supervisor、启动/清理/环境脚本 | S/D/V | PS mock进程所有权；容器/服务未运行 |
| 依赖/配置 | requirements/environment、AGENTS、settings/database/env.example、YAML/JSON | S/V | verify_env/pip check/config contract通过；CVE未查询 |
| 测试 | 全部309跟踪测试文件登记，收集运行not live | S/V | 2593pass/2skip/9fail；6疑点复测5pass/1fail；缺数据4未补 |
| 文档/模型/历史报告 | README/SECURITY/STARTUP/governance/handoff/目录文档与metadata | S/I | 不把历史报告视为本次测试结果 |
| 性能 | 大模块、同步文件读取、队列/cache增长、fanout/批量上限 | S/D/V局部 | 没有吞吐、延迟、长周期内存或多进程压力基准 |

所有跟踪文件在下表登记；分报告与{part('inventory.json')}提供对应机器索引。文件数为去重路径数，子代理LOC可能重叠，不能相加声称总行数。没有报告行覆盖率百分比。

## 目录计数及处理

| 顶层目录/文件 | 跟踪数 | 已完成处理 |
|---|---:|---|
"""
for top, count in INVENTORY["summary"]["by_top_directory"].items():
    treatment = "S；D重点链；适用V见领域表"
    if top in {"docs", "reports", "models", "data", "LICENSE", ".openclaw"}:
        treatment = "S/I；元信息/引用/当前接口核对，历史结论不复算"
    elif top == "tests":
        treatment = "S/V；not live隔离suite；浏览器/live范围未运行"
    elif top == "scripts":
        treatment = "S全量；D启动/清理/监督/环境/验证副作用，V安全mock"
    cov += f"| `{top}` | {count} | {treatment} |\n"

cov += "\n## 主工程逐文件登记\n\n语法栏仅对相应格式验证；非源码产物显示I，避免将解析成功误标业务通过。\n\n| 文件 | 类型 | 字节/行 | 完成登记/静态检查 |\n|---|---|---:|---|\n"
for rec in INVENTORY["files"]:
    path = rec["path"]
    ext = rec["suffix"] or "无后缀"
    size = str(rec["bytes"]) + (f" / {rec['lines']}" if "lines" in rec else "")
    status = "S：登记/类型与引用范围"
    if path.endswith(".py"):
        status = "S：AST通过、结构/敏感模式扫描；D/V依模块证据"
    elif path.endswith(".ps1"):
        status = "S：PS AST通过；D/V依脚本副作用范围"
    elif path.endswith((".js", ".cjs")):
        status = "S：node语法通过；D/V依前端边界范围"
    elif path.endswith((".yaml", ".yml")):
        status = "S：YAML解析通过；配置结构审阅"
    elif path.endswith(".json"):
        status = "S/I：JSON语法通过；业务/历史schema未全量验证"
    elif path.split("/")[0] in {"docs", "reports", "models", "data"}:
        status = "I：登记/引用及元信息；历史结果未复算"
    cov += f"| {link(path)} | {ext} | {size} | {status} |\n"

cov += "\n## ignored第一方辅助源码\n\n以下12个文件不在git archive测试快照，已只读检查副作用和用途，未执行。logs/tmp的旧pytest包装、模板重写与浏览器读取辅助属于历史维护产物；data下运行/切换/监控辅助的审阅见附录。未把它们悄然排除为第三方。\n\n| 文件 | 行数 | 证据/用途 |\n|---|---:|---|\n"
for rec in ignored_records:
    purpose = "历史pytest包装；会在原树运行/写日志，不执行"
    if rec["path"].startswith("data/"):
        purpose = "运行/策略切换/监督辅助；只读副作用审阅"
    elif rec["path"].endswith("redesign_agent.py"):
        purpose = "历史模板重写器；可写真实template，不执行"
    elif rec["path"].endswith(".cjs"):
        purpose = "历史浏览器读取/截图辅助；访问真实本机服务，不执行"
    cov += f"| {link(rec['path'])} | {rec['lines']} | {purpose} |\n"
cov += f"\n文件SHA/行数记录：{part('ignored_source_inventory.json')}。模块细化：{part('data_research_coverage.md')}、{part('web_security_coverage.json')}、{part('trading.md')}；全部机器主清单：{part('inventory.json')}。本次新增审计生成器和复现脚本属于交付证据，未混入产品源码统计。\n"

(ROOT / "reports/full_audit_2026-10-01.md").write_text(report, encoding="utf-8")
(ROOT / "reports/full_audit_2026-10-01_coverage.md").write_text(cov, encoding="utf-8")
summary = {"head": HEAD, "confirmed_findings": len(FINDINGS), "severity": {s: sum(f[1] == s for f in FINDINGS) for s in ["P0", "P1", "P2", "P3"]}, "tracked_files": len(INVENTORY["files"]), "ignored_first_party_source_helpers": len(ignored_records), "json_syntax_files": len(json_paths), "json_errors": json_errors, "generated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(), "business_code_modified": False, "live_operations_performed": False}
(PARTS / "audit_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(summary, ensure_ascii=False, indent=2))
