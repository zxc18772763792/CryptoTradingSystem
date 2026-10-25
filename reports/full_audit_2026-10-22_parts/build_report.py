"""Build the local audit report from verified observations, never business state."""
from collections import Counter
from datetime import datetime,timezone
import json
from pathlib import Path
import shutil
import xml.etree.ElementTree as ET

OUT=Path(__file__).resolve().parent
ROOT=OUT.parents[1]
def read(name):return json.loads((OUT/name).read_text(encoding='utf-8-sig'))
def save(name,obj):(OUT/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2),encoding='utf-8')
def link(path,line=None,label=None):
    suffix=f':{line}' if line else ''
    return f'[{label or path+suffix}]({ROOT.as_posix()}/{path}{suffix})'
def evidence(name):return link('reports/full_audit_2026-10-22_parts/'+name,label=name)

findings=[
{'id':'SEC-01','severity':'P1','title':'只读账户接口返回受支持的凭据 metadata，泄露角色边界',
 'locations':[('web/api/trading_accounts.py',14),('web/api/trading_accounts.py',69),('core/trading/account_manager.py',246),('core/trading/account_manager.py',279)],
 'trigger':'账户使用系统支持的 metadata.credentials、exchange_credentials、exchanges 或旧 api_key/api_secret/passphrase 字段保存交易凭据；调用者只有 read_trading_state。',
 'cause':'list_accounts() 将账户 dataclass 原样 asdict，账户列表和汇总接口原样返回，没有凭据输出白名单或递归脱敏。',
 'proof':'AUDITOR 读取合成账户列表得到 HTTP 200，响应中同时包含合成 api_key 和 api_secret。该角色没有 manage_accounts。汇总接口嵌入同一个原始账户对象，属于同根问题。',
 'impact':'只读 API 用户可获得账户中保存的交易所凭据。没有读取或证明任何真实账户曾泄露；配置仅使用环境变量而未将凭据放入 metadata 时不触发该例。',
 'fix':'建立账户专用输出 schema，凭据只写不读；列表、汇总、创建、更新响应统一白名单，覆盖嵌套和旧字段。用合成秘密做所有响应的否定断言。',
 'cases':['test_readonly_account_api_returns_synthetic_credential_fields']},
{'id':'SEC-02','severity':'P1','title':'省略 mode 即可绕过已有 live 账户的审批保护',
 'locations':[('web/api/trading_accounts.py',39),('core/trading/account_manager.py',411),('core/governance/rbac.py',64)],
 'trigger':'已有 mode=live 的账户；OPERATOR 可以 manage_accounts，却没有 approve_live；更新请求省略 mode。',
 'cause':'审批只检查本次 payload.mode 是否等于 live，没有检查账户当前及修改后的有效模式。',
 'proof':'合成 live 账户最初 disabled、sandbox=true。显式 PUT mode=live 被 403 拒绝；省略 mode、设置 enabled=true 并更换 credentials.sandbox=false 返回 200，账户仍为 live。',
 'impact':'可重新启用 live 账户或修改其连接目标/凭据，绕过原本要求的 live 审批。并不意味着该请求会自动将全局 paper 模式切到 live；风险在后续使用该 live 账户的流程。',
 'fix':'对已有或最终有效模式为 live 的敏感修改要求 approve_live，尤其 enabled、交易所、账户关系和 credentials；审批应基于合并后的状态。',
 'cases':['test_operator_can_enable_and_retarget_existing_live_account_without_approval']},
{'id':'SEC-03','severity':'P1','title':'AI 配置接口允许普通操作员打开 allow_live',
 'locations':[('web/api/ai_agent.py',29),('web/api/ai_research.py',3221),('core/ai/autonomous_agent.py',1360)],
 'trigger':'OPERATOR 持有 manage_ai_agent，但不持有 approve_live；向运行配置接口提交 allow_live=true。',
 'cause':'路由只要求 manage_ai_agent，服务层直接调用 update_runtime_config，后者更新并保存允许实盘的 overlay。',
 'proof':'合成身份通过真实 RBAC 检查，POST /api/ai/runtime-config/autonomous-agent 得到 HTTP 200 和 config.allow_live=true。保存函数已替换为测试桩，未启动 AI 或调用模型。',
 'impact':'突破 AI 的实盘许可开关。仍需其他引擎/账户条件满足才可能执行实盘，不能据此声称已经发生真实下单。',
 'fix':'allow_live 从 false 到 true、以及已有 live-capable 配置下扩大交易能力的变更，额外要求 approve_live；普通 paper 配置维护保持原权限。',
 'cases':['test_operator_can_enable_ai_live_gate_without_live_permission']},
{'id':'SEC-04','severity':'P1','title':'确认告警权限可用于重置日内亏损熔断',
 'locations':[('web/api/trading_runtime.py',138),('web/api/auth.py',165),('core/risk/risk_manager.py',817)],
 'trigger':'OPERATOR 仅依靠 ack_alerts，调用 POST /api/trading/risk/reset。',
 'cause':'require_sensitive_ops_permissions(ack_alerts, approve_risk_change) 的多个权限是 OR。该接口执行 reset_halt，远超过确认告警。',
 'proof':'不具备 approve_risk_change 的合成 OPERATOR 得到 HTTP 200；reset_halt 测试桩被调用一次。真实函数会清空熔断、重设日内权益基线/已实现损益，并设置 live 120 秒的 daily-stop guard。',
 'impact':'操作员可绕过风险所有者解除亏损停机，且重设风险计量基线。其他 /api/risk 和 /ops 风险复位接口的审批保护不能覆盖这个旁路。',
 'fix':'该接口要求 approve_risk_change；ack_alerts 只用于告警确认。把所有复位入口放入同一个角色矩阵测试。',
 'cases':['test_ack_alert_role_can_reset_daily_halt']},
{'id':'SEC-05','severity':'P1','title':'AI 管理权限可修改共享风险阈值',
 'locations':[('web/api/ai_agent.py',45),('web/api/ai_research.py',3236),('web/api/ai_research.py',240)],
 'trigger':'OPERATOR 使用 AI risk-config 接口修改自治交易风险纪律参数。',
 'cause':'路由只要求 manage_ai_agent，处理函数直接更新共享 risk_manager；没有 approve_risk_change。',
 'proof':'POST /api/ai/autonomous-agent/risk-config 携带 autonomy_max_drawdown_reduce_only=1.0 返回 200；共享 risk_manager.update_parameters 收到该阈值。合成角色没有 approve_risk_change。',
 'impact':'可通过 AI 侧接口提高风险限制，而标准风险配置接口要求的风险审批被绕开。',
 'fix':'共享风险参数修改统一要求 approve_risk_change，并在服务边界集中校验，避免不同 UI 路由权限漂移。',
 'cases':['test_operator_ai_route_can_change_shared_risk_thresholds']},
{'id':'TR-01','severity':'P2','title':'非有限数可穿过订单 schema 和风险比较，并污染模拟订单',
 'locations':[('web/api/trading.py',1287),('core/risk/risk_manager.py',939),('core/trading/order_manager.py',549)],
 'trigger':'输入金额字符串 "NaN"，或上游账户权益变为 float NaN；风险检查依赖 <=、> 等浮点比较。',
 'cause':'订单输入为普通 float；风控只判断正负，未先 math.isfinite。NaN 的比较为 false，导致额度分支被跳过。模拟订单也缺少最终的有限数验证。',
 'proof':'正常权益 1000 的 1e12 超额订单被拒绝，权益 NaN 的同订单却通过 pre_trade_check；OrderRequest 接受合法 JSON 可表示的字符串 NaN；内部模拟限价单可登记 CLOSED、filled=NaN。',
 'impact':'异常数据下风控 fail-open；模拟账本、统计及 JSON 响应可被非有限值污染。没有证明真实交易所会接受 NaN，也没有发送真实订单。',
 'fix':'API 使用有限正数约束，风控及订单提交边界再次验证 equity/notional/amount/price/leverage 的有限性与合法范围；退出单仍需合法数量，只豁免入场额度。',
 'cases':['test_nonfinite_order_schema_and_risk_check_accept_nan','test_paper_order_records_nonfinite_fill_quantity']},
{'id':'TR-02','severity':'P2','title':'行情跳变超过 15% 时，模拟市价单反而按调用方旧价成交',
 'locations':[('core/trading/order_manager.py',499),('core/trading/order_manager.py',33)],
 'trigger':'有效新行情相对请求携带价格偏离超过 15%，例如快速上涨的币种。',
 'cause':'代码把价差大视为新行情不可信，并保留 requested_price，随后仍登记为已成交；paper_fill_source 标记不能阻止其进入收益统计。',
 'proof':'新报价 200、bid/ask=199/201、age_ms=0，请求市价买入携带旧价 100；去除费用/滑点干扰后订单 CLOSED、成交价 100，来源 request_live_implausible。',
 'impact':'通用模拟订单会得到市场上无法成交的价格，制造或扭曲收益。Alastar 冻结 v2/v3 使用自己的公共 K 线账本，本反例没有证明其直接调用该订单管理器。',
 'fix':'报价冲突时拒绝/等待验证或使用已确认的新市场价；不得把未经验证的旧价回退当作正常市场成交计入可用业绩。保留不确定成交的单独状态。',
 'cases':['test_paper_market_fill_reverts_valid_live_quote_to_request']},
{'id':'OPS-01','severity':'P2','title':'Docker 忽略规则遗漏 .env.production 等秘密文件变体',
 'locations':[('.dockerignore',59),('Dockerfile',61),('.gitignore',51)],
 'trigger':'操作员在项目目录保留 .env.production、.env.backup 或 .env.test，然后以当前目录执行 Docker 构建。',
 'cause':'.dockerignore 只忽略 .env、.env.local、.env.*.local；Dockerfile 对整个上下文执行 COPY . /app。Git 忽略规则不代替 Docker 上下文过滤。',
 'proof':'根文件名规则分析和独立反例均确认上述三个名字被 Git 忽略、却没有匹配 Docker 排除规则；.env/.env.local 正常排除作为控制组。未创建或读取真实秘密变体，未构建镜像。',
 'impact':'若这些文件包含凭据，凭据会进入构建上下文和最终镜像。当前未证实任何实际镜像已泄露。',
 'fix':'.dockerignore 覆盖 .env 和全部 .env.*，明确仅放行不含秘密的 .env.example；用构建上下文合成文件测试覆盖常用变体。',
 'cases':['test_docker_context_includes_git_ignored_env_variants']},
{'id':'DEP-01','severity':'P2','title':'当前环境的 HTTP/证书等依赖命中已公开安全公告',
 'locations':[('requirements.txt',17),('requirements.txt',87)],
 'trigger':'本机解释器安装版本与公开 OSV 受影响版本范围匹配；具体利用仍取决于调用路径和攻击者控制的输入/网络。',
 'cause':'运行时版本尚未包含近期安全修复；pip check 只检查依赖相容性，不能替代漏洞数据库核查。',
 'proof':'129 个已安装包的 OSV querybatch 全部返回、无分页遗漏：8 个包命中 79 条公告记录（含 GHSA/PYSEC 别名，不能当成 79 个独立漏洞）。公告原文及抓取失败均保留。',
 'impact':'certifi 2026.6.17 命中根证书信任事件；aiohttp 3.14.3 命中连接池耗尽等公告；urllib3 2.7.0 命中代理 TLS/分块读取等公告。其余 PyJWT、multidict、pytest、tornado、Pillow 需按实际使用路径分流。未对应用执行漏洞利用，不能声称现有认证已被绕过。',
 'fix':'先在独立环境验证 certifi、HTTP 栈的修复版本及相容性，再更新受影响环境/镜像；原生依赖遵守 conda 管理。Pillow/PyJWT 等按项目可达性安排优先级。pytest 修复涉及当前 <9 声明，不能直接强行升级生产环境。',
 'cases':[]},
{'id':'ENV-01','severity':'P3','title':'环境核验通过，但未满足 requirements.txt 的完整契约',
 'locations':[('requirements.txt',35),('requirements.txt',47),('requirements.txt',72),('scripts/verify_env.py',63)],
 'trigger':'使用当前项目解释器执行启动依赖核验和 pip check，两者都显示成功。',
 'cause':'启动核验只检查列出的模块能否导入；pip check 只验证已安装包元数据，不验证 requirements.txt 每一项。',
 'proof':'requirements 要求 web3<8，实际 8.0.0；schedule、pytest-cov、mypy、types-redis 四项没有安装。没有将这些误报成必需启动模块缺失，verify_env --strict 实际通过。',
 'impact':'开发检查缺失、环境不可完全按声明复现，DEX 主版本偏离支持范围；本轮没有证明 web3 8 已造成具体交易故障。',
 'fix':'增加独立的 requirements 版本/缺项契约检查，区分启动、可选功能和开发工具；在独立 conda 环境校准声明与实际版本。本轮没有安装或卸载依赖。',
 'cases':[]}
]

def build():
    inv=read('inventory.json');counts=read('coverage_counts.json');osv=read('osv_querybatch.json');details=read('osv_advisories.json');runtime=read('runtime_readonly.json')
    snapshot=read('snapshot.json');source=Path(snapshot['source'])
    shutil.copy2(source/'audit_observations.jsonl',OUT/'observations_all_runs.jsonl')
    all_observations=[json.loads(x) for x in (OUT/'observations_all_runs.jsonl').read_text(encoding='utf-8').splitlines()]
    (OUT/'observations_latest.jsonl').write_text('\n'.join(json.dumps(x,ensure_ascii=False) for x in all_observations[-9:])+'\n',encoding='utf-8')
    severity=Counter(x['severity'] for x in findings)
    for f in findings:
        f['locations']=[{'file':p,'line':line} for p,line in f['locations']]
        f['verification']='synthetic_counterexample' if f['cases'] else 'installed_metadata_and_public_advisories' if f['id']=='DEP-01' else 'installed_metadata_and_requirement_specifiers'
        f['status']='open'
    save('findings.json',findings)
    summary={'generated_at_utc':datetime.now(timezone.utc).isoformat(),'head':inv['head'],'scope':'current working tree including existing modifications and nonignored untracked files',
        'confirmed_findings':len(findings),'severity':dict(severity),'inventory_files':inv['counts']['files'],'python_files':inv['counts']['python_files'],'python_lines':inv['counts']['python_lines'],
        'route_decorators':len(read('routes_static.json')),'pytest':{'passed':3311,'skipped':2,'failures':0},'new_current_bad_behavior_counterexamples':9,
        'javascript_node_tests_passed':6,'browser_fixture_smoke_passed':True,'osv_package_queries':129,'osv_affected_packages':8,'osv_advisory_records':79,
        'osv_detail_fetch_success':sum(not x.get('fetch_error') for x in details),'osv_detail_fetch_failures':sum(bool(x.get('fetch_error')) for x in details),
        'business_code_modified':False,'live_orders_submitted':False,'services_restarted':False,'environment_modified':False,'code_published':False}
    save('audit_summary.json',summary)
    heading='''# 项目全量审计 · 2026-10-22

本轮确认 **10 项待修问题：5 项 P1、4 项 P2、1 项 P3**。最优先处理账户凭据输出和跨接口的实盘/风控审批旁路。既有测试全绿并没有覆盖这些边界；新增 9 个合成反例已稳定证明当前行为。

这是当前工作树的全项目范围审计：全量文件盘点、可解析代码检查、关键链路审阅、隔离测试、依赖公告和当前运行状态只读核验。全量范围不表示 33 万行都逐行深读或所有状态组合都经过形式证明。

本轮只新增审计报告与辅助证据；未修改业务源码、冻结策略、协议、账户配置、运行模式、环境依赖，未提交/推送代码、重启服务或发送真实订单。既有用户改动保留。严重性按项目权限/资金和证据影响评定；上游公告自身的 CRITICAL 等级不直接等于本项目 P0。

## 快速索引

'''
    report=heading+f"- 覆盖范围与未验证边界：{link('reports/full_audit_2026-10-22_coverage.md')}。\n- 机器可读问题清单：{evidence('findings.json')}。\n- 完整基线：HEAD `{inv['head']}` 加当前未提交改动，文件 SHA 见 {evidence('inventory.json')}。\n- 本轮结论汇总：{evidence('audit_summary.json')}。\n\n"
    report+='| 编号 | 级别 | 问题 | 主要位置 |\n|---|---|---|---|\n'
    for f in findings:
        loc=f['locations'][0];report+=f"| {f['id']} | {f['severity']} | {f['title']} | {link(loc['file'],loc['line'])} |\n"
    report+='\n## 确认问题与修复验收\n\n'
    for f in findings:
        report+=f"### {f['id']} · {f['severity']} · {f['title']}\n\n"
        report+='位置：'+'；'.join(link(x['file'],x['line']) for x in f['locations'])+'。\n\n'
        for label,key in [('触发条件','trigger'),('原因','cause'),('复现证据','proof'),('影响与界限','impact'),('建议修复','fix')]:report+=f"**{label}：** {f[key]}\n\n"
        if f['cases']:report+='独立反例：'+', '.join('`'+x+'`' for x in f['cases'])+'，见 '+evidence('test_current_findings.py')+' 与 '+evidence('pytest_222424.log')+'。这些测试断言的是当前错误行为，修复后应改成拒绝/脱敏等安全预期。\n\n'
        elif f['id']=='DEP-01':report+='版本及原始返回：'+evidence('installed_packages.json')+'、'+evidence('osv_querybatch.json')+'、'+evidence('osv_triage.json')+'。\n\n'
        else:report+='逐项契约结果：'+evidence('requirements_contract.json')+'。\n\n'
    report+='''## 依赖公告的可达性与修复顺序

数据库在本次查询时命中的 8 个包如下。公告编号有别名重叠，79 条记录不作为应用漏洞数量计入；DEP-01 只算一项依赖治理问题。仅扫描当前解释器的 Python 分发包，没有扫描操作系统、浏览器、Node 依赖或实际容器镜像。公告详情首轮有 TLS EOF，单轮有界重试后的剩余失败见下方证据。

| 包与实际版本 | 记录数 | 当前审计能支持的判断 |
|---|---:|---|
| certifi 2026.6.17 | 2 | HTTP 客户端证书信任链优先复核，需满足攻击者在路径上的条件；并未证明本次流量遭截获 |
| urllib3 2.7.0 | 6 | HTTP/代理/流式响应边界需修复，是否触发取决于具体传输与代理配置 |
| aiohttp 3.14.3 | 8 | 项目采集器和 ccxt HTTP 栈广泛使用；客户端公告需优先验证，服务器专属公告不能直接套到 FastAPI |
| PyJWT 2.13.0 | 28 | 本轮未发现项目核心认证直接使用 jwt.decode，不据此声称 OPS/RBAC 可被伪造 |
| multidict 6.7.1 | 2 | HTTP 栈依赖，受影响的集合操作可达性仍需确认 |
| pytest 8.4.2 | 2 | 开发/测试环境风险；已使用独立临时目录，不等于应用公开接口漏洞 |
| tornado 6.5.8 | 3 | 当前 Web 服务为 FastAPI，本轮未证明对外暴露 Tornado 服务 |
| Pillow 12.3.0 | 28 | 图像研究/渲染环境；恶意图像处理的具体路径与本地原生依赖升级兼容性需另测 |

'''
    report+='已核对的维护者原始公告：certifi 的受影响信任根及 2026.10.19 修复版本见 [certifi 安全公告](https://github.com/certifi/python-certifi/security/advisories/GHSA-6q48-6c7w-9gw2)；aiohttp 连接池耗尽及 3.14.4 修复见 [aiohttp 安全公告](https://github.com/aio-libs/aiohttp/security/advisories/GHSA-6cjc-53gc-5rqm)；urllib3 分块响应风险及 2.8.0 修复见 [urllib3 安全公告](https://github.com/urllib3/urllib3/security/advisories/GHSA-vxq7-64xx-v4gw)。这些是相应公告的修复版本，不是已经验证好的整套项目升级方案。\n\n'
    report+=f"本轮共成功取得 {summary['osv_detail_fetch_success']}/79 条详情，{summary['osv_detail_fetch_failures']} 条仍为传输失败；原始 ID、范围、别名和错误保留于 {evidence('osv_advisories.json')}、{evidence('osv_detail_retry.json')}。querybatch 的 129 条版本查询本身完整且没有 next_page_token，接口语义见 [OSV 官方文档](https://google.github.io/osv.dev/post-v1-querybatch/)。\n\n"
    report+='Docker 上下文判断依据是当前根文件名模式和 `COPY . /app`，没有把 Git 忽略误当成 Docker 忽略；上下文及 `.dockerignore` 的语义见 [Docker 官方文档](https://docs.docker.com/build/concepts/context/#dockerignore-files)。\n\n'
    report+='''## 测试、旧整改与反例的关系

| 检查 | 实际结果 | 证据 |
|---|---|---|
'''
    for title,value,file in [('隔离全量 pytest，排除 live','3311 通过、2 跳过、0 失败；约 553 秒','pytest_215344.log'),('新增安全/交易/部署反例','9 通过；证明 8 项当前问题的坏行为；两项环境问题另有元数据证据','pytest_222424.log'),('旧 10/01 整改对应回归','65 个用例，0 失败；不表示旧 41 项所有路径都已重新穷举','coverage_counts.json'),('Alastar 相关测试','206 个用例，0 失败','coverage_counts.json'),('前端 Node 单元测试','6 个通过','node_tests.json'),('浏览器 fixture 冒烟','通过：键盘、转义、草稿、模拟按钮、错误状态及 4 种宽度；全部请求拦截','browser_fixture_smoke.json'),('启动依赖核验 --strict','通过；仅证明其列出的模块可导入','environment_check.json'),('pip check','通过；不代表满足 requirements 全部声明或没有漏洞','pip_check.json'),('配置契约','通过：默认 loopback、paper 启动保护等','config_contract.json'),('Python/JSON/YAML','1021/202/10 个文件解析无语法错误','inventory.json'),('JavaScript / PowerShell','24 / 26 个文件语法检查通过','javascript_syntax.json')]:report+=f'| {title} | {value} | {evidence(file)} |\n'
    report+='\nPowerShell 的独立详细证据：'+evidence('powershell_syntax.json')+'。全量 pytest 的唯一警告为 Starlette TestClient 对 httpx 路径的弃用提示，未安装新包来消除它。\n\n'
    report+='两个跳过分别是：`test_ake_btw_and_17_plus_2_baseline_event_regression` 缺少本地 frozen baseline；`test_buy_take_profit_above_current_price` 的随机种子未触发 BUY crossing。后者意味着该具体分支本次没有得到验证，应在后续维护时改为确定性数据。见 '+evidence('full_suite_summary.json')+'。\n\n'
    report+='10/01 的报告与整改台账作为历史背景读取；当前保留了策略 live 审批、订单 ID 账户作用域、合约单位、反向平仓、Polymarket 原子成交等既有回归。本轮 SEC-04 是另一个复位入口，TR-01/02 是旧防护未覆盖的异常输入/报价分歧，不能因为旧整改台账曾标记关闭就忽略。\n\n'
    report+='''## 当前运行与 Alastar 只读快照

'''
    rc=runtime['alastar_collection'];books=runtime['alastar_books']
    report+=f"核验时间 `{runtime['checked_at_utc']}`（北京时间约 22:26）。`/livez` 与 `/readyz` 均 200，`/api/status` 为 paper，16 个受管任务。只请求只读健康/状态接口，没有调用账户、余额或下单 API。详细证据：{evidence('runtime_readonly.json')}。\n\n"
    report+=f"Alastar v2/v3 均已激活、心跳在 30 秒内、last_pass_error 为空；冻结源码和协议 SHA 与激活记录一致，账本当前都只有 1 条 activation、没有 freeze/entry/settlement。两者仍为 0 个前瞻样本。相关 206 项测试和冻结校验不等于策略有效性证明。\n\n"
    report+=f"采集档案 {rc['run_count']} 轮，哈希链有效；最近 6 小时 {rc['last_6h_live_runs']} 轮公开预览，轮询间隔中位约 {rc['last_6h_median_poll_gap_seconds']:.1f} 秒、最大 {rc['last_6h_max_poll_gap_seconds']:.1f} 秒，未观察到超过 600 秒的间隔；全历史最大间隔约 {rc['all_time_max_poll_gap_seconds']/3600:.2f} 小时。真实首次收件样本仍为 {rc['new_live_first_receipt_count']}，因此无法估计发帖到收到的真实延迟分布，更不能保证未来分钟级交付。\n\n"
    report+='语义复核仍需人工/当前会话处理；没有启用自动 AI 标签。多周期比较继续只看共同成熟样本，不挑选历史表现最好的周期作为已确认策略。做空 1 倍近似损失路径没有替代标记价、维持保证金、杠杆及账户全过程验证。保持用户已确认的原入场规则，本轮没有加入“先涨 10% 再入场”。\n\n'
    report+='''## 修复顺序与关闭条件

1. 先修 SEC-01～05：输出 schema 去凭据；按有效状态审核 live 变更；按动作审核 AI/risk；所有同功能入口共用角色矩阵。使用 AUDITOR、ENGINEER、OPERATOR、RISK_OWNER、SYSTEM 的允许/拒绝对照，不能只测未登录 401。
2. 修 TR-01/02：有限数校验和可信成交状态。异常行情、NaN/inf、过时/分歧报价必须不会成为正常已成交业绩；合法减仓不应被入场额度误挡。
3. 修 OPS-01，并在独立环境验证 DEP-01 的 HTTP/证书栈更新和镜像重建；不得绕过本仓库 conda 原生依赖约束直接批量 pip 升级。
4. 收敛 ENV-01，补确定性测试数据及开发环境契约。修复时把本轮“坏行为成立”的反例改写为安全预期，再跑受影响回归；不是单纯让现有 9 个测试继续绿。

关闭条件是相应行为反例不再成立、合法路径保持可用、全部旁路纳入覆盖，并更新本地整改台账。当前 10 项均为 open，本轮未实施修复。

## 复现与审计工具边界

所有业务测试在当前工作树的临时副本运行，去除真实 env/运行数据与继承凭据；仅使用合成身份、账户和行情。测试护栏阻止主项目和 `_radar_refactor` 的读写、外部连接及现有本机服务连接，只允许测试自行绑定的 loopback 端口。Python 子进程继承护栏；少数 PowerShell 合成测试经过源码检查。它是审计防误操作措施，不是防恶意原生代码的操作系统级沙箱。

'''
    report+=f"准备/运行工具：{evidence('audit_workspace.py')}、{evidence('test_guard.py')}、{evidence('run_tests.py')}；隔离目录记录在 {evidence('snapshot.json')}。fixture 浏览器测试通过 page.route 拦截全部 HTTP 请求，只提供 agent.test 合成页面。只读当前运行检查是另一次明确的 GET 核验，不属于离线测试。\n\n"
    report+='```powershell\n'
    report+="$auditPython = 'F:\\9_Crypto\\.conda\\miniforge3\\envs\\crypto_trading\\python.exe'\n"
    report+="Set-Location 'F:\\9_Crypto\\crypto_trading_system'\n"
    report+="& $auditPython reports/full_audit_2026-10-22_parts/run_tests.py --guard-probe\n"
    report+="& $auditPython reports/full_audit_2026-10-22_parts/run_tests.py\n"
    report+="& $auditPython reports/full_audit_2026-10-22_parts/run_tests.py tests/audit20261022_repro.py\n"
    report+="& $auditPython reports/full_audit_2026-10-22_parts/additional_checks.py\n"
    report+="& powershell.exe -NoProfile -ExecutionPolicy Bypass -File reports/full_audit_2026-10-22_parts/parse_powershell.ps1\n"
    report+="& $auditPython reports/full_audit_2026-10-22_parts/browser_check.py\n"
    report+='```\n\n'
    report+='上述命令依赖本次 snapshot.json 中的临时副本仍存在；若清理临时目录，需先运行 audit_workspace.py 创建新副本，再将 test_current_findings.py 复制为新副本的 tests/audit20261022_repro.py。准备脚本会生成新的清单/快照，归档本次证据后再重跑，避免覆盖本次基线。运行命令本身只适用于该隔离流程，不能把反例测试直接对准正在运行的服务。\n\n'
    report+='工具过程中出现的 Path 拼接、Windows NUL 访问、UTF-8 BOM、Git stdin CRLF 和采样时间跨心跳等问题已在审计辅助代码中修正，属于审计工具问题，没有计入项目缺陷。首次失败日志保留。1 个源码秘密扫描候选是合成测试值，已排除；指定秘密文件名的 Git 历史路径查询为空，但没有声称完成所有历史内容的秘密证明。\n\n'
    report+='源码保护结果见 '+evidence('working_tree_after.json')+'：1221 个基线非 reports 文件的 SHA 全部保持一致。密钥、私有运行日志/账户数据未写入审计产物。\n'
    (ROOT/'reports/full_audit_2026-10-22.md').write_text(report,encoding='utf-8')

    deep={
        'web/api/trading_accounts.py':'账户输出与 live 更新审批', 'core/trading/account_manager.py':'list/update/credentials 边界',
        'web/api/auth.py':'本地会话与 OR 权限语义','core/governance/rbac.py':'角色能力表',
        'web/api/trading_runtime.py':'风险复位与配置接口','web/api/ai_agent.py':'AI 配置/风险路由',
        'web/api/ai_research.py':'AI 配置/共享风险更新、研究跟踪入口（选择性段落）',
        'core/ai/autonomous_agent.py':'allow_live 更新/持久化（选择性段落）',
        'core/risk/risk_manager.py':'pre_trade_check 与 reset_halt',
        'core/trading/order_manager.py':'模拟报价/成交、账户订单作用域（选择性段落）',
        'core/trading/execution_engine.py':'账户隔离、反向平仓/风险路径（选择性段落）',
        'web/main.py':'启动/任务/健康、会话、WebSocket（选择性段落）',
        'core/ops/service/trading_routes.py':'风控复位旁路对照',
        'core/research/alastar_forward_paper.py':'冻结、收件、准入、同币零分布/停止门槛',
        'core/research/alastar_paper_tracker.py':'语义队列、时效、工作任务与汇总',
        'core/research/alastar_horizon_tracker.py':'共同成熟周期、损失路径、资金费与冻结',
        'core/research/retirement.py':'预登记、聚类与独立规则',
        'core/research/tracker_registry.py':'独立研究账本注册',
        'core/news/alastar_observation.py':'收件证据链、只读时延复算',
        'core/news/collectors/rss.py':'来源 URL/解析候选检查',
        'core/news/collectors/manager.py':'URL 归一化候选检查',
        'core/news/storage/db.py':'新闻 URL 保存与去重选择性检查',
        'prediction_markets/polymarket/paper_trading.py':'有限数、原子成交回归对照',
        'core/backtest/cost_models.py':'成本和滑点辅助函数',
        'core/backtest/common_pnl.py':'公共业绩口径',
        'web/static/js/dashboard_news.js':'URL 输出候选检查，未证实可利用 XSS',
        'tests/conftest.py':'测试隔离与临时路径',
        'scripts/web.ps1':'受管启动保护和只读 status（选择性段落）',
        'scripts/check_config_contract.ps1':'只读配置契约',
        'Dockerfile':'上下文、用户、入口/健康检查','.dockerignore':'构建上下文秘密过滤',
        'docker-compose.yml':'发布地址、服务凭据要求/健康、默认模式',
        'scripts/verify_env.py':'启动模块核验与版本契约差异','requirements.txt':'版本约束、开发工具和安全公告匹配'
    }
    executed={x.get('classname','').split('.')[1] for x in ET.parse(OUT/'pytest_215344.xml').iter('testcase') if len(x.get('classname','').split('.'))>1}
    file_rows=[]
    for row in inv['files']:
        p=row['path'];tags=['I'];checks=[]
        if row.get('syntax')=='ok':tags.append('S');checks.append('syntax_parse')
        if row.get('suffix') in ('.js','.cjs','.mjs'):tags.append('S');checks.append('node_check')
        if row.get('suffix')=='.ps1':tags.append('S');checks.append('powershell_parser')
        if p in deep:tags.append('D')
        if p.startswith('tests/') and Path(p).stem in executed:tags.append('V');checks.append('pytest_collected_module')
        if p=='tests/research_tracker_diagnostics.test.cjs':tags.append('V');checks.append('node_test_6')
        if p=='tests/ai_agent_workspace_smoke.cjs':tags.append('V');checks.append('fixture_browser_smoke')
        file_rows.append({'file':p,'levels':sorted(set(tags)),'checks':checks,'deep_scope':deep.get(p),'sha256':row.get('sha256')})
    save('file_coverage.json',{'definition':'D refers only to listed critical regions, V is test-file execution, not production line coverage','files':file_rows})
    coverage=f'''# 审计覆盖与边界 · 2026-10-22

审计对象：`{ROOT}`，HEAD `{inv['head']}` 加当前工作树。基线时间 `{inv['created_at_utc']}`。不是另一个分支，也没有把用户未提交的修改排除在外。

## 全量与深度的定义

- I：文件盘点、大小/SHA/类型及角色登记，不代表逐行审阅。
- S：适用文件的 Python AST、JSON/YAML 或 JS/PS 语法检查；另有路由装饰器提取与脱敏模式扫描。语法通过不等于语义正确。
- D：人工追踪列明的关键函数/边界；大模块只表示指定段落，不能解读为整文件每一行已审计。
- V：隔离执行的测试文件/特定合成反例或只读证据；没有安装 coverage 工具，不能提供生产代码行覆盖率。
- X：有明确原因未执行的行为/数据。保留边界，不把未观察到问题当作安全认证。

逐文件记录：{evidence('file_coverage.json')}。原始清单：{evidence('inventory.json')}；路由装饰器 {len(read('routes_static.json'))} 个：{evidence('routes_static.json')}。

## 基线量化

共 1534 个可盘点文件，1021 个 Python、333497 行 Python（包含测试、脚本和 reports 下可追踪的研究代码）；JSON 202、YAML 10、JavaScript/CJS 24、PowerShell 26。上述类型的语法/解析检查均通过。审计自身新增目录未计入基线。Git 忽略的业务目录里没有额外第一方源码文件；忽略的缓存、秘密及真实运行数据未进入测试副本。

| 模块 | 文件 | Python | 本轮覆盖方式 |
|---|---:|---:|---|
'''
    for area in ('core','web','config','strategies','prediction_markets','scripts','tests','docs','models','reports','data','.openclaw','(root)'):
        desc={'core':'S；交易/风控/研究/认证关键段 D；相关回归和反例 V','web':'S；权限/生命周期/状态关键段 D；接口反例和 fixture UI V','config':'S；模式/环境/测试隔离选择性检查','strategies':'S；既有策略测试 V，未重跑全市场回测','prediction_markets':'S；纸面交易路径对照与现有原子性回归 V','scripts':'S；受管启动、配置契约和审计工具相关段 D/V；未执行生产操作脚本','tests':'S；3313 个 pytest 用例含 2 跳过；另有 JS 和 fixture UI','docs':'I；AGENTS/SECURITY/启动、旧审计及 Alastar 交接选择性阅读','models':'I；未重训/重放全部模型，未加载不可信模型制品','reports':'I/S（按文件类型）；旧审计与最新 Alastar 研究选择性阅读','data':'I；跟踪到的非秘密文件；忽略的大型历史数据不属于全量测试输入','.openclaw':'I/S（按文件类型），未操作外部智能体','(root)':'I/S；Docker/环境/启动/pytest 契约 D'}[area]
        coverage+=f"| {area} | {counts['root_file_counts'].get(area,0)} | {counts['python_file_counts'].get(area,0)} | {desc} |\n"
    coverage+='\n## 关键段深读清单\n\n| 文件 | 实际检查内容 |\n|---|---|\n'
    for path,scope in deep.items():coverage+=f'| {link(path)} | {scope} |\n'
    coverage+='''
## 没有升级为确认问题的候选

- 新闻 RSS/URL 链路允许非 http(s) 的字符串，并被 HTML 转义后写入 href。没有完成带 target=_blank/noopener 等浏览器条件的同源可利用证明；本轮不将其升级为“已证实存储型 XSS”。后续应补协议白名单及浏览器反例，现阶段是未决检查项。
- ML 制品清理路径、协方差数据的前向/反向填充等搜索候选没有形成完整可达错误证据，不计入确认问题数量。
- 旧风险/交易回归、环境启动核验及语法检查通过，没有因此声称所有交易所行为或研究策略已验证。

## 本轮 X：明确没有执行

1. 真实账户连接、余额刷新、交易所下单/撤单/杠杆修改、账户资金或密钥操作；不验证实盘成交、保证金、爆仓或真实网络故障恢复。
2. Docker 镜像构建/启动、PostgreSQL 迁移与恢复演练、Redis/监控压力；部署配置仅静态审阅。
3. 全部行情仓库的完整性/时区/退市覆盖扫描、全量历史策略重算、模型训练/过拟合重新评估。既有对应测试可以回归，但不能替代对全部数据的重算。
4. 连接正在运行交易服务的全页面浏览器 E2E；仅执行完全拦截网络的 AI fixture UI。已有 playwright_smoke.spec.js 依赖 @playwright/test，仓库没有独立 Node 包清单；本轮不为跑它安装工具或接入生产服务。
5. 自动化定时任务长期运行、小时/天级断线恢复和负载/并发穷举。本机 GET 健康检查只是一个时间点的快照。
6. 所有 Git 历史内容、二进制、私有运行文件的完整秘密审计。只做当前可盘点源码的脱敏模式扫描和指定秘密文件名的历史路径核验。
7. 操作系统/conda 系统库/CUDA、Node/browser 包和实际镜像的供应链漏洞扫描。OSV 查询仅覆盖 129 个当前 Python 分发包；上游命中尚未逐条完成应用可达性证明。

本轮以全项目盘点和关键风险证据为交付边界。未给出“可安全实盘”“全部源码逐行证明”或“历史策略收益可信”的通行结论。
'''
    (ROOT/'reports/full_audit_2026-10-22_coverage.md').write_text(coverage,encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=True,indent=2))

if __name__=='__main__':build()
