# 被gitignore排除的运行辅助脚本：补充静态审计

日期：2026-10-01。范围严格限定下面5个文件，共301行；全部逐文件只读审阅，3个Python文件通过AST解析，2个PowerShell文件按源码/控制流静态检查。**没有执行这5个脚本、它们的函数或其子脚本**，没有访问网络、账户、计划任务、进程运行状态、日志内容或数据文件，也未读取任何secret配置。工具显示的源码字符串全部经过遮蔽；本报告不包含token值、私人账户数据或代理凭据。只新增本审计报告，原文件保留。

`git check-ignore`确认5个都被忽略。ignore仅说明版本追踪状态，不代表脚本不能运行。它们属于人工WS验收、切换和监控辅助；硬编码路径/日期/本地端口表明用途专一，但不能仅凭源码证明目前没有被调用。尤其两个PowerShell脚本注册持久计划任务，副作用可超过一次人工执行。

## 覆盖与副作用

| 文件 | 行数 | 用途与静态确认副作用 | 运行/历史边界 |
|---|---:|---|---|
| data/_level1_relaxed_run.py | 46 | 运行带放宽容忍参数的market WS shadow selfcheck，再调用报告evaluator；创建时间戳stdout/stderr文件，向本地服务传token CLI参数 | ROOT与WT固定且不同，selfcheck/evaluator来自另一固定目录；未审阅或运行该目录。本脚本本身不包含直接交易下单调用，但被调用工具行为未在此次五文件范围内证明 |
| data/_level1_watcher.py | 54 | 等待固定STAMP目标日志/JSON最多7.5h，再运行evaluator；读取指定历史输出和service error日志，打印评估文本 | 固定STAMP使其更像某次验收的收尾器，不会自动选最新run。未读取对应日志，不证明当前被调用 |
| data/_restart_live8000.ps1 | 61 | 写启动.cmd；设置WS shadow配置；检查8000监听进程命令行，强制停止旧web.main，注册无限运行时间计划任务，再轮询health | 完整可执行运维脚本，不仅是说明文本；可以产生持久计划任务。名字含live，但源码未显式设置全局trading_mode=live，不能仅从文件名断言它启动实盘 |
| data/_switch_strategy_primary.ps1 | 62 | 同上，WS mode设strategy_primary，fail_closed=true；强制切换8000服务并注册持久任务 | 可切到策略主行情模式；注释把fail_closed=true描述为不健康时回REST，语义容易误解，实际运行语义仍由项目provider决定 |
| data/_ws_proxy_monitor.py | 78 | 默认6h循环，经指定代理GET公共Binance futures time及stream endpoint；按周期append JSONL并打印统计 | 公共连通性监控，不访问私有交易账户、不创建订单；把代理字符串写入LOG首条，若代理URL携带认证信息会落日志，未读取或显示实际代理值 |

## 已确认的问题

### IS1 · P2 · selfcheck/evaluator失败被打印成FAIL，但脚本仍正常退出0

定位：[data/_level1_relaxed_run.py:34](F:/9_Crypto/crypto_trading_system/data/_level1_relaxed_run.py:34)–46；[data/_level1_watcher.py:46](F:/9_Crypto/crypto_trading_system/data/_level1_watcher.py:46)–54。

静态证明：relaxed_run保存selfcheck返回码，只打印后继续评估；evaluator返回码仅用于打印PASS/FAIL，没有 `sys.exit(r.returncode)` 或raise。watcher也只打印evaluator结果；没有目标输出时只打印说明。脚本主体最终正常结束。

触发：子评估返回非零，或watcher直到超时仍无输出；只要脚本自身没有另一个未捕获异常，父shell看到exit0。影响：把这些辅助脚本接入调度/CI/外部告警时，失败验收可被当任务成功。此缺陷限于这些helper的退出码，不能据此断言主工程evaluator/标准验收同样误判。

建议：selfcheck失败是否允许生成诊断可显式约定，但最终退出码必须反映selfcheck与evaluator结果；watcher超时/缺失输出返回独立非零码。不要只在文本中写FAIL。

### IS2 · P2 · 代理监控把futures time接口HTTP错误计为健康

定位：[data/_ws_proxy_monitor.py:34](F:/9_Crypto/crypto_trading_system/data/_ws_proxy_monitor.py:34)–40、[data/_ws_proxy_monitor.py:55](F:/9_Crypto/crypto_trading_system/data/_ws_proxy_monitor.py:55)–63。

静态证明：probe只要requests.get收到HTTP响应就返回 `(True, r.status_code, ...)`，没有raise_for_status或status筛选。stream结果有200/400/404选择，但总good表达式仅检查f_ok和s_reach，忽略f_code。因此time接口返回401、429或500，而stream可达时，会计入ok并清零失败连续计数。

影响：若拿该统计证明“行情WS/REST链路健康”，服务限频/错误可被隐藏；对单纯TCP/TLS连通性检查，收到HTTP错误确能表示连通，所以报告需明确定义指标，不能把这个脚本统计直接当市场数据正常。它也没有读取time payload、建立WebSocket或检查消息freshness；不能当WS健康验收。

建议：区分transport_reachable与endpoint_healthy，time接口healthy要求HTTP200及合法serverTime；若目标是WS健康，应做实际WS握手和消息/时间戳检查。监控整体失败也应给调用方可读的退出状态。

## 运行风险和敏感信息边界

### IS3 · 条件性运维风险：绕过管理启动器，强杀后由独立持久任务直接运行uvicorn

定位：[data/_restart_live8000.ps1:19](F:/9_Crypto/crypto_trading_system/data/_restart_live8000.ps1:19)、[data/_restart_live8000.ps1:40](F:/9_Crypto/crypto_trading_system/data/_restart_live8000.ps1:40)–48；[data/_switch_strategy_primary.ps1:20](F:/9_Crypto/crypto_trading_system/data/_switch_strategy_primary.ps1:20)、[data/_switch_strategy_primary.ps1:41](F:/9_Crypto/crypto_trading_system/data/_switch_strategy_primary.ps1:41)–49。

两者直接以项目Python运行 `uvicorn web.main:app --host 127.0.0.1 --port 8000`，不经过web.ps1/web.bat supervisor；Register-ScheduledTask -Force可覆盖同名任务，Stop-Process -Force强制停止旧服务。源码有命令行web.main校验，避免误杀明显不相关的8000进程，但不是受管服务生命周期校验或优雅停止。

这与AGENTS.md第4节“Do not start uvicorn by hand — the supervisor owns process lifecycle”相冲突，绕过启动器提供的生命周期/pid/worker管理及paper默认启动约束。源码注释说明其他配置来自.env；本审计未读取.env，**没有证明新服务实际mode或现存计划任务**，因此此项作为历史/人工辅助的条件风险，不增加已确认当前生产缺陷计数。

建议：若仍需使用，把WS配置交给受管启动流程；记录变更、优雅停止并检查旧进程退出。若已弃用，移入明确的archive并添加不可误运行的提示，不直接删除用户现有文件。

### IS4 · 固定token与代理日志：确认存在传播路径，但未验证任何凭据有效性

relaxed_run.py:12含固定TOKEN字面量，23将它放入 `--token` 参数；34由subprocess.run创建子进程。值未显示、未验证，因此不声明它是有效生产凭据，但若有效会出现在子进程命令行并留在被忽略源码中。优先从安全运行环境/受限凭据接口传入，避免argv含token。

ws_proxy_monitor.py:24–29从env/default取代理；50把完整PROXY写入JSONL。若用户配置含userinfo的认证代理URL，日志会记录它。这里确认的是代码传播路径，不是本环境存在真实泄露。日志应只保存host/port和必要脱敏标记。

## 结论边界

五文件覆盖完毕并停止扩展。补充发现为helper自身的两个P2静态逻辑缺陷，以及条件性启动/凭据传播风险。未执行历史自检、未操作服务、未审阅另一个目录或统计真实运行状态；没有把固定STAMP收尾器等同于当前受管生产启动入口。这一补充可并入主审计覆盖附录，不改变交易分报告的8个资金路径结论。

