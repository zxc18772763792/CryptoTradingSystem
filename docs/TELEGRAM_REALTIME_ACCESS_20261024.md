# Telegram 三来源实时接入研究

核查日期：2026-10-24，Asia/Shanghai。本文记录接入研究及后续经用户授权的本机会话验证。已建立并加密保存独立 Telethon 用户会话，详情见末节；尚未开通常驻采集、修改交易运行配置、发送聊天消息或启动新的模型任务。

## 结论

技术上可通过独立 Telegram 用户客户端接收这三个来源的新消息、消息编辑，并在账号有访问权限的范围内获取历史；不必依赖桌面窗口、截图或反复导出 JSON。账号能读到内容，不代表已获得持续采集、保存、AI处理和交易用途的全部许可。

建议先验证 CryptoQuant 官方结构化数据 API 的产品价值，同时解决两个群的内容使用授权。获得适用许可后，再实现白名单式用户 API 采集与模拟观察。不要把实时接入等同于已有可靠交易信号。

## 三个来源分别怎么接

| 来源 | 可行技术入口 | 使用定位与限制 |
|---|---|---|
| `cryptoquant_official` | 获许可后用用户 API 接收频道更新；已有公开页解析器可作许可范围内的可选辅助 | 原频道研究观点。公开预览属于轮询，有延迟、可见范围与漏读限制，不能保证消息级完整性 |
| `BinanceChinese` | 登录账号的用户 API，前提是账号可访问该群及相应历史 | 群情绪候选来源；需要去机器人/广告/复读、按作者等权，尚无本项目真实群语料验证 |
| `Binance_api_Chinese` | 同上 | 从其公开群简介“仅讨论API和相关程序设计”推断，优先评估为技术事件来源；报错、签名错误、下单参数不是市场恐慌或看空结论 |

不能仅靠现有通知用 Bot Token 读取任意第三方群或频道：机器人须被允许加入来源，群内还受隐私模式和权限影响。`messages.getHistory` 是用户方法，普通 Bot API 不是历史补采通道。

来源：[Bot FAQ](https://core.telegram.org/bots/faq)、[用户历史接口](https://core.telegram.org/method/messages.getHistory)、[API中文群定位](https://t.me/Binance_api_Chinese)。

## 先补齐内容许可核对

Telegram 当前的[API条款1.5](https://core.telegram.org/api/terms)及[内容许可条款](https://telegram.org/tos/content-licensing)明确涉及 AI/机器学习的训练、微调、验证、开发、增强、基准测试及部署；不能假定“仅推理、不训练”自动豁免。

内容条款列明的例外涉及所有相关用户针对特定内容/群聊范围作出明确、知情、肯定、持续的个别同意。单个订阅者的同意不代表作者授权；群管理员的许可也不能直接当成所有发言者的个别同意。具体适用许可及平台条件需在上线前确认。

上一轮建议导出 JSON 解决的是数据格式与可读性，并没有解决上述内容使用许可。现有回溯研究不能充当持续 AI 部署的授权证明。复制到自建群、先导出再上传、改成本地模型或词典，也不应当被当作绕过内容许可的途径。

对于难以取得适用许可的大群，优先选择允许该用途的数据服务或由主动同意参与者组成的独立研究群；新研究群不能借机转存原群未获授权的消息。对供应商可明确询问：是否允许私人量化研究、持续消息保存、第三方模型推理、衍生情绪指标及模拟/交易用途，保存期限及删除要求是什么。

## 你的准备步骤

1. 明确用途与许可：区分普通消息客户端、研究归档、AI情绪推理及后续交易用途；记录允许的来源、内容范围、处理方式、期限与撤回要求。
2. 在你已登录的 Telegram 账号中确认三个地址均能打开，并核对可见历史；不绕过群权限、历史可见性或付费限制。
3. 如采用用户 API，由你本人在 [my.telegram.org](https://my.telegram.org) 的 API development tools 创建应用，取得自己的 `api_id` 与 `api_hash`，保存在本机受保护配置中。官方提供的是 Telegram 应用凭据，不是 OpenAI 密钥，也不是 BotFather 的 Bot Token。
4. 在本机完成独立采集客户端的账号登录；验证码、二步验证密码由你本人输入，不发到聊天。新客户端可使用同一合法账号，不需要复制桌面版 `tdata`。桌面版已登录并不自动授权新程序。
5. 为后台进程单独检查网络/代理：桌面版能连接，不代表 Python 客户端自动使用同一代理。首次连通后确认实际可读来源、事件延迟与历史范围，再开常驻进程。
6. 按允许范围做初始历史同步、实时接收与断线续传；历史补采单独标记，不能回填成当时已实时收到的前向信号。
7. 先验证数据覆盖和模拟观察结果，再决定是否加入交易决策。前一次 CryptoQuant 回测没有证明稳定独立优势，不能因为接通了数据就自动注册/下单。

来源：[官方应用申请](https://core.telegram.org/api/obtaining_api_id)、[Telethon登录及代理](https://docs.telethon.dev/en/stable/basic/signing-in.html)。

## 后台客户端与账号会话

本项目为 Python/asyncio，原型可选择 Telethon；官方 TDLib 是另一条可行路线，提供连接、存储和更新顺序管理，但其本地库部署与当前 conda 原生依赖需另行验证。本次没有安装新依赖。

登录后的后台服务不依赖桌面 Telegram 窗口保持打开；采集程序及其主机仍须运行且网络可用，主机睡眠期间无法声称实时在线。应保存更新状态并在恢复后补齐可恢复缺口，不能承诺补回已被删除或从未可见的内容。

用户 session 文件含账号授权能力。“只读”需要由程序接口、固定聊天白名单及运行权限限制；普通账号 session 不是只允许读取三个群的服务器侧受限令牌。session 不上传、不输出到日志、不提交 Git，使用独立受保护目录。授权撤回或账号退出时应显示明确故障状态。

来源：[Telethon sessions](https://docs.telethon.dev/en/stable/concepts/sessions.html)、[Telegram Updates](https://core.telegram.org/api/updates)、[TDLib](https://core.telegram.org/tdlib)。

## 项目现状：已检查代码与脱敏配置存在性

- `core/news/collectors/telegram.py` 为 `t.me/s/<channel>` 公开预览采集器，支持白名单与首见/修订归档；当前 YAML 白名单只配置 `alastartrades`。
- `core/news/service/worker.py` 的 `telegram_alastar` 默认间隔为60秒。这只是默认轮询周期，不是端到端延迟保证。
- 当前通知模块使用 Bot API `sendMessage`；有通知机器人不等于能读取两个官方群。
- 已有 `core/research/telegram_text_signals.py` 的 JSON 导入、脱敏与作者等权小时情绪特征，尚不是登录态实时采集器；它的参数与冻结研究规则不应被静默改成另一套周期。
- 存在 `core/data/cryptoquant_collector.py` 的可选链上 API 骨架，描述为4小时缓存刷新；这不是 Analyst Consensus API 接入实现，也没有在本次通过付费权限实测。
- 脱敏存在性检查：本机相关环境及 `.env` / `.env.local` 未发现 `TELEGRAM_API_ID`、`TELEGRAM_API_HASH`、`TG_API_ID`、`TG_API_HASH`、`CRYPTOQUANT_API_KEY` 配置，亦无现有 CryptoQuant key 文件。没有读取或输出密钥值。

## 获许可后的实现设计

建议独立 `telegram_user_worker`，与 Web 进程、通知机器人及 Alastar 既有冻结账本隔离：

```text
用户 API 的新消息 / 编辑事件
        ↓
聊天数字 ID 白名单 + 来源身份核对
        ↓
许可范围与有效期限检查
        ↓
接收时间 / 发布与编辑时间 / 来源 / 修订指纹
        ↓
去重、脱敏、按许可期限保存
        ↓
获准来源的文本处理队列
        ↓
分来源情绪或技术事件 → 策略面板 → 模拟观察
```

数据质量要求：

- 使用 `(chat_id, message_id)` 标识消息，并单独记录修订；不只按标题或URL做去重。
- 同时记录 `published_at`、`received_at`、`edited_at`、是否补采；前向使用实际可用时点。实时归档不能还原接入前的历史稿。
- 更新序列有缺口时恢复并补采；退出权限、断线、限流与积压显式告警。遵守服务端等待时间，不靠频繁重登或换账号绕过限流。
- 编辑不覆盖已发布信号的原始时点记录；内容留存、删除和撤回遵循适用许可及平台要求，不能以审计不可变为由保存被要求删除/过期的正文。
- 删除事件是尽力接收，不能承诺100%捕获所有删除。Telethon 文档明确说明删除通知并非完全可靠。
- 群、研究频道分别计算；API技术讨论单独分类；不将群消息条数直接当作看涨强度。沿用现有作者等权小时特征可作初始观察基线，缩短至5/15分钟需另立研究规则与验证。
- 状态面板显示连接状态、最近接收时间、处理积压、有效作者/观点样本、许可状态及模型错误；未接收数据不显示为中性情绪。

来源：[事件接口](https://docs.telethon.dev/en/stable/quick-references/events-reference.html)、[更新状态与缺口恢复](https://core.telegram.org/api/updates)。

## 更直接的 CryptoQuant 结构化情绪入口

官方 [Analyst Consensus API](https://docs.cryptoquant.com/sentiment/overview) 已提供分析师共识、看涨/看跌比例、近期观点等结构化数据。2026-10-24 从本机用无凭据 GET 实测：

| 请求 | 结果 |
|---|---|
| `https://consensus.cryptoquant.com/api/v1/consensus?asset=BTC` | HTTP 200，JSON含资产、日期、共识指数、看涨/看跌比例等字段 |
| `https://consensus.cryptoquant.com/openapi.json` | HTTP 200，当前 `info.version=3.1.0` |

这里只验证连通与字段存在，未购买套餐、创建密钥、分析数值或接入项目运行链路。

要区分：

- 这是多平台分析师聚合数据，应作为独立的“CryptoQuant分析师共识”源；不等于 `cryptoquant_official` Telegram频道逐帖情绪，更不代表两个中文群。
- 官方当前说明指数与stance为已完成UTC日的日频数据，公开载荷有5分钟CDN缓存；不能作为秒级消息流宣传。breakdown有自己的更新节奏。[覆盖与更新说明](https://docs.cryptoquant.com/sentiment/data-coverage)
- 最新公开数据可先验证；历史、逐分析师数据及费用取决于当前套餐和key权限，尚未验证本机历史访问权。
- Analyst Consensus key/请求头与普通 CryptoQuant Data API 的 Bearer token 不同。实施时按实时 OpenAPI 核对，不把两个接口或凭据混用。
- 完整原始帖子正文在官方文档和实测 OpenAPI 中均列为单独的 Integration 授权；不能认为购买Premium/Enterprise就自动获得正文权限。[权限说明](https://docs.cryptoquant.com/sentiment/plans-and-access)
- 官方文档示例提到的API版本与实测 `info.version` 不完全一致，接入时以实时OpenAPI及实际响应复核。响应中的日期、生成时间和实际接收时间分别保存。
- 使用官方声明支持的接口，也仍需遵守该供应商对分析、再分发和模型用途的适用条款。无需抓取Telegram再复制进另一个系统以取得同类聚合数值。

## 推荐顺序

1. 先验证 CryptoQuant 最新结构化共识数据的可用性和业务价值，再决定是否申请历史权限；不必为首次连通测试付费。
2. 解决两个群及原频道的适用内容许可；若不能取得，就换用覆盖所需用途的授权数据源，而不是默认启动大群AI采集。
3. 获许可后完成用户 API 凭据与本人登录、网络测试，再开发独立采集工作进程。
4. 接收完整性、时点与模型处理许可确认后，做分来源前向模拟观察。本次研究没有变更现有交易与数据源配置。

## GitHub 工具与 Bot / 用户会话核查补充

2026-10-24：Bot Token 与用户客户端的 `api_id/api_hash` 是不同凭据。Bot 可用于通知、用户主动发给它的消息，以及被允许加入的来源中的消息；不能因持有 Token 就读取任意第三方群。群内消息范围还取决于权限及隐私模式。参见 [Bot FAQ](https://core.telegram.org/bots/faq)。聊天中披露过的 Bot Token 应先在 BotFather 撤销并重新生成，不将原值写入本报告或代码。

本机项目解释器的环境校验通过；实际导入 Telethon 成功，版本为 `1.36.0`。对进程环境及 `.env` / `.env.local` 的脱敏检查未发现用户 API 凭据；因此本次没有完成用户账号的网络认证，也没有验证三个来源的消息接收。标准 Telethon 登录（包括使用 Bot Token 的 MTProto 登录）仍需应用 API 凭据。[登录文档](https://docs.telethon.dev/en/stable/basic/signing-in.html)

| 工具 | 接入方式 | 对当前问题的意义 |
|---|---|---|
| [Telethon](https://github.com/LonamiWebs/Telethon) | 用户 MTProto 客户端，支持历史及更新事件 | 标准路径仍需 API ID/hash。GitHub 仓库已归档并声明迁往 Codeberg，不等于项目整体停止维护 |
| [opentele](https://github.com/thedemons/opentele) | 桌面 `tdata` 与 Telethon 会话转换，附带官方客户端 API 预设 | 作者 README 明确停止维护；现场仓库最后推送为2024-07-15，不宜直接视为当前桌面版的稳定方案 |
| [tdata_reader](https://github.com/kr-aleksey/tdata_reader) | 将本机桌面授权转换为 Telethon 会话，依赖 Telethon、内置客户端 API 预设 | 理论上可以省去个人应用申请，但不是无需 Telegram API。现场仓库仅1次提交、0星，维护与兼容性证据有限，应先做源码及格式兼容性验证 |
| [telegram-channel-scraper](https://github.com/the-ai-entrepreneur-ai-hub/telegram-channel-scraper) | 公开网页预览及可选 API 后端 | 无凭据模式只解决公开预览可见内容，不能据此认定可读取两个群的完整历史与实时流 |

静态核查通过 GitHub 公共 API 获取仓库元数据，并以 AST 解析 `tdata_reader` 的7个 Python 源文件，共643行；未执行这些第三方源码。导入项及显式网络/子进程调用关键词检查未发现相应引用，这只是有限的初筛，不是无外传或安全性的证明，亦未检查发行包与仓库源码的一致性。没有读取、复制或转换本机 `tdata`，没有创建账号会话。

会话转换器所提供的 API ID/hash 来自客户端预设，不应误报为从桌面存储中找回了属于用户自己的应用凭据。不能采信项目关于伪装客户端即可避免检测或封禁的保证。若后续实验，应先审查固定提交的源码、核对当前桌面格式，仅在本机保护会话，并先验证身份和访问权限；不要将会话或本地锁密码交给第三方转换网站。

## 桌面会话转 Telethon：本机验证结果

本节更新了上一节“尚未读取桌面会话”的历史状态。用户随后明确授权继续验证这条路径。

- 桌面客户端：Telegram Desktop `7.3.3.0`；离线解析得到1个账号，授权密钥长度有效。
- 转换源码固定为 `kr-aleksey/tdata_reader` 的提交 `364a8479110bdc70badef891760ebf883da1527a`，7个 Python 文件逐一审阅并固定 SHA-256。源码位于被 Git 忽略的 `runtime/telegram_tool_audit/`，未安装 PyPI 同名发行包。
- 通过已有桌面授权完成身份核对；当前服务器记录的应用 API ID 与转换器桌面预设一致。桌面版及脚本使用同一现有本地代理 `127.0.0.1:7890`；没有轮换出口或修改代理配置。
- 短时退出桌面客户端后，用原授权批准了一个独立的 Telethon 登录，确认新授权密钥与桌面密钥不同。验证结束已恢复桌面客户端；后续独立会话探测不读取原 `tdata`。
- 独立会话只以 Windows CurrentUser DPAPI 密文保存于 `%USERPROFILE%\.crypto_trading_system\telegram\reader_session.dpapi`。目录关闭继承，文件访问规则仅当前用户和 SYSTEM。没有生成明文 `.session` 文件或将 StringSession 写入输出。
- 已验证密文可解密并重新连接，同一账号身份核对通过。最初一次存储预检因 MSIX AppData 路径虚拟化失败；那次未保存的测试授权已明确撤销，改为稳定用户目录后成功。脚本也新增了存储预检和失败时撤销本次新授权的处理，保留原桌面授权。

| 来源 | 成员状态 | 历史访问 | 实时状态 |
|---|---|---|---|
| `cryptoquant_official` | 已订阅 | 成功，读取最近一条的时间元数据 | 最后120秒观测成功取得增量响应，期间无新帖、无事件；尚无真实新帖接收样本 |
| `BinanceChinese` | 已入群 | 成功 | 最后120秒观测同上，历史没有新增；持续实时覆盖仍待后续活跃窗口确认 |
| `Binance_api_Chinese` | 未入群 | 可读取公开历史 | 本轮未纳入监听；显式查看公开群时的短轮询可另行验证，长期成员订阅也可由本人加入后验证 |

### 使用本机验证脚本

`scripts/telegram_desktop_bridge.py` 是有界接入验证脚本，不是已接入 Web 生命周期的后台采集器。仅核对来源、成员状态、最近消息时间及白名单事件计数；不输出或归档聊天正文，不发送消息，不自动入群，不调用模型、不参与交易。用户账号会话本身仍是账号级授权，白名单是脚本层面的限制。

在项目根目录执行已有独立会话的访问检查：

```powershell
& 'F:\9_Crypto\.conda\miniforge3\envs\crypto_trading\python.exe' scripts\telegram_desktop_bridge.py --probe --watch-seconds 180
```

状态写入 Git 忽略的 `runtime/telegram_bridge_status.json`，只含脱敏连接结果和三个固定来源的元数据。`source_events_received` 表示实际收到白名单消息/编辑事件；`no_source_events_observed` 仅表示观测窗口内未见事件。脚本另比较监听前后最新消息；若历史新增但消息计数为0，将输出 `watch_gap_detected` 并以非零状态退出，避免将漏收误判为来源无活动。

首次单纯监听被动推送的180秒实验发现 BinanceChinese 历史新增、事件计数为0，不能认定实时成功。随后按 [Telegram 官方更新机制](https://core.telegram.org/api/updates#subscribing-to-updates-of-channels-supergroups)增加 `updates.getChannelDifference`：初始化固定来源的 `pts`，遵守服务端返回的 `timeout`，将增量与推送按消息/修订去重；差量过大、状态倒退、无进展或限流时显式失败。最后一轮120秒观测中，两个来源均返回增量响应，历史无新增，未获得真实新消息的前向接收样本。该结果验证了账号认证、历史访问及增量接口可调用性，尚不构成持续实时覆盖的证明。

`--bootstrap --tdata <本机路径>` 只供首次授权或明确重新授权时使用：已有密文时拒绝覆盖，桌面客户端运行时拒绝复用原授权；不循环登录、不自动等待或绕过限流。若遇二步验证，会以密文保留待完成的独立会话，由本人在本机交互终端运行 `--probe --complete-login` 输入密码，密码不发到聊天或作为命令行参数。本次实际授权未要求输入二步验证密码。

8项针对性测试通过，覆盖 DPAPI 错误上下文拒绝解密、源码篡改/额外模块拦截、存储预检失败时不读取桌面凭据、保存失败时仅撤销新建测试授权、事件去重且不读取正文/作者、缺少频道状态时拒绝报告成功，以及被动推送缺失时通过官方差量取得消息。持续采集的状态持久化、完整缺口恢复及 AI 内容处理仍是后续工作，不由这次连接成功推定完成或获许可。
